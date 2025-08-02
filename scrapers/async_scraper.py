#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
リファクタリングされた非同期食べログスクレイパー
"""

import asyncio
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
import random
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path

from scrapers.base import BaseTabelogScraper
from utils.progress import BatchProgressTracker
from utils.error_handler import (
    ErrorHandler, NetworkError, ParseError,
    retry_on_error, NetworkErrorHandler
)
from utils.validators import RestaurantData, DataValidator
from config.settings import settings
from config.constants import USER_AGENTS


class TabelogAsyncScraper(BaseTabelogScraper):
    """非同期食べログスクレイパー"""
    
    def __init__(self, max_concurrent: Optional[int] = None):
        """
        初期化
        
        Args:
            max_concurrent: 最大同時接続数
        """
        super().__init__()
        
        # 設定を読み込み
        self.max_concurrent = max_concurrent or settings.max_concurrent
        self.delay_range = settings.get_delay_range()
        self.timeout = settings.timeout
        self.batch_size = settings.batch_size
        
        # セマフォとセッション
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 進捗トラッカー
        progress_file = Path(settings.cache_dir) / 'async_progress.json'
        results_file = Path(settings.cache_dir) / 'async_results.json'
        self.progress_tracker = BatchProgressTracker(
            str(progress_file),
            str(results_file),
            self.batch_size
        )
        
        # エラーハンドラー
        self.error_handler = ErrorHandler(max_retries=3)
    
    async def __aenter__(self):
        """非同期コンテキストマネージャーの開始"""
        timeout = ClientTimeout(total=self.timeout, connect=10, sock_read=10)
        connector = TCPConnector(limit=self.max_concurrent, force_close=True)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                'User-Agent': USER_AGENTS['windows'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        # 進捗を読み込む
        self.progress_tracker.load_progress()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同期コンテキストマネージャーの終了"""
        if self.session:
            await self.session.close()
    
    async def scrape_restaurants(
        self,
        areas: List[str],
        max_per_area: int
    ) -> List[Dict]:
        """
        レストラン情報を非同期で取得
        
        Args:
            areas: 対象地域リスト
            max_per_area: 地域あたりの最大取得件数
            
        Returns:
            レストラン情報のリスト
        """
        all_restaurants = []
        area_urls = self.get_area_urls()
        
        for area in areas:
            if area not in area_urls:
                self.log_warning(f"未対応の地域: {area}")
                continue
            
            self.log_info(f"🍽️ {area}のデータ取得開始")
            area_url = area_urls[area]
            
            # 必要なページ数を計算（1ページ20件として）
            max_pages = (max_per_area + 19) // 20
            
            # 店舗URLリストを取得
            restaurant_urls = await self._scrape_restaurant_list(area_url, max_pages)
            
            # 指定件数に制限
            restaurant_urls = restaurant_urls[:max_per_area]
            self.progress_tracker.set_total_items(len(restaurant_urls))
            
            # バッチで詳細情報を取得
            restaurants = await self._scrape_details_batch(restaurant_urls)
            all_restaurants.extend(restaurants)
            
            self.log_info(f"✅ {area}から {len(restaurants)} 件取得完了")
        
        # 最終保存
        self.progress_tracker.save_progress()
        
        return all_restaurants
    
    @retry_on_error(exceptions=(NetworkError,), max_attempts=3)
    async def _fetch_page(self, url: str) -> Optional[str]:
        """ページを非同期で取得"""
        async with self.semaphore:
            try:
                # ランダムな遅延
                delay = random.uniform(*self.delay_range)
                await asyncio.sleep(delay)
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        raise NetworkError(f"Rate limited: {url}")
                    else:
                        self.log_warning(f"HTTPエラー {response.status}: {url}")
                        return None
                        
            except asyncio.TimeoutError:
                self.error_handler.log_error_with_context(
                    TimeoutError("Request timed out"),
                    "ページ取得",
                    url
                )
                raise NetworkError(f"Timeout: {url}")
            except Exception as e:
                self.error_handler.log_error_with_context(e, "ページ取得", url)
                raise
    
    async def _scrape_restaurant_list(
        self,
        area_url: str,
        max_pages: int
    ) -> List[str]:
        """レストランリストページから個別店舗URLを取得"""
        restaurant_urls = []
        
        # ページURLを生成
        page_urls = [area_url]
        for page in range(2, max_pages + 1):
            page_urls.append(f"{area_url}rstLst/{page}/")
        
        # 非同期でページを取得
        tasks = [self._fetch_page(url) for url in page_urls]
        pages = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 各ページから店舗URLを抽出
        for i, result in enumerate(pages):
            if isinstance(result, Exception):
                self.log_error(f"ページ {i+1} の取得に失敗: {result}")
                continue
                
            if result:
                try:
                    soup = BeautifulSoup(result, 'html.parser')
                    links = self._extract_restaurant_links(soup)
                    
                    for href in links:
                        full_url = f"{self.base_url}{href}" if href.startswith('/') else href
                        if full_url not in restaurant_urls:
                            restaurant_urls.append(full_url)
                    
                    self.log_info(f"ページ {i+1}: {len(links)}件の店舗URL取得")
                except Exception as e:
                    self.error_handler.log_error_with_context(
                        e, f"ページ {i+1} のパース", page_urls[i]
                    )
        
        self.log_info(f"合計 {len(restaurant_urls)} 件の店舗URL取得")
        return restaurant_urls
    
    async def _scrape_details_batch(self, restaurant_urls: List[str]) -> List[Dict]:
        """バッチで詳細情報を取得"""
        all_restaurants = []
        
        for i in range(0, len(restaurant_urls), self.batch_size):
            batch_urls = restaurant_urls[i:i + self.batch_size]
            batch_end = min(i + self.batch_size, len(restaurant_urls))
            
            # 未処理のURLのみ取得
            urls_to_process = [
                url for url in batch_urls 
                if not self.progress_tracker.is_processed(url)
            ]
            
            if not urls_to_process:
                continue
            
            self.progress_tracker.start_batch(i // self.batch_size + 1, i, batch_end)
            
            # 詳細情報を非同期で取得
            tasks = [self._scrape_restaurant_detail(url) for url in urls_to_process]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 有効な結果のみ処理
            for url, result in zip(urls_to_process, results):
                if isinstance(result, Exception):
                    self.log_error(f"詳細取得エラー {url}: {result}")
                    continue
                
                if result:
                    all_restaurants.append(result)
                    self.progress_tracker.add_result(result)
                
                self.progress_tracker.mark_as_processed(url)
            
            # バッチ完了
            self.progress_tracker.complete_batch()
        
        return all_restaurants
    
    async def _scrape_restaurant_detail(self, restaurant_url: str) -> Optional[Dict]:
        """個別店舗ページから詳細情報を取得"""
        try:
            # メインページを取得
            main_html = await self._fetch_page(restaurant_url)
            if not main_html:
                return None
            
            soup = BeautifulSoup(main_html, 'html.parser')
            
            # 基本情報を抽出
            shop_name = self._extract_shop_name(soup)
            if not shop_name:
                self.log_warning(f"店名が取得できませんでした: {restaurant_url}")
                return None
            
            # 詳細情報を抽出
            restaurant_data = RestaurantData(
                shop_name=shop_name,
                phone=self._extract_phone(soup),
                address=self._extract_address(soup),
                genre=self._extract_genre(soup),
                station=self._extract_station(soup),
                open_time=self._extract_open_time(soup),
                url=restaurant_url,
                source='食べログ'
            )
            
            # データのバリデーション
            is_valid, errors = DataValidator.validate_restaurant_data(restaurant_data)
            if not is_valid:
                self.log_warning(f"データ検証エラー {restaurant_url}: {errors}")
                return None
            
            # 電話番号と住所が不足している場合は地図ページも確認
            if not restaurant_data.phone or not restaurant_data.address:
                map_url = restaurant_url.rstrip('/') + '/dtlmap/'
                map_html = await self._fetch_page(map_url)
                
                if map_html:
                    map_soup = BeautifulSoup(map_html, 'html.parser')
                    if not restaurant_data.phone:
                        restaurant_data.phone = self._extract_phone(map_soup)
                    if not restaurant_data.address:
                        restaurant_data.address = self._extract_address(map_soup)
            
            # 正規化
            restaurant_data.phone = DataValidator.normalize_phone_number(restaurant_data.phone)
            restaurant_data.shop_name = DataValidator.clean_text(restaurant_data.shop_name)
            restaurant_data.address = DataValidator.clean_text(restaurant_data.address)
            
            result_count = len(self.progress_tracker.results) + 1
            self.log_info(f"✅ 店舗情報取得成功 [{result_count}件目]: {shop_name}")
            
            return restaurant_data.to_dict()
            
        except Exception as e:
            self.error_handler.log_error_with_context(
                e, "店舗詳細取得", restaurant_url
            )
            return None