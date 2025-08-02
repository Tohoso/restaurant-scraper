#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
非同期版食べログスクレイピングクライアント
大量データ取得に対応した高速スクレイパー
"""

import asyncio
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
import time
import re
from typing import List, Dict, Optional, Set
import logging
from urllib.parse import urljoin, urlparse
import random
from datetime import datetime
import json
import os
from pathlib import Path

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TabelogScraperAsync:
    """非同期版食べログスクレイパー"""
    
    def __init__(self, max_concurrent: int = 10, delay_range: tuple = (0.5, 1.0)):
        """
        初期化
        
        Args:
            max_concurrent: 最大同時接続数
            delay_range: リクエスト間の遅延範囲（秒）
        """
        self.max_concurrent = max_concurrent
        self.delay_range = delay_range
        self.base_url = "https://tabelog.com"
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.progress_file = Path("cache/scraping_progress.json")
        self.results_file = Path("cache/partial_results.json")
        self.processed_urls: Set[str] = set()
        self.results: List[Dict] = []
        
    async def __aenter__(self):
        """非同期コンテキストマネージャーの開始"""
        timeout = ClientTimeout(total=30, connect=10, sock_read=10)
        connector = TCPConnector(limit=self.max_concurrent, force_close=True)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
        )
        # 進捗を読み込む
        self._load_progress()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同期コンテキストマネージャーの終了"""
        if self.session:
            await self.session.close()
            
    def _load_progress(self):
        """前回の進捗を読み込む"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_urls = set(data.get('processed_urls', []))
                    logger.info(f"前回の進捗を読み込みました: {len(self.processed_urls)}件処理済み")
            except Exception as e:
                logger.error(f"進捗読み込みエラー: {e}")
                
        if self.results_file.exists():
            try:
                with open(self.results_file, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                    logger.info(f"前回の結果を読み込みました: {len(self.results)}件")
            except Exception as e:
                logger.error(f"結果読み込みエラー: {e}")
    
    def _save_progress(self):
        """進捗を保存"""
        try:
            self.progress_file.parent.mkdir(exist_ok=True)
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_urls': list(self.processed_urls),
                    'timestamp': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
                
            with open(self.results_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"進捗保存エラー: {e}")
    
    def get_area_urls(self) -> Dict[str, str]:
        """地域別URLを取得"""
        return {
            '東京都': 'https://tabelog.com/tokyo/',
            '大阪府': 'https://tabelog.com/osaka/',
            '神奈川県': 'https://tabelog.com/kanagawa/',
            '愛知県': 'https://tabelog.com/aichi/',
            '福岡県': 'https://tabelog.com/fukuoka/',
            '北海道': 'https://tabelog.com/hokkaido/',
            '京都府': 'https://tabelog.com/kyoto/',
            '兵庫県': 'https://tabelog.com/hyogo/',
            '埼玉県': 'https://tabelog.com/saitama/',
            '千葉県': 'https://tabelog.com/chiba/'
        }
    
    async def fetch_page(self, url: str) -> Optional[str]:
        """ページを非同期で取得"""
        async with self.semaphore:
            try:
                # ランダムな遅延
                delay = random.uniform(*self.delay_range)
                await asyncio.sleep(delay)
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        logger.warning(f"HTTPエラー {response.status}: {url}")
                        return None
                        
            except asyncio.TimeoutError:
                logger.error(f"タイムアウト: {url}")
                return None
            except Exception as e:
                logger.error(f"取得エラー {url}: {e}")
                return None
    
    async def scrape_restaurant_list_async(self, area_url: str, max_pages: int = 5) -> List[str]:
        """
        レストランリストページから個別店舗URLを非同期で取得
        """
        restaurant_urls = []
        tasks = []
        
        # ページURLを生成
        page_urls = [area_url]
        for page in range(2, max_pages + 1):
            page_urls.append(f"{area_url}rstLst/{page}/")
        
        # 非同期でページを取得
        for page_url in page_urls:
            task = self.fetch_page(page_url)
            tasks.append(task)
        
        pages = await asyncio.gather(*tasks)
        
        # 各ページから店舗URLを抽出
        for i, html in enumerate(pages):
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                links = self._extract_restaurant_links(soup)
                
                for href in links:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in restaurant_urls:
                        restaurant_urls.append(full_url)
                
                logger.info(f"ページ {i+1}: {len(links)}件の店舗URL取得")
        
        logger.info(f"合計 {len(restaurant_urls)} 件の店舗URL取得")
        return restaurant_urls
    
    def _extract_restaurant_links(self, soup: BeautifulSoup) -> List[str]:
        """ページから店舗リンクを抽出"""
        links = []
        
        # 複数のセレクタパターンを試す
        selectors = [
            'a.list-rst__rst-name-target',
            'a[href*="/A"][href*="/A"][href*="/"]',
            '.list-rst__rst-name a',
            '.rst-name a'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                links = [elem.get('href') for elem in elements if elem.get('href')]
                break
        
        # hrefパターンでも検索
        if not links:
            all_links = soup.find_all('a', href=True)
            links = [link.get('href') for link in all_links 
                    if re.match(r'/[^/]+/A\d+/A\d+/\d+/', link.get('href', ''))]
        
        return links
    
    async def scrape_restaurant_detail_async(self, restaurant_url: str) -> Optional[Dict]:
        """個別店舗ページから詳細情報を非同期で取得"""
        
        # 既に処理済みの場合はスキップ
        if restaurant_url in self.processed_urls:
            return None
            
        try:
            # メインページと地図ページを同時に取得
            main_task = self.fetch_page(restaurant_url)
            map_url = restaurant_url.rstrip('/') + '/dtlmap/'
            map_task = self.fetch_page(map_url)
            
            main_html, map_html = await asyncio.gather(main_task, map_task)
            
            if not main_html:
                return None
            
            soup = BeautifulSoup(main_html, 'html.parser')
            
            # 店名取得
            shop_name = self._extract_shop_name(soup)
            if not shop_name:
                logger.warning(f"店名が取得できませんでした: {restaurant_url}")
                return None
            
            # 電話番号と住所を地図ページから取得
            phone = ""
            address = ""
            if map_html:
                map_soup = BeautifulSoup(map_html, 'html.parser')
                phone, address = self._extract_contact_info(map_soup)
            
            # その他の情報
            genre = self._extract_info(soup, ['.rst-info__category', '.genre'])
            station = self._extract_info(soup, ['.rst-info__station', '.station'])
            open_time = self._extract_info(soup, ['.rst-info__open-hours', '.open-hours'])
            
            restaurant_info = {
                'shop_name': shop_name,
                'phone': phone,
                'address': address,
                'genre': genre,
                'station': station,
                'open_time': open_time,
                'url': restaurant_url,
                'source': '食べログ',
                'scraped_at': datetime.now().isoformat()
            }
            
            # 処理済みとして記録
            self.processed_urls.add(restaurant_url)
            self.results.append(restaurant_info)
            
            # 定期的に進捗を保存（10件ごと）
            if len(self.results) % 10 == 0:
                self._save_progress()
                logger.info(f"進捗保存: {len(self.results)}件完了")
            
            logger.info(f"✅ 店舗情報取得成功 [{len(self.results)}件目]: {shop_name}")
            return restaurant_info
            
        except Exception as e:
            logger.error(f"店舗詳細取得エラー {restaurant_url}: {e}")
            return None
    
    def _extract_shop_name(self, soup: BeautifulSoup) -> str:
        """店名を抽出"""
        selectors = ['h1.display-name', '.rst-name', 'h1', '.shop-name']
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if ' - ' in text:
                    text = text.split(' - ')[0]
                if '(' in text:
                    text = text.split('(')[0]
                return text.strip()
        
        # タイトルタグから取得
        title = soup.select_one('title')
        if title:
            text = title.get_text(strip=True)
            if ' - ' in text:
                return text.split(' - ')[0].strip()
        
        return ""
    
    def _extract_contact_info(self, soup: BeautifulSoup) -> tuple:
        """電話番号と住所を抽出"""
        phone = ""
        address = ""
        
        # 電話番号
        phone_patterns = [
            r'03-\d{4}-\d{4}',
            r'0\d{2,3}-\d{3,4}-\d{4}',
            r'0\d{9,10}'
        ]
        
        text = soup.get_text()
        for pattern in phone_patterns:
            match = re.search(pattern, text)
            if match:
                phone = match.group()
                break
        
        # 住所
        address_selectors = ['.rst-info__address', '.address', 'p:contains("住所")']
        for selector in address_selectors:
            elem = soup.select_one(selector)
            if elem:
                address = elem.get_text(strip=True)
                address = re.sub(r'^住所[：:]', '', address).strip()
                break
        
        return phone, address
    
    def _extract_info(self, soup: BeautifulSoup, selectors: List[str]) -> str:
        """汎用情報抽出"""
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                return elem.get_text(strip=True)
        return ""
    
    async def scrape_restaurants_batch(self, areas: List[str], max_per_area: int = 100) -> List[Dict]:
        """
        複数地域のレストラン情報をバッチで取得
        """
        area_urls = self.get_area_urls()
        all_restaurants = []
        
        for area in areas:
            if area not in area_urls:
                logger.warning(f"未対応の地域: {area}")
                continue
            
            logger.info(f"🍽️ {area}のデータ取得開始")
            area_url = area_urls[area]
            
            # 必要なページ数を計算（1ページ20件として）
            max_pages = (max_per_area + 19) // 20
            
            # 店舗URLリストを取得
            restaurant_urls = await self.scrape_restaurant_list_async(area_url, max_pages)
            
            # 指定件数に制限
            restaurant_urls = restaurant_urls[:max_per_area]
            
            # バッチで詳細情報を取得
            batch_size = 20  # 20件ずつ処理
            for i in range(0, len(restaurant_urls), batch_size):
                batch_urls = restaurant_urls[i:i + batch_size]
                
                tasks = []
                for url in batch_urls:
                    if url not in self.processed_urls:
                        task = self.scrape_restaurant_detail_async(url)
                        tasks.append(task)
                
                if tasks:
                    logger.info(f"バッチ処理中: {i+1}-{min(i+batch_size, len(restaurant_urls))}/{len(restaurant_urls)}")
                    batch_results = await asyncio.gather(*tasks)
                    
                    # 有効な結果のみ追加
                    valid_results = [r for r in batch_results if r is not None]
                    all_restaurants.extend(valid_results)
                    
                    # 進捗を保存
                    self._save_progress()
        
        # 最終保存
        self._save_progress()
        
        return all_restaurants