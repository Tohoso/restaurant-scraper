#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
非同期版食べログスクレイピングクライアント V2
最新の食べログHTML構造に対応した高速スクレイパー
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

class TabelogScraperAsyncV2:
    """非同期版食べログスクレイパー V2"""
    
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
        self.progress_file = Path("cache/scraping_progress_v2.json")
        self.results_file = Path("cache/partial_results_v2.json")
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
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
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
            'a.js-restaurant-link',
            'h3.list-rst__rst-name a',
            'div.list-rst__rst-name a',
            'a[href*="/A"][href*="/A"][href*="/"]'
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
            # メインページを取得
            main_html = await self.fetch_page(restaurant_url)
            
            if not main_html:
                return None
            
            soup = BeautifulSoup(main_html, 'html.parser')
            
            # 店名取得（改良版）
            shop_name = self._extract_shop_name_v2(soup)
            if not shop_name:
                logger.warning(f"店名が取得できませんでした: {restaurant_url}")
                return None
            
            # 基本情報を取得（新しいセレクタ）
            address = self._extract_address_v2(soup)
            phone = self._extract_phone_v2(soup)
            genre = self._extract_genre_v2(soup)
            station = self._extract_station_v2(soup)
            open_time = self._extract_open_time_v2(soup)
            
            # 情報が不足している場合は地図ページも確認
            if not address or not phone:
                map_url = restaurant_url.rstrip('/') + '/dtlmap/'
                map_html = await self.fetch_page(map_url)
                if map_html:
                    map_soup = BeautifulSoup(map_html, 'html.parser')
                    if not phone:
                        phone = self._extract_phone_from_map(map_soup)
                    if not address:
                        address = self._extract_address_from_map(map_soup)
            
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
    
    def _extract_shop_name_v2(self, soup: BeautifulSoup) -> str:
        """店名を抽出（改良版）"""
        # 優先順位の高い順にセレクタを試す
        selectors = [
            'h2.display-name span',
            'h2.display-name',
            'h1.display-name span',
            'h1.display-name',
            'div.rdheader-info__name span',
            'div.rdheader-info__name',
            '.rstinfo-table__name',
            'h1',
            'title'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                # クリーンアップ
                text = re.sub(r'\s*\([^)]+\)\s*$', '', text)  # 括弧内の読み仮名を削除
                text = re.sub(r'\s+', ' ', text)  # 余分な空白を削除
                if text and text != '食べログ':
                    return text.strip()
        
        return ""
    
    def _extract_address_v2(self, soup: BeautifulSoup) -> str:
        """住所を抽出（改良版）"""
        # 優先順位の高い順にセレクタを試す
        selectors = [
            'p.rstinfo-table__address',
            'span.rstinfo-table__address',
            'td:contains("住所") + td',
            'th:contains("住所") + td',
            'div.rstinfo-table__address',
            'p.rstdtl-side-address__text',
            '.rdheader-subinfo__item-text'
        ]
        
        for selector in selectors:
            if ':contains' in selector:
                # :contains セレクタの処理
                elements = soup.find_all(text=re.compile('住所'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            address = next_elem.get_text(strip=True)
                            if address:
                                return address
            else:
                elem = soup.select_one(selector)
                if elem:
                    address = elem.get_text(strip=True)
                    # 郵便番号や不要な文字を削除
                    address = re.sub(r'〒\d{3}-\d{4}\s*', '', address)
                    address = re.sub(r'^\s*住所\s*[:：]\s*', '', address)
                    if address:
                        return address
        
        return ""
    
    def _extract_phone_v2(self, soup: BeautifulSoup) -> str:
        """電話番号を抽出（改良版）"""
        # 電話番号のパターン
        phone_patterns = [
            r'03-\d{4}-\d{4}',
            r'0\d{2,3}-\d{3,4}-\d{4}',
            r'0\d{9,10}',
            r'\d{2,4}-\d{2,4}-\d{4}'
        ]
        
        # 優先順位の高い順にセレクタを試す
        selectors = [
            'span.rstinfo-table__tel-num',
            'p.rstinfo-table__tel',
            'td:contains("電話番号") + td',
            'th:contains("電話番号") + td',
            'div.rstinfo-table__tel',
            '.rdheader-subinfo__item--tel'
        ]
        
        for selector in selectors:
            if ':contains' in selector:
                # :contains セレクタの処理
                elements = soup.find_all(text=re.compile('電話番号'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            text = next_elem.get_text(strip=True)
                            for pattern in phone_patterns:
                                match = re.search(pattern, text)
                                if match:
                                    return match.group()
            else:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    for pattern in phone_patterns:
                        match = re.search(pattern, text)
                        if match:
                            return match.group()
        
        return ""
    
    def _extract_genre_v2(self, soup: BeautifulSoup) -> str:
        """ジャンルを抽出（改良版）"""
        # まず、明確にジャンルとラベルされているセレクタを優先
        priority_selectors = [
            'td:contains("ジャンル") + td',
            'th:contains("ジャンル") + td',
            '.rstinfo-table__genre',
            'span[property="v:category"]'
        ]
        
        for selector in priority_selectors:
            if ':contains' in selector:
                elements = soup.find_all(text=re.compile('ジャンル'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            genre = next_elem.get_text(strip=True)
                            if genre:
                                return genre
            else:
                elem = soup.select_one(selector)
                if elem:
                    genre = elem.get_text(strip=True)
                    if genre and genre != '飲食店':
                        return genre
        
        # 次に、より一般的なセレクタを試すが、駅名を除外
        general_selectors = [
            'span.linktree__parent-target-text',
            'div.rdheader-subinfo__item-text'
        ]
        
        for selector in general_selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text(strip=True)
                # 駅名でないことを確認（「駅」が含まれていない、かつ料理ジャンルの特徴がある）
                if text and '駅' not in text and not text.endswith('線'):
                    # 一般的な料理ジャンルのキーワードをチェック
                    genre_keywords = ['料理', '焼', '鍋', '寿司', '鮨', 'そば', 'うどん', 'ラーメン', 
                                    'カレー', 'イタリアン', 'フレンチ', '中華', '和食', '洋食', 
                                    'カフェ', 'バー', '居酒屋', '食堂', 'レストラン', '肉', '魚', 
                                    '野菜', '串', '天ぷら', '丼', '定食']
                    if any(keyword in text for keyword in genre_keywords) or len(text) < 20:
                        return text
        
        return ""
    
    def _extract_station_v2(self, soup: BeautifulSoup) -> str:
        """最寄り駅を抽出（改良版）"""
        selectors = [
            'span.linktree__parent-target-text:contains("駅")',
            'td:contains("交通手段") + td',
            'th:contains("交通手段") + td',
            'td:contains("最寄り駅") + td',
            'th:contains("最寄り駅") + td',
            '.rstinfo-table__access',
            'dl.rdheader-subinfo__item--station'
        ]
        
        for selector in selectors:
            if ':contains' in selector:
                if '交通手段' in selector or '最寄り駅' in selector:
                    pattern = '交通手段' if '交通手段' in selector else '最寄り駅'
                    elements = soup.find_all(text=re.compile(pattern))
                    for elem in elements:
                        parent = elem.parent
                        if parent and parent.name in ['td', 'th']:
                            next_elem = parent.find_next_sibling()
                            if next_elem:
                                station = next_elem.get_text(strip=True)
                                # 最初の駅名だけ抽出
                                station = station.split('、')[0].split('から')[0]
                                if station:
                                    return station
                else:
                    elem = soup.select_one(selector)
                    if elem:
                        station = elem.get_text(strip=True)
                        if '駅' in station:
                            return station
            else:
                elem = soup.select_one(selector)
                if elem:
                    station = elem.get_text(strip=True)
                    station = station.split('、')[0].split('から')[0]
                    if station:
                        return station
        
        return ""
    
    def _extract_open_time_v2(self, soup: BeautifulSoup) -> str:
        """営業時間を抽出（改良版）"""
        selectors = [
            'td:contains("営業時間") + td',
            'th:contains("営業時間") + td',
            'p.rstinfo-table__open-hours',
            '.rstinfo-table__open-hours',
            'dl.rdheader-subinfo__item--open-hours'
        ]
        
        for selector in selectors:
            if ':contains' in selector:
                elements = soup.find_all(text=re.compile('営業時間'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            hours = next_elem.get_text(strip=True)
                            # 改行を整理
                            hours = re.sub(r'\s+', ' ', hours)
                            if hours:
                                return hours
            else:
                elem = soup.select_one(selector)
                if elem:
                    hours = elem.get_text(strip=True)
                    hours = re.sub(r'\s+', ' ', hours)
                    if hours:
                        return hours
        
        return ""
    
    def _extract_phone_from_map(self, soup: BeautifulSoup) -> str:
        """地図ページから電話番号を抽出"""
        phone_patterns = [
            r'03-\d{4}-\d{4}',
            r'0\d{2,3}-\d{3,4}-\d{4}',
            r'0\d{9,10}'
        ]
        
        text = soup.get_text()
        for pattern in phone_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group()
        
        return ""
    
    def _extract_address_from_map(self, soup: BeautifulSoup) -> str:
        """地図ページから住所を抽出"""
        # scriptタグ内のJSONデータから住所を探す
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'address' in data:
                    if isinstance(data['address'], dict):
                        address_parts = []
                        for key in ['addressRegion', 'addressLocality', 'streetAddress']:
                            if key in data['address']:
                                address_parts.append(data['address'][key])
                        if address_parts:
                            return ''.join(address_parts)
                    elif isinstance(data['address'], str):
                        return data['address']
            except:
                pass
        
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