#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
食べログスクレイパー ウルトラ版 V3
- 50,000件の大量収集対応
- メモリ効率の最適化
- エラーからの自動復旧
- 地域とジャンルの組み合わせで収集範囲を拡大
"""

import asyncio
import aiohttp
import json
import re
import random
import time
import os
import sys
import gc
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from bs4 import BeautifulSoup
from pathlib import Path
import logging
from aiohttp import ClientTimeout, TCPConnector

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tabelog_ultra_v3.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TabelogUltraScraperV3:
    """ウルトラ高速・大量収集対応食べログスクレイパー"""
    
    def __init__(self, max_concurrent: int = 100):
        """初期化"""
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.delay_range = (0.5, 1.5)  # より短い遅延
        
        # 進捗管理
        self.processed_urls: Set[str] = set()
        self.results: List[Dict] = []
        self.errors_count = 0
        self.consecutive_errors = 0
        
        # ファイルパス
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.progress_file = self.cache_dir / "ultra_progress_v3.json"
        self.results_file = self.cache_dir / "ultra_results_v3.json"
        
        # バッチ処理設定
        self.batch_size = 50  # バッチサイズを増加
        self.save_interval = 100  # 保存間隔
        self.memory_limit_mb = 500  # メモリ使用量の上限
        
        # 地域とジャンルの組み合わせ
        self.genres = [
            'japanese', 'italian', 'french', 'chinese', 'korean',
            'thai', 'indian', 'spanish', 'mexican', 'vietnamese',
            'american', 'british', 'german', 'russian', 'turkish',
            'greek', 'brazilian', 'cafe', 'sweets', 'bar',
            'izakaya', 'ramen', 'sushi', 'yakiniku', 'kaiseki',
            'tempura', 'tonkatsu', 'udon', 'soba', 'curry',
            'hamburger', 'steak', 'seafood', 'vegetarian', 'vegan',
            'buffet', 'dining-bar', 'wine-bar', 'beer', 'sake'
        ]
        
        # 東京の詳細エリア
        self.tokyo_areas = {
            'shinjuku': 'A1304',
            'shibuya': 'A1303', 
            'minato': 'A1301',
            'chiyoda': 'A1302',
            'chuo': 'A1313',
            'toshima': 'A1305',
            'taito': 'A1311',
            'sumida': 'A1312',
            'koto': 'A1313',
            'shinagawa': 'A1314',
            'meguro': 'A1317',
            'ota': 'A1315',
            'setagaya': 'A1318',
            'nakano': 'A1319',
            'suginami': 'A1320',
            'kita': 'A1323',
            'arakawa': 'A1324',
            'itabashi': 'A1322',
            'nerima': 'A1321',
            'adachi': 'A1324',
            'katsushika': 'A1323',
            'edogawa': 'A1313'
        }
        
    async def __aenter__(self):
        """非同期コンテキストマネージャ入口"""
        timeout = ClientTimeout(total=30, connect=10, sock_read=10)
        connector = TCPConnector(limit=self.max_concurrent, force_close=True)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                'User-Agent': self._get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        # 前回の進捗を読み込む
        self.load_progress()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同期コンテキストマネージャ出口"""
        if self.session:
            await self.session.close()
    
    def _get_random_user_agent(self) -> str:
        """ランダムなUser-Agentを取得"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        return random.choice(user_agents)
    
    def load_progress(self):
        """前回の進捗を読み込む"""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_urls = set(data.get('processed_urls', []))
                    logger.info(f"前回の進捗を読み込みました: {len(self.processed_urls)}件処理済み")
            
            if self.results_file.exists():
                with open(self.results_file, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                    logger.info(f"前回の結果を読み込みました: {len(self.results)}件")
        except Exception as e:
            logger.error(f"進捗読み込みエラー: {e}")
    
    def save_progress(self):
        """進捗を保存（メモリ効率化）"""
        try:
            # 進捗データのみ保存（URLリストを圧縮）
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_urls': list(self.processed_urls)[-10000:],  # 最新10000件のみ保持
                    'total_processed': len(self.processed_urls),
                    'timestamp': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
            
            # 結果は追記モードで保存（メモリ効率化）
            if len(self.results) > 1000:
                # 1000件ごとに別ファイルに保存
                chunk_num = len(self.results) // 1000
                chunk_file = self.cache_dir / f"ultra_results_v3_chunk_{chunk_num}.json"
                with open(chunk_file, 'w', encoding='utf-8') as f:
                    json.dump(self.results[-1000:], f, ensure_ascii=False, indent=2)
                # メモリから古いデータを削除
                self.results = self.results[-100:]
                gc.collect()  # ガベージコレクション実行
            else:
                with open(self.results_file, 'w', encoding='utf-8') as f:
                    json.dump(self.results, f, ensure_ascii=False, indent=2)
                    
        except Exception as e:
            logger.error(f"進捗保存エラー: {e}")
    
    async def fetch_page(self, url: str) -> Optional[str]:
        """ページを非同期で取得（エラー耐性強化）"""
        async with self.semaphore:
            for attempt in range(3):  # 3回までリトライ
                try:
                    # ランダムな遅延
                    delay = random.uniform(*self.delay_range)
                    await asyncio.sleep(delay)
                    
                    # セッションヘッダーを更新
                    self.session.headers['User-Agent'] = self._get_random_user_agent()
                    
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            self.consecutive_errors = 0
                            return await response.text()
                        elif response.status == 404:
                            return None
                        elif response.status == 429:  # Rate limit
                            logger.warning(f"レート制限検出: {url}")
                            await asyncio.sleep(30)  # 30秒待機
                            continue
                        else:
                            logger.warning(f"HTTPエラー {response.status}: {url}")
                            
                except asyncio.TimeoutError:
                    logger.warning(f"タイムアウト (試行 {attempt+1}/3): {url}")
                except Exception as e:
                    logger.error(f"取得エラー (試行 {attempt+1}/3) {url}: {e}")
                
                if attempt < 2:
                    await asyncio.sleep(5 * (attempt + 1))  # リトライ前に待機
            
            self.errors_count += 1
            self.consecutive_errors += 1
            
            # 連続エラーが多い場合は長めに待機
            if self.consecutive_errors > 10:
                logger.warning(f"連続エラー多数。60秒待機します...")
                await asyncio.sleep(60)
                self.consecutive_errors = 0
            
            return None
    
    def build_search_urls(self) -> List[str]:
        """検索URLを構築（ジャンルとエリアの組み合わせ）"""
        urls = []
        base_url = "https://tabelog.com"
        
        # 東京の各エリアとジャンルの組み合わせ
        for area_name, area_code in self.tokyo_areas.items():
            for genre in self.genres:
                # ジャンル別URL
                genre_url = f"{base_url}/tokyo/{area_code}/rstLst/{genre}/"
                urls.append(genre_url)
                
                # 価格帯別も追加
                for price in ['1', '2', '3', '4', '5']:  # 価格帯
                    price_url = f"{base_url}/tokyo/{area_code}/rstLst/{genre}/?price={price}"
                    urls.append(price_url)
        
        # 通常のエリア別URLも追加
        for area_code in self.tokyo_areas.values():
            for page in range(1, 61):  # 各エリア60ページまで
                area_url = f"{base_url}/tokyo/{area_code}/rstLst/{page}/"
                urls.append(area_url)
        
        # URLをシャッフルして偏りを防ぐ
        random.shuffle(urls)
        return urls
    
    async def scrape_list_page(self, url: str) -> List[str]:
        """リストページから店舗URLを抽出"""
        html = await self.fetch_page(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        restaurant_urls = []
        
        # 複数のセレクタで店舗URLを探す
        selectors = [
            'a.list-rst__rst-name-target',
            'h3.list-rst__rst-name a',
            'div.list-rst__wrap a[href*="/A"]',
            'a[class*="rst-name"]'
        ]
        
        for selector in selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href', '')
                if href and '/A' in href and href not in self.processed_urls:
                    full_url = href if href.startswith('http') else f"https://tabelog.com{href}"
                    restaurant_urls.append(full_url)
        
        # 重複を除去
        return list(set(restaurant_urls))
    
    async def scrape_restaurant_detail(self, url: str) -> Optional[Dict]:
        """店舗詳細を取得"""
        if url in self.processed_urls:
            return None
        
        html = await self.fetch_page(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # 店名取得
        shop_name = None
        name_selectors = [
            'h2.display-name span',
            'h1.rstinfo-name',
            'h1[class*="name"]',
            'div.rstinfo-table__name'
        ]
        for selector in name_selectors:
            elem = soup.select_one(selector)
            if elem:
                shop_name = elem.get_text(strip=True)
                break
        
        if not shop_name:
            return None
        
        # 基本情報を抽出
        info = {
            'shop_name': shop_name,
            'url': url,
            'phone': self._extract_info(soup, ['電話番号', 'TEL', 'Tel']),
            'address': self._extract_info(soup, ['住所', '所在地']),
            'genre': self._extract_info(soup, ['ジャンル', '料理']),
            'station': self._extract_info(soup, ['最寄り駅', '最寄駅', 'アクセス']),
            'open_time': self._extract_info(soup, ['営業時間', '営業', '定休日']),
            'source': '食べログ',
            'scraped_at': datetime.now().isoformat()
        }
        
        self.processed_urls.add(url)
        return info
    
    def _extract_info(self, soup: BeautifulSoup, keywords: List[str]) -> str:
        """情報を抽出する汎用メソッド"""
        for keyword in keywords:
            # テーブルから検索
            for th in soup.find_all(['th', 'td'], string=re.compile(keyword)):
                next_elem = th.find_next_sibling()
                if next_elem:
                    text = next_elem.get_text(strip=True)
                    if text:
                        return text
            
            # その他の要素から検索
            for elem in soup.find_all(string=re.compile(keyword)):
                parent = elem.parent
                if parent:
                    text = parent.get_text(strip=True).replace(keyword, '').strip()
                    if text and len(text) > 2:
                        return text
        
        return ""
    
    async def scrape_restaurants_ultra(self, target_count: int = 50000):
        """超大量の店舗情報を収集"""
        logger.info(f"🚀 ウルトラモード起動: 目標 {target_count}件")
        
        # 検索URLを生成
        search_urls = self.build_search_urls()
        logger.info(f"検索URL数: {len(search_urls)}")
        
        # 既に目標数に達している場合
        current_count = len(self.results)
        if current_count >= target_count:
            logger.info(f"既に目標数 {target_count}件を達成しています（現在: {current_count}件）")
            return self.results
        
        # バッチ処理でURLを収集
        all_restaurant_urls = []
        for i in range(0, len(search_urls), 50):
            batch_urls = search_urls[i:i+50]
            tasks = [self.scrape_list_page(url) for url in batch_urls]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, list):
                    all_restaurant_urls.extend(result)
            
            # 重複を除去
            all_restaurant_urls = list(set(all_restaurant_urls) - self.processed_urls)
            
            logger.info(f"収集済みURL数: {len(all_restaurant_urls)}")
            
            # 十分なURLが集まったら詳細取得に移る
            if len(all_restaurant_urls) >= target_count - current_count:
                break
            
            # メモリチェック
            if i % 200 == 0:
                gc.collect()
        
        # 詳細情報を取得
        logger.info(f"詳細情報取得開始: {len(all_restaurant_urls)}件")
        
        for i in range(0, len(all_restaurant_urls), self.batch_size):
            if len(self.results) >= target_count:
                break
            
            batch_urls = all_restaurant_urls[i:i+self.batch_size]
            tasks = [self.scrape_restaurant_detail(url) for url in batch_urls]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, dict) and result:
                    self.results.append(result)
                    current_count = len(self.results)
                    
                    if current_count % 10 == 0:
                        logger.info(f"✅ 収集済み: {current_count}件")
                    
                    if current_count % self.save_interval == 0:
                        self.save_progress()
                        logger.info(f"💾 進捗保存: {current_count}件")
            
            # レート制限対策
            if i % 200 == 0 and i > 0:
                logger.info("⏸️ レート制限対策: 10秒待機")
                await asyncio.sleep(10)
        
        # 最終保存
        self.save_progress()
        logger.info(f"🎉 収集完了: 合計 {len(self.results)}件")
        
        return self.results


async def main():
    """メイン処理"""
    target_count = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
    
    async with TabelogUltraScraperV3(max_concurrent=100) as scraper:
        results = await scraper.scrape_restaurants_ultra(target_count)
        
        # 全結果を統合してExcelに出力
        from restaurant_data_integrator import RestaurantDataIntegrator
        integrator = RestaurantDataIntegrator()
        
        # チャンクファイルも読み込む
        all_results = []
        cache_dir = Path("cache")
        
        # チャンクファイルを読み込み
        for chunk_file in sorted(cache_dir.glob("ultra_results_v3_chunk_*.json")):
            with open(chunk_file, 'r', encoding='utf-8') as f:
                chunk_data = json.load(f)
                all_results.extend(chunk_data)
        
        # 最新の結果も追加
        all_results.extend(results)
        
        # 重複除去
        unique_results = []
        seen_urls = set()
        for r in all_results:
            if r['url'] not in seen_urls:
                unique_results.append(r)
                seen_urls.add(r['url'])
        
        logger.info(f"統合結果: {len(unique_results)}件（重複除去後）")
        
        # Excel出力
        integrator.add_restaurants(unique_results)
        integrator.remove_duplicates()
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'output/東京都_飲食店リスト_ULTRA_{len(unique_results)}件_{timestamp}.xlsx'
        integrator.create_excel_report(output_file)
        
        logger.info(f"✅ Excelファイル作成完了: {output_file}")
        
        # 統計情報
        with_phone = sum(1 for r in unique_results if r.get('phone'))
        with_address = sum(1 for r in unique_results if r.get('address'))
        logger.info(f"📊 データ品質:")
        logger.info(f"  - 電話番号あり: {with_phone}件 ({with_phone/len(unique_results)*100:.1f}%)")
        logger.info(f"  - 住所あり: {with_address}件 ({with_address/len(unique_results)*100:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())