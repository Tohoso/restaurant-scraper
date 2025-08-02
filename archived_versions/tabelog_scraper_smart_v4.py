#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
食べログスクレイパー スマート版 V4
- レート制限に適応的に対応
- 段階的なスクレイピング戦略
- 実績のあるv2版をベースに大量収集対応
"""

import asyncio
import aiohttp
import json
import re
import random
import time
import os
import sys
from datetime import datetime, timedelta
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
        logging.FileHandler('tabelog_smart_v4.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TabelogSmartScraperV4:
    """スマート版食べログスクレイパー - レート制限適応型"""
    
    def __init__(self, max_concurrent: int = 20):
        """初期化"""
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        
        # 適応的な遅延管理
        self.min_delay = 1.0
        self.current_delay = 2.0
        self.max_delay = 10.0
        self.rate_limit_count = 0
        self.success_count = 0
        
        # 進捗管理
        self.processed_urls: Set[str] = set()
        self.results: List[Dict] = []
        self.errors_count = 0
        
        # ファイルパス
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.progress_file = self.cache_dir / "smart_progress_v4.json"
        self.results_dir = Path("output") / "smart_results_v4"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # 東京のエリア（シンプル化）
        self.tokyo_main_areas = {
            '新宿・代々木・大久保': 'https://tabelog.com/tokyo/A1304/',
            '渋谷': 'https://tabelog.com/tokyo/A1303/',
            '恵比寿・目黒・品川': 'https://tabelog.com/tokyo/A1316/',
            '銀座・新橋・有楽町': 'https://tabelog.com/tokyo/A1301/',
            '東京・丸の内・日本橋': 'https://tabelog.com/tokyo/A1302/',
            '上野・浅草・日暮里': 'https://tabelog.com/tokyo/A1311/',
            '池袋～高田馬場・早稲田': 'https://tabelog.com/tokyo/A1305/',
            '原宿・表参道・青山': 'https://tabelog.com/tokyo/A1306/',
            '六本木・麻布・広尾': 'https://tabelog.com/tokyo/A1307/',
            '赤坂・永田町・溜池': 'https://tabelog.com/tokyo/A1308/',
            '四ツ谷・市ヶ谷・飯田橋': 'https://tabelog.com/tokyo/A1309/',
            '秋葉原・神田・水道橋': 'https://tabelog.com/tokyo/A1310/',
            '錦糸町・押上・新小岩': 'https://tabelog.com/tokyo/A1312/',
            '葛飾・江戸川・江東': 'https://tabelog.com/tokyo/A1313/',
            '蒲田・大森・羽田周辺': 'https://tabelog.com/tokyo/A1315/',
            '自由が丘・中目黒・学芸大学': 'https://tabelog.com/tokyo/A1317/',
            '下北沢・明大前・成城学園前': 'https://tabelog.com/tokyo/A1318/',
            '中野・吉祥寺・三鷹': 'https://tabelog.com/tokyo/A1319/',
            '西荻窪・荻窪・阿佐ヶ谷': 'https://tabelog.com/tokyo/A1320/',
            '板橋・東武練馬・下赤塚': 'https://tabelog.com/tokyo/A1322/',
            '大塚・巣鴨・駒込・赤羽': 'https://tabelog.com/tokyo/A1323/',
            '千住・綾瀬・葛飾': 'https://tabelog.com/tokyo/A1324/'
        }
        
    async def __aenter__(self):
        """非同期コンテキストマネージャ入口"""
        timeout = ClientTimeout(total=30, connect=10, sock_read=10)
        connector = TCPConnector(limit=self.max_concurrent, force_close=True)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self._get_headers()
        )
        
        # 前回の進捗を読み込む
        self.load_progress()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同期コンテキストマネージャ出口"""
        if self.session:
            await self.session.close()
    
    def _get_headers(self) -> Dict[str, str]:
        """リクエストヘッダーを取得"""
        return {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
    
    def _get_random_user_agent(self) -> str:
        """ランダムなUser-Agentを取得"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',
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
            
            # 既存の結果ファイルをカウント
            existing_results = list(self.results_dir.glob("results_*.json"))
            if existing_results:
                total_count = 0
                for result_file in existing_results:
                    with open(result_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        total_count += len(data)
                logger.info(f"既存の結果ファイル: {len(existing_results)}個、合計{total_count}件")
                
        except Exception as e:
            logger.error(f"進捗読み込みエラー: {e}")
    
    def save_progress(self):
        """進捗を保存"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_urls': list(self.processed_urls)[-50000:],  # 最新50000件のみ保持
                    'total_processed': len(self.processed_urls),
                    'timestamp': datetime.now().isoformat(),
                    'current_delay': self.current_delay,
                    'rate_limit_count': self.rate_limit_count
                }, f, ensure_ascii=False, indent=2)
            
            # 結果をチャンクファイルに保存（1000件ごと）
            if self.results and len(self.results) >= 1000:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                chunk_file = self.results_dir / f"results_{timestamp}_{len(self.results)}.json"
                with open(chunk_file, 'w', encoding='utf-8') as f:
                    json.dump(self.results, f, ensure_ascii=False, indent=2)
                logger.info(f"💾 結果を保存しました: {chunk_file}")
                self.results = []  # メモリクリア
                    
        except Exception as e:
            logger.error(f"進捗保存エラー: {e}")
    
    async def fetch_with_adaptive_delay(self, url: str) -> Optional[str]:
        """適応的な遅延でページを取得"""
        async with self.semaphore:
            # 適応的な遅延
            await asyncio.sleep(self.current_delay + random.uniform(0, 1))
            
            try:
                # ヘッダーを更新
                self.session.headers['User-Agent'] = self._get_random_user_agent()
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        # 成功したら遅延を減らす
                        self.success_count += 1
                        if self.success_count > 10:
                            self.current_delay = max(self.min_delay, self.current_delay * 0.9)
                            self.success_count = 0
                        return await response.text()
                    
                    elif response.status == 429:
                        # レート制限検出
                        self.rate_limit_count += 1
                        self.current_delay = min(self.max_delay, self.current_delay * 1.5)
                        logger.warning(f"レート制限検出 (遅延を{self.current_delay:.1f}秒に調整)")
                        
                        # 長時間待機
                        wait_time = 30 + (self.rate_limit_count * 10)
                        logger.info(f"⏸️ {wait_time}秒待機します...")
                        await asyncio.sleep(wait_time)
                        return None
                    
                    elif response.status == 404:
                        return None
                    
                    else:
                        logger.warning(f"HTTPエラー {response.status}: {url}")
                        return None
                        
            except asyncio.TimeoutError:
                logger.warning(f"タイムアウト: {url}")
                self.errors_count += 1
                return None
            except Exception as e:
                logger.error(f"取得エラー {url}: {e}")
                self.errors_count += 1
                return None
    
    async def scrape_list_page(self, url: str) -> List[str]:
        """リストページから店舗URLを抽出"""
        html = await self.fetch_with_adaptive_delay(url)
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
        
        return list(set(restaurant_urls))
    
    async def scrape_restaurant_detail(self, url: str) -> Optional[Dict]:
        """店舗詳細を取得（v2版のロジックを使用）"""
        if url in self.processed_urls:
            return None
        
        html = await self.fetch_with_adaptive_delay(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # 店名取得
        shop_name = self._extract_shop_name(soup)
        if not shop_name:
            return None
        
        # 基本情報を取得
        info = {
            'shop_name': shop_name,
            'url': url,
            'phone': self._extract_phone(soup),
            'address': self._extract_address(soup),
            'genre': self._extract_genre(soup),
            'station': self._extract_station(soup),
            'open_time': self._extract_open_time(soup),
            'source': '食べログ',
            'scraped_at': datetime.now().isoformat()
        }
        
        self.processed_urls.add(url)
        return info
    
    def _extract_shop_name(self, soup: BeautifulSoup) -> str:
        """店名を抽出"""
        selectors = [
            'h2.display-name span',
            'h2.display-name',
            'h1.display-name span',
            'h1.display-name',
            'div.rdheader-info__name span',
            'div.rdheader-info__name',
            '.rstinfo-table__name'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                text = re.sub(r'\s*\([^)]+\)\s*$', '', text)
                text = re.sub(r'\s+', ' ', text)
                if text and text != '食べログ':
                    return text.strip()
        
        return ""
    
    def _extract_address(self, soup: BeautifulSoup) -> str:
        """住所を抽出"""
        selectors = [
            'p.rstinfo-table__address',
            'span.rstinfo-table__address',
            'div.rstinfo-table__address',
            'p.rstdtl-side-address__text'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                address = elem.get_text(strip=True)
                address = re.sub(r'〒\d{3}-\d{4}\s*', '', address)
                if address:
                    return address
        
        # テーブルから検索
        for th in soup.find_all(['th', 'td'], string=re.compile('住所')):
            next_elem = th.find_next_sibling()
            if next_elem:
                address = next_elem.get_text(strip=True)
                if address:
                    return address
        
        return ""
    
    def _extract_phone(self, soup: BeautifulSoup) -> str:
        """電話番号を抽出"""
        phone_patterns = [
            r'03-\d{4}-\d{4}',
            r'0\d{2,3}-\d{3,4}-\d{4}',
            r'0\d{9,10}',
            r'\d{2,4}-\d{2,4}-\d{4}'
        ]
        
        selectors = [
            'span.rstinfo-table__tel-num',
            'p.rstinfo-table__tel',
            'div.rstinfo-table__tel'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                for pattern in phone_patterns:
                    match = re.search(pattern, text)
                    if match:
                        return match.group()
        
        # テーブルから検索
        for th in soup.find_all(['th', 'td'], string=re.compile('電話番号')):
            next_elem = th.find_next_sibling()
            if next_elem:
                text = next_elem.get_text(strip=True)
                for pattern in phone_patterns:
                    match = re.search(pattern, text)
                    if match:
                        return match.group()
        
        return ""
    
    def _extract_genre(self, soup: BeautifulSoup) -> str:
        """ジャンルを抽出"""
        # ジャンル専用セレクタを優先
        priority_selectors = [
            '.rstinfo-table__genre',
            'span[property="v:category"]'
        ]
        
        for selector in priority_selectors:
            elem = soup.select_one(selector)
            if elem:
                genre = elem.get_text(strip=True)
                if genre and genre != '飲食店':
                    return genre
        
        # テーブルから検索
        for th in soup.find_all(['th', 'td'], string=re.compile('ジャンル')):
            next_elem = th.find_next_sibling()
            if next_elem:
                genre = next_elem.get_text(strip=True)
                if genre:
                    return genre
        
        # 一般的なセレクタ（駅名を除外）
        for elem in soup.select('span.linktree__parent-target-text'):
            text = elem.get_text(strip=True)
            if text and '駅' not in text and not text.endswith('線'):
                genre_keywords = ['料理', '焼', '鍋', '寿司', '鮨', 'そば', 'うどん', 'ラーメン',
                                'カレー', 'イタリアン', 'フレンチ', '中華', '和食', '洋食',
                                'カフェ', 'バー', '居酒屋', '食堂', 'レストラン']
                if any(keyword in text for keyword in genre_keywords):
                    return text
        
        return ""
    
    def _extract_station(self, soup: BeautifulSoup) -> str:
        """最寄り駅を抽出"""
        selectors = [
            'span.linktree__parent-target-text:contains("駅")',
            '.rstinfo-table__access',
            'dl.rdheader-subinfo__item--station'
        ]
        
        for selector in selectors:
            if ':contains' in selector:
                for elem in soup.select('span.linktree__parent-target-text'):
                    text = elem.get_text(strip=True)
                    if '駅' in text:
                        return text
            else:
                elem = soup.select_one(selector)
                if elem:
                    station = elem.get_text(strip=True)
                    station = station.split('、')[0].split('から')[0]
                    if station:
                        return station
        
        # テーブルから検索
        for keyword in ['交通手段', '最寄り駅', '最寄駅']:
            for th in soup.find_all(['th', 'td'], string=re.compile(keyword)):
                next_elem = th.find_next_sibling()
                if next_elem:
                    station = next_elem.get_text(strip=True)
                    station = station.split('、')[0].split('から')[0]
                    if station:
                        return station
        
        return ""
    
    def _extract_open_time(self, soup: BeautifulSoup) -> str:
        """営業時間を抽出"""
        selectors = [
            'p.rstinfo-table__open-hours',
            '.rstinfo-table__open-hours',
            'dl.rdheader-subinfo__item--open-hours'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                hours = elem.get_text(strip=True)
                hours = re.sub(r'\s+', ' ', hours)
                if hours:
                    return hours
        
        # テーブルから検索
        for th in soup.find_all(['th', 'td'], string=re.compile('営業時間')):
            next_elem = th.find_next_sibling()
            if next_elem:
                hours = next_elem.get_text(strip=True)
                hours = re.sub(r'\s+', ' ', hours)
                if hours:
                    return hours
        
        return ""
    
    async def scrape_restaurants_smart(self, target_count: int = 50000):
        """スマートな大量収集"""
        logger.info(f"🎯 スマートモード起動: 目標 {target_count}件")
        logger.info(f"📊 初期遅延: {self.current_delay:.1f}秒")
        
        # 既存の結果数をカウント
        existing_count = 0
        for result_file in self.results_dir.glob("results_*.json"):
            with open(result_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                existing_count += len(data)
        
        if existing_count >= target_count:
            logger.info(f"既に目標数を達成しています: {existing_count}件")
            return
        
        remaining_count = target_count - existing_count
        logger.info(f"📈 追加収集必要数: {remaining_count}件")
        
        # エリアごとに段階的に収集
        all_restaurant_urls = []
        
        for area_name, area_url in self.tokyo_main_areas.items():
            if len(all_restaurant_urls) >= remaining_count * 1.2:  # 20%余分に収集
                break
            
            logger.info(f"🏙️ {area_name}のデータ収集開始")
            
            # 各エリアで最大60ページまで収集
            area_urls = []
            for page in range(1, 61):
                page_url = f"{area_url}rstLst/{page}/"
                area_urls.append(page_url)
            
            # バッチ処理でURLを収集
            for i in range(0, len(area_urls), 10):
                batch_urls = area_urls[i:i+10]
                tasks = [self.scrape_list_page(url) for url in batch_urls]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, list):
                        all_restaurant_urls.extend(result)
                
                logger.info(f"  収集URL数: {len(all_restaurant_urls)}")
                
                # レート制限が多い場合は次のエリアへ
                if self.rate_limit_count > 5:
                    logger.warning(f"レート制限が多いため、次のエリアへ移動します")
                    self.rate_limit_count = 0
                    await asyncio.sleep(60)  # エリア切り替え時は長めに待機
                    break
        
        # 重複を除去
        all_restaurant_urls = list(set(all_restaurant_urls) - self.processed_urls)
        logger.info(f"📋 収集したユニークURL数: {len(all_restaurant_urls)}")
        
        # 詳細情報を取得
        collected_count = 0
        for i in range(0, len(all_restaurant_urls), 10):
            if collected_count >= remaining_count:
                break
            
            batch_urls = all_restaurant_urls[i:i+10]
            tasks = [self.scrape_restaurant_detail(url) for url in batch_urls]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, dict) and result:
                    self.results.append(result)
                    collected_count += 1
                    
                    if collected_count % 100 == 0:
                        logger.info(f"✅ 収集済み: {collected_count}/{remaining_count}件 (遅延: {self.current_delay:.1f}秒)")
                    
                    if collected_count % 1000 == 0:
                        self.save_progress()
            
            # 定期的に長めの休憩
            if i % 100 == 0 and i > 0:
                logger.info(f"⏸️ 定期休憩: 30秒")
                await asyncio.sleep(30)
        
        # 最終保存
        if self.results:
            self.save_progress()
        
        # 統計情報
        total_collected = existing_count + collected_count
        logger.info(f"🎉 収集完了: 新規 {collected_count}件、合計 {total_collected}件")
        logger.info(f"📊 最終遅延: {self.current_delay:.1f}秒")
        logger.info(f"❌ エラー数: {self.errors_count}")


async def main():
    """メイン処理"""
    target_count = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
    
    async with TabelogSmartScraperV4(max_concurrent=20) as scraper:
        await scraper.scrape_restaurants_smart(target_count)
        
        # 全結果を統合してExcelに出力
        from restaurant_data_integrator import RestaurantDataIntegrator
        integrator = RestaurantDataIntegrator()
        
        # 全結果ファイルを読み込み
        all_results = []
        results_dir = Path("output") / "smart_results_v4"
        
        for result_file in sorted(results_dir.glob("results_*.json")):
            with open(result_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_results.extend(data)
        
        if not all_results:
            logger.warning("収集されたデータがありません")
            return
        
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
        output_file = f'output/東京都_飲食店リスト_SMART_{len(unique_results)}件_{timestamp}.xlsx'
        integrator.create_excel_report(output_file)
        
        logger.info(f"✅ Excelファイル作成完了: {output_file}")
        
        # データ品質統計
        with_phone = sum(1 for r in unique_results if r.get('phone'))
        with_address = sum(1 for r in unique_results if r.get('address'))
        with_genre = sum(1 for r in unique_results if r.get('genre'))
        with_station = sum(1 for r in unique_results if r.get('station'))
        
        logger.info(f"📊 データ品質:")
        logger.info(f"  - 電話番号あり: {with_phone}件 ({with_phone/len(unique_results)*100:.1f}%)")
        logger.info(f"  - 住所あり: {with_address}件 ({with_address/len(unique_results)*100:.1f}%)")
        logger.info(f"  - ジャンルあり: {with_genre}件 ({with_genre/len(unique_results)*100:.1f}%)")
        logger.info(f"  - 最寄り駅あり: {with_station}件 ({with_station/len(unique_results)*100:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())