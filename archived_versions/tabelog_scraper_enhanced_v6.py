#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
食べログスクレイパー 拡張版 V6
- 席数、公式URL、口コミ数を含む詳細情報を取得
- 既存の1200件データを拡張
"""

import asyncio
import aiohttp
import json
import re
import random
import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from bs4 import BeautifulSoup
from pathlib import Path
import logging
from aiohttp import ClientTimeout, TCPConnector

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tabelog_enhanced_v6.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TabelogEnhancedScraperV6:
    """拡張版食べログスクレイパー"""
    
    def __init__(self, max_concurrent: int = 5):
        """初期化"""
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.delay_range = (3.0, 5.0)  # 適度な遅延
        
        # 進捗管理
        self.processed_urls: Set[str] = set()
        self.results: List[Dict] = []
        
        # ファイルパス
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.progress_file = self.cache_dir / "enhanced_progress_v6.json"
        self.results_file = self.cache_dir / "enhanced_results_v6.json"
        
    async def __aenter__(self):
        """非同期コンテキストマネージャ入口"""
        timeout = ClientTimeout(total=30, connect=10, sock_read=10)
        connector = TCPConnector(limit=self.max_concurrent, force_close=True)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self._get_headers()
        )
        
        # 進捗を読み込む
        self.load_progress()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同期コンテキストマネージャ出口"""
        if self.session:
            await self.session.close()
    
    def _get_headers(self) -> Dict[str, str]:
        """リクエストヘッダーを取得"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0'
        ]
        
        return {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def load_progress(self):
        """前回の進捗を読み込む"""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_urls = set(data.get('processed_urls', []))
                    logger.info(f"前回の進捗を読み込み: {len(self.processed_urls)}件処理済み")
            
            if self.results_file.exists():
                with open(self.results_file, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                    logger.info(f"前回の結果を読み込み: {len(self.results)}件")
                
        except Exception as e:
            logger.error(f"進捗読み込みエラー: {e}")
    
    def save_progress(self):
        """進捗を保存"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_urls': list(self.processed_urls),
                    'total_processed': len(self.processed_urls),
                    'timestamp': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
            
            with open(self.results_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
                    
        except Exception as e:
            logger.error(f"進捗保存エラー: {e}")
    
    async def fetch_page(self, url: str) -> Optional[str]:
        """ページを非同期で取得"""
        async with self.semaphore:
            try:
                # ランダムな遅延
                delay = random.uniform(*self.delay_range)
                await asyncio.sleep(delay)
                
                # ヘッダーを更新
                self.session.headers['User-Agent'] = random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                ])
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        logger.warning(f"レート制限検出: {url}")
                        await asyncio.sleep(30)
                        return None
                    else:
                        logger.warning(f"HTTPエラー {response.status}: {url}")
                        return None
                        
            except asyncio.TimeoutError:
                logger.error(f"タイムアウト: {url}")
                return None
            except Exception as e:
                logger.error(f"取得エラー {url}: {e}")
                return None
    
    async def scrape_restaurant_enhanced(self, restaurant_url: str) -> Optional[Dict]:
        """店舗の拡張情報を取得"""
        
        if restaurant_url in self.processed_urls:
            return None
        
        html = await self.fetch_page(restaurant_url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # 基本情報取得
        shop_name = self._extract_shop_name(soup)
        if not shop_name:
            return None
        
        # 拡張情報を含む詳細データ
        info = {
            'shop_name': shop_name,
            'url': restaurant_url,
            'phone': self._extract_phone(soup),
            'address': self._extract_address(soup),
            'genre': self._extract_genre(soup),
            'station': self._extract_station(soup),
            'open_time': self._extract_open_time(soup),
            # 新規追加項目
            'seats': self._extract_seats(soup),
            'official_url': self._extract_official_url(soup),
            'review_count': self._extract_review_count(soup),
            'rating': self._extract_rating(soup),
            'budget_dinner': self._extract_budget(soup, 'dinner'),
            'budget_lunch': self._extract_budget(soup, 'lunch'),
            'source': '食べログ',
            'scraped_at': datetime.now().isoformat()
        }
        
        self.processed_urls.add(restaurant_url)
        return info
    
    def _extract_shop_name(self, soup: BeautifulSoup) -> str:
        """店名を抽出"""
        selectors = [
            'h2.display-name span',
            'h2.display-name',
            'h1.display-name span',
            'h1.display-name',
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
    
    def _extract_phone(self, soup: BeautifulSoup) -> str:
        """電話番号を抽出"""
        phone_patterns = [
            r'03-\d{4}-\d{4}',
            r'0\d{2,3}-\d{3,4}-\d{4}',
            r'0\d{9,10}'
        ]
        
        selectors = [
            'span.rstinfo-table__tel-num',
            'p.rstinfo-table__tel'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                for pattern in phone_patterns:
                    match = re.search(pattern, text)
                    if match:
                        return match.group()
        
        return ""
    
    def _extract_address(self, soup: BeautifulSoup) -> str:
        """住所を抽出"""
        selectors = [
            'p.rstinfo-table__address',
            'span.rstinfo-table__address'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                address = elem.get_text(strip=True)
                address = re.sub(r'〒\d{3}-\d{4}\s*', '', address)
                if address:
                    return address
        
        return ""
    
    def _extract_genre(self, soup: BeautifulSoup) -> str:
        """ジャンルを抽出"""
        # ジャンル専用セレクタ
        for th in soup.find_all(['th', 'td'], string=re.compile('ジャンル')):
            next_elem = th.find_next_sibling()
            if next_elem:
                genre = next_elem.get_text(strip=True)
                if genre:
                    return genre
        
        # その他のセレクタ
        genre_elem = soup.select_one('.rstinfo-table__genre')
        if genre_elem:
            return genre_elem.get_text(strip=True)
        
        return ""
    
    def _extract_station(self, soup: BeautifulSoup) -> str:
        """最寄り駅を抽出"""
        for th in soup.find_all(['th', 'td'], string=re.compile('交通手段|最寄り駅')):
            next_elem = th.find_next_sibling()
            if next_elem:
                station = next_elem.get_text(strip=True)
                station = station.split('、')[0].split('から')[0]
                if station:
                    return station
        
        return ""
    
    def _extract_open_time(self, soup: BeautifulSoup) -> str:
        """営業時間を抽出"""
        for th in soup.find_all(['th', 'td'], string=re.compile('営業時間')):
            next_elem = th.find_next_sibling()
            if next_elem:
                hours = next_elem.get_text(strip=True)
                hours = re.sub(r'\s+', ' ', hours)
                if hours:
                    return hours
        
        hours_elem = soup.select_one('p.rstinfo-table__open-hours')
        if hours_elem:
            return re.sub(r'\s+', ' ', hours_elem.get_text(strip=True))
        
        return ""
    
    def _extract_seats(self, soup: BeautifulSoup) -> str:
        """席数を抽出"""
        # 席数のパターン
        seat_patterns = [
            r'(\d+)\s*席',
            r'席数\s*[:：]\s*(\d+)',
            r'(\d+)\s*seats'
        ]
        
        # テーブルから検索
        for th in soup.find_all(['th', 'td'], string=re.compile('席数|座席')):
            next_elem = th.find_next_sibling()
            if next_elem:
                text = next_elem.get_text(strip=True)
                for pattern in seat_patterns:
                    match = re.search(pattern, text)
                    if match:
                        return match.group(1) + "席"
                # パターンにマッチしなくても席数情報があれば返す
                if text and '席' in text:
                    return text
        
        # rstinfo-tableから検索
        info_table = soup.select_one('.rstinfo-table')
        if info_table:
            text = info_table.get_text()
            for pattern in seat_patterns:
                match = re.search(pattern, text)
                if match:
                    return match.group(1) + "席"
        
        return ""
    
    def _extract_official_url(self, soup: BeautifulSoup) -> str:
        """公式サイトURLを抽出"""
        # 公式サイトリンクのパターン
        official_patterns = [
            'ホームページ',
            '公式',
            'Official',
            'HP',
            'Website'
        ]
        
        # テーブルから検索
        for pattern in official_patterns:
            for th in soup.find_all(['th', 'td'], string=re.compile(pattern, re.IGNORECASE)):
                next_elem = th.find_next_sibling()
                if next_elem:
                    link = next_elem.find('a')
                    if link and link.get('href'):
                        return link.get('href')
        
        # 外部リンクから検索
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            # 食べログ以外の外部リンクで公式っぽいもの
            if 'tabelog.com' not in href and any(p in text for p in official_patterns):
                if href.startswith('http'):
                    return href
        
        # SNSリンク（Instagram, Twitter/X, Facebook）
        sns_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if any(sns in href for sns in ['instagram.com', 'twitter.com', 'x.com', 'facebook.com']):
                sns_links.append(href)
        
        # SNSリンクがあれば最初のものを返す
        if sns_links:
            return sns_links[0]
        
        return ""
    
    def _extract_review_count(self, soup: BeautifulSoup) -> str:
        """口コミ数を抽出"""
        # 口コミ数のセレクタ
        review_selectors = [
            'em.rstdtl-rating__review-count',
            'span.rstdtl-rating__review-count',
            '.rdheader-rating__review-count em',
            'em.rdheader-rating__review-count'
        ]
        
        for selector in review_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                # 数字のみ抽出
                match = re.search(r'(\d+)', text)
                if match:
                    return match.group(1)
        
        # パターンマッチング
        review_patterns = [
            r'(\d+)\s*件の口コミ',
            r'口コミ\s*(\d+)\s*件',
            r'レビュー\s*(\d+)',
            r'(\d+)\s*reviews'
        ]
        
        text = soup.get_text()
        for pattern in review_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        
        return "0"
    
    def _extract_rating(self, soup: BeautifulSoup) -> str:
        """評価点を抽出"""
        rating_selectors = [
            'span.rdheader-rating__score-val-dtl',
            'b.c-rating__val',
            'span.rstdtl-rating__score',
            '.rdheader-rating__score em'
        ]
        
        for selector in rating_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                # 数値のみ抽出（3.58 のような形式）
                match = re.search(r'(\d+\.\d+)', text)
                if match:
                    return match.group(1)
        
        return ""
    
    def _extract_budget(self, soup: BeautifulSoup, meal_type: str) -> str:
        """予算を抽出"""
        if meal_type == 'dinner':
            keywords = ['夜', 'ディナー', '夕食']
        else:
            keywords = ['昼', 'ランチ', '昼食']
        
        # 予算情報を探す
        for keyword in keywords:
            for elem in soup.find_all(string=re.compile(keyword)):
                parent = elem.parent
                if parent:
                    text = parent.get_text(strip=True)
                    # 価格パターン（¥1,000～¥1,999 など）
                    match = re.search(r'¥[\d,]+\s*[~～-]\s*¥[\d,]+', text)
                    if match:
                        return match.group()
        
        return ""
    
    async def scrape_restaurant_list(self, area_url: str, max_pages: int = 10) -> List[str]:
        """レストランリストページから店舗URLを取得"""
        restaurant_urls = []
        
        for page in range(1, max_pages + 1):
            page_url = f"{area_url}rstLst/{page}/"
            html = await self.fetch_page(page_url)
            
            if not html:
                continue
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # 店舗リンクを抽出
            links = soup.select('a.list-rst__rst-name-target')
            if not links:
                links = soup.select('h3.list-rst__rst-name a')
            
            for link in links:
                href = link.get('href', '')
                if href:
                    full_url = href if href.startswith('http') else f"https://tabelog.com{href}"
                    restaurant_urls.append(full_url)
            
            logger.info(f"ページ {page}: {len(links)}件の店舗URL取得")
        
        return restaurant_urls
    
    async def scrape_enhanced_data(self, target_count: int = 1200):
        """拡張データを収集"""
        logger.info(f"🚀 拡張版スクレイパー起動: 目標 {target_count}件")
        
        # 東京の主要エリア
        area_urls = [
            'https://tabelog.com/tokyo/A1301/',  # 銀座・新橋
            'https://tabelog.com/tokyo/A1302/',  # 東京・丸の内
            'https://tabelog.com/tokyo/A1303/',  # 渋谷
            'https://tabelog.com/tokyo/A1304/',  # 新宿
            'https://tabelog.com/tokyo/A1305/',  # 池袋
            'https://tabelog.com/tokyo/A1306/',  # 原宿・表参道
            'https://tabelog.com/tokyo/A1307/',  # 六本木・麻布
            'https://tabelog.com/tokyo/A1308/',  # 赤坂
            'https://tabelog.com/tokyo/A1309/',  # 四ツ谷・市ヶ谷
            'https://tabelog.com/tokyo/A1310/',  # 秋葉原・神田
        ]
        
        all_urls = []
        
        # 各エリアから店舗URLを収集
        for area_url in area_urls:
            if len(all_urls) >= target_count * 1.2:  # 20%余分に収集
                break
            
            area_name = area_url.split('/')[-2]
            logger.info(f"📍 エリア {area_name} のデータ収集開始")
            
            urls = await self.scrape_restaurant_list(area_url, max_pages=10)
            all_urls.extend(urls)
            logger.info(f"  合計 {len(all_urls)} 件のURL収集済み")
        
        # 重複を除去
        all_urls = list(set(all_urls))[:target_count]
        logger.info(f"📋 {len(all_urls)}件のユニークURLを処理します")
        
        # 詳細情報を取得
        for i, url in enumerate(all_urls):
            if len(self.results) >= target_count:
                break
            
            if url in self.processed_urls:
                continue
            
            logger.info(f"処理中 {i+1}/{len(all_urls)}: {url}")
            result = await self.scrape_restaurant_enhanced(url)
            
            if result:
                self.results.append(result)
                logger.info(f"  ✅ 収集成功: {result['shop_name']}")
                logger.info(f"    席数: {result['seats'] or 'N/A'}")
                logger.info(f"    公式URL: {result['official_url'] or 'N/A'}")
                logger.info(f"    口コミ数: {result['review_count']}")
                
                # 10件ごとに保存
                if len(self.results) % 10 == 0:
                    self.save_progress()
                    logger.info(f"💾 進捗保存: {len(self.results)}件")
        
        # 最終保存
        self.save_progress()
        logger.info(f"🎉 収集完了: 合計 {len(self.results)}件")
        
        return self.results


async def main():
    """メイン処理"""
    async with TabelogEnhancedScraperV6(max_concurrent=5) as scraper:
        results = await scraper.scrape_enhanced_data(target_count=1200)
        
        if results:
            # Excel出力
            from restaurant_data_integrator import RestaurantDataIntegrator
            integrator = RestaurantDataIntegrator()
            
            # 拡張データ用にintegratorを修正する必要があるため、
            # ここでは直接pandasを使用
            import pandas as pd
            
            df = pd.DataFrame(results)
            
            # カラムの順序を整理
            columns = [
                'shop_name', 'phone', 'address', 'genre', 'station', 
                'open_time', 'seats', 'official_url', 'review_count',
                'rating', 'budget_dinner', 'budget_lunch', 'url', 'source', 'scraped_at'
            ]
            
            # 存在するカラムのみ選択
            existing_columns = [col for col in columns if col in df.columns]
            df = df[existing_columns]
            
            # Excel出力
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'output/東京都_飲食店リスト_拡張_{len(results)}件_{timestamp}.xlsx'
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='飲食店リスト', index=False)
                
                # カラム幅を自動調整
                worksheet = writer.sheets['飲食店リスト']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"✅ Excelファイル作成: {output_file}")
            
            # データ品質統計
            with_seats = sum(1 for r in results if r.get('seats'))
            with_official = sum(1 for r in results if r.get('official_url'))
            with_reviews = sum(1 for r in results if r.get('review_count') and r['review_count'] != '0')
            with_rating = sum(1 for r in results if r.get('rating'))
            
            logger.info(f"\n📊 データ品質:")
            logger.info(f"  席数情報: {with_seats}/{len(results)} ({with_seats/len(results)*100:.1f}%)")
            logger.info(f"  公式URL: {with_official}/{len(results)} ({with_official/len(results)*100:.1f}%)")
            logger.info(f"  口コミあり: {with_reviews}/{len(results)} ({with_reviews/len(results)*100:.1f}%)")
            logger.info(f"  評価点: {with_rating}/{len(results)} ({with_rating/len(results)*100:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())