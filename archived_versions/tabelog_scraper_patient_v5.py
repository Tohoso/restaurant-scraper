#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
食べログスクレイパー 忍耐版 V5
- 超低速だが確実な収集
- レート制限を完全に回避
- 長時間実行前提
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tabelog_patient_v5.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TabelogPatientScraperV5:
    """忍耐強い食べログスクレイパー"""
    
    def __init__(self):
        """初期化"""
        self.max_concurrent = 1  # 同時接続数を1に制限
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.session = None
        
        # 固定の長い遅延
        self.base_delay = 15.0  # 基本遅延15秒
        self.random_delay_range = (5.0, 10.0)  # 追加ランダム遅延
        
        # 進捗管理
        self.processed_urls: Set[str] = set()
        self.results: List[Dict] = []
        
        # ファイルパス
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.progress_file = self.cache_dir / "patient_progress_v5.json"
        self.results_file = self.cache_dir / "patient_results_v5.json"
        
        # 東京の全エリアURL
        self.area_base_urls = []
        for i in range(1301, 1325):  # A1301からA1324まで
            self.area_base_urls.append(f"https://tabelog.com/tokyo/A{i}/")
        
    async def __aenter__(self):
        """非同期コンテキストマネージャ入口"""
        timeout = ClientTimeout(total=60, connect=20, sock_read=20)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
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
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
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
    
    async def patient_fetch(self, url: str) -> Optional[str]:
        """忍耐強くページを取得"""
        # 長い遅延
        total_delay = self.base_delay + random.uniform(*self.random_delay_range)
        logger.info(f"⏳ {total_delay:.1f}秒待機中...")
        await asyncio.sleep(total_delay)
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    logger.info(f"✅ 取得成功: {url}")
                    return await response.text()
                elif response.status == 429:
                    logger.warning(f"⚠️ レート制限検出。追加で60秒待機")
                    await asyncio.sleep(60)
                    return None
                else:
                    logger.warning(f"HTTPエラー {response.status}: {url}")
                    return None
                    
        except Exception as e:
            logger.error(f"取得エラー {url}: {e}")
            return None
    
    async def scrape_list_page(self, url: str) -> List[str]:
        """リストページから店舗URLを抽出"""
        html = await self.patient_fetch(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        restaurant_urls = []
        
        # シンプルなセレクタのみ使用
        links = soup.select('a[href*="/A"][href*="/A"]')
        for link in links:
            href = link.get('href', '')
            if re.match(r'.*/A\d+/A\d+/\d+/$', href):
                full_url = f"https://tabelog.com{href}" if not href.startswith('http') else href
                if full_url not in self.processed_urls:
                    restaurant_urls.append(full_url)
        
        return list(set(restaurant_urls))
    
    async def scrape_restaurant_detail(self, url: str) -> Optional[Dict]:
        """店舗詳細を取得"""
        if url in self.processed_urls:
            return None
        
        html = await self.patient_fetch(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # 店名
        shop_name = ""
        name_elem = soup.select_one('h2.display-name span') or soup.select_one('h2.display-name')
        if name_elem:
            shop_name = name_elem.get_text(strip=True)
            shop_name = re.sub(r'\s*\([^)]+\)\s*$', '', shop_name)
        
        if not shop_name:
            return None
        
        # 基本情報
        info = {
            'shop_name': shop_name,
            'url': url,
            'phone': "",
            'address': "",
            'genre': "",
            'station': "",
            'open_time': "",
            'source': '食べログ',
            'scraped_at': datetime.now().isoformat()
        }
        
        # 電話番号
        phone_elem = soup.select_one('span.rstinfo-table__tel-num')
        if phone_elem:
            phone_match = re.search(r'[\d\-]+', phone_elem.get_text())
            if phone_match:
                info['phone'] = phone_match.group()
        
        # 住所
        addr_elem = soup.select_one('p.rstinfo-table__address')
        if addr_elem:
            info['address'] = addr_elem.get_text(strip=True)
        
        # ジャンル
        for th in soup.find_all(['th', 'td'], string=re.compile('ジャンル')):
            next_elem = th.find_next_sibling()
            if next_elem:
                info['genre'] = next_elem.get_text(strip=True)
                break
        
        # 最寄り駅
        for elem in soup.select('span.linktree__parent-target-text'):
            text = elem.get_text(strip=True)
            if '駅' in text:
                info['station'] = text
                break
        
        # 営業時間
        hours_elem = soup.select_one('p.rstinfo-table__open-hours')
        if hours_elem:
            info['open_time'] = re.sub(r'\s+', ' ', hours_elem.get_text(strip=True))
        
        self.processed_urls.add(url)
        return info
    
    async def scrape_restaurants_patient(self, target_count: int = 50000):
        """忍耐強く大量収集"""
        logger.info(f"🐢 忍耐モード起動: 目標 {target_count}件")
        logger.info(f"⏱️ 基本遅延: {self.base_delay}秒 + ランダム{self.random_delay_range}秒")
        
        current_count = len(self.results)
        if current_count >= target_count:
            logger.info(f"既に目標数を達成: {current_count}件")
            return
        
        # 各エリアを順番に処理
        for area_url in self.area_base_urls:
            if len(self.results) >= target_count:
                break
            
            area_code = area_url.split('/')[-2]
            logger.info(f"\n📍 エリア {area_code} の処理開始")
            
            # 各エリアで1-30ページまで
            for page in range(1, 31):
                if len(self.results) >= target_count:
                    break
                
                list_url = f"{area_url}rstLst/{page}/"
                logger.info(f"\n📄 ページ {page}/30 を処理中...")
                
                # リストページから店舗URL取得
                restaurant_urls = await self.scrape_list_page(list_url)
                logger.info(f"  → {len(restaurant_urls)}件の新規店舗URL発見")
                
                # 各店舗の詳細を取得
                for i, rest_url in enumerate(restaurant_urls):
                    if len(self.results) >= target_count:
                        break
                    
                    logger.info(f"\n🍴 店舗 {i+1}/{len(restaurant_urls)} を処理中...")
                    result = await self.scrape_restaurant_detail(rest_url)
                    
                    if result:
                        self.results.append(result)
                        current_count = len(self.results)
                        logger.info(f"  ✅ 収集成功: {result['shop_name']} (合計: {current_count}件)")
                        
                        # 10件ごとに保存
                        if current_count % 10 == 0:
                            self.save_progress()
                            logger.info(f"💾 進捗保存: {current_count}件")
                        
                        # 100件ごとに統計表示
                        if current_count % 100 == 0:
                            elapsed = (datetime.now() - datetime.fromisoformat(self.results[0]['scraped_at'])).total_seconds()
                            rate = current_count / (elapsed / 3600)  # 件/時間
                            eta_hours = (target_count - current_count) / rate if rate > 0 else 0
                            logger.info(f"\n📊 統計:")
                            logger.info(f"  収集速度: {rate:.1f}件/時間")
                            logger.info(f"  推定完了時間: {eta_hours:.1f}時間後")
            
            # エリア切り替え時は長めに休憩
            logger.info(f"\n⏸️ エリア切り替え: 60秒休憩")
            await asyncio.sleep(60)
        
        # 最終保存
        self.save_progress()
        logger.info(f"\n🎉 収集完了: 合計 {len(self.results)}件")


async def main():
    """メイン処理"""
    target_count = int(sys.argv[1]) if len(sys.argv) > 1 else 5000  # デフォルトは5000件
    
    async with TabelogPatientScraperV5() as scraper:
        await scraper.scrape_restaurants_patient(target_count)
        
        # Excel出力
        if scraper.results:
            from restaurant_data_integrator import RestaurantDataIntegrator
            integrator = RestaurantDataIntegrator()
            
            integrator.add_restaurants(scraper.results)
            integrator.remove_duplicates()
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'output/東京都_飲食店リスト_忍耐_{len(scraper.results)}件_{timestamp}.xlsx'
            integrator.create_excel_report(output_file)
            
            logger.info(f"✅ Excelファイル作成: {output_file}")
            
            # データ品質
            with_phone = sum(1 for r in scraper.results if r.get('phone'))
            with_address = sum(1 for r in scraper.results if r.get('address'))
            logger.info(f"\n📊 データ品質:")
            logger.info(f"  電話番号: {with_phone}/{len(scraper.results)} ({with_phone/len(scraper.results)*100:.1f}%)")
            logger.info(f"  住所: {with_address}/{len(scraper.results)} ({with_address/len(scraper.results)*100:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())