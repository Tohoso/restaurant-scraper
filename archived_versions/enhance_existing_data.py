#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
既存の1200件データに追加情報を付与
- 席数、公式URL、口コミ数を追加取得
"""

import asyncio
import aiohttp
import json
import re
import random
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup
from pathlib import Path
import logging
from aiohttp import ClientTimeout, TCPConnector

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhance_existing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataEnhancer:
    """既存データ拡張クラス"""
    
    def __init__(self):
        self.session = None
        self.semaphore = asyncio.Semaphore(3)  # 同時接続数を制限
        self.delay = 5.0  # 基本遅延
        
    async def __aenter__(self):
        timeout = ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_page(self, url: str) -> Optional[str]:
        """ページを取得"""
        async with self.semaphore:
            await asyncio.sleep(self.delay + random.uniform(0, 2))
            
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        logger.warning(f"レート制限: {url}")
                        await asyncio.sleep(30)
                        return None
                    else:
                        return None
            except Exception as e:
                logger.error(f"取得エラー {url}: {e}")
                return None
    
    def extract_additional_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """追加情報を抽出"""
        info = {
            'seats': '',
            'official_url': '',
            'review_count': '0',
            'rating': '',
            'budget_dinner': '',
            'budget_lunch': ''
        }
        
        # 席数
        for th in soup.find_all(['th', 'td'], string=re.compile('席数|座席')):
            next_elem = th.find_next_sibling()
            if next_elem:
                text = next_elem.get_text(strip=True)
                if text:
                    # 数字を抽出
                    match = re.search(r'(\d+)', text)
                    if match:
                        info['seats'] = match.group(1) + "席"
                    elif '席' in text:
                        info['seats'] = text
                    break
        
        # 公式URL（ホームページ）
        for th in soup.find_all(['th', 'td'], string=re.compile('ホームページ|公式|HP')):
            next_elem = th.find_next_sibling()
            if next_elem:
                link = next_elem.find('a')
                if link and link.get('href'):
                    info['official_url'] = link.get('href')
                    break
        
        # SNSリンクも探す
        if not info['official_url']:
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if any(sns in href for sns in ['instagram.com', 'twitter.com', 'x.com', 'facebook.com']):
                    info['official_url'] = href
                    break
        
        # 口コミ数
        review_elem = soup.select_one('em.rstdtl-rating__review-count, em.rdheader-rating__review-count')
        if review_elem:
            text = review_elem.get_text(strip=True)
            match = re.search(r'(\d+)', text)
            if match:
                info['review_count'] = match.group(1)
        
        # 評価点
        rating_elem = soup.select_one('span.rdheader-rating__score-val-dtl, b.c-rating__val')
        if rating_elem:
            text = rating_elem.get_text(strip=True)
            match = re.search(r'(\d+\.\d+)', text)
            if match:
                info['rating'] = match.group(1)
        
        # 予算（夜）
        for elem in soup.find_all(string=re.compile('夜|ディナー')):
            parent = elem.parent
            if parent:
                text = parent.get_text(strip=True)
                match = re.search(r'¥[\d,]+\s*[~～-]\s*¥[\d,]+', text)
                if match:
                    info['budget_dinner'] = match.group()
                    break
        
        # 予算（昼）
        for elem in soup.find_all(string=re.compile('昼|ランチ')):
            parent = elem.parent
            if parent:
                text = parent.get_text(strip=True)
                match = re.search(r'¥[\d,]+\s*[~～-]\s*¥[\d,]+', text)
                if match:
                    info['budget_lunch'] = match.group()
                    break
        
        return info
    
    async def enhance_restaurant(self, restaurant: Dict) -> Dict:
        """レストラン情報を拡張"""
        url = restaurant.get('url', '')
        if not url:
            return restaurant
        
        logger.info(f"処理中: {restaurant.get('shop_name', 'Unknown')}")
        
        html = await self.fetch_page(url)
        if not html:
            logger.warning(f"  取得失敗: {url}")
            return restaurant
        
        soup = BeautifulSoup(html, 'html.parser')
        additional_info = self.extract_additional_info(soup)
        
        # 既存データに追加情報をマージ
        enhanced = restaurant.copy()
        enhanced.update(additional_info)
        
        logger.info(f"  ✅ 席数: {additional_info['seats'] or 'N/A'}")
        logger.info(f"  ✅ 公式URL: {additional_info['official_url'] or 'N/A'}")
        logger.info(f"  ✅ 口コミ数: {additional_info['review_count']}")
        logger.info(f"  ✅ 評価: {additional_info['rating'] or 'N/A'}")
        
        return enhanced


async def main():
    """メイン処理"""
    # 既存のデータを読み込み
    existing_file = Path('cache/partial_results_v2.json')
    if not existing_file.exists():
        logger.error("既存データファイルが見つかりません")
        return
    
    with open(existing_file, 'r', encoding='utf-8') as f:
        existing_data = json.load(f)
    
    logger.info(f"📊 既存データ: {len(existing_data)}件")
    
    # データ拡張
    enhanced_data = []
    
    async with DataEnhancer() as enhancer:
        # バッチ処理
        batch_size = 10
        for i in range(0, len(existing_data), batch_size):
            batch = existing_data[i:i+batch_size]
            
            logger.info(f"\n📦 バッチ {i//batch_size + 1}/{(len(existing_data) + batch_size - 1)//batch_size}")
            
            tasks = [enhancer.enhance_restaurant(r) for r in batch]
            results = await asyncio.gather(*tasks)
            enhanced_data.extend(results)
            
            # 進捗保存
            if len(enhanced_data) % 50 == 0:
                with open('cache/enhanced_partial.json', 'w', encoding='utf-8') as f:
                    json.dump(enhanced_data, f, ensure_ascii=False, indent=2)
                logger.info(f"💾 進捗保存: {len(enhanced_data)}件")
    
    # 最終保存
    output_file = 'cache/enhanced_results_final.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enhanced_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n✅ 拡張完了: {len(enhanced_data)}件")
    
    # Excel出力
    df = pd.DataFrame(enhanced_data)
    
    # カラムの順序を整理
    columns_order = [
        'shop_name', 'phone', 'address', 'genre', 'station', 
        'open_time', 'seats', 'official_url', 'review_count',
        'rating', 'budget_dinner', 'budget_lunch', 'url', 'source', 'scraped_at'
    ]
    
    # 存在するカラムのみ選択
    existing_columns = [col for col in columns_order if col in df.columns]
    df = df[existing_columns]
    
    # Excel出力
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_file = f'output/東京都_飲食店リスト_拡張_1200件_{timestamp}.xlsx'
    
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='飲食店リスト', index=False)
        
        # カラム幅調整
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
    
    logger.info(f"📊 Excelファイル作成: {excel_file}")
    
    # データ品質統計
    with_seats = sum(1 for r in enhanced_data if r.get('seats'))
    with_official = sum(1 for r in enhanced_data if r.get('official_url'))
    with_reviews = sum(1 for r in enhanced_data if r.get('review_count') and r['review_count'] != '0')
    with_rating = sum(1 for r in enhanced_data if r.get('rating'))
    
    logger.info(f"\n📈 データ品質統計:")
    logger.info(f"  席数情報: {with_seats}/{len(enhanced_data)} ({with_seats/len(enhanced_data)*100:.1f}%)")
    logger.info(f"  公式URL: {with_official}/{len(enhanced_data)} ({with_official/len(enhanced_data)*100:.1f}%)")
    logger.info(f"  口コミあり: {with_reviews}/{len(enhanced_data)} ({with_reviews/len(enhanced_data)*100:.1f}%)")
    logger.info(f"  評価点: {with_rating}/{len(enhanced_data)} ({with_rating/len(enhanced_data)*100:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())