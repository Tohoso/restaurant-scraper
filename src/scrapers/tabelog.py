#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
食べログスクレイパー
リファクタリング済みの統合版
"""

import asyncio
import re
from typing import Dict, List, Optional
from datetime import datetime
from .base import BaseScraper
from ..extractors.tabelog import TabelogExtractor
import logging

logger = logging.getLogger(__name__)


class TabelogScraper(BaseScraper):
    """食べログスクレイパー"""
    
    # 東京のエリアコード
    TOKYO_AREAS = {
        '銀座・新橋・有楽町': 'A1301',
        '東京・丸の内・日本橋': 'A1302',
        '渋谷': 'A1303',
        '新宿・代々木・大久保': 'A1304',
        '池袋～高田馬場・早稲田': 'A1305',
        '原宿・表参道・青山': 'A1306',
        '六本木・麻布・広尾': 'A1307',
        '赤坂・永田町・溜池': 'A1308',
        '四ツ谷・市ヶ谷・飯田橋': 'A1309',
        '秋葉原・神田・水道橋': 'A1310',
        '上野・浅草・日暮里': 'A1311',
        '錦糸町・押上・新小岩': 'A1312',
        '葛飾・江戸川・江東': 'A1313',
        '蒲田・大森・羽田周辺': 'A1315',
        '恵比寿・目黒・品川': 'A1316',
        '自由が丘・中目黒・学芸大学': 'A1317',
        '下北沢・明大前・成城学園前': 'A1318',
        '中野・吉祥寺・三鷹': 'A1319',
        '西荻窪・荻窪・阿佐ヶ谷': 'A1320',
        '板橋・東武練馬・下赤塚': 'A1322',
        '大塚・巣鴨・駒込・赤羽': 'A1323',
        '千住・綾瀬・葛飾': 'A1324'
    }
    
    def __init__(self, **kwargs):
        """初期化"""
        super().__init__(**kwargs)
        self.extractor = TabelogExtractor()
        self.base_url = "https://tabelog.com"
    
    def build_list_url(self, area_code: str, page: int = 1) -> str:
        """
        リストページのURLを構築
        
        Args:
            area_code: エリアコード（例: 'A1301'）
            page: ページ番号
            
        Returns:
            URL文字列
        """
        return f"{self.base_url}/tokyo/{area_code}/rstLst/{page}/"
    
    async def extract_restaurant_urls(self, html: str) -> List[str]:
        """
        リストページから店舗URLを抽出
        
        Args:
            html: HTML文字列
            
        Returns:
            店舗URLのリスト
        """
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        urls = []
        
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
                # 正しい店舗URLパターンかチェック
                if re.match(r'.*/A\d+/A\d+/\d+/?$', href):
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    urls.append(full_url)
        
        return list(set(urls))
    
    async def scrape_restaurant_detail(self, url: str) -> Optional[Dict]:
        """
        店舗詳細を取得
        
        Args:
            url: 店舗URL
            
        Returns:
            店舗情報の辞書
        """
        if url in self.processed_urls:
            return None
        
        html = await self.fetch_page(url)
        if not html:
            return None
        
        # 抽出器を使用してデータを取得
        self.extractor.set_html(html)
        info = self.extractor.extract_all()
        
        # メタデータを追加
        info['url'] = url
        info['source'] = '食べログ'
        info['scraped_at'] = datetime.now().isoformat()
        
        # 店名がない場合は無効なデータ
        if not info.get('shop_name'):
            logger.warning(f"店名が取得できません: {url}")
            return None
        
        self.processed_urls.add(url)
        return info
    
    async def scrape_area(
        self,
        area_code: str,
        max_pages: int = 10,
        max_restaurants: int = None
    ) -> List[Dict]:
        """
        特定エリアのレストランをスクレイプ
        
        Args:
            area_code: エリアコード
            max_pages: 最大ページ数
            max_restaurants: 最大取得件数
            
        Returns:
            レストラン情報のリスト
        """
        area_results = []
        restaurant_urls = []
        
        # リストページから店舗URLを収集
        for page in range(1, max_pages + 1):
            list_url = self.build_list_url(area_code, page)
            logger.info(f"ページ {page}/{max_pages} を取得中: {list_url}")
            
            html = await self.fetch_page(list_url)
            if not html:
                logger.warning(f"ページ取得失敗: {list_url}")
                continue
            
            page_urls = await self.extract_restaurant_urls(html)
            if not page_urls:
                logger.info(f"これ以上店舗がありません: ページ {page}")
                break
            
            restaurant_urls.extend(page_urls)
            logger.info(f"  {len(page_urls)}件のURLを取得")
            
            if max_restaurants and len(restaurant_urls) >= max_restaurants:
                restaurant_urls = restaurant_urls[:max_restaurants]
                break
        
        # 各店舗の詳細を取得
        logger.info(f"合計 {len(restaurant_urls)}件の店舗詳細を取得します")
        
        for i, url in enumerate(restaurant_urls, 1):
            if max_restaurants and len(area_results) >= max_restaurants:
                break
            
            logger.info(f"処理中 {i}/{len(restaurant_urls)}: {url}")
            result = await self.scrape_restaurant_detail(url)
            
            if result:
                area_results.append(result)
                logger.info(f"  ✅ {result['shop_name']}")
                
                # 定期的に進捗を保存
                if len(area_results) % 10 == 0:
                    self.results.extend(area_results[-10:])
                    self.save_progress()
        
        return area_results
    
    async def scrape(
        self,
        areas: List[str] = None,
        max_pages_per_area: int = 10,
        max_restaurants_per_area: int = None,
        total_limit: int = None
    ) -> List[Dict]:
        """
        メインスクレイピングメソッド
        
        Args:
            areas: スクレイプするエリア名のリスト
            max_pages_per_area: エリアごとの最大ページ数
            max_restaurants_per_area: エリアごとの最大レストラン数
            total_limit: 全体の最大取得件数
            
        Returns:
            全レストラン情報のリスト
        """
        if areas is None:
            areas = list(self.TOKYO_AREAS.keys())
        
        all_results = []
        
        for area_name in areas:
            if total_limit and len(all_results) >= total_limit:
                break
            
            area_code = self.TOKYO_AREAS.get(area_name)
            if not area_code:
                logger.warning(f"不明なエリア: {area_name}")
                continue
            
            logger.info(f"\n{'='*50}")
            logger.info(f"エリア: {area_name} ({area_code})")
            logger.info(f"{'='*50}")
            
            # エリアごとの制限を計算
            area_limit = max_restaurants_per_area
            if total_limit:
                remaining = total_limit - len(all_results)
                if area_limit:
                    area_limit = min(area_limit, remaining)
                else:
                    area_limit = remaining
            
            area_results = await self.scrape_area(
                area_code,
                max_pages_per_area,
                area_limit
            )
            
            all_results.extend(area_results)
            self.results = all_results
            
            logger.info(f"エリア完了: {len(area_results)}件取得")
            logger.info(f"合計: {len(all_results)}件")
            
            # エリア間で少し待機
            await asyncio.sleep(5)
        
        # 最終保存
        self.save_progress()
        self.save_results()
        
        # 統計表示
        stats = self.get_stats()
        logger.info(f"\n{'='*50}")
        logger.info("スクレイピング完了")
        logger.info(f"取得件数: {len(all_results)}件")
        logger.info(f"成功率: {stats['success_rate']:.1f}%")
        logger.info(f"レート制限: {stats['rate_limited']}回")
        logger.info(f"{'='*50}")
        
        return all_results