#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ホットペッパースクレイパー
HotPepper APIを使用した飲食店情報取得
"""

import asyncio
import time
from typing import Dict, List, Optional
from datetime import datetime
from .base import BaseScraper
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from hotpepper_api_client import HotpepperAPIClient, AREA_COORDINATES
import logging

logger = logging.getLogger(__name__)


class HotPepperScraper(BaseScraper):
    """ホットペッパースクレイパー"""
    
    # エリア名のマッピング
    AREA_MAPPING = {
        '東京都': ['東京都心', '新宿', '渋谷', '池袋', '銀座'],
        '大阪府': ['大阪梅田', '大阪難波'],
        '神奈川県': ['横浜駅'],
        '愛知県': ['名古屋駅'],
        '福岡県': ['福岡天神'],
        # 食べログのエリア名をホットペッパーのエリアにマッピング
        '銀座・新橋・有楽町': ['銀座'],
        '新宿・代々木・大久保': ['新宿'],
        '渋谷': ['渋谷'],
        '池袋～高田馬場・早稲田': ['池袋'],
        '東京・丸の内・日本橋': ['東京都心']
    }
    
    def __init__(self, api_key: str, **kwargs):
        """
        初期化
        
        Args:
            api_key: HotPepper APIキー
        """
        super().__init__(**kwargs)
        self.api_key = api_key
        self.client = HotpepperAPIClient(api_key)
    
    async def scrape(
        self,
        areas: List[str] = None,
        max_per_area: int = 100,
        total_limit: int = None,
        keyword: str = None,
        genre: str = None
    ) -> List[Dict]:
        """
        メインスクレイピングメソッド
        
        Args:
            areas: スクレイプするエリア名のリスト
            max_per_area: エリアごとの最大取得件数
            total_limit: 全体の最大取得件数
            keyword: 検索キーワード
            genre: ジャンル
            
        Returns:
            全レストラン情報のリスト
        """
        if not self.api_key or self.api_key == "YOUR_API_KEY_HERE":
            logger.error("有効なAPIキーが設定されていません")
            return []
        
        if areas is None:
            areas = ['東京都']
        
        all_results = []
        
        for area_name in areas:
            if total_limit and len(all_results) >= total_limit:
                break
            
            logger.info(f"\n{'='*50}")
            logger.info(f"エリア: {area_name}")
            logger.info(f"{'='*50}")
            
            # エリア名から座標を取得
            hotpepper_areas = self.AREA_MAPPING.get(area_name, [])
            if not hotpepper_areas:
                logger.warning(f"未対応のエリア: {area_name}")
                continue
            
            area_results = []
            
            for hp_area in hotpepper_areas:
                if total_limit and len(all_results) >= total_limit:
                    break
                
                coords = AREA_COORDINATES.get(hp_area)
                if not coords:
                    continue
                
                logger.info(f"  検索エリア: {hp_area}")
                
                # エリアごとの制限を計算
                area_limit = max_per_area
                if total_limit:
                    remaining = total_limit - len(all_results)
                    area_limit = min(area_limit, remaining)
                
                # 同期的にAPIを呼び出し（HotPepperClientは同期的）
                shops = await self._fetch_shops(
                    coords['lat'],
                    coords['lng'],
                    keyword=keyword,
                    genre=genre,
                    max_count=area_limit
                )
                
                area_results.extend(shops)
                logger.info(f"  {len(shops)}件取得")
            
            all_results.extend(area_results)
            self.results = all_results
            
            logger.info(f"エリア完了: {len(area_results)}件取得")
            logger.info(f"合計: {len(all_results)}件")
        
        # 最終保存
        self.save_progress()
        self.save_results()
        
        # 統計表示
        stats = self.get_stats()
        logger.info(f"\n{'='*50}")
        logger.info("スクレイピング完了")
        logger.info(f"取得件数: {len(all_results)}件")
        logger.info(f"{'='*50}")
        
        return all_results
    
    async def _fetch_shops(
        self,
        lat: float,
        lng: float,
        keyword: str = None,
        genre: str = None,
        max_count: int = 100
    ) -> List[Dict]:
        """
        非同期で店舗情報を取得（実際は同期的にAPIを呼び出し）
        
        Args:
            lat: 緯度
            lng: 経度
            keyword: キーワード
            genre: ジャンル
            max_count: 最大取得件数
            
        Returns:
            店舗情報のリスト
        """
        # 同期的なAPI呼び出しを非同期で実行
        loop = asyncio.get_event_loop()
        shops = await loop.run_in_executor(
            None,
            self.client.get_all_shops,
            lat, lng, 3, keyword, genre, max_count
        )
        
        # 店舗情報を抽出
        extracted = self.client.extract_shop_info(shops)
        
        # 追加情報を付与
        for shop in extracted:
            shop['scraped_at'] = datetime.now().isoformat()
            # 口コミ数、評価はHotPepper APIでは提供されない
            shop['review_count'] = ''
            shop['rating'] = ''
            # 公式URLは提供されない
            shop['official_url'] = ''
            # 予算情報はbudget_dinnerに既に設定済み
            shop['budget_lunch'] = ''  # ランチ予算は別途取得不可
            
            # seatsは既にhotpepper_api_clientで設定済み
            # party_capacityが設定されている場合は補足情報として追加
            if shop.get('party_capacity'):
                if shop.get('seats'):
                    shop['seats'] = f"{shop['seats']}席（宴会最大{shop['party_capacity']}名）"
                else:
                    shop['seats'] = f"宴会最大{shop['party_capacity']}名"
        
        return extracted