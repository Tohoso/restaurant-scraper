#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ホットペッパーグルメAPIクライアント
店名・電話番号・住所を取得するためのクライアント
"""

import requests
import time
import json
from typing import List, Dict, Optional
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HotpepperAPIClient:
    """ホットペッパーグルメAPIクライアント"""
    
    def __init__(self, api_key: str):
        """
        初期化
        
        Args:
            api_key (str): ホットペッパーグルメAPIキー
        """
        self.api_key = api_key
        self.base_url = "http://webservice.recruit.co.jp/hotpepper/gourmet/v1/"
        self.session = requests.Session()
        
    def search_shops(self, 
                    lat: Optional[float] = None,
                    lng: Optional[float] = None,
                    range_km: int = 3,
                    keyword: Optional[str] = None,
                    genre: Optional[str] = None,
                    count: int = 100,
                    start: int = 1) -> Dict:
        """
        店舗検索
        
        Args:
            lat (float, optional): 緯度
            lng (float, optional): 経度
            range_km (int): 検索範囲（1:300m, 2:500m, 3:1000m, 4:2000m, 5:3000m）
            keyword (str, optional): キーワード
            genre (str, optional): ジャンル
            count (int): 取得件数（最大100）
            start (int): 検索開始位置
            
        Returns:
            Dict: API レスポンス
        """
        params = {
            'key': self.api_key,
            'format': 'json',
            'count': min(count, 100),  # 最大100件
            'start': start
        }
        
        # 位置情報
        if lat is not None and lng is not None:
            params['lat'] = lat
            params['lng'] = lng
            params['range'] = range_km
            
        # キーワード
        if keyword:
            params['keyword'] = keyword
            
        # ジャンル
        if genre:
            params['genre'] = genre
            
        try:
            logger.info(f"API リクエスト: start={start}, count={count}")
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'results' not in data:
                logger.error(f"API エラー: {data}")
                return {'results': {'shop': []}}
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API リクエストエラー: {e}")
            return {'results': {'shop': []}}
        except json.JSONDecodeError as e:
            logger.error(f"JSON デコードエラー: {e}")
            return {'results': {'shop': []}}
            
    def get_all_shops(self,
                     lat: Optional[float] = None,
                     lng: Optional[float] = None,
                     range_km: int = 3,
                     keyword: Optional[str] = None,
                     genre: Optional[str] = None,
                     max_count: int = 1000) -> List[Dict]:
        """
        全店舗取得（ページネーション対応）
        
        Args:
            lat (float, optional): 緯度
            lng (float, optional): 経度
            range_km (int): 検索範囲
            keyword (str, optional): キーワード
            genre (str, optional): ジャンル
            max_count (int): 最大取得件数
            
        Returns:
            List[Dict]: 店舗リスト
        """
        all_shops = []
        start = 1
        
        while len(all_shops) < max_count:
            # 残り取得件数を計算
            remaining = max_count - len(all_shops)
            count = min(100, remaining)
            
            # API 呼び出し
            data = self.search_shops(
                lat=lat,
                lng=lng,
                range_km=range_km,
                keyword=keyword,
                genre=genre,
                count=count,
                start=start
            )
            
            shops = data.get('results', {}).get('shop', [])
            
            if not shops:
                logger.info("これ以上の店舗データがありません")
                break
                
            all_shops.extend(shops)
            logger.info(f"取得済み店舗数: {len(all_shops)}")
            
            # 次のページへ
            start += count
            
            # API レート制限対応（1秒待機）
            time.sleep(1)
            
        return all_shops[:max_count]
    
    def extract_shop_info(self, shops: List[Dict]) -> List[Dict]:
        """
        店舗情報から必要な情報を抽出
        
        Args:
            shops (List[Dict]): 店舗リスト
            
        Returns:
            List[Dict]: 抽出された店舗情報
        """
        extracted_shops = []
        
        for shop in shops:
            try:
                shop_info = {
                    'shop_name': shop.get('name', ''),
                    'phone': shop.get('ktai_tel', '') or shop.get('tel', ''),  # 携帯電話番号を優先
                    'address': shop.get('address', ''),
                    'genre': shop.get('genre', {}).get('name', ''),
                    'station': shop.get('station_name', ''),
                    'access': shop.get('access', ''),
                    'open_time': shop.get('open', ''),
                    'close_time': shop.get('close', ''),
                    'budget': shop.get('budget', {}).get('name', ''),
                    'url': shop.get('urls', {}).get('pc', ''),
                    'source': 'ホットペッパーグルメ'
                }
                
                # 必須項目チェック
                if shop_info['shop_name'] and shop_info['address']:
                    extracted_shops.append(shop_info)
                    
            except Exception as e:
                logger.error(f"店舗情報抽出エラー: {e}")
                continue
                
        return extracted_shops

# 地域別検索用の座標データ
AREA_COORDINATES = {
    '東京都心': {'lat': 35.6762, 'lng': 139.6503},
    '新宿': {'lat': 35.6896, 'lng': 139.7006},
    '渋谷': {'lat': 35.6598, 'lng': 139.7006},
    '池袋': {'lat': 35.7295, 'lng': 139.7109},
    '銀座': {'lat': 35.6762, 'lng': 139.7649},
    '大阪梅田': {'lat': 34.7024, 'lng': 135.4959},
    '大阪難波': {'lat': 34.6661, 'lng': 135.5000},
    '名古屋駅': {'lat': 35.1706, 'lng': 136.8816},
    '横浜駅': {'lat': 35.4657, 'lng': 139.6201},
    '福岡天神': {'lat': 33.5904, 'lng': 130.4017}
}

def test_hotpepper_api():
    """ホットペッパーAPIのテスト"""
    # テスト用のダミーAPIキー（実際の使用時は有効なAPIキーが必要）
    api_key = "YOUR_API_KEY_HERE"
    
    if api_key == "YOUR_API_KEY_HERE":
        print("⚠️  実際のAPIキーを設定してください")
        print("リクルートWEBサービス（https://webservice.recruit.co.jp/）でAPIキーを取得してください")
        return
    
    client = HotpepperAPIClient(api_key)
    
    # 東京都心で居酒屋を検索
    print("🔍 ホットペッパーAPI テスト開始")
    coords = AREA_COORDINATES['東京都心']
    
    shops = client.get_all_shops(
        lat=coords['lat'],
        lng=coords['lng'],
        range_km=3,
        keyword="居酒屋",
        max_count=10
    )
    
    if shops:
        extracted = client.extract_shop_info(shops)
        print(f"✅ {len(extracted)}件の店舗情報を取得しました")
        
        for i, shop in enumerate(extracted[:3], 1):
            print(f"\n{i}. {shop['shop_name']}")
            print(f"   電話: {shop['phone']}")
            print(f"   住所: {shop['address']}")
            print(f"   ジャンル: {shop['genre']}")
    else:
        print("❌ 店舗情報を取得できませんでした")

if __name__ == "__main__":
    test_hotpepper_api()

