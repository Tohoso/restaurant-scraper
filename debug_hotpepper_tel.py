#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ホットペッパーAPI電話番号フィールドのデバッグ
"""

from hotpepper_api_client import HotpepperAPIClient, AREA_COORDINATES
import json

def debug_tel():
    api_key = "3e9f658f6ee72cf7"
    client = HotpepperAPIClient(api_key)
    
    print("🔍 電話番号フィールドのデバッグ")
    
    # 複数のエリアでテスト
    test_areas = ['東京都心', '渋谷', '新宿']
    
    for area in test_areas:
        coords = AREA_COORDINATES.get(area)
        if not coords:
            continue
            
        print(f"\n=== {area}エリアのテスト ===")
        
        # 5件取得
        data = client.search_shops(
            lat=coords['lat'],
            lng=coords['lng'],
            range_km=2,
            count=5
        )
        
        shops = data.get('results', {}).get('shop', [])
        
        for i, shop in enumerate(shops, 1):
            print(f"\n{i}. {shop.get('name', 'Unknown')}")
            
            # 全ての電話番号関連フィールドをチェック
            tel_fields = ['tel', 'ktai_tel', 'ktai', 'mobile', 'phone']
            for field in tel_fields:
                if field in shop:
                    print(f"   {field}: {shop[field]}")
            
            # URLsセクション内も確認
            if 'urls' in shop:
                print(f"   urls: {json.dumps(shop['urls'], ensure_ascii=False)}")
            
            # その他の連絡先情報
            if 'ktai_coupon' in shop:
                print(f"   ktai_coupon: {shop['ktai_coupon']}")

if __name__ == "__main__":
    debug_tel()