#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
既存データの統合と報告
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def combine_all_data():
    """すべての既存データを統合"""
    all_data = []
    seen_urls = set()
    
    # キャッシュファイルから読み込み
    cache_files = [
        'cache/partial_results_v2.json',
        'cache/async_results.json', 
        'cache/partial_results.json'
    ]
    
    for file_path in cache_files:
        if Path(file_path).exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            if item.get('url') and item['url'] not in seen_urls:
                                all_data.append(item)
                                seen_urls.add(item['url'])
                        logger.info(f"{file_path}: {len(data)}件読み込み")
            except Exception as e:
                logger.error(f"{file_path}の読み込みエラー: {e}")
    
    # JSONファイルから読み込み
    json_pattern = "output/restaurant_list*.json"
    for json_file in Path().glob(json_pattern):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        if item.get('url') and item['url'] not in seen_urls:
                            all_data.append(item)
                            seen_urls.add(item['url'])
                    logger.info(f"{json_file}: {len(data)}件読み込み")
        except Exception as e:
            logger.error(f"{json_file}の読み込みエラー: {e}")
    
    logger.info(f"\n統合結果: 合計{len(all_data)}件（重複除去後）")
    
    # データ品質の分析
    with_phone = sum(1 for d in all_data if d.get('phone'))
    with_address = sum(1 for d in all_data if d.get('address'))
    with_genre = sum(1 for d in all_data if d.get('genre'))
    with_station = sum(1 for d in all_data if d.get('station'))
    with_hours = sum(1 for d in all_data if d.get('open_time'))
    
    logger.info("\n📊 データ品質分析:")
    logger.info(f"  電話番号あり: {with_phone}件 ({with_phone/len(all_data)*100:.1f}%)")
    logger.info(f"  住所あり: {with_address}件 ({with_address/len(all_data)*100:.1f}%)")
    logger.info(f"  ジャンルあり: {with_genre}件 ({with_genre/len(all_data)*100:.1f}%)")
    logger.info(f"  最寄り駅あり: {with_station}件 ({with_station/len(all_data)*100:.1f}%)")
    logger.info(f"  営業時間あり: {with_hours}件 ({with_hours/len(all_data)*100:.1f}%)")
    
    # Excel出力
    from restaurant_data_integrator import RestaurantDataIntegrator
    integrator = RestaurantDataIntegrator()
    integrator.add_restaurants(all_data)
    integrator.remove_duplicates()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'output/東京都_飲食店リスト_統合_{len(all_data)}件_{timestamp}.xlsx'
    integrator.create_excel_report(output_file)
    
    logger.info(f"\n✅ 統合Excelファイル作成完了: {output_file}")
    
    return all_data

if __name__ == "__main__":
    combine_all_data()