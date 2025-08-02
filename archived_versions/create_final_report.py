#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
収集済みデータから最終レポートを作成
"""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from restaurant_data_integrator import RestaurantDataIntegrator

def create_final_report():
    """最終レポートを作成"""
    
    # キャッシュファイルを読み込み
    cache_dir = Path("cache")
    results_file = cache_dir / "partial_results_v2.json"
    
    if not results_file.exists():
        print("エラー: 結果ファイルが見つかりません")
        return
    
    # 結果を読み込み
    with open(results_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    print(f"✅ {len(results)}件のデータを読み込みました")
    
    # データ統合
    integrator = RestaurantDataIntegrator()
    integrator.add_restaurants(results)
    
    # 重複除去
    integrator.remove_duplicates()
    
    # データ品質統計を手動で計算
    total_count = len(integrator.restaurants)
    with_phone = sum(1 for r in integrator.restaurants if r.get('phone'))
    with_address = sum(1 for r in integrator.restaurants if r.get('address'))
    with_genre = sum(1 for r in integrator.restaurants if r.get('genre'))
    with_hours = sum(1 for r in integrator.restaurants if r.get('open_time'))
    
    print("\n📊 データ品質統計:")
    print(f"  - 総件数: {total_count}件")
    print(f"  - 電話番号あり: {with_phone}件 ({with_phone/total_count*100:.1f}%)")
    print(f"  - 住所あり: {with_address}件 ({with_address/total_count*100:.1f}%)")
    print(f"  - ジャンルあり: {with_genre}件 ({with_genre/total_count*100:.1f}%)")
    print(f"  - 営業時間あり: {with_hours}件 ({with_hours/total_count*100:.1f}%)")
    
    # Excelファイル作成
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'東京都_飲食店リスト_{len(results)}件_{timestamp}.xlsx'
    
    # 出力ディレクトリを作成
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / output_file
    
    integrator.create_excel_report(str(output_path))
    print(f"\n✅ Excelファイルを作成しました: {output_path}")
    
    # サンプルデータを表示
    print("\n📝 データサンプル（最初の5件）:")
    df = pd.DataFrame(results[:5])
    print(df[['shop_name', 'phone', 'genre', 'address']].to_string(index=False))

if __name__ == "__main__":
    create_final_report()