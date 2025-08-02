#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
飲食店営業リスト作成アプリ
食べログとホットペッパーから店名・電話番号・住所を取得してExcelファイルに出力
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from typing import List, Dict

# 自作モジュールのインポート
from hotpepper_api_client import HotpepperAPIClient
from tabelog_scraper_improved import TabelogScraperImproved
from restaurant_data_integrator import RestaurantDataIntegrator

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('restaurant_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RestaurantScraperApp:
    """飲食店スクレイピングアプリケーション"""
    
    def __init__(self):
        """初期化"""
        self.hotpepper_client = None  # 必要時に初期化
        self.tabelog_scraper = TabelogScraperImproved()
        self.data_integrator = RestaurantDataIntegrator()
        
    def scrape_restaurants(self, 
                          areas: List[str] = None,
                          max_per_area: int = 100,
                          use_hotpepper: bool = True,
                          use_tabelog: bool = True,
                          hotpepper_api_key: str = None) -> List[Dict]:
        """
        飲食店データを取得
        
        Args:
            areas (List[str]): 対象地域リスト
            max_per_area (int): 地域あたりの最大取得件数
            use_hotpepper (bool): ホットペッパーを使用するか
            use_tabelog (bool): 食べログを使用するか
            hotpepper_api_key (str): ホットペッパーAPIキー
            
        Returns:
            List[Dict]: 取得した飲食店データ
        """
        if not areas:
            areas = ['東京都', '大阪府', '神奈川県']
        
        all_restaurants = []
        
        # ホットペッパーからデータ取得
        if use_hotpepper and hotpepper_api_key:
            logger.info("🌶️ ホットペッパーからデータ取得開始")
            if not self.hotpepper_client:
                self.hotpepper_client = HotpepperAPIClient(hotpepper_api_key)
            else:
                self.hotpepper_client.api_key = hotpepper_api_key
            
            for area in areas:
                try:
                    restaurants = self.hotpepper_client.search_restaurants_by_area(
                        area, max_results=max_per_area
                    )
                    all_restaurants.extend(restaurants)
                    logger.info(f"ホットペッパー {area}: {len(restaurants)}件取得")
                except Exception as e:
                    logger.error(f"ホットペッパー {area} でエラー: {e}")
        
        # 食べログからデータ取得
        if use_tabelog:
            logger.info("🍽️ 食べログからデータ取得開始")
            
            for area in areas:
                try:
                    restaurants = self.tabelog_scraper.scrape_area_restaurants(
                        area, max_restaurants=max_per_area
                    )
                    all_restaurants.extend(restaurants)
                    logger.info(f"食べログ {area}: {len(restaurants)}件取得")
                except Exception as e:
                    logger.error(f"食べログ {area} でエラー: {e}")
        
        logger.info(f"総取得件数: {len(all_restaurants)}件")
        return all_restaurants
    
    def process_and_export(self, 
                          restaurants: List[Dict],
                          output_filename: str = None) -> str:
        """
        データを処理してExcelファイルに出力
        
        Args:
            restaurants (List[Dict]): 飲食店データ
            output_filename (str): 出力ファイル名
            
        Returns:
            str: 作成されたファイルのパス
        """
        logger.info("📊 データ処理開始")
        
        # データ統合処理
        self.data_integrator.add_restaurants(restaurants)
        self.data_integrator.validate_data()
        self.data_integrator.remove_duplicates()
        
        # 統計情報表示
        stats = self.data_integrator.get_statistics()
        logger.info(f"処理結果統計:")
        logger.info(f"  総件数: {stats['total_count']}")
        logger.info(f"  電話番号あり: {stats['with_phone']}")
        logger.info(f"  住所あり: {stats['with_address']}")
        logger.info(f"  食べログ: {stats['tabelog_count']}")
        logger.info(f"  ホットペッパー: {stats['hotpepper_count']}")
        
        # Excelファイル作成
        if not output_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"飲食店営業リスト_{timestamp}.xlsx"
        
        excel_path = self.data_integrator.create_excel_report(output_filename)
        logger.info(f"✅ Excelファイル作成完了: {excel_path}")
        
        return excel_path
    
    def run_interactive_mode(self):
        """対話モードで実行"""
        print("🍽️ 飲食店営業リスト作成アプリ")
        print("=" * 50)
        
        # 地域選択
        available_areas = ['東京都', '大阪府', '神奈川県', '愛知県', '福岡県', 
                          '北海道', '京都府', '兵庫県', '埼玉県', '千葉県']
        
        print("対象地域を選択してください（複数選択可、カンマ区切り）:")
        for i, area in enumerate(available_areas, 1):
            print(f"  {i}. {area}")
        
        area_input = input("地域番号を入力 (例: 1,2,3): ").strip()
        
        selected_areas = []
        try:
            area_indices = [int(x.strip()) - 1 for x in area_input.split(',')]
            selected_areas = [available_areas[i] for i in area_indices 
                            if 0 <= i < len(available_areas)]
        except:
            selected_areas = ['東京都']  # デフォルト
        
        if not selected_areas:
            selected_areas = ['東京都']
        
        print(f"選択された地域: {', '.join(selected_areas)}")
        
        # 取得件数設定
        try:
            max_per_area = int(input("地域あたりの最大取得件数 (デフォルト: 50): ") or "50")
        except:
            max_per_area = 50
        
        # データソース選択
        use_tabelog = input("食べログを使用しますか？ (y/n, デフォルト: y): ").lower() != 'n'
        
        use_hotpepper = False
        hotpepper_api_key = None
        
        if input("ホットペッパーを使用しますか？ (y/n, デフォルト: n): ").lower() == 'y':
            hotpepper_api_key = input("ホットペッパーAPIキーを入力: ").strip()
            if hotpepper_api_key:
                use_hotpepper = True
        
        # 出力ファイル名
        output_filename = input("出力ファイル名 (デフォルト: 自動生成): ").strip()
        if not output_filename:
            output_filename = None
        
        print("\n🚀 データ取得開始...")
        
        # データ取得・処理・出力
        try:
            restaurants = self.scrape_restaurants(
                areas=selected_areas,
                max_per_area=max_per_area,
                use_hotpepper=use_hotpepper,
                use_tabelog=use_tabelog,
                hotpepper_api_key=hotpepper_api_key
            )
            
            if restaurants:
                excel_path = self.process_and_export(restaurants, output_filename)
                print(f"\n✅ 完了！Excelファイルを作成しました: {excel_path}")
            else:
                print("\n❌ データを取得できませんでした")
                
        except Exception as e:
            logger.error(f"実行エラー: {e}")
            print(f"\n❌ エラーが発生しました: {e}")

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='飲食店営業リスト作成アプリ')
    parser.add_argument('--areas', nargs='+', default=['東京都'],
                       help='対象地域 (例: --areas 東京都 大阪府)')
    parser.add_argument('--max-per-area', type=int, default=50,
                       help='地域あたりの最大取得件数')
    parser.add_argument('--output', '-o', help='出力ファイル名')
    parser.add_argument('--hotpepper-key', help='ホットペッパーAPIキー')
    parser.add_argument('--no-tabelog', action='store_true',
                       help='食べログを使用しない')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='対話モードで実行')
    
    args = parser.parse_args()
    
    app = RestaurantScraperApp()
    
    if args.interactive:
        app.run_interactive_mode()
    else:
        # コマンドライン引数で実行
        use_hotpepper = bool(args.hotpepper_key)
        use_tabelog = not args.no_tabelog
        
        if not use_hotpepper and not use_tabelog:
            print("❌ エラー: 少なくとも1つのデータソースを指定してください")
            sys.exit(1)
        
        try:
            restaurants = app.scrape_restaurants(
                areas=args.areas,
                max_per_area=args.max_per_area,
                use_hotpepper=use_hotpepper,
                use_tabelog=use_tabelog,
                hotpepper_api_key=args.hotpepper_key
            )
            
            if restaurants:
                excel_path = app.process_and_export(restaurants, args.output)
                print(f"✅ 完了！Excelファイルを作成しました: {excel_path}")
            else:
                print("❌ データを取得できませんでした")
                
        except Exception as e:
            logger.error(f"実行エラー: {e}")
            print(f"❌ エラーが発生しました: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()

