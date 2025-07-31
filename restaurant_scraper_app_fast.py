#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
高速版飲食店営業リスト作成アプリ
大量データ取得に対応した非同期処理版
"""

import sys
import os
import argparse
import asyncio
import logging
from datetime import datetime
from typing import List, Dict
import signal
import json
from pathlib import Path

# 自作モジュールのインポート
from hotpepper_api_client import HotpepperAPIClient
from tabelog_scraper_async import TabelogScraperAsync
from restaurant_data_integrator import RestaurantDataIntegrator

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('restaurant_scraper_fast.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RestaurantScraperAppFast:
    """高速版飲食店スクレイピングアプリケーション"""
    
    def __init__(self):
        """初期化"""
        self.hotpepper_client = None
        self.data_integrator = RestaurantDataIntegrator()
        self.interrupted = False
        self.progress_data = {}
        
        # シグナルハンドラーを設定（Ctrl+C対応）
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """割り込みシグナルハンドラー"""
        logger.info("\n⚠️ 処理を中断しています... 進捗を保存します")
        self.interrupted = True
        
    async def scrape_restaurants_async(self, 
                                     areas: List[str] = None,
                                     max_per_area: int = 100,
                                     use_hotpepper: bool = True,
                                     use_tabelog: bool = True,
                                     hotpepper_api_key: str = None,
                                     max_concurrent: int = 10) -> List[Dict]:
        """
        非同期で飲食店データを取得
        
        Args:
            areas: 対象地域リスト
            max_per_area: 地域あたりの最大取得件数
            use_hotpepper: ホットペッパーAPIを使用するか
            use_tabelog: 食べログを使用するか
            hotpepper_api_key: ホットペッパーAPIキー
            max_concurrent: 最大同時接続数
            
        Returns:
            List[Dict]: 飲食店データリスト
        """
        all_restaurants = []
        start_time = datetime.now()
        
        # デフォルト地域
        if not areas:
            areas = ['東京都']
        
        try:
            # 食べログからデータ取得
            if use_tabelog and not self.interrupted:
                logger.info("🍽️ 食べログからデータ取得開始（非同期モード）")
                
                async with TabelogScraperAsync(max_concurrent=max_concurrent) as scraper:
                    tabelog_data = await scraper.scrape_restaurants_batch(areas, max_per_area)
                    all_restaurants.extend(tabelog_data)
                    
                    logger.info(f"✅ 食べログから {len(tabelog_data)} 件取得完了")
                    
                    # 進捗を保存
                    self.progress_data['tabelog_completed'] = True
                    self.progress_data['tabelog_count'] = len(tabelog_data)
                    self._save_progress()
            
            # ホットペッパーからデータ取得
            if use_hotpepper and hotpepper_api_key and not self.interrupted:
                logger.info("🍽️ ホットペッパーからデータ取得開始")
                
                if not self.hotpepper_client:
                    self.hotpepper_client = HotpepperAPIClient(hotpepper_api_key)
                
                for area in areas:
                    if self.interrupted:
                        break
                        
                    logger.info(f"  {area}のデータ取得中...")
                    hotpepper_data = self.hotpepper_client.search_restaurants(
                        keyword=area,
                        count=max_per_area
                    )
                    all_restaurants.extend(hotpepper_data)
                    logger.info(f"  {area}: {len(hotpepper_data)}件取得")
                
                logger.info(f"✅ ホットペッパーから合計 {len(hotpepper_data)} 件取得完了")
                
                # 進捗を保存
                self.progress_data['hotpepper_completed'] = True
                self._save_progress()
            
            # データ統合
            if all_restaurants and not self.interrupted:
                logger.info("🔄 データ統合処理中...")
                # データを追加して重複を削除
                self.data_integrator.add_restaurants(all_restaurants)
                self.data_integrator.remove_duplicates()
                integrated_data = self.data_integrator.restaurants
                
                # 統計情報
                elapsed_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"\n📊 処理完了統計:")
                logger.info(f"  総取得件数: {len(all_restaurants)}件")
                logger.info(f"  統合後件数: {len(integrated_data)}件")
                logger.info(f"  処理時間: {elapsed_time:.1f}秒")
                logger.info(f"  処理速度: {len(all_restaurants)/elapsed_time:.1f}件/秒")
                
                return integrated_data
            
            return all_restaurants
            
        except Exception as e:
            logger.error(f"エラーが発生しました: {e}")
            if all_restaurants:
                logger.info(f"エラー発生前に {len(all_restaurants)} 件のデータを取得済み")
            return all_restaurants
    
    def _save_progress(self):
        """進捗を保存"""
        progress_file = Path("cache/app_progress.json")
        progress_file.parent.mkdir(exist_ok=True)
        
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
    
    def save_to_excel_with_progress(self, restaurants: List[Dict], output_file: str = None):
        """
        進捗を表示しながらExcelファイルに保存
        """
        if not restaurants:
            logger.warning("保存するデータがありません")
            return
        
        # デフォルトファイル名
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'restaurant_list_{timestamp}.xlsx'
        
        # outputディレクトリに保存
        output_path = Path("output") / output_file
        output_path.parent.mkdir(exist_ok=True)
        
        try:
            logger.info(f"📝 Excelファイル作成中: {output_path}")
            
            # データ統合モジュールのExcel出力機能を使用
            self.data_integrator.restaurants = restaurants
            self.data_integrator.create_excel_report(str(output_path))
            
            logger.info(f"✅ Excelファイル保存完了: {output_path}")
            logger.info(f"   ファイルサイズ: {output_path.stat().st_size / 1024:.1f} KB")
            
        except Exception as e:
            logger.error(f"Excel保存エラー: {e}")
            # 緊急バックアップとしてJSONで保存
            backup_file = output_path.with_suffix('.json')
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(restaurants, f, ensure_ascii=False, indent=2)
            logger.info(f"📄 バックアップファイル保存: {backup_file}")
    
    def run_interactive_mode(self):
        """対話モードで実行"""
        print("\n🍽️ 飲食店営業リスト作成アプリ（高速版）")
        print("=" * 50)
        
        # 地域選択
        print("\n対象地域を選択してください（複数選択可）:")
        print("1. 東京都")
        print("2. 大阪府")
        print("3. 神奈川県")
        print("4. 愛知県")
        print("5. 福岡県")
        print("6. 北海道")
        print("7. 京都府")
        print("8. 兵庫県")
        print("9. 埼玉県")
        print("10. 千葉県")
        
        area_map = {
            '1': '東京都', '2': '大阪府', '3': '神奈川県',
            '4': '愛知県', '5': '福岡県', '6': '北海道',
            '7': '京都府', '8': '兵庫県', '9': '埼玉県',
            '10': '千葉県'
        }
        
        selected_areas = []
        while True:
            choice = input("\n番号を入力（複数の場合はカンマ区切り、終了はEnter）: ").strip()
            if not choice:
                break
                
            for num in choice.split(','):
                num = num.strip()
                if num in area_map:
                    area = area_map[num]
                    if area not in selected_areas:
                        selected_areas.append(area)
                        print(f"  ✓ {area}を追加")
        
        if not selected_areas:
            selected_areas = ['東京都']
            print("  → デフォルト: 東京都")
        
        # 取得件数
        while True:
            count_str = input("\n地域あたりの取得件数を入力（デフォルト: 100）: ").strip()
            if not count_str:
                max_per_area = 100
                break
            try:
                max_per_area = int(count_str)
                if max_per_area > 0:
                    break
                else:
                    print("  ❌ 1以上の数値を入力してください")
            except ValueError:
                print("  ❌ 数値を入力してください")
        
        # 同時接続数
        while True:
            concurrent_str = input("\n同時接続数を入力（デフォルト: 10、最大: 50）: ").strip()
            if not concurrent_str:
                max_concurrent = 10
                break
            try:
                max_concurrent = int(concurrent_str)
                if 1 <= max_concurrent <= 50:
                    break
                else:
                    print("  ❌ 1-50の範囲で入力してください")
            except ValueError:
                print("  ❌ 数値を入力してください")
        
        # ホットペッパーAPI
        use_hotpepper = input("\nホットペッパーAPIを使用しますか？（y/N）: ").strip().lower() == 'y'
        hotpepper_key = None
        if use_hotpepper:
            hotpepper_key = input("APIキーを入力: ").strip()
            if not hotpepper_key:
                use_hotpepper = False
                print("  → APIキーが入力されていないため、ホットペッパーは使用しません")
        
        # 出力ファイル名
        output_file = input("\n出力ファイル名（デフォルト: restaurant_list_[日時].xlsx）: ").strip()
        
        # 実行確認
        print("\n" + "=" * 50)
        print("実行内容:")
        print(f"  対象地域: {', '.join(selected_areas)}")
        print(f"  取得件数: 各地域 {max_per_area} 件")
        print(f"  同時接続数: {max_concurrent}")
        print(f"  データソース: 食べログ" + (", ホットペッパー" if use_hotpepper else ""))
        print("=" * 50)
        
        if input("\n実行しますか？（Y/n）: ").strip().lower() != 'n':
            print("\n処理を開始します... (Ctrl+Cで中断可能)")
            
            # 非同期処理を実行
            restaurants = asyncio.run(
                self.scrape_restaurants_async(
                    areas=selected_areas,
                    max_per_area=max_per_area,
                    use_hotpepper=use_hotpepper,
                    hotpepper_api_key=hotpepper_key,
                    max_concurrent=max_concurrent
                )
            )
            
            if restaurants:
                self.save_to_excel_with_progress(restaurants, output_file)
                print(f"\n✅ 処理が完了しました！")
                print(f"   取得件数: {len(restaurants)}件")
            else:
                print("\n❌ データを取得できませんでした")

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description='高速版飲食店営業リスト作成アプリ',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 対話モード
  python restaurant_scraper_app_fast.py -i
  
  # 東京都から1000件取得（高速）
  python restaurant_scraper_app_fast.py --areas 東京都 --max-per-area 1000 --concurrent 20
  
  # 複数地域から取得
  python restaurant_scraper_app_fast.py --areas 東京都 大阪府 神奈川県 --max-per-area 500
        """
    )
    
    parser.add_argument('--areas', nargs='+', help='対象地域 (例: --areas 東京都 大阪府)')
    parser.add_argument('--max-per-area', type=int, default=100, help='地域あたりの最大取得件数')
    parser.add_argument('--output', '-o', help='出力ファイル名')
    parser.add_argument('--hotpepper-key', help='ホットペッパーAPIキー')
    parser.add_argument('--no-tabelog', action='store_true', help='食べログを使用しない')
    parser.add_argument('--concurrent', type=int, default=10, help='最大同時接続数（1-50）')
    parser.add_argument('--interactive', '-i', action='store_true', help='対話モードで実行')
    
    args = parser.parse_args()
    
    # アプリケーション実行
    app = RestaurantScraperAppFast()
    
    if args.interactive:
        app.run_interactive_mode()
    else:
        # コマンドライン実行
        restaurants = asyncio.run(
            app.scrape_restaurants_async(
                areas=args.areas,
                max_per_area=args.max_per_area,
                use_hotpepper=not args.no_tabelog,
                use_tabelog=not args.no_tabelog,
                hotpepper_api_key=args.hotpepper_key,
                max_concurrent=min(max(args.concurrent, 1), 50)
            )
        )
        
        if restaurants:
            app.save_to_excel_with_progress(restaurants, args.output)

if __name__ == '__main__':
    main()