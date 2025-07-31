#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
高速版飲食店営業リスト作成アプリ V2
改良版スクレイパーで全ての情報を正確に取得
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
from tabelog_scraper_async_v2 import TabelogScraperAsyncV2
from restaurant_data_integrator import RestaurantDataIntegrator

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('restaurant_scraper_fast_v2.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RestaurantScraperAppFastV2:
    """高速版飲食店スクレイピングアプリケーション V2"""
    
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
                logger.info("🍽️ 食べログからデータ取得開始（改良版V2）")
                
                async with TabelogScraperAsyncV2(max_concurrent=max_concurrent) as scraper:
                    tabelog_data = await scraper.scrape_restaurants_batch(areas, max_per_area)
                    all_restaurants.extend(tabelog_data)
                    
                    logger.info(f"✅ 食べログから {len(tabelog_data)} 件取得完了")
                    
                    # データ品質チェック
                    valid_count = sum(1 for r in tabelog_data if r.get('address') and r.get('phone'))
                    logger.info(f"📊 データ品質: {valid_count}/{len(tabelog_data)} 件が住所・電話番号あり")
                    
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
                
                # データ品質統計
                stats = self._calculate_data_quality_stats(integrated_data)
                logger.info(f"\n📈 データ品質統計:")
                logger.info(f"  電話番号あり: {stats['phone_count']}/{stats['total']} ({stats['phone_rate']:.1f}%)")
                logger.info(f"  住所あり: {stats['address_count']}/{stats['total']} ({stats['address_rate']:.1f}%)")
                logger.info(f"  ジャンルあり: {stats['genre_count']}/{stats['total']} ({stats['genre_rate']:.1f}%)")
                logger.info(f"  最寄り駅あり: {stats['station_count']}/{stats['total']} ({stats['station_rate']:.1f}%)")
                logger.info(f"  営業時間あり: {stats['hours_count']}/{stats['total']} ({stats['hours_rate']:.1f}%)")
                
                return integrated_data
            
            return all_restaurants
            
        except Exception as e:
            logger.error(f"エラーが発生しました: {e}")
            if all_restaurants:
                logger.info(f"エラー発生前に {len(all_restaurants)} 件のデータを取得済み")
            return all_restaurants
    
    def _calculate_data_quality_stats(self, restaurants: List[Dict]) -> Dict:
        """データ品質統計を計算"""
        total = len(restaurants)
        if total == 0:
            return {
                'total': 0,
                'phone_count': 0, 'phone_rate': 0,
                'address_count': 0, 'address_rate': 0,
                'genre_count': 0, 'genre_rate': 0,
                'station_count': 0, 'station_rate': 0,
                'hours_count': 0, 'hours_rate': 0
            }
        
        phone_count = sum(1 for r in restaurants if r.get('phone'))
        address_count = sum(1 for r in restaurants if r.get('address'))
        genre_count = sum(1 for r in restaurants if r.get('genre'))
        station_count = sum(1 for r in restaurants if r.get('station'))
        hours_count = sum(1 for r in restaurants if r.get('open_time'))
        
        return {
            'total': total,
            'phone_count': phone_count,
            'phone_rate': (phone_count / total) * 100,
            'address_count': address_count,
            'address_rate': (address_count / total) * 100,
            'genre_count': genre_count,
            'genre_rate': (genre_count / total) * 100,
            'station_count': station_count,
            'station_rate': (station_count / total) * 100,
            'hours_count': hours_count,
            'hours_rate': (hours_count / total) * 100
        }
    
    def _save_progress(self):
        """進捗を保存"""
        progress_file = Path("cache/app_progress_v2.json")
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
            output_file = f'restaurant_list_v2_{timestamp}.xlsx'
        
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

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description='高速版飲食店営業リスト作成アプリ V2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 東京都から100件取得（全情報を正確に取得）
  python restaurant_scraper_app_fast_v2.py --areas 東京都 --max-per-area 100
  
  # 高速設定で大量取得
  python restaurant_scraper_app_fast_v2.py --areas 東京都 --max-per-area 1000 --concurrent 20
        """
    )
    
    parser.add_argument('--areas', nargs='+', help='対象地域 (例: --areas 東京都 大阪府)')
    parser.add_argument('--max-per-area', type=int, default=100, help='地域あたりの最大取得件数')
    parser.add_argument('--output', '-o', help='出力ファイル名')
    parser.add_argument('--hotpepper-key', help='ホットペッパーAPIキー')
    parser.add_argument('--no-tabelog', action='store_true', help='食べログを使用しない')
    parser.add_argument('--concurrent', type=int, default=10, help='最大同時接続数（1-50）')
    
    args = parser.parse_args()
    
    # アプリケーション実行
    app = RestaurantScraperAppFastV2()
    
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