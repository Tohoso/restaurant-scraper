#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Restaurant Scraper メインエントリーポイント
リファクタリング済みの統合版
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import get_settings
from src.scrapers.tabelog import TabelogScraper
from src.scrapers.hotpepper import HotPepperScraper
from restaurant_data_integrator import RestaurantDataIntegrator

# ロギング設定
logger = logging.getLogger(__name__)


def setup_logging(settings):
    """ロギングを設定"""
    log_config = settings.get_logging_config()
    logging.basicConfig(**log_config)
    logger.info("ロギング設定完了")


def parse_arguments():
    """コマンドライン引数を解析"""
    parser = argparse.ArgumentParser(
        description='食べログ・ホットペッパーから飲食店情報を収集します'
    )
    
    parser.add_argument(
        '--areas',
        nargs='+',
        help='収集するエリア名（例: 銀座・新橋・有楽町 渋谷）'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='収集する最大件数（デフォルト: 100）'
    )
    
    parser.add_argument(
        '--pages',
        type=int,
        default=10,
        help='エリアごとの最大ページ数（デフォルト: 10）'
    )
    
    parser.add_argument(
        '--concurrent',
        type=int,
        default=10,
        help='最大同時接続数（デフォルト: 10）'
    )
    
    parser.add_argument(
        '--delay-min',
        type=float,
        default=1.0,
        help='最小遅延秒数（デフォルト: 1.0）'
    )
    
    parser.add_argument(
        '--delay-max',
        type=float,
        default=3.0,
        help='最大遅延秒数（デフォルト: 3.0）'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='設定ファイルのパス'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='出力ファイル名（拡張子なし）'
    )
    
    parser.add_argument(
        '--no-excel',
        action='store_true',
        help='Excel出力をスキップ'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='デバッグモードを有効化'
    )
    
    parser.add_argument(
        '--source',
        choices=['tabelog', 'hotpepper', 'both'],
        default='tabelog',
        help='データソース（デフォルト: tabelog）'
    )
    
    parser.add_argument(
        '--hotpepper-key',
        type=str,
        help='ホットペッパーAPIキー'
    )
    
    parser.add_argument(
        '--keyword',
        type=str,
        help='検索キーワード（ホットペッパー用）'
    )
    
    return parser.parse_args()


def create_excel_report(results, output_path):
    """Excel形式のレポートを作成"""
    if not results:
        logger.warning("出力するデータがありません")
        return
    
    df = pd.DataFrame(results)
    
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
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='飲食店リスト', index=False)
        
        # カラム幅を自動調整
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
    
    logger.info(f"Excelファイル作成: {output_path}")


def print_statistics(results):
    """統計情報を表示"""
    if not results:
        return
    
    total = len(results)
    with_phone = sum(1 for r in results if r.get('phone'))
    with_address = sum(1 for r in results if r.get('address'))
    with_genre = sum(1 for r in results if r.get('genre'))
    with_station = sum(1 for r in results if r.get('station'))
    with_seats = sum(1 for r in results if r.get('seats'))
    with_official = sum(1 for r in results if r.get('official_url'))
    with_reviews = sum(1 for r in results if r.get('review_count') and r['review_count'] != '0')
    
    # ソース別統計
    tabelog_count = sum(1 for r in results if r.get('source') == '食べログ')
    hotpepper_count = sum(1 for r in results if r.get('source') == 'ホットペッパーグルメ')
    
    print("\n" + "="*50)
    print("データ品質統計")
    print("="*50)
    print(f"総件数: {total}件")
    if tabelog_count > 0:
        print(f"  食べログ: {tabelog_count}件")
    if hotpepper_count > 0:
        print(f"  ホットペッパー: {hotpepper_count}件")
    print(f"電話番号: {with_phone}件 ({with_phone/total*100:.1f}%)")
    print(f"住所: {with_address}件 ({with_address/total*100:.1f}%)")
    print(f"ジャンル: {with_genre}件 ({with_genre/total*100:.1f}%)")
    print(f"最寄り駅: {with_station}件 ({with_station/total*100:.1f}%)")
    print(f"席数: {with_seats}件 ({with_seats/total*100:.1f}%)")
    print(f"公式URL: {with_official}件 ({with_official/total*100:.1f}%)")
    print(f"口コミ: {with_reviews}件 ({with_reviews/total*100:.1f}%)")
    print("="*50)


async def main():
    """メイン処理"""
    args = parse_arguments()
    
    # 設定を初期化
    settings = get_settings(args.config)
    
    # コマンドライン引数で設定を上書き
    if args.concurrent:
        settings.set('max_concurrent', args.concurrent)
    if args.delay_min:
        settings.set('delay_min', args.delay_min)
    if args.delay_max:
        settings.set('delay_max', args.delay_max)
    if args.debug:
        settings.set('log_level', 'DEBUG')
    
    # ロギング設定
    setup_logging(settings)
    
    logger.info("Restaurant Scraper 起動")
    logger.info(f"設定: {settings.config}")
    logger.info(f"データソース: {args.source}")
    
    results = []
    
    # スクレイパーを初期化
    scraper_config = settings.get_scraper_config()
    
    # 食べログから取得
    if args.source in ['tabelog', 'both']:
        logger.info("\n=== 食べログから取得開始 ===")
        async with TabelogScraper(**scraper_config) as scraper:
            # スクレイピング実行
            tabelog_results = await scraper.scrape(
                areas=args.areas,
                max_pages_per_area=args.pages,
                total_limit=args.limit if args.source == 'tabelog' else args.limit // 2
            )
            results.extend(tabelog_results)
    
    # ホットペッパーから取得
    if args.source in ['hotpepper', 'both']:
        # APIキーの確認
        api_key = args.hotpepper_key or settings.get('hotpepper_api_key')
        if not api_key or api_key == 'YOUR_API_KEY_HERE':
            logger.warning("ホットペッパーAPIキーが設定されていません")
            if args.source == 'hotpepper':
                logger.error("ホットペッパーのみ指定時はAPIキーが必須です")
                return
        else:
            logger.info("\n=== ホットペッパーから取得開始 ===")
            async with HotPepperScraper(api_key=api_key, **scraper_config) as scraper:
                # スクレイピング実行
                hp_results = await scraper.scrape(
                    areas=args.areas,
                    max_per_area=args.pages * 20,  # 1ページ20件相当
                    total_limit=args.limit if args.source == 'hotpepper' else args.limit // 2,
                    keyword=args.keyword
                )
                results.extend(hp_results)
    
    if not results:
        logger.warning("データが取得できませんでした")
        return
    
    # 重複除去（両方から取得した場合）
    if args.source == 'both':
        # RestaurantDataIntegratorを使用して重複を除去
        integrator = RestaurantDataIntegrator()
        integrator.add_restaurants(results)
        integrator.remove_duplicates()
        results = integrator.restaurants
        logger.info(f"重複除去後: {len(results)}件")
    
    logger.info(f"取得完了: {len(results)}件")
    
    # Excel出力
    if not args.no_excel:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if args.output:
            output_filename = f"{args.output}_{len(results)}件_{timestamp}.xlsx"
        else:
            output_filename = f"飲食店リスト_{len(results)}件_{timestamp}.xlsx"
        
        output_path = Path(settings.get('output_dir')) / output_filename
        create_excel_report(results, output_path)
    
    # 統計表示
    print_statistics(results)
    
    logger.info("処理完了")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("ユーザーによって中断されました")
        sys.exit(0)
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        sys.exit(1)