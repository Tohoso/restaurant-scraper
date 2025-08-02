#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
共通定数の定義
"""

from typing import Dict, List

# 地域別URL
AREA_URLS: Dict[str, str] = {
    '東京都': 'https://tabelog.com/tokyo/',
    '大阪府': 'https://tabelog.com/osaka/',
    '神奈川県': 'https://tabelog.com/kanagawa/',
    '愛知県': 'https://tabelog.com/aichi/',
    '福岡県': 'https://tabelog.com/fukuoka/',
    '北海道': 'https://tabelog.com/hokkaido/',
    '京都府': 'https://tabelog.com/kyoto/',
    '兵庫県': 'https://tabelog.com/hyogo/',
    '埼玉県': 'https://tabelog.com/saitama/',
    '千葉県': 'https://tabelog.com/chiba/'
}

# User-Agent設定
USER_AGENTS: Dict[str, str] = {
    'default': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'windows': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# 電話番号パターン
PHONE_PATTERNS: List[str] = [
    r'03-\d{4}-\d{4}',
    r'0\d{2,3}-\d{3,4}-\d{4}',
    r'0\d{9,10}',
    r'\d{2,4}-\d{2,4}-\d{4}'
]

# セレクタ定義
SELECTORS: Dict[str, List[str]] = {
    'shop_name': [
        'h2.display-name span',
        'h2.display-name',
        'h1.display-name span',
        'h1.display-name',
        'div.rdheader-info__name span',
        'div.rdheader-info__name',
        '.rstinfo-table__name',
        'h1',
        '.rst-name',
        '.shop-name'
    ],
    'restaurant_links': [
        'a.list-rst__rst-name-target',
        'a.js-restaurant-link',
        'h3.list-rst__rst-name a',
        'div.list-rst__rst-name a',
        'a[href*="/A"][href*="/A"][href*="/"]',
        '.list-rst__rst-name a',
        '.rst-name a'
    ],
    'address': [
        'p.rstinfo-table__address',
        'span.rstinfo-table__address',
        'td:contains("住所") + td',
        'th:contains("住所") + td',
        'div.rstinfo-table__address',
        'p.rstdtl-side-address__text',
        '.rdheader-subinfo__item-text'
    ],
    'phone': [
        'span.rstinfo-table__tel-num',
        'p.rstinfo-table__tel',
        'td:contains("電話番号") + td',
        'th:contains("電話番号") + td',
        'div.rstinfo-table__tel',
        '.rdheader-subinfo__item--tel'
    ],
    'genre': [
        'td:contains("ジャンル") + td',
        'th:contains("ジャンル") + td',
        '.rstinfo-table__genre',
        'span[property="v:category"]'
    ],
    'station': [
        'td:contains("交通手段") + td',
        'th:contains("交通手段") + td',
        'td:contains("最寄り駅") + td',
        'th:contains("最寄り駅") + td',
        '.rstinfo-table__access',
        'dl.rdheader-subinfo__item--station',
        'span.linktree__parent-target-text:contains("駅")'
    ],
    'open_time': [
        'td:contains("営業時間") + td',
        'th:contains("営業時間") + td',
        'p.rstinfo-table__open-hours',
        '.rstinfo-table__open-hours',
        'dl.rdheader-subinfo__item--open-hours'
    ]
}

# 料理ジャンルキーワード
GENRE_KEYWORDS: List[str] = [
    '料理', '焼', '鍋', '寿司', '鮨', 'そば', 'うどん', 'ラーメン',
    'カレー', 'イタリアン', 'フレンチ', '中華', '和食', '洋食',
    'カフェ', 'バー', '居酒屋', '食堂', 'レストラン', '肉', '魚',
    '野菜', '串', '天ぷら', '丼', '定食', 'ステーキ', 'ハンバーグ',
    'ビストロ', 'ダイニング', '韓国', 'タイ', 'ベトナム', 'インド',
    'スペイン', 'メキシコ', '沖縄', '餃子', 'ホルモン', '焼肉'
]

# デフォルト設定
DEFAULT_CONFIG = {
    'max_concurrent': 10,
    'delay_range': (0.5, 1.0),
    'timeout': 30,
    'max_retries': 3,
    'batch_size': 20,
    'progress_save_interval': 10,
    'cache_dir': 'cache',
    'output_dir': 'output',
    'log_dir': 'logs'
}