#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
食べログ専用データ抽出クラス
食べログのHTML構造に特化した抽出ロジック
"""

import re
from typing import Dict, Optional
from .base import BaseExtractor
import logging

logger = logging.getLogger(__name__)


class TabelogExtractor(BaseExtractor):
    """食べログ専用データ抽出クラス"""
    
    # 食べログ固有のセレクタ
    SELECTORS = {
        'shop_name': [
            'h2.display-name span',
            'h2.display-name',
            'h1.display-name span',
            'h1.display-name',
            '.rstinfo-table__name'
        ],
        'phone': [
            'span.rstinfo-table__tel-num',
            'p.rstinfo-table__tel',
            'div.rstinfo-table__tel'
        ],
        'address': [
            'p.rstinfo-table__address',
            'span.rstinfo-table__address',
            'div.rstinfo-table__address',
            'p.rstdtl-side-address__text'
        ],
        'genre': [
            '.rstinfo-table__genre',
            'span[property="v:category"]'
        ],
        'station': [
            '.rstinfo-table__access',
            'dl.rdheader-subinfo__item--station'
        ],
        'open_time': [
            'p.rstinfo-table__open-hours',
            '.rstinfo-table__open-hours',
            'dl.rdheader-subinfo__item--open-hours'
        ],
        'seats': [
            '.rstinfo-table__seats'
        ],
        'rating': [
            'span.rdheader-rating__score-val-dtl',
            'b.c-rating__val',
            'span.rstdtl-rating__score',
            '.rdheader-rating__score em'
        ],
        'review_count': [
            'a[href*="/dtlrvwlst/"] em',
            'span.rdheader-rating__review-target em',
            '.rdheader-rating__review-target em',
            'em.rstdtl-rating__review-count',
            'span.rstdtl-rating__review-count',
            '.rdheader-rating__review-count em',
            'em.rdheader-rating__review-count'
        ]
    }
    
    # ジャンルキーワード
    GENRE_KEYWORDS = [
        '料理', '焼', '鍋', '寿司', '鮨', 'そば', 'うどん', 'ラーメン',
        'カレー', 'イタリアン', 'フレンチ', '中華', '和食', '洋食',
        'カフェ', 'バー', '居酒屋', '食堂', 'レストラン', 'ビストロ',
        '焼肉', '焼鳥', 'ステーキ', 'ハンバーグ', 'とんかつ',
        '天ぷら', '串カツ', '串焼き', 'ホルモン', '餃子',
        'パスタ', 'ピザ', 'バーガー', 'パン', 'スイーツ', 'ケーキ'
    ]
    
    def extract_shop_name(self) -> str:
        """店名を抽出"""
        name = self.extract_text_by_selectors(self.SELECTORS['shop_name'])
        if name:
            # 括弧内の補足情報を削除
            name = re.sub(r'\s*\([^)]+\)\s*$', '', name)
            # 余分な空白を削除
            name = self.clean_text(name)
            # 「食べログ」という文字列を除外
            if name and name != '食べログ':
                return name
        return ""
    
    def extract_phone(self) -> str:
        """電話番号を抽出"""
        # セレクタで直接取得
        phone_elem_text = self.extract_text_by_selectors(self.SELECTORS['phone'])
        if phone_elem_text:
            phone = self.extract_phone_number(phone_elem_text)
            if phone:
                return phone
        
        # ラベルから取得
        phone = self.extract_by_label(['電話番号', 'TEL', 'Tel'])
        if phone:
            return self.extract_phone_number(phone) or ""
        
        return ""
    
    def extract_address(self) -> str:
        """住所を抽出"""
        # セレクタで直接取得
        address = self.extract_text_by_selectors(self.SELECTORS['address'])
        if address:
            return self.remove_postal_code(address)
        
        # ラベルから取得
        address = self.extract_by_label(['住所', '所在地'])
        if address:
            return self.remove_postal_code(address)
        
        return ""
    
    def extract_genre(self) -> str:
        """ジャンルを抽出"""
        # セレクタで直接取得
        genre = self.extract_text_by_selectors(self.SELECTORS['genre'])
        if genre and genre != '飲食店':
            return genre
        
        # ラベルから取得
        genre = self.extract_by_label(['ジャンル', 'カテゴリー', 'Category'])
        if genre:
            return genre
        
        # リンクテキストから探す（駅名を除外）
        if self.soup:
            for elem in self.soup.select('span.linktree__parent-target-text'):
                text = elem.get_text(strip=True)
                if text and '駅' not in text and not text.endswith('線'):
                    # ジャンルキーワードを含むか確認
                    if any(keyword in text for keyword in self.GENRE_KEYWORDS):
                        return text
        
        return ""
    
    def extract_station(self) -> str:
        """最寄り駅を抽出"""
        # セレクタで直接取得
        station = self.extract_text_by_selectors(self.SELECTORS['station'])
        if station:
            # 「から」以前の部分を取得
            station = station.split('から')[0].split('、')[0]
            return station.strip()
        
        # ラベルから取得
        station = self.extract_by_label(['交通手段', '最寄り駅', '最寄駅', 'アクセス'])
        if station:
            station = station.split('から')[0].split('、')[0]
            return station.strip()
        
        # リンクテキストから駅名を探す
        if self.soup:
            for elem in self.soup.select('span.linktree__parent-target-text'):
                text = elem.get_text(strip=True)
                if '駅' in text:
                    return text
        
        return ""
    
    def extract_open_time(self) -> str:
        """営業時間を抽出"""
        # セレクタで直接取得
        hours = self.extract_text_by_selectors(self.SELECTORS['open_time'])
        if hours:
            return self.clean_text(hours)
        
        # ラベルから取得
        hours = self.extract_by_label(['営業時間', '営業', 'Hours'])
        if hours:
            return self.clean_text(hours)
        
        return ""
    
    def extract_seats(self) -> str:
        """席数を抽出"""
        # セレクタで直接取得
        seats_text = self.extract_text_by_selectors(self.SELECTORS['seats'])
        if seats_text:
            number = self.extract_number(seats_text)
            if number:
                return f"{number}席"
        
        # ラベルから取得
        seats = self.extract_by_label(['席数', '座席', '座席数', 'Seats'])
        if seats:
            # 数字を抽出
            number = self.extract_number(seats)
            if number:
                return f"{number}席"
            # パターンにマッチしなくても席数情報があれば返す
            if '席' in seats:
                return seats
        
        return ""
    
    def extract_official_url(self) -> str:
        """公式サイトURLを抽出"""
        # ラベルから取得
        official_labels = ['ホームページ', '公式', 'Official', 'HP', 'Website', '公式サイト']
        
        for label in official_labels:
            if self.soup:
                for th in self.soup.find_all(['th', 'td'], string=re.compile(label, re.IGNORECASE)):
                    next_elem = th.find_next_sibling()
                    if next_elem:
                        link = next_elem.find('a')
                        if link and link.get('href'):
                            return link.get('href')
        
        # SNSリンクを探す
        if self.soup:
            sns_domains = ['instagram.com', 'twitter.com', 'x.com', 'facebook.com']
            for link in self.soup.find_all('a', href=True):
                href = link.get('href', '')
                if any(domain in href for domain in sns_domains):
                    return href
        
        return ""
    
    def extract_review_count(self) -> str:
        """口コミ数を抽出"""
        # セレクタで直接取得
        review_text = self.extract_text_by_selectors(self.SELECTORS['review_count'])
        if review_text:
            number = self.extract_number(review_text)
            if number:
                return number
        
        # 追加セレクタで試す（口コミリンク）
        if self.soup:
            # 口コミページへのリンクを探す
            review_link = self.soup.select_one('span.rdheader-rating__review-target')
            if review_link:
                text = review_link.get_text(strip=True)
                # "口コミ96人" のような形式から数字を抽出
                match = re.search(r'(\d+)', text)
                if match:
                    return match.group(1)
        
        # パターンマッチング
        if self.soup:
            text = self.soup.get_text()
            patterns = [
                r'口コミ\s*(\d+)\s*人',
                r'(\d+)\s*件の口コミ',
                r'口コミ\s*(\d+)\s*件',
                r'レビュー\s*(\d+)',
                r'(\d+)\s*reviews'
            ]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    return match.group(1)
        
        return "0"
    
    def extract_rating(self) -> str:
        """評価点を抽出"""
        # セレクタで直接取得
        rating_text = self.extract_text_by_selectors(self.SELECTORS['rating'])
        if rating_text:
            # 数値のみ抽出（3.58 のような形式）
            match = re.search(r'(\d+\.\d+)', rating_text)
            if match:
                return match.group(1)
        
        return ""
    
    def extract_budget(self, meal_type: str = 'dinner') -> str:
        """
        予算を抽出
        
        Args:
            meal_type: 'dinner' または 'lunch'
            
        Returns:
            予算文字列
        """
        if meal_type == 'dinner':
            keywords = ['夜', 'ディナー', '夕食']
        else:
            keywords = ['昼', 'ランチ', '昼食']
        
        if self.soup:
            for keyword in keywords:
                for elem in self.soup.find_all(string=re.compile(keyword)):
                    parent = elem.parent
                    if parent:
                        text = parent.get_text(strip=True)
                        price = self.extract_price(text)
                        if price:
                            return price
        
        return ""
    
    def extract_all(self) -> Dict:
        """
        全ての情報を抽出
        
        Returns:
            抽出されたデータの辞書
        """
        return {
            'shop_name': self.extract_shop_name(),
            'phone': self.extract_phone(),
            'address': self.extract_address(),
            'genre': self.extract_genre(),
            'station': self.extract_station(),
            'open_time': self.extract_open_time(),
            'seats': self.extract_seats(),
            'official_url': self.extract_official_url(),
            'review_count': self.extract_review_count(),
            'rating': self.extract_rating(),
            'budget_dinner': self.extract_budget('dinner'),
            'budget_lunch': self.extract_budget('lunch')
        }