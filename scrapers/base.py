#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ベーススクレイパークラス
"""

import re
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup

from config.constants import (
    AREA_URLS, SELECTORS, PHONE_PATTERNS, GENRE_KEYWORDS
)
from config.logging_config import LoggerMixin


class BaseTabelogScraper(LoggerMixin):
    """食べログスクレイパーの基底クラス"""
    
    def __init__(self):
        """初期化"""
        self.base_url = "https://tabelog.com"
        self._selectors = SELECTORS
        self._phone_patterns = PHONE_PATTERNS
        self._genre_keywords = GENRE_KEYWORDS
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def get_area_urls(self) -> Dict[str, str]:
        """地域別URLを取得"""
        return AREA_URLS
    
    def _extract_shop_name(self, soup: BeautifulSoup) -> str:
        """店名を抽出"""
        for selector in self._selectors['shop_name']:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                # クリーンアップ
                text = re.sub(r'\s*\([^)]+\)\s*$', '', text)  # 括弧内の読み仮名を削除
                text = re.sub(r'\s+', ' ', text)  # 余分な空白を削除
                if text and text not in ['食べログ', '', ' ']:
                    return text.strip()
        
        # タイトルタグから取得
        title = soup.select_one('title')
        if title:
            text = title.get_text(strip=True)
            if ' - ' in text:
                return text.split(' - ')[0].strip()
        
        return ""
    
    def _extract_restaurant_links(self, soup: BeautifulSoup) -> List[str]:
        """ページから店舗リンクを抽出"""
        links = []
        
        for selector in self._selectors['restaurant_links']:
            elements = soup.select(selector)
            if elements:
                links = [elem.get('href') for elem in elements if elem.get('href')]
                break
        
        # hrefパターンでも検索
        if not links:
            all_links = soup.find_all('a', href=True)
            links = [link.get('href') for link in all_links 
                    if re.match(r'/[^/]+/A\d+/A\d+/\d+/', link.get('href', ''))]
        
        return links
    
    def _extract_phone(self, soup: BeautifulSoup) -> str:
        """電話番号を抽出"""
        # セレクタベースの抽出
        for selector in self._selectors['phone']:
            if ':contains' in selector:
                # :contains セレクタの処理
                elements = soup.find_all(text=re.compile('電話番号'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            text = next_elem.get_text(strip=True)
                            phone = self._extract_phone_from_text(text)
                            if phone:
                                return phone
            else:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    phone = self._extract_phone_from_text(text)
                    if phone:
                        return phone
        
        # テキスト全体から検索
        text = soup.get_text()
        return self._extract_phone_from_text(text)
    
    def _extract_phone_from_text(self, text: str) -> str:
        """テキストから電話番号を抽出"""
        for pattern in self._phone_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group()
        return ""
    
    def _extract_address(self, soup: BeautifulSoup) -> str:
        """住所を抽出"""
        for selector in self._selectors['address']:
            if ':contains' in selector:
                # :contains セレクタの処理
                elements = soup.find_all(text=re.compile('住所'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            address = next_elem.get_text(strip=True)
                            if address:
                                return self._clean_address(address)
            else:
                elem = soup.select_one(selector)
                if elem:
                    address = elem.get_text(strip=True)
                    if address:
                        return self._clean_address(address)
        
        return ""
    
    def _clean_address(self, address: str) -> str:
        """住所をクリーンアップ"""
        # 郵便番号や不要な文字を削除
        address = re.sub(r'〒\d{3}-\d{4}\s*', '', address)
        address = re.sub(r'^\s*住所\s*[:：]\s*', '', address)
        return address.strip()
    
    def _extract_genre(self, soup: BeautifulSoup) -> str:
        """ジャンルを抽出"""
        genre_texts = []
        
        # 明確にジャンルとラベルされているセレクタを優先
        for selector in self._selectors['genre']:
            if ':contains' in selector:
                elements = soup.find_all(text=re.compile('ジャンル'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            # 複数のspanタグがある場合
                            spans = next_elem.find_all('span')
                            if spans:
                                for span in spans:
                                    text = span.get_text(strip=True)
                                    if text and text != '飲食店':
                                        genre_texts.append(text)
                            else:
                                genre = next_elem.get_text(strip=True)
                                if genre and genre != '飲食店':
                                    genre_texts.append(genre)
            else:
                elem = soup.select_one(selector)
                if elem:
                    genre = elem.get_text(strip=True)
                    if genre and genre != '飲食店':
                        genre_texts.append(genre)
        
        return "、".join(genre_texts) if genre_texts else ""
    
    def _extract_station(self, soup: BeautifulSoup) -> str:
        """最寄り駅を抽出"""
        # rdheader-subinfo__itemを優先的にチェック
        station_elem = soup.select_one('.rdheader-subinfo__item-text')
        if station_elem:
            return station_elem.get_text(strip=True)
            
        for selector in self._selectors['station']:
            if ':contains' in selector:
                pattern = '交通手段' if '交通手段' in selector else '最寄り駅'
                elements = soup.find_all(text=re.compile(pattern))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th', 'dt']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            station = next_elem.get_text(strip=True)
                            if station:
                                return station
            else:
                elem = soup.select_one(selector)
                if elem:
                    station = elem.get_text(strip=True)
                    if station:
                        return station
        
        return ""
    
    def _extract_open_time(self, soup: BeautifulSoup) -> str:
        """営業時間を抽出"""
        for selector in self._selectors['open_time']:
            if ':contains' in selector:
                elements = soup.find_all(text=re.compile('営業時間'))
                for elem in elements:
                    parent = elem.parent
                    if parent and parent.name in ['td', 'th']:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            hours = next_elem.get_text(strip=True)
                            # 改行を整理
                            hours = re.sub(r'\s+', ' ', hours)
                            if hours:
                                return hours
            else:
                elem = soup.select_one(selector)
                if elem:
                    hours = elem.get_text(strip=True)
                    hours = re.sub(r'\s+', ' ', hours)
                    if hours:
                        return hours
        
        return ""
    
    def _extract_restaurant_links(self, soup: BeautifulSoup) -> List[str]:
        """レストランリンクを抽出"""
        links = []
        
        # リストページのセレクタを使用
        list_selectors = [
            'h3.list-rst__rst-name a',
            'a.list-rst__rst-name-target',
            '.list-rst__wrap h3 a'
        ]
        
        for selector in list_selectors:
            elements = soup.select(selector)
            for elem in elements:
                href = elem.get('href')
                if href:
                    links.append(href)
        
        return links
    
    def _normalize_text(self, text: Optional[str]) -> str:
        """テキストを正規化"""
        if not text:
            return ""
        # 前後の空白を削除
        text = text.strip()
        # 連続する空白を単一スペースに変換
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def _extract_all_texts(self, element) -> List[str]:
        """要素内のすべてのテキストを抽出"""
        texts = []
        if element:
            for string in element.stripped_strings:
                texts.append(string)
        return texts
    
    def _extract_text_by_patterns(self, soup: BeautifulSoup, patterns: List[Tuple]) -> str:
        """パターンに基づいてテキストを抽出"""
        for tag, attrs in patterns:
            elem = soup.find(tag, attrs)
            if elem:
                # 次の兄弟要素から値を取得
                next_elem = elem.find_next_sibling()
                if next_elem:
                    return next_elem.get_text(strip=True)
        return ""
    
    def _is_valid_genre(self, genre: str) -> bool:
        """有効なジャンルかチェック"""
        if not genre:
            return False
        
        # 駅名を除外
        if any(word in genre for word in ['駅', '徒歩', '分']):
            return False
        
        # ジャンルキーワードが含まれているか確認
        for category_keywords in self._genre_keywords.values():
            if any(keyword in genre for keyword in category_keywords):
                return True
        
        return False
    
    def _validate_restaurant_data(self, data: Dict) -> bool:
        """レストランデータの妥当性を検証"""
        # 必須フィールドのチェック
        required_fields = ['shop_name', 'url']
        for field in required_fields:
            if not data.get(field):
                return False
        
        # URLの形式チェック
        if not re.match(r'https?://tabelog\.com/', data['url']):
            return False
        
        return True
    
    def _create_restaurant_info(
        self,
        shop_name: str,
        phone: str = "",
        address: str = "",
        genre: str = "",
        station: str = "",
        open_time: str = "",
        url: str = "",
        scraped_at: str = ""
    ) -> Dict:
        """レストラン情報の辞書を作成"""
        return {
            'shop_name': shop_name,
            'phone': phone,
            'address': address,
            'genre': genre,
            'station': station,
            'open_time': open_time,
            'url': url,
            'source': '食べログ',
            'scraped_at': scraped_at
        }