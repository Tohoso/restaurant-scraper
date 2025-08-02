#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基底データ抽出クラス
HTMLからデータを抽出する共通機能を提供
"""

import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """基底データ抽出クラス"""
    
    # 電話番号のパターン
    PHONE_PATTERNS = [
        r'03-\d{4}-\d{4}',
        r'0\d{2,3}-\d{3,4}-\d{4}',
        r'0\d{9,10}',
        r'\d{2,4}-\d{2,4}-\d{4}'
    ]
    
    # 価格のパターン
    PRICE_PATTERNS = [
        r'¥[\d,]+\s*[~～-]\s*¥[\d,]+',
        r'[\d,]+円\s*[~～-]\s*[\d,]+円',
        r'￥[\d,]+\s*[~～-]\s*￥[\d,]+'
    ]
    
    def __init__(self):
        """初期化"""
        self.soup = None
    
    def set_html(self, html: str):
        """
        HTML文字列をセット
        
        Args:
            html: HTML文字列
        """
        self.soup = BeautifulSoup(html, 'html.parser')
    
    def extract_text_by_selectors(self, selectors: List[str]) -> Optional[str]:
        """
        複数のCSSセレクタを試して最初に見つかったテキストを返す
        
        Args:
            selectors: CSSセレクタのリスト
            
        Returns:
            抽出されたテキストまたはNone
        """
        if not self.soup:
            return None
        
        for selector in selectors:
            elem = self.soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text
        
        return None
    
    def extract_by_label(self, labels: List[str], element_types: List[str] = None) -> Optional[str]:
        """
        ラベルテキストから隣接要素のテキストを抽出
        
        Args:
            labels: 検索するラベルのリスト
            element_types: 検索する要素タイプ（デフォルト: ['th', 'td']）
            
        Returns:
            抽出されたテキストまたはNone
        """
        if not self.soup:
            return None
        
        if element_types is None:
            element_types = ['th', 'td']
        
        for label in labels:
            for elem_type in element_types:
                elements = self.soup.find_all(elem_type, string=re.compile(label))
                for elem in elements:
                    next_elem = elem.find_next_sibling()
                    if next_elem:
                        text = next_elem.get_text(strip=True)
                        if text:
                            return text
        
        return None
    
    def extract_phone_number(self, text: str = None) -> Optional[str]:
        """
        電話番号を抽出
        
        Args:
            text: 検索対象のテキスト（Noneの場合はページ全体）
            
        Returns:
            電話番号またはNone
        """
        if text is None and self.soup:
            text = self.soup.get_text()
        
        if not text:
            return None
        
        for pattern in self.PHONE_PATTERNS:
            match = re.search(pattern, text)
            if match:
                return match.group()
        
        return None
    
    def extract_price(self, text: str = None) -> Optional[str]:
        """
        価格を抽出
        
        Args:
            text: 検索対象のテキスト（Noneの場合はページ全体）
            
        Returns:
            価格またはNone
        """
        if text is None and self.soup:
            text = self.soup.get_text()
        
        if not text:
            return None
        
        for pattern in self.PRICE_PATTERNS:
            match = re.search(pattern, text)
            if match:
                return match.group()
        
        return None
    
    def extract_number(self, text: str) -> Optional[str]:
        """
        テキストから数字を抽出
        
        Args:
            text: 検索対象のテキスト
            
        Returns:
            数字またはNone
        """
        if not text:
            return None
        
        match = re.search(r'\d+', text)
        if match:
            return match.group()
        
        return None
    
    def clean_text(self, text: str) -> str:
        """
        テキストをクリーンアップ
        
        Args:
            text: クリーンアップするテキスト
            
        Returns:
            クリーンアップされたテキスト
        """
        if not text:
            return ""
        
        # 余分な空白を削除
        text = re.sub(r'\s+', ' ', text)
        # 前後の空白を削除
        text = text.strip()
        
        return text
    
    def remove_postal_code(self, address: str) -> str:
        """
        住所から郵便番号を削除
        
        Args:
            address: 住所文字列
            
        Returns:
            郵便番号を削除した住所
        """
        if not address:
            return ""
        
        # 郵便番号パターンを削除
        address = re.sub(r'〒\d{3}-\d{4}\s*', '', address)
        address = re.sub(r'^\d{3}-\d{4}\s*', '', address)
        
        return address.strip()
    
    @abstractmethod
    def extract_all(self) -> Dict:
        """
        全ての情報を抽出（サブクラスで実装）
        
        Returns:
            抽出されたデータの辞書
        """
        pass