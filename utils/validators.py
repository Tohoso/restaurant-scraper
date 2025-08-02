#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
データバリデーションユーティリティ
"""

import re
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RestaurantData:
    """レストランデータの型定義"""
    shop_name: str
    phone: str = ""
    address: str = ""
    genre: str = ""
    station: str = ""
    open_time: str = ""
    url: str = ""
    source: str = "食べログ"
    scraped_at: str = ""
    
    def __post_init__(self):
        """初期化後の処理"""
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, str]:
        """辞書形式に変換"""
        return {
            'shop_name': self.shop_name,
            'phone': self.phone,
            'address': self.address,
            'genre': self.genre,
            'station': self.station,
            'open_time': self.open_time,
            'url': self.url,
            'source': self.source,
            'scraped_at': self.scraped_at
        }
    
    def is_valid(self) -> bool:
        """データの妥当性をチェック"""
        return bool(self.shop_name and self.url)


class DataValidator:
    """データバリデーションクラス"""
    
    @staticmethod
    def validate_phone_number(phone: str) -> bool:
        """
        電話番号の妥当性をチェック
        
        Args:
            phone: 電話番号
            
        Returns:
            妥当な場合True
        """
        if not phone:
            return True  # 空は許可
        
        # 日本の電話番号パターン
        patterns = [
            r'^0\d{1,4}-\d{1,4}-\d{3,4}$',  # ハイフン区切り
            r'^0\d{9,10}$',  # ハイフンなし
            r'^\+81-?\d{1,4}-?\d{1,4}-?\d{3,4}$'  # 国際番号
        ]
        
        return any(re.match(pattern, phone) for pattern in patterns)
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """
        URLの妥当性をチェック
        
        Args:
            url: URL
            
        Returns:
            妥当な場合True
        """
        if not url:
            return False
        
        # 食べログURLパターン
        tabelog_pattern = r'^https?://tabelog\.com/[^/]+/A\d+/A\d+/\d+/?$'
        return bool(re.match(tabelog_pattern, url))
    
    @staticmethod
    def validate_address(address: str) -> bool:
        """
        住所の妥当性をチェック
        
        Args:
            address: 住所
            
        Returns:
            妥当な場合True
        """
        if not address:
            return True  # 空は許可
        
        # 最低限の長さチェック
        if len(address) < 5:
            return False
        
        # 都道府県名が含まれているかチェック
        prefectures = [
            '北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
            '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
            '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県',
            '静岡県', '愛知県', '三重県', '滋賀県', '京都府', '大阪府', '兵庫県',
            '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県', '山口県',
            '徳島県', '香川県', '愛媛県', '高知県', '福岡県', '佐賀県', '長崎県',
            '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県'
        ]
        
        return any(pref in address for pref in prefectures)
    
    @staticmethod
    def validate_restaurant_data(data: Union[Dict, RestaurantData]) -> tuple[bool, List[str]]:
        """
        レストランデータ全体の妥当性をチェック
        
        Args:
            data: レストランデータ
            
        Returns:
            (妥当性, エラーメッセージリスト)
        """
        errors = []
        
        # RestaurantDataインスタンスの場合は辞書に変換
        if isinstance(data, RestaurantData):
            data_dict = data.to_dict()
        else:
            data_dict = data
        
        # 必須フィールドチェック
        if not data_dict.get('shop_name'):
            errors.append("店名が必須です")
        
        if not data_dict.get('url'):
            errors.append("URLが必須です")
        elif not DataValidator.validate_url(data_dict['url']):
            errors.append("URLの形式が不正です")
        
        # オプションフィールドチェック
        if data_dict.get('phone') and not DataValidator.validate_phone_number(data_dict['phone']):
            errors.append("電話番号の形式が不正です")
        
        if data_dict.get('address') and not DataValidator.validate_address(data_dict['address']):
            errors.append("住所の形式が不正です")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def clean_text(text: str, max_length: Optional[int] = None) -> str:
        """
        テキストをクリーンアップ
        
        Args:
            text: クリーンアップするテキスト
            max_length: 最大長
            
        Returns:
            クリーンアップされたテキスト
        """
        if not text:
            return ""
        
        # 前後の空白を削除
        text = text.strip()
        
        # 連続する空白を単一スペースに変換
        text = re.sub(r'\s+', ' ', text)
        
        # 制御文字を削除
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # 最大長で切り詰め
        if max_length and len(text) > max_length:
            text = text[:max_length] + "..."
        
        return text
    
    @staticmethod
    def normalize_phone_number(phone: str) -> str:
        """
        電話番号を正規化
        
        Args:
            phone: 電話番号
            
        Returns:
            正規化された電話番号
        """
        if not phone:
            return ""
        
        # 数字とハイフンのみ残す
        phone = re.sub(r'[^\d\-]', '', phone)
        
        # 括弧が削除された場合の特別処理
        if len(phone) == 10 and phone.startswith('03'):
            phone = f"{phone[:2]}-{phone[2:6]}-{phone[6:]}"
            return phone
        
        # 全角数字を半角に変換
        trans_table = str.maketrans('０１２３４５６７８９', '0123456789')
        phone = phone.translate(trans_table)
        
        # ハイフンの正規化
        if '-' not in phone and len(phone) >= 10:
            # 市外局番のパターンに基づいてハイフンを挿入
            if phone.startswith('03') or phone.startswith('06'):
                # 東京・大阪 (03-xxxx-xxxx)
                if len(phone) == 10:
                    phone = f"{phone[:2]}-{phone[2:6]}-{phone[6:]}"
            elif phone.startswith('0'):
                # その他 (0xx-xxx-xxxx or 0xxx-xx-xxxx)
                if len(phone) == 10:
                    phone = f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
                elif len(phone) == 11:
                    # 090, 080, 070などの携帯番号
                    if phone[:3] in ['090', '080', '070', '050']:
                        phone = f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
                    else:
                        # 0120などの4桁市外局番
                        phone = f"{phone[:4]}-{phone[4:7]}-{phone[7:]}"
        
        return phone