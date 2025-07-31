#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
改良版食べログスクレイピングクライアント
店名・電話番号・住所を正確に取得するためのスクレイパー
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from typing import List, Dict, Optional
import logging
from urllib.parse import urljoin, urlparse
import random

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TabelogScraperImproved:
    """改良版食べログスクレイパー"""
    
    def __init__(self):
        """初期化"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.base_url = "https://tabelog.com"
        
    def get_area_urls(self) -> Dict[str, str]:
        """地域別URLを取得"""
        return {
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
    
    def scrape_restaurant_list(self, area_url: str, max_pages: int = 5) -> List[str]:
        """
        レストランリストページから個別店舗URLを取得
        
        Args:
            area_url (str): 地域URL
            max_pages (int): 最大ページ数
            
        Returns:
            List[str]: 店舗URL リスト
        """
        restaurant_urls = []
        
        for page in range(1, max_pages + 1):
            try:
                # ページURL構築
                if page == 1:
                    page_url = area_url
                else:
                    page_url = f"{area_url}rstLst/{page}/"
                
                logger.info(f"ページ取得中: {page_url}")
                
                # ページ取得
                response = self.session.get(page_url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # 店舗リンクを抽出（複数のセレクタを試す）
                restaurant_links = []
                
                # セレクタのパターンを試す
                selectors = [
                    'a.list-rst__rst-name-target',
                    'a[href*="/A"][href*="/A"][href*="/"]',
                    '.list-rst__rst-name a',
                    '.rst-name a'
                ]
                
                for selector in selectors:
                    links = soup.select(selector)
                    if links:
                        restaurant_links = links
                        break
                
                # hrefパターンでも検索
                if not restaurant_links:
                    all_links = soup.find_all('a', href=True)
                    restaurant_links = [link for link in all_links 
                                     if re.match(r'/[^/]+/A\d+/A\d+/\d+/', link.get('href', ''))]
                
                page_urls = []
                for link in restaurant_links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in restaurant_urls:
                            restaurant_urls.append(full_url)
                            page_urls.append(full_url)
                
                logger.info(f"ページ {page}: {len(page_urls)}件の店舗URL取得")
                
                if not page_urls:
                    logger.info(f"ページ {page} で店舗URLが見つかりませんでした")
                    break
                
                # レート制限対応
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                logger.error(f"ページ {page} の取得エラー: {e}")
                continue
        
        logger.info(f"合計 {len(restaurant_urls)} 件の店舗URL取得")
        return restaurant_urls
    
    def scrape_restaurant_detail(self, restaurant_url: str) -> Optional[Dict]:
        """
        個別店舗ページから詳細情報を取得
        
        Args:
            restaurant_url (str): 店舗URL
            
        Returns:
            Optional[Dict]: 店舗情報
        """
        try:
            logger.info(f"店舗詳細取得中: {restaurant_url}")
            
            # メインページを取得
            response = self.session.get(restaurant_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 店名取得
            shop_name = self._extract_shop_name(soup)
            
            # 電話番号と住所は地図ページから取得
            map_url = restaurant_url.rstrip('/') + '/dtlmap/'
            phone, address = self._extract_contact_info_from_map(map_url)
            
            # その他の情報を取得
            genre = self._extract_genre(soup)
            station = self._extract_station(soup)
            open_time = self._extract_open_time(soup)
            
            # 必須項目チェック
            if not shop_name:
                logger.warning(f"店名が取得できませんでした: {restaurant_url}")
                return None
            
            restaurant_info = {
                'shop_name': shop_name,
                'phone': phone,
                'address': address,
                'genre': genre,
                'station': station,
                'open_time': open_time,
                'url': restaurant_url,
                'source': '食べログ'
            }
            
            logger.info(f"✅ 店舗情報取得成功: {shop_name}")
            return restaurant_info
            
        except Exception as e:
            logger.error(f"店舗詳細取得エラー {restaurant_url}: {e}")
            return None
    
    def _extract_shop_name(self, soup: BeautifulSoup) -> str:
        """店名を抽出"""
        name_selectors = [
            'h1.display-name',
            '.rst-name',
            'h1',
            '.shop-name',
            'title'
        ]
        
        for selector in name_selectors:
            name_elem = soup.select_one(selector)
            if name_elem:
                name_text = name_elem.get_text(strip=True)
                # タイトルから店名部分を抽出
                if ' - ' in name_text:
                    name_text = name_text.split(' - ')[0]
                if '(' in name_text:
                    name_text = name_text.split('(')[0]
                return name_text.strip()
        
        return ""
    
    def _extract_contact_info_from_map(self, map_url: str) -> tuple:
        """地図ページから電話番号と住所を取得"""
        try:
            response = self.session.get(map_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            phone = ""
            address = ""
            
            # 店舗情報テーブルから取得
            info_rows = soup.find_all('tr')
            for row in info_rows:
                th = row.find('th')
                td = row.find('td')
                
                if th and td:
                    th_text = th.get_text(strip=True)
                    td_text = td.get_text(strip=True)
                    
                    if '予約' in th_text or 'お問い合わせ' in th_text:
                        # 電話番号を抽出
                        phone_match = re.search(r'[\d\-\(\)]+', td_text)
                        if phone_match:
                            phone = phone_match.group()
                    
                    elif '住所' in th_text:
                        address = td_text
            
            # 別のセレクタも試す
            if not phone:
                phone_elems = soup.select('.rst-info-table__tel, .tel-wrap')
                for elem in phone_elems:
                    phone_text = elem.get_text(strip=True)
                    phone_match = re.search(r'[\d\-\(\)]+', phone_text)
                    if phone_match:
                        phone = phone_match.group()
                        break
            
            if not address:
                address_elems = soup.select('.rst-info-table__address, .address')
                for elem in address_elems:
                    address = elem.get_text(strip=True)
                    if address:
                        break
            
            return phone, address
            
        except Exception as e:
            logger.error(f"地図ページ取得エラー {map_url}: {e}")
            return "", ""
    
    def _extract_genre(self, soup: BeautifulSoup) -> str:
        """ジャンルを抽出"""
        genre_selectors = [
            '.rst-info-table__genre',
            '.genre',
            '.category'
        ]
        
        for selector in genre_selectors:
            genre_elem = soup.select_one(selector)
            if genre_elem:
                return genre_elem.get_text(strip=True)
        
        return ""
    
    def _extract_station(self, soup: BeautifulSoup) -> str:
        """最寄り駅を抽出"""
        station_selectors = [
            '.rst-info-table__station',
            '.station',
            '.access'
        ]
        
        for selector in station_selectors:
            station_elem = soup.select_one(selector)
            if station_elem:
                return station_elem.get_text(strip=True)
        
        return ""
    
    def _extract_open_time(self, soup: BeautifulSoup) -> str:
        """営業時間を抽出"""
        time_selectors = [
            '.rst-info-table__open-time',
            '.open-time',
            '.business-hour'
        ]
        
        for selector in time_selectors:
            time_elem = soup.select_one(selector)
            if time_elem:
                return time_elem.get_text(strip=True)
        
        return ""
    
    def scrape_area_restaurants(self, area_name: str, max_restaurants: int = 100) -> List[Dict]:
        """
        地域の飲食店情報を取得
        
        Args:
            area_name (str): 地域名
            max_restaurants (int): 最大取得件数
            
        Returns:
            List[Dict]: 店舗情報リスト
        """
        area_urls = self.get_area_urls()
        
        if area_name not in area_urls:
            logger.error(f"未対応の地域です: {area_name}")
            return []
        
        area_url = area_urls[area_name]
        
        # 店舗URLリスト取得
        max_pages = max(1, max_restaurants // 20)  # 1ページ約20件として計算
        restaurant_urls = self.scrape_restaurant_list(area_url, max_pages)
        
        # 最大件数に制限
        restaurant_urls = restaurant_urls[:max_restaurants]
        
        # 各店舗の詳細情報取得
        restaurants = []
        for i, url in enumerate(restaurant_urls, 1):
            logger.info(f"進捗: {i}/{len(restaurant_urls)}")
            
            restaurant_info = self.scrape_restaurant_detail(url)
            if restaurant_info:
                restaurants.append(restaurant_info)
            
            # レート制限対応
            time.sleep(random.uniform(2, 4))
            
            # 途中経過表示
            if i % 10 == 0:
                logger.info(f"取得済み: {len(restaurants)}/{i} 件")
        
        logger.info(f"地域 {area_name}: {len(restaurants)} 件の店舗情報取得完了")
        return restaurants

def test_improved_tabelog_scraper():
    """改良版食べログスクレイパーのテスト"""
    scraper = TabelogScraperImproved()
    
    print("🔍 改良版食べログスクレイパー テスト開始")
    
    # 東京都で少数の店舗をテスト取得
    restaurants = scraper.scrape_area_restaurants('東京都', max_restaurants=3)
    
    if restaurants:
        print(f"✅ {len(restaurants)}件の店舗情報を取得しました")
        
        for i, restaurant in enumerate(restaurants, 1):
            print(f"\n{i}. {restaurant['shop_name']}")
            print(f"   電話: {restaurant['phone']}")
            print(f"   住所: {restaurant['address']}")
            print(f"   ジャンル: {restaurant['genre']}")
            print(f"   最寄り駅: {restaurant['station']}")
    else:
        print("❌ 店舗情報を取得できませんでした")

if __name__ == "__main__":
    test_improved_tabelog_scraper()

