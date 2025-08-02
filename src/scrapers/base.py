#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基底スクレイパークラス
全てのスクレイパーの共通機能を提供
"""

import asyncio
import aiohttp
import json
import random
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set
from pathlib import Path
from datetime import datetime
from aiohttp import ClientTimeout, TCPConnector

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """基底スクレイパークラス"""
    
    # ユーザーエージェントのプール
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    def __init__(
        self,
        max_concurrent: int = 10,
        delay_range: tuple = (1.0, 3.0),
        cache_dir: str = "cache",
        timeout: int = 30
    ):
        """
        初期化
        
        Args:
            max_concurrent: 最大同時接続数
            delay_range: リクエスト間の遅延範囲（秒）
            cache_dir: キャッシュディレクトリ
            timeout: タイムアウト時間（秒）
        """
        self.max_concurrent = max_concurrent
        self.delay_range = delay_range
        self.timeout = timeout
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # キャッシュとプログレス管理
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.processed_urls: Set[str] = set()
        self.results: List[Dict] = []
        
        # 統計情報
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'rate_limited': 0
        }
    
    async def __aenter__(self):
        """非同期コンテキストマネージャ入口"""
        timeout = ClientTimeout(total=self.timeout, connect=10, sock_read=10)
        connector = TCPConnector(limit=self.max_concurrent, force_close=True)
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self._get_default_headers()
        )
        
        # 前回の進捗を読み込む
        self.load_progress()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """非同期コンテキストマネージャ出口"""
        if self.session:
            await self.session.close()
    
    def _get_default_headers(self) -> Dict[str, str]:
        """デフォルトのHTTPヘッダーを取得"""
        return {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def _get_random_user_agent(self) -> str:
        """ランダムなUser-Agentを取得"""
        return random.choice(self.USER_AGENTS)
    
    async def fetch_page(self, url: str) -> Optional[str]:
        """
        ページを非同期で取得
        
        Args:
            url: 取得するURL
            
        Returns:
            HTML文字列またはNone
        """
        async with self.semaphore:
            try:
                # ランダムな遅延
                delay = random.uniform(*self.delay_range)
                await asyncio.sleep(delay)
                
                # User-Agentをランダムに更新
                self.session.headers['User-Agent'] = self._get_random_user_agent()
                
                self.stats['total_requests'] += 1
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        self.stats['successful_requests'] += 1
                        logger.debug(f"成功: {url}")
                        return await response.text()
                    elif response.status == 429:
                        self.stats['rate_limited'] += 1
                        logger.warning(f"レート制限: {url}")
                        await self._handle_rate_limit()
                        return None
                    else:
                        self.stats['failed_requests'] += 1
                        logger.warning(f"HTTPエラー {response.status}: {url}")
                        return None
                        
            except asyncio.TimeoutError:
                self.stats['failed_requests'] += 1
                logger.error(f"タイムアウト: {url}")
                return None
            except Exception as e:
                self.stats['failed_requests'] += 1
                logger.error(f"取得エラー {url}: {e}")
                return None
    
    async def _handle_rate_limit(self):
        """レート制限の処理"""
        wait_time = 30
        logger.info(f"レート制限検出。{wait_time}秒待機...")
        await asyncio.sleep(wait_time)
    
    def load_progress(self):
        """前回の進捗を読み込む"""
        progress_file = self.cache_dir / f"{self.__class__.__name__}_progress.json"
        if progress_file.exists():
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_urls = set(data.get('processed_urls', []))
                    logger.info(f"進捗読み込み: {len(self.processed_urls)}件処理済み")
            except Exception as e:
                logger.error(f"進捗読み込みエラー: {e}")
    
    def save_progress(self):
        """進捗を保存"""
        progress_file = self.cache_dir / f"{self.__class__.__name__}_progress.json"
        try:
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_urls': list(self.processed_urls)[-10000:],  # 最新10000件のみ
                    'total_processed': len(self.processed_urls),
                    'timestamp': datetime.now().isoformat(),
                    'stats': self.stats
                }, f, ensure_ascii=False, indent=2)
            logger.debug(f"進捗保存: {len(self.processed_urls)}件")
        except Exception as e:
            logger.error(f"進捗保存エラー: {e}")
    
    def save_results(self, filename: str = None):
        """結果を保存"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"results_{timestamp}.json"
        
        results_file = self.cache_dir / filename
        try:
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
            logger.info(f"結果保存: {results_file}")
        except Exception as e:
            logger.error(f"結果保存エラー: {e}")
    
    def get_stats(self) -> Dict:
        """統計情報を取得"""
        return {
            **self.stats,
            'processed_urls': len(self.processed_urls),
            'results': len(self.results),
            'success_rate': (
                self.stats['successful_requests'] / self.stats['total_requests'] * 100
                if self.stats['total_requests'] > 0 else 0
            )
        }
    
    @abstractmethod
    async def scrape(self, **kwargs):
        """
        スクレイピングのメインメソッド（サブクラスで実装）
        """
        pass