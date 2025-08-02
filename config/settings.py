#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
環境変数と設定管理
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv

class Settings:
    """アプリケーション設定管理クラス"""
    
    def __init__(self, env_file: Optional[str] = None):
        """
        設定を初期化
        
        Args:
            env_file: .envファイルのパス
        """
        # .envファイルを読み込み
        if env_file and Path(env_file).exists():
            load_dotenv(env_file)
        else:
            load_dotenv()
        
        # 環境変数から設定を読み込み
        self._load_settings()
    
    def _load_settings(self):
        """環境変数から設定を読み込み"""
        # API設定
        self.hotpepper_api_key = os.getenv('HOTPEPPER_API_KEY', '')
        
        # デフォルト設定
        self.default_area = os.getenv('DEFAULT_AREA', '東京都')
        self.max_restaurants_per_area = int(os.getenv('MAX_RESTAURANTS_PER_AREA', '100'))
        self.output_filename = os.getenv('OUTPUT_FILENAME', 'restaurant_list.xlsx')
        
        # スクレイピング設定
        self.tabelog_delay_min = float(os.getenv('TABELOG_DELAY_MIN', '2'))
        self.tabelog_delay_max = float(os.getenv('TABELOG_DELAY_MAX', '4'))
        self.user_agent = os.getenv('USER_AGENT', 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        # 並行処理設定
        self.max_concurrent = int(os.getenv('MAX_CONCURRENT', '10'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '20'))
        self.timeout = int(os.getenv('REQUEST_TIMEOUT', '30'))
        
        # ディレクトリ設定
        self.cache_dir = os.getenv('CACHE_DIR', 'cache')
        self.output_dir = os.getenv('OUTPUT_DIR', 'output')
        self.log_dir = os.getenv('LOG_DIR', 'logs')
        
        # ログ設定
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.log_file = os.getenv('LOG_FILE', 'restaurant_scraper.log')
        
        # 進捗管理
        self.progress_save_interval = int(os.getenv('PROGRESS_SAVE_INTERVAL', '10'))
        
    def get_delay_range(self) -> tuple:
        """遅延範囲を取得"""
        return (self.tabelog_delay_min, self.tabelog_delay_max)
    
    def get_directories(self) -> Dict[str, str]:
        """ディレクトリ設定を取得"""
        return {
            'cache': self.cache_dir,
            'output': self.output_dir,
            'logs': self.log_dir
        }
    
    def create_directories(self):
        """必要なディレクトリを作成"""
        for directory in self.get_directories().values():
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def to_dict(self) -> Dict[str, Any]:
        """設定を辞書形式で取得"""
        return {
            'hotpepper_api_key': self.hotpepper_api_key,
            'default_area': self.default_area,
            'max_restaurants_per_area': self.max_restaurants_per_area,
            'output_filename': self.output_filename,
            'tabelog_delay_min': self.tabelog_delay_min,
            'tabelog_delay_max': self.tabelog_delay_max,
            'user_agent': self.user_agent,
            'max_concurrent': self.max_concurrent,
            'batch_size': self.batch_size,
            'timeout': self.timeout,
            'cache_dir': self.cache_dir,
            'output_dir': self.output_dir,
            'log_dir': self.log_dir,
            'log_level': self.log_level,
            'log_file': self.log_file,
            'progress_save_interval': self.progress_save_interval
        }
    
    def validate(self) -> bool:
        """設定の妥当性を検証"""
        errors = []
        
        # 数値の範囲チェック
        if self.max_concurrent < 1 or self.max_concurrent > 50:
            errors.append("max_concurrent must be between 1 and 50")
        
        if self.batch_size < 1:
            errors.append("batch_size must be greater than 0")
        
        if self.timeout < 1:
            errors.append("timeout must be greater than 0")
        
        if self.tabelog_delay_min < 0:
            errors.append("tabelog_delay_min must be non-negative")
        
        if self.tabelog_delay_max < self.tabelog_delay_min:
            errors.append("tabelog_delay_max must be >= tabelog_delay_min")
        
        if errors:
            for error in errors:
                print(f"Configuration Error: {error}")
            return False
        
        return True

# シングルトンインスタンス
settings = Settings()