#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
アプリケーション設定
環境変数と設定ファイルを管理
"""

import os
from pathlib import Path
from typing import Dict, Any
import json
import logging

logger = logging.getLogger(__name__)


class Settings:
    """設定管理クラス"""
    
    # デフォルト設定
    DEFAULTS = {
        # スクレイピング設定
        'max_concurrent': 10,
        'delay_min': 1.0,
        'delay_max': 3.0,
        'timeout': 30,
        'max_retries': 3,
        
        # ディレクトリ設定
        'cache_dir': 'cache',
        'output_dir': 'output',
        'log_dir': 'logs',
        
        # ログ設定
        'log_level': 'INFO',
        'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        
        # 食べログ設定
        'tabelog_base_url': 'https://tabelog.com',
        'tabelog_max_pages_per_area': 10,
        
        # HotPepper API設定
        'hotpepper_api_key': '',
        'hotpepper_base_url': 'http://webservice.recruit.co.jp/hotpepper/gourmet/v1/',
        
        # Excel出力設定
        'excel_max_column_width': 50,
        'excel_sheet_name': '飲食店リスト'
    }
    
    def __init__(self, config_file: str = None):
        """
        初期化
        
        Args:
            config_file: 設定ファイルのパス
        """
        self.config = self.DEFAULTS.copy()
        
        # 環境変数から読み込み
        self._load_from_env()
        
        # 設定ファイルから読み込み
        if config_file and Path(config_file).exists():
            self._load_from_file(config_file)
        
        # ディレクトリを作成
        self._create_directories()
    
    def _load_from_env(self):
        """環境変数から設定を読み込み"""
        env_mappings = {
            'SCRAPER_MAX_CONCURRENT': 'max_concurrent',
            'SCRAPER_DELAY_MIN': 'delay_min',
            'SCRAPER_DELAY_MAX': 'delay_max',
            'SCRAPER_TIMEOUT': 'timeout',
            'SCRAPER_LOG_LEVEL': 'log_level',
            'HOTPEPPER_API_KEY': 'hotpepper_api_key'
        }
        
        for env_key, config_key in env_mappings.items():
            value = os.environ.get(env_key)
            if value:
                # 数値の変換
                if config_key in ['max_concurrent', 'timeout', 'max_retries']:
                    try:
                        self.config[config_key] = int(value)
                    except ValueError:
                        logger.warning(f"無効な環境変数値: {env_key}={value}")
                elif config_key in ['delay_min', 'delay_max']:
                    try:
                        self.config[config_key] = float(value)
                    except ValueError:
                        logger.warning(f"無効な環境変数値: {env_key}={value}")
                else:
                    self.config[config_key] = value
    
    def _load_from_file(self, config_file: str):
        """設定ファイルから読み込み"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
                self.config.update(file_config)
                logger.info(f"設定ファイル読み込み: {config_file}")
        except Exception as e:
            logger.error(f"設定ファイル読み込みエラー: {e}")
    
    def _create_directories(self):
        """必要なディレクトリを作成"""
        for dir_key in ['cache_dir', 'output_dir', 'log_dir']:
            dir_path = Path(self.config[dir_key])
            dir_path.mkdir(exist_ok=True)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        設定値を取得
        
        Args:
            key: 設定キー
            default: デフォルト値
            
        Returns:
            設定値
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any):
        """
        設定値を設定
        
        Args:
            key: 設定キー
            value: 設定値
        """
        self.config[key] = value
    
    def save(self, config_file: str):
        """
        設定をファイルに保存
        
        Args:
            config_file: 保存先ファイルパス
        """
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
                logger.info(f"設定保存: {config_file}")
        except Exception as e:
            logger.error(f"設定保存エラー: {e}")
    
    def get_scraper_config(self) -> Dict[str, Any]:
        """スクレイパー用の設定を取得"""
        return {
            'max_concurrent': self.config['max_concurrent'],
            'delay_range': (self.config['delay_min'], self.config['delay_max']),
            'cache_dir': self.config['cache_dir'],
            'timeout': self.config['timeout']
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """ログ設定を取得"""
        return {
            'level': getattr(logging, self.config['log_level']),
            'format': self.config['log_format'],
            'handlers': [
                logging.FileHandler(
                    Path(self.config['log_dir']) / 'scraper.log',
                    encoding='utf-8'
                ),
                logging.StreamHandler()
            ]
        }


# シングルトンインスタンス
_settings = None


def get_settings(config_file: str = None) -> Settings:
    """
    設定インスタンスを取得
    
    Args:
        config_file: 設定ファイルパス
        
    Returns:
        設定インスタンス
    """
    global _settings
    if _settings is None:
        _settings = Settings(config_file)
    return _settings