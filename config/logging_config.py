#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
統一ロギング設定
"""

import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Optional

def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    console: bool = True
) -> logging.Logger:
    """
    統一されたロガーを設定
    
    Args:
        name: ロガー名
        log_file: ログファイルパス（オプション）
        level: ログレベル
        format_string: ログフォーマット文字列
        console: コンソール出力の有無
    
    Returns:
        設定済みのロガー
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 既存のハンドラをクリア
    logger.handlers.clear()
    
    # デフォルトフォーマット
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    formatter = logging.Formatter(format_string)
    
    # コンソールハンドラ
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # ファイルハンドラ
    if log_file:
        # ログディレクトリを作成
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_default_logger(
    name: str,
    log_to_file: bool = True,
    log_dir: str = 'logs'
) -> logging.Logger:
    """
    デフォルト設定でロガーを取得
    
    Args:
        name: ロガー名
        log_to_file: ファイルにログを出力するか
        log_dir: ログディレクトリ
    
    Returns:
        設定済みのロガー
    """
    log_file = None
    if log_to_file:
        timestamp = datetime.now().strftime('%Y%m%d')
        log_file = os.path.join(log_dir, f'{name}_{timestamp}.log')
    
    return setup_logger(
        name=name,
        log_file=log_file,
        level=logging.INFO,
        format_string='%(asctime)s - %(levelname)s - %(message)s',
        console=True
    )

class LoggerMixin:
    """ロギング機能を提供するMixinクラス"""
    
    @property
    def logger(self) -> logging.Logger:
        """ロガーインスタンスを取得"""
        if not hasattr(self, '_logger'):
            self._logger = get_default_logger(self.__class__.__name__)
        return self._logger
    
    def log_info(self, message: str):
        """情報レベルのログ出力"""
        self.logger.info(message)
    
    def log_error(self, message: str, exc_info: bool = False):
        """エラーレベルのログ出力"""
        self.logger.error(message, exc_info=exc_info)
    
    def log_warning(self, message: str):
        """警告レベルのログ出力"""
        self.logger.warning(message)
    
    def log_debug(self, message: str):
        """デバッグレベルのログ出力"""
        self.logger.debug(message)