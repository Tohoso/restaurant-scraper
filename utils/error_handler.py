#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
エラーハンドリングユーティリティ
"""

import functools
import asyncio
import time
from typing import Any, Callable, Optional, TypeVar, Union
from datetime import datetime

from config.logging_config import LoggerMixin

T = TypeVar('T')


class ScraperError(Exception):
    """スクレイパー基底例外クラス"""
    pass


class NetworkError(ScraperError):
    """ネットワーク関連エラー"""
    pass


class ParseError(ScraperError):
    """パース関連エラー"""
    pass


class RateLimitError(ScraperError):
    """レート制限エラー"""
    pass


class ErrorHandler(LoggerMixin):
    """エラーハンドリングを提供するクラス"""
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        """
        初期化
        
        Args:
            max_retries: 最大リトライ回数
            retry_delay: リトライ間隔（秒）
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.error_count = 0
        self.error_history = []
    
    def log_error_with_context(
        self,
        error: Exception,
        context: str,
        url: Optional[str] = None
    ):
        """コンテキスト付きでエラーをログ"""
        self.error_count += 1
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context,
            'url': url
        }
        self.error_history.append(error_info)
        
        log_message = f"{context}"
        if url:
            log_message += f" - URL: {url}"
        log_message += f" - Error: {type(error).__name__}: {error}"
        
        self.log_error(log_message)
    
    def get_error_stats(self) -> dict:
        """エラー統計を取得"""
        if not self.error_history:
            return {
                'total_errors': 0,
                'error_types': {},
                'error_rate': 0.0
            }
        
        error_types = {}
        for error in self.error_history:
            error_type = error['error_type']
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            'total_errors': self.error_count,
            'error_types': error_types,
            'recent_errors': self.error_history[-10:]  # 直近10件
        }


def retry_on_error(
    exceptions: tuple = (Exception,),
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
):
    """
    エラー時にリトライするデコレータ
    
    Args:
        exceptions: リトライ対象の例外
        max_attempts: 最大試行回数
        delay: 初回リトライまでの遅延（秒）
        backoff: リトライごとの遅延倍率
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        if args and hasattr(args[0], 'log_warning'):
                            args[0].log_warning(
                                f"Retry {attempt + 1}/{max_attempts} after {current_delay}s - {e}"
                            )
                        time.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        if args and hasattr(args[0], 'log_warning'):
                            args[0].log_warning(
                                f"Retry {attempt + 1}/{max_attempts} after {current_delay}s - {e}"
                            )
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception
        
        # 非同期関数かどうかで適切なラッパーを返す
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def handle_errors(
    default_return: Any = None,
    log_errors: bool = True,
    raise_errors: bool = False
):
    """
    エラーハンドリングデコレータ
    
    Args:
        default_return: エラー時のデフォルト返り値
        log_errors: エラーをログに記録するか
        raise_errors: エラーを再発生させるか
    """
    def decorator(func: Callable[..., T]) -> Callable[..., Union[T, Any]]:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Union[T, Any]:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_errors and args and hasattr(args[0], 'log_error'):
                    args[0].log_error(f"Error in {func.__name__}: {e}", exc_info=True)
                if raise_errors:
                    raise
                return default_return
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Union[T, Any]:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if log_errors and args and hasattr(args[0], 'log_error'):
                    args[0].log_error(f"Error in {func.__name__}: {e}", exc_info=True)
                if raise_errors:
                    raise
                return default_return
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


class NetworkErrorHandler:
    """ネットワークエラー専用ハンドラー"""
    
    @staticmethod
    def is_rate_limit_error(error: Exception) -> bool:
        """レート制限エラーかチェック"""
        error_str = str(error).lower()
        rate_limit_indicators = [
            '429', 'rate limit', 'too many requests',
            'quota exceeded', 'throttled'
        ]
        return any(indicator in error_str for indicator in rate_limit_indicators)
    
    @staticmethod
    def is_timeout_error(error: Exception) -> bool:
        """タイムアウトエラーかチェック"""
        return isinstance(error, (asyncio.TimeoutError, TimeoutError))
    
    @staticmethod
    def is_connection_error(error: Exception) -> bool:
        """接続エラーかチェック"""
        error_types = (ConnectionError, ConnectionRefusedError, ConnectionResetError)
        return isinstance(error, error_types)
    
    @staticmethod
    def get_retry_delay(error: Exception, base_delay: float = 1.0) -> float:
        """エラータイプに基づいてリトライ遅延を決定"""
        if NetworkErrorHandler.is_rate_limit_error(error):
            return base_delay * 10  # レート制限の場合は長めに待つ
        elif NetworkErrorHandler.is_timeout_error(error):
            return base_delay * 2
        else:
            return base_delay