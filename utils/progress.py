#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
進捗管理ユーティリティ
"""

import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set, Optional

from config.logging_config import LoggerMixin


class ProgressMixin(LoggerMixin):
    """進捗管理機能を提供するMixin"""
    
    def __init__(self, progress_file: str, results_file: str):
        """
        初期化
        
        Args:
            progress_file: 進捗ファイルのパス
            results_file: 結果ファイルのパス
        """
        self.progress_file = Path(progress_file)
        self.results_file = Path(results_file)
        self.processed_urls: Set[str] = set()
        self.results: List[Dict] = []
        self._ensure_directories()
    
    def _ensure_directories(self):
        """必要なディレクトリを作成"""
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        self.results_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_progress(self) -> bool:
        """
        前回の進捗を読み込む
        
        Returns:
            読み込みに成功した場合True
        """
        loaded = False
        
        # 進捗ファイルを読み込む
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_urls = set(data.get('processed_urls', []))
                    self.log_info(f"前回の進捗を読み込みました: {len(self.processed_urls)}件処理済み")
                    loaded = True
            except Exception as e:
                self.log_error(f"進捗読み込みエラー: {e}")
        
        # 結果ファイルを読み込む
        if self.results_file.exists():
            try:
                with open(self.results_file, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                    self.log_info(f"前回の結果を読み込みました: {len(self.results)}件")
                    loaded = True
            except Exception as e:
                self.log_error(f"結果読み込みエラー: {e}")
        
        return loaded
    
    def save_progress(self) -> bool:
        """
        進捗を保存
        
        Returns:
            保存に成功した場合True
        """
        try:
            # 進捗を保存
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_urls': list(self.processed_urls),
                    'timestamp': datetime.now().isoformat(),
                    'total_processed': len(self.processed_urls),
                    'total_results': len(self.results)
                }, f, ensure_ascii=False, indent=2)
            
            # 結果を保存
            with open(self.results_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            self.log_error(f"進捗保存エラー: {e}")
            return False
    
    def is_processed(self, url: str) -> bool:
        """
        URLが処理済みかチェック
        
        Args:
            url: チェックするURL
            
        Returns:
            処理済みの場合True
        """
        return url in self.processed_urls
    
    def mark_as_processed(self, url: str):
        """URLを処理済みとしてマーク"""
        self.processed_urls.add(url)
    
    def add_result(self, result: Dict):
        """結果を追加"""
        self.results.append(result)
    
    def should_save_progress(self, interval: int = 10) -> bool:
        """
        進捗を保存すべきかチェック
        
        Args:
            interval: 保存間隔
            
        Returns:
            保存すべき場合True
        """
        return len(self.results) % interval == 0
    
    def clear_progress(self):
        """進捗をクリア"""
        self.processed_urls.clear()
        self.results.clear()
        
        # ファイルも削除
        if self.progress_file.exists():
            self.progress_file.unlink()
        if self.results_file.exists():
            self.results_file.unlink()
    
    def get_stats(self) -> Dict:
        """進捗統計を取得"""
        return {
            'processed_urls': len(self.processed_urls),
            'results': len(self.results),
            'success_rate': (len(self.results) / len(self.processed_urls) * 100) 
                          if self.processed_urls else 0
        }


class BatchProgressTracker(ProgressMixin):
    """バッチ処理用の進捗トラッカー"""
    
    def __init__(self, progress_file: str, results_file: str, batch_size: int = 20):
        """
        初期化
        
        Args:
            progress_file: 進捗ファイルのパス
            results_file: 結果ファイルのパス  
            batch_size: バッチサイズ
        """
        super().__init__(progress_file, results_file)
        self.batch_size = batch_size
        self.current_batch = 0
        self.total_items = 0
    
    def set_total_items(self, total: int):
        """総アイテム数を設定"""
        self.total_items = total
    
    def start_batch(self, batch_num: int, start_idx: int, end_idx: int):
        """バッチ処理開始をログ"""
        self.current_batch = batch_num
        self.log_info(f"バッチ処理中: {start_idx+1}-{end_idx}/{self.total_items}")
    
    def complete_batch(self):
        """バッチ処理完了をログ"""
        self.save_progress()
        processed = len(self.results)
        if processed > 0:
            self.log_info(f"進捗保存: {processed}件完了")
    
    def get_remaining_items(self) -> int:
        """残りアイテム数を取得"""
        return self.total_items - len(self.results) if self.total_items > 0 else 0
    
    def get_progress_percentage(self) -> float:
        """進捗率を取得"""
        if self.total_items == 0:
            return 0.0
        return (len(self.results) / self.total_items) * 100