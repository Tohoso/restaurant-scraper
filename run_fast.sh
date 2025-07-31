#!/bin/bash

# 高速版Restaurant Scraper実行スクリプト

echo "🚀 飲食店営業リスト作成アプリ（高速版）"
echo "================================"
echo ""

# Python存在確認
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3がインストールされていません"
    exit 1
fi

# 引数がある場合はそのまま渡す
if [ $# -gt 0 ]; then
    python3 restaurant_scraper_app_fast.py "$@"
else
    # 引数がない場合は対話モード
    python3 restaurant_scraper_app_fast.py -i
fi