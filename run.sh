#!/bin/bash

# Restaurant Scraper Quick Start Script

echo "🍽️ 飲食店営業リスト作成アプリ"
echo "================================"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3がインストールされていません"
    exit 1
fi

# Run in interactive mode by default
python3 restaurant_scraper_app.py -i
