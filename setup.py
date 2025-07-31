#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
飲食店営業リスト作成アプリ セットアップスクリプト
"""

import subprocess
import sys
import os

def install_requirements():
    """必要なパッケージをインストール"""
    print("📦 必要なパッケージをインストール中...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
        ])
        print("✅ パッケージのインストールが完了しました")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ パッケージのインストールに失敗しました: {e}")
        return False

def test_installation():
    """インストールをテスト"""
    print("🧪 インストールをテスト中...")
    
    try:
        # 必要なモジュールのインポートテスト
        import requests
        import bs4
        import pandas
        import openpyxl
        import lxml
        
        print("✅ 全ての必要なパッケージが正常にインストールされています")
        
        # アプリケーションの基本テスト
        from restaurant_data_integrator import RestaurantDataIntegrator
        integrator = RestaurantDataIntegrator()
        print("✅ アプリケーションの基本機能が正常に動作します")
        
        return True
        
    except ImportError as e:
        print(f"❌ パッケージのインポートに失敗しました: {e}")
        return False
    except Exception as e:
        print(f"❌ テストに失敗しました: {e}")
        return False

def main():
    """メイン関数"""
    print("🍽️ 飲食店営業リスト作成アプリ セットアップ")
    print("=" * 50)
    
    # 現在のディレクトリを確認
    current_dir = os.getcwd()
    print(f"現在のディレクトリ: {current_dir}")
    
    # requirements.txtの存在確認
    if not os.path.exists("requirements.txt"):
        print("❌ requirements.txtが見つかりません")
        return False
    
    # パッケージインストール
    if not install_requirements():
        return False
    
    # インストールテスト
    if not test_installation():
        return False
    
    print("\n🎉 セットアップが完了しました！")
    print("\n使用方法:")
    print("  対話モード: python restaurant_scraper_app.py -i")
    print("  コマンドライン: python restaurant_scraper_app.py --areas 東京都 --max-per-area 100")
    print("\n詳細なヘルプ: python restaurant_scraper_app.py --help")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

