# ディレクトリ整理計画

## 保持するファイル（リファクタリング版）

### メインシステム
- `main.py` - 新しいメインエントリーポイント
- `restaurant_data_integrator.py` - データ統合（他のファイルが依存）
- `hotpepper_api_client.py` - HotPepper API連携

### src/ ディレクトリ（新構造）
- `src/scrapers/base.py` - 基底スクレイパー
- `src/scrapers/tabelog.py` - 食べログスクレイパー
- `src/extractors/base.py` - 基底抽出クラス
- `src/extractors/tabelog.py` - 食べログ抽出ロジック
- `src/config/settings.py` - 設定管理

### ユーティリティ（テストで使用）
- `utils/validators.py`
- `utils/error_handler.py`
- `utils/progress.py`

### テスト
- `tests/` ディレクトリ全体

### ドキュメント
- `README.md`
- `requirements.txt`
- `refactoring_plan.md`

## アーカイブするファイル（旧バージョン）

### スクレイパーの旧バージョン
- `tabelog_scraper_async.py` (v1)
- `tabelog_scraper_async_v2.py` (v2)
- `tabelog_scraper_improved.py` (初期版)
- `tabelog_scraper_ultra_v3.py` (v3)
- `tabelog_scraper_smart_v4.py` (v4)
- `tabelog_scraper_patient_v5.py` (v5)
- `tabelog_scraper_enhanced_v6.py` (v6)

### アプリケーションの旧バージョン
- `restaurant_scraper_app.py` (初期版)
- `restaurant_scraper_app_fast.py` (高速版v1)
- `restaurant_scraper_app_fast_v2.py` (高速版v2)

### ユーティリティスクリプト
- `combine_existing_data.py` (一時的なスクリプト)
- `enhance_existing_data.py` (一時的なスクリプト)
- `create_final_report.py` (一時的なスクリプト)

### 重複ディレクトリ
- `config/` (旧設定、src/config/に統合済み)
- `scrapers/` (旧スクレイパー、src/scrapers/に統合済み)

## 削除するファイル

### セットアップ関連
- `setup.py` (requirements.txtで十分)
- `.serena/` ディレクトリ（外部ツール）

## 実行手順

1. バックアップディレクトリ作成 ✓
2. 旧バージョンファイルをアーカイブ
3. 重複ディレクトリを削除
4. 動作確認
5. 最終クリーンアップ