# プロジェクト構造

## 📁 ディレクトリ構造

```
restaurant-scraper/
│
├── 📦 src/                     # ソースコード（リファクタリング版）
│   ├── scrapers/               # スクレイパー実装
│   │   ├── __init__.py
│   │   ├── base.py            # 基底スクレイパークラス
│   │   └── tabelog.py         # 食べログスクレイパー
│   │
│   ├── extractors/            # データ抽出ロジック
│   │   ├── __init__.py
│   │   ├── base.py           # 基底抽出クラス
│   │   └── tabelog.py        # 食べログ専用抽出
│   │
│   └── config/               # 設定管理
│       ├── __init__.py
│       └── settings.py       # アプリケーション設定
│
├── 🧪 tests/                   # テストコード
│   ├── __init__.py
│   ├── test_base_scraper.py
│   ├── test_error_handler.py
│   ├── test_integration.py
│   ├── test_progress.py
│   └── test_validators.py
│
├── 🛠️ utils/                   # ユーティリティ
│   ├── __init__.py
│   ├── error_handler.py      # エラーハンドリング
│   ├── progress.py           # 進捗管理
│   └── validators.py         # データ検証
│
├── 📂 cache/                   # キャッシュと進捗データ
│   ├── *.json                # 進捗ファイル
│   └── results_*.json        # 結果キャッシュ
│
├── 📊 output/                  # 出力ファイル
│   └── *.xlsx                # Excel形式の結果
│
├── 📝 logs/                    # ログファイル
│   └── scraper.log           # アプリケーションログ
│
├── 🗄️ archived_versions/       # アーカイブされた旧バージョン
│   ├── tabelog_scraper_*.py  # 旧スクレイパー (v1-v6)
│   └── restaurant_scraper_app*.py # 旧アプリ
│
├── 📄 docs/                    # ドキュメント
│   ├── architecture.md
│   ├── api-reference.md
│   └── migration-guide.md
│
├── 🚀 main.py                  # メインエントリーポイント
├── restaurant_data_integrator.py # データ統合モジュール
├── hotpepper_api_client.py    # HotPepper API クライアント
│
├── 📋 requirements.txt         # 依存関係
├── 📖 README.md               # プロジェクト説明書
├── 📐 STRUCTURE.md            # このファイル
├── refactoring_plan.md        # リファクタリング計画
└── cleanup_plan.md            # クリーンアップ計画

```

## 🔧 主要コンポーネント

### 1. メインシステム (`main.py`)
- コマンドライン引数の処理
- スクレイパーの初期化と実行
- Excel出力の管理
- 統計情報の表示

### 2. スクレイパー (`src/scrapers/`)
- **base.py**: 共通機能（HTTP通信、レート制限、進捗管理）
- **tabelog.py**: 食べログ専用の実装

### 3. 抽出器 (`src/extractors/`)
- **base.py**: 共通の抽出メソッド
- **tabelog.py**: 食べログのHTML構造に特化した抽出

### 4. 設定管理 (`src/config/`)
- 環境変数のサポート
- 設定ファイルの読み込み
- デフォルト値の管理

### 5. ユーティリティ (`utils/`)
- データ検証とクリーニング
- エラーハンドリングとリトライ
- 進捗の保存と復元

## 🎯 使用方法

```bash
# 基本的な使用
python3 main.py --limit 100

# エリア指定
python3 main.py --areas "渋谷" "新宿・代々木・大久保" --limit 200

# 詳細設定
python3 main.py \
  --areas "銀座・新橋・有楽町" \
  --limit 500 \
  --concurrent 5 \
  --delay-min 2.0 \
  --delay-max 5.0 \
  --output "results"
```

## 📈 パフォーマンス

- **並行処理**: 最大10同時接続（設定可能）
- **レート制限**: 自動調整（1-3秒の遅延）
- **進捗保存**: 中断からの自動再開
- **メモリ効率**: チャンク単位での処理

## 🔄 メンテナンス

### 新しいサイトの追加
1. `src/extractors/`に新しい抽出クラスを作成
2. `src/scrapers/`に新しいスクレイパーを実装
3. `main.py`に統合

### ログの確認
```bash
tail -f logs/scraper.log
```

### キャッシュのクリア
```bash
rm -rf cache/*
```

## 📊 統計情報

プログラム実行後、以下の統計が表示されます：
- 総取得件数
- データ品質（電話番号、住所、ジャンルなどの充足率）
- 成功率
- レート制限の発生回数