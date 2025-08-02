# Restaurant Scraper リファクタリング計画

## 現状の問題点

### 1. コードの重複
- 複数のスクレイパーバージョン（v2〜v6）に同じ抽出ロジックが散在
- 各バージョンで同じヘッダー設定、エラーハンドリングが重複

### 2. ファイル構造の混乱
- ルートディレクトリに16個のPythonファイルが散乱
- バージョン番号付きファイルが多数存在（整理が必要）

### 3. 設定の分散
- 各スクレイパーに設定がハードコード
- 統一された設定管理がない

## リファクタリング方針

### 1. ディレクトリ構造の再編成

```
restaurant-scraper/
├── src/
│   ├── scrapers/
│   │   ├── base.py          # 基底スクレイパークラス
│   │   ├── tabelog.py       # 食べログスクレイパー
│   │   └── hotpepper.py     # HotPepper APIクライアント
│   ├── extractors/
│   │   ├── base.py          # 基底抽出クラス
│   │   └── tabelog.py       # 食べログ専用抽出ロジック
│   ├── integrators/
│   │   └── data_integrator.py
│   ├── utils/
│   │   ├── validators.py
│   │   ├── error_handler.py
│   │   └── progress.py
│   └── config/
│       ├── settings.py
│       └── constants.py
├── tests/
├── output/
├── cache/
├── logs/
├── main.py                   # メインエントリーポイント
├── requirements.txt
└── README.md
```

### 2. 主要コンポーネント

#### BaseScraperクラス
- 共通のHTTPクライアント管理
- レート制限処理
- エラーハンドリング
- プログレス管理

#### DataExtractorクラス
- HTML解析ロジックの統一
- 各フィールドの抽出メソッド
- データ検証

#### ConfigManager
- 環境変数管理
- 設定ファイル読み込み
- 実行時パラメータ管理

### 3. 実装優先順位

1. **Phase 1: 基盤整備**
   - ディレクトリ構造の作成
   - 基底クラスの実装
   - 共通ユーティリティの統合

2. **Phase 2: スクレイパー統合**
   - 各バージョンの最良の部分を統合
   - 重複コードの削除
   - テスト追加

3. **Phase 3: 最適化**
   - パフォーマンス改善
   - メモリ使用量最適化
   - ログ改善

## 削除予定ファイル
- tabelog_scraper_v2.py〜v6.py（統合後）
- 一時的なテストファイル
- 重複した設定ファイル

## 保持するファイル
- 実績のある抽出ロジック
- テスト済みのユーティリティ
- 設定ファイル（統合版）