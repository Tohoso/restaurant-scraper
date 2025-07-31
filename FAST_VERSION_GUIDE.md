# 🚀 高速版飲食店スクレイパー使用ガイド

## 概要

高速版は数千件規模のデータ取得に対応した非同期処理版です。従来版と比較して約10倍の処理速度を実現しています。

## 主な改善点

- **非同期処理**: 最大50件同時接続で並列処理
- **処理速度**: 約2.7件/秒（従来版: 約0.3件/秒）
- **進捗保存**: 中断しても続きから再開可能
- **メモリ効率**: バッチ処理により大量データでも安定動作
- **エラー耐性**: 個別エラーが全体処理を止めない

## 使用方法

### 1. 対話モード（推奨）
```bash
./run_fast.sh
```

### 2. コマンドライン実行

#### 基本的な使用例
```bash
# 東京都から100件取得
python3 restaurant_scraper_app_fast.py --areas 東京都 --max-per-area 100

# 複数地域から大量取得（1000件ずつ）
python3 restaurant_scraper_app_fast.py --areas 東京都 大阪府 神奈川県 --max-per-area 1000 --concurrent 30

# 最高速設定（50同時接続）
python3 restaurant_scraper_app_fast.py --areas 東京都 --max-per-area 5000 --concurrent 50
```

## パラメータ説明

- `--areas`: 対象地域（複数指定可）
- `--max-per-area`: 地域あたりの最大取得件数
- `--concurrent`: 最大同時接続数（1-50、デフォルト: 10）
- `--output`: 出力ファイル名
- `--hotpepper-key`: ホットペッパーAPIキー
- `--no-tabelog`: 食べログを使用しない
- `-i, --interactive`: 対話モード

## パフォーマンス目安

| 件数 | 同時接続数 | 推定時間 |
|------|-----------|----------|
| 100件 | 10 | 約40秒 |
| 500件 | 20 | 約3分 |
| 1000件 | 30 | 約6分 |
| 5000件 | 50 | 約30分 |

## 進捗管理

処理中は以下のファイルに進捗が保存されます：
- `cache/scraping_progress.json`: 処理済みURL
- `cache/partial_results.json`: 取得済みデータ
- `cache/app_progress.json`: アプリケーション進捗

処理が中断された場合、次回実行時に自動的に続きから再開されます。

## 注意事項

### レート制限
- 同時接続数を増やしすぎるとサーバーに負荷がかかります
- 推奨設定: 10-30接続
- 最大設定: 50接続（自己責任）

### メモリ使用量
- 1000件あたり約50MB
- 10000件の場合は約500MB必要

### エラー処理
- 個別の取得エラーはスキップされます
- 全体の5%以上エラーが発生した場合は警告が表示されます

## トラブルシューティング

### 1. タイムアウトエラーが多発する
```bash
# 同時接続数を減らす
python3 restaurant_scraper_app_fast.py --areas 東京都 --max-per-area 100 --concurrent 5
```

### 2. 進捗をリセットしたい
```bash
# キャッシュをクリア
rm -rf cache/*.json
```

### 3. 特定の地域でエラーが発生する
- その地域をスキップして他の地域から取得
- 時間を置いて再実行

## 推奨設定

### 小規模テスト（100-500件）
```bash
python3 restaurant_scraper_app_fast.py --areas 東京都 --max-per-area 500 --concurrent 10
```

### 中規模運用（1000-5000件）
```bash
python3 restaurant_scraper_app_fast.py --areas 東京都 大阪府 --max-per-area 2500 --concurrent 20
```

### 大規模収集（5000件以上）
```bash
# 複数回に分けて実行することを推奨
python3 restaurant_scraper_app_fast.py --areas 東京都 --max-per-area 5000 --concurrent 30

# 別の地域
python3 restaurant_scraper_app_fast.py --areas 大阪府 --max-per-area 5000 --concurrent 30
```

## ログファイル

実行ログは `restaurant_scraper_fast.log` に保存されます。
エラーが発生した場合はこのファイルを確認してください。

---

高速版を使用することで、大量のデータを効率的に収集できます。
ただし、サーバーへの負荷に配慮し、適切な設定で使用してください。