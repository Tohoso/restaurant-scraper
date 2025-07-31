# 飲食店営業リスト作成アプリ 使用ガイド

## 🚀 クイックスタート

### 対話モード（推奨）
```bash
./run.sh
```
または
```bash
python3 restaurant_scraper_app.py -i
```

### コマンドラインモード
```bash
# 東京都の飲食店を50件取得
python3 restaurant_scraper_app.py --areas 東京都 --max-per-area 50

# 複数地域から取得
python3 restaurant_scraper_app.py --areas 東京都 大阪府 神奈川県 --max-per-area 100
```

## 📝 初期化完了情報

- **初期化日時**: 2025-07-31 21:38:52
- **Python バージョン**: 3.9.6
- **作業ディレクトリ**: /Users/invis/Projects/taberogu/restaurant-scraper

## 📁 ディレクトリ構造

```
restaurant-scraper/
├── restaurant_scraper_app.py    # メインアプリケーション
├── hotpepper_api_client.py      # ホットペッパーAPI
├── tabelog_scraper_improved.py  # 食べログスクレイパー
├── restaurant_data_integrator.py # データ統合
├── .serena/                     # SerenaMCP設定
├── logs/                        # ログファイル
├── output/                      # 出力ファイル
├── cache/                       # キャッシュ
└── run.sh                       # クイックスタート
```

## 🔧 トラブルシューティング

問題が発生した場合は、以下を確認してください：

1. Python 3.8以上がインストールされているか
2. すべての依存関係がインストールされているか
3. インターネット接続が有効か
4. `logs/`フォルダ内のログファイル

## 📞 サポート

READMEファイルを参照してください。
