# 移行ガイド

## 概要

このガイドでは、既存のスクレイパー実装から新しいリファクタリングされたアーキテクチャへの移行方法を説明します。

## 主な変更点

### 1. ディレクトリ構造の変更

**旧構造:**
```
restaurant-scraper/
├── tabelog_scraper_async_v2.py
├── restaurant_scraper_app_fast_v2.py
├── restaurant_data_integrator.py
└── その他のファイル...
```

**新構造:**
```
restaurant-scraper/
├── config/
├── scrapers/
├── utils/
├── tests/
└── 既存のファイル（後方互換性のため保持）
```

### 2. コードの分割と整理

#### 設定管理

**Before:**
```python
# ファイル内で直接定義
AREA_URLS = {
    '東京都': 'https://tabelog.com/tokyo/',
    # ...
}

MAX_CONCURRENT = 50
TIMEOUT = 30
```

**After:**
```python
from config.constants import AREA_URLS
from config.settings import settings

# 環境変数で設定可能
max_concurrent = settings.max_concurrent
timeout = settings.timeout
```

#### エラーハンドリング

**Before:**
```python
try:
    # 処理
except Exception as e:
    print(f"エラー: {e}")
    # 手動でリトライ
```

**After:**
```python
from utils.error_handler import retry_on_error, NetworkError

@retry_on_error(exceptions=(NetworkError,), max_attempts=3)
async def fetch_page(url):
    # 自動リトライ付き
    ...
```

#### データバリデーション

**Before:**
```python
# 各所で個別に実装
phone = re.sub(r'[^\d\-]', '', phone) if phone else ""
```

**After:**
```python
from utils.validators import DataValidator, RestaurantData

# 統一されたバリデーション
phone = DataValidator.normalize_phone_number(phone)

# 型安全なデータ
data = RestaurantData(
    shop_name=name,
    phone=phone,
    # ...
)
```

## 移行手順

### ステップ1: 環境設定

1. 新しいディレクトリ構造を作成:
```bash
mkdir -p config scrapers utils tests
```

2. 環境変数ファイルを作成（オプション）:
```bash
cat > .env << EOF
MAX_CONCURRENT=50
TIMEOUT=60
DELAY_MIN=1.0
DELAY_MAX=3.0
BATCH_SIZE=20
LOG_LEVEL=INFO
EOF
```

### ステップ2: 既存コードのリファクタリング

#### 例: tabelog_scraper_async_v2.pyの移行

**1. インポートの更新:**
```python
# Before
import aiohttp
import asyncio
from bs4 import BeautifulSoup
# 独自の定数定義

# After
from scrapers.base import BaseTabelogScraper
from utils.validators import RestaurantData, DataValidator
from utils.error_handler import retry_on_error, NetworkError
from utils.progress import BatchProgressTracker
from config.settings import settings
from config.constants import USER_AGENTS
```

**2. クラスの継承:**
```python
# Before
class TabelogScraperAsyncV2:
    def __init__(self):
        # 独自の初期化

# After
class TabelogAsyncScraper(BaseTabelogScraper):
    def __init__(self, max_concurrent: Optional[int] = None):
        super().__init__()  # 基底クラスの初期化
        # 追加の初期化
```

**3. 抽出メソッドの使用:**
```python
# Before
# 各クラスで独自に実装
def extract_shop_name(self, soup):
    # 独自のロジック

# After
# 基底クラスのメソッドを使用
shop_name = self._extract_shop_name(soup)
# 必要に応じてオーバーライド可能
```

### ステップ3: テストの追加

新しいテストファイルを使用してコードの動作を検証:

```bash
# 個別のテストを実行
python -m unittest tests.test_validators
python -m unittest tests.test_error_handler

# すべてのテストを実行
python -m unittest discover tests
```

### ステップ4: 段階的な移行

1. **並行運用期間:**
   - 既存のファイルはそのまま残す
   - 新しい実装を別名で作成
   - 徐々に新しい実装に切り替え

2. **互換性レイヤーの作成:**
```python
# compatibility.py
from scrapers.async_scraper import TabelogAsyncScraper

# 既存のインターフェースを維持
class TabelogScraperAsyncV2:
    def __init__(self):
        self.scraper = TabelogAsyncScraper()
    
    async def scrape_restaurants(self, areas, max_per_area):
        async with self.scraper as s:
            return await s.scrape_restaurants(areas, max_per_area)
```

## 新機能の活用

### 1. 環境変数による設定

```bash
# 実行時に設定を上書き
MAX_CONCURRENT=100 python your_script.py
```

### 2. 進捗の永続化

```python
# 自動的に進捗が保存され、中断から再開可能
scraper = TabelogAsyncScraper()
# 中断しても、次回実行時に続きから開始
```

### 3. 詳細なエラー統計

```python
# エラー統計の取得
stats = scraper.error_handler.get_error_stats()
print(f"総エラー数: {stats['total_errors']}")
print(f"エラータイプ: {stats['error_types']}")
```

### 4. 型安全なデータ処理

```python
from utils.validators import RestaurantData

# IDEの補完とタイプチェックが効く
data = RestaurantData(
    shop_name="テストレストラン",
    phone="03-1234-5678"
)

# バリデーション
if data.is_valid():
    result = data.to_dict()
```

## トラブルシューティング

### インポートエラー

```python
# プロジェクトルートからの実行を確認
import sys
sys.path.append('/path/to/restaurant-scraper')
```

### 設定の不一致

```python
# 設定値の確認
from config.settings import settings
print(f"MAX_CONCURRENT: {settings.max_concurrent}")
print(f"TIMEOUT: {settings.timeout}")
```

### パフォーマンスの問題

```python
# 設定の調整
export MAX_CONCURRENT=30  # 接続数を減らす
export BATCH_SIZE=10      # バッチサイズを小さく
```

## ベストプラクティス

1. **段階的な移行:**
   - 一度にすべてを変更せず、モジュールごとに移行
   - テストを書きながら進める

2. **後方互換性の維持:**
   - 既存のAPIを変更する場合は互換性レイヤーを提供
   - 非推奨警告を追加

3. **ドキュメントの更新:**
   - 変更点をREADMEに記載
   - コード例を新しい実装に更新

4. **継続的なテスト:**
   - 移行の各段階でテストを実行
   - パフォーマンスベンチマークを取る

## まとめ

この移行により、以下のメリットが得られます：

- **保守性の向上**: モジュール化されたコード構造
- **拡張性**: 新しいスクレイパーの追加が容易
- **信頼性**: 堅牢なエラーハンドリングと進捗管理
- **開発効率**: 型ヒントとテストによる品質保証

段階的に移行を進めることで、既存の機能を維持しながら、新しいアーキテクチャの恩恵を受けることができます。