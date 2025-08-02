# API リファレンス

## scrapers.base

### class BaseTabelogScraper

食べログスクレイパーの基底クラス。共通の抽出ロジックを提供します。

#### メソッド

##### `__init__(self)`
スクレイパーを初期化します。

##### `get_area_urls(self) -> Dict[str, str]`
対応エリアのURLマッピングを返します。

**戻り値:**
- `Dict[str, str]`: エリア名をキー、URLを値とする辞書

##### `_extract_shop_name(self, soup: BeautifulSoup) -> str`
HTMLから店名を抽出します。

**引数:**
- `soup`: BeautifulSoupオブジェクト

**戻り値:**
- `str`: 店名（見つからない場合は空文字列）

##### `_extract_phone(self, soup: BeautifulSoup) -> str`
HTMLから電話番号を抽出します。

**引数:**
- `soup`: BeautifulSoupオブジェクト

**戻り値:**
- `str`: 電話番号（見つからない場合は空文字列）

##### `_extract_address(self, soup: BeautifulSoup) -> str`
HTMLから住所を抽出します。

**引数:**
- `soup`: BeautifulSoupオブジェクト

**戻り値:**
- `str`: 住所（見つからない場合は空文字列）

##### `_extract_genre(self, soup: BeautifulSoup) -> str`
HTMLからジャンル情報を抽出します。駅名を除外します。

**引数:**
- `soup`: BeautifulSoupオブジェクト

**戻り値:**
- `str`: ジャンル（カンマ区切り、見つからない場合は空文字列）

##### `_extract_station(self, soup: BeautifulSoup) -> str`
HTMLから最寄駅情報を抽出します。

**引数:**
- `soup`: BeautifulSoupオブジェクト

**戻り値:**
- `str`: 最寄駅情報（見つからない場合は空文字列）

##### `_extract_open_time(self, soup: BeautifulSoup) -> str`
HTMLから営業時間を抽出します。

**引数:**
- `soup`: BeautifulSoupオブジェクト

**戻り値:**
- `str`: 営業時間（見つからない場合は空文字列）

## scrapers.async_scraper

### class TabelogAsyncScraper(BaseTabelogScraper)

非同期処理による高速な食べログスクレイパー。

#### メソッド

##### `__init__(self, max_concurrent: Optional[int] = None)`
非同期スクレイパーを初期化します。

**引数:**
- `max_concurrent`: 最大同時接続数（デフォルト: 設定値）

##### `async scrape_restaurants(self, areas: List[str], max_per_area: int) -> List[Dict]`
指定エリアからレストラン情報を取得します。

**引数:**
- `areas`: 対象エリアのリスト
- `max_per_area`: エリアあたりの最大取得件数

**戻り値:**
- `List[Dict]`: レストラン情報の辞書リスト

**使用例:**
```python
async with TabelogAsyncScraper() as scraper:
    restaurants = await scraper.scrape_restaurants(
        areas=['東京都', '大阪府'],
        max_per_area=100
    )
```

## utils.validators

### @dataclass RestaurantData

レストランデータを表現するデータクラス。

**フィールド:**
- `shop_name: str` - 店名（必須）
- `phone: str` - 電話番号
- `address: str` - 住所
- `genre: str` - ジャンル
- `station: str` - 最寄駅
- `open_time: str` - 営業時間
- `url: str` - URL
- `source: str` - データソース（デフォルト: "食べログ"）
- `scraped_at: str` - スクレイピング日時（自動設定）

**メソッド:**

##### `to_dict(self) -> Dict[str, str]`
データクラスを辞書に変換します。

##### `is_valid(self) -> bool`
データの基本的な妥当性をチェックします。

### class DataValidator

データバリデーション用のユーティリティクラス。

#### 静的メソッド

##### `validate_phone_number(phone: str) -> bool`
電話番号の形式を検証します。

**引数:**
- `phone`: 検証する電話番号

**戻り値:**
- `bool`: 妥当な場合True

##### `validate_url(url: str) -> bool`
食べログURLの形式を検証します。

**引数:**
- `url`: 検証するURL

**戻り値:**
- `bool`: 妥当な場合True

##### `validate_address(address: str) -> bool`
住所の妥当性を検証します。

**引数:**
- `address`: 検証する住所

**戻り値:**
- `bool`: 妥当な場合True

##### `validate_restaurant_data(data: Union[Dict, RestaurantData]) -> tuple[bool, List[str]]`
レストランデータ全体を検証します。

**引数:**
- `data`: 検証するデータ（辞書またはRestaurantDataインスタンス）

**戻り値:**
- `tuple[bool, List[str]]`: (妥当性, エラーメッセージのリスト)

##### `clean_text(text: str, max_length: Optional[int] = None) -> str`
テキストをクリーンアップします。

**引数:**
- `text`: クリーンアップするテキスト
- `max_length`: 最大長（指定時は切り詰め）

**戻り値:**
- `str`: クリーンアップされたテキスト

##### `normalize_phone_number(phone: str) -> str`
電話番号を正規化します。

**引数:**
- `phone`: 正規化する電話番号

**戻り値:**
- `str`: 正規化された電話番号

## utils.error_handler

### 例外クラス

#### class ScraperError(Exception)
スクレイパーの基底例外クラス。

#### class NetworkError(ScraperError)
ネットワーク関連のエラー。

#### class ParseError(ScraperError)
パース関連のエラー。

#### class RateLimitError(ScraperError)
レート制限エラー。

### class ErrorHandler

エラーハンドリングを提供するクラス。

#### メソッド

##### `__init__(self, max_retries: int = 3, retry_delay: float = 1.0)`
エラーハンドラーを初期化します。

**引数:**
- `max_retries`: 最大リトライ回数
- `retry_delay`: リトライ間隔（秒）

##### `log_error_with_context(self, error: Exception, context: str, url: Optional[str] = None)`
コンテキスト付きでエラーをログに記録します。

**引数:**
- `error`: 発生したエラー
- `context`: エラーが発生したコンテキスト
- `url`: 関連するURL（オプション）

##### `get_error_stats(self) -> dict`
エラー統計情報を取得します。

**戻り値:**
- `dict`: エラー統計（総数、種類別カウント、最近のエラー）

### デコレータ

#### `@retry_on_error`
エラー時に自動的にリトライするデコレータ。

**引数:**
- `exceptions`: リトライ対象の例外タプル
- `max_attempts`: 最大試行回数
- `delay`: 初回リトライまでの遅延（秒）
- `backoff`: リトライごとの遅延倍率

**使用例:**
```python
@retry_on_error(exceptions=(NetworkError,), max_attempts=3)
async def fetch_page(url):
    # ネットワークエラー時に最大3回リトライ
    ...
```

#### `@handle_errors`
エラーをキャッチして処理するデコレータ。

**引数:**
- `default_return`: エラー時のデフォルト返り値
- `log_errors`: エラーをログに記録するか
- `raise_errors`: エラーを再発生させるか

### class NetworkErrorHandler

ネットワークエラー専用のハンドラー。

#### 静的メソッド

##### `is_rate_limit_error(error: Exception) -> bool`
レート制限エラーかどうかを判定します。

##### `is_timeout_error(error: Exception) -> bool`
タイムアウトエラーかどうかを判定します。

##### `is_connection_error(error: Exception) -> bool`
接続エラーかどうかを判定します。

##### `get_retry_delay(error: Exception, base_delay: float = 1.0) -> float`
エラータイプに基づいてリトライ遅延を計算します。

## utils.progress

### class ProgressMixin

進捗管理機能を提供するMixinクラス。

#### メソッド

##### `__init__(self, progress_file: str, results_file: str)`
進捗管理を初期化します。

**引数:**
- `progress_file`: 進捗ファイルのパス
- `results_file`: 結果ファイルのパス

##### `load_progress(self) -> bool`
前回の進捗を読み込みます。

**戻り値:**
- `bool`: 読み込みに成功した場合True

##### `save_progress(self) -> bool`
現在の進捗を保存します。

**戻り値:**
- `bool`: 保存に成功した場合True

##### `is_processed(self, url: str) -> bool`
URLが処理済みかどうかをチェックします。

##### `mark_as_processed(self, url: str)`
URLを処理済みとしてマークします。

##### `add_result(self, result: Dict)`
結果を追加します。

##### `clear_progress(self)`
進捗をクリアします。

##### `get_stats(self) -> Dict`
進捗統計を取得します。

### class BatchProgressTracker(ProgressMixin)

バッチ処理用の拡張進捗トラッカー。

#### メソッド

##### `__init__(self, progress_file: str, results_file: str, batch_size: int = 20)`
バッチ進捗トラッカーを初期化します。

**引数:**
- `progress_file`: 進捗ファイルのパス
- `results_file`: 結果ファイルのパス
- `batch_size`: バッチサイズ

##### `set_total_items(self, total: int)`
総アイテム数を設定します。

##### `start_batch(self, batch_num: int, start_idx: int, end_idx: int)`
バッチ処理の開始をログに記録します。

##### `complete_batch(self)`
バッチ処理の完了を記録し、進捗を保存します。

##### `get_remaining_items(self) -> int`
残りアイテム数を取得します。

##### `get_progress_percentage(self) -> float`
進捗率（パーセント）を取得します。

## config.settings

### class Settings

環境変数から設定を読み込むクラス。

**プロパティ:**
- `max_concurrent: int` - 最大同時接続数
- `timeout: int` - タイムアウト（秒）
- `delay_min: float` - 最小遅延（秒）
- `delay_max: float` - 最大遅延（秒）
- `batch_size: int` - バッチサイズ
- `cache_dir: str` - キャッシュディレクトリ
- `log_level: str` - ログレベル

**メソッド:**

##### `get_delay_range(self) -> tuple[float, float]`
遅延範囲のタプルを返します。

## config.logging_config

### class LoggerMixin

ロギング機能を提供するMixinクラス。

**プロパティ:**
- `logger: logging.Logger` - ロガーインスタンス

**メソッド:**
- `log_info(message: str)` - INFOレベルでログ出力
- `log_warning(message: str)` - WARNINGレベルでログ出力
- `log_error(message: str, exc_info: bool = False)` - ERRORレベルでログ出力
- `log_debug(message: str)` - DEBUGレベルでログ出力