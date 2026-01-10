# Stock Value Notifier

東証上場銘柄の中からバリュー銘柄を自動的に抽出し、毎日定時にSlackチャンネルに通知するシステムです。

## 🚀 概要

このシステムは以下の機能を提供します：

- **自動データ取得**: yfinanceライブラリを使用して東証銘柄の株価・財務データを取得
- **包括的銘柄カバレッジ**: 約130銘柄（日次）または全銘柄約3,800銘柄（週次）を分析
- **スマート銘柄ローテーション**: 全銘柄を5つのグループに分割し、曜日別に効率的にスクリーニング
- **スマートキャッシュ**: データキャッシュによる高速化とAPI制限回避
- **バリュー銘柄スクリーニング**: PER、PBR、配当利回り等の複数指標による銘柄抽出
- **進捗通知**: 長時間実行時の進捗をSlackでリアルタイム通知
- **Slack通知**: 日本語・英語両対応の自動通知機能
- **GitHub Actions**: 平日午前9時（日次）・日曜午前6時（週次全銘柄）の自動実行

## 📅 実行スケジュール

### 日次ローテーションスクリーニング（平日午前9時）
- **対象**: 全銘柄を5つのグループに分割し、曜日別に実行
  - 月曜日: グループ1（約760銘柄）
  - 火曜日: グループ2（約760銘柄）
  - 水曜日: グループ3（約760銘柄）
  - 木曜日: グループ4（約760銘柄）
  - 金曜日: グループ5（約760銘柄）
- **期間**: 過去3年間のデータ
- **実行時間**: 約5-10分
- **通知**: 開始通知 + 100銘柄ごとの進捗通知 + 曜日グループ情報付きの結果通知

### 日次厳選スクリーニング（手動実行）
- **対象**: 厳選130銘柄
- **期間**: 過去3年間のデータ
- **実行時間**: 約10-15分
- **通知**: 結果のみ

### 週次全銘柄スクリーニング（日曜午前6時）
- **対象**: 東証全銘柄（約3,800銘柄）
- **期間**: 過去1年間のデータ（高速化のため）
- **実行時間**: 約2-4時間
- **通知**: 開始通知 + 100銘柄ごとの進捗通知 + 結果通知

### 手動実行
- GitHub Actionsから手動でトリガー可能
- `curated`（130銘柄）、`all`（全銘柄）、または`rotation`（ローテーション）を選択可能

## 📋 スクリーニング条件

システムは以下の条件でバリュー銘柄を抽出します：
- **PER**: 15倍以下
- **配当利回り**: 2%以上
- **継続増配**: 過去3年間継続
- **PER安定性**: 変動係数30%以下
- **継続増収**: 過去3年間継続
- **継続増益**: 過去3年間継続

## 🛠️ セットアップ

### 前提条件

- GitHub アカウント
- Slack ワークスペースとBot Token
- Python 3.10+ (ローカル開発の場合)

### 1. リポジトリのフォーク

このリポジトリを自分のGitHubアカウントにフォークしてください。

### 2. GitHub Secrets の設定

リポジトリの設定でSecrets and variablesを設定します：

#### 必須設定

1. **GitHub リポジトリの設定画面に移動**
   - リポジトリページで `Settings` タブをクリック
   - 左サイドバーの `Secrets and variables` > `Actions` をクリック

2. **以下のSecretsを追加**:

   **`SLACK_BOT_TOKEN`** (必須)
   ```
   xoxb-your-slack-bot-token-here
   ```
   
   **`SLACK_CHANNEL`** (必須)
   ```
   #your-channel-name
   ```
   または Channel ID:
   ```
   C1234567890
   ```

#### Slack Bot Token の取得方法

1. **Slack App の作成**:
   - https://api.slack.com/apps にアクセス
   - `Create New App` をクリック
   - `From scratch` を選択
   - App名を入力（例：`Stock Value Notifier`）
   - ワークスペースを選択

2. **Bot Token Scopes の設定**:
   - 左サイドバーの `OAuth & Permissions` をクリック
   - `Scopes` セクションの `Bot Token Scopes` で以下を追加：
     - `chat:write` - メッセージ送信権限
     - `chat:write.public` - パブリックチャンネルへの送信権限

3. **Bot Token の取得**:
   - `Install to Workspace` をクリック
   - 権限を確認して `Allow` をクリック
   - `Bot User OAuth Token` をコピー（`xoxb-` で始まる）

4. **チャンネルへのBot追加**:
   - 通知したいSlackチャンネルで `/invite @your-bot-name` を実行
   - または、チャンネル設定の `Integrations` タブからBotを追加

### 3. オプション設定

環境変数で以下の設定をカスタマイズできます（GitHub Secrets または Variables で設定）：

```bash
# スクリーニング条件
MAX_PER=15.0                    # PER上限
MAX_PBR=1.5                     # PBR上限
MIN_DIVIDEND_YIELD=2.0          # 配当利回り下限
MIN_GROWTH_YEARS=3              # 継続成長年数
MAX_PER_VOLATILITY=30.0         # PER変動係数上限

# スクリーニング範囲
SCREENING_MODE=rotation         # "curated" (130銘柄), "all" (全銘柄 ~3800), "rotation" (ローテーション)

# Slack設定
SLACK_USERNAME=バリュー株通知Bot    # Bot表示名
SLACK_ICON_EMOJI=:chart_with_upwards_trend:  # Botアイコン
```

#### ローテーションスクリーニングについて

`SCREENING_MODE=rotation` を設定すると、効率的な銘柄ローテーション機能を利用できます：

**仕組み**:
- 東証全銘柄（約3,800銘柄）を5つの均等なグループに分割
- 各曜日に異なるグループを自動選択してスクリーニング
- 1週間で全銘柄をカバーする効率的なアプローチ

**メリット**:
- **高速実行**: 1日あたり約760銘柄で実行時間を短縮（5-10分）
- **包括的カバレッジ**: 1週間で全銘柄を網羅
- **API制限回避**: 1日あたりの処理量を制限してyfinanceの制限を回避
- **進捗追跡**: 曜日グループ情報で週次進捗を可視化
- **リアルタイム通知**: 100銘柄ごとの進捗通知で実行状況を監視

**スケジュール**:
- 月曜日: グループ1（銘柄コード順 1-760番目）
- 火曜日: グループ2（銘柄コード順 761-1520番目）
- 水曜日: グループ3（銘柄コード順 1521-2280番目）
- 木曜日: グループ4（銘柄コード順 2281-3040番目）
- 金曜日: グループ5（銘柄コード順 3041番目以降）

**推奨設定**:
- 日次実行には `rotation` モード（デフォルト）
- 包括的分析には週次 `all` モード
- 高速テストには `curated` モード

#### 全銘柄スクリーニングについて

`SCREENING_MODE=all` を設定すると、東証全銘柄（約3,800銘柄）をスクリーニングできます：

**メリット**:
- より多くの隠れたバリュー銘柄を発見可能
- 小型株・新興株も含めた包括的な分析

**注意点**:
- **実行時間**: 数時間かかる可能性があります
- **API制限**: yfinanceのレート制限により失敗する可能性があります
- **GitHub Actions制限**: 6時間の実行時間制限があります

**推奨設定**:
- 初回は `curated` モードで動作確認
- 必要に応じて `all` モードに変更

### 4. ワークフローの有効化

1. フォークしたリポジトリの `Actions` タブに移動
2. `I understand my workflows, go ahead and enable them` をクリック
3. `Daily Value Stock Screening` ワークフローが表示されることを確認

## 🔄 使用方法

### 自動実行

- **スケジュール**: 平日午前9時（JST）に自動実行
- **実行条件**: 市場営業日のみ（祝日・休場日は自動スキップ）
- **通知内容**: 
  - バリュー銘柄が見つかった場合：銘柄詳細情報
  - 銘柄が見つからない場合：「本日はバリュー銘柄が見つかりませんでした」

### 手動実行

テスト目的で手動実行することも可能です：

1. リポジトリの `Actions` タブに移動
2. `Daily Value Stock Screening` ワークフローを選択
3. `Run workflow` ボタンをクリック
4. `Run workflow` を再度クリックして実行

### ローカル実行

開発・テスト目的でローカル実行する場合：

```bash
# リポジトリをクローン
git clone https://github.com/your-username/stock-value-notifier.git
cd stock-value-notifier

# 依存関係をインストール
pip install -r requirements.txt

# 環境変数を設定
export SLACK_BOT_TOKEN="xoxb-your-token"
export SLACK_CHANNEL="#your-channel"

# 実行
python main.py
```

## 📊 通知例

### ローテーションモード - 開始通知

```
🔄 ローテーションスクリーニング開始 - 月曜日グループ（1/5）

本日の分析対象: 760 銘柄 (月曜日)
予想実行時間: 5-10分

🔄 Rotation Screening Started - Monday Group (1/5)

Today's target: 760 stocks (Monday)
Estimated time: 5-10 minutes

📅 週次進捗 / Weekly Progress: 1/5 完了予定
```

### ローテーションモード - 進捗通知

```
📊 スクリーニング進捗 / Screening Progress

進捗: 200 / 760 銘柄 (26.3%)
Progress: 200 / 760 stocks (26.3%)

現在処理中: トヨタ自動車
Currently processing: Toyota Motor

[█████░░░░░░░░░░░░░░░] 26.3%

直近処理銘柄 / Recent stocks:
```
ソニーグループ      | 任天堂             | KDDI
NTT               | ソフトバンクグループ | 三菱UFJ
トヨタ自動車        | ホンダ             | 日産自動車
```
```

### ローテーションモード - バリュー銘柄発見時

```
🎯 本日のバリュー銘柄 - 月曜日グループ（1/5）

1. トヨタ自動車 (7203)
┌─ 株価情報
│  現在株価: ¥2,500
│  PER: 12.5倍 | PBR: 1.2倍
│  配当利回り: 2.8%
└─ 成長実績
   継続増配: 5年 | 増収: 4年 | 増益: 3年

2. ソニーグループ (6758)
┌─ 株価情報
│  現在株価: ¥12,800
│  PER: 14.2倍 | PBR: 1.4倍
│  配当利回り: 2.1%
└─ 成長実績
   継続増配: 3年 | 増収: 3年 | 増益: 3年

📊 本日の分析対象銘柄 (760銘柄) - 月曜日
```
トヨタ自動車        | ソニーグループ      | 任天堂
ソフトバンクグループ | KDDI              | NTT
...
```

─────────────────────────────────────────────────────

🎯 Today's Value Stocks - Monday Group (1/5)

1. Toyota Motor (7203)
┌─ Stock Information
│  Current Price: ¥2,500
│  PER: 12.5x | PBR: 1.2x
│  Dividend Yield: 2.8%
└─ Growth Track Record
   Dividend Growth: 5yrs | Revenue: 4yrs | Profit: 3yrs

2. Sony Group (6758)
┌─ Stock Information
│  Current Price: ¥12,800
│  PER: 14.2x | PBR: 1.4x
│  Dividend Yield: 2.1%
└─ Growth Track Record
   Dividend Growth: 3yrs | Revenue: 3yrs | Profit: 3yrs

📊 Today's Analyzed Stocks (760 stocks) - Monday
```
Toyota Motor        | Sony Group          | Nintendo
SoftBank Group      | KDDI               | NTT
...
```
```

### ローテーションモード - バリュー銘柄なしの場合

```
📊 本日の結果 - 火曜日グループ（2/5）

本日はバリュー銘柄が見つかりませんでした。
明日も引き続き監視いたします。

📊 本日の分析対象銘柄 (760銘柄) - 火曜日
```
三菱UFJフィナンシャル | 三井住友フィナンシャル | みずほフィナンシャル
東京海上ホールディングス | MS&ADインシュアランス | SOMPOホールディングス
...
```

─────────────────────────────────────────────────────

📊 Today's Results - Tuesday Group (2/5)

No value stocks found today.
We'll continue monitoring tomorrow.

📊 Today's Analyzed Stocks (760 stocks) - Tuesday
```
Mitsubishi UFJ Financial | Sumitomo Mitsui Financial | Mizuho Financial
Tokio Marine Holdings    | MS&AD Insurance          | SOMPO Holdings
...
```
```

### 厳選モード - バリュー銘柄発見時

```
🎯 本日のバリュー銘柄

1. トヨタ自動車 (7203)
┌─ 株価情報
│  現在株価: ¥2,500
│  PER: 12.5倍 | PBR: 1.2倍
│  配当利回り: 2.8%
└─ 成長実績
   継続増配: 5年 | 増収: 4年 | 増益: 3年

2. ソニーグループ (6758)
┌─ 株価情報
│  現在株価: ¥12,800
│  PER: 14.2倍 | PBR: 1.4倍
│  配当利回り: 2.1%
└─ 成長実績
   継続増配: 3年 | 増収: 3年 | 増益: 3年

📊 分析対象銘柄 (130銘柄)
```
トヨタ自動車        | ソニーグループ      | 任天堂
ソフトバンクグループ | KDDI              | NTT
...
```
```

### バリュー銘柄なしの場合

```
📊 本日の結果

本日はバリュー銘柄が見つかりませんでした。
明日も引き続き監視いたします。

📊 分析対象銘柄 (130銘柄)
```
トヨタ自動車        | ソニーグループ      | 任天堂
ソフトバンクグループ | KDDI              | NTT
...
```
```

## 🔧 トラブルシューティング

### よくある問題

#### 1. ワークフローが実行されない

**症状**: スケジュール時刻になってもワークフローが実行されない

**原因と対処法**:
- **GitHub Actionsが無効**: リポジトリの `Actions` タブでワークフローを有効化
- **フォークの制限**: フォークしたリポジトリでは、最初の手動実行が必要な場合があります
- **リポジトリの非アクティブ**: 60日間非アクティブなリポジトリではスケジュールが停止します

**解決方法**:
```bash
# 手動でワークフローを実行
1. Actions タブ → Daily Value Stock Screening
2. Run workflow ボタンをクリック
```

#### 2. Slack通知が送信されない

**症状**: ワークフローは成功するがSlack通知が届かない

**原因と対処法**:

**A. Bot Token の問題**
```bash
# 確認事項:
- SLACK_BOT_TOKEN が xoxb- で始まっているか
- Bot Token が有効期限内か
- Bot に chat:write 権限があるか
```

**B. チャンネル設定の問題**
```bash
# 確認事項:
- チャンネル名が正しいか (#channel-name 形式)
- Bot がチャンネルに追加されているか
- チャンネルがプライベートの場合、Bot に参加権限があるか
```

**C. 権限の問題**
```bash
# Bot権限の確認:
1. Slack App設定 → OAuth & Permissions
2. Bot Token Scopes に以下が含まれているか確認:
   - chat:write
   - chat:write.public (パブリックチャンネルの場合)
```

#### 3. データ取得エラー

**症状**: "Data fetching failed" エラーが発生

**原因と対処法**:
- **yfinance API制限**: 一時的なAPI制限の可能性
- **ネットワーク問題**: GitHub Actions環境のネットワーク問題
- **銘柄コード問題**: 無効な銘柄コードの指定

**解決方法**:
```bash
# ログの確認:
1. Actions タブ → 失敗したワークフロー実行
2. "Run value stock screening" ステップのログを確認
3. エラーメッセージから原因を特定
```

#### 4. 環境変数の設定エラー

**症状**: "Missing required environment variable" エラー

**解決方法**:
```bash
# GitHub Secrets の確認:
1. Settings → Secrets and variables → Actions
2. 以下のSecretsが設定されているか確認:
   - SLACK_BOT_TOKEN
   - SLACK_CHANNEL
3. 値に余分なスペースや改行が含まれていないか確認
```

#### 5. None値比較エラー

**症状**: "TypeError: '>' not supported between instances of 'NoneType' and 'NoneType'" エラー

**原因**: 財務データにNone値が含まれている場合の比較処理エラー

**解決方法**: 
- v1.2.1で修正済み
- 古いバージョンを使用している場合は最新版に更新してください

```bash
# 最新版の取得
git pull origin main
```

#### 6. Python バージョン互換性エラー

**症状**: "TypeError: unsupported operand type(s) for |" エラー

**原因**: yfinance ライブラリが Python 3.10+ の型注釈を使用

**解決方法**: 
- GitHub Actions では Python 3.10 を使用（自動設定済み）
- ローカル開発では Python 3.10+ を使用してください

### ログの確認方法

#### GitHub Actions ログ

1. リポジトリの `Actions` タブに移動
2. 該当するワークフロー実行をクリック
3. 各ステップのログを展開して詳細を確認

#### ログファイルのダウンロード

ワークフロー実行後、ログファイルがArtifactとして保存されます：

1. ワークフロー実行ページの下部 `Artifacts` セクション
2. `screening-logs-{run-number}` をダウンロード
3. 解凍してログファイルを確認

### デバッグモード

詳細なログを有効にするには、GitHub Secrets に以下を追加：

```bash
ACTIONS_STEP_DEBUG=true
ACTIONS_RUNNER_DEBUG=true
```

## 🧪 テスト

### 単体テスト実行

```bash
# 全テスト実行
pytest

# 特定のモジュールのテスト
pytest test/test_data_fetcher.py

# カバレッジ付きテスト実行
pytest --cov=src
```

### プロパティベーステスト

```bash
# Hypothesisを使用したプロパティテスト
pytest test/ -v --hypothesis-show-statistics
```

## 📁 プロジェクト構造

```
stock-value-notifier/
├── .github/                                    # GitHub Actions設定
│   └── workflows/
│       └── daily-screening.yml                # 自動実行ワークフロー
├── src/                                        # メインソースコード
│   ├── __init__.py                            # パッケージ初期化
│   ├── cache_manager.py                       # キャッシュ管理
│   ├── config_manager.py                      # 設定管理
│   ├── data_fetcher.py                        # データ取得（yfinance）
│   ├── data_validator.py                      # データ検証
│   ├── enhanced_logger.py                     # ログ管理
│   ├── error_handler.py                       # エラーハンドリング
│   ├── error_handling_config.py               # エラーハンドリング設定
│   ├── error_metrics.py                       # エラーメトリクス
│   ├── exceptions.py                          # カスタム例外
│   ├── models.py                              # データモデル
│   ├── retry_manager.py                       # リトライ処理
│   ├── rotation_manager.py                    # 銘柄ローテーション管理
│   ├── screening_engine.py                    # スクリーニングエンジン
│   ├── slack_notifier.py                      # Slack通知
│   ├── symbol_filter.py                       # 銘柄フィルタ
│   ├── symbol_validator.py                    # 銘柄検証
│   ├── timezone_handler.py                    # タイムゾーン処理
│   ├── tse_stock_list_manager.py              # TSE銘柄リスト管理
│   ├── validation_error_processor.py          # 検証エラー処理
│   └── workflow_runner.py                     # ワークフロー実行
├── test/                                       # テストコード
│   ├── test_cache_idempotency.py              # キャッシュ冪等性テスト
│   ├── test_config_manager.py                 # 設定管理テスト
│   ├── test_data_fetcher.py                   # データ取得テスト
│   ├── test_data_validation.py                # データ検証テスト
│   ├── test_delisted_stock_handling.py        # 上場廃止銘柄処理テスト
│   ├── test_error_conditions.py               # エラー条件テスト
│   ├── test_error_handler.py                  # エラーハンドリングテスト
│   ├── test_error_handling_config.py          # エラーハンドリング設定テスト
│   ├── test_error_metrics.py                  # エラーメトリクステスト
│   ├── test_optimized_stock_fetcher.py        # 最適化データ取得テスト
│   ├── test_performance_stability.py          # パフォーマンス安定性テスト
│   ├── test_retry_manager.py                  # リトライ管理テスト
│   ├── test_rotation_properties.py            # ローテーションプロパティテスト
│   ├── test_rotation.py                       # ローテーションテスト
│   ├── test_slack_notifier.py                 # Slack通知テスト
│   ├── test_symbol_filter.py                  # 銘柄フィルタテスト
│   ├── test_tse_integration.py                # TSE統合テスト
│   ├── test_tse_performance.py                # TSEパフォーマンステスト
│   ├── test_tse_stock_list_manager.py         # TSE銘柄リストテスト
│   ├── test_yfinance_methods.py               # yfinanceメソッドテスト
│   └── test_yfinance_screener.py              # yfinanceスクリーナーテスト
├── stock_list/                                 # TSE銘柄データ
│   └── data_j.xls                             # TSE公式銘柄リスト
├── cache/                                      # キャッシュディレクトリ
│   ├── dividend_history.json                  # 配当履歴キャッシュ
│   ├── financial_info.json                    # 財務情報キャッシュ
│   └── tse_stocks_cache.json                  # TSE銘柄キャッシュ
├── logs/                                       # ログディレクトリ
│   ├── .gitkeep                               # ディレクトリ保持用
│   ├── data_retrieval_demonstration_report.md # データ取得デモレポート
│   ├── delisted_stocks.log                    # 上場廃止銘柄ログ
│   ├── errors.log                             # エラーログ
│   ├── github_actions.log                     # GitHub Actionsログ
│   ├── health.log                             # ヘルスチェックログ
│   ├── stock_notifier.log                     # メインログ
│   ├── timezone_errors.log                    # タイムゾーンエラーログ
│   ├── tse_final_checkpoint_report.md         # TSE最終チェックポイントレポート
│   ├── tse_integration_report.md              # TSE統合レポート
│   ├── tse_integration_test.log               # TSE統合テストログ
│   ├── tse_performance_report.md              # TSEパフォーマンスレポート
│   ├── tse_performance_test.log               # TSEパフォーマンステストログ
│   └── validation_errors.log                  # 検証エラーログ
├── main.py                                     # メインエントリーポイント
├── requirements.txt                            # Python依存関係
├── README.md                                   # プロジェクト説明（このファイル）
├── .gitignore                                  # Git除外設定
├── comprehensive_demo.py                       # 包括的デモスクリプト
├── simple_demo.py                             # シンプルデモスクリプト
├── calculate_stocks.py                        # 銘柄数計算分析
├── external_stock_sources.py                  # 外部データソース調査
├── final_implementation_proposal.py           # 最終実装提案
├── improved_stock_fetcher.py                  # 改良データ取得実装
├── stock_reduction_analysis.py                # 銘柄削減ロジック分析
├── validate_optimization.py                   # 最適化検証スクリプト
└── tse_stocks_cache.json                      # TSE銘柄キャッシュファイル
```

### ディレクトリ説明

#### 📂 src/ - メインソースコード
- **コア機能**: データ取得、スクリーニング、通知の主要機能
- **管理機能**: 設定、キャッシュ、エラーハンドリング、ログ管理
- **TSE統合**: 東証公式データファイル統合機能
- **ローテーション**: 効率的な銘柄ローテーション機能

#### 🧪 test/ - テストコード
- **単体テスト**: 各モジュールの機能テスト
- **統合テスト**: TSE統合、パフォーマンステスト
- **プロパティテスト**: Hypothesisを使用した包括的テスト
- **エラーテスト**: エラー条件とエッジケースのテスト

#### 📊 stock_list/ - TSE銘柄データ
- **data_j.xls**: 東証から提供される公式銘柄リストファイル
- **ETF除外**: 約370銘柄のETF・投資信託を自動除外
- **通常株式**: 約4,000銘柄の通常株式を対象

#### 💾 cache/ - キャッシュディレクトリ
- **dividend_history.json**: 配当履歴データのキャッシュ
- **financial_info.json**: 財務情報データのキャッシュ
- **tse_stocks_cache.json**: TSE銘柄リストのキャッシュ

#### 📝 logs/ - ログディレクトリ
- **実行ログ**: システムの動作ログとエラーログ
- **テストログ**: TSE統合とパフォーマンステストのログ
- **レポート**: 詳細な分析レポート（Markdown形式）

#### 🔧 分析・最適化スクリプト
- **デモスクリプト**: 機能確認用のデモ実行スクリプト
- **分析スクリプト**: 銘柄数計算、削減ロジック分析
- **最適化スクリプト**: パフォーマンス最適化と検証
- **調査スクリプト**: 外部データソースの調査結果

## 🔒 セキュリティ

### Secrets の管理

- **絶対にコードにトークンを含めない**: 全ての認証情報はGitHub Secretsで管理
- **最小権限の原則**: Slack Botには必要最小限の権限のみ付与
- **定期的なトークン更新**: セキュリティのため定期的にBot Tokenを更新

### プライベートリポジトリの推奨

本番環境では、リポジトリをプライベートに設定することを推奨します：

1. Settings → General → Danger Zone
2. Change repository visibility → Make private

## 🤝 コントリビューション

1. このリポジトリをフォーク
2. フィーチャーブランチを作成 (`git checkout -b feature/amazing-feature`)
3. 変更をコミット (`git commit -m 'Add amazing feature'`)
4. ブランチにプッシュ (`git push origin feature/amazing-feature`)
5. プルリクエストを作成

## 📄 ライセンス

このプロジェクトはMITライセンスの下で公開されています。詳細は [LICENSE](LICENSE) ファイルを参照してください。

## 📞 サポート

問題が発生した場合：

1. **GitHub Issues**: バグ報告や機能要望は [Issues](https://github.com/your-username/stock-value-notifier/issues) で報告
2. **ドキュメント**: このREADMEのトラブルシューティングセクションを確認
3. **ログ確認**: GitHub ActionsのログやArtifactを確認

## 🔄 更新履歴

### v1.2.1 (2025-01-10) - エラーハンドリング改善
- **None値処理の修正**:
  - 財務データにNone値が含まれる場合のTypeError修正
  - `_calculate_revenue_growth_years()`, `_calculate_profit_growth_years()`, `_calculate_dividend_growth_years()`, `calculate_per_stability()`でのNone値チェック追加
  - None値を0に変換してから比較処理を実行
  - `TypeError: '>' not supported between instances of 'NoneType' and 'NoneType'`エラーを解決
- **安定性向上**:
  - 不完全な財務データでもスクリーニング処理が継続可能
  - データ品質に関わらず安定したシステム動作を実現

### v1.2.0 (2024-01-XX) - 銘柄ローテーション機能
- **銘柄ローテーション機能**:
  - 全銘柄を5つのグループに自動分割
  - 曜日別の効率的なスクリーニング（月曜日〜金曜日）
  - 1週間で全銘柄をカバーする包括的アプローチ
- **スマートグループ分割**:
  - 銘柄コード順による均等分割アルゴリズム
  - グループ間の銘柄数差を最小化（±1銘柄以内）
  - 再現可能な一貫したグループ分割
- **進捗追跡機能**:
  - 曜日グループ情報の通知表示
  - 100銘柄ごとの進捗通知（処理中銘柄名と進捗バー表示）
  - 週次進捗の可視化（例：「月曜日グループ（1/5）」）
  - ローテーションスケジュールの管理
- **GitHub Actions統合**:
  - 平日自動実行でローテーションモードをデフォルト化
  - 手動実行時のモード選択オプション拡張
  - 既存のcurated/allモードとの完全互換性
- **パフォーマンス最適化**:
  - 1日あたり約760銘柄で実行時間を5-10分に短縮
  - API制限回避による安定性向上
  - 効率的なメモリ使用量管理

### v1.1.0 (2024-01-XX) - 週次全銘柄分析対応
- **週次全銘柄スクリーニング機能**:
  - 毎週日曜午前6時に全銘柄（約3,800銘柄）を自動分析
  - 日次は厳選130銘柄で高速実行を維持
- **スマートキャッシュシステム**:
  - 財務データと配当履歴をJSONファイルでキャッシュ
  - GitHub Artifactsによる週次データ保存
  - API呼び出し削減による高速化と安定性向上
- **進捗通知機能**:
  - 長時間実行時の開始通知
  - 100銘柄ごとの進捗通知（処理中銘柄名と進捗バー表示）
  - 直近処理銘柄の一覧表示
- **パフォーマンス最適化**:
  - 全銘柄分析時は過去1年データに短縮（3年→1年）
  - キャッシュヒット率向上による実行時間短縮
- **GitHub Actions改善**:
  - スケジュールベースの自動モード切替
  - 手動実行時のモード選択オプション
  - データキャッシュの自動アップロード/ダウンロード

### v1.0.5 (2024-01-XX)
- 全銘柄スクリーニング機能を追加
  - `SCREENING_MODE=all` で東証全銘柄（約3,800銘柄）をスクリーニング可能
  - デフォルトは `curated` モード（130銘柄）で高速実行
  - 実行時間とAPI制限に関する警告とガイダンスを追加

### v1.0.4 (2024-01-XX)
- 分析対象銘柄を大幅拡張
  - 20銘柄 → 約130銘柄に拡大
  - 大型株、中型株、テック株、金融株、消費財株、不動産株を包括的にカバー
  - セクター別分散でより幅広いバリュー銘柄発見が可能

### v1.0.3 (2024-01-XX)
- Slack通知に取得した全銘柄名を追加
  - バリュー銘柄発見時：バリュー銘柄詳細 + 分析対象銘柄一覧
  - 銘柄なしの場合：分析対象銘柄一覧のみ
  - 銘柄名を3列形式で見やすく表示

### v1.0.2 (2024-01-XX)
- Python バージョンを 3.9 から 3.10 に更新
  - yfinance ライブラリの型注釈互換性のため
  - Python 3.10+ の Union 型構文 (`|`) をサポート

### v1.0.1 (2024-01-XX)
- GitHub Actions の非推奨アクション修正
  - `actions/setup-python@v4` → `v5` に更新
  - `actions/cache@v3` → `v4` に更新  
  - `actions/upload-artifact@v3` → `v4` に更新
- 不要な環境変数 `J_QUANTS_API_TOKEN` を削除

### v1.0.0 (2024-01-XX)
- 初回リリース
- yfinanceを使用したデータ取得機能
- 多条件バリュー銘柄スクリーニング
- 日英両対応Slack通知
- GitHub Actions自動実行

---

**注意**: このシステムは投資判断の参考情報を提供するものであり、投資の推奨や保証を行うものではありません。投資は自己責任で行ってください。
