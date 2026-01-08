# Stock Value Notifier

東証上場銘柄の中からバリュー銘柄を自動的に抽出し、毎日定時にSlackチャンネルに通知するシステムです。

## 🚀 概要

このシステムは以下の機能を提供します：

- **自動データ取得**: yfinanceライブラリを使用して東証銘柄の株価・財務データを取得
- **バリュー銘柄スクリーニング**: PER、PBR、配当利回り等の複数指標による銘柄抽出
- **Slack通知**: 日本語・英語両対応の自動通知機能
- **GitHub Actions**: 平日午前9時の自動実行（祝日・市場休場日は除く）

## 📋 スクリーニング条件

システムは以下の条件でバリュー銘柄を抽出します：

- **PER**: 15倍以下
- **PBR**: 1.5倍以下
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

# Slack設定
SLACK_USERNAME=バリュー株通知Bot    # Bot表示名
SLACK_ICON_EMOJI=:chart_with_upwards_trend:  # Botアイコン
```

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

### バリュー銘柄発見時

```
🎯 バリュー銘柄が見つかりました！

📈 7203 - トヨタ自動車
💰 現在株価: ¥2,500
📊 PER: 12.5倍, PBR: 1.2倍, 配当利回り: 2.8%
🔄 継続増配: 5年, 増収: 4年, 増益: 3年
📈 PER安定性: 18.5% (変動係数)

📈 6758 - ソニーグループ
💰 現在株価: ¥12,800
📊 PER: 14.2倍, PBR: 1.4倍, 配当利回り: 2.1%
🔄 継続増配: 3年, 増収: 3年, 増益: 3年
📈 PER安定性: 25.3% (変動係数)
```

### バリュー銘柄なしの場合

```
📊 本日はバリュー銘柄が見つかりませんでした

市場の状況により、設定された条件を満たす銘柄がありませんでした。
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

#### 5. Python バージョン互換性エラー

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
├── .github/
│   └── workflows/
│       └── daily-screening.yml    # GitHub Actions ワークフロー
├── src/                           # ソースコード
│   ├── __init__.py
│   ├── config_manager.py          # 設定管理
│   ├── data_fetcher.py            # データ取得
│   ├── models.py                  # データモデル
│   ├── screening_engine.py        # スクリーニングエンジン
│   ├── slack_notifier.py          # Slack通知
│   └── workflow_runner.py         # ワークフロー実行
├── test/                          # テストコード
│   ├── test_config_manager.py
│   ├── test_data_fetcher.py
│   └── test_slack_notifier.py
├── logs/                          # ログファイル
├── main.py                        # メイン実行スクリプト
├── requirements.txt               # 依存関係
└── README.md                      # このファイル
```

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
