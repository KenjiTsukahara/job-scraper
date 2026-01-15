# 求人自動収集システム

東京都の求人情報を毎日自動で収集するシステムです。GitHub Actionsを使用して**完全無料**で運用できます。

## 機能

- **16業種**の求人を自動収集
- **24時間以内の新着求人**をフィルタリング
- **毎日午前9時（日本時間）**に自動実行
- CSV/JSON形式で保存
- 重複除去機能付き

## 対象業種

| 業種 | 検索キーワード |
|------|----------------|
| 歯科 | 歯科衛生士 |
| 介護 | 介護職 |
| 医療事務 | 医療事務 |
| 保育 | 保育士 |
| 美容 | 美容師 |
| 建設 | 建設作業員 |
| 事務 | 一般事務 |
| 経理 | 経理事務 |
| 看護 | 看護師 |
| 治療家 | 柔道整復師 |
| ドライバー | ドライバー |
| 警備 | 警備員 |
| 製造 | 製造スタッフ |
| 清掃 | 清掃スタッフ |
| 派遣 | 派遣スタッフ |
| 飲食 | 飲食スタッフ |

## セットアップ手順

### 1. リポジトリをフォーク

このリポジトリをあなたのGitHubアカウントにフォークします。

### 2. GitHub Actionsを有効化

1. フォークしたリポジトリの「Settings」タブを開く
2. 左メニューの「Actions」→「General」を選択
3. 「Allow all actions and reusable workflows」を選択
4. 「Save」をクリック

### 3. ワークフローの権限設定

1. 「Settings」→「Actions」→「General」
2. 「Workflow permissions」セクションで「Read and write permissions」を選択
3. 「Save」をクリック

### 4. 手動実行でテスト

1. 「Actions」タブを開く
2. 「求人データ自動収集」ワークフローを選択
3. 「Run workflow」ボタンをクリック
4. 実行が完了するまで待機（約20-30分）

## 出力ファイル

収集されたデータは `data/` ディレクトリに保存されます：

```
data/
├── tokyo_jobs_20260114.csv    # CSV形式の求人データ
├── tokyo_jobs_20260114.json   # JSON形式の求人データ
└── summary_20260114.txt       # 収集サマリー
```

### CSVフォーマット

| カラム | 説明 |
|--------|------|
| title | 求人タイトル |
| company | 会社名 |
| location | 勤務地 |
| salary | 給与 |
| employment_type | 雇用形態 |
| industry | 業種 |
| source | 求人媒体 |
| url | 求人URL |
| is_new | 新着フラグ |
| scraped_at | 収集日時 |

## 実行スケジュール

デフォルトでは毎日午前9時（日本時間）に自動実行されます。

スケジュールを変更する場合は `.github/workflows/scrape.yml` の `cron` 設定を編集してください：

```yaml
schedule:
  - cron: '0 0 * * *'  # UTC 0:00 = 日本時間 9:00
```

## ローカルでの実行

```bash
# 依存パッケージをインストール
pip install -r requirements.txt

# スクリプトを実行
python scraper.py
```

## コスト

**完全無料**で運用できます：

- GitHub Actions: パブリックリポジトリは無料（月2,000分まで）
- 1回の実行: 約20-30分
- 月間使用量: 約600-900分（30日 × 20-30分）

## 注意事項

- 求人ボックスの利用規約を遵守してください
- 過度なアクセスは避けてください（スクリプトには適切な待機時間が設定されています）
- 収集したデータの商用利用については、各求人サイトの規約を確認してください

## ライセンス

MIT License


## Salesforce連携（オプション）

収集した求人データをSalesforceに自動でアップロードできます。

### 1. Salesforceの準備

Salesforceに求人データを格納するためのカスタムオブジェクトを作成します。

- **オブジェクト名**: `Job_Listing__c`
- **フィールド**:

| フィールド名 | データ型 | 文字数 |
|--------------|----------|--------|
| `Name` | テキスト | 80 |
| `Company__c` | テキスト | 255 |
| `Location__c` | テキスト | 255 |
| `Salary__c` | テキスト | 255 |
| `Employment_Type__c` | テキスト | 255 |
| `Industry__c` | テキスト | 255 |
| `Source__c` | テキスト | 255 |
| `URL__c` | URL | 255 |
| `Is_New__c` | チェックボックス | - |
| `Scraped_At__c` | 日付/時間 | - |

### 2. GitHub Secretsを設定

1. リポジトリの「Settings」→「Secrets and variables」→「Actions」を開く
2. 「New repository secret」をクリックして、以下の4つのSecretを作成します：

| Secret名 | 値 |
|-------------------|--------------------------------|
| `SF_USERNAME` | Salesforceのユーザー名 |
| `SF_PASSWORD` | Salesforceのパスワード |
| `SF_SECURITY_TOKEN` | Salesforceのセキュリティトークン |
| `SF_DOMAIN` | `login` または `test`（Sandboxの場合） |

### 3. ワークフローを有効化

1. `.github/workflows/scrape.yml` を削除または無効化
2. `.github/workflows/scrape-with-salesforce.yml` を有効化（ファイル名を `scrape.yml` に変更するなど）

これで、毎日収集したデータが自動でSalesforceにアップロードされます。
