#!/usr/bin/env python3
"""
Salesforce連携スクリプト
収集した求人データをSalesforceにアップロード

使用方法:
1. 環境変数を設定:
   - SF_USERNAME: Salesforceユーザー名
   - SF_PASSWORD: Salesforceパスワード
   - SF_SECURITY_TOKEN: Salesforceセキュリティトークン
   - SF_DOMAIN: ドメイン（'login' または 'test'）

2. スクリプトを実行:
   python salesforce_upload.py data/tokyo_jobs_20260114.csv
"""

import os
import sys
import csv
import json
from datetime import datetime

try:
    from simple_salesforce import Salesforce
    SALESFORCE_AVAILABLE = True
except ImportError:
    SALESFORCE_AVAILABLE = False
    print("⚠️ simple-salesforce がインストールされていません")
    print("インストール: pip install simple-salesforce")


def connect_salesforce():
    """Salesforceに接続"""
    username = os.environ.get('SF_USERNAME')
    password = os.environ.get('SF_PASSWORD')
    security_token = os.environ.get('SF_SECURITY_TOKEN')
    domain = os.environ.get('SF_DOMAIN', 'login')
    
    if not all([username, password, security_token]):
        raise ValueError(
            "Salesforce認証情報が設定されていません。\n"
            "以下の環境変数を設定してください:\n"
            "  - SF_USERNAME\n"
            "  - SF_PASSWORD\n"
            "  - SF_SECURITY_TOKEN"
        )
    
    sf = Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        domain=domain
    )
    
    print(f"✅ Salesforceに接続しました: {username}")
    return sf


def load_jobs_from_csv(csv_file):
    """CSVファイルから求人データを読み込み"""
    jobs = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            jobs.append(row)
    print(f"📄 {len(jobs)}件の求人データを読み込みました: {csv_file}")
    return jobs


def upload_to_salesforce(sf, jobs, object_name='Job_Listing__c'):
    """
    求人データをSalesforceにアップロード
    
    注意: Salesforceのカスタムオブジェクト名とフィールド名は
    お使いの環境に合わせて変更してください。
    """
    success_count = 0
    error_count = 0
    errors = []
    
    for job in jobs:
        try:
            # Salesforceのフィールドにマッピング
            # ※ フィールド名はお使いの環境に合わせて変更してください
            record = {
                'Name': job.get('title', '')[:80],  # Name は80文字制限
                'Company__c': job.get('company', '')[:255],
                'Location__c': job.get('location', '')[:255],
                'Salary__c': job.get('salary', '')[:255],
                'Employment_Type__c': job.get('employment_type', ''),
                'Industry__c': job.get('industry', ''),
                'Source__c': job.get('source', ''),
                'URL__c': job.get('url', ''),
                'Is_New__c': job.get('is_new', 'False') == 'True',
                'Scraped_At__c': job.get('scraped_at', ''),
            }
            
            # レコードを作成
            result = sf.__getattr__(object_name).create(record)
            
            if result.get('success'):
                success_count += 1
            else:
                error_count += 1
                errors.append({
                    'job': job.get('title'),
                    'error': result.get('errors')
                })
                
        except Exception as e:
            error_count += 1
            errors.append({
                'job': job.get('title', 'Unknown'),
                'error': str(e)
            })
    
    return success_count, error_count, errors


def main():
    if not SALESFORCE_AVAILABLE:
        print("❌ simple-salesforce をインストールしてください")
        sys.exit(1)
    
    if len(sys.argv) < 2:
        print("使用方法: python salesforce_upload.py <csv_file>")
        print("例: python salesforce_upload.py data/tokyo_jobs_20260114.csv")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    if not os.path.exists(csv_file):
        print(f"❌ ファイルが見つかりません: {csv_file}")
        sys.exit(1)
    
    print("=" * 60)
    print("Salesforce アップロード")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Salesforceに接続
    sf = connect_salesforce()
    
    # 求人データを読み込み
    jobs = load_jobs_from_csv(csv_file)
    
    if not jobs:
        print("⚠️ アップロードする求人データがありません")
        sys.exit(0)
    
    # アップロード
    print(f"\n📤 {len(jobs)}件の求人をアップロード中...")
    success, errors_count, errors = upload_to_salesforce(sf, jobs)
    
    # 結果を表示
    print("\n" + "=" * 60)
    print("アップロード完了")
    print("=" * 60)
    print(f"✅ 成功: {success}件")
    print(f"❌ エラー: {errors_count}件")
    
    if errors:
        print("\nエラー詳細:")
        for err in errors[:10]:  # 最初の10件のみ表示
            print(f"  - {err['job']}: {err['error']}")
        if len(errors) > 10:
            print(f"  ... 他 {len(errors) - 10}件")
    
    return success, errors_count


if __name__ == "__main__":
    main()
