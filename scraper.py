#!/usr/bin/env python3
"""
求人ボックス 自動スクレイピングスクリプト
GitHub Actions対応版 - 毎日自動実行用

対象: 東京都 / 24時間以内の新着求人
出力: CSV, JSON形式
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import csv
import time
import urllib.parse
from datetime import datetime
import re
import json
import os
import sys
import urllib3

# SSL警告を抑制
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定
BASE_URL = "https://xn--pckua2a7gp15o89zb.com"
MAX_PAGES_PER_SEARCH = 60  # 求人ボックスの制限
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', 'data')

# 対象業種（営業を除く）
INDUSTRIES = [
    ("歯科衛生士", "歯科"),
    ("介護職", "介護"),
    ("医療事務", "医療事務"),
    ("保育士", "保育"),
    ("美容師", "美容"),
    ("建設作業員", "建設"),
    ("一般事務", "事務"),
    ("経理事務", "経理"),
    ("看護師", "看護"),
    ("柔道整復師", "治療家"),
    ("ドライバー", "ドライバー"),
    ("警備員", "警備"),
    ("製造スタッフ", "製造"),
    ("清掃スタッフ", "清掃"),
    ("派遣スタッフ", "派遣"),
    ("飲食スタッフ", "飲食"),
]


def create_session():
    """リトライ設定付きセッションを作成"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    })
    return session


def get_search_url(keyword, prefecture="東京都", page=1):
    """検索URLを生成（24時間以内フィルタ付き）"""
    encoded_keyword = urllib.parse.quote(keyword)
    encoded_prefecture = urllib.parse.quote(prefecture)
    url = f"{BASE_URL}/{encoded_keyword}の仕事-{encoded_prefecture}?td=1&pg={page}"
    return url


def parse_job_listing(section, industry_name):
    """求人セクションから情報を抽出"""
    job = {}
    
    try:
        title_link = section.find('a')
        if title_link:
            job['title'] = title_link.get_text(strip=True)[:200]
            href = title_link.get('href', '')
            if href:
                if href.startswith('http'):
                    job['url'] = href
                else:
                    job['url'] = BASE_URL + href
            else:
                return None
        else:
            return None
        
        full_text = section.get_text('\n', strip=True)
        text_lines = [line.strip() for line in full_text.split('\n') if line.strip()]
        
        # 会社名
        if len(text_lines) > 1:
            for line in text_lines[1:]:
                if not re.match(r'^(東京都|月給|時給|年収|日給|正社員|アルバイト|派遣)', line):
                    job['company'] = line[:100]
                    break
            else:
                job['company'] = text_lines[1][:100] if len(text_lines) > 1 else ''
        else:
            job['company'] = ''
        
        # 場所
        location_match = re.search(r'東京都[^\s　]*(?:区|市|町|村)?[^\s　]*', full_text)
        job['location'] = location_match.group()[:80] if location_match else '東京都'
        
        # 給与
        salary_patterns = [
            r'(月給[0-9,]+万?円?～?[0-9,]*万?円?)',
            r'(時給[0-9,]+円?～?[0-9,]*円?)',
            r'(年収[0-9,]+万?円?～?[0-9,]*万?円?)',
            r'(日給[0-9,]+万?円?～?[0-9,]*万?円?)',
        ]
        job['salary'] = ''
        for pattern in salary_patterns:
            salary_match = re.search(pattern, full_text)
            if salary_match:
                job['salary'] = salary_match.group(1)[:60]
                break
        
        # 雇用形態
        employment_types = ['正社員', 'アルバイト・パート', '派遣社員', '契約社員', 'アルバイト', 'パート']
        job['employment_type'] = ''
        for et in employment_types:
            if et in full_text:
                job['employment_type'] = et
                break
        
        job['is_new'] = '新着' in full_text or '時間前' in full_text
        job['industry'] = industry_name
        job['source'] = '求人ボックス'
        job['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return job
        
    except Exception as e:
        return None


def scrape_page_with_retry(session, url, industry_name, max_retries=3):
    """リトライ機能付きでページを取得"""
    jobs = []
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            sections = soup.find_all('section')
            
            for section in sections:
                section_text = section.get_text()
                if section.find('a') and ('円' in section_text or '万円' in section_text):
                    job = parse_job_listing(section, industry_name)
                    if job and job.get('title') and job.get('url'):
                        if 'かんたん応募' not in job['title'] and '求人' not in job['title'][:10]:
                            jobs.append(job)
            
            return jobs, soup
            
        except requests.exceptions.SSLError as e:
            print(f"      SSLエラー (試行 {attempt+1}/{max_retries})")
            time.sleep(3 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f"      リクエストエラー (試行 {attempt+1}/{max_retries})")
            time.sleep(2 * (attempt + 1))
        except Exception as e:
            print(f"      エラー (試行 {attempt+1}/{max_retries}): {str(e)[:50]}")
            time.sleep(2)
    
    return [], None


def get_new_job_count(soup):
    """新着件数を取得"""
    try:
        page_text = soup.get_text()
        new_match = re.search(r'本日の新着\s*([0-9,]+)\s*件', page_text)
        if new_match:
            return int(new_match.group(1).replace(',', ''))
    except:
        pass
    return 0


def scrape_industry(session, keyword, industry_name, prefecture="東京都"):
    """1つの業種の全ページを取得"""
    all_jobs = []
    
    first_url = get_search_url(keyword, prefecture, 1)
    print(f"\n{'='*60}")
    print(f"業種: {industry_name} (検索: {keyword})")
    
    first_jobs, soup = scrape_page_with_retry(session, first_url, industry_name)
    
    if soup is None:
        print(f"  ❌ ページを取得できませんでした")
        return []
    
    new_jobs = get_new_job_count(soup)
    print(f"  本日の新着: {new_jobs:,}件")
    
    pages_to_scrape = min((new_jobs // 25) + 1, MAX_PAGES_PER_SEARCH) if new_jobs > 0 else MAX_PAGES_PER_SEARCH
    print(f"  取得予定: {pages_to_scrape}ページ")
    
    all_jobs.extend(first_jobs)
    print(f"  ページ 1/{pages_to_scrape}: {len(first_jobs)}件 (累計: {len(all_jobs)}件)")
    
    for page in range(2, pages_to_scrape + 1):
        url = get_search_url(keyword, prefecture, page)
        time.sleep(1.5)
        
        jobs, _ = scrape_page_with_retry(session, url, industry_name)
        
        if not jobs:
            print(f"  ページ {page}: 求人なし - 終了")
            break
        
        all_jobs.extend(jobs)
        
        if page % 10 == 0:
            print(f"  ページ {page}/{pages_to_scrape}: {len(jobs)}件 (累計: {len(all_jobs)}件)")
    
    print(f"  ✅ 完了: {industry_name} 合計 {len(all_jobs)}件")
    return all_jobs


def main():
    """メイン処理"""
    start_time = datetime.now()
    
    print("=" * 70)
    print("求人ボックス 自動スクレイピング")
    print(f"開始時刻: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("対象: 東京都 / 24時間以内の新着求人")
    print("=" * 70)
    
    # 出力ディレクトリを作成
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    session = create_session()
    all_jobs = []
    industry_summary = {}
    
    for keyword, industry_name in INDUSTRIES:
        jobs = scrape_industry(session, keyword, industry_name, "東京都")
        all_jobs.extend(jobs)
        industry_summary[industry_name] = len(jobs)
        time.sleep(3)
    
    # 重複除去
    seen_urls = set()
    unique_jobs = []
    for job in all_jobs:
        url = job.get('url', '')
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_jobs.append(job)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 70)
    print("収集完了")
    print("=" * 70)
    print(f"総取得件数: {len(all_jobs):,}件")
    print(f"重複除去後: {len(unique_jobs):,}件")
    print(f"実行時間: {duration:.1f}秒")
    
    # ファイル名に日付を含める
    date_str = start_time.strftime('%Y%m%d')
    timestamp = start_time.strftime('%Y%m%d_%H%M%S')
    
    # CSVに保存
    csv_file = os.path.join(OUTPUT_DIR, f"tokyo_jobs_{date_str}.csv")
    fieldnames = ['title', 'company', 'location', 'salary', 'employment_type', 
                  'industry', 'source', 'url', 'is_new', 'scraped_at']
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unique_jobs)
    
    print(f"\nCSV保存: {csv_file}")
    
    # JSONに保存
    json_file = os.path.join(OUTPUT_DIR, f"tokyo_jobs_{date_str}.json")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'scraped_at': start_time.isoformat(),
                'completed_at': end_time.isoformat(),
                'duration_seconds': duration,
                'total_jobs': len(unique_jobs),
                'industries': industry_summary
            },
            'jobs': unique_jobs
        }, f, ensure_ascii=False, indent=2)
    
    print(f"JSON保存: {json_file}")
    
    # サマリーをファイルに保存
    summary_file = os.path.join(OUTPUT_DIR, f"summary_{date_str}.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"求人収集サマリー - {date_str}\n")
        f.write("=" * 50 + "\n")
        f.write(f"実行時刻: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"完了時刻: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"実行時間: {duration:.1f}秒\n")
        f.write(f"総取得件数: {len(unique_jobs):,}件\n\n")
        f.write("業種別内訳:\n")
        f.write("-" * 30 + "\n")
        for industry_name, count in sorted(industry_summary.items(), key=lambda x: -x[1]):
            f.write(f"  {industry_name}: {count:,}件\n")
    
    print(f"サマリー保存: {summary_file}")
    
    # 業種別サマリー表示
    print("\n業種別サマリー:")
    print("-" * 40)
    for industry_name, count in sorted(industry_summary.items(), key=lambda x: -x[1]):
        print(f"  {industry_name}: {count:,}件")
    print("-" * 40)
    print(f"  合計: {len(unique_jobs):,}件")
    
    # GitHub Actions用の出力
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write(f"total_jobs={len(unique_jobs)}\n")
            f.write(f"csv_file={csv_file}\n")
            f.write(f"json_file={json_file}\n")
    
    return unique_jobs


if __name__ == "__main__":
    try:
        jobs = main()
        print(f"\n✅ 正常終了: {len(jobs)}件の求人を収集しました")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ エラー発生: {str(e)}")
        sys.exit(1)
