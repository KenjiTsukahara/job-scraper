#!/usr/bin/env python3
"""Kyujinbox job scraper (multi-prefecture edition).

This script is a refactor of the repository's original ``scraper.py``.
The original implementation successfully scraped 求人ボックス by using
the **path-based** search URL:

  /<keyword>の仕事-<prefecture>?td=1&pg=1

Some later experiments used a /search endpoint that can return 404 on
求人ボックス; this script intentionally keeps the proven URL format.

Main additions:
  - Multiple prefectures in one run (Kanto/Tokai/Kansai/Kyushu set)
  - ``--days`` to control td=<N> (1 = last 24 hours)
  - Concurrency across (prefecture, industry) jobs (bounded)
  - Better debug logging for 403/404 (prints requested URL + body snippet)

Outputs:
  - CSV / JSON / Summary written under --output-dir (default: data)

Notes:
  - Respect the target site's terms and rate limits.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# SSL warnings suppression (same as original scraper.py)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


BASE_URL = os.environ.get("JOBBOX_BASE_URL", "https://xn--pckua2a7gp15o89zb.com")
OUTPUT_DIR_DEFAULT = os.environ.get("OUTPUT_DIR", "data")

# Default industries (same as original scraper.py)
INDUSTRIES: List[Tuple[str, str]] = [
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


# Prefectures requested by the user
DEFAULT_PREFECTURES: List[str] = [
    # Kanto
    "東京都",
    "千葉県",
    "埼玉県",
    "神奈川県",
    # Tokai
    "愛知県",
    "岐阜県",
    "静岡県",
    # Kansai
    "大阪府",
    "京都府",
    "滋賀県",
    # Kyushu
    "福岡県",
]


USER_AGENTS: Tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)


def create_session() -> requests.Session:
    """Create a requests session with retries and browser-like headers."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    )
    return session


def _rotate_headers(session: requests.Session) -> None:
    session.headers["User-Agent"] = random.choice(USER_AGENTS)
    session.headers["Referer"] = BASE_URL


def get_search_url(keyword: str, prefecture: str, days: int, page: int) -> str:
    """Generate a path-based search URL (proven to work with 求人ボックス)."""
    encoded_keyword = urllib.parse.quote(keyword)
    encoded_prefecture = urllib.parse.quote(prefecture)
    return f"{BASE_URL}/{encoded_keyword}の仕事-{encoded_prefecture}?td={days}&pg={page}"


def parse_job_listing(section: BeautifulSoup, industry_name: str, prefecture: str) -> Optional[Dict[str, str]]:
    """Extract job data from a section."""
    job: Dict[str, str] = {}
    try:
        title_link = section.find("a")
        if not title_link:
            return None

        job["title"] = title_link.get_text(strip=True)[:200]
        href = title_link.get("href", "")
        if not href:
            return None
        job["url"] = href if href.startswith("http") else (BASE_URL + href)

        full_text = section.get_text("\n", strip=True)
        text_lines = [line.strip() for line in full_text.split("\n") if line.strip()]

        # Company name: find the first line that doesn't look like location/salary/employment type
        company = ""
        if len(text_lines) > 1:
            for line in text_lines[1:]:
                if not re.match(rf"^({re.escape(prefecture)}|月給|時給|年収|日給|正社員|アルバイト|派遣)", line):
                    company = line[:100]
                    break
            if not company:
                company = text_lines[1][:100]
        job["company"] = company

        # Location
        location_match = re.search(rf"{re.escape(prefecture)}[^\s　]*(?:区|市|町|村)?[^\s　]*", full_text)
        job["location"] = location_match.group()[:80] if location_match else prefecture

        # Salary
        salary_patterns = [
            r"(月給[0-9,]+万?円?～?[0-9,]*万?円?)",
            r"(時給[0-9,]+円?～?[0-9,]*円?)",
            r"(年収[0-9,]+万?円?～?[0-9,]*万?円?)",
            r"(日給[0-9,]+万?円?～?[0-9,]*万?円?)",
        ]
        salary = ""
        for pattern in salary_patterns:
            m = re.search(pattern, full_text)
            if m:
                salary = m.group(1)[:60]
                break
        job["salary"] = salary

        # Employment type
        employment_types = ["正社員", "アルバイト・パート", "派遣社員", "契約社員", "アルバイト", "パート"]
        employment_type = ""
        for et in employment_types:
            if et in full_text:
                employment_type = et
                break
        job["employment_type"] = employment_type

        job["is_new"] = "新着" in full_text or "時間前" in full_text
        job["industry"] = industry_name
        job["source"] = "求人ボックス"
        job["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job["prefecture"] = prefecture

        return job
    except Exception:
        return None


def _log_http_failure(site: str, url: str, resp: requests.Response) -> None:
    snippet = (resp.text or "").replace("\n", " ")[:200]
    print(f"      ⚠ {site} HTTP {resp.status_code} for {url} | body: {snippet!r}")


def scrape_page_with_retry(
    session: requests.Session,
    url: str,
    industry_name: str,
    prefecture: str,
    max_retries: int = 3,
) -> Tuple[List[Dict[str, str]], Optional[BeautifulSoup]]:
    """Fetch a page with retry and parse job sections."""
    jobs: List[Dict[str, str]] = []
    for attempt in range(max_retries):
        try:
            _rotate_headers(session)
            response = session.get(url, timeout=30, verify=False)

            # Helpful debug for 403/404
            if response.status_code in (403, 404):
                _log_http_failure("求人ボックス", url, response)
                return [], None

            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            for section in soup.find_all("section"):
                section_text = section.get_text()
                if section.find("a") and ("円" in section_text or "万円" in section_text):
                    job = parse_job_listing(section, industry_name, prefecture)
                    if job and job.get("title") and job.get("url"):
                        if "かんたん応募" not in job["title"] and "求人" not in job["title"][:10]:
                            jobs.append(job)
            return jobs, soup
        except requests.exceptions.SSLError:
            print(f"      SSLエラー (試行 {attempt+1}/{max_retries})")
            time.sleep(3 * (attempt + 1))
        except requests.exceptions.RequestException:
            print(f"      リクエストエラー (試行 {attempt+1}/{max_retries})")
            time.sleep(2 * (attempt + 1))
        except Exception as e:
            print(f"      エラー (試行 {attempt+1}/{max_retries}): {str(e)[:50]}")
            time.sleep(2)
    return [], None


def get_new_job_count(soup: BeautifulSoup) -> int:
    """Extract '本日の新着' count if available."""
    try:
        page_text = soup.get_text()
        m = re.search(r"本日の新着\s*([0-9,]+)\s*件", page_text)
        if m:
            return int(m.group(1).replace(",", ""))
    except Exception:
        pass
    return 0


def scrape_industry(
    session: requests.Session,
    keyword: str,
    industry_name: str,
    prefecture: str,
    days: int,
    max_pages_per_search: int,
) -> List[Dict[str, str]]:
    """Scrape a single (prefecture, industry) pair."""
    all_jobs: List[Dict[str, str]] = []
    first_url = get_search_url(keyword, prefecture, days, 1)
    print(f"\n{'='*60}")
    print(f"地域: {prefecture} / 業種: {industry_name} (検索: {keyword})")

    first_jobs, soup = scrape_page_with_retry(session, first_url, industry_name, prefecture)
    if soup is None:
        print("  ❌ ページを取得できませんでした")
        return []

    new_jobs = get_new_job_count(soup)
    if days == 1 and new_jobs > 0:
        pages_to_scrape = min((new_jobs // 25) + 1, max_pages_per_search)
        print(f"  本日の新着: {new_jobs:,}件")
    else:
        # For td>1 the site may not show '本日の新着'; fall back to conservative cap
        pages_to_scrape = max_pages_per_search
        print(f"  新着件数: (取得できず) -> {pages_to_scrape}ページ上限で取得")

    all_jobs.extend(first_jobs)
    print(f"  ページ 1/{pages_to_scrape}: {len(first_jobs)}件 (累計: {len(all_jobs)}件)")

    # Next pages
    for page in range(2, pages_to_scrape + 1):
        url = get_search_url(keyword, prefecture, days, page)
        page_jobs, _ = scrape_page_with_retry(session, url, industry_name, prefecture)
        if not page_jobs:
            break
        all_jobs.extend(page_jobs)
        print(f"  ページ {page}/{pages_to_scrape}: {len(page_jobs)}件 (累計: {len(all_jobs)}件)")
        time.sleep(random.uniform(1.0, 2.5))

    return all_jobs


def save_outputs(jobs: List[Dict[str, str]], output_dir: str) -> Tuple[str, str, str]:
    os.makedirs(output_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = os.path.join(output_dir, f"jobs_{date_str}.csv")
    json_path = os.path.join(output_dir, f"jobs_{date_str}.json")
    summary_path = os.path.join(output_dir, f"summary_{date_str}.txt")

    # CSV
    fieldnames = [
        "title",
        "company",
        "location",
        "salary",
        "employment_type",
        "industry",
        "source",
        "url",
        "is_new",
        "scraped_at",
        "prefecture",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for job in jobs:
            writer.writerow({k: job.get(k, "") for k in fieldnames})

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

    # Summary
    by_pref: Dict[str, int] = {}
    by_ind: Dict[str, int] = {}
    for job in jobs:
        by_pref[job.get("prefecture", "")] = by_pref.get(job.get("prefecture", ""), 0) + 1
        by_ind[job.get("industry", "")] = by_ind.get(job.get("industry", ""), 0) + 1

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Total jobs: {len(jobs)}\n")
        f.write("\nBy prefecture:\n")
        for k in sorted(by_pref.keys()):
            f.write(f"- {k}: {by_pref[k]}\n")
        f.write("\nBy industry:\n")
        for k in sorted(by_ind.keys()):
            f.write(f"- {k}: {by_ind[k]}\n")

    return csv_path, json_path, summary_path


def parse_prefectures_arg(s: Optional[str]) -> List[str]:
    if not s:
        return list(DEFAULT_PREFECTURES)
    parts = [p.strip() for p in s.split(",") if p.strip()]
    # Normalize common inputs (e.g., "福岡" -> "福岡県")
    normalized: List[str] = []
    for p in parts:
        if p.endswith("都") or p.endswith("道") or p.endswith("府") or p.endswith("県"):
            normalized.append(p)
        else:
            # best-effort suffix
            if p in ("東京",):
                normalized.append("東京都")
            elif p in ("大阪",):
                normalized.append("大阪府")
            elif p in ("京都",):
                normalized.append("京都府")
            else:
                normalized.append(p + "県")
    return normalized


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="求人ボックスの求人を複数都道府県で収集")
    parser.add_argument("--prefectures", default=None, help="都道府県をカンマ区切りで指定（例: 東京都,神奈川県,大阪府）")
    parser.add_argument("--days", type=int, default=1, help="新着日数 td=<N>（1=24時間以内）")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="出力ディレクトリ")
    parser.add_argument("--workers", type=int, default=4, help="並列数（多すぎるとブロックされる可能性あり）")
    parser.add_argument(
        "--max-pages-per-search",
        type=int,
        default=None,
        help="業種ごとの最大ページ数（未指定なら、都道府県が複数のとき10、単数のとき60）",
    )
    args = parser.parse_args(argv)

    prefectures = parse_prefectures_arg(args.prefectures)
    # Adaptive default to avoid Actions timeout when many prefectures are used
    max_pages_per_search = args.max_pages_per_search
    if max_pages_per_search is None:
        max_pages_per_search = 60 if len(prefectures) <= 1 else 10

    print("=" * 80)
    print("求人ボックス自動収集（複数都道府県）")
    print(f"都道府県: {prefectures}")
    print(f"days(td): {args.days}")
    print(f"max_pages_per_search: {max_pages_per_search}")
    print(f"workers: {args.workers}")
    print("=" * 80)

    session = create_session()

    tasks: List[Tuple[str, str, str]] = []
    for pref in prefectures:
        for keyword, label in INDUSTRIES:
            tasks.append((pref, keyword, label))

    all_jobs: List[Dict[str, str]] = []
    seen_urls: set = set()

    def run_one(pref: str, keyword: str, label: str) -> List[Dict[str, str]]:
        return scrape_industry(session, keyword, label, pref, args.days, max_pages_per_search)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = {ex.submit(run_one, pref, keyword, label): (pref, label) for pref, keyword, label in tasks}
        for fut in as_completed(futures):
            pref, label = futures[fut]
            try:
                jobs = fut.result()
            except Exception as e:
                print(f"❌ Failed: {pref}/{label}: {e}")
                continue
            for j in jobs:
                url = j.get("url")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_jobs.append(j)
            print(f"✅ Done: {pref}/{label} -> +{len(jobs)} jobs")

    print("\n" + "=" * 80)
    print(f"Total unique jobs: {len(all_jobs)}")
    csv_path, json_path, summary_path = save_outputs(all_jobs, args.output_dir)
    print(f"CSV: {csv_path}")
    print(f"JSON: {json_path}")
    print(f"Summary: {summary_path}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
