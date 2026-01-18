#!/usr/bin/env python3
"""Kyujinbox job scraper - improved version with thread safety.

Improvements:
  - Thread-safe sessions (each worker gets its own session)
  - Better logging with Python logging module
  - Proper error handling with logging
  - Type consistency (is_new as string)
  - Prefer stable job URLs (/jb/) over redirect URLs (/rd/)
  - Better company name extraction
  - Non-zero exit code on complete failure

Outputs:
  - CSV / JSON / Summary under --output-dir (default: data)
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = os.environ.get("JOBBOX_BASE_URL", "https://xn--pckua2a7gp15o89zb.com")
OUTPUT_DIR_DEFAULT = os.environ.get("OUTPUT_DIR", "data")

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

DEFAULT_PREFECTURES: List[str] = [
    # Kanto
    "東京都", "千葉県", "埼玉県", "神奈川県",
    # Tokai
    "愛知県", "岐阜県", "静岡県",
    # Kansai
    "大阪府", "京都府", "滋賀県",
    # Kyushu
    "福岡県",
]

USER_AGENTS: Tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

# Heuristics: skip lines that are NOT company name
COMPANY_SKIP_PREFIXES = (
    "新着", "PR", "急募", "おすすめ", "注目",
    "本日の新着", "掲載", "更新", "終了間近",
    "【仕事内容】", "【対象となる方】", "【勤務地】", "【給与】", "【勤務時間】",
    "【雇用形態】", "【待遇】", "【応募】", "【応募先】",
    "【こんなキーワード", "【この仕事の特徴】",
)
EMPLOYMENT_KEYWORDS = ("正社員", "アルバイト", "パート", "派遣", "契約", "業務委託")
SALARY_PREFIXES = ("月給", "時給", "年収", "日給")


# Thread-local storage for sessions
_thread_local = threading.local()


def get_session() -> requests.Session:
    """Get a thread-local session (thread-safe)."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = _create_session()
    return _thread_local.session


def _create_session() -> requests.Session:
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


def _log_http_failure(site: str, url: str, resp: requests.Response) -> None:
    snippet = (resp.text or "").replace("\n", " ")[:200]
    logger.warning("%s HTTP %d for %s | body: %r", site, resp.status_code, url, snippet)


def _best_job_href(section: BeautifulSoup) -> Optional[str]:
    """
    Prefer stable job URLs over redirect (/rd/).
    Priority:
      1) /jb/...
      2) any absolute/relative link that is NOT /rd/
      3) fallback: first link
    """
    links = [a.get("href") for a in section.find_all("a", href=True)]
    if not links:
        return None
    # Prefer /jb/ links
    for href in links:
        if href and "/jb/" in href:
            return href
    # Then any non-/rd/ link
    for href in links:
        if href and "/rd/" not in href:
            return href
    # Fallback
    return links[0]


def _normalize_url(href: str) -> str:
    """Make absolute URL and strip query for /jb/ (keep stable path)."""
    url = href if href.startswith("http") else urljoin(BASE_URL, href)
    try:
        parsed = urlparse(url)
        # For /jb/ links, query is often not needed; drop it to stabilize
        if "/jb/" in parsed.path:
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return url
    except Exception:
        return url


def _looks_like_company(line: str, prefecture: str) -> bool:
    if not line:
        return False
    # Skip headings / labels
    for p in COMPANY_SKIP_PREFIXES:
        if line.startswith(p):
            return False
    if line.startswith("【") or line.endswith("】"):
        return False
    # Skip location/salary/employment-ish lines
    if line.startswith(prefecture):
        return False
    if any(line.startswith(s) for s in SALARY_PREFIXES):
        return False
    if any(k in line for k in EMPLOYMENT_KEYWORDS):
        return False
    # Too long / looks like a paragraph
    if len(line) > 120:
        return False
    return True


def parse_job_listing(
    section: BeautifulSoup, industry_name: str, prefecture: str
) -> Optional[Dict[str, str]]:
    """Extract job data from a section."""
    try:
        title_link = section.find("a")
        if not title_link:
            return None

        title = title_link.get_text(strip=True)[:200]
        if not title:
            return None

        href = _best_job_href(section)
        if not href:
            return None
        job_url = _normalize_url(href)

        full_text = section.get_text("\n", strip=True)
        text_lines = [line.strip() for line in full_text.split("\n") if line.strip()]

        # Company: choose a plausible line after title
        company = ""
        for line in text_lines[1:]:
            if _looks_like_company(line, prefecture):
                company = line[:100]
                break
        company = company[:100]

        # Location
        location_match = re.search(
            rf"{re.escape(prefecture)}[^\s　]*(?:区|市|町|村)?[^\s　]*", full_text
        )
        location = location_match.group()[:80] if location_match else prefecture

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

        # Employment type
        employment_types = [
            "正社員", "アルバイト・パート", "派遣社員", "契約社員", "アルバイト", "パート"
        ]
        employment_type = ""
        for et in employment_types:
            if et in full_text:
                employment_type = et
                break

        job: Dict[str, str] = {
            "title": title,
            "company": company,
            "location": location,
            "salary": salary,
            "employment_type": employment_type,
            "industry": industry_name,
            "source": "求人ボックス",
            "url": job_url,
            "is_new": str("新着" in full_text or "時間前" in full_text),  # String for type consistency
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "prefecture": prefecture,
        }
        return job
    except Exception as e:
        logger.debug("Error parsing job listing: %s", e)
        return None


def scrape_page_with_retry(
    url: str,
    industry_name: str,
    prefecture: str,
    max_retries: int = 3,
) -> Tuple[List[Dict[str, str]], Optional[BeautifulSoup]]:
    """Fetch a page with retry and parse job sections."""
    session = get_session()  # Thread-safe session

    for attempt in range(max_retries):
        try:
            _rotate_headers(session)
            response = session.get(url, timeout=30, verify=False)

            if response.status_code in (403, 404):
                _log_http_failure("求人ボックス", url, response)
                # 403/404 can be transient / pseudo-block: retry with backoff
                time.sleep(2 * (attempt + 1))
                continue

            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            jobs: List[Dict[str, str]] = []
            for section in soup.find_all("section"):
                section_text = section.get_text()
                if section.find("a") and ("円" in section_text or "万円" in section_text):
                    job = parse_job_listing(section, industry_name, prefecture)
                    if job and job.get("title") and job.get("url"):
                        jobs.append(job)

            return jobs, soup

        except requests.exceptions.SSLError:
            logger.warning("SSLエラー (試行 %d/%d)", attempt + 1, max_retries)
            time.sleep(3 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            logger.warning("リクエストエラー (試行 %d/%d): %s", attempt + 1, max_retries, str(e)[:80])
            time.sleep(2 * (attempt + 1))
        except Exception as e:
            logger.warning("エラー (試行 %d/%d): %s", attempt + 1, max_retries, str(e)[:80])
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
    keyword: str,
    industry_name: str,
    prefecture: str,
    days: int,
    max_pages_per_search: int,
) -> List[Dict[str, str]]:
    """Scrape a single (prefecture, industry) pair."""
    all_jobs: List[Dict[str, str]] = []

    first_url = get_search_url(keyword, prefecture, days, 1)

    logger.info("=" * 60)
    logger.info("地域: %s / 業種: %s (検索: %s)", prefecture, industry_name, keyword)

    first_jobs, soup = scrape_page_with_retry(first_url, industry_name, prefecture)

    if soup is None:
        logger.error("  ページを取得できませんでした")
        return []

    new_jobs = get_new_job_count(soup)
    if days == 1 and new_jobs > 0:
        pages_to_scrape = min((new_jobs // 25) + 1, max_pages_per_search)
        logger.info("  本日の新着: %d件", new_jobs)
    else:
        pages_to_scrape = max_pages_per_search
        logger.info("  新着件数: (取得できず) -> %dページ上限で取得", pages_to_scrape)

    all_jobs.extend(first_jobs)
    logger.info("  ページ 1/%d: %d件 (累計: %d件)", pages_to_scrape, len(first_jobs), len(all_jobs))

    for page in range(2, pages_to_scrape + 1):
        url = get_search_url(keyword, prefecture, days, page)
        page_jobs, _ = scrape_page_with_retry(url, industry_name, prefecture)
        if not page_jobs:
            break
        all_jobs.extend(page_jobs)
        logger.info("  ページ %d/%d: %d件 (累計: %d件)", page, pages_to_scrape, len(page_jobs), len(all_jobs))
        time.sleep(random.uniform(1.5, 3.5))

    return all_jobs


def save_outputs(jobs: List[Dict[str, str]], output_dir: str) -> Tuple[str, str, str]:
    os.makedirs(output_dir, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = os.path.join(output_dir, f"jobs_{date_str}.csv")
    json_path = os.path.join(output_dir, f"jobs_{date_str}.json")
    summary_path = os.path.join(output_dir, f"summary_{date_str}.txt")

    fieldnames = [
        "title", "company", "location", "salary", "employment_type",
        "industry", "source", "url", "is_new", "scraped_at", "prefecture",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for job in jobs:
            writer.writerow({k: job.get(k, "") for k in fieldnames})

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

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
    normalized: List[str] = []
    for p in parts:
        if p.endswith(("都", "道", "府", "県")):
            normalized.append(p)
        else:
            if p == "東京":
                normalized.append("東京都")
            elif p == "大阪":
                normalized.append("大阪府")
            elif p == "京都":
                normalized.append("京都府")
            else:
                normalized.append(p + "県")
    return normalized


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="求人ボックスの求人を複数都道府県で収集")
    parser.add_argument(
        "--prefectures", default=None,
        help="都道府県をカンマ区切りで指定（例: 東京都,神奈川県,大阪府）"
    )
    parser.add_argument("--days", type=int, default=1, help="新着日数 td=<N>（1=24時間以内）")
    parser.add_argument("--output-dir", default=OUTPUT_DIR_DEFAULT, help="出力ディレクトリ")
    parser.add_argument(
        "--workers", type=int, default=3,
        help="並列数（多すぎるとブロックされる可能性あり）"
    )
    parser.add_argument(
        "--max-pages-per-search", type=int, default=None,
        help="業種ごとの最大ページ数（未指定なら、都道府県が複数のとき10、単数のとき60）",
    )
    args = parser.parse_args(argv)

    prefectures = parse_prefectures_arg(args.prefectures)
    max_pages_per_search = args.max_pages_per_search
    if max_pages_per_search is None:
        max_pages_per_search = 60 if len(prefectures) <= 1 else 10

    logger.info("=" * 80)
    logger.info("求人ボックス自動収集（複数都道府県）")
    logger.info("都道府県: %s", prefectures)
    logger.info("days(td): %d", args.days)
    logger.info("max_pages_per_search: %d", max_pages_per_search)
    logger.info("workers: %d", args.workers)
    logger.info("=" * 80)

    tasks: List[Tuple[str, str, str]] = []
    for pref in prefectures:
        for keyword, label in INDUSTRIES:
            tasks.append((pref, keyword, label))

    all_jobs: List[Dict[str, str]] = []
    seen: set = set()
    success_count = 0
    fail_count = 0

    def run_one(pref: str, keyword: str, label: str) -> List[Dict[str, str]]:
        return scrape_industry(keyword, label, pref, args.days, max_pages_per_search)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = {
            ex.submit(run_one, pref, keyword, label): (pref, label)
            for pref, keyword, label in tasks
        }
        for fut in as_completed(futures):
            pref, label = futures[fut]
            try:
                jobs = fut.result()
                success_count += 1
            except Exception as e:
                logger.error("Failed: %s/%s: %s", pref, label, e)
                fail_count += 1
                continue

            added = 0
            for j in jobs:
                u = j.get("url", "")
                if not u:
                    continue
                # Dedup by URL (now more stable since we prefer /jb/)
                if u not in seen:
                    seen.add(u)
                    all_jobs.append(j)
                    added += 1
            logger.info("Done: %s/%s -> +%d unique jobs", pref, label, added)

    logger.info("=" * 80)
    logger.info("Total unique jobs: %d", len(all_jobs))
    logger.info("Tasks succeeded: %d, failed: %d", success_count, fail_count)

    csv_path, json_path, summary_path = save_outputs(all_jobs, args.output_dir)
    logger.info("CSV: %s", csv_path)
    logger.info("JSON: %s", json_path)
    logger.info("Summary: %s", summary_path)
    logger.info("=" * 80)

    # Return non-zero if all tasks failed
    if success_count == 0 and fail_count > 0:
        logger.error("All tasks failed!")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
