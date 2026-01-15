#!/usr/bin/env python3
"""
Improved job scraping script for Tokyo job listings.

This script builds upon the original `scraper.py` provided in the
`job‑scraper` repository. The original version was designed to run as a
GitHub Actions workflow that scraped the "求人ボックス" website once per
day for a fixed set of industries and outputted CSV, JSON and summary
files. While functional, there were several limitations in the
original implementation:

* **Configurability:** The prefecture, list of search keywords/industries
  and the time window for new listings were hard‑coded. Running the
  scraper for other regions or custom industries required editing the
  source code.
* **Performance:** Industries were scraped sequentially. A full run
  against all 16 default industries could take upwards of half an hour
  even if each industry returned only a handful of pages. Leveraging
  concurrency can dramatically reduce the wall clock time.
* **Error handling & logging:** Exceptions were printed directly to
  standard output and the retry logic was basic. For troubleshooting
  longer runs it helps to use Python’s built‑in logging module and to
  surface HTTP and parsing issues more clearly.
* **Modularity:** All logic lived in a single module without clear
  separation of concerns. Breaking the code into smaller functions
  makes it easier to test and maintain.

This improved version addresses those points. It adds a simple CLI for
customising the scrape parameters, uses a thread pool to parallelise
scraping across industries, and surfaces more detailed logging. It also
adds optional random delays between requests to reduce the chance of
triggering site rate limits. Note that scraping external sites is
subject to the website’s terms of service; always respect robots.txt
and do not overload the target site with excessive requests.

"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


###############################################################################
# Configuration defaults
###############################################################################

# Base URL of 求人ボックス. Exposed as an environment variable for easy
# overrides during testing or future domain changes.
BASE_URL: str = os.environ.get("JOBBOX_BASE_URL", "https://xn--pckua2a7gp15o89zb.com")

# Default industries and keywords. Each tuple maps a search keyword to a
# user‑friendly industry label. You can override this via the `--industries`
# CLI flag.
DEFAULT_INDUSTRIES: Tuple[Tuple[str, str], ...] = (
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
)

# Maximum number of pages to scrape for a given search. The target site
# generally returns 25 results per page and may cap accessible pages.
MAX_PAGES_PER_SEARCH: int = int(os.environ.get("MAX_PAGES_PER_SEARCH", "60"))

# Default time window (in days) for new jobs. The original script only
# looked at jobs posted in the last 24 hours (`td=1`). Here we allow the
# user to set an arbitrary window: 1 day, 3 days, etc. The value maps
# directly to the `td` URL parameter.
DEFAULT_NEW_DAYS: int = int(os.environ.get("NEW_JOB_DAYS", "1"))

# Directory for output files
DEFAULT_OUTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "data")

# Number of concurrent threads to use when scraping multiple industries.
# Too many threads may cause the target site to block your IP. Adjust
# accordingly.
DEFAULT_WORKERS: int = int(os.environ.get("SCRAPER_WORKERS", "4"))

# Random delay range (in seconds) between successive page requests. Set
# both values to 0 to disable random delays entirely.
DELAY_RANGE: Tuple[float, float] = (
    float(os.environ.get("MIN_DELAY", "1.0")),
    float(os.environ.get("MAX_DELAY", "2.5")),
)


###############################################################################
# Data structures
###############################################################################

@dataclass
class Job:
    """Container for a single job listing."""

    title: str
    company: str
    location: str
    salary: str
    employment_type: str
    industry: str
    source: str
    url: str
    is_new: bool
    scraped_at: str


###############################################################################
# Helper functions
###############################################################################

def create_session() -> requests.Session:
    """Create an HTTP session with retry logic and appropriate headers.

    The session is configured to retry transient HTTP errors (status 429,
    500, 502, 503, 504) up to five times with exponential backoff. A
    desktop browser User‑Agent is used to reduce the likelihood of
    receiving a 403 Forbidden response.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 "
                "Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    )
    return session


def get_search_url(keyword: str, prefecture: str, days: int, page: int) -> str:
    """Construct the search URL for a given keyword, prefecture, time window and page.

    Args:
        keyword: Search keyword such as "介護職".
        prefecture: Prefecture name (e.g. "東京都").
        days: Number of days within which new jobs should be returned. Maps to
            the `td` query parameter on the site (1 = 24 hours).
        page: Page number (1‑based).

    Returns:
        Fully qualified URL string.
    """
    encoded_keyword = requests.utils.quote(keyword)
    encoded_pref = requests.utils.quote(prefecture)
    # Example format: https://site/介護職の仕事-東京都?td=1&pg=2
    return f"{BASE_URL}/{encoded_keyword}の仕事-{encoded_pref}?td={days}&pg={page}"


def parse_job_listing(section: BeautifulSoup, industry_name: str) -> Optional[Job]:
    """Extract a job listing from a `<section>` element.

    The parsing logic is heuristically derived from the structure of
    求人ボックス result pages. If essential fields (title and URL) are
    missing, the function returns `None`.

    Args:
        section: A BeautifulSoup tag representing a job listing container.
        industry_name: Human‑readable industry label associated with the search.

    Returns:
        A `Job` instance or `None` if parsing fails.
    """
    try:
        title_link = section.find("a")
        if not title_link:
            return None
        title = title_link.get_text(strip=True)
        # Filter out generic "求人" titles
        if title.startswith("求人"):
            return None
        href = title_link.get("href")
        if not href:
            return None
        url = href if href.startswith("http") else f"{BASE_URL}{href}"

        full_text = section.get_text("\n", strip=True)
        lines = [l for l in (line.strip() for line in full_text.split("\n")) if l]

        # Company name: first non‑salary/location/employment_type line
        company = ""
        for line in lines[1:]:
            if not re.match(r"^(東京都|月給|時給|年収|日給|正社員|アルバイト|派遣|契約)", line):
                company = line[:100]
                break

        # Location (prefecture + city etc.)
        loc_match = re.search(r"東京都[^\s　]*(?:区|市|町|村)?[^\s　]*", full_text)
        location = loc_match.group()[:80] if loc_match else "東京都"

        # Salary extraction
        salary = ""
        for pattern in (
            r"(月給[0-9,]+万?円?～?[0-9,]*万?円?)",
            r"(時給[0-9,]+円?～?[0-9,]*円?)",
            r"(年収[0-9,]+万?円?～?[0-9,]*万?円?)",
            r"(日給[0-9,]+万?円?～?[0-9,]*万?円?)",
        ):
            m = re.search(pattern, full_text)
            if m:
                salary = m.group(1)[:60]
                break

        # Employment type
        employment_type = ""
        for et in ("正社員", "アルバイト・パート", "派遣社員", "契約社員", "アルバイト", "パート"):
            if et in full_text:
                employment_type = et
                break

        is_new = "新着" in full_text or "時間前" in full_text
        scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return Job(
            title=title[:200],
            company=company,
            location=location,
            salary=salary,
            employment_type=employment_type,
            industry=industry_name,
            source="求人ボックス",
            url=url,
            is_new=is_new,
            scraped_at=scraped_at,
        )
    except Exception as exc:
        logging.debug("Error parsing job listing: %s", exc)
        return None


def scrape_page(session: requests.Session, url: str, industry_name: str) -> Tuple[List[Job], Optional[BeautifulSoup]]:
    """Fetch a results page and parse job listings.

    Returns a list of `Job` objects and the parsed `BeautifulSoup` object for
    the page. If an error occurs, returns an empty list and `None`.
    """
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs: List[Job] = []
        # We look for `<section>` tags that contain both an anchor and salary
        sections = soup.find_all("section")
        for section in sections:
            text = section.get_text()
            if section.find("a") and ("円" in text or "万円" in text):
                job = parse_job_listing(section, industry_name)
                if job:
                    jobs.append(job)
        return jobs, soup
    except requests.exceptions.RequestException as exc:
        logging.warning("Request error for %s: %s", url, exc)
    except Exception as exc:
        logging.warning("Unexpected error scraping %s: %s", url, exc)
    return [], None


def get_new_job_count(soup: BeautifulSoup) -> int:
    """Extract the number of new jobs listed on the page.

    The site typically displays a string like "本日の新着 123 件". This
    function returns the parsed integer if available, otherwise 0.
    """
    if not soup:
        return 0
    try:
        text = soup.get_text()
        match = re.search(r"本日の新着\s*([0-9,]+)\s*件", text)
        if match:
            return int(match.group(1).replace(",", ""))
    except Exception:
        pass
    return 0


def scrape_industry(
    session: requests.Session,
    keyword: str,
    industry_name: str,
    prefecture: str,
    days: int,
    max_pages: int = MAX_PAGES_PER_SEARCH,
    delay_range: Tuple[float, float] = DELAY_RANGE,
) -> List[Job]:
    """Scrape all pages for a single search keyword/industry.

    This function handles pagination, respects the `max_pages` limit and
    throttles requests using random delays. It logs high‑level progress
    information and returns a list of `Job` objects.
    """
    jobs_collected: List[Job] = []

    # Fetch first page
    url = get_search_url(keyword, prefecture, days, page=1)
    page_jobs, soup = scrape_page(session, url, industry_name)

    if soup is None:
        logging.error("Failed to fetch first page for %s (%s)", industry_name, keyword)
        return []

    new_jobs = get_new_job_count(soup)
    # Determine how many pages to scrape: estimate 25 jobs per page
    pages_to_scrape = min((new_jobs // 25) + 1 if new_jobs else max_pages, max_pages)

    jobs_collected.extend(page_jobs)
    logging.info(
        "%s: page 1/%s yielded %s jobs (estimated new jobs: %s)",
        industry_name,
        pages_to_scrape,
        len(page_jobs),
        new_jobs,
    )

    # Subsequent pages
    for page in range(2, pages_to_scrape + 1):
        # Respect delay
        if delay_range[1] > 0:
            time.sleep(random.uniform(*delay_range))
        url = get_search_url(keyword, prefecture, days, page=page)
        p_jobs, _ = scrape_page(session, url, industry_name)
        if not p_jobs:
            logging.info("%s: page %s contained no jobs, stopping early", industry_name, page)
            break
        jobs_collected.extend(p_jobs)
        if page % 10 == 0:
            logging.info(
                "%s: scraped %s pages (%s jobs total)",
                industry_name,
                page,
                len(jobs_collected),
            )

    logging.info("%s: finished with %s jobs", industry_name, len(jobs_collected))
    return jobs_collected


def deduplicate_jobs(jobs: Iterable[Job]) -> List[Job]:
    """Remove duplicate job listings based on their URL."""
    seen: set[str] = set()
    unique: List[Job] = []
    for job in jobs:
        if job.url not in seen:
            seen.add(job.url)
            unique.append(job)
    return unique


def save_csv(jobs: Iterable[Job], path: str) -> None:
    """Write jobs to a CSV file."""
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
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for job in jobs:
            writer.writerow(job.__dict__)


def save_json(jobs: Iterable[Job], summary: Dict[str, int], path: str, start: datetime, end: datetime) -> None:
    """Write jobs and metadata to a JSON file."""
    data = {
        "metadata": {
            "scraped_at": start.isoformat(),
            "completed_at": end.isoformat(),
            "duration_seconds": (end - start).total_seconds(),
            "total_jobs": len(list(jobs)),
            "industries": summary,
        },
        "jobs": [job.__dict__ for job in jobs],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_summary(summary: Dict[str, int], total_jobs: int, path: str, start: datetime, end: datetime) -> None:
    """Write a human‑readable summary text file."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"求人収集サマリー - {start:%Y%m%d}\n")
        f.write("=" * 50 + "\n")
        f.write(f"実行時刻: {start:%Y-%m-%d %H:%M:%S}\n")
        f.write(f"完了時刻: {end:%Y-%m-%d %H:%M:%S}\n")
        f.write(f"実行時間: {(end - start).total_seconds():.1f}秒\n")
        f.write(f"総取得件数: {total_jobs:,}件\n\n")
        f.write("業種別内訳:\n")
        f.write("-" * 30 + "\n")
        for name, count in sorted(summary.items(), key=lambda x: -x[1]):
            f.write(f"  {name}: {count:,}件\n")


def parse_industries_arg(value: str) -> Tuple[Tuple[str, str], ...]:
    """Parse the --industries CLI argument into a tuple of (keyword, name) pairs.

    The argument should be a comma‑separated list of `keyword:label` pairs,
    e.g. `介護職:介護,経理事務:経理`. If the label is omitted (no colon), the
    keyword itself is used as the label.
    """
    industries: List[Tuple[str, str]] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            keyword, label = item.split(":", 1)
            industries.append((keyword.strip(), label.strip()))
        else:
            industries.append((item, item))
    return tuple(industries)


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for the improved scraper.

    This function parses command line arguments, orchestrates the scraping
    process (possibly concurrently) and writes out the results. It returns
    an exit status code (0 for success).
    """
    parser = argparse.ArgumentParser(description="Scrape job listings from 求人ボックス.")
    parser.add_argument(
        "--prefecture",
        default="東京都",
        help="Prefecture to search within (default: 東京都)",
    )
    parser.add_argument(
        "--industries",
        type=parse_industries_arg,
        default=DEFAULT_INDUSTRIES,
        help=(
            "Comma‑separated list of keyword:label pairs to search. "
            "For example: '介護職:介護,経理事務:経理'. If label is omitted, "
            "the keyword is used as the label. Defaults to the built‑in set of 16 industries."
        ),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_NEW_DAYS,
        help="Number of days within which new jobs should be returned (maps to td parameter).",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory in which to place output files. Created if missing.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of concurrent threads to use. 1 disables concurrency.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args(argv)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Start timer
    start_time = datetime.now()
    logging.info("Starting job scraping at %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    logging.info(
        "Target prefecture: %s | Industries: %s | Days within: %s | Workers: %s",
        args.prefecture,
        ", ".join(f"{kw}:{name}" for kw, name in args.industries),
        args.days,
        args.workers,
    )

    session = create_session()
    industry_jobs: Dict[str, List[Job]] = {}

    if args.workers > 1:
        # Concurrent scraping
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_industry = {
                executor.submit(
                    scrape_industry,
                    session,
                    keyword,
                    name,
                    args.prefecture,
                    args.days,
                    MAX_PAGES_PER_SEARCH,
                    DELAY_RANGE,
                ): (keyword, name)
                for keyword, name in args.industries
            }
            for future in as_completed(future_to_industry):
                keyword, name = future_to_industry[future]
                try:
                    jobs = future.result()
                except Exception as exc:
                    logging.error("%s (%s) generated an exception: %s", name, keyword, exc)
                    jobs = []
                industry_jobs[name] = jobs
    else:
        # Sequential scraping
        for keyword, name in args.industries:
            jobs = scrape_industry(
                session, keyword, name, args.prefecture, args.days, MAX_PAGES_PER_SEARCH, DELAY_RANGE
            )
            industry_jobs[name] = jobs

    # Flatten and deduplicate
    all_jobs = [job for jobs in industry_jobs.values() for job in jobs]
    unique_jobs = deduplicate_jobs(all_jobs)

    # Summaries
    industry_summary = {name: len(jobs) for name, jobs in industry_jobs.items()}
    end_time = datetime.now()

    logging.info("Total jobs collected before deduplication: %s", len(all_jobs))
    logging.info("Unique jobs after deduplication: %s", len(unique_jobs))
    logging.info("Elapsed time: %.1f seconds", (end_time - start_time).total_seconds())

    # File paths
    date_str = start_time.strftime("%Y%m%d")
    csv_path = os.path.join(args.output_dir, f"jobs_{date_str}.csv")
    json_path = os.path.join(args.output_dir, f"jobs_{date_str}.json")
    summary_path = os.path.join(args.output_dir, f"summary_{date_str}.txt")

    # Write outputs
    save_csv(unique_jobs, csv_path)
    save_json(unique_jobs, industry_summary, json_path, start_time, end_time)
    save_summary(industry_summary, len(unique_jobs), summary_path, start_time, end_time)

    logging.info("CSV written to %s", csv_path)
    logging.info("JSON written to %s", json_path)
    logging.info("Summary written to %s", summary_path)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
        sys.exit(1)