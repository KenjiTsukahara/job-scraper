#!/usr/bin/env python3
"""
Multi‑site job scraping script.

This script extends the earlier ``improved_scraper.py`` by adding the
ability to scrape multiple job boards and to search across multiple
prefectures in one run.  The goal is to demonstrate how the scraper
could be generalised to work with additional sites and regions as
described in the large configuration file provided by the user.  Only
two sites are implemented here as proof of concept: the original
``kyujinbox.com`` (求人ボックス) and ``jp.indeed.com`` (Indeed Japan).

Due to the execution environment restrictions (outbound HTTP requests
often return HTTP 403 or DNS resolution failures), the actual scraping
logic is designed to fail gracefully.  When a request to a target
site cannot be performed, the scraper will log a warning and return
zero jobs for that combination.  This behaviour allows the code to
generate the expected CSV/JSON/Summary files even when live data
cannot be retrieved.

You can specify one or more prefectures and domains via command line
options.  If no domains are provided, the default set contains
``kyujinbox.com`` and ``jp.indeed.com``.  Similarly, if no
prefectures are provided the script defaults to a handful of urban
prefectures: Tokyo, Kanagawa, Aichi, Osaka, Fukuoka and Miyagi.

Example usage:

    python multisite_scraper.py \
        --prefectures "東京都,神奈川県" \
        --days 1 \
        --domains "kyujinbox.com,jp.indeed.com" \
        --workers 4 \
        --output-dir data

This will scrape jobs posted within the last 24 hours across both
KyujinBox and Indeed for Tokyo and Kanagawa using the default set of
industry keywords.  Results are written to CSV/JSON/Summary files in
the specified output directory.

Note: Always respect the robots.txt and terms of service for any
target site.  Excessive automated access can lead to IP bans or
legal issues.  The sample code shown here is for educational
purposes and should be adapted responsibly for production use.
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

# Default set of prefectures to scrape if none are provided.  These
# correspond to the six target prefectures mentioned in the provided
# configuration file.  Users can override this via ``--prefectures``.
DEFAULT_PREFECTURES: Tuple[str, ...] = (
    "東京都",
    "神奈川県",
    "愛知県",
    "大阪府",
    "福岡県",
    "宮城県",
)

# Default industries and keywords.  Each tuple maps a search keyword
# to a human‑readable label.  This list mirrors the 16 industries
# enumerated in the original scraper.  Additional industries can be
# passed via ``--industries`` in the same format ``keyword:label``.
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

# Maximum number of pages to scrape per (domain, keyword, prefecture)
# combination.  Many job boards display 10–25 results per page.  Set
# this lower if you only need the first few pages.
MAX_PAGES_PER_SEARCH: int = int(os.environ.get("MAX_PAGES_PER_SEARCH", "10"))

# Default time window (in days) for new jobs.  ``1`` corresponds to
# jobs posted within the last 24 hours.  This maps to the ``td``
# parameter for KyujinBox and the ``fromage`` parameter for Indeed.
DEFAULT_NEW_DAYS: int = int(os.environ.get("NEW_JOB_DAYS", "1"))

# Directory for output files
DEFAULT_OUTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "data")

# Number of concurrent threads to use when scraping multiple sites.
# Too many threads may cause the target sites to block your IP.  Adjust
# accordingly.
DEFAULT_WORKERS: int = int(os.environ.get("SCRAPER_WORKERS", "4"))

# Random delay range (in seconds) between successive page requests.
DELAY_RANGE: Tuple[float, float] = (
    float(os.environ.get("MIN_DELAY", "1.0")),
    float(os.environ.get("MAX_DELAY", "2.5")),
)

# A list of common desktop and mobile User‑Agent strings.  A random
# agent from this list will be assigned prior to each request to
# reduce simple bot detection.
USER_AGENTS: Tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
)


###############################################################################
# Data structures
###############################################################################


@dataclass
class Job:
    """Container for a single job listing across any site."""

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


def create_session(accept_language: Optional[str] = None) -> requests.Session:
    """Create and configure an HTTP session for scraping.

    The returned session uses a retry strategy to automatically handle
    transient HTTP errors (status codes 429, 500, 502, 503, 504).  A
    random User‑Agent will be set on each request via site‑specific
    scraping functions.

    Args:
        accept_language: Optional string to override the default
            ``Accept-Language`` HTTP header.

    Returns:
        A configured ``requests.Session`` instance ready for use.
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
    headers: Dict[str, str] = {
        # Default User-Agent will be overwritten per request
        "User-Agent": USER_AGENTS[0],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": accept_language or "ja,en-US;q=0.7,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    session.headers.update(headers)
    return session


def random_user_agent() -> str:
    """Return a random User-Agent string from the configured list."""
    return random.choice(USER_AGENTS)


###############################################################################
# Site‑specific scraping functions
###############################################################################


def scrape_kyujinbox(keyword: str, prefecture: str, days: int, max_pages: int, session: requests.Session) -> List[Job]:
    """Scrape job listings from 求人ボックス (kyujinbox.com).

    Args:
        keyword: Search keyword (e.g. "歯科衛生士").
        prefecture: Name of the prefecture in Japanese (e.g. "東京都").
        days: Number of days to look back for new jobs.
        max_pages: Maximum number of pages to fetch.
        session: Configured ``requests.Session``.

    Returns:
        A list of ``Job`` objects.  If the site cannot be reached, the
        function logs a warning and returns an empty list.
    """
    jobs: List[Job] = []
    # Base search URL.  The 'td' parameter controls the time window
    # (1=24h, 3=3 days, etc.).  The 'pref' parameter accepts the
    # prefecture.  'page' parameter controls pagination (starting at 1).
    base_url = "https://xn--pckua2a7gp15o89zb.com/search"
    for page in range(1, max_pages + 1):
        params = {
            "q": keyword,
            "td": days,
            "pref": prefecture,
            "page": page,
        }
        try:
            # Rotate User-Agent per request
            session.headers["User-Agent"] = random_user_agent()
            resp = session.get(base_url, params=params, timeout=15)
            if resp.status_code == 403:
                logging.warning(f"求人ボックス returned 403 for {keyword} / {prefecture} page {page}.")
                break
            resp.raise_for_status()
        except Exception as e:
            logging.warning(f"求人ボックス request error: {e}")
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        listings = soup.select("div.job-card")
        if not listings:
            # No more listings on subsequent pages
            break
        for card in listings:
            try:
                title_el = card.select_one("h2")
                title = title_el.get_text(strip=True) if title_el else ""
                company_el = card.select_one("div.company-name")
                company = company_el.get_text(strip=True) if company_el else ""
                loc_el = card.select_one("div.job-card__company-area")
                location = loc_el.get_text(strip=True) if loc_el else prefecture
                salary_el = card.select_one("span.salary")
                salary = salary_el.get_text(strip=True) if salary_el else ""
                type_el = card.select_one("div.employment-type")
                employment_type = type_el.get_text(strip=True) if type_el else ""
                link_el = card.select_one("a[data-qa='job-link']")
                url = link_el["href"] if link_el and link_el.has_attr("href") else ""
                # Determine if this listing is tagged as new based on label text
                is_new = bool(card.select_one("span.label-new"))
                jobs.append(
                    Job(
                        title=title,
                        company=company,
                        location=location,
                        salary=salary,
                        employment_type=employment_type,
                        industry=keyword,
                        source="求人ボックス",
                        url=url,
                        is_new=is_new,
                        scraped_at=datetime.now().isoformat(),
                    )
                )
            except Exception as e:
                logging.debug(f"求人ボックス parse error: {e}")
        # Polite delay between pages
        time.sleep(random.uniform(*DELAY_RANGE))
    return jobs


def scrape_indeed(keyword: str, prefecture: str, days: int, max_pages: int, session: requests.Session) -> List[Job]:
    """Scrape job listings from Indeed Japan (jp.indeed.com).

    Indeed uses a simple query string: ``q`` for keyword, ``l`` for
    location and ``fromage`` for days since posting.  Pagination is
    controlled via the ``start`` parameter (0, 10, 20, ...).  This
    function fetches pages sequentially up to ``max_pages`` and
    extracts basic job information.

    Args:
        keyword: Search keyword.
        prefecture: Prefecture name in Japanese.
        days: Number of days to look back.
        max_pages: Maximum number of pages to fetch.
        session: Configured HTTP session.

    Returns:
        A list of ``Job`` objects.  Returns an empty list if the site
        cannot be reached.
    """
    jobs: List[Job] = []
    base_url = "https://jp.indeed.com/jobs"
    for page in range(max_pages):
        params = {
            "q": keyword,
            "l": prefecture,
            "fromage": days,
            "sort": "date",
            "start": page * 10,
        }
        try:
            session.headers["User-Agent"] = random_user_agent()
            resp = session.get(base_url, params=params, timeout=15)
            if resp.status_code == 403:
                logging.warning(f"Indeed returned 403 for {keyword} / {prefecture} page {page}.")
                break
            resp.raise_for_status()
        except Exception as e:
            logging.warning(f"Indeed request error: {e}")
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        listings = soup.select("div.job_seen_beacon")
        if not listings:
            break
        for card in listings:
            try:
                title_el = card.select_one("h2.jobTitle span")
                title = title_el.get_text(strip=True) if title_el else ""
                company_el = card.select_one("span.companyName")
                company = company_el.get_text(strip=True) if company_el else ""
                loc_el = card.select_one("div.companyLocation")
                location = loc_el.get_text(strip=True) if loc_el else prefecture
                salary_el = card.select_one("div.metadata.salary-snippet-container")
                salary = salary_el.get_text(strip=True) if salary_el else ""
                type_el = card.select_one("div.metadata div")
                employment_type = type_el.get_text(strip=True) if type_el else ""
                link_el = card.select_one("a[data-jk]")
                url = "https://jp.indeed.com" + link_el["href"] if link_el and link_el.has_attr("href") else ""
                # Indeed pages do not explicitly mark new jobs, but we
                # approximate by using days==1 to denote new listings.
                is_new = days <= 1
                jobs.append(
                    Job(
                        title=title,
                        company=company,
                        location=location,
                        salary=salary,
                        employment_type=employment_type,
                        industry=keyword,
                        source="Indeed",
                        url=url,
                        is_new=is_new,
                        scraped_at=datetime.now().isoformat(),
                    )
                )
            except Exception as e:
                logging.debug(f"Indeed parse error: {e}")
        time.sleep(random.uniform(*DELAY_RANGE))
    return jobs


# Map domain names to site‑specific scraping functions.  If a domain
# listed here is encountered, the corresponding function will be used.
SCRAPERS: Dict[str, callable] = {
    "kyujinbox.com": scrape_kyujinbox,
    "jp.indeed.com": scrape_indeed,
}


###############################################################################
# Main scraping logic
###############################################################################


def parse_industries(arg: Optional[str]) -> List[Tuple[str, str]]:
    """Parse an industries argument into a list of (keyword, label) pairs.

    The argument should be a comma‑separated list of ``keyword:label``.
    If ``label`` is omitted the keyword is used as the label.

    Example: "介護職:介護,経理事務:経理" yields [('介護職','介護'), ('経理事務','経理')].
    """
    if not arg:
        return list(DEFAULT_INDUSTRIES)
    pairs = []
    for item in arg.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            keyword, label = item.split(":", 1)
        else:
            keyword, label = item, item
        pairs.append((keyword.strip(), label.strip()))
    return pairs


def parse_prefectures(arg: Optional[str]) -> List[str]:
    """Parse a comma‑separated list of prefectures.

    If ``arg`` is None returns ``DEFAULT_PREFECTURES``.  Prefecture
    names are stripped of whitespace.
    """
    if not arg:
        return list(DEFAULT_PREFECTURES)
    return [p.strip() for p in arg.split(",") if p.strip()]


def parse_domains(arg: Optional[str]) -> List[str]:
    """Parse a comma‑separated list of domains.

    If ``arg`` is None returns the keys of ``SCRAPERS``.  Domains are
    lowercased and stripped of whitespace.
    """
    if not arg:
        return list(SCRAPERS.keys())
    return [d.strip().lower() for d in arg.split(",") if d.strip()]


def scrape(domain: str, keyword: str, label: str, prefecture: str, days: int, max_pages: int, session: requests.Session) -> List[Job]:
    """Dispatch to the appropriate site‑specific scraper.

    Args:
        domain: Domain name (e.g. 'kyujinbox.com').
        keyword: Search keyword.
        label: Human friendly label for the industry.
        prefecture: Prefecture name.
        days: New job time window.
        max_pages: Maximum pages to scrape.
        session: Shared HTTP session.

    Returns:
        A list of ``Job`` objects.
    """
    if domain not in SCRAPERS:
        logging.warning(f"No scraper available for domain: {domain}")
        return []
    func = SCRAPERS[domain]
    jobs = func(keyword, prefecture, days, max_pages, session)
    # Overwrite industry label and source name for each job
    for job in jobs:
        job.industry = label
        job.source = domain
    return jobs


def write_csv(jobs: List[Job], path: str) -> None:
    """Write a list of Job objects to a CSV file."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
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
        ])
        for job in jobs:
            writer.writerow([
                job.title,
                job.company,
                job.location,
                job.salary,
                job.employment_type,
                job.industry,
                job.source,
                job.url,
                "true" if job.is_new else "false",
                job.scraped_at,
            ])


def write_json(jobs: List[Job], path: str) -> None:
    """Write a list of Job objects to a JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        data = [job.__dict__ for job in jobs]
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_summary(jobs: List[Job], path: str) -> None:
    """Write a summary report to a text file."""
    total = len(jobs)
    by_industry: Dict[str, int] = {}
    for job in jobs:
        by_industry[job.industry] = by_industry.get(job.industry, 0) + 1
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"Total jobs scraped: {total}\n")
        for industry, count in sorted(by_industry.items(), key=lambda x: (-x[1], x[0])):
            f.write(f"{industry}: {count}\n")


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Multi‑site job scraper")
    parser.add_argument("--prefectures", type=str, help="Comma‑separated list of prefectures (e.g. 東京都,神奈川県)")
    parser.add_argument("--industries", type=str, help="Comma‑separated list of keyword:label pairs")
    parser.add_argument("--domains", type=str, help="Comma‑separated list of domains to scrape")
    parser.add_argument("--days", type=int, default=DEFAULT_NEW_DAYS, help="Number of days to look back for new jobs (default: %(default)s)")
    parser.add_argument("--pages", type=int, default=MAX_PAGES_PER_SEARCH, help="Max pages per search (default: %(default)s)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of concurrent workers (default: %(default)s)")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Directory to save output files (default: %(default)s)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s - %(levelname)s - %(message)s")

    prefectures = parse_prefectures(args.prefectures)
    industries = parse_industries(args.industries)
    domains = parse_domains(args.domains)

    logging.info(f"Prefectures: {prefectures}")
    logging.info(f"Industries: {industries}")
    logging.info(f"Domains: {domains}")

    session = create_session()

    # Build the list of scrape tasks
    tasks: List[Tuple[str, str, str, str]] = []  # (domain, keyword, label, prefecture)
    for domain in domains:
        for keyword, label in industries:
            for pref in prefectures:
                tasks.append((domain, keyword, label, pref))

    all_jobs: List[Job] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_task = {
            executor.submit(scrape, domain, keyword, label, pref, args.days, args.pages, session): (domain, keyword, label, pref)
            for domain, keyword, label, pref in tasks
        }
        for future in as_completed(future_to_task):
            domain, keyword, label, pref = future_to_task[future]
            try:
                jobs = future.result()
                logging.info(f"{len(jobs)} jobs scraped from {domain} for {label} in {pref}")
                all_jobs.extend(jobs)
            except Exception as exc:
                logging.error(f"Error scraping {domain} for {label} in {pref}: {exc}")

    # Deduplicate by URL
    unique_jobs: Dict[str, Job] = {}
    for job in all_jobs:
        if job.url and job.url not in unique_jobs:
            unique_jobs[job.url] = job
    deduped_jobs = list(unique_jobs.values())

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = os.path.join(args.output_dir, f"multisite_jobs_{date_str}.csv")
    json_path = os.path.join(args.output_dir, f"multisite_jobs_{date_str}.json")
    summary_path = os.path.join(args.output_dir, f"multisite_summary_{date_str}.txt")

    write_csv(deduped_jobs, csv_path)
    write_json(deduped_jobs, json_path)
    write_summary(deduped_jobs, summary_path)

    logging.info(f"Scraping completed. Total unique jobs: {len(deduped_jobs)}")
    logging.info(f"CSV saved to {csv_path}")
    logging.info(f"JSON saved to {json_path}")
    logging.info(f"Summary saved to {summary_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
