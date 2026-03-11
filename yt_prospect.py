"""
yt_prospect.py — YouTube Channel Prospector for Blinkframe Media
Searches YouTube, filters by subscriber count, appends new prospects to Google Sheets.

Setup:
    pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib python-dotenv

Credentials needed in config.env:
    YOUTUBE_API_KEY=your_youtube_data_api_key
    GOOGLE_SHEET_ID=your_google_sheet_id
    GOOGLE_CREDENTIALS_FILE=credentials.json   # OAuth2 credentials from Google Cloud Console
"""

import os
import re
import json
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv("config.env")

YOUTUBE_API_KEY      = os.getenv("YOUTUBE_API_KEY")
SHEET_ID             = os.getenv("GOOGLE_SHEET_ID")
CREDENTIALS_FILE     = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE           = "token.pickle"
QUERIES_FILE         = "queries.json"
SHEET_NAME           = "Sheet1"   # Change if your tab has a different name
LOG_FILE             = "prospect_runs.log"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Expected header columns — order doesn't matter, script maps by name
REQUIRED_HEADERS = [
    "channel_name", "channel_url", "subscriber_count", "podcast?",
    "found_date", "email_date", "contact", "email", "portfolio?", "response?", "ruled_out", "found_via", "starred", "niche", "pillar"
]

TIME_WINDOW_OPTIONS = {
    "1": ("24 hours",  timedelta(hours=24)),
    "2": ("7 days",    timedelta(days=7)),
    "3": ("30 days",   timedelta(days=30)),
}

LANGUAGE_OPTIONS = {
    "1": ("English",  "en"),
    "2": ("Spanish",  "es"),
    "3": ("French",   "fr"),
    "4": ("German",   "de"),
    "5": ("Any",      None),
}

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def log(msg: str):
    print(msg)
    logging.info(msg)

DURATION_OPTIONS = {
    "1": ("Medium (4–20 min)",  ["medium"]),
    "2": ("Long (20+ min)",     ["long"]),
    "3": ("Both",               ["medium", "long"]),
}

# Keywords that trigger a skip if found in channel name or video title (case-insensitive)
BLACKLIST_KEYWORDS = [
    "english", "learn english", "esl", "español", "deutsch", "français",
    "university", "lifestyle", "reaction", "day in my life", "vlog life"
]

NICHE_OPTIONS = {
    "1":  "Trades",
    "2":  "Real Estate",
    "3":  "Martial Arts",
    "4":  "Finance",
    "5":  "Personal Development",
    "6":  "Fitness",
    "7":  "Med Spa",
    "8":  "Dental",
    "9":  "Restaurant",
    "10": "Law",
    "11": "Home Services",
}

# ── Auth ──────────────────────────────────────────────────────────────────────

def get_sheets_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)
    return build("sheets", "v4", credentials=creds)

def get_youtube_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# ── Sheet helpers ─────────────────────────────────────────────────────────────

def get_sheet_data(sheets_svc):
    result = sheets_svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=SHEET_NAME
    ).execute()
    return result.get("values", [])

def validate_headers(rows: list) -> dict:
    """Returns a dict mapping header name -> column index. Exits if headers are missing."""
    if not rows:
        print("\n[ERROR] Sheet appears to be empty. Add your header row first.")
        print("Expected headers:", ", ".join(REQUIRED_HEADERS))
        exit(1)

    headers = rows[0]
    header_map = {h.strip(): i for i, h in enumerate(headers)}

    missing = [h for h in REQUIRED_HEADERS if h not in header_map]
    if missing:
        print(f"\n[ERROR] Missing expected headers: {missing}")
        print("Check your sheet's header row and try again.")
        exit(1)

    print(f"[OK] Headers validated. {len(headers)} columns found.")
    return header_map

def get_existing_urls(rows: list, header_map: dict) -> set:
    url_col = header_map["channel_url"]
    urls = set()
    for row in rows[1:]:  # skip header
        if len(row) > url_col and row[url_col].strip():
            urls.add(row[url_col].strip())
    return urls

def append_rows(sheets_svc, new_rows: list):
    body = {"values": new_rows}
    sheets_svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=SHEET_NAME,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

# ── YouTube helpers ───────────────────────────────────────────────────────────

MAX_PAGES_PER_SEARCH = 10  # hard cap to prevent infinite loops

def search_videos(yt_svc, query: str, max_results: int, published_after: str, language: str, duration: str, skip_pages: int = 0):
    """Returns list of (videoId, channelId, title) tuples. Skips first skip_pages pages of results."""
    results = []
    next_page_token = None
    fetched = 0
    pages_seen = 0

    while fetched < max_results and pages_seen < MAX_PAGES_PER_SEARCH:
        batch = min(50, max_results - fetched)
        kwargs = dict(
            part="snippet",
            q=query,
            type="video",
            order="relevance",
            publishedAfter=published_after,
            maxResults=batch,
            pageToken=next_page_token,
        )
        if language:
            kwargs["relevanceLanguage"] = language
        if duration:
            kwargs["videoDuration"] = duration

        print(f"      [page {pages_seen + 1}] fetching...", end=" ", flush=True)
        resp = yt_svc.search().list(**kwargs).execute()
        items = resp.get("items", [])
        pages_seen += 1

        if pages_seen <= skip_pages:
            print(f"skipped (page {pages_seen} of {skip_pages} to skip)")
        else:
            for item in items:
                title = item["snippet"].get("title", "")
                results.append((item["id"]["videoId"], item["snippet"]["channelId"], title))
            fetched += len(items)
            print(f"{len(items)} videos ({fetched} collected so far)")

        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            print(f"      [no more pages]")
            break
        time.sleep(random.uniform(1, 3))

    if pages_seen >= MAX_PAGES_PER_SEARCH:
        print(f"      [hit page cap of {MAX_PAGES_PER_SEARCH}]")

    return results

def get_channel_details(yt_svc, channel_map: dict) -> dict:
    """channel_map: {channelId: video_title}. Returns dict of channelId -> details."""
    channel_ids = list(channel_map.keys())
    """Returns dict of channelId -> {name, url, subs}. Batches in groups of 50."""
    details = {}
    ids = list(channel_ids)
    for i in range(0, len(ids), 50):
        batch = ids[i:i+50]
        resp = yt_svc.channels().list(
            part="snippet,statistics",
            id=",".join(batch)
        ).execute()
        for item in resp.get("items", []):
            cid = item["id"]
            subs_raw = item["statistics"].get("subscriberCount", "0")
            details[cid] = {
                "name":       item["snippet"]["title"],
                "url":        f"https://www.youtube.com/channel/{cid}",
                "subs":       int(subs_raw) if subs_raw else 0,
                "country":    item["snippet"].get("country", ""),
                "video_title": channel_map.get(cid, ""),
            }
        time.sleep(random.uniform(1, 2))
    return details

def load_niche_queries() -> dict:
    if not os.path.exists(QUERIES_FILE):
        print(f"[ERROR] {QUERIES_FILE} not found. Place it alongside yt_prospect.py.")
        exit(1)
    with open(QUERIES_FILE, "r") as f:
        return json.load(f)

# ── Runtime prompts ───────────────────────────────────────────────────────────

def prompt_int(label: str, default: int) -> int:
    val = input(f"{label} [default {default}]: ").strip()
    return int(val) if val.isdigit() else default

def prompt_config() -> dict:
    print("\n── Blinkframe YouTube Prospector ──────────────────────")

    NICHE_QUERIES = load_niche_queries()
    NICHE_OPTIONS = {str(i+1): niche for i, niche in enumerate(NICHE_QUERIES.keys())}

    print("\nNiche:")
    for k, label in NICHE_OPTIONS.items():
        print(f"  {k}) {label}")
    niche_choice = input(f"Choose [1-{len(NICHE_OPTIONS)}, default 1]: ").strip() or "1"
    niche = NICHE_OPTIONS.get(niche_choice, NICHE_OPTIONS["1"])
    queries = NICHE_QUERIES[niche]
    print(f"  Loaded {len(queries)} queries for '{niche}': {queries}")

    sub_floor        = prompt_int("Min subscribers", 1000)
    sub_ceiling      = prompt_int("Max subscribers", 50000)
    target_prospects = prompt_int("Target new prospects per query (max to add)", 50)
    max_results      = target_prospects * 6  # fetch 6x to account for dedup/filter/country attrition

    skip_pages = prompt_int("Skip first X pages of results (50 results/page, default 1)", 1)

    print("\nTime window:")
    for k, (label, _) in TIME_WINDOW_OPTIONS.items():
        print(f"  {k}) {label}")
    tw_choice = input("Choose [1/2/3, default 2]: ").strip() or "2"
    tw_label, tw_delta = TIME_WINDOW_OPTIONS.get(tw_choice, TIME_WINDOW_OPTIONS["2"])

    print("\nLanguage filter:")
    for k, (label, _) in LANGUAGE_OPTIONS.items():
        print(f"  {k}) {label}")
    lang_choice = input("Choose [1-5, default 1]: ").strip() or "1"
    lang_label, lang_code = LANGUAGE_OPTIONS.get(lang_choice, LANGUAGE_OPTIONS["1"])

    published_after = (datetime.now(timezone.utc) - tw_delta).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("\nDuration filter:")
    for k, (label, _) in DURATION_OPTIONS.items():
        print(f"  {k}) {label}")
    dur_choice = input("Choose [1/2/3, default 3]: ").strip() or "3"
    dur_label, dur_values = DURATION_OPTIONS.get(dur_choice, DURATION_OPTIONS["3"])

    print(f"\n[Config] Queries={queries} | Niche={niche} | Subs={sub_floor}–{sub_ceiling} | "
          f"Target={target_prospects}/query | Window={tw_label} | Language={lang_label} | Duration={dur_label}")
    confirm = input("Proceed? [Y/n]: ").strip().lower()
    if confirm == "n":
        exit(0)

    return {
        "queries":          queries,
        "niche":            niche,
        "sub_floor":        sub_floor,
        "sub_ceiling":      sub_ceiling,
        "max_results":      max_results,
        "target_prospects": target_prospects,
        "published_after":  published_after,
        "lang_code":        lang_code,
        "tw_label":         tw_label,
        "lang_label":       lang_label,
        "dur_values":       dur_values,
        "dur_label":        dur_label,
        "skip_pages":       skip_pages,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run_query(yt_svc, sheets_svc, cfg, query, header_map, existing_urls, today):
    """Run a single query and append results to sheet. Returns (new_count, dupe_count, skip_count)."""
    print(f"\n── Query: '{query}' ──────────────────────────────────")
    results = []
    per_duration = cfg["max_results"] // len(cfg["dur_values"])
    for dur in cfg["dur_values"]:
        batch = search_videos(
            yt_svc,
            query,
            per_duration,
            cfg["published_after"],
            cfg["lang_code"],
            dur,
            cfg["skip_pages"]
        )
        results.extend(batch)
        print(f"      [{dur}] {len(batch)} videos returned.")
    print(f"      {len(results)} total videos.")

    # Deduplicate channel IDs
    seen_channels = {}
    for _vid, cid, title in results:
        if cid not in seen_channels:
            seen_channels[cid] = title

    print(f"      Fetching details for {len(seen_channels)} unique channels...")
    channel_details = get_channel_details(yt_svc, seen_channels)

    print(f"      Filtering...")
    new_rows            = []
    dupe_count          = 0
    skip_count          = 0
    dupe_urls_to_update = {}  # url -> {query, niche} for updating found_via and niche on dupes
    num_cols            = len(header_map)

    for cid, info in channel_details.items():
        subs = info["subs"]
        url  = info["url"]

        if subs < cfg["sub_floor"] or subs > cfg["sub_ceiling"]:
            skip_count += 1
            continue

        if info["country"] and info["country"] != "US":
            log(f"[SKIP] {info['name']} — country='{info['country']}' (not US)")
            skip_count += 1
            continue

        combined_text = (info["name"] + " " + info["video_title"]).lower()
        blacklisted = next((kw for kw in BLACKLIST_KEYWORDS if kw in combined_text), None)
        if blacklisted:
            log(f"[SKIP] {info['name']} — matched blacklist keyword '{blacklisted}'")
            skip_count += 1
            continue

        if len(new_rows) >= cfg["target_prospects"]:
            skip_count += 1
            continue

        if url in existing_urls:
            log(f"[DUPE] {info['name']} ({url}) already in sheet — checking found_via.")
            dupe_urls_to_update[url] = {"query": query, "niche": cfg["niche"]}
            dupe_count += 1
            continue

        row = [""] * num_cols
        row[header_map["channel_name"]]      = info["name"]
        row[header_map["channel_url"]]       = url
        row[header_map["subscriber_count"]]  = subs
        row[header_map["podcast?"]]          = ""
        row[header_map["found_date"]]        = today
        row[header_map["found_via"]]         = query
        row[header_map["niche"]]             = cfg["niche"]

        new_rows.append(row)
        existing_urls.add(url)

    if new_rows:
        append_rows(sheets_svc, new_rows)

    # Update found_via and niche for dupes
    if dupe_urls_to_update:
        updated    = 0
        url_col    = header_map["channel_url"]
        via_col    = header_map["found_via"]
        niche_col  = header_map["niche"]
        sheet_rows = get_sheet_data(sheets_svc)
        for row_idx, row in enumerate(sheet_rows[1:], start=2):
            if len(row) <= url_col:
                continue
            row_url = row[url_col].strip()
            if row_url in dupe_urls_to_update:
                update_data  = dupe_urls_to_update[row_url]
                new_query    = update_data["query"]
                new_niche    = update_data["niche"]

                # Update found_via
                existing_via = row[via_col].strip() if len(row) > via_col else ""
                via_terms = [t.strip() for t in existing_via.split(",") if t.strip()]
                via_changed = False
                if new_query not in via_terms:
                    via_terms.append(new_query)
                    via_changed = True

                # Update niche
                existing_niche = row[niche_col].strip() if len(row) > niche_col else ""
                niche_terms = [t.strip() for t in existing_niche.split(",") if t.strip()]
                niche_changed = False
                if new_niche not in niche_terms:
                    niche_terms.append(new_niche)
                    niche_changed = True

                if via_changed:
                    cell = f"{SHEET_NAME}!{chr(65 + via_col)}{row_idx}"
                    sheets_svc.spreadsheets().values().update(
                        spreadsheetId=SHEET_ID, range=cell,
                        valueInputOption="RAW",
                        body={"values": [[", ".join(via_terms)]]}
                    ).execute()

                if niche_changed:
                    cell = f"{SHEET_NAME}!{chr(65 + niche_col)}{row_idx}"
                    sheets_svc.spreadsheets().values().update(
                        spreadsheetId=SHEET_ID, range=cell,
                        valueInputOption="RAW",
                        body={"values": [[", ".join(niche_terms)]]}
                    ).execute()

                if via_changed or niche_changed:
                    log(f"[DUPE UPDATE] {row_url} | found_via→{', '.join(via_terms)} | niche→{', '.join(niche_terms)}")
                    updated += 1

    summary = (f"  Query='{query}' | Videos={len(results)} | Channels={len(channel_details)} | "
               f"Added={len(new_rows)} | Dupes={dupe_count} | Skipped={skip_count}")
    log(summary)
    return len(new_rows), dupe_count, skip_count


def main():
    if not YOUTUBE_API_KEY or not SHEET_ID:
        print("[ERROR] Missing YOUTUBE_API_KEY or GOOGLE_SHEET_ID in config.env")
        exit(1)

    print("\n[1/4] Authenticating with Google Sheets...")
    sheets_svc = get_sheets_service()

    print("[2/4] Validating sheet headers...")
    rows = get_sheet_data(sheets_svc)
    header_map    = validate_headers(rows)
    existing_urls = get_existing_urls(rows, header_map)
    print(f"      {len(existing_urls)} existing prospects found in sheet.")

    cfg   = prompt_config()
    yt_svc = get_youtube_service()
    today  = datetime.now().strftime("%Y-%m-%d")

    print(f"\n[3/4] Running {len(cfg['queries'])} quer{'y' if len(cfg['queries']) == 1 else 'ies'}...")
    total_added = total_dupes = total_skipped = 0
    for i, query in enumerate(cfg["queries"], 1):
        print(f"\n  [{i}/{len(cfg['queries'])}]", end="")
        added, dupes, skipped = run_query(yt_svc, sheets_svc, cfg, query, header_map, existing_urls, today)
        total_added   += added
        total_dupes   += dupes
        total_skipped += skipped
        if i < len(cfg["queries"]):
            delay = random.uniform(2, 4)
            print(f"      Sleeping {delay:.1f}s before next query...")
            time.sleep(delay)

    print(f"\n[4/4] Done.")
    summary = (
        f"Batch complete | Niche={cfg['niche']} | Queries={len(cfg['queries'])} | "
        f"Window={cfg['tw_label']} | Subs={cfg['sub_floor']}–{cfg['sub_ceiling']} | "
        f"Total Added={total_added} | Total Dupes={total_dupes} | Total Skipped={total_skipped}"
    )
    log(f"\n{'─'*60}")
    log(summary)
    log(f"{'─'*60}\n")

if __name__ == "__main__":
    main()
