#!/usr/bin/env python3
"""
ingest.py — PKB Ingestion Script
Fetches RSS feeds from sources.json, deduplicates by URL hash,
and saves raw items to /ingestion/raw/{date}/{source_id}.json

Requirements:
    pip install feedparser requests python-dateutil

Usage:
    python scripts/ingest.py                    # fetch all active sources
    python scripts/ingest.py --topic power-bi   # fetch one topic only
    python scripts/ingest.py --source sqlbi     # fetch one source only
    python scripts/ingest.py --dry-run          # print without saving
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent.parent
CONFIG_DIR  = BASE_DIR / "config"
RAW_DIR     = BASE_DIR / "ingestion" / "raw"
HASHES_FILE = BASE_DIR / "ingestion" / "seen_hashes.json"

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict | list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().lower().encode()).hexdigest()[:16]


def load_seen_hashes() -> set:
    if HASHES_FILE.exists():
        data = load_json(HASHES_FILE)
        return set(data.get("hashes", []))
    return set()


def save_seen_hashes(hashes: set) -> None:
    HASHES_FILE.parent.mkdir(parents=True, exist_ok=True)
    save_json(HASHES_FILE, {"hashes": sorted(hashes), "updated_at": datetime.now(timezone.utc).isoformat()})


def parse_rss(feed_url: str) -> list[dict]:
    """Fetch and parse an RSS/Atom feed. Returns list of raw entry dicts."""
    try:
        feed = feedparser.parse(feed_url)
        entries = []
        for entry in feed.entries:
            url  = entry.get("link", "").strip()
            title = entry.get("title", "").strip()
            if not url or not title:
                continue

            # Normalise publication date
            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
            elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc).isoformat()

            entries.append({
                "url":       url,
                "title":     title,
                "summary":   entry.get("summary", ""),
                "author":    entry.get("author", ""),
                "published": published,
            })
        return entries
    except Exception as e:
        print(f"  [WARN] Failed to fetch {feed_url}: {e}", file=sys.stderr)
        return []


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PKB Ingestion Script")
    parser.add_argument("--topic",   help="Filter by topic ID")
    parser.add_argument("--source",  help="Filter by source ID")
    parser.add_argument("--dry-run", action="store_true", help="Print results without saving")
    args = parser.parse_args()

    sources_config = load_json(CONFIG_DIR / "sources.json")
    sources = sources_config["sources"]

    # Apply filters
    if args.topic:
        sources = [s for s in sources if s["topic"] == args.topic]
    if args.source:
        sources = [s for s in sources if s["id"] == args.source]

    active_sources = [s for s in sources if s.get("active", False) and s.get("rss")]

    print(f"[PKB Ingest] {datetime.now().strftime('%Y-%m-%d %H:%M')} — fetching {len(active_sources)} sources")

    seen_hashes = load_seen_hashes()
    today_str = datetime.now().strftime("%Y-%m-%d")
    new_count = 0

    for source in active_sources:
        sid  = source["id"]
        name = source["name"]
        rss  = source["rss"]
        topic = source["topic"]
        tier  = source.get("credibility_tier", 3)

        print(f"  → {name} ({sid})")
        entries = parse_rss(rss)

        new_entries = []
        for entry in entries:
            h = url_hash(entry["url"])
            if h in seen_hashes:
                continue  # already processed
            entry["hash"]        = h
            entry["source_id"]   = sid
            entry["source_name"] = name
            entry["source_tier"] = tier
            entry["topic"]       = topic
            entry["fetched_at"]  = datetime.now(timezone.utc).isoformat()
            entry["status"]      = "raw"
            new_entries.append(entry)

        if not new_entries:
            print(f"     (no new items)")
            continue

        print(f"     {len(new_entries)} new items")

        if not args.dry_run:
            out_path = RAW_DIR / today_str / f"{sid}.json"
            # Load existing entries for this source today (append mode)
            existing = []
            if out_path.exists():
                existing = load_json(out_path)
            save_json(out_path, existing + new_entries)

            for entry in new_entries:
                seen_hashes.add(entry["hash"])
            new_count += len(new_entries)
        else:
            for e in new_entries[:3]:
                print(f"     [DRY] {e['title'][:80]}")
            if len(new_entries) > 3:
                print(f"     [DRY] ... and {len(new_entries) - 3} more")

    if not args.dry_run:
        save_seen_hashes(seen_hashes)
        print(f"\n[PKB Ingest] Done. {new_count} new items saved to {RAW_DIR / today_str}/")
    else:
        print("\n[PKB Ingest] Dry run complete — nothing saved.")


if __name__ == "__main__":
    main()
