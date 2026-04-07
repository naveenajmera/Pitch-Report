#!/usr/bin/env python3
"""
digest.py — PKB Digest Assembly Script
Reads curated items for a date, selects digest-eligible items,
groups them by topic, and writes the final digest JSON.

Usage:
    python scripts/digest.py                          # assemble today's daily digest
    python scripts/digest.py --date 2026-04-04        # assemble for a specific date
    python scripts/digest.py --weekly                 # assemble current week's digest
    python scripts/digest.py --preview                # print digest summary to stdout
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).resolve().parent.parent
CURATED_DIR  = BASE_DIR / "ingestion" / "curated"
DIGESTS_DIR  = BASE_DIR / "digests"

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict | list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_week_str(dt: datetime) -> str:
    return f"{dt.isocalendar().year}-W{dt.isocalendar().week:02d}"


def load_curated_for_date(date_str: str) -> list[dict]:
    path = CURATED_DIR / f"{date_str}.json"
    if not path.exists():
        return []
    data = load_json(path)
    return data if isinstance(data, list) else []


def load_weekly_curated(week_str: str) -> list[dict]:
    """Load all curated items for the dates in a given ISO week."""
    # Find the Monday of the week
    year, week = map(int, week_str.split("-W"))
    monday = datetime.fromisocalendar(year, week, 1)
    all_items = []
    for i in range(7):
        date_str = (monday + timedelta(days=i)).strftime("%Y-%m-%d")
        all_items.extend(load_curated_for_date(date_str))
    return all_items


def check_already_in_prior_digest(item_hash: str, prior_digests_dir: Path) -> bool:
    """Check if this item hash appeared in any prior digest."""
    if not prior_digests_dir.exists():
        return False
    for digest_file in prior_digests_dir.rglob("*.json"):
        try:
            digest = load_json(digest_file)
            for d_item in digest.get("items", []):
                if d_item.get("hash") == item_hash:
                    return True
        except Exception:
            continue
    return False


def build_digest(items: list[dict], digest_type: str, date_str: str) -> dict:
    """Build the final digest structure from curated items."""
    now = datetime.now(timezone.utc)

    # Select eligible items
    eligible = []
    for item in items:
        routing = item.get("routing", {})

        if digest_type == "daily":
            if not routing.get("in_digest", False):
                continue
        elif digest_type == "weekly":
            composite = item.get("scores", {}).get("composite", 0)
            if composite < 6.5:
                continue

        # Check not already in a prior digest
        if check_already_in_prior_digest(item.get("hash", ""), DIGESTS_DIR):
            continue

        eligible.append(item)

    # Sort by composite score descending
    eligible.sort(key=lambda x: x.get("scores", {}).get("composite", 0), reverse=True)

    # Build clean digest items (drop internal-only fields)
    digest_items = []
    for item in eligible:
        di = {
            "id":          item.get("id", ""),
            "topic":       item.get("topic", ""),
            "subtopic":    item.get("subtopic", ""),
            "headline":    item.get("headline", ""),
            "one_liner":   item.get("one_liner", ""),
            "summary":     item.get("summary", ""),
            "url":         item.get("url", ""),
            "source_id":   item.get("source_id", ""),
            "source_name": item.get("source_name", ""),
            "source_tier": item.get("source_tier", 3),
            "published_at": item.get("published_at"),
            "scores":      item.get("scores", {}),
            "tags":        item.get("tags", []),
            "routing":     item.get("routing", {}),
            "wiki_ref":    item.get("wiki_ref"),
            "content_idea_ref": item.get("content_idea_ref"),
            "hash":        item.get("hash", ""),
        }
        digest_items.append(di)

    topics_covered = sorted(set(i["topic"] for i in digest_items))
    dt = datetime.fromisoformat(date_str)

    digest_id = f"digest-{date_str}-{digest_type}" if digest_type == "daily" else f"digest-{get_week_str(dt)}-weekly"

    return {
        "id":             digest_id,
        "type":           digest_type,
        "date":           date_str,
        "week":           get_week_str(dt),
        "generated_at":   now.isoformat(),
        "curator_notes":  "",
        "topics_covered": topics_covered,
        "item_count":     len(digest_items),
        "items":          digest_items,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PKB Digest Assembly Script")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--weekly",  action="store_true", help="Build weekly digest instead of daily")
    parser.add_argument("--preview", action="store_true", help="Print digest summary, no file written")
    args = parser.parse_args()

    digest_type = "weekly" if args.weekly else "daily"

    if digest_type == "weekly":
        dt = datetime.fromisoformat(args.date)
        week_str = get_week_str(dt)
        items = load_weekly_curated(week_str)
        print(f"[PKB Digest] Weekly {week_str} — {len(items)} curated items loaded")
    else:
        items = load_curated_for_date(args.date)
        print(f"[PKB Digest] Daily {args.date} — {len(items)} curated items loaded")

    if not items:
        print("[PKB Digest] No curated items found. Run curate.py first.")
        sys.exit(0)

    digest = build_digest(items, digest_type, args.date)
    digest_items = digest["items"]

    print(f"[PKB Digest] {len(digest_items)} items selected for digest")
    print()

    # Preview
    for i, item in enumerate(digest_items, 1):
        score = item["scores"].get("composite", 0)
        topic = item["topic"].upper()
        print(f"  {i:2}. [{score:.1f}] [{topic}] {item['headline'][:80]}")

    print()

    if args.preview:
        print("[PKB Digest] Preview only — file not written.")
        return

    # Determine output path: digests/YYYY/MM/digest-YYYY-MM-DD-daily.json
    dt = datetime.fromisoformat(args.date)
    out_dir  = DIGESTS_DIR / str(dt.year) / f"{dt.month:02d}"
    out_path = out_dir / f"{digest['id']}.json"

    if out_path.exists():
        print(f"[PKB Digest] Digest already exists at {out_path}")
        print("  Overwrite? (y/N): ", end="")
        if input().strip().lower() != "y":
            print("  Aborted.")
            sys.exit(0)

    save_json(out_path, digest)
    print(f"[PKB Digest] Saved → {out_path}")

    # Print routing summary
    wiki_candidates = [i for i in digest_items if i["routing"].get("wiki_candidate")]
    content_ideas   = [i for i in digest_items if i["routing"].get("content_idea")]
    research_notes  = [i for i in digest_items if i["routing"].get("research_note")]

    print(f"\n  → {len(wiki_candidates)} wiki candidates")
    print(f"  → {len(content_ideas)} content ideas")
    print(f"  → {len(research_notes)} items for research notes")


if __name__ == "__main__":
    main()
