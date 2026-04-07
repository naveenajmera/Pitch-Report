#!/usr/bin/env python3
"""
curate.py — PKB Curation Script
Reads raw ingested items, calls the AI API with topic-specific prompts,
calculates composite scores, applies routing logic, and saves curated items.

Requirements:
    pip install anthropic python-dotenv

Usage:
    python scripts/curate.py                          # curate today's raw items
    python scripts/curate.py --date 2026-04-04        # curate a specific date
    python scripts/curate.py --topic power-bi         # curate one topic only
    python scripts/curate.py --dry-run                # print output, no API calls

Environment:
    Create a .env file in the pkb/ root:
        ANTHROPIC_API_KEY=sk-ant-...
        # or
        OPENAI_API_KEY=sk-...
        AI_PROVIDER=anthropic   # or: openai
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
CONFIG_DIR    = BASE_DIR / "config"
PROMPTS_FILE  = BASE_DIR / "prompts" / "curator-prompts.json"
RAW_DIR       = BASE_DIR / "ingestion" / "raw"
CURATED_DIR   = BASE_DIR / "ingestion" / "curated"

load_dotenv(BASE_DIR / ".env")

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict | list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def calculate_composite(scores: dict) -> float:
    weights = {
        "relevance":          0.25,
        "practical_value":    0.20,
        "novelty":            0.20,
        "source_credibility": 0.20,
        "content_potential":  0.15,
    }
    return round(sum(scores.get(k, 5) * w for k, w in weights.items()), 2)


def apply_routing(scores: dict, composite: float, published_at: str | None) -> dict:
    """Apply routing rules from scoring.json thresholds."""
    age_hours = 999
    if published_at:
        try:
            pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            age_hours = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
        except Exception:
            pass

    return {
        "in_digest":      composite >= 7.5 and age_hours <= 48,
        "research_note":  composite >= 6.5 and scores.get("practical_value", 0) >= 8,
        "wiki_candidate": composite >= 8.5 or scores.get("relevance", 0) >= 9,
        "content_idea":   scores.get("content_potential", 0) >= 8 and scores.get("novelty", 0) >= 7,
    }


def call_ai_api(prompt: str, article_text: str) -> dict | None:
    """Call the configured AI provider. Returns parsed JSON or None on failure."""
    provider = os.getenv("AI_PROVIDER", "anthropic").lower()
    full_prompt = f"{prompt}\n\n---\nARTICLE TEXT:\n{article_text[:4000]}"

    if provider == "anthropic":
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            message = client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=1024,
                messages=[{"role": "user", "content": full_prompt}]
            )
            raw = message.content[0].text
            # Extract JSON from response
            start = raw.find("{")
            end   = raw.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(raw[start:end])
        except Exception as e:
            print(f"  [ERROR] Anthropic API call failed: {e}", file=sys.stderr)

    elif provider == "openai":
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": full_prompt}],
                response_format={"type": "json_object"},
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"  [ERROR] OpenAI API call failed: {e}", file=sys.stderr)

    return None


def build_curation_prompt(topic: str, prompts_config: dict, raw_item: dict) -> str:
    """Build the full AI prompt for a raw item."""
    topic_cfg = prompts_config["topic_prompts"].get(topic, {})
    system    = prompts_config.get("system_prompt", "")
    role      = topic_cfg.get("role", "")
    instructions = "\n".join(f"- {i}" for i in topic_cfg.get("instructions", []))
    output_fmt   = topic_cfg.get("output_format", "Return JSON with: headline, one_liner, summary, subtopic, scores (relevance, practical_value, novelty, source_credibility, content_potential), tags, routing (research_note, wiki_candidate, content_idea).")

    title   = raw_item.get("title", "")
    summary = raw_item.get("summary", "")
    url     = raw_item.get("url", "")

    article_text = f"Title: {title}\nURL: {url}\nExcerpt: {summary}"

    return f"""{system}

ROLE: {role}

ARTICLE ANALYSIS INSTRUCTIONS:
{instructions}

OUTPUT FORMAT:
{output_fmt}

---
ARTICLE:
{article_text}
"""


def score_from_tier(tier: int) -> int:
    """Convert source tier to credibility score."""
    return {1: 10, 2: 8, 3: 6, 4: 3}.get(tier, 5)


def curate_item(raw_item: dict, prompts_config: dict, dry_run: bool) -> dict | None:
    """Curate a single raw item. Returns curated dict or None if skipped."""
    topic      = raw_item.get("topic", "unknown")
    source_tier = raw_item.get("source_tier", 3)
    source_id   = raw_item.get("source_id", "")

    # Auto-reject tier 4 without explicit override
    if source_tier == 4:
        print(f"     [SKIP] Tier-4 source ({source_id}) — requires manual approval")
        return None

    # Assign credibility score from source tier
    credibility_score = score_from_tier(source_tier)

    if dry_run:
        # Produce a mock curated item without an API call
        scores = {
            "relevance":          7,
            "practical_value":    7,
            "novelty":            7,
            "source_credibility": credibility_score,
            "content_potential":  6,
        }
        scores["composite"] = calculate_composite(scores)
        routing = apply_routing(scores, scores["composite"], raw_item.get("published"))
        return {
            "id":          f"ci-{raw_item['hash']}",
            "topic":       topic,
            "headline":    raw_item.get("title", ""),
            "one_liner":   "[DRY RUN — AI not called]",
            "summary":     raw_item.get("summary", "")[:300],
            "url":         raw_item.get("url", ""),
            "source_id":   source_id,
            "source_name": raw_item.get("source_name", ""),
            "source_tier": source_tier,
            "published_at": raw_item.get("published"),
            "scores":      scores,
            "tags":        [],
            "routing":     routing,
            "hash":        raw_item.get("hash", ""),
            "curated_at":  datetime.now(timezone.utc).isoformat(),
            "ai_used":     False,
        }

    # Build prompt and call AI
    prompt = build_curation_prompt(topic, prompts_config, raw_item)
    ai_result = call_ai_api(prompt, raw_item.get("summary", "") + " " + raw_item.get("title", ""))

    if not ai_result:
        print(f"     [WARN] AI call failed for: {raw_item.get('title', '')[:60]}")
        return None

    # Merge AI scores with credibility from source tier
    ai_scores = ai_result.get("scores", {})
    if "source_credibility" not in ai_scores:
        ai_scores["source_credibility"] = credibility_score
    ai_scores["composite"] = calculate_composite(ai_scores)

    routing = apply_routing(ai_scores, ai_scores["composite"], raw_item.get("published"))

    # Override routing flags from AI if it provided them
    if "routing" in ai_result:
        for key in ["research_note", "wiki_candidate", "content_idea"]:
            if key in ai_result["routing"]:
                routing[key] = routing[key] or ai_result["routing"][key]

    return {
        "id":          f"di-{raw_item['hash']}",
        "topic":       topic,
        "subtopic":    ai_result.get("subtopic", ""),
        "headline":    ai_result.get("headline", raw_item.get("title", "")),
        "one_liner":   ai_result.get("one_liner", ""),
        "summary":     ai_result.get("summary", ""),
        "url":         raw_item.get("url", ""),
        "source_id":   source_id,
        "source_name": raw_item.get("source_name", ""),
        "source_tier": source_tier,
        "published_at": raw_item.get("published"),
        "scores":      ai_scores,
        "tags":        ai_result.get("tags", []),
        "routing":     routing,
        "wiki_ref":    None,
        "content_idea_ref": None,
        "hash":        raw_item.get("hash", ""),
        "curated_at":  datetime.now(timezone.utc).isoformat(),
        "ai_used":     True,
        "stats_extract": ai_result.get("stats_extract"),  # for cricket items
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PKB Curation Script")
    parser.add_argument("--date",    default=datetime.now().strftime("%Y-%m-%d"), help="Date folder to curate (YYYY-MM-DD)")
    parser.add_argument("--topic",   help="Filter by topic ID")
    parser.add_argument("--dry-run", action="store_true", help="Mock curation without API calls")
    args = parser.parse_args()

    raw_date_dir = RAW_DIR / args.date
    if not raw_date_dir.exists():
        print(f"[PKB Curate] No raw data for {args.date}. Run ingest.py first.")
        sys.exit(0)

    prompts_config = load_json(PROMPTS_FILE)

    raw_files = list(raw_date_dir.glob("*.json"))
    if args.topic:
        # Filter: load each file's items and check topic
        pass  # handled per-item below

    print(f"[PKB Curate] {args.date} — {len(raw_files)} source files to process")

    curated_today = []

    for raw_file in raw_files:
        raw_items = load_json(raw_file)
        if not isinstance(raw_items, list):
            raw_items = [raw_items]

        source_label = raw_file.stem
        print(f"  → {source_label} ({len(raw_items)} items)")

        for raw_item in raw_items:
            if args.topic and raw_item.get("topic") != args.topic:
                continue

            title = raw_item.get("title", "")[:70]
            result = curate_item(raw_item, prompts_config, args.dry_run)

            if result:
                score = result["scores"].get("composite", 0)
                flag  = "★ DIGEST" if result["routing"].get("in_digest") else ""
                print(f"     [{score:.1f}] {title} {flag}")
                curated_today.append(result)

    if not args.dry_run:
        out_path = CURATED_DIR / f"{args.date}.json"
        # Merge with any existing curated items for the day
        existing = []
        if out_path.exists():
            existing = load_json(out_path)
            existing_hashes = {i.get("hash") for i in existing}
            curated_today = [i for i in curated_today if i.get("hash") not in existing_hashes]

        save_json(out_path, existing + curated_today)
        print(f"\n[PKB Curate] Done. {len(curated_today)} new curated items → {out_path}")
    else:
        digest_candidates = [i for i in curated_today if i["routing"].get("in_digest")]
        print(f"\n[PKB Curate] Dry run. {len(curated_today)} processed, {len(digest_candidates)} would go to digest.")


if __name__ == "__main__":
    main()
