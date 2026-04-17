"""
fetch_triggers.py — Scan Exa for procurement-relevant trigger events at each
prospect company. Use Claude to classify each trigger. Write triggers.csv.

Usage:
    python scripts/fetch_triggers.py              # full run, writes triggers.csv
    python scripts/fetch_triggers.py --dry-run    # print results, no write
    python scripts/fetch_triggers.py --company "Continental AG"  # single company
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths and env
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
INSIGHTS_CSV = ROOT / "pipeline" / "akirolabs" / "insights.csv"
TRIGGERS_CSV = ROOT / "pipeline" / "akirolabs" / "triggers.csv"
MODEL = "claude-sonnet-4-6"

TRIGGER_ANGLE_MAP = {
    "leadership_change": "angle2",
    "transformation": "angle1",
    "earnings_signal": "angle1",
    "headcount_cut": "angle1",
    "peer_pressure": "angle3",
    "industry_news": "angle3",
    "none": "none",
}

CLASSIFY_SYSTEM = """\
You are a BDR signal analyst. Given news snippets about a company, identify the \
single most relevant trigger event for selling procurement strategy software."""

CLASSIFY_USER_TEMPLATE = """\
Company: {company}
Industry: {industry}

News snippets:
{snippets}

Trigger types:
- leadership_change: new CPO, new Head of Procurement, new CFO (procurement implications)
- transformation: restructuring, cost reduction programme, M&A, spinoff
- earnings_signal: earnings call or investor report mentioning supply chain, procurement, or indirect spend
- headcount_cut: layoffs in procurement or finance functions
- peer_pressure: a direct competitor of this company announced a procurement technology initiative
- industry_news: sector-wide regulatory or market pressure on procurement
- none: nothing procurement-relevant found

Return ONLY valid JSON (no markdown fences):
{{"trigger_type": "...", "trigger_summary": "...", "trigger_url": "...", "urgency": 1}}

Rules:
- trigger_summary: 1-2 sentences, factual, past tense, specific to this company. Write like Reuters wire copy.
- If multiple triggers, pick highest urgency one.
- If no relevant signal: trigger_type="none", other fields="", urgency=0
- urgency: 3=major event last 90 days, 2=moderate signal, 1=weak/older
- No em dashes in trigger_summary. No "actually"."""


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def load_companies(filter_company: str | None = None) -> list[dict]:
    """Load companies from insights.csv."""
    if not INSIGHTS_CSV.is_file():
        print(f"ERROR: {INSIGHTS_CSV} not found.", file=sys.stderr)
        sys.exit(1)
    with INSIGHTS_CSV.open(newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if filter_company:
        rows = [r for r in rows if r.get("company", "").strip().lower() == filter_company.strip().lower()]
        if not rows:
            print(f"ERROR: company '{filter_company}' not found in insights.csv.", file=sys.stderr)
            sys.exit(1)
    return rows


def search_exa(exa, company: str) -> list[dict]:
    """Run 3 Exa searches for a company, return list of snippet dicts."""
    queries = [
        f'"{company}" CPO OR "chief procurement officer" OR "head of procurement" OR "head of strategic sourcing" 2025 2026',
        f'"{company}" restructuring OR "cost reduction" OR "cost transformation" OR M&A OR merger 2025 2026',
        f'"{company}" "procurement strategy" OR "indirect spend" OR "supply chain" 2025 2026',
    ]
    snippets = []
    for query in queries:
        try:
            results = exa.search_and_contents(
                query,
                num_results=3,
                text={"max_characters": 300},
            )
            if results.results:
                top = results.results[0]
                title = (top.title or "").strip()
                url = (top.url or "").strip()
                text = (top.text or "").strip() if hasattr(top, "text") else ""
                if title:
                    snippets.append({"title": title, "url": url, "text": text})
        except Exception as exc:
            print(f"  Warning: Exa search failed for query: {exc}", file=sys.stderr)
    return snippets


def classify_trigger(client, company: str, industry: str, snippets: list[dict]) -> dict:
    """Use Claude to classify snippets into a trigger type."""
    if not snippets:
        return {
            "trigger_type": "none",
            "trigger_summary": "",
            "trigger_url": "",
            "urgency": 0,
        }

    snippet_text = ""
    for i, s in enumerate(snippets, 1):
        snippet_text += f"{i}. {s['title']}\n   URL: {s['url']}\n   {s['text']}\n\n"

    prompt = CLASSIFY_USER_TEMPLATE.format(
        company=company,
        industry=industry,
        snippets=snippet_text.strip(),
    )

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=CLASSIFY_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
            if raw.lower().startswith("json"):
                raw = raw[4:].strip()
        result = json.loads(raw)
        # Validate required fields
        trigger_type = result.get("trigger_type", "none")
        if trigger_type not in TRIGGER_ANGLE_MAP:
            trigger_type = "none"
        return {
            "trigger_type": trigger_type,
            "trigger_summary": result.get("trigger_summary", ""),
            "trigger_url": result.get("trigger_url", ""),
            "urgency": int(result.get("urgency", 0)),
        }
    except Exception as exc:
        print(f"  Warning: Claude classification failed: {exc}", file=sys.stderr)
        return {
            "trigger_type": "none",
            "trigger_summary": "",
            "trigger_url": "",
            "urgency": 0,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch procurement trigger signals via Exa + Claude.")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing CSV.")
    parser.add_argument("--company", type=str, default=None, help="Process a single company only.")
    args = parser.parse_args()

    load_env_file(ROOT / ".env")

    exa_api_key = os.environ.get("EXA_API_KEY", "")
    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not exa_api_key:
        print("ERROR: EXA_API_KEY not set. Add it to .env.", file=sys.stderr)
        sys.exit(1)
    if not anthropic_api_key:
        print("ERROR: ANTHROPIC_API_KEY not set. Add it to .env.", file=sys.stderr)
        sys.exit(1)

    try:
        from exa_py import Exa
    except ImportError:
        print("ERROR: exa_py not installed. Run: pip install exa_py", file=sys.stderr)
        sys.exit(1)

    try:
        import anthropic
    except ImportError:
        print("ERROR: anthropic not installed. Run: pip install anthropic", file=sys.stderr)
        sys.exit(1)

    companies = load_companies(args.company)
    total = len(companies)
    exa = Exa(exa_api_key)
    client = anthropic.Anthropic(api_key=anthropic_api_key)

    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []
    trigger_count = 0
    high_urgency = 0
    moderate_urgency = 0

    for idx, row in enumerate(companies, 1):
        company = row.get("company", "").strip()
        industry = row.get("industry", "").strip()
        company_id = row.get("id", "").strip()

        print(f"Fetching signals {idx}/{total}: {company}...")

        snippets = search_exa(exa, company)
        classification = classify_trigger(client, company, industry, snippets)

        trigger_type = classification["trigger_type"]
        recommended_angle = TRIGGER_ANGLE_MAP.get(trigger_type, "none")
        urgency = classification["urgency"]

        result_row = {
            "company_id": company_id,
            "company": company,
            "trigger_type": trigger_type,
            "trigger_summary": classification["trigger_summary"],
            "trigger_url": classification["trigger_url"],
            "recommended_angle": recommended_angle,
            "urgency": urgency,
            "fetched_at": now,
        }
        results.append(result_row)

        if trigger_type != "none":
            trigger_count += 1
            if urgency == 3:
                high_urgency += 1
            elif urgency == 2:
                moderate_urgency += 1

        # Print per-company result
        if trigger_type != "none":
            angle_name = {
                "angle1": "Speed Gap",
                "angle2": "Suite Fatigue",
                "angle3": "Spend Trigger",
            }.get(recommended_angle, recommended_angle)
            print(f"  -> {trigger_type} (urgency {urgency}) -> {angle_name}")
            summary = classification["trigger_summary"]
            if summary:
                print(f"     {summary[:120]}")
        else:
            print("  -> no trigger found")

    # Summary
    print()
    print(f"Done. {trigger_count} triggers found ({high_urgency} high urgency, {moderate_urgency} moderate).", end="")

    if args.dry_run:
        print(" (dry run, no file written)")
    else:
        # Write CSV
        TRIGGERS_CSV.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "company_id", "company", "trigger_type", "trigger_summary",
            "trigger_url", "recommended_angle", "urgency", "fetched_at",
        ]
        with TRIGGERS_CSV.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f" Wrote {TRIGGERS_CSV.relative_to(ROOT)}.")


if __name__ == "__main__":
    main()
