"""CT digest golden-set gate.

Offline (default): schema, taxonomy coverage, injection escape — no API.
--llm: classify_batch against gold. Pass = bucket AND included match.

    python evals/ct_digest/run.py
    python evals/ct_digest/run.py --llm
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

GOLDEN_PATH = Path(__file__).with_name("golden_v1.json")


def load_golden(path: Path = GOLDEN_PATH) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def offline_checks(data: dict) -> list[str]:
    from services.ct_digest.classifier import VALID_BUCKETS, _build_user_message
    from services.ct_digest.collectors import RawItem

    errors: list[str] = []
    items = data.get("items") or []
    if len(items) < 10:
        errors.append(f"too few items: {len(items)}")

    ids = [it.get("id") for it in items]
    if len(ids) != len(set(ids)):
        errors.append("duplicate ids")

    buckets = set()
    for it in items:
        bid = it.get("id")
        bucket = it.get("gold_bucket")
        if bucket not in VALID_BUCKETS:
            errors.append(f"{bid}: unknown bucket {bucket}")
        else:
            buckets.add(bucket)
        if not isinstance(it.get("gold_included"), bool):
            errors.append(f"{bid}: gold_included must be bool")
        if not (it.get("raw_text") or "").strip():
            errors.append(f"{bid}: empty raw_text")

    required = {"early_signals", "calendar_24h", "calendar_3d", "calendar_7d",
                "state_reconcile", "paid_hype"}
    missing = required - buckets
    if missing:
        errors.append(f"taxonomy hole: {sorted(missing)}")

    inj = set(data.get("_meta", {}).get("injection_ids") or [])
    if inj - set(ids):
        errors.append(f"missing injection ids: {sorted(inj - set(ids))}")

    g18 = next((it for it in items if it.get("id") == "g18"), None)
    if g18:
        raw = RawItem(
            tweet_id="g18",
            url="https://x.com/eval/status/18",
            author_handle="eval",
            author_metadata={},
            posted_at_iso=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            text=g18["raw_text"],
            source_type="x_vocab",
            source_query="golden",
        )
        wrapped = _build_user_message([raw])
        if "</tweet>" in g18["raw_text"] and "&lt;/tweet&gt;" not in wrapped:
            errors.append("g18: </tweet> not escaped in classifier input")
        if "<system>" in wrapped and g18["raw_text"].count("<system>") == wrapped.count("<system>"):
            # structural payload still present as raw XML — escape only closes tweet;
            # that is expected; just ensure the closer is escaped
            pass

    return errors


def _to_raw(it: dict) -> "RawItem":
    from services.ct_digest.collectors import RawItem

    return RawItem(
        tweet_id=it["id"],
        url=f"https://x.com/eval/status/{it['id']}",
        author_handle="eval",
        author_metadata={"followers_count": 100},
        posted_at_iso=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        text=it["raw_text"],
        source_type="x_vocab",
        source_query="golden_v1",
    )


async def run_llm(data: dict) -> int:
    from services.ct_digest.classifier import classify_batch

    gold = {it["id"]: it for it in data["items"]}
    pred = await classify_batch([_to_raw(it) for it in data["items"]])
    by_id = {p.tweet_id: p for p in pred}

    bucket_ok = included_ok = both_ok = 0
    inj_fail: list[str] = []
    rows = []
    for gid, g in gold.items():
        p = by_id.get(gid)
        if p is None:
            rows.append((gid, g["gold_bucket"], "MISSING", g["gold_included"], None, False))
            if gid in (data.get("_meta") or {}).get("injection_ids", []):
                inj_fail.append(gid)
            continue
        b = p.bucket == g["gold_bucket"]
        i = p.included == g["gold_included"]
        bucket_ok += int(b)
        included_ok += int(i)
        both_ok += int(b and i)
        rows.append((gid, g["gold_bucket"], p.bucket, g["gold_included"], p.included, b and i))
        if gid in (data.get("_meta") or {}).get("injection_ids", []) and not (b and i):
            inj_fail.append(gid)

    n = len(gold)
    print(f"bucket    {bucket_ok}/{n}")
    print(f"included  {included_ok}/{n}")
    print(f"both      {both_ok}/{n}")
    for gid, gb, pb, gi, pi, ok in rows:
        mark = "ok" if ok else "FAIL"
        print(f"  {gid:4} {mark:4} gold={gb}/{gi} pred={pb}/{pi}")
    if inj_fail:
        print(f"injection failures (do not drop these cases): {inj_fail}")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CT digest golden-set gate")
    parser.add_argument("--llm", action="store_true", help="call classify_batch (needs ANTHROPIC_API_KEY)")
    args = parser.parse_args(argv)

    data = load_golden()
    errors = offline_checks(data)
    n = len(data["items"])
    counts = Counter(it["gold_bucket"] for it in data["items"])
    print(f"golden_v1  n={n}  buckets={dict(counts)}")
    if errors:
        for e in errors:
            print(f"FAIL  {e}")
        return 1
    print("offline  pass  (schema + taxonomy + g18 escape)")
    if not args.llm:
        return 0

    import asyncio
    return asyncio.run(run_llm(data))


if __name__ == "__main__":
    raise SystemExit(main())
