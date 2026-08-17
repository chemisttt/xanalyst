"""Golden-set contract for CT digest. No API."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from evals.ct_digest.run import GOLDEN_PATH, load_golden, offline_checks, main as golden_main
from services.ct_digest.classifier import VALID_BUCKETS


def test_golden_file_is_the_prod_artifact():
    data = load_golden()
    meta = data["_meta"]
    assert meta["name"] == "ct_digest_golden_v1"
    assert "lesson" not in json.dumps(meta).lower()
    assert "student" not in json.dumps(meta).lower()
    assert "instructor" not in json.dumps(meta).lower()
    assert set(meta["injection_ids"]) == {"g16", "g18"}


def test_offline_gate_passes():
    assert offline_checks(load_golden()) == []
    assert golden_main([]) == 0


def test_covers_production_buckets():
    buckets = {it["gold_bucket"] for it in load_golden()["items"]}
    assert buckets <= VALID_BUCKETS
    assert "paid_hype" in buckets
    assert "early_signals" in buckets
    assert "state_reconcile" in buckets


def test_paid_hype_is_never_dropped():
    for it in load_golden()["items"]:
        if it["gold_bucket"] == "paid_hype":
            assert it["gold_included"] is True


def test_golden_path_location():
    assert GOLDEN_PATH == PROJECT_ROOT / "evals" / "ct_digest" / "golden_v1.json"
    assert GOLDEN_PATH.is_file()
