"""Dest-only: example routing, not live chat_ids."""

from shared.source_routing import classify


def test_example_caller_a_route():
    hit = classify(1000000004, None)
    assert hit["ingest"] is True
    assert hit["named_caller"] == "CALLER_A"


def test_unknown_chat_id_is_not_ingested():
    miss = classify(2000000001, 133)
    assert miss["ingest"] is False
    assert miss["named_caller"] is None
