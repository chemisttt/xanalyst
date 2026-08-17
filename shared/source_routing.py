"""
Routing config.

Three tables:
  1. INGEST_WHITELIST     — which chats to ingest (chat_id → label)
  2. NAMED_CALLER_ROUTES  — (chat_id, topic_id) → named dest topic
  3. CATEGORY_ROUTES      — (chat_id, topic_id) → merged-feed category

Priority: NAMED_CALLER_ROUTES → CATEGORY_ROUTES → ingest-only.

IDs and labels below are examples, not real sources.
"""

CATEGORY_PARAMS: dict[str, dict] = {'ARB': {'threshold': 3, 'window_sec': 90}, 'PUMP_DUMP': {'threshold': 4, 'window_sec': 300}, 'MEME': {'threshold': 3, 'window_sec': 900}, 'WHALE': {'threshold': 2, 'window_sec': 1800}}

# example chat_ids / labels — not real sources
INGEST_WHITELIST: dict[int, str] = {
    1000000001: "Alpha Forum",
    1000000002: "Signals Hub",
    1000000003: "Research Chat",
    1000000004: "Caller A",
    1000000005: "Caller B",
    1000000006: "Community Board",
    1000000007: "Flow Desk",
    1000000008: "Watch Desk",
}

NAMED_CALLER_ROUTES: dict[tuple[int, int | None], str] = {
    (1000000004, None): "CALLER_A",
    (1000000005, 101): "CALLER_B",
    (1000000006, None): "COMMUNITY",
    (1000000003, 201): "OTHERS",
}

CATEGORY_ROUTES: dict[tuple[int, int | None], str] = {
    (1000000002, 11): "ARB",
    (1000000002, 12): "ARB",
    (1000000007, 21): "PUMP_DUMP",
    (1000000007, 22): "PUMP_DUMP",
    (1000000008, 31): "MEME",
    (1000000001, 41): "WHALE",
}


def classify(chat_id: int, topic_id: int | None) -> dict:
    """Routing decision for (chat_id, topic_id)."""
    if chat_id not in INGEST_WHITELIST:
        return {"ingest": False, "named_caller": None, "category": None, "source_label": None}
    source_label = INGEST_WHITELIST[chat_id]
    key = (chat_id, topic_id)
    key_any_topic = (chat_id, None)
    named = NAMED_CALLER_ROUTES.get(key) or NAMED_CALLER_ROUTES.get(key_any_topic)
    category = CATEGORY_ROUTES.get(key) or CATEGORY_ROUTES.get(key_any_topic)
    return {
        "ingest": True,
        "named_caller": named,
        "category": category,
        "source_label": source_label,
    }


CHAT_TOPIC_WHITELIST: list[tuple[int, int | None, str]] = [
    (1000000001, 501, "Alpha Forum / Chat"),
    (1000000002, 502, "Signals Hub / Chat"),
    (1000000003, 503, "Research Chat / Lounge"),
    (1000000007, 504, "Flow Desk / Chat"),
]

CHAT_TOPIC_CHAT_IDS: frozenset[int] = frozenset(
    chat_id for (chat_id, _, _) in CHAT_TOPIC_WHITELIST
)


def is_chat_topic(chat_id: int, topic_id: int | None) -> bool:
    for (ci, ti, _) in CHAT_TOPIC_WHITELIST:
        if ci == chat_id and (ti == topic_id or ti is None):
            return True
    return False


def get_bucket_label(chat_id: int, topic_id: int | None) -> str | None:
    for (ci, ti, label) in CHAT_TOPIC_WHITELIST:
        if ci == chat_id and ti == topic_id:
            return label
    for (ci, ti, label) in CHAT_TOPIC_WHITELIST:
        if ci == chat_id and ti is None:
            return label
    return None


def iter_chat_topic_buckets():
    for (chat_id, topic_id, label) in CHAT_TOPIC_WHITELIST:
        yield chat_id, topic_id, label
