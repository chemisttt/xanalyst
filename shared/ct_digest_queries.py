"""
Spec 004: CT Alpha Digest V1 — query config (broad alpha + NFT).

Mirror of `shared/source_routing.py` pattern: Python lists, not DB tables.
Validated against live X в 2 rounds (см. spec Context) — NFT slice.
Token-launch trio (bonding curve / v4 hook / going live) added 2026-05-19 после
user verification: каждый query вернул primitive-level находки в 24h окне на live X.

Phase 2: extract token-launch queries into separate TOKEN_LAUNCH_VOCAB if a
dedicated profile is needed; for V1 they live alongside NFT queries — classifier
buckets them by content semantics regardless of source query.
"""

from __future__ import annotations

# Vocabulary collector queries — semantic search via x_search.
NFT_VOCAB_QUERIES: list[str] = [
    # NFT mint phase patterns
    '"public mint" (today OR tomorrow OR live OR LIVE)',
    '"WL spot" OR "GTD" OR "FCFS"',
    '"holders" (snapshot OR claim OR airdrop) (today OR tomorrow)',
    '"DEGEN MINT"',
    '"DYOR" (mint OR nft OR drop)',
    '"public in" (mins OR mint)',
    '"sweep" (floor OR collection) -LitVM -SweepHaus -LiteForge',
    # Token-launch primitives (added 2026-05-19, user-verified producing hits)
    '"bonding curve" (launch OR mint OR live)',
    '"v4 hook" (mint OR live)',
    '"going live" (mint OR launch)',
]

# Account-feed collector — concentrated alpha curators.
# Single query: from:wh7nft OR from:0xvaidhik OR ... (joined в AccountFeedCollector).
NFT_CURATORS: list[str] = [
    "wh7nft",
    "0xvaidhik",
    "0xLawl",
    "cartyisme",
    "aspenshredder",
    "0xKnownxd",
    "0wnexpect",
    "punk9059",
    "mikesnft",
    "callersquad",
]

# Project-cluster collector — known active NFT projects + mint/drop/live/launch context.
# Joined в ProjectClusterCollector as (@a OR @b OR ...) (mint OR drop OR live OR launch).
NFT_PROJECT_CLUSTERS: list[str] = [
    "abnormalmfers",
    "depunksClub",
    "Soulseth_nft",
    "CC0_Studios",
    "bobosoneth",
    "BackPunks",
    "ZZZ_EN",
    "normiesART",
    "DedGorgez",
    "GoblynzNFT",
    "SpaceRidersXYZ",
]


def build_account_query(handles: list[str]) -> str:
    """from:a OR from:b OR ... — single x_search query для account feed."""
    return " OR ".join(f"from:{h}" for h in handles)


def build_project_cluster_query(handles: list[str], context_words: tuple[str, ...] = (
    "mint", "drop", "live", "launch",
)) -> str:
    """(@a OR @b OR ...) (mint OR drop OR live OR launch)."""
    accounts = " OR ".join(f"@{h}" for h in handles)
    context = " OR ".join(context_words)
    return f"({accounts}) ({context})"
