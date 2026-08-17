"""Unit tests для shared/author_parser.py.

Coverage:
- Stdandard aggregator format extraction
- Lowercase normalization
- Edge cases: None, empty, no prefix
- Anti-false-positive: email, inline mentions, quoted text
- SQL/Python equivalence (with pg_pool fixture)
"""

import pytest
from shared.author_parser import extract_real_author, extract_real_author_sql


def test_extracts_from_standard_prefix():
    text = "[CALLER_B](tg://user?id=1100000001) (@example_handle)\n800к на бинанс завели"
    assert extract_real_author(text) == "example_handle"


def test_lowercase_normalized():
    text = "Some Display (@example_handle)\nbody"
    assert extract_real_author(text) == "example_handle"


def test_missing_returns_none():
    assert extract_real_author("plain text no parens") is None


def test_empty_returns_none():
    assert extract_real_author("") is None
    assert extract_real_author(None) is None


def test_does_not_match_email():
    assert extract_real_author("user@gmail.com Hello") is None


def test_does_not_match_inline_at_mention():
    """Inline @-mention without parens should NOT be extracted as real author."""
    assert extract_real_author("привет @alice как дела") is None


def test_anchored_to_first_line():
    """Regex searches ONLY first line — quoted (@user) в body should not override header."""
    text = "[Header](tg://...) (@bob)\nон сказал (@alice) лох"
    assert extract_real_author(text) == "bob"


def test_no_header_with_inline_in_body():
    """If first line has no prefix, inline (@user) in body should NOT match."""
    text = "plain header\nsomething (@alice) inline"
    assert extract_real_author(text) is None


def test_underscores_in_username():
    text = "[Display](tg://...) (@user_name_123)\nbody"
    assert extract_real_author(text) == "user_name_123"


def test_numbers_in_username():
    text = "[Display](tg://...) (@trader42)\nbody"
    assert extract_real_author(text) == "trader42"


@pytest.mark.asyncio
async def test_sql_expr_matches_python(pg_pool):
    """Postgres POSIX regex и Python re должны давать identical results на realistic samples."""
    test_cases = [
        ("[CALLER_B](tg://...) (@example_handle)\n800k", "example_handle"),
        ("[NAME](tg://...) (@USER_42)\nbody", "user_42"),
        ("plain text", None),
        ("", None),
        ("привет @alice", None),
        ("[H](tg://...) (@first)\nquoted (@second)", "first"),
    ]
    async with pg_pool.acquire() as conn:
        for text, expected in test_cases:
            sql = f"SELECT {extract_real_author_sql()} AS author"
            row = await conn.fetchrow(sql.replace('text', f"'{text}'::text"))
            sql_result = row['author']
            py_result = extract_real_author(text)
            assert py_result == sql_result == expected, \
                f"Divergence on {text!r}: py={py_result!r}, sql={sql_result!r}, expected={expected!r}"
