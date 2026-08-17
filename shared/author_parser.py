"""Real-author extraction из aggregator-сообщений.

aggregator format: первая строка = `[Display Name](tg://user?id=NNN) (@username)`.
Тело — следующие строки.

Extract username via regex `\\(@(\\w+)\\)`, anchored to FIRST LINE to avoid matching
inline `(@user)` mentions inside quoted body text.

Both Python и SQL helpers provided для consistent extraction в aggregations.
"""

import re

_AUTHOR_RE = re.compile(r'\(@(\w+)\)')


def extract_real_author(text: str | None) -> str | None:
    """Извлечь real username из aggregator prefix.

    Args:
        text: full message text. Could be None или empty.

    Returns:
        Lowercase username (without `@`) или None если prefix не найден.

    Examples:
        >>> extract_real_author('[CALLER_B](tg://user?id=1100000001) (@example_handle)\\n800k')
        'example_handle'
        >>> extract_real_author('(@USER_Name)\\nbody')
        'user_name'
        >>> extract_real_author('plain text')
        >>> extract_real_author('hello @alice')  # inline @-mention != real author
        >>> extract_real_author(None)
    """
    if not text:
        return None
    first_line = text.split('\n', 1)[0]
    m = _AUTHOR_RE.search(first_line)
    return m.group(1).lower() if m else None


def extract_real_author_sql() -> str:
    """SQL expression returning lowercase real author username.

    Postgres POSIX regex matches `\\w` as `[A-Za-z0-9_]` (same as Python re).
    Anchored to first line via `split_part(text, E'\\n', 1)`.

    Returns:
        SQL expression string (no params), to be embedded в aggregate queries.

    Usage:
        sql = f\"\"\"
            SELECT COUNT(DISTINCT {extract_real_author_sql()})
            FROM channel_messages
            WHERE source_account='private_mirror'
        \"\"\"
    """
    return r"LOWER(substring(split_part(text, E'\n', 1) from '\(@(\w+)\)'))"
