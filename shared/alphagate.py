"""
Alphagate API — история прошлых юзернеймов Twitter-аккаунтов.

Использует reverse-engineered API расширения Alphagate.
Куки сессионные — протухают, нужно обновлять вручную.
При ошибке (403, таймаут и т.д.) возвращает None — graceful fallback.
"""

import asyncio
import logging

import requests

from shared.config import settings

log = logging.getLogger("alphagate")

# Эндпоинт Alphagate API
API_URL = "https://api.alphagate.io/api/v1/ext/child"

# ID расширения (из заголовков браузера)
EXTENSION_ID = "niokainjnclnhaflagpmeobbnklokill"


async def get_username_history(twitter_id: int, username: str) -> list[str] | None:
    """
    Получить список прошлых юзернеймов через Alphagate API.

    Возвращает:
    - list[str] — список прошлых юзернеймов (без текущего), например ["whitewhalenfts", "bob_sponge72200"]
    - None — если куки не заданы или API недоступен (graceful fallback)
    """
    if not settings.alphagate_cookies:
        return None

    # Запрос блокирующий (requests), оборачиваем в to_thread
    return await asyncio.to_thread(_fetch, twitter_id, username)


def _fetch(twitter_id: int, username: str) -> list[str] | None:
    """Синхронный запрос к Alphagate API."""
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36"
        ),
        "X-Origin-ID": EXTENSION_ID,
        "Cookie": settings.alphagate_cookies,
    }

    params = {
        "twitterId": str(twitter_id),
        "username": username.lower(),
        "strict": "true",
    }

    try:
        resp = requests.get(API_URL, headers=headers, params=params, timeout=10)

        if resp.status_code == 403:
            log.warning("Alphagate: 403 — куки протухли, обнови ALPHAGATE_COOKIES в .env")
            return None

        if resp.status_code != 200:
            log.warning(f"Alphagate: HTTP {resp.status_code} для @{username}")
            return None

        data = resp.json()
        child = data.get("data", {}).get("child", {})
        prev = child.get("prev_usernames", [])

        if prev:
            log.info(f"Alphagate: @{username} — прошлые имена: {prev}")
        else:
            log.info(f"Alphagate: @{username} — прошлых имён нет")

        return prev if isinstance(prev, list) else []

    except requests.exceptions.Timeout:
        log.warning(f"Alphagate: таймаут для @{username}")
        return None
    except Exception as e:
        log.warning(f"Alphagate: ошибка для @{username}: {e}")
        return None
