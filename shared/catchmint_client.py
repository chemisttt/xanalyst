"""
Async HTTP client для api.catchmint.xyz (spec 003).

Тонкая обёртка над aiohttp. Делает 4 read-only вызова к публичным endpoint'ам,
никаких авторизаций — read-only feed.

M5 (revised after smoke-test): EVM-адреса НЕ нормализуются в lowercase. CatchMint detail
endpoint `/contracts/{address}/` case-sensitive — на lowercase возвращает 404 (тестировано
17 мая: `0x894e3f1cA45F9404...` → 200, lowercase → 404). API возвращает checksum-форму
консистентно во всех endpoint'ах, dedup тоже работает на checksum. PG/Redis keys = checksum
из API. Если catchmint когда-то начнёт возвращать разный case — заметим по дублям.
M6: HTTP 429 поднимается типизированным `RateLimited` исключением с `retry_after`.
"""

from __future__ import annotations

import logging
from typing import Optional

import aiohttp

log = logging.getLogger("catchmint_client")


class RateLimited(Exception):
    """HTTP 429 от catchmint. `retry_after` — секунды (из Retry-After header или 60 default)."""

    def __init__(self, retry_after: int = 60):
        self.retry_after = retry_after
        super().__init__(f"rate limited, retry after {retry_after}s")


class CatchmintClient:
    """
    Async context-managed клиент. Использование:

        async with CatchmintClient(base, ua) as client:
            rows = await client.get_overview()
    """

    def __init__(self, base_url: str, user_agent: str, timeout: int = 10):
        self.base = base_url.rstrip("/")
        self._headers = {
            "User-Agent": user_agent,
            "Origin": "https://catchmint.xyz",
            "Referer": "https://catchmint.xyz/",
            "Accept": "application/json",
        }
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(headers=self._headers, timeout=self._timeout)
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()
            self._session = None

    async def _get_json(self, path: str):
        assert self._session is not None, "use as `async with CatchmintClient(...)`"
        async with self._session.get(f"{self.base}/{path}") as r:
            if r.status == 429:
                try:
                    ra = int(r.headers.get("Retry-After", "60"))
                except (ValueError, TypeError):
                    ra = 60
                raise RateLimited(retry_after=ra)
            r.raise_for_status()
            return await r.json()

    async def get_overview(self, window_sec: int = 600) -> list[dict]:
        """
        GET /timeseries/mints/overview/?window=<sec> — коллекции с activity за указанный window.

        catchmint всегда возвращает 6 равных bucket'ов (bucket_size = window/6).
        totalCounts = sum(counts) = mints за весь window. Это точная метрика для display.

        Default window=600s (10мин). Меньшее window → меньше rows (катча возвращает только
        active коллекции для этого окна; например window=600 даёт ~29 rows vs 50 для window=86400).
        """
        path = f"timeseries/mints/overview/?window={int(window_sec)}"
        return await self._get_json(path)

    async def get_live(self) -> list[dict]:
        """GET /timeseries/mints/live/ — 30 свежих tx за ~60 сек. Addr в checksum-форме."""
        return await self._get_json("timeseries/mints/live/")

    async def get_contract_detail(self, address: str) -> dict:
        """GET /contracts/{address}/ — детали. address ДОЛЖЕН быть в checksum-форме (lowercase → 404)."""
        return await self._get_json(f"contracts/{address}/")

    async def get_contract_flags(self, address: str) -> list[dict]:
        """GET /contracts/{address}/flags/ — [{"label":"Scam","count":2}, ...]. address checksum."""
        return await self._get_json(f"contracts/{address}/flags/")

    async def get_contract_holders(self, address: str) -> list[dict]:
        """GET /contracts/{address}/holders/ — [{"address","quantity","ensName"}, ...].
        Capped на 100 top-holders by quantity. Pagination params (page, limit) игнорируются API."""
        return await self._get_json(f"contracts/{address}/holders/")
