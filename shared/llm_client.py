"""LLM client с async-обёрткой + Gemini-first chain для theme_burst.

Две функции для двух use cases:

1. `call_llm_async`: общая async-обёртка над sync `call_llm` из digest.
   Preserves digest's Perplexity-first chain — backwards-compat для digest reuse.

2. `call_llm_async_gemini_first`: theme_burst-specific override.
   Gemini-first → Perplexity → Groq. Cost model: Gemini Flash free tier (1500 RPD)
   для ~30 calls/day expected. Paid Perplexity only on Gemini outage.

JSON-mode support: `response_format='json'` parses response as JSON, raises
`LLMResponseInvalidError` on parse fail.

Error handling:
  - 429 → 1 retry with 5s backoff → fall through chain.
  - 5xx → next provider.
  - All providers fail → `AllProvidersFailedError`.
"""

import asyncio
import json
import logging
import re

from openai import OpenAI

from shared.config import settings


log = logging.getLogger("llm_client")


class AllProvidersFailedError(Exception):
    """Raised when all LLM providers fail (network, rate limit, auth)."""


class LLMResponseInvalidError(Exception):
    """Raised when LLM response cannot be parsed as expected format."""


# Theme-burst Gemini-first chain (different ordering from digest's Perplexity-first).
THEME_BURST_PROVIDERS = [
    ("gemini",     "gemini_api_key",     "https://generativelanguage.googleapis.com/v1beta/openai/", "gemini-2.0-flash"),
    ("perplexity", "perplexity_api_key", "https://api.perplexity.ai", "sonar"),
    ("groq",       "groq_api_key",       "https://api.groq.com/openai/v1", "llama-3.3-70b-versatile"),
]


def _strip_json_fence(text: str) -> str:
    """Strip ```json ... ``` fences sometimes returned by models."""
    text = text.strip()
    m = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.DOTALL)
    return m.group(1) if m else text


# Providers that reject OpenAI-style response_format json_object (400).
# Perplexity sonar 2026: only "json_schema" | "text" — json_object fails hard.
_JSON_OBJECT_UNSUPPORTED = frozenset({"perplexity"})


def _try_provider_sync(
    api_key: str, base_url: str, model: str,
    system_prompt: str, user_content: str,
    timeout: float, response_format: str,
    *,
    provider_name: str = "",
) -> str:
    """Single provider call — sync. Wrapped in asyncio.to_thread by async caller."""
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
    kwargs = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
    }
    if response_format == "json" and provider_name not in _JSON_OBJECT_UNSUPPORTED:
        # Gemini-via-OpenAI / Groq accept json_object; Perplexity does not (400).
        # For unsupported: rely on system prompt "return JSON" + _strip_json_fence parse.
        kwargs["response_format"] = {"type": "json_object"}
    resp = client.chat.completions.create(**kwargs)
    return resp.choices[0].message.content or ""


async def call_llm_async_gemini_first(
    system_prompt: str,
    user_content: str,
    *,
    response_format: str = "json",
    timeout: float = 15.0,
) -> tuple[dict | str, str]:
    """Theme-burst LLM judge call.

    Returns:
        (parsed_response, provider_name). parsed_response is dict if response_format='json'
        else raw string.

    Raises:
        AllProvidersFailedError: all providers failed (timeout, network, rate limit).
        LLMResponseInvalidError: response_format='json' but parse failed.
    """
    errors = []
    for provider_name, key_attr, base_url, model in THEME_BURST_PROVIDERS:
        api_key = getattr(settings, key_attr, None)
        if not api_key:
            errors.append(f"{provider_name}: no API key")
            continue

        for attempt in range(2):  # 1 retry on 429
            try:
                raw = await asyncio.wait_for(
                    asyncio.to_thread(
                        _try_provider_sync,
                        api_key, base_url, model,
                        system_prompt, user_content,
                        timeout, response_format,
                        provider_name=provider_name,
                    ),
                    timeout=timeout + 2,  # async wrapper margin
                )
                log.info(f"LLM {provider_name}: OK ({len(raw)} chars, attempt {attempt+1})")

                if response_format == "json":
                    raw = _strip_json_fence(raw)
                    try:
                        return json.loads(raw), provider_name
                    except json.JSONDecodeError as e:
                        # Don't fall through — JSON parse fail is a content issue,
                        # not provider issue. Raise specific exception.
                        raise LLMResponseInvalidError(
                            f"{provider_name} returned invalid JSON: {raw[:200]}..."
                        ) from e
                else:
                    return raw, provider_name

            except (asyncio.TimeoutError, Exception) as e:
                err_str = f"{provider_name}: {type(e).__name__}: {str(e)[:200]}"
                # 429: retry once with backoff
                if "429" in str(e) and attempt == 0:
                    log.warning(f"{err_str} (will retry once)")
                    await asyncio.sleep(5)
                    continue
                # LLMResponseInvalidError propagates — not a provider failure
                if isinstance(e, LLMResponseInvalidError):
                    raise
                log.warning(err_str)
                errors.append(err_str)
                break  # next provider

    raise AllProvidersFailedError("All LLM providers failed: " + " | ".join(errors))


async def call_llm_async(
    system_prompt: str,
    user_content: str,
    *,
    response_format: str = "text",
    timeout: float = 30.0,
) -> tuple[str, str | None]:
    """Generic async wrapper над sync `call_llm` из digest (Perplexity-first).

    Preserves digest's existing chain. NOT used by theme_burst (which uses
    `call_llm_async_gemini_first`). Provided for future refactors.
    """
    from services.private_mirror_digest import call_llm as _sync_call_llm

    return await asyncio.wait_for(
        asyncio.to_thread(_sync_call_llm, system_prompt, user_content),
        timeout=timeout + 2,
    )


# ============================================================
# Spec 004 — Anthropic native client (Claude Sonnet 4.x) with prompt cache
# ============================================================
#
# Sonnet 4.6 pricing (verified 2026-05-19 — DECAY-CHECK TARGET, see CLAUDE.md spec 004):
#   input:                 $3.00 / 1M tokens
#   cache_read_input:      $0.30 / 1M tokens  (10x discount on cache hits)
#   cache_creation_input:  $3.75 / 1M tokens  (1.25x premium on cache write)
#   output:                $15.00 / 1M tokens
# If Anthropic adjusts pricing, update these constants AND CLAUDE.md decay-check note.

_SONNET_PRICING = {
    "input":             3.00 / 1_000_000,
    "cache_read":        0.30 / 1_000_000,
    "cache_creation":    3.75 / 1_000_000,
    "output":           15.00 / 1_000_000,
}


def _end_of_month_unix() -> int:
    """Unix timestamp UTC for last second of current month — Redis cost key TTL."""
    import calendar
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    last_day = calendar.monthrange(now.year, now.month)[1]
    eom = now.replace(day=last_day, hour=23, minute=59, second=59, microsecond=0)
    return int(eom.timestamp())


class AnthropicClient:
    """
    Async wrapper для anthropic SDK с prompt caching + cost tracking.

    Cost тread:
      - per call cost calculated from response.usage с getattr() guards
        (cache_read_input_tokens / cache_creation_input_tokens могут отсутствовать
        в старых SDK versions ИЛИ при cold-cache call)
      - if cost_redis_key provided → INCRBYFLOAT в Redis + EXPIREAT end-of-month UTC
      - Redis client получается через `shared.redis_client.get_redis()` (caller must init)

    Use:
        client = AnthropicClient(api_key=settings.anthropic_api_key)
        text, meta = await client.call(
            system="You are a classifier...",
            messages=[{"role": "user", "content": "<items>...</items>"}],
            cost_redis_key="llm_cost:ct_digest:2026-05",
        )
        print(meta["cost_cents"], meta["usage"])
    """

    def __init__(
        self,
        api_key: str,
        model: str = "claude-sonnet-4-6",
        max_tokens: int = 4096,
        base_url: str | None = None,
    ):
        if not api_key:
            raise ValueError("anthropic api_key required (ANTHROPIC_API_KEY env)")
        # Late import — anthropic SDK не used elsewhere, не блокируем module load
        # если SDK не установлен в окружениях которые не используют CT digest.
        import os
        from anthropic import AsyncAnthropic
        # Explicit base_url: vibecode proxy (ANTHROPIC_BASE_URL). SDK also reads env,
        # but pass through so partial env/import order can't silently hit api.anthropic.com.
        resolved_base = base_url or os.environ.get("ANTHROPIC_BASE_URL") or None
        kwargs = {"api_key": api_key}
        if resolved_base:
            kwargs["base_url"] = resolved_base.rstrip("/")
        self._client = AsyncAnthropic(**kwargs)
        self._model = model
        self._max_tokens = max_tokens
        self._base_url = resolved_base

    async def call(
        self,
        system: str,
        messages: list[dict],
        max_tokens: int | None = None,
        cost_redis_key: str | None = None,
    ) -> tuple[str, dict]:
        """
        Один Sonnet call с prompt cache enabled на system prompt.

        **STREAMING mode required** (load-bearing per api.example.com proxy policy
        2026-05-19): non-streaming requests can hang past 100s timeout; 5+ such hangs/hour
        produce 429. We use messages.stream() context manager which collects chunks
        and returns final_message с full usage attrs.

        Returns (text, metadata) where metadata = {cost_cents, usage}.
        """
        async with self._client.messages.stream(
            model=self._model,
            max_tokens=max_tokens or self._max_tokens,
            temperature=0.0,
            system=[{
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=messages,
        ) as stream:
            # Consume the stream; SDK accumulates content + usage internally.
            await stream.until_done()
            resp = await stream.get_final_message()
        usage = resp.usage
        # getattr guards — старые SDK или cold-cache могут не иметь этих полей
        input_tokens = getattr(usage, "input_tokens", 0)
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        cache_create = getattr(usage, "cache_creation_input_tokens", 0) or 0
        output_tokens = getattr(usage, "output_tokens", 0)

        cost_cents = round(
            (input_tokens * _SONNET_PRICING["input"]
             + cache_read * _SONNET_PRICING["cache_read"]
             + cache_create * _SONNET_PRICING["cache_creation"]
             + output_tokens * _SONNET_PRICING["output"]) * 100,
            4,
        )

        if cost_redis_key:
            try:
                from shared.redis_client import get_redis
                r = get_redis()
                await r.incrbyfloat(cost_redis_key, cost_cents)
                await r.expireat(cost_redis_key, _end_of_month_unix())
            except Exception as e:
                # Cost tracking optional — don't fail the LLM call if Redis hiccups
                log.warning("cost tracking failed for %s: %s", cost_redis_key, e)

        text = resp.content[0].text if resp.content else ""
        return text, {
            "cost_cents": cost_cents,
            "usage": {
                "input_tokens": input_tokens,
                "cache_read_input_tokens": cache_read,
                "cache_creation_input_tokens": cache_create,
                "output_tokens": output_tokens,
            },
        }
