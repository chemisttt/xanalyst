# xanalyst

Personal production pipeline I built and ran: ingest messages from chats the client is added to, extract public X/Twitter links from Discord, score profiles, merge independent sources, fire discussion-burst alerts, and emit an evening CT digest.

> Channel IDs and labels in `shared/source_routing.py` and empty IDs in `.env.example` are **examples**, not the sources this system ran on.

## Stack

- Python 3.10+, asyncio
- PostgreSQL 16, Redis 7 Streams
- Telethon (Telegram ingest), python-telegram-bot (outbound)
- discord.py-self (Discord ingest of public X links)
- Twikit (X/Twitter profile fetch)

## What it does

1. **Ingest** — Telegram client reads a whitelist of chats (`INGEST_WHITELIST`). Discord client watches channel IDs from env and extracts public `x.com` / `twitter.com` links.
2. **Route** — `shared/source_routing.py` maps `(chat_id, topic_id)` to a named caller topic or a merged category (`ARB`, `PUMP_DUMP`, `MEME`, `WHALE`).
3. **Dedup (Variant C)** — first mention stays in Redis; at threshold `n` independent sources the bot posts; later confirms silent-edit the same message; window close locks it.
4. **Theme Burst** — every 5 minutes, z-score / rate signals on discussion topics vs a 7-day baseline. LLM judge, then an alert that quotes the source messages.
5. **CT Alpha Digest** — evening classification of crypto-twitter into buckets, with a hard cap and project-level dedup.
6. **Twitter scoring** — profile metrics and a silent save path so digest-discovered handles do not spam the alert topic.

## Layout

```
services/     workers (ingest, dedup, digest, theme burst, twitter, health)
shared/       config, db, redis, routing, notifier, llm, metrics
evals/        held-out CT digest golden set (human labels)
scripts/      SQL migrations
deploy/       systemd units + crontab snippet (example paths under /opt/xanalyst)
tests/        pytest
```

## Run locally

```bash
cp .env.example .env
# fill tokens; leave channel IDs as examples or your own
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
pytest tests/test_author_parser.py tests/test_example_routing.py -q
```

Postgres + Redis are required for the integration tests (`tests/conftest.py`).

## CT digest eval

Human-labeled held-out set in `evals/ct_digest/golden_v1.json` (n=18). Pass = bucket **and** included match. Injection cases stay in the set.

```bash
python evals/ct_digest/run.py          # schema + taxonomy + escape
python evals/ct_digest/run.py --llm    # classify_batch (ANTHROPIC_API_KEY)
```

## Config

See `.env.example`. Topic IDs `TELEGRAM_MIRROR_CALLER_{A,B,COMMUNITY}_TOPIC_ID` are optional — publish is skipped when unset.

This snapshot is a frozen export. It is not the live private tree and is not kept in sync.
