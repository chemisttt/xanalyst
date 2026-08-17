# xanalyst (public snapshot)

Personal crypto signal pipeline. IDs in `shared/source_routing.py` are examples.

## Stack

Python 3.10 asyncio, PostgreSQL, Redis Streams, Telethon, discord.py-self, Twikit.

## Layout

- `services/telegram_monitor.py` — main Telegram ingest
- `services/private_mirror_monitor.py` — second inbox, inbound-only
- `services/private_mirror_dedup.py` — Variant C merge + named-caller fast path
- `services/discord_monitor.py` — X/Twitter links from env channel IDs
- `services/twitter_worker.py` — profile scoring
- `services/theme_burst_worker.py` — discussion anomaly alerts
- `services/ct_alpha_digest.py` — evening CT digest
- `shared/source_routing.py` — whitelist + routes (example IDs)
- `shared/notifier.py` — Bot API send/edit

## Env (dest)

Optional mirror topic IDs:

- `TELEGRAM_MIRROR_CALLER_A_TOPIC_ID`
- `TELEGRAM_MIRROR_CALLER_B_TOPIC_ID`
- `TELEGRAM_MIRROR_COMMUNITY_TOPIC_ID`

If unset, that route does not publish.

Deploy unit files use `User=deploy` and `WorkingDirectory=/opt/xanalyst`. They are examples.

Do not commit `.env`, session files, or cookies.
