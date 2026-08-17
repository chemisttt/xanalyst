"""
Конфигурация проекта.
Загружает переменные из .env файла и предоставляет их как объект Settings.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Загружаем .env из корня проекта
# Path(__file__) → shared/config.py, .parent.parent → корень проекта
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")


class Settings:
    """Все настройки проекта в одном месте."""

    def __init__(self):
        # --- Telegram Monitoring (Telethon userbot) ---
        self.telegram_api_id = self._get("TELEGRAM_API_ID", required=False)
        self.telegram_api_hash = self._get("TELEGRAM_API_HASH", required=False)
        self.telegram_phone = self._get("TELEGRAM_PHONE", required=False)
        self.telegram_channels = self._get_list("TELEGRAM_CHANNELS")

        # Исключённые топики форумов: "chat_id:topic_id,chat_id:topic_id"
        # Сообщения из этих топиков игнорируются
        self.telegram_exclude_topics = self._parse_exclude_topics(
            self._get("TELEGRAM_EXCLUDE_TOPICS", default="")
        )

        # --- Telegram Notifications (бот) ---
        self.telegram_bot_token = self._get("TELEGRAM_BOT_TOKEN", required=False)
        self.telegram_notify_chat_id = self._get("TELEGRAM_NOTIFY_CHAT_ID", required=False)
        self.telegram_notify_topic_id = self._get("TELEGRAM_NOTIFY_TOPIC_ID", required=False)
        self.telegram_early_topic_id = self._get("TELEGRAM_EARLY_TOPIC_ID", required=False)
        self.telegram_watchlist_topic_id = self._get("TELEGRAM_WATCHLIST_TOPIC_ID", required=False)
        self.telegram_admin_id = self._get("TELEGRAM_ADMIN_ID", required=False)

        # --- Private Mirror Monitor V1 (spec 001) ---
        # Второй TG-аккаунт (userbot для приваток)
        self.telegram_private_phone = self._get("TELEGRAM_PRIVATE_PHONE", required=False)
        self.telegram_private_session_name = self._get("TELEGRAM_PRIVATE_SESSION_NAME", default="xanalyst_session")

        # Forum topic IDs для private_mirror routing (создаются вручную в боте)
        # Если topic_id отсутствует — соответствующее routing пропускается с WARNING, сервис не падает
        self.telegram_mirror_caller_a_topic_id      = self._get("TELEGRAM_MIRROR_CALLER_A_TOPIC_ID",      required=False)
        self.telegram_mirror_caller_b_topic_id      = self._get("TELEGRAM_MIRROR_CALLER_B_TOPIC_ID",      required=False)
        self.telegram_mirror_others_topic_id    = self._get("TELEGRAM_MIRROR_OTHERS_TOPIC_ID",    required=False)
        self.telegram_mirror_community_topic_id  = self._get("TELEGRAM_MIRROR_COMMUNITY_TOPIC_ID",  required=False)
        self.telegram_mirror_arb_topic_id       = self._get("TELEGRAM_MIRROR_ARB_TOPIC_ID",       required=False)
        self.telegram_mirror_pump_topic_id      = self._get("TELEGRAM_MIRROR_PUMP_TOPIC_ID",      required=False)
        self.telegram_mirror_whale_topic_id     = self._get("TELEGRAM_MIRROR_WHALE_TOPIC_ID",     required=False)
        self.telegram_mirror_meme_topic_id      = self._get("TELEGRAM_MIRROR_MEME_TOPIC_ID",      required=False)
        self.telegram_mirror_digest_topic_id    = self._get("TELEGRAM_MIRROR_DIGEST_TOPIC_ID",    required=False)
        self.telegram_mirror_raw_topic_id       = self._get("TELEGRAM_MIRROR_RAW_TOPIC_ID",       required=False)
        self.telegram_mirror_syshealth_topic_id = self._get("TELEGRAM_MIRROR_SYSHEALTH_TOPIC_ID", required=False)
        # Caller Digest (human callers LLM summary). Optional — fallback to DIGEST topic.
        self.telegram_mirror_caller_digest_topic_id = self._get(
            "TELEGRAM_MIRROR_CALLER_DIGEST_TOPIC_ID", required=False
        )

        # --- Spec 002 V1.5 — Theme Burst Detector ---
        # Forum topic IDs (production + shadow). Создаются вручную в боте.
        self.telegram_mirror_theme_burst_topic_id        = self._get("TELEGRAM_MIRROR_THEME_BURST_TOPIC_ID",        required=False)
        self.telegram_mirror_theme_burst_shadow_topic_id = self._get("TELEGRAM_MIRROR_THEME_BURST_SHADOW_TOPIC_ID", required=False)

        # Shadow vs production routing.
        # Default TRUE для первого deploy. Flip to false после 7d shadow review + reconcile.
        self.theme_burst_dry_run = self._get_bool("THEME_BURST_DRY_RUN", default=True)

        # ====== theme_burst thresholds ======
        # PROVENANCE: first-choice, NOT swept against historical data.
        # Re-tune AFTER replay backtest (scripts/replay_theme_burst.py) AND 7d shadow review.
        # Heuristic transfer from onchain-radar 014 — domain-specific failure modes possible.
        # Rev 3: widened week-1 thresholds (composite_fire 4.0 → 3.0, z 2.5 → 2.0) reflecting
        # M17 actionable caveat — text-behavioral signal, не price-numerical как onchain-radar.

        # Activation thresholds
        # 2026-08 recalibration: discussion volume упал vs deploy-era; defaults
        # closer to live ForumB/ForumA peaks (~10–30 msgs/30m).
        self.theme_burst_s1_z_threshold        = self._get_float("THEME_BURST_S1_Z_THRESHOLD",        default=1.5)
        self.theme_burst_s2_ratio_threshold    = self._get_float("THEME_BURST_S2_RATIO_THRESHOLD",    default=2.0)
        self.theme_burst_s3_author_z_threshold = self._get_float("THEME_BURST_S3_AUTHOR_Z_THRESHOLD", default=1.5)
        self.theme_burst_s4_min_authors_hard   = self._get_int("THEME_BURST_S4_MIN_AUTHORS_HARD",     default=3)
        self.theme_burst_s4_min_mentions       = self._get_int("THEME_BURST_S4_MIN_MENTIONS",         default=3)
        self.theme_burst_s5_min_authors        = self._get_int("THEME_BURST_S5_MIN_AUTHORS",          default=2)
        self.theme_burst_s6_rare_window_days   = self._get_int("THEME_BURST_S6_RARE_WINDOW_DAYS",     default=30)
        self.theme_burst_s6_rare_max_msgs      = self._get_int("THEME_BURST_S6_RARE_MAX_MSGS",        default=3)
        self.theme_burst_composite_fire        = self._get_float("THEME_BURST_COMPOSITE_FIRE",        default=2.0)
        self.theme_burst_cooldown_sec          = self._get_int("THEME_BURST_COOLDOWN_SEC",            default=5400)

        # z-signal activation gate (per-bucket cold-start protection)
        # Was 200 msgs / 100 slots — ForumA live ~95/21 → z_active=False forever.
        self.theme_burst_min_msgs_7d_for_z     = self._get_int("THEME_BURST_MIN_MSGS_7D_FOR_Z",       default=60)
        self.theme_burst_min_slots_7d_for_z    = self._get_int("THEME_BURST_MIN_SLOTS_7D_FOR_Z",      default=12)

        # Composite score weights
        from types import SimpleNamespace
        self.theme_burst_weights = SimpleNamespace(
            w1=self._get_float("THEME_BURST_W1", default=1.5),
            w2=self._get_float("THEME_BURST_W2", default=1.2),
            w3=self._get_float("THEME_BURST_W3", default=1.0),
            w4=self._get_float("THEME_BURST_W4", default=1.5),
            w6=self._get_float("THEME_BURST_W6", default=1.0),
            w8=self._get_float("THEME_BURST_W8", default=0.6),
        )

        # Kill-switch (rev 2 C10 + rev 3 shadow/prod separation)
        self.theme_burst_kill_max_fires_per_hour  = self._get_int("THEME_BURST_KILL_MAX_FIRES_PER_HOUR",   default=10)
        self.theme_burst_kill_min_noise_pct_prod  = self._get_float("THEME_BURST_KILL_MIN_NOISE_PCT_PROD",  default=0.05)
        self.theme_burst_kill_min_noise_pct_shadow= self._get_float("THEME_BURST_KILL_MIN_NOISE_PCT_SHADOW",default=0.02)
        self.theme_burst_kill_max_noise_pct       = self._get_float("THEME_BURST_KILL_MAX_NOISE_PCT",       default=0.80)
        self.theme_burst_kill_max_raw_signal_pct  = self._get_float("THEME_BURST_KILL_MAX_RAW_SIGNAL_PCT",  default=0.30)

        # Anti-habituation (rev 3 NEW)
        self.theme_burst_per_day_budget          = self._get_int("THEME_BURST_PER_DAY_BUDGET",           default=15)
        self.theme_burst_noise_pause_threshold   = self._get_int("THEME_BURST_NOISE_PAUSE_THRESHOLD",    default=3)
        self.theme_burst_noise_pause_duration_sec= self._get_int("THEME_BURST_NOISE_PAUSE_DURATION_SEC", default=86400)

        # LLM
        self.theme_burst_llm_timeout_sec = self._get_int("THEME_BURST_LLM_TIMEOUT_SEC", default=15)
        self.theme_burst_llm_judge_max_msgs = self._get_int("THEME_BURST_LLM_JUDGE_MAX_MSGS", default=30)
        # Сколько исходных msgs показать в TG-алерте (контекст «о чём речь»)
        self.theme_burst_alert_quote_msgs = self._get_int("THEME_BURST_ALERT_QUOTE_MSGS", default=6)
        self.theme_burst_alert_quote_chars = self._get_int("THEME_BURST_ALERT_QUOTE_CHARS", default=160)

        # --- Spec 003 — CatchMint Mint Radar ---
        # Burst-детектор для NFT-минтов с api.catchmint.xyz.
        # Топик создаётся в TG-боте отдельно (CATCHMINT_TOPIC_ID).
        self.catchmint_enabled              = self._get_bool("CATCHMINT_ENABLED",              default=True)
        self.catchmint_api_base             = self._get("CATCHMINT_API_BASE",                  default="https://api.catchmint.xyz")
        self.catchmint_topic_id             = self._get_int("CATCHMINT_TOPIC_ID",              default=0)
        self.catchmint_overview_poll_sec    = self._get_int("CATCHMINT_OVERVIEW_POLL_SEC",     default=60)
        self.catchmint_live_enabled         = self._get_bool("CATCHMINT_LIVE_ENABLED",         default=False)
        self.catchmint_live_poll_sec        = self._get_int("CATCHMINT_LIVE_POLL_SEC",         default=30)
        # Activity window. catchmint endpoint /timeseries/mints/overview/?window=<sec>:
        # totalCounts = sum mints за это окно. Gate: totalCounts >= min_mints_in_window.
        # 600 = 10мин (как UI default). Перешли от часовых bucket'ов после Gate E v2.
        self.catchmint_window_sec           = self._get_int("CATCHMINT_WINDOW_SEC",            default=600)
        self.catchmint_min_mints_in_window  = self._get_int("CATCHMINT_MIN_MINTS_IN_WINDOW",   default=15)
        self.catchmint_cooldown_hours       = self._get_int("CATCHMINT_COOLDOWN_HOURS",        default=4)
        self.catchmint_max_supply_fraction  = self._get_float("CATCHMINT_MAX_SUPPLY_FRACTION", default=0.95)
        self.catchmint_require_verified         = self._get_bool("CATCHMINT_REQUIRE_VERIFIED",         default=True)
        # spec 003 Gate E live-data calibration (2026-05-17): только 6/50 коллекций имели
        # simulationPassed=True И все они с counts<50. Если require=True → zero-fires
        # (повтор spec 002 ошибки). Defaultim → warn-бэйдж в TG, не блокер.
        self.catchmint_require_simulation_pass  = self._get_bool("CATCHMINT_REQUIRE_SIMULATION_PASS",  default=False)
        self.catchmint_user_agent = self._get(
            "CATCHMINT_USER_AGENT",
            default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )

        # Red-flag enrichment (per-fire fetch /contracts/{addr}/ + /flags/)
        self.catchmint_enrich_enabled            = self._get_bool("CATCHMINT_ENRICH_ENABLED",            default=True)
        self.catchmint_skip_on_honeypot          = self._get_bool("CATCHMINT_SKIP_ON_HONEYPOT",          default=True)
        self.catchmint_skip_on_drain             = self._get_bool("CATCHMINT_SKIP_ON_DRAIN",             default=True)
        self.catchmint_skip_on_scam              = self._get_bool("CATCHMINT_SKIP_ON_SCAM",              default=False)
        # M3 fix: default False — notable_flag шумит, warn-badge лучше чем skip
        self.catchmint_skip_on_notable_flag      = self._get_bool("CATCHMINT_SKIP_ON_NOTABLE_FLAG",      default=False)
        self.catchmint_skip_on_hide_ratio        = self._get_float("CATCHMINT_SKIP_ON_HIDE_RATIO",       default=0.10)
        self.catchmint_warn_on_fresh_deploy_minutes = self._get_int("CATCHMINT_WARN_ON_FRESH_DEPLOY_MINUTES", default=30)
        self.catchmint_warn_on_proxy             = self._get_bool("CATCHMINT_WARN_ON_PROXY",             default=True)

        # --- Discord ---
        self.discord_user_token = self._get("DISCORD_USER_TOKEN", required=False)
        self.discord_channel_ids = self._get_list("DISCORD_CHANNEL_IDS")

        # --- Twitter ---
        self.twitter_username = self._get("TWITTER_USERNAME", required=False)
        self.twitter_password = self._get("TWITTER_PASSWORD", required=False)
        self.twitter_email = self._get("TWITTER_EMAIL", required=False)

        # --- AI ---
        self.perplexity_api_key = self._get("PERPLEXITY_API_KEY", required=False)
        self.gemini_api_key = self._get("GEMINI_API_KEY", required=False)
        self.groq_api_key = self._get("GROQ_API_KEY", required=False)
        self.anthropic_api_key = self._get("ANTHROPIC_API_KEY", required=False)

        # --- Spec 004: CT Alpha Digest ---
        # CT_DIGEST_TOPIC_ID — required for prod CT digest service; absent on dev OK.
        # Startup assertion для ANTHROPIC_API_KEY when this is set лежит в
        # services/ct_alpha_digest.py:main() (НЕ здесь — иначе все сервисы упадут
        # при module import если только CT_DIGEST_TOPIC_ID set без ANTHROPIC_API_KEY).
        _ct_topic = self._get("CT_DIGEST_TOPIC_ID", required=False)
        self.ct_digest_topic_id = int(_ct_topic) if _ct_topic else None
        # CT_DIGEST_PAUSED — kill switch ("1" pauses classifier + promise_cron).
        self.ct_digest_paused = self._get("CT_DIGEST_PAUSED", default="0") == "1"

        # --- Alphagate (история юзернеймов Twitter) ---
        self.alphagate_cookies = self._get("ALPHAGATE_COOKIES", required=False)

        # --- Database ---
        self.database_url = self._get(
            "DATABASE_URL",
            default="postgresql://xanalyst:password@localhost:5432/xanalyst",
        )

        # --- Redis ---
        self.redis_url = self._get("REDIS_URL", default="redis://localhost:6379/0")

        # --- Watchlist Tweet Watcher ---
        self.watchlist_keywords = self._get_list("WATCHLIST_KEYWORDS") or [
            "mint", "form", "discord",
        ]

        # --- Rate Limits ---
        self.max_twitter_analyses_per_day = int(
            self._get("MAX_TWITTER_ANALYSES_PER_DAY", default="50")
        )
        self.twitter_scrape_delay = int(
            self._get("TWITTER_SCRAPE_DELAY", default="5")
        )
        self.twitter_cooldown_hours = int(
            self._get("TWITTER_COOLDOWN_HOURS", default="24")
        )

    def _get(self, key: str, default: str | None = None, required: bool = False) -> str | None:
        """Получить переменную окружения."""
        value = os.getenv(key, default)
        if required and not value:
            print(f"[ОШИБКА] Переменная {key} не задана в .env!")
            sys.exit(1)
        return value

    def _get_list(self, key: str) -> list[str]:
        """Получить список из переменной окружения (через запятую)."""
        raw = os.getenv(key, "")
        if not raw:
            return []
        return [item.strip() for item in raw.split(",") if item.strip()]

    def _get_int(self, key: str, default: int = 0) -> int:
        """Получить int из ENV. На parse error возвращает default."""
        raw = os.getenv(key)
        if raw is None or raw == "":
            return default
        try:
            return int(raw)
        except ValueError:
            print(f"[WARN] {key}={raw!r} — не int, использую default {default}")
            return default

    def _get_float(self, key: str, default: float = 0.0) -> float:
        """Получить float из ENV. На parse error возвращает default."""
        raw = os.getenv(key)
        if raw is None or raw == "":
            return default
        try:
            return float(raw)
        except ValueError:
            print(f"[WARN] {key}={raw!r} — не float, использую default {default}")
            return default

    def _get_bool(self, key: str, default: bool = False) -> bool:
        """Получить bool из ENV. Truthy: 'true', '1', 'yes', 'on' (case-insensitive)."""
        raw = os.getenv(key)
        if raw is None or raw == "":
            return default
        return raw.strip().lower() in ("true", "1", "yes", "on")

    @staticmethod
    def _parse_exclude_topics(raw: str | None) -> dict[int, set[int]]:
        """
        Парсинг TELEGRAM_EXCLUDE_TOPICS.
        Формат: "chat_id:topic_id,chat_id:topic_id"
        Возвращает: {positive_chat_id: {topic_id1, topic_id2, ...}}
        chat_id нормализуется: -1001639919522 → 1639919522
        """
        result: dict[int, set[int]] = {}
        if not raw:
            return result
        for pair in raw.split(","):
            pair = pair.strip()
            if ":" not in pair:
                continue
            try:
                chat_str, topic_str = pair.split(":", 1)
                # Нормализуем chat_id: убираем -100 префикс
                chat_id_raw = chat_str.strip().lstrip("-")
                if chat_id_raw.startswith("100") and len(chat_id_raw) > 10:
                    chat_id = int(chat_id_raw[3:])
                else:
                    chat_id = int(chat_id_raw)
                topic_id = int(topic_str.strip())
                result.setdefault(chat_id, set()).add(topic_id)
            except (ValueError, TypeError):
                continue
        return result

    def check_telegram_monitor(self) -> bool:
        """Проверить, что есть всё для Telegram мониторинга."""
        missing = []
        if not self.telegram_api_id:
            missing.append("TELEGRAM_API_ID")
        if not self.telegram_api_hash:
            missing.append("TELEGRAM_API_HASH")
        if not self.telegram_phone:
            missing.append("TELEGRAM_PHONE")
        if not self.telegram_channels:
            missing.append("TELEGRAM_CHANNELS")
        if missing:
            print(f"[!] Для Telegram мониторинга нужно задать: {', '.join(missing)}")
            return False
        return True

    def check_telegram_notifier(self) -> bool:
        """Проверить, что есть всё для Telegram уведомлений."""
        missing = []
        if not self.telegram_bot_token:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not self.telegram_notify_chat_id:
            missing.append("TELEGRAM_NOTIFY_CHAT_ID")
        if missing:
            print(f"[!] Для Telegram уведомлений нужно задать: {', '.join(missing)}")
            return False
        return True


# Глобальный экземпляр — импортируй settings из этого модуля
settings = Settings()
