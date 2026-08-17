"""
Twitter клиент — обёртка над Twikit.

Поддерживает авторизацию через cookies (безопаснее чем login/password).
Получает профиль, твиты и sample фолловеров для анализа.

Включает:
- Retry с backoff (3 попытки, 5с → 15с → 30с)
- Circuit breaker (5 ошибок подряд → пауза 30 мин)
- Валидация авторизации при старте
- Автогенерация ct0 если отсутствует
- Алерты в Telegram при критических проблемах

Использование:
    from shared.twitter_client import TwitterClient

    tc = TwitterClient()
    await tc.init()
    profile = await tc.get_profile("elonmusk")
    tweets = await tc.get_recent_tweets("elonmusk", count=20)
"""

import json
import secrets
import time
import asyncio
import logging
from pathlib import Path
from dataclasses import dataclass

from twikit import Client as TwikitClient
from twikit.errors import (
    Unauthorized,
    Forbidden,
    TooManyRequests,
    ServerError,
    RequestTimeout,
    AccountLocked,
    AccountSuspended,
    TwitterException,
)

from shared.config import settings, PROJECT_ROOT
from shared.notifier import send_admin_alert

log = logging.getLogger("twitter_client")

# Файл для сохранения cookies
COOKIES_FILE = PROJECT_ROOT / "data" / "twitter_cookies.json"

# Retry настройки
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]

# Circuit breaker настройки
CIRCUIT_BREAKER_THRESHOLD = 5        # ошибок подряд до открытия
CIRCUIT_BREAKER_COOLDOWN = 30 * 60   # 30 мин авто-сброс


@dataclass
class TwitterProfile:
    """Данные профиля Twitter."""
    user_id: int
    handle: str           # screen_name (без @)
    display_name: str
    bio: str
    followers_count: int
    following_count: int
    tweets_count: int
    created_at: str       # дата создания аккаунта
    verified: bool
    profile_image_url: str
    is_blue_verified: bool = False  # Twitter Blue (платная верификация)
    location: str = ""              # "Based in" из профиля (self-reported)


@dataclass
class TweetData:
    """Данные одного твита."""
    tweet_id: str
    text: str
    created_at: str
    likes: int
    retweets: int
    replies: int
    views: int | None
    is_retweet: bool = False  # True если это ретвит чужого поста
    handle: str = ""          # screen_name автора (заполняется в search_tweets)


class TwitterClient:
    """Обёртка над Twikit с поддержкой cookies, retry и circuit breaker."""

    def __init__(self):
        self._client = TwikitClient("en-US")
        self._ready = False
        # Circuit breaker state
        self._consecutive_failures = 0
        self._circuit_open = False
        self._circuit_opened_at: float | None = None

    # --- Классификация ошибок ---

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        """
        Определить, стоит ли повторять запрос при этой ошибке.

        НЕ ретраить: Unauthorized, Forbidden, AccountLocked, AccountSuspended
        (эти ошибки не исчезнут при повторе)
        TooManyRequests обрабатывается отдельно (своя логика ожидания)
        """
        # Фатальные — повтор бесполезен
        if isinstance(exc, (Unauthorized, Forbidden, AccountLocked, AccountSuspended)):
            return False
        # TooManyRequests — отдельная ветка в _retry_with_backoff
        if isinstance(exc, TooManyRequests):
            return False
        # Серверные и сетевые ошибки — можно повторить
        if isinstance(exc, (ServerError, RequestTimeout, ConnectionError, asyncio.TimeoutError)):
            return True
        # Прочие TwitterException — на всякий случай ретраим
        if isinstance(exc, TwitterException):
            return True
        # Сетевые ошибки (OSError покрывает ConnectionError и т.д.)
        if isinstance(exc, OSError):
            return True
        return False

    # --- Retry с backoff ---

    async def _retry_with_backoff(self, func, *args, operation_name: str = ""):
        """
        Вызвать func(*args) с retry.

        - 3 попытки с задержками 5с → 15с → 30с
        - TooManyRequests: ждём до rate_limit_reset (или 15 мин дефолт)
        - Не-retryable ошибки → сразу raise
        """
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                result = await func(*args)
                return result

            except TooManyRequests as e:
                # Читаем время сброса лимита
                reset_time = getattr(e, "rate_limit_reset", None)
                if reset_time:
                    wait = max(int(reset_time) - int(time.time()), 1)
                    wait = min(wait, 15 * 60)  # Не больше 15 мин
                else:
                    wait = 15 * 60  # Дефолт 15 мин

                log.warning(
                    f"429 Rate limit ({operation_name}), жду {wait}с "
                    f"(попытка {attempt + 1}/{MAX_RETRIES})"
                )
                await asyncio.sleep(wait)
                last_exc = e

            except Exception as e:
                last_exc = e

                if not self._is_retryable(e):
                    # Фатальная ошибка — сразу прокидываем
                    raise

                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    log.warning(
                        f"Ошибка {type(e).__name__} ({operation_name}), "
                        f"retry через {delay}с (попытка {attempt + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(delay)
                else:
                    log.error(
                        f"Все {MAX_RETRIES} попыток исчерпаны ({operation_name}): {e}"
                    )

        # Все попытки провалились
        if last_exc:
            raise last_exc

    # --- Валидация авторизации ---

    async def _validate_auth(self) -> bool:
        """
        Тестовый запрос для проверки валидности кук.

        Запрашивает профиль @Twitter — если 401/403, куки мертвы.
        """
        try:
            await self._client.get_user_by_screen_name("Twitter")
            log.info("Twitter: авторизация валидна ✓")
            return True
        except Unauthorized:
            log.error("Twitter: куки невалидны (401 Unauthorized)")
            send_admin_alert(
                "Twitter куки невалидны (401).\n"
                "Нужно обновить auth_token:\n"
                "<code>python scripts/setup_twitter_cookies.py</code>",
                urgent=True,
            )
            return False
        except Forbidden:
            log.error("Twitter: аккаунт забанен или ограничен (403 Forbidden)")
            send_admin_alert(
                "Twitter аккаунт забанен/ограничен (403).\n"
                "Нужно сменить аккаунт и обновить куки.",
                urgent=True,
            )
            return False
        except Exception as e:
            # Сетевая ошибка — оптимистично считаем куки валидными
            log.warning(f"Twitter: не удалось проверить авторизацию ({e}), продолжаем")
            return True

    # --- Circuit breaker ---

    def _check_circuit(self) -> bool:
        """
        Проверить состояние circuit breaker.

        Возвращает True если можно делать запросы, False если circuit открыт.
        Автоматически сбрасывает circuit через CIRCUIT_BREAKER_COOLDOWN.
        """
        if not self._circuit_open:
            return True

        # Авто-сброс по таймауту
        if self._circuit_opened_at:
            elapsed = time.time() - self._circuit_opened_at
            if elapsed >= CIRCUIT_BREAKER_COOLDOWN:
                log.info(
                    f"Circuit breaker: авто-сброс после {int(elapsed)}с паузы"
                )
                self._circuit_open = False
                self._consecutive_failures = 0
                self._circuit_opened_at = None
                return True

        return False

    def _record_success(self):
        """Успешный запрос — сбрасываем счётчик ошибок."""
        if self._consecutive_failures > 0:
            log.debug(
                f"Circuit breaker: сброс после {self._consecutive_failures} ошибок"
            )
        self._consecutive_failures = 0

    def _record_failure(self, exc: Exception):
        """
        Неуспешный запрос — инкрементируем счётчик.
        При достижении порога — открываем circuit breaker.
        """
        self._consecutive_failures += 1
        log.warning(
            f"Circuit breaker: ошибка #{self._consecutive_failures} "
            f"({type(exc).__name__}: {exc})"
        )

        if self._consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD and not self._circuit_open:
            self._circuit_open = True
            self._circuit_opened_at = time.time()
            log.error(
                f"Circuit breaker ОТКРЫТ после {self._consecutive_failures} ошибок подряд. "
                f"Пауза {CIRCUIT_BREAKER_COOLDOWN // 60} мин."
            )
            send_admin_alert(
                f"Twitter circuit breaker открыт!\n"
                f"{self._consecutive_failures} ошибок подряд.\n"
                f"Последняя: <code>{type(exc).__name__}: {exc}</code>\n"
                f"Авто-сброс через {CIRCUIT_BREAKER_COOLDOWN // 60} мин.",
                urgent=True,
            )

    @property
    def is_healthy(self) -> bool:
        """Готов ли клиент к запросам (инициализирован + circuit closed)."""
        if not self._ready:
            return False
        # Проверяем с авто-сбросом
        return self._check_circuit()

    # --- Инициализация ---

    async def init(self) -> bool:
        """
        Инициализация: загрузить cookies, автогенерация ct0, валидация.

        1. Загрузить cookies JSON
        2. Если нет ct0 → сгенерировать secrets.token_hex(16) и сохранить
        3. load_cookies() в Twikit
        4. _validate_auth() → если False → fallback на login/password
        """
        # Пробуем загрузить и подготовить cookies
        if COOKIES_FILE.exists():
            try:
                # Загружаем JSON вручную — проверяем/генерируем ct0
                with open(COOKIES_FILE) as f:
                    cookies = json.load(f)

                if not cookies.get("auth_token"):
                    log.warning("Twitter cookies: нет auth_token")
                else:
                    # Авто-генерация ct0 если отсутствует
                    if not cookies.get("ct0"):
                        cookies["ct0"] = secrets.token_hex(16)
                        log.info("Twitter: ct0 сгенерирован автоматически")
                        # Сохраняем обратно
                        with open(COOKIES_FILE, "w") as f:
                            json.dump(cookies, f, indent=2)

                    # Загружаем в Twikit
                    self._client.load_cookies(str(COOKIES_FILE))
                    self._ready = True
                    log.info("Twitter: cookies загружены из файла")

                    # Проверяем валидность
                    if await self._validate_auth():
                        return True
                    else:
                        # Куки мертвы — пробуем login/password
                        self._ready = False
                        log.warning("Twitter: cookies невалидны, пробую login/password")

            except json.JSONDecodeError as e:
                log.error(f"Twitter: ошибка парсинга cookies JSON: {e}")
            except Exception as e:
                log.warning(f"Не удалось загрузить cookies: {e}")

        # Fallback: пробуем залогиниться через логин/пароль
        if settings.twitter_username and settings.twitter_password:
            try:
                await self._client.login(
                    auth_info_1=settings.twitter_username,
                    auth_info_2=settings.twitter_email,
                    password=settings.twitter_password,
                )
                # Сохраняем cookies для следующего раза
                COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
                self._client.save_cookies(str(COOKIES_FILE))
                self._ready = True
                log.info("Twitter: залогинился и сохранил cookies")
                return True
            except Exception as e:
                log.error(f"Ошибка логина в Twitter: {e}")

        # Всё провалилось — алерт админу
        log.error(
            "Twitter: нет валидных cookies и не удалось залогиниться. "
            "Запусти: python scripts/setup_twitter_cookies.py"
        )
        send_admin_alert(
            "Twitter клиент не смог авторизоваться!\n"
            "Ни cookies, ни login/password не сработали.\n\n"
            "Нужно обновить auth_token:\n"
            "<code>python scripts/setup_twitter_cookies.py</code>",
            urgent=True,
        )
        return False

    # --- Основные методы ---

    async def get_profile(self, handle: str) -> TwitterProfile | None:
        """Получить профиль пользователя по handle."""
        if not self._ready:
            log.error("Twitter клиент не инициализирован")
            return None

        if not self._check_circuit():
            log.warning(f"Circuit breaker открыт, пропускаю @{handle}")
            return None

        try:
            user = await self._retry_with_backoff(
                self._client.get_user_by_screen_name,
                handle,
                operation_name=f"get_profile(@{handle})",
            )
            self._record_success()
            return TwitterProfile(
                user_id=int(user.id),
                handle=user.screen_name,
                display_name=user.name or "",
                bio=user.description or "",
                followers_count=user.followers_count or 0,
                following_count=user.following_count or 0,
                tweets_count=user.statuses_count or 0,
                created_at=user.created_at or "",
                verified=bool(user.verified),
                profile_image_url=getattr(user, "profile_image_url", "") or "",
                is_blue_verified=bool(getattr(user, "is_blue_verified", False)),
                location=getattr(user, "location", "") or "",
            )
        except Exception as e:
            self._record_failure(e)
            log.error(f"Ошибка получения профиля @{handle}: {e}")
            return None

    async def get_recent_tweets(self, handle: str, count: int = 20) -> list[TweetData]:
        """Получить последние твиты пользователя."""
        if not self._ready:
            return []

        if not self._check_circuit():
            log.warning(f"Circuit breaker открыт, пропускаю твиты @{handle}")
            return []

        try:
            user = await self._retry_with_backoff(
                self._client.get_user_by_screen_name,
                handle,
                operation_name=f"get_user_for_tweets(@{handle})",
            )
            tweets_raw = await self._retry_with_backoff(
                self._client.get_user_tweets,
                user.id, "Tweets",
                operation_name=f"get_tweets(@{handle})",
            )

            tweets = []
            for t in tweets_raw:
                # Определяем ретвит: через свойство Twikit или по тексту "RT @"
                try:
                    is_rt = t.retweeted_tweet is not None
                except Exception:
                    is_rt = (t.text or "").startswith("RT @")

                tweets.append(TweetData(
                    tweet_id=t.id,
                    text=t.text or "",
                    created_at=t.created_at or "",
                    likes=t.favorite_count or 0,
                    retweets=t.retweet_count or 0,
                    replies=t.reply_count or 0,
                    views=getattr(t, "view_count", None),
                    is_retweet=is_rt,
                ))

            self._record_success()
            return tweets
        except Exception as e:
            self._record_failure(e)
            log.error(f"Ошибка получения твитов @{handle}: {e}")
            return []

    async def search_tweets(self, query: str, count: int = 20) -> list[TweetData]:
        """
        Поиск твитов по запросу (SearchTimeline API).

        Используется tweet_watcher для мониторинга keywords в watchlist.
        query — строка поиска Twitter (from:h1 OR from:h2) (kw1 OR kw2)
        """
        if not self._ready:
            return []

        if not self._check_circuit():
            log.warning("Circuit breaker открыт, пропускаю search_tweets")
            return []

        try:
            results = await self._retry_with_backoff(
                self._client.search_tweet,
                query, "Latest", count,
                operation_name=f"search_tweets({query[:60]}...)",
            )

            tweets = []
            for t in results:
                try:
                    is_rt = t.retweeted_tweet is not None
                except Exception:
                    is_rt = (t.text or "").startswith("RT @")

                # Извлекаем handle автора
                author_handle = ""
                if hasattr(t, "user") and t.user:
                    author_handle = getattr(t.user, "screen_name", "") or ""

                tweets.append(TweetData(
                    tweet_id=t.id,
                    text=t.text or "",
                    created_at=t.created_at or "",
                    likes=t.favorite_count or 0,
                    retweets=t.retweet_count or 0,
                    replies=t.reply_count or 0,
                    views=getattr(t, "view_count", None),
                    is_retweet=is_rt,
                    handle=author_handle,
                ))

            self._record_success()
            return tweets
        except Exception as e:
            self._record_failure(e)
            log.error(f"Ошибка search_tweets: {e}")
            return []

    async def get_follower_sample(self, handle: str, count: int = 100) -> list[dict]:
        """
        Получить sample фолловеров для bot detection.
        ОСТОРОЖНО: это самая рискованная операция, используй маленький count.
        """
        if not self._ready:
            return []

        if not self._check_circuit():
            return []

        try:
            user = await self._retry_with_backoff(
                self._client.get_user_by_screen_name,
                handle,
                operation_name=f"get_user_for_followers(@{handle})",
            )
            followers_raw = await self._retry_with_backoff(
                self._client.get_user_followers,
                user.id,
                operation_name=f"get_followers(@{handle})",
            )

            followers = []
            for f in followers_raw:
                followers.append({
                    "user_id": f.id,
                    "handle": f.screen_name or "",
                    "followers_count": f.followers_count or 0,
                    "following_count": f.following_count or 0,
                    "tweets_count": f.statuses_count or 0,
                    "created_at": f.created_at or "",
                    "has_avatar": not getattr(f, "default_profile_image", True),
                    "has_bio": bool(f.description),
                })

            self._record_success()
            return followers
        except Exception as e:
            self._record_failure(e)
            log.error(f"Ошибка получения фолловеров @{handle}: {e}")
            return []
