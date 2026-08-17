"""
Расчёт Twitter-метрик.

Вычисляет engagement rate, bot detection, quality score,
итоговый Twitter Score (0-100) и tier (S/A/B/C/D).

Отдельный скоринг для early projects (< 2000 followers).
"""

import logging
from datetime import datetime, timezone
from shared.twitter_client import TwitterProfile, TweetData

log = logging.getLogger("metrics")


# ==================== Утилиты ====================

def parse_account_created_at(created_at: str) -> datetime | None:
    """Парсинг даты создания аккаунта Twitter.
    Формат: "Thu Oct 26 06:12:33 +0000 2023"
    """
    if not created_at:
        return None
    try:
        return datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    except (ValueError, TypeError):
        return None


def calc_account_age_days(created_at: str) -> int:
    """Возраст аккаунта в днях. 0 если не удалось распарсить."""
    dt = parse_account_created_at(created_at)
    if not dt:
        return 0
    return max((datetime.now(timezone.utc) - dt).days, 0)


def format_account_age(age_days: int) -> str:
    """Человекочитаемый возраст: '14 дн.', '3 мес.', '2 г.'"""
    if age_days <= 0:
        return "?"
    if age_days < 60:
        return f"{age_days} дн."
    if age_days < 365:
        months = age_days // 30
        return f"{months} мес."
    years = age_days // 365
    return f"{years} г."


# ==================== Новые метрики ====================

def calc_rt_percentage(tweets: list[TweetData]) -> float:
    """Процент ретвитов среди последних твитов."""
    if not tweets:
        return 0.0
    rt_count = sum(1 for t in tweets if t.is_retweet)
    return round((rt_count / len(tweets)) * 100, 1)


def calc_growth_velocity(
    followers_count: int,
    account_age_days: int,
    prev_followers: int | None = None,
    days_since_prev: int | None = None,
) -> float:
    """
    Скорость роста фолловеров (фолловеров/день).

    Если есть предыдущий snapshot — считаем разницу.
    Иначе — среднее за всё время (followers / age).
    """
    # Есть предыдущий snapshot — точный расчёт
    if prev_followers is not None and days_since_prev and days_since_prev > 0:
        diff = followers_count - prev_followers
        return round(diff / days_since_prev, 2)

    # Fallback: среднее за всё время жизни аккаунта
    if account_age_days > 0:
        return round(followers_count / account_age_days, 2)

    return 0.0


# ==================== Базовые метрики (без изменений) ====================

def calc_engagement_rate(tweets: list[TweetData], followers_count: int) -> float:
    """
    Engagement rate = (лайки + RT + реплаи) / фолловеры * 100.
    Считается по последним твитам.
    """
    if not tweets or followers_count <= 0:
        return 0.0

    total_engagement = sum(
        t.likes + t.retweets + t.replies
        for t in tweets
    )
    avg_per_tweet = total_engagement / len(tweets)
    rate = (avg_per_tweet / followers_count) * 100
    return round(rate, 2)


def calc_bot_percentage(followers: list[dict]) -> float:
    """
    Bot detection по red flags системе.
    Каждый фолловер получает red flag points, >= 5 = бот.

    Red flags:
    - Нет аватарки: +3
    - Нет био: +2
    - Аккаунт < 30 дней: +2
    - Following/followers > 10: +2
    - Твитов < 10: +1
    - Following > 2000: +1
    - Followers < 10: +1
    """
    if not followers:
        return 0.0  # нет данных — не можем судить

    bots = 0
    for f in followers:
        flags = 0

        if not f.get("has_avatar", True):
            flags += 3
        if not f.get("has_bio", True):
            flags += 2

        # Возраст аккаунта
        created = f.get("created_at", "")
        if created:
            dt = parse_account_created_at(created)
            if dt:
                age_days = (datetime.now(timezone.utc) - dt).days
                if age_days < 30:
                    flags += 2

        following = f.get("following_count", 0)
        followers_count = f.get("followers_count", 0)

        # Ratio following/followers
        if followers_count > 0 and following / max(followers_count, 1) > 10:
            flags += 2

        if f.get("tweets_count", 0) < 10:
            flags += 1
        if following > 2000:
            flags += 1
        if followers_count < 10:
            flags += 1

        if flags >= 5:
            bots += 1

    return round((bots / len(followers)) * 100, 1)


# ==================== Скоринг: обычный (>= 2000 followers) ====================

def calc_twitter_score(
    profile: TwitterProfile,
    engagement_rate: float,
    quality_score: float,
) -> int:
    """
    Итоговый Twitter Score (0-100).

    Компоненты:
    - Followers weight (max 25)
    - Quality score (max 25)
    - Engagement rate (max 25)
    - Activity (max 15)
    - Authority (max 10)
    """
    score = 0.0

    # --- Followers (max 25) ---
    fc = profile.followers_count
    if fc >= 100_000:
        score += 25
    elif fc >= 50_000:
        score += 22
    elif fc >= 10_000:
        score += 18
    elif fc >= 5_000:
        score += 14
    elif fc >= 1_000:
        score += 10
    elif fc >= 500:
        score += 6
    elif fc >= 100:
        score += 3

    # --- Quality (max 25) ---
    score += quality_score * 0.25

    # --- Engagement (max 25) ---
    if engagement_rate >= 5.0:
        score += 25
    elif engagement_rate >= 3.0:
        score += 20
    elif engagement_rate >= 2.0:
        score += 15
    elif engagement_rate >= 1.0:
        score += 10
    elif engagement_rate >= 0.5:
        score += 5

    # --- Activity (max 15) ---
    age_days = calc_account_age_days(profile.created_at)
    if age_days > 0:
        tweets_per_day = profile.tweets_count / age_days
        if tweets_per_day >= 3:
            score += 15
        elif tweets_per_day >= 1:
            score += 12
        elif tweets_per_day >= 0.3:
            score += 8
        elif tweets_per_day >= 0.1:
            score += 4

    # --- Authority (max 10) ---
    if profile.verified:
        score += 5

    # Здоровый ratio following/followers (0.1 - 2.0)
    if profile.followers_count > 0:
        ratio = profile.following_count / profile.followers_count
        if 0.1 <= ratio <= 2.0:
            score += 5
        elif ratio <= 5.0:
            score += 2

    return min(int(score), 100)


def calc_tier(
    score: int,
    followers_count: int,
    engagement_rate: float,
) -> str:
    """Определить тир проекта (для >= 2000 followers)."""
    if score >= 80 and followers_count >= 10_000 and engagement_rate >= 5.0:
        return "S"
    elif score >= 65 and followers_count >= 5_000 and engagement_rate >= 3.0:
        return "A"
    elif score >= 50 and followers_count >= 2_000:
        return "B"
    elif score >= 30:
        return "C"
    else:
        return "D"


# ==================== Скоринг: early projects (< 2000 followers) ====================

def calc_early_score(
    profile: TwitterProfile,
    engagement_rate: float,
    rt_percentage: float,
    growth_velocity: float,
    account_age_days: int,
) -> int:
    """
    Скоринг для early projects (< 2000 followers).

    Компоненты:
    - Content quality / RT% (max 25)
    - Engagement (max 25)
    - Growth velocity (max 20)
    - Account health (max 20)
    - Activity (max 10)
    """
    score = 0.0

    # --- Content quality по RT% (max 25) ---
    # Меньше RT = больше оригинального контента
    if rt_percentage <= 10:
        score += 25
    elif rt_percentage <= 30:
        score += 20
    elif rt_percentage <= 50:
        score += 15
    elif rt_percentage <= 70:
        score += 8
    elif rt_percentage <= 90:
        score += 3

    # --- Engagement (max 25) ---
    if engagement_rate >= 10.0:
        score += 25
    elif engagement_rate >= 5.0:
        score += 20
    elif engagement_rate >= 3.0:
        score += 15
    elif engagement_rate >= 1.0:
        score += 10
    elif engagement_rate >= 0.5:
        score += 5

    # --- Growth velocity (max 20) ---
    if growth_velocity >= 20:
        score += 20
    elif growth_velocity >= 10:
        score += 16
    elif growth_velocity >= 5:
        score += 12
    elif growth_velocity >= 2:
        score += 8
    elif growth_velocity >= 0.5:
        score += 4

    # --- Account health (max 20) ---
    health = 0.0

    # Возраст: 30-365 дней — идеально для early
    if 30 <= account_age_days <= 365:
        health += 8
    elif account_age_days > 365:
        health += 5  # старый аккаунт — менее типично для early
    elif account_age_days >= 7:
        health += 3

    # Verified (любой тип)
    if profile.verified or getattr(profile, "is_blue_verified", False):
        health += 4

    # Following/followers ratio
    if profile.followers_count > 0:
        ratio = profile.following_count / profile.followers_count
        if 0.2 <= ratio <= 3.0:
            health += 4
        elif ratio <= 5.0:
            health += 2

    # Есть bio
    if profile.bio and len(profile.bio) > 20:
        health += 4

    score += min(health, 20)

    # --- Activity (max 10) ---
    if account_age_days > 0:
        tweets_per_day = profile.tweets_count / account_age_days
        if tweets_per_day >= 3:
            score += 10
        elif tweets_per_day >= 1:
            score += 8
        elif tweets_per_day >= 0.3:
            score += 5
        elif tweets_per_day >= 0.1:
            score += 2

    return min(int(score), 100)


def calc_early_tier(
    score: int,
    engagement_rate: float,
    rt_percentage: float,
) -> str:
    """Определить тир для early projects."""
    if score >= 75 and engagement_rate >= 3.0 and rt_percentage < 50:
        return "S"
    elif score >= 60 and engagement_rate >= 2.0:
        return "A"
    elif score >= 45:
        return "B"
    elif score >= 25:
        return "C"
    else:
        return "D"


# ==================== Удобная обёртка ====================

def analyze_profile(
    profile: TwitterProfile,
    tweets: list[TweetData],
    followers_sample: list[dict] | None = None,
    prev_followers: int | None = None,
    days_since_prev: int | None = None,
) -> dict:
    """
    Полный анализ профиля. Возвращает словарь с метриками.

    Если followers_sample=None, bot detection пропускается (quality=100).
    Для early projects (< 2000 followers) используется отдельный скоринг.
    """
    engagement_rate = calc_engagement_rate(tweets, profile.followers_count)

    if followers_sample:
        bot_pct = calc_bot_percentage(followers_sample)
    else:
        bot_pct = 0.0  # нет данных — считаем optimistic

    quality_score = 100.0 - bot_pct

    # Новые метрики
    rt_percentage = calc_rt_percentage(tweets)
    account_age_days = calc_account_age_days(profile.created_at)
    account_age_human = format_account_age(account_age_days)
    growth_velocity = calc_growth_velocity(
        profile.followers_count, account_age_days,
        prev_followers, days_since_prev,
    )

    # Выбираем скоринг: early (< 2000) или обычный
    is_early = profile.followers_count < 2000

    if is_early:
        twitter_score = calc_early_score(
            profile, engagement_rate, rt_percentage,
            growth_velocity, account_age_days,
        )
        tier = calc_early_tier(twitter_score, engagement_rate, rt_percentage)
    else:
        twitter_score = calc_twitter_score(profile, engagement_rate, quality_score)
        tier = calc_tier(twitter_score, profile.followers_count, engagement_rate)

    return {
        "engagement_rate": engagement_rate,
        "bot_percentage": bot_pct,
        "quality_score": quality_score,
        "twitter_score": twitter_score,
        "tier": tier,
        "rt_percentage": rt_percentage,
        "account_age_days": account_age_days,
        "account_age_human": account_age_human,
        "growth_velocity": growth_velocity,
        "is_early": is_early,
    }
