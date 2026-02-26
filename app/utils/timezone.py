import logging
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "Europe/Moscow"
logger = logging.getLogger(__name__)
FALLBACK_FIXED_OFFSETS = {
    "Europe/Moscow": timezone(timedelta(hours=3), name="Europe/Moscow"),
}


def now_utc_naive() -> datetime:
    """UTC now without tzinfo for naive-UTC DB convention."""
    return datetime.now(UTC).replace(tzinfo=None)


@lru_cache(maxsize=128)
def _resolve_timezone(name: str) -> tzinfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        fallback = FALLBACK_FIXED_OFFSETS.get(name)
        if fallback is not None:
            logger.warning(
                "[timezone] Таймзона '%s' недоступна через zoneinfo. Используется фиксированный UTC-сдвиг для '%s'.",
                name,
                name,
            )
            return fallback

        logger.warning("[timezone] Таймзона '%s' не найдена, используем UTC.", name)
        return UTC


def get_zoneinfo(tz_name: str | None) -> tzinfo:
    name = (tz_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        resolved = _resolve_timezone(name)
        if resolved is not UTC or name == "UTC":
            return resolved

        if name != DEFAULT_TIMEZONE:
            logger.warning(
                "[timezone] Переход на таймзону по умолчанию '%s' после ошибки в '%s'.",
                DEFAULT_TIMEZONE,
                name,
            )
            return _resolve_timezone(DEFAULT_TIMEZONE)

        return UTC
    except Exception:  # noqa: BLE001
        logger.exception("[timezone] Непредвиденная ошибка резолва таймзоны '%s'. Используется UTC.", name)
        return UTC


def local_to_utc_naive(local_naive_dt: datetime, tz_name: str | None) -> datetime:
    aware_local = local_naive_dt.replace(tzinfo=get_zoneinfo(tz_name))
    return aware_local.astimezone(UTC).replace(tzinfo=None)


def utc_naive_to_local(utc_naive_dt: datetime, tz_name: str | None) -> datetime:
    aware_utc = utc_naive_dt.replace(tzinfo=UTC)
    return aware_utc.astimezone(get_zoneinfo(tz_name)).replace(tzinfo=None)
