import logging
from datetime import UTC, datetime, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "Europe/Moscow"
logger = logging.getLogger(__name__)


def now_utc_naive() -> datetime:
    """UTC now without tzinfo for naive-UTC DB convention."""
    return datetime.now(UTC).replace(tzinfo=None)


def get_zoneinfo(tz_name: str | None) -> tzinfo:
    name = (tz_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        logger.warning("[timezone] Таймзона '%s' не найдена, пробуем значение по умолчанию '%s'", name, DEFAULT_TIMEZONE)

    try:
        return ZoneInfo(DEFAULT_TIMEZONE)
    except ZoneInfoNotFoundError:
        logger.error(
            "[timezone] Таймзона по умолчанию '%s' недоступна (tzdata не установлена). Используется UTC.",
            DEFAULT_TIMEZONE,
        )
        return UTC


def local_to_utc_naive(local_naive_dt: datetime, tz_name: str | None) -> datetime:
    aware_local = local_naive_dt.replace(tzinfo=get_zoneinfo(tz_name))
    return aware_local.astimezone(UTC).replace(tzinfo=None)


def utc_naive_to_local(utc_naive_dt: datetime, tz_name: str | None) -> datetime:
    aware_utc = utc_naive_dt.replace(tzinfo=UTC)
    return aware_utc.astimezone(get_zoneinfo(tz_name)).replace(tzinfo=None)
