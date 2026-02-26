from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_TIMEZONE = "Europe/Moscow"


def now_utc_naive() -> datetime:
    """UTC now without tzinfo for naive-UTC DB convention."""
    return datetime.now(UTC).replace(tzinfo=None)


def get_zoneinfo(tz_name: str | None) -> ZoneInfo:
    name = (tz_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo(DEFAULT_TIMEZONE)


def local_to_utc_naive(local_naive_dt: datetime, tz_name: str | None) -> datetime:
    aware_local = local_naive_dt.replace(tzinfo=get_zoneinfo(tz_name))
    return aware_local.astimezone(UTC).replace(tzinfo=None)


def utc_naive_to_local(utc_naive_dt: datetime, tz_name: str | None) -> datetime:
    aware_utc = utc_naive_dt.replace(tzinfo=UTC)
    return aware_utc.astimezone(get_zoneinfo(tz_name)).replace(tzinfo=None)
