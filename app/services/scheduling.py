from datetime import datetime, time as dt_time, timedelta

from sqlalchemy import func

from app.extensions import db
from app.models import Channel, Post, Publication
from app.utils.timezone import local_to_utc_naive, now_utc_naive, utc_naive_to_local


def parse_time(value: str) -> dt_time:
    return datetime.strptime(value, "%H:%M").time()


def calculate_next_slot(channel: Channel) -> tuple[datetime, int]:
    now_local = utc_naive_to_local(now_utc_naive(), channel.timezone)
    daily = parse_time(channel.daily_time)
    base_local = datetime.combine(now_local.date(), daily)
    if base_local <= now_local:
        base_local += timedelta(days=1)

    planned_utc = local_to_utc_naive(base_local, channel.timezone)
    for _ in range(365):
        day_start = planned_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        day_count = (
            db.session.query(func.count(Publication.id))
            .join(Post, Post.id == Publication.post_id)
            .filter(
                Post.channel_id == channel.id,
                Publication.planned_at >= day_start,
                Publication.planned_at < day_end,
            )
            .scalar()
            or 0
        )
        slot_index = int(day_count)
        candidate = planned_utc + timedelta(seconds=slot_index)
        if candidate > now_utc_naive():
            return candidate, slot_index
        planned_utc += timedelta(days=1)

    return now_utc_naive() + timedelta(minutes=1), 0


def adjust_to_window(channel: Channel, planned_utc: datetime) -> datetime:
    start = parse_time(channel.allowed_window_start)
    end = parse_time(channel.allowed_window_end)
    planned_local = utc_naive_to_local(planned_utc, channel.timezone)
    current_time = planned_local.time()

    if start <= current_time <= end:
        return planned_utc

    if current_time < start:
        adjusted_local = datetime.combine(planned_local.date(), start)
    else:
        adjusted_local = datetime.combine(planned_local.date() + timedelta(days=1), start)
    return local_to_utc_naive(adjusted_local, channel.timezone)
