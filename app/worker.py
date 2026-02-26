import os
import time
from datetime import timedelta

from flask import Flask

from app.extensions import db
from app.models import Publication
from app.services.audit import log_action
from app.services.publishing import send_publication
from app.utils.timezone import now_utc_naive


def recover_stuck_publications(app: Flask, worker_id: str) -> int:
    ttl = app.config["PROCESSING_TTL_SECONDS"]
    stuck_before = now_utc_naive() - timedelta(seconds=ttl)
    restored = (
        Publication.query.filter(
            Publication.status == "processing",
            Publication.locked_at.isnot(None),
            Publication.locked_at <= stuck_before,
            Publication.attempts < app.config["MAX_ATTEMPTS"],
        ).update(
            {
                "status": "retry",
                "ready_at": now_utc_naive(),
                "locked_at": None,
                "locked_by": worker_id,
                "last_error": "processing_ttl_expired",
            }
        )
    )
    if restored:
        db.session.commit()
    return int(restored or 0)


def run_worker(app: Flask) -> None:
    worker_id = f"worker-{os.getpid()}"
    max_attempts = app.config["MAX_ATTEMPTS"]
    default_retry_minutes = app.config["DEFAULT_RETRY_MINUTES"]
    interval = app.config["WORKER_INTERVAL_SECONDS"]

    with app.app_context():
        while True:
            recover_stuck_publications(app, worker_id)
            now = now_utc_naive()
            due = (
                Publication.query.filter(
                    Publication.status.in_(["scheduled", "retry"]),
                    Publication.ready_at <= now,
                    Publication.attempts < max_attempts,
                )
                .order_by(Publication.ready_at.asc(), Publication.planned_at.asc(), Publication.id.asc())
                .limit(20)
                .all()
            )

            for pub in due:
                locked = (
                    Publication.query.filter(
                        Publication.id == pub.id,
                        Publication.status.in_(["scheduled", "retry"]),
                    ).update({"status": "processing", "locked_at": now_utc_naive(), "locked_by": worker_id})
                )
                db.session.commit()
                if not locked:
                    continue

                refreshed = db.session.get(Publication, pub.id)
                if not refreshed:
                    continue
                if refreshed.telegram_message_id:
                    refreshed.status = "sent"
                    refreshed.sent_at = now_utc_naive()
                    db.session.commit()
                    continue

                result = send_publication(refreshed)
                if result.ok:
                    refreshed.status = "sent"
                    refreshed.telegram_message_id = result.message_id
                    refreshed.sent_at = now_utc_naive()
                    refreshed.last_error = None
                    refreshed.locked_at = None
                    refreshed.locked_by = worker_id
                    log_action("publication", refreshed.id, "send", {"message_id": result.message_id})

                    pending = (
                        Publication.query.filter_by(post_id=refreshed.post_id)
                        .filter(Publication.status.in_(["scheduled", "retry", "processing"]))
                        .count()
                    )
                    if pending == 0:
                        refreshed.post.status = "sent"
                else:
                    refreshed.attempts += 1
                    refreshed.last_error = result.error
                    refreshed.locked_at = None
                    refreshed.locked_by = worker_id
                    if (not result.retryable) or refreshed.attempts >= max_attempts:
                        refreshed.status = "failed"
                        refreshed.post.status = "failed"
                        log_action("publication", refreshed.id, "fail", {"error": result.error})
                    else:
                        retry_delay = max(default_retry_minutes * 60, int(result.retry_after_seconds or 0))
                        refreshed.status = "retry"
                        refreshed.ready_at = now_utc_naive() + timedelta(seconds=retry_delay)
                        log_action(
                            "publication",
                            refreshed.id,
                            "retry",
                            {"error": result.error, "delay_seconds": retry_delay},
                        )
                db.session.commit()

            time.sleep(interval)
