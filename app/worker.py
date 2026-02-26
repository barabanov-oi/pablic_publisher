import logging
import os
import time
from datetime import timedelta

from flask import Flask

from app.extensions import db
from app.models import Publication
from app.services.audit import log_action
from app.services.publishing import get_retry_ready_at, send_publication
from app.utils.timezone import now_utc_naive


logger = logging.getLogger(__name__)


def recover_stuck_publications(app: Flask, worker_id: str) -> int:
    ttl = app.config["PROCESSING_TTL_SECONDS"]
    max_attempts = app.config["MAX_ATTEMPTS"]
    stuck_before = now_utc_naive() - timedelta(seconds=ttl)
    now = now_utc_naive()

    restored = (
        Publication.query.filter(
            Publication.status == "processing",
            Publication.locked_at.isnot(None),
            Publication.locked_at <= stuck_before,
            Publication.attempts < max_attempts,
        ).update(
            {
                "status": "retry",
                "ready_at": now,
                "locked_at": None,
                "locked_by": None,
                "last_error": "processing_ttl_expired",
            }
        )
    )
    if restored:
        db.session.commit()
        logger.warning(
            "[worker] Восстановлены зависшие публикации: worker_id=%s restored=%s ttl=%s",
            worker_id,
            restored,
            ttl,
        )
    return int(restored or 0)


def _claim_due_publication_ids(worker_id: str, limit: int, max_attempts: int) -> list[int]:
    now = now_utc_naive()
    due_ids = [
        row[0]
        for row in (
            db.session.query(Publication.id)
            .filter(
                Publication.status.in_(["scheduled", "retry"]),
                Publication.ready_at <= now,
                Publication.attempts < max_attempts,
            )
            .order_by(Publication.ready_at.asc(), Publication.planned_at.asc(), Publication.id.asc())
            .limit(limit)
            .all()
        )
    ]
    if not due_ids:
        return []

    locked_ids: list[int] = []
    for pub_id in due_ids:
        updated = (
            Publication.query.filter(
                Publication.id == pub_id,
                Publication.status.in_(["scheduled", "retry"]),
            ).update(
                {
                    "status": "processing",
                    "locked_at": now_utc_naive(),
                    "locked_by": worker_id,
                }
            )
        )
        if updated:
            locked_ids.append(pub_id)
    db.session.commit()
    return locked_ids


def _process_publication(pub_id: int, max_attempts: int, default_retry_minutes: int) -> None:
    publication = db.session.get(Publication, pub_id)
    if not publication:
        logger.warning("[worker] Публикация не найдена после захвата: publication_id=%s", pub_id)
        return

    try:
        if publication.telegram_message_id:
            publication.status = "sent"
            publication.sent_at = now_utc_naive()
            publication.last_error = None
            publication.locked_at = None
            publication.locked_by = None
            db.session.commit()
            logger.info(
                "[worker] Публикация уже была отправлена ранее: publication_id=%s message_id=%s",
                publication.id,
                publication.telegram_message_id,
            )
            return

        result = send_publication(publication)
        publication.locked_at = None
        publication.locked_by = None

        if result.ok:
            publication.status = "sent"
            publication.telegram_message_id = result.message_id
            publication.sent_at = now_utc_naive()
            publication.last_error = None
            log_action("publication", publication.id, "send", {"message_id": result.message_id})

            pending = (
                Publication.query.filter_by(post_id=publication.post_id)
                .filter(Publication.status.in_(["scheduled", "retry", "processing"]))
                .count()
            )
            if pending == 0:
                publication.post.status = "sent"

            logger.info("[worker] Публикация отправлена: publication_id=%s message_id=%s", publication.id, result.message_id)
        else:
            publication.attempts += 1
            publication.last_error = result.error

            if (not result.retryable) or publication.attempts >= max_attempts:
                publication.status = "failed"
                publication.post.status = "failed"
                log_action("publication", publication.id, "fail", {"error": result.error})
                logger.error(
                    "[worker] Публикация помечена failed: publication_id=%s attempts=%s error=%s",
                    publication.id,
                    publication.attempts,
                    result.error,
                )
            else:
                new_status, ready_at = get_retry_ready_at(default_retry_minutes, result.retry_after_seconds)
                publication.status = new_status
                publication.ready_at = ready_at
                log_action(
                    "publication",
                    publication.id,
                    "retry",
                    {"error": result.error, "ready_at": ready_at.isoformat()},
                )
                logger.warning(
                    "[worker] Публикация отправлена в retry: publication_id=%s attempts=%s ready_at=%s error=%s",
                    publication.id,
                    publication.attempts,
                    ready_at,
                    result.error,
                )

        db.session.commit()
    except Exception as exc:  # noqa: BLE001
        db.session.rollback()
        logger.exception("[worker] Ошибка обработки публикации publication_id=%s: %s", pub_id, exc)

        publication = db.session.get(Publication, pub_id)
        if publication:
            publication.locked_at = None
            publication.locked_by = None
            publication.attempts += 1
            publication.last_error = f"worker_error: {exc}"
            if publication.attempts >= max_attempts:
                publication.status = "failed"
                publication.post.status = "failed"
            else:
                publication.status = "retry"
                publication.ready_at = now_utc_naive() + timedelta(minutes=default_retry_minutes)
            db.session.commit()


def run_worker(app: Flask) -> None:
    worker_id = f"worker-{os.getpid()}"
    max_attempts = app.config["MAX_ATTEMPTS"]
    default_retry_minutes = app.config["DEFAULT_RETRY_MINUTES"]
    interval = app.config["WORKER_INTERVAL_SECONDS"]
    batch_size = 20

    with app.app_context():
        logger.info(
            "[worker] Запуск: worker_id=%s interval=%s max_attempts=%s retry_minutes=%s",
            worker_id,
            interval,
            max_attempts,
            default_retry_minutes,
        )

        while True:
            try:
                recover_stuck_publications(app, worker_id)
                pub_ids = _claim_due_publication_ids(worker_id, batch_size, max_attempts)
                logger.info("[worker] Захвачено задач: worker_id=%s count=%s", worker_id, len(pub_ids))

                for pub_id in pub_ids:
                    _process_publication(pub_id, max_attempts, default_retry_minutes)
            except Exception as exc:  # noqa: BLE001
                db.session.rollback()
                logger.exception("[worker] Критическая ошибка итерации worker_id=%s: %s", worker_id, exc)

            time.sleep(interval)
