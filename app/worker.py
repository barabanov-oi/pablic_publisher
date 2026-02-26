import logging
import os
import time
from datetime import timedelta

from flask import Flask

from app.extensions import db
from app.models import Publication
from app.services.audit import log_action
from app.services.publishing import send_publication
from app.utils.timezone import now_utc_naive


logger = logging.getLogger(__name__)


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
                "locked_by": None,
                "last_error": "processing_ttl_expired",
            }
        )
    )
    if restored:
        db.session.commit()
        logger.info("[queue] Восстановлены зависшие публикации: %s", restored)
    return int(restored or 0)


def run_worker(app: Flask) -> None:
    worker_id = f"worker-{os.getpid()}"
    max_attempts = app.config["MAX_ATTEMPTS"]
    default_retry_minutes = app.config["DEFAULT_RETRY_MINUTES"]
    interval = app.config["WORKER_INTERVAL_SECONDS"]

    with app.app_context():
        logger.info("[queue] Воркер запущен: %s", worker_id)
        while True:
            restored = recover_stuck_publications(app, worker_id)
            if restored:
                logger.info("[queue] Восстановлено из processing в retry: %s", restored)
            now = now_utc_naive()

            exhausted = (
                Publication.query.filter(
                    Publication.status.in_(["scheduled", "retry"]),
                    Publication.attempts >= max_attempts,
                ).all()
            )
            if exhausted:
                for item in exhausted:
                    item.status = "failed"
                    item.last_error = item.last_error or "max_attempts_reached"
                    item.locked_at = None
                    item.locked_by = worker_id
                    if item.post:
                        item.post.status = "failed"
                    log_action("publication", item.id, "fail", {"error": item.last_error})
                    logger.error("[queue] Публикация id=%s переведена в failed: достигнут лимит попыток (%s)", item.id, max_attempts)
                db.session.commit()

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

            if due:
                logger.info("[queue] Найдено публикаций к отправке: %s", len(due))

            for pub in due:
                logger.info("[queue] Обработка публикации id=%s post_id=%s attempts=%s", pub.id, pub.post_id, pub.attempts)
                locked = (
                    Publication.query.filter(
                        Publication.id == pub.id,
                        Publication.status.in_(["scheduled", "retry"]),
                    ).update({"status": "processing", "locked_at": now_utc_naive(), "locked_by": worker_id})
                )
                db.session.commit()
                if not locked:
                    logger.info("[queue] Публикация id=%s уже захвачена другим воркером", pub.id)
                    continue

                refreshed = db.session.get(Publication, pub.id)
                if not refreshed:
                    logger.error("[queue] Публикация id=%s не найдена после захвата", pub.id)
                    continue
                if refreshed.telegram_message_id:
                    logger.info("[queue] Публикация id=%s уже отправлена ранее, помечаем sent", refreshed.id)
                    refreshed.status = "sent"
                    refreshed.sent_at = now_utc_naive()
                    db.session.commit()
                    continue

                logger.info("[queue] Отправка публикации id=%s в Telegram", refreshed.id)
                result = send_publication(refreshed)
                if result.ok:
                    logger.info("[queue] Успешная отправка публикации id=%s message_id=%s", refreshed.id, result.message_id)
                    refreshed.status = "sent"
                    refreshed.telegram_message_id = result.message_id
                    refreshed.sent_at = now_utc_naive()
                    refreshed.last_error = None
                    refreshed.locked_at = None
                    refreshed.locked_by = None
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
                    refreshed.locked_by = None
                    logger.error(
                        "[queue] Ошибка отправки публикации id=%s: %s (retryable=%s, attempts=%s)",
                        refreshed.id,
                        result.error,
                        result.retryable,
                        refreshed.attempts,
                    )
                    if (not result.retryable) or refreshed.attempts >= max_attempts:
                        refreshed.status = "failed"
                        refreshed.post.status = "failed"
                        log_action("publication", refreshed.id, "fail", {"error": result.error})
                        logger.error("[queue] Публикация id=%s переведена в failed", refreshed.id)
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
                        logger.info(
                            "[queue] Публикация id=%s переведена в retry, повтор через %s сек",
                            refreshed.id,
                            retry_delay,
                        )
                db.session.commit()

            time.sleep(interval)
