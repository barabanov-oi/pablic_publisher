import os
import time
import logging
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
                "locked_by": worker_id,
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


def run_worker(app: Flask) -> None:
    worker_id = f"worker-{os.getpid()}"
    max_attempts = app.config["MAX_ATTEMPTS"]
    default_retry_minutes = app.config["DEFAULT_RETRY_MINUTES"]
    interval = app.config["WORKER_INTERVAL_SECONDS"]

    with app.app_context():
        logger.info(
            "[worker] Воркер запущен: worker_id=%s interval=%s max_attempts=%s default_retry_minutes=%s",
            worker_id,
            interval,
            max_attempts,
            default_retry_minutes,
        )
        while True:
            logger.info("[worker] Старт итерации обработки: worker_id=%s", worker_id)
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
            logger.info("[worker] Найдено публикаций к отправке: worker_id=%s count=%s", worker_id, len(due))

            for pub in due:
                logger.info(
                    "[worker] Попытка захвата публикации: worker_id=%s publication_id=%s current_status=%s attempts=%s",
                    worker_id,
                    pub.id,
                    pub.status,
                    pub.attempts,
                )
                locked = (
                    Publication.query.filter(
                        Publication.id == pub.id,
                        Publication.status.in_(["scheduled", "retry"]),
                    ).update({"status": "processing", "locked_at": now_utc_naive(), "locked_by": worker_id})
                )
                db.session.commit()
                if not locked:
                    logger.info(
                        "[worker] Публикация уже захвачена другим воркером: worker_id=%s publication_id=%s",
                        worker_id,
                        pub.id,
                    )
                    continue

                refreshed = db.session.get(Publication, pub.id)
                if not refreshed:
                    logger.warning(
                        "[worker] Публикация не найдена после захвата: worker_id=%s publication_id=%s",
                        worker_id,
                        pub.id,
                    )
                    continue
                if refreshed.telegram_message_id:
                    logger.info(
                        "[worker] Публикация уже отправлена ранее, помечаем sent: worker_id=%s publication_id=%s message_id=%s",
                        worker_id,
                        refreshed.id,
                        refreshed.telegram_message_id,
                    )
                    refreshed.status = "sent"
                    refreshed.sent_at = now_utc_naive()
                    db.session.commit()
                    continue

                logger.info(
                    "[worker] Отправка публикации в Telegram: worker_id=%s publication_id=%s",
                    worker_id,
                    refreshed.id,
                )
                result = send_publication(refreshed)
                if result.ok:
                    logger.info(
                        "[worker] Публикация успешно отправлена: worker_id=%s publication_id=%s message_id=%s",
                        worker_id,
                        refreshed.id,
                        result.message_id,
                    )
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
                        logger.info(
                            "[worker] Все публикации поста отправлены, обновляем статус поста: worker_id=%s post_id=%s",
                            worker_id,
                            refreshed.post_id,
                        )
                        refreshed.post.status = "sent"
                else:
                    logger.error(
                        "[worker] Ошибка отправки публикации: worker_id=%s publication_id=%s error=%s retryable=%s",
                        worker_id,
                        refreshed.id,
                        result.error,
                        result.retryable,
                    )
                    refreshed.attempts += 1
                    refreshed.last_error = result.error
                    refreshed.locked_at = None
                    refreshed.locked_by = worker_id
                    if (not result.retryable) or refreshed.attempts >= max_attempts:
                        logger.error(
                            "[worker] Публикация помечена failed: worker_id=%s publication_id=%s attempts=%s",
                            worker_id,
                            refreshed.id,
                            refreshed.attempts,
                        )
                        refreshed.status = "failed"
                        refreshed.post.status = "failed"
                        log_action("publication", refreshed.id, "fail", {"error": result.error})
                    else:
                        retry_delay = max(default_retry_minutes * 60, int(result.retry_after_seconds or 0))
                        logger.info(
                            "[worker] Запланирован ретрай публикации: worker_id=%s publication_id=%s delay_seconds=%s",
                            worker_id,
                            refreshed.id,
                            retry_delay,
                        )
                        refreshed.status = "retry"
                        refreshed.ready_at = now_utc_naive() + timedelta(seconds=retry_delay)
                        log_action(
                            "publication",
                            refreshed.id,
                            "retry",
                            {"error": result.error, "delay_seconds": retry_delay},
                        )
                db.session.commit()
                logger.info(
                    "[worker] Состояние публикации сохранено: worker_id=%s publication_id=%s new_status=%s attempts=%s",
                    worker_id,
                    refreshed.id,
                    refreshed.status,
                    refreshed.attempts,
                )

            logger.info("[worker] Итерация завершена, сон %s секунд: worker_id=%s", interval, worker_id)
            time.sleep(interval)
