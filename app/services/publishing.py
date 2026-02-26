from datetime import timedelta
import logging

from app.models import Publication
from app.services.json_fields import JsonFieldError, parse_post_payload
from app.services.telegram_client import (
    SendResult,
    TelegramClient,
    build_inline_keyboard,
    normalize_chat_id,
    normalize_media_type,
)
from app.utils.timezone import now_utc_naive


logger = logging.getLogger(__name__)


def send_publication(publication: Publication) -> SendResult:
    post = publication.post
    channel = post.channel

    logger.info("[posting] Начало отправки publication_id=%s post_id=%s channel_id=%s", publication.id, post.id, channel.id)
    try:
        payload = parse_post_payload(post.media, post.buttons, post.options)
    except JsonFieldError as exc:
        logger.error("[posting] Ошибка JSON-полей publication_id=%s: %s", publication.id, exc)
        return SendResult(ok=False, error=str(exc), retryable=False)

    keyboard = build_inline_keyboard(payload.buttons)
    base_payload = {
        "chat_id": normalize_chat_id(channel.telegram_chat_id),
        "disable_notification": bool(payload.options.get("disable_notification", False)),
        "protect_content": bool(payload.options.get("protect_content", False)),
    }
    client = TelegramClient(channel.bot_token)

    try:
        logger.info("[posting] Подготовлено медиа: publication_id=%s count=%s", publication.id, len(payload.media))
        if len(payload.media) == 0:
            message_payload = {
                **base_payload,
                "text": post.body_html,
                "parse_mode": "HTML",
                "disable_web_page_preview": bool(payload.options.get("disable_preview", False)),
            }
            if keyboard:
                message_payload["reply_markup"] = keyboard
            logger.info("[posting] Отправка текстового сообщения publication_id=%s", publication.id)
            result = client.send_message(message_payload)
            if result.ok:
                logger.info("[posting] Текстовое сообщение отправлено publication_id=%s message_id=%s", publication.id, result.message_id)
            else:
                logger.error("[posting] Ошибка отправки текста publication_id=%s: %s", publication.id, result.error)
            return result

        if len(payload.media) == 1:
            item = payload.media[0]
            media_type = normalize_media_type(item.get("type"))
            method = {
                "photo": client.send_photo,
                "video": client.send_video,
                "document": client.send_document,
            }[media_type]
            media_payload = {**base_payload, media_type: item.get("url")}
            if post.body_html:
                media_payload["caption"] = post.body_html
                media_payload["parse_mode"] = "HTML"
            if keyboard:
                media_payload["reply_markup"] = keyboard
            logger.info("[posting] Отправка одиночного медиа publication_id=%s type=%s", publication.id, media_type)
            result = method(media_payload)
            if result.ok:
                logger.info("[posting] Одиночное медиа отправлено publication_id=%s message_id=%s", publication.id, result.message_id)
            else:
                logger.error("[posting] Ошибка отправки одиночного медиа publication_id=%s: %s", publication.id, result.error)
            if result.ok and payload.options.get("pin") and result.message_id:
                logger.info("[posting] Закрепление одиночного сообщения publication_id=%s message_id=%s", publication.id, result.message_id)
                client.pin_message(base_payload["chat_id"], int(result.message_id))
            return result

        group = []
        for idx, item in enumerate(payload.media):
            group_item = {"type": normalize_media_type(item.get("type")), "media": item.get("url")}
            if idx == 0 and post.body_html:
                group_item["caption"] = post.body_html
                group_item["parse_mode"] = "HTML"
            group.append(group_item)

        logger.info("[posting] Отправка группы медиа publication_id=%s items=%s", publication.id, len(group))
        result = client.send_media_group({**base_payload, "media": group})
        if not result.ok:
            logger.error("[posting] Ошибка отправки группы медиа publication_id=%s: %s", publication.id, result.error)
            return result

        logger.info("[posting] Группа медиа отправлена publication_id=%s first_message_id=%s", publication.id, result.message_id)

        message_id = result.message_id
        if keyboard:
            logger.info("[posting] Отправка сообщения с кнопками для группы publication_id=%s", publication.id)
            btn_result = client.send_message({**base_payload, "text": "Подробнее:", "reply_markup": keyboard})
            if btn_result.ok:
                logger.info("[posting] Сообщение с кнопками отправлено publication_id=%s message_id=%s", publication.id, btn_result.message_id)
                message_id = btn_result.message_id
            else:
                logger.error("[posting] Ошибка отправки кнопок для группы publication_id=%s: %s", publication.id, btn_result.error)

        if payload.options.get("pin") and message_id:
            logger.info("[posting] Закрепление сообщения группы publication_id=%s message_id=%s", publication.id, message_id)
            client.pin_message(base_payload["chat_id"], int(message_id))

        logger.info("[posting] Успешное завершение отправки publication_id=%s result_message_id=%s", publication.id, message_id)
        return SendResult(ok=True, message_id=message_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[posting] Непредвиденная ошибка отправки publication_id=%s", publication.id)
        return SendResult(ok=False, error=f"unexpected_error: {exc}")


def get_retry_ready_at(default_retry_minutes: int, retry_after_seconds: int | None) -> tuple[str, object]:
    retry_delay = max(default_retry_minutes * 60, int(retry_after_seconds or 0))
    return "retry", now_utc_naive() + timedelta(seconds=retry_delay)
