from datetime import timedelta
import logging

from app.models import Publication
from app.services.json_fields import JsonFieldError, PostPayload, parse_post_payload
from app.services.telegram_client import (
    SendResult,
    TelegramClient,
    build_inline_keyboard,
    normalize_chat_id,
    normalize_media_type,
)
from app.utils.timezone import now_utc_naive


logger = logging.getLogger(__name__)


def _build_base_payload(publication: Publication, payload: PostPayload) -> dict:
    channel = publication.post.channel
    return {
        "chat_id": normalize_chat_id(channel.telegram_chat_id),
        "disable_notification": bool(payload.options.get("disable_notification", False)),
        "protect_content": bool(payload.options.get("protect_content", False)),
    }


def _pin_if_requested(
    client: TelegramClient,
    publication: Publication,
    base_payload: dict,
    options: dict,
    message_id: str | None,
) -> None:
    if not (options.get("pin") and message_id):
        return

    logger.info("[posting] Закрепление сообщения publication_id=%s message_id=%s", publication.id, message_id)
    pin_result = client.pin_message(base_payload["chat_id"], int(message_id))
    if not pin_result.ok:
        logger.warning(
            "[posting] Не удалось закрепить сообщение publication_id=%s message_id=%s: %s",
            publication.id,
            message_id,
            pin_result.error,
        )


def _send_text_only(
    client: TelegramClient,
    publication: Publication,
    payload: PostPayload,
    base_payload: dict,
    keyboard: dict | None,
) -> SendResult:
    post = publication.post
    message_payload = {
        **base_payload,
        "text": post.body_html or "",
        "parse_mode": "HTML",
        "disable_web_page_preview": bool(payload.options.get("disable_preview", False)),
    }
    if keyboard:
        message_payload["reply_markup"] = keyboard

    logger.info("[posting] Отправка текстового сообщения publication_id=%s", publication.id)
    return client.send_message(message_payload)


def _send_single_media(
    client: TelegramClient,
    publication: Publication,
    payload: PostPayload,
    base_payload: dict,
    keyboard: dict | None,
) -> SendResult:
    post = publication.post
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
    return method(media_payload)


def _send_media_group(
    client: TelegramClient,
    publication: Publication,
    payload: PostPayload,
    base_payload: dict,
    keyboard: dict | None,
) -> SendResult:
    post = publication.post
    group: list[dict] = []
    for idx, item in enumerate(payload.media):
        group_item = {"type": normalize_media_type(item.get("type")), "media": item.get("url")}
        if idx == 0 and post.body_html:
            group_item["caption"] = post.body_html
            group_item["parse_mode"] = "HTML"
        group.append(group_item)

    logger.info("[posting] Отправка группы медиа publication_id=%s items=%s", publication.id, len(group))
    group_result = client.send_media_group({**base_payload, "media": group})
    if not group_result.ok:
        return group_result

    final_message_id = group_result.message_id
    if keyboard:
        # Для media_group Telegram не поддерживает inline-кнопки, отправляем follow-up сообщение.
        follow_payload = {
            **base_payload,
            "text": post.body_html or "Подробнее:",
            "parse_mode": "HTML",
            "disable_web_page_preview": bool(payload.options.get("disable_preview", False)),
            "reply_markup": keyboard,
        }
        logger.info("[posting] Отправка follow-up сообщения для группы publication_id=%s", publication.id)
        follow_result = client.send_message(follow_payload)
        if not follow_result.ok:
            return follow_result
        final_message_id = follow_result.message_id

    return SendResult(ok=True, message_id=final_message_id)


def send_publication(publication: Publication) -> SendResult:
    post = publication.post
    channel = post.channel

    logger.info(
        "[posting] Начало отправки publication_id=%s post_id=%s channel_id=%s",
        publication.id,
        post.id,
        channel.id,
    )

    try:
        payload = parse_post_payload(post.media, post.buttons, post.options)
    except JsonFieldError as exc:
        logger.error("[posting] Ошибка JSON-полей publication_id=%s: %s", publication.id, exc)
        return SendResult(ok=False, error=str(exc), retryable=False)

    keyboard = build_inline_keyboard(payload.buttons)
    base_payload = _build_base_payload(publication, payload)
    client = TelegramClient(channel.bot_token)

    try:
        media_count = len(payload.media)
        logger.info("[posting] Подготовлено медиа publication_id=%s count=%s", publication.id, media_count)

        if media_count == 0:
            result = _send_text_only(client, publication, payload, base_payload, keyboard)
        elif media_count == 1:
            result = _send_single_media(client, publication, payload, base_payload, keyboard)
        else:
            result = _send_media_group(client, publication, payload, base_payload, keyboard)

        if not result.ok:
            logger.error("[posting] Ошибка отправки publication_id=%s: %s", publication.id, result.error)
            return result

        _pin_if_requested(client, publication, base_payload, payload.options, result.message_id)
        logger.info(
            "[posting] Публикация успешно отправлена publication_id=%s message_id=%s",
            publication.id,
            result.message_id,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception("[posting] Непредвиденная ошибка отправки publication_id=%s", publication.id)
        return SendResult(ok=False, error=f"unexpected_error: {exc}")


def get_retry_ready_at(default_retry_minutes: int, retry_after_seconds: int | None) -> tuple[str, object]:
    retry_delay = max(default_retry_minutes * 60, int(retry_after_seconds or 0))
    return "retry", now_utc_naive() + timedelta(seconds=retry_delay)
