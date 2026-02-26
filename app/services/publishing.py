from datetime import timedelta

import requests

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


def send_publication(publication: Publication) -> SendResult:
    post = publication.post
    channel = post.channel

    try:
        payload = parse_post_payload(post.media, post.buttons, post.options)
    except JsonFieldError as exc:
        return SendResult(ok=False, error=str(exc), retryable=False)

    keyboard = build_inline_keyboard(payload.buttons)
    base_payload = {
        "chat_id": normalize_chat_id(channel.telegram_chat_id),
        "disable_notification": bool(payload.options.get("disable_notification", False)),
        "protect_content": bool(payload.options.get("protect_content", False)),
    }
    client = TelegramClient(channel.bot_token)

    try:
        if len(payload.media) == 0:
            message_payload = {
                **base_payload,
                "text": post.body_html,
                "parse_mode": "HTML",
                "disable_web_page_preview": bool(payload.options.get("disable_preview", False)),
            }
            if keyboard:
                message_payload["reply_markup"] = keyboard
            return client.send_message(message_payload)

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
            result = method(media_payload)
            if result.ok and payload.options.get("pin") and result.message_id:
                client.pin_message(base_payload["chat_id"], int(result.message_id))
            return result

        group = []
        for idx, item in enumerate(payload.media):
            group_item = {"type": normalize_media_type(item.get("type")), "media": item.get("url")}
            if idx == 0 and post.body_html:
                group_item["caption"] = post.body_html
                group_item["parse_mode"] = "HTML"
            group.append(group_item)

        result = client.send_media_group({**base_payload, "media": group})
        if not result.ok:
            return result

        message_id = result.message_id
        if keyboard:
            btn_result = client.send_message({**base_payload, "text": "Подробнее:", "reply_markup": keyboard})
            if btn_result.ok:
                message_id = btn_result.message_id

        if payload.options.get("pin") and message_id:
            client.pin_message(base_payload["chat_id"], int(message_id))

        return SendResult(ok=True, message_id=message_id)
    except requests.RequestException as exc:
        return SendResult(ok=False, error=f"network_error: {exc}")
    except Exception as exc:  # noqa: BLE001
        return SendResult(ok=False, error=f"unexpected_error: {exc}")


def get_retry_ready_at(default_retry_minutes: int, retry_after_seconds: int | None) -> tuple[str, object]:
    retry_delay = max(default_retry_minutes * 60, int(retry_after_seconds or 0))
    return "retry", now_utc_naive() + timedelta(seconds=retry_delay)
