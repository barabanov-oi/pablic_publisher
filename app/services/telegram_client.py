import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests


@dataclass
class SendResult:
    ok: bool
    message_id: str | None = None
    error: str | None = None
    retry_after_seconds: int | None = None
    retryable: bool = True


class TelegramClient:
    def __init__(self, token: str, timeout: int = 20) -> None:
        self.token = token
        self.timeout = timeout

    def request(self, method: str, payload: dict[str, Any]) -> requests.Response:
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        return requests.post(url, json=payload, timeout=self.timeout)

    def parse_tg_error(self, response: requests.Response, data: dict[str, Any]) -> SendResult:
        error_text = data.get("description") or response.text
        params = data.get("parameters") or {}
        retry_after = int(params["retry_after"]) if "retry_after" in params else None

        retryable = response.status_code not in {400, 401, 403, 404}
        if response.status_code == 429:
            retryable = True

        if "bot is not a member of the channel chat" in (error_text or "").lower():
            error_text = (
                "Telegram отклонил отправку: бот не может писать в этот канал. "
                "Проверьте chat_id, что бот добавлен именно в целевой канал и назначен администратором с правом публикации."
            )
        return SendResult(ok=False, error=error_text, retry_after_seconds=retry_after, retryable=retryable)

    def send_message(self, payload: dict[str, Any]) -> SendResult:
        response = self.request("sendMessage", payload)
        data = response.json()
        if response.ok and data.get("ok"):
            return SendResult(ok=True, message_id=str(data["result"]["message_id"]))
        return self.parse_tg_error(response, data)

    def send_photo(self, payload: dict[str, Any]) -> SendResult:
        return self._send_media_single("sendPhoto", payload)

    def send_video(self, payload: dict[str, Any]) -> SendResult:
        return self._send_media_single("sendVideo", payload)

    def send_document(self, payload: dict[str, Any]) -> SendResult:
        return self._send_media_single("sendDocument", payload)

    def _send_media_single(self, method: str, payload: dict[str, Any]) -> SendResult:
        response = self.request(method, payload)
        data = response.json()
        if response.ok and data.get("ok"):
            return SendResult(ok=True, message_id=str(data["result"]["message_id"]))
        return self.parse_tg_error(response, data)

    def send_media_group(self, payload: dict[str, Any]) -> SendResult:
        response = self.request("sendMediaGroup", payload)
        data = response.json()
        if response.ok and data.get("ok"):
            return SendResult(ok=True, message_id=str(data["result"][0]["message_id"]))
        return self.parse_tg_error(response, data)

    def pin_message(self, chat_id: str, message_id: int) -> None:
        self.request("pinChatMessage", {"chat_id": chat_id, "message_id": message_id})


def normalize_media_type(raw_type: str | None) -> str:
    media_type = (raw_type or "photo").strip().lower()
    aliases = {"image": "photo", "img": "photo", "gif": "document", "file": "document"}
    media_type = aliases.get(media_type, media_type)
    return media_type if media_type in {"photo", "video", "document"} else "photo"


def normalize_chat_id(raw_chat_id: str) -> str:
    value = (raw_chat_id or "").strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = value.removeprefix(prefix)
    if value.startswith("@"):
        return value
    if value.lstrip("-").isdigit():
        return value
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", value):
        return f"@{value}"
    return value


def verify_channel_access(bot_token: str, chat_id: str) -> tuple[bool, str]:
    client = TelegramClient(bot_token)
    try:
        response = client.request("getChat", {"chat_id": chat_id})
        data = response.json()
    except requests.RequestException as exc:
        return False, f"Сетевая ошибка при проверке канала: {exc}"
    except ValueError:
        return False, "Некорректный ответ Telegram при проверке канала"

    if response.ok and data.get("ok"):
        chat = data.get("result") or {}
        chat_title = chat.get("title") or chat.get("username") or str(chat.get("id", chat_id))
        try:
            me_response = client.request("getMe", {})
            me_data = me_response.json()
            if not (me_response.ok and me_data.get("ok")):
                descr = me_data.get("description") or me_response.text or "Unknown error"
                return False, f"Ошибка Telegram при проверке бота: {descr}"

            bot_id = (me_data.get("result") or {}).get("id")
            if not bot_id:
                return False, "Ошибка Telegram: не удалось определить id бота"

            member_response = client.request("getChatMember", {"chat_id": chat_id, "user_id": bot_id})
            member_data = member_response.json()
            if member_response.ok and member_data.get("ok"):
                status = ((member_data.get("result") or {}).get("status") or "").lower()
                chat_type = (chat.get("type") or "").lower()
                if chat_type == "channel":
                    if status in {"creator", "administrator"}:
                        return True, f"OK: доступ подтверждён ({chat_title})"
                    return False, (
                        "Для публикации в канале бот должен быть администратором. "
                        f"Текущий статус: '{status or 'unknown'}'. Выдайте боту права на публикацию."
                    )
                if status in {"left", "kicked", "restricted"}:
                    return False, (
                        "Бот не может публиковать в этом чате: "
                        f"статус '{status}'. Проверьте, что бот добавлен и имеет права на отправку сообщений."
                    )
                return True, f"OK: доступ подтверждён ({chat_title})"

            descr = member_data.get("description") or member_response.text or "Unknown error"
            return False, f"Не удалось проверить членство бота в чате. Telegram: {descr}"
        except requests.RequestException as exc:
            return False, f"Сетевая ошибка при проверке членства бота: {exc}"
        except ValueError:
            return False, "Некорректный ответ Telegram при проверке членства бота"

    description = data.get("description") or response.text or "Unknown error"
    hint = ""
    if "chat not found" in description.lower():
        hint = " Проверьте chat_id/username и что бот добавлен в канал/группу."
    elif "forbidden" in description.lower():
        hint = " Проверьте права бота на отправку сообщений."
    return False, f"Ошибка Telegram: {description}.{hint}".strip()


def build_inline_keyboard(buttons: list[dict[str, str]]) -> dict[str, Any] | None:
    if not buttons:
        return None
    rows = []
    for button in buttons:
        text = button.get("text")
        url = button.get("url")
        if text and url:
            rows.append([{"text": text, "url": url}])
    return {"inline_keyboard": rows} if rows else None


def extract_domain(href: str) -> str:
    return (urlparse(href).netloc or "").lower()
