import asyncio
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from aiogram import Bot
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)


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

    def _run(self, coroutine: Any) -> Any:
        return asyncio.run(coroutine)

    async def _execute(self, method: str, payload: dict[str, Any]) -> SendResult:
        bot = Bot(token=self.token)
        try:
            if method == "sendMessage":
                message = await bot.send_message(**payload)
                return SendResult(ok=True, message_id=str(message.message_id))
            if method == "sendPhoto":
                message = await bot.send_photo(**payload)
                return SendResult(ok=True, message_id=str(message.message_id))
            if method == "sendVideo":
                message = await bot.send_video(**payload)
                return SendResult(ok=True, message_id=str(message.message_id))
            if method == "sendDocument":
                message = await bot.send_document(**payload)
                return SendResult(ok=True, message_id=str(message.message_id))
            if method == "sendMediaGroup":
                messages = await bot.send_media_group(**payload)
                if not messages:
                    return SendResult(ok=False, error="Telegram вернул пустой media group ответ", retryable=True)
                return SendResult(ok=True, message_id=str(messages[0].message_id))
            if method == "pinChatMessage":
                await bot.pin_chat_message(**payload)
                return SendResult(ok=True)
            return SendResult(ok=False, error=f"Unsupported Telegram method: {method}", retryable=False)
        except TelegramRetryAfter as exc:
            return SendResult(ok=False, error=str(exc), retry_after_seconds=exc.retry_after, retryable=True)
        except (TelegramBadRequest, TelegramUnauthorizedError, TelegramForbiddenError, TelegramNotFound) as exc:
            return self._parse_tg_error(exc, retryable=False)
        except (TelegramNetworkError, TelegramAPIError) as exc:
            return self._parse_tg_error(exc, retryable=True)
        finally:
            await bot.session.close()

    def _parse_tg_error(self, exc: Exception, retryable: bool) -> SendResult:
        error_text = str(exc)
        if "bot is not a member of the channel chat" in error_text.lower():
            error_text = (
                "Telegram отклонил отправку: бот не может писать в этот канал. "
                "Проверьте chat_id, что бот добавлен именно в целевой канал и назначен администратором с правом публикации."
            )
        return SendResult(ok=False, error=error_text, retryable=retryable)

    def send_message(self, payload: dict[str, Any]) -> SendResult:
        return self._run(self._execute("sendMessage", self._prepare_payload(payload)))

    def send_photo(self, payload: dict[str, Any]) -> SendResult:
        return self._run(self._execute("sendPhoto", self._prepare_payload(payload)))

    def send_video(self, payload: dict[str, Any]) -> SendResult:
        return self._run(self._execute("sendVideo", self._prepare_payload(payload)))

    def send_document(self, payload: dict[str, Any]) -> SendResult:
        return self._run(self._execute("sendDocument", self._prepare_payload(payload)))

    def send_media_group(self, payload: dict[str, Any]) -> SendResult:
        prepared_payload = self._prepare_payload(payload)
        prepared_payload["media"] = self._prepare_media_group(prepared_payload.get("media", []))
        return self._run(self._execute("sendMediaGroup", prepared_payload))

    def pin_message(self, chat_id: str, message_id: int) -> None:
        self._run(self._execute("pinChatMessage", {"chat_id": chat_id, "message_id": message_id}))

    def _prepare_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        prepared = {**payload}
        reply_markup = prepared.get("reply_markup")
        if isinstance(reply_markup, dict):
            prepared["reply_markup"] = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=button["text"], url=button["url"]) for button in row]
                    for row in reply_markup.get("inline_keyboard", [])
                ]
            )
        return prepared

    def _prepare_media_group(self, media_items: list[dict[str, Any]]) -> list[InputMediaPhoto | InputMediaVideo | InputMediaDocument]:
        prepared: list[InputMediaPhoto | InputMediaVideo | InputMediaDocument] = []
        for item in media_items:
            media_type = normalize_media_type(item.get("type"))
            common = {
                "media": item.get("media"),
                "caption": item.get("caption"),
                "parse_mode": item.get("parse_mode"),
            }
            if media_type == "video":
                prepared.append(InputMediaVideo(**common))
            elif media_type == "document":
                prepared.append(InputMediaDocument(**common))
            else:
                prepared.append(InputMediaPhoto(**common))
        return prepared


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

    async def _verify() -> tuple[bool, str]:
        bot = Bot(token=bot_token)
        try:
            chat = await bot.get_chat(chat_id)
            chat_title = chat.title or chat.username or str(chat.id)

            me = await bot.get_me()
            if not me.id:
                return False, "Ошибка Telegram: не удалось определить id бота"

            member = await bot.get_chat_member(chat_id=chat_id, user_id=me.id)
            status = (member.status or "").lower()
            chat_type = (chat.type or "").lower()

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
        except TelegramBadRequest as exc:
            description = str(exc)
            hint = ""
            if "chat not found" in description.lower():
                hint = " Проверьте chat_id/username и что бот добавлен в канал/группу."
            elif "forbidden" in description.lower():
                hint = " Проверьте права бота на отправку сообщений."
            return False, f"Ошибка Telegram: {description}.{hint}".strip()
        except TelegramForbiddenError as exc:
            return False, f"Ошибка Telegram: {exc}. Проверьте права бота на отправку сообщений."
        except TelegramUnauthorizedError as exc:
            return False, f"Ошибка Telegram при проверке бота: {exc}"
        except TelegramNetworkError as exc:
            return False, f"Сетевая ошибка при проверке канала: {exc}"
        except TelegramAPIError as exc:
            return False, f"Ошибка Telegram: {exc}"
        finally:
            await bot.session.close()

    return client._run(_verify())


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
