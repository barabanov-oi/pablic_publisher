import json
from dataclasses import dataclass
from typing import Any


class JsonFieldError(ValueError):
    pass


@dataclass(slots=True)
class PostPayload:
    media: list[dict[str, Any]]
    buttons: list[dict[str, str]]
    options: dict[str, Any]


def parse_json_field(raw: str, default: Any, field_name: str) -> Any:
    if not (raw or "").strip():
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise JsonFieldError(f"Некорректный JSON в поле {field_name}") from exc


def parse_post_payload(media_raw: str, buttons_raw: str, options_raw: str) -> PostPayload:
    media = parse_json_field(media_raw, [], "media")
    buttons = parse_json_field(buttons_raw, [], "buttons")
    options = parse_json_field(options_raw, {}, "options")

    if not isinstance(media, list):
        raise JsonFieldError("Поле media должно быть JSON-массивом")
    if not isinstance(buttons, list):
        raise JsonFieldError("Поле buttons должно быть JSON-массивом")
    if not isinstance(options, dict):
        raise JsonFieldError("Поле options должно быть JSON-объектом")

    return PostPayload(media=media, buttons=buttons, options=options)
