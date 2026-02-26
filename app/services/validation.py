import re
from html.parser import HTMLParser
from urllib.parse import urlparse

from app.models import BlacklistRule, Post
from app.services.json_fields import JsonFieldError, parse_post_payload


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def validate_post(post: Post) -> tuple[bool, str | None]:
    if len(post.body_html or "") > 4096:
        return False, "Длина текста превышает 4096 символов"

    try:
        payload = parse_post_payload(post.media, post.buttons, post.options)
    except JsonFieldError as exc:
        return False, str(exc)

    if len(payload.media) > 10:
        return False, "Допускается максимум 10 медиа-файлов"

    parser = LinkExtractor()
    parser.feed(post.body_html or "")

    rules = BlacklistRule.query.filter_by(is_enabled=True).all()
    text_lower = (post.body_html or "").lower()

    for href in parser.links:
        parsed = urlparse(href)
        if parsed.scheme not in {"http", "https"}:
            return False, f"Недопустимая схема ссылки: {href}"

    for rule in rules:
        pattern = rule.pattern.strip()
        if rule.type == "word" and pattern.lower() in text_lower:
            return False, f"Обнаружено запрещённое слово: {pattern}"
        if rule.type == "domain":
            for href in parser.links:
                domain = (urlparse(href).netloc or "").lower()
                if pattern.lower() in domain:
                    return False, f"Обнаружен запрещённый домен: {pattern}"
        if rule.type == "regex" and re.search(pattern, post.body_html or "", flags=re.IGNORECASE):
            return False, f"Совпадение с regex-правилом: {pattern}"

    return True, None
