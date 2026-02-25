import csv
import io
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, time as dt_time
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

import requests
from flask import Flask, flash, redirect, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func


db = SQLAlchemy()
MAX_ATTEMPTS = 5
DEFAULT_RETRY_MINUTES = 30
WORKER_INTERVAL_SECONDS = 20
MOSCOW_OFFSET = timedelta(hours=3)


def utc_now() -> datetime:
    """Возвращает текущее UTC-время без tzinfo для совместимости с существующей схемой БД."""
    return datetime.now(UTC).replace(tzinfo=None)


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attr_dict = dict(attrs)
        href = attr_dict.get("href")
        if href:
            self.links.append(href)


class Channel(db.Model):
    __tablename__ = "channels"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), nullable=False)
    telegram_chat_id = db.Column(db.String(255), nullable=False)
    timezone = db.Column(db.String(64), default="Europe/Moscow", nullable=False)
    bot_token = db.Column(db.String(255), nullable=False)
    daily_time = db.Column(db.String(5), default="10:00", nullable=False)
    allowed_window_start = db.Column(db.String(5), default="08:00", nullable=False)
    allowed_window_end = db.Column(db.String(5), default="22:00", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class Post(db.Model):
    __tablename__ = "posts"

    id = db.Column(db.Integer, primary_key=True)
    channel_id = db.Column(db.Integer, db.ForeignKey("channels.id"), nullable=False)
    title = db.Column(db.String(255), nullable=False)
    body_html = db.Column(db.Text, nullable=False, default="")
    media = db.Column(db.Text, nullable=False, default="[]")
    buttons = db.Column(db.Text, nullable=False, default="[]")
    options = db.Column(db.Text, nullable=False, default="{}")
    blacklist_check_status = db.Column(db.String(32), default="ok", nullable=False)
    blacklist_reason = db.Column(db.Text)
    status = db.Column(db.String(32), default="draft", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    channel = db.relationship("Channel", backref=db.backref("posts", lazy=True))


class Publication(db.Model):
    __tablename__ = "publications"

    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey("posts.id"), nullable=False)
    planned_at = db.Column(db.DateTime, nullable=False)
    ready_at = db.Column(db.DateTime, nullable=False)
    status = db.Column(db.String(32), default="scheduled", nullable=False)
    attempts = db.Column(db.Integer, default=0, nullable=False)
    locked_at = db.Column(db.DateTime)
    locked_by = db.Column(db.String(128))
    telegram_message_id = db.Column(db.String(64))
    sent_at = db.Column(db.DateTime)
    last_error = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    post = db.relationship("Post", backref=db.backref("publications", lazy=True))


class BlacklistRule(db.Model):
    __tablename__ = "blacklist_rules"

    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(32), nullable=False)  # word/domain/regex
    pattern = db.Column(db.String(255), nullable=False)
    is_enabled = db.Column(db.Boolean, default=True, nullable=False)


class AuditLog(db.Model):
    __tablename__ = "audit_log"

    id = db.Column(db.Integer, primary_key=True)
    entity_type = db.Column(db.String(32), nullable=False)
    entity_id = db.Column(db.Integer, nullable=False)
    action = db.Column(db.String(64), nullable=False)
    meta = db.Column(db.Text, default="{}", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


@dataclass
class SendResult:
    ok: bool
    message_id: str | None = None
    error: str | None = None
    retry_after_seconds: int | None = None
    retryable: bool = True


def log_action(entity_type: str, entity_id: int, action: str, meta: dict[str, Any] | None = None) -> None:
    db.session.add(AuditLog(entity_type=entity_type, entity_id=entity_id, action=action, meta=json.dumps(meta or {}, ensure_ascii=False)))


def parse_time(value: str) -> dt_time:
    return datetime.strptime(value, "%H:%M").time()


def to_moscow_now() -> datetime:
    return utc_now() + MOSCOW_OFFSET


def moscow_to_utc(naive_moscow_dt: datetime) -> datetime:
    return naive_moscow_dt - MOSCOW_OFFSET


def utc_to_moscow(utc_dt: datetime) -> datetime:
    return utc_dt + MOSCOW_OFFSET


def calculate_next_slot(channel: Channel) -> tuple[datetime, int]:
    now_msk = to_moscow_now()
    daily = parse_time(channel.daily_time)
    base = datetime.combine(now_msk.date(), daily)
    if base <= now_msk:
        base += timedelta(days=1)

    planned_utc = moscow_to_utc(base)
    for _ in range(365):
        day_start = planned_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        day_count = (
            db.session.query(func.count(Publication.id))
            .join(Post, Post.id == Publication.post_id)
            .filter(Post.channel_id == channel.id, Publication.planned_at >= day_start, Publication.planned_at < day_end)
            .scalar()
            or 0
        )
        slot_index = int(day_count)
        candidate = planned_utc + timedelta(seconds=slot_index)
        if candidate > utc_now():
            return candidate, slot_index
        planned_utc += timedelta(days=1)

    return utc_now() + timedelta(minutes=1), 0


def adjust_to_window(channel: Channel, planned_utc: datetime) -> datetime:
    start = parse_time(channel.allowed_window_start)
    end = parse_time(channel.allowed_window_end)
    planned_msk = utc_to_moscow(planned_utc)
    current_time = planned_msk.time()

    if start <= current_time <= end:
        return planned_utc

    if current_time < start:
        adjusted_msk = datetime.combine(planned_msk.date(), start)
    else:
        adjusted_msk = datetime.combine(planned_msk.date() + timedelta(days=1), start)
    return moscow_to_utc(adjusted_msk)


def get_json_field(text: str, default: Any) -> Any:
    if not text.strip():
        return default
    return json.loads(text)


def validate_post(post: Post) -> tuple[bool, str | None]:
    if len(post.body_html or "") > 4096:
        return False, "Длина текста превышает 4096 символов"

    try:
        media = get_json_field(post.media, [])
        if len(media) > 10:
            return False, "Допускается максимум 10 медиа-файлов"
    except json.JSONDecodeError:
        return False, "Некорректный JSON в поле media"

    try:
        _ = get_json_field(post.buttons, [])
    except json.JSONDecodeError:
        return False, "Некорректный JSON в поле buttons"

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
        if rule.type == "regex":
            if re.search(pattern, post.body_html or "", flags=re.IGNORECASE):
                return False, f"Совпадение с regex-правилом: {pattern}"

    return True, None


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


def normalize_media_type(raw_type: str | None) -> str:
    media_type = (raw_type or "photo").strip().lower()
    aliases = {
        "image": "photo",
        "img": "photo",
        "gif": "document",
        "file": "document",
    }
    media_type = aliases.get(media_type, media_type)
    return media_type if media_type in {"photo", "video", "document"} else "photo"


def normalize_chat_id(raw_chat_id: str) -> str:
    value = (raw_chat_id or "").strip()
    if value.startswith("https://t.me/"):
        value = value.removeprefix("https://t.me/")
    if value.startswith("http://t.me/"):
        value = value.removeprefix("http://t.me/")
    if value.startswith("t.me/"):
        value = value.removeprefix("t.me/")
    if value.startswith("@"):
        return value
    if value.lstrip("-").isdigit():
        return value
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", value):
        return f"@{value}"
    return value


def verify_channel_access(bot_token: str, chat_id: str) -> tuple[bool, str]:
    payload = {"chat_id": chat_id}
    try:
        response = tg_request(bot_token, "getChat", payload)
        data = response.json()
    except requests.RequestException as exc:
        return False, f"Сетевая ошибка при проверке канала: {exc}"
    except ValueError:
        return False, "Некорректный ответ Telegram при проверке канала"

    if response.ok and data.get("ok"):
        chat = data.get("result") or {}
        chat_title = chat.get("title") or chat.get("username") or str(chat.get("id", chat_id))
        return True, f"OK: доступ подтверждён ({chat_title})"

    description = data.get("description") or response.text or "Unknown error"
    hint = ""
    if "chat not found" in description.lower():
        hint = " Проверьте chat_id/username и что бот добавлен в канал/группу."
    elif "forbidden" in description.lower():
        hint = " Проверьте права бота на отправку сообщений."
    return False, f"Ошибка Telegram: {description}.{hint}".strip()


def tg_request(token: str, method: str, payload: dict[str, Any]) -> requests.Response:
    url = f"https://api.telegram.org/bot{token}/{method}"
    return requests.post(url, json=payload, timeout=20)


def send_to_telegram(publication: Publication) -> SendResult:
    post = publication.post
    channel = post.channel

    media = get_json_field(post.media, [])
    buttons = get_json_field(post.buttons, [])
    options = get_json_field(post.options, {})
    keyboard = build_inline_keyboard(buttons)

    base_payload = {
        "chat_id": normalize_chat_id(channel.telegram_chat_id),
        "disable_notification": bool(options.get("disable_notification", False)),
        "protect_content": bool(options.get("protect_content", False)),
    }

    try:
        if len(media) == 0:
            payload = {
                **base_payload,
                "text": post.body_html,
                "parse_mode": "HTML",
                "disable_web_page_preview": bool(options.get("disable_preview", False)),
            }
            if keyboard:
                payload["reply_markup"] = keyboard
            response = tg_request(channel.bot_token, "sendMessage", payload)
            response_data = response.json()
            if response.ok and response_data.get("ok"):
                return SendResult(ok=True, message_id=str(response_data["result"]["message_id"]))
            return parse_tg_error(response, response_data)

        if len(media) == 1:
            item = media[0]
            media_type = normalize_media_type(item.get("type"))
            method = {"photo": "sendPhoto", "video": "sendVideo", "document": "sendDocument"}[media_type]
            payload = {
                **base_payload,
                media_type: item.get("url"),
            }
            if post.body_html:
                payload["caption"] = post.body_html
                payload["parse_mode"] = "HTML"
            if keyboard:
                payload["reply_markup"] = keyboard
            response = tg_request(channel.bot_token, method, payload)
            response_data = response.json()
            if response.ok and response_data.get("ok"):
                msg_id = str(response_data["result"]["message_id"])
                if options.get("pin"):
                    tg_request(channel.bot_token, "pinChatMessage", {"chat_id": normalize_chat_id(channel.telegram_chat_id), "message_id": int(msg_id)})
                return SendResult(ok=True, message_id=msg_id)
            return parse_tg_error(response, response_data)

        group = []
        for idx, item in enumerate(media):
            group_item = {"type": normalize_media_type(item.get("type")), "media": item.get("url")}
            if idx == 0 and post.body_html:
                group_item["caption"] = post.body_html
                group_item["parse_mode"] = "HTML"
            group.append(group_item)

        response = tg_request(channel.bot_token, "sendMediaGroup", {**base_payload, "media": group})
        response_data = response.json()
        if not (response.ok and response_data.get("ok")):
            return parse_tg_error(response, response_data)

        first_message_id = str(response_data["result"][0]["message_id"])
        if keyboard:
            msg_response = tg_request(channel.bot_token, "sendMessage", {
                **base_payload,
                "text": "Подробнее:",
                "reply_markup": keyboard,
            })
            if msg_response.ok and msg_response.json().get("ok"):
                first_message_id = str(msg_response.json()["result"]["message_id"])

        if options.get("pin"):
            tg_request(channel.bot_token, "pinChatMessage", {"chat_id": normalize_chat_id(channel.telegram_chat_id), "message_id": int(first_message_id)})

        return SendResult(ok=True, message_id=first_message_id)
    except requests.RequestException as exc:
        return SendResult(ok=False, error=f"network_error: {exc}")
    except Exception as exc:  # noqa: BLE001
        return SendResult(ok=False, error=f"unexpected_error: {exc}")


def parse_tg_error(response: requests.Response, data: dict[str, Any]) -> SendResult:
    error_text = data.get("description") or response.text
    retry_after = None
    retryable = True
    params = data.get("parameters") or {}
    if "retry_after" in params:
        retry_after = int(params["retry_after"])
    if response.status_code in {400, 401, 403, 404}:
        retryable = False
    if response.status_code == 429:
        retryable = True
    return SendResult(ok=False, error=error_text, retry_after_seconds=retry_after, retryable=retryable)


def run_scheduler(app: Flask) -> None:
    worker_id = f"worker-{os.getpid()}"
    with app.app_context():
        while True:
            now = utc_now()
            due = (
                Publication.query
                .filter(Publication.status.in_(["scheduled", "retry"]), Publication.ready_at <= now, Publication.attempts < MAX_ATTEMPTS)
                .order_by(Publication.ready_at.asc(), Publication.planned_at.asc(), Publication.id.asc())
                .limit(20)
                .all()
            )

            for pub in due:
                locked = (
                    Publication.query
                    .filter(Publication.id == pub.id, Publication.status.in_(["scheduled", "retry"]))
                    .update({"status": "processing", "locked_at": utc_now(), "locked_by": worker_id})
                )
                db.session.commit()
                if not locked:
                    continue

                refreshed = db.session.get(Publication, pub.id)
                if refreshed.telegram_message_id:
                    refreshed.status = "sent"
                    refreshed.sent_at = utc_now()
                    db.session.commit()
                    continue

                result = send_to_telegram(refreshed)
                if result.ok:
                    refreshed.status = "sent"
                    refreshed.telegram_message_id = result.message_id
                    refreshed.sent_at = utc_now()
                    refreshed.last_error = None
                    log_action("publication", refreshed.id, "send", {"message_id": result.message_id})

                    pending = Publication.query.filter_by(post_id=refreshed.post_id).filter(Publication.status.in_(["scheduled", "retry", "processing"])).count()
                    if pending == 0:
                        refreshed.post.status = "sent"
                else:
                    refreshed.attempts += 1
                    refreshed.last_error = result.error
                    if (not result.retryable) or refreshed.attempts >= MAX_ATTEMPTS:
                        refreshed.status = "failed"
                        refreshed.post.status = "failed"
                        log_action("publication", refreshed.id, "fail", {"error": result.error})
                    else:
                        retry_delay = max(DEFAULT_RETRY_MINUTES * 60, int(result.retry_after_seconds or 0))
                        refreshed.status = "retry"
                        refreshed.ready_at = utc_now() + timedelta(seconds=retry_delay)
                        log_action("publication", refreshed.id, "retry", {"error": result.error, "delay_seconds": retry_delay})
                db.session.commit()

            time.sleep(WORKER_INTERVAL_SECONDS)


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-key")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///publisher.db")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)

    @app.cli.command("init-db")
    def init_db_command() -> None:
        db.create_all()
        print("Database initialized")

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/channels", methods=["GET", "POST"])
    def channels():
        if request.method == "POST":
            raw_chat_id = request.form["telegram_chat_id"]
            raw_bot_token = request.form["bot_token"]
            normalized_chat_id = normalize_chat_id(raw_chat_id)
            bot_token = raw_bot_token.strip()

            is_ok, check_message = verify_channel_access(bot_token, normalized_chat_id)
            if not is_ok:
                flash(f"Канал не добавлен. {check_message}")
                return redirect(url_for("channels"))

            channel = Channel(
                title=request.form["title"],
                telegram_chat_id=normalized_chat_id,
                bot_token=bot_token,
                timezone=request.form.get("timezone") or "Europe/Moscow",
                daily_time=request.form.get("daily_time") or "10:00",
                allowed_window_start=request.form.get("allowed_window_start") or "08:00",
                allowed_window_end=request.form.get("allowed_window_end") or "22:00",
            )
            db.session.add(channel)
            db.session.commit()
            log_action("channel", channel.id, "create", {"check": check_message})
            db.session.commit()
            flash(f"Канал добавлен. {check_message}")
            return redirect(url_for("channels"))

        items = Channel.query.order_by(Channel.created_at.desc()).all()
        return render_template("channels.html", channels=items)

    @app.route("/posts")
    def posts():
        items = Post.query.order_by(Post.updated_at.desc()).all()
        return render_template("posts.html", posts=items)

    @app.route("/posts/new", methods=["GET", "POST"])
    def post_new():
        channels = Channel.query.order_by(Channel.title.asc()).all()
        if request.method == "POST":
            post = Post(
                channel_id=int(request.form["channel_id"]),
                title=request.form["title"],
                body_html=request.form.get("body_html", ""),
                media=request.form.get("media", "[]"),
                buttons=request.form.get("buttons", "[]"),
                options=request.form.get("options", "{}"),
                status="draft",
            )
            ok, reason = validate_post(post)
            post.blacklist_check_status = "ok" if ok else "blocked"
            post.blacklist_reason = reason
            db.session.add(post)
            db.session.commit()
            log_action("post", post.id, "create")
            db.session.commit()
            flash("Пост сохранён")
            return redirect(url_for("posts"))
        return render_template("post_form.html", channels=channels, post=None)

    @app.route("/posts/<int:post_id>/edit", methods=["GET", "POST"])
    def post_edit(post_id: int):
        post = db.get_or_404(Post, post_id)
        channels = Channel.query.order_by(Channel.title.asc()).all()
        if request.method == "POST":
            post.channel_id = int(request.form["channel_id"])
            post.title = request.form["title"]
            post.body_html = request.form.get("body_html", "")
            post.media = request.form.get("media", "[]")
            post.buttons = request.form.get("buttons", "[]")
            post.options = request.form.get("options", "{}")
            ok, reason = validate_post(post)
            post.blacklist_check_status = "ok" if ok else "blocked"
            post.blacklist_reason = reason
            db.session.commit()
            log_action("post", post.id, "update")
            db.session.commit()
            flash("Пост обновлён")
            return redirect(url_for("posts"))
        return render_template("post_form.html", channels=channels, post=post)

    @app.post("/posts/<int:post_id>/duplicate")
    def post_duplicate(post_id: int):
        src = db.get_or_404(Post, post_id)
        copy_post = Post(
            channel_id=src.channel_id,
            title=f"{src.title} (копия)",
            body_html=src.body_html,
            media=src.media,
            buttons=src.buttons,
            options=src.options,
            status="draft",
            blacklist_check_status=src.blacklist_check_status,
            blacklist_reason=src.blacklist_reason,
        )
        db.session.add(copy_post)
        db.session.commit()
        log_action("post", copy_post.id, "duplicate", {"source_id": src.id})
        db.session.commit()
        flash("Пост продублирован")
        return redirect(url_for("posts"))

    @app.post("/posts/<int:post_id>/cancel")
    def post_cancel(post_id: int):
        post = db.get_or_404(Post, post_id)
        post.status = "canceled"
        Publication.query.filter(Publication.post_id == post.id, Publication.status.in_(["scheduled", "retry", "processing"])).update({"status": "canceled"})
        log_action("post", post.id, "cancel")
        db.session.commit()
        flash("Пост отменён")
        return redirect(url_for("posts"))

    @app.post("/posts/<int:post_id>/schedule")
    def post_schedule(post_id: int):
        post = db.get_or_404(Post, post_id)
        ok, reason = validate_post(post)
        post.blacklist_check_status = "ok" if ok else "blocked"
        post.blacklist_reason = reason

        if not ok:
            db.session.commit()
            flash(f"Нельзя запланировать: {reason}")
            return redirect(url_for("posts"))

        planned_text = request.form.get("planned_at", "").strip()
        if planned_text:
            planned_local = datetime.strptime(planned_text, "%Y-%m-%dT%H:%M")
            planned_utc = moscow_to_utc(planned_local)
            slot_index = 0
        else:
            planned_utc, slot_index = calculate_next_slot(post.channel)

        planned_utc = adjust_to_window(post.channel, planned_utc)
        publication = Publication(post_id=post.id, planned_at=planned_utc, ready_at=planned_utc, status="scheduled")
        post.status = "scheduled"
        db.session.add(publication)
        db.session.flush()
        log_action("publication", publication.id, "schedule", {"slot_index": slot_index, "planned_at": planned_utc.isoformat()})
        db.session.commit()
        flash("Публикация запланирована")
        return redirect(url_for("publications"))

    @app.route("/publications")
    def publications():
        items = Publication.query.order_by(Publication.ready_at.asc(), Publication.planned_at.asc(), Publication.id.asc()).all()
        return render_template("publications.html", publications=items)

    @app.post("/publications/<int:pub_id>/reschedule")
    def publication_reschedule(pub_id: int):
        publication = db.get_or_404(Publication, pub_id)
        planned_text = request.form["planned_at"].strip()
        planned_local = datetime.strptime(planned_text, "%Y-%m-%dT%H:%M")
        planned_utc = adjust_to_window(publication.post.channel, moscow_to_utc(planned_local))
        publication.planned_at = planned_utc
        publication.ready_at = planned_utc
        publication.status = "scheduled"
        publication.attempts = 0
        publication.last_error = None
        publication.locked_at = None
        publication.locked_by = None
        publication.post.status = "scheduled"
        log_action("publication", publication.id, "reschedule", {"planned_at": planned_utc.isoformat()})
        db.session.commit()
        flash("Публикация перепланирована")
        return redirect(url_for("publications"))

    @app.post("/publications/<int:pub_id>/retry-now")
    def publication_retry_now(pub_id: int):
        publication = db.get_or_404(Publication, pub_id)
        publication.status = "retry"
        publication.ready_at = utc_now()
        publication.attempts = 0
        publication.last_error = None
        publication.post.status = "queued"
        log_action("publication", publication.id, "retry_now")
        db.session.commit()
        flash("Публикация поставлена на немедленную переотправку")
        return redirect(url_for("publications"))

    @app.route("/reports")
    def reports():
        rows = (
            db.session.query(Publication, Post, Channel)
            .join(Post, Publication.post_id == Post.id)
            .join(Channel, Post.channel_id == Channel.id)
            .order_by(Publication.created_at.desc())
            .all()
        )
        error_rows = (
            db.session.query(Publication.last_error, func.count(Publication.id))
            .filter(Publication.last_error.isnot(None))
            .group_by(Publication.last_error)
            .order_by(func.count(Publication.id).desc())
            .limit(10)
            .all()
        )
        return render_template("reports.html", rows=rows, error_rows=error_rows, utc_to_moscow=utc_to_moscow)

    @app.route("/blacklist", methods=["GET", "POST"])
    def blacklist():
        if request.method == "POST":
            rule = BlacklistRule(type=request.form["type"], pattern=request.form["pattern"], is_enabled=bool(request.form.get("is_enabled")))
            db.session.add(rule)
            db.session.commit()
            flash("Правило blacklist добавлено")
            return redirect(url_for("blacklist"))
        rules = BlacklistRule.query.order_by(BlacklistRule.id.desc()).all()
        return render_template("blacklist.html", rules=rules)

    @app.route("/import", methods=["GET", "POST"])
    def csv_import():
        if request.method == "POST":
            mode = request.form.get("mode", "draft")
            file = request.files.get("file")
            if not file:
                flash("Выберите CSV-файл")
                return redirect(url_for("csv_import"))

            data = file.read().decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(data))
            created = 0
            for row in reader:
                channel = None
                if row.get("channel_id"):
                    channel = db.session.get(Channel, int(row["channel_id"]))
                elif row.get("channel_title"):
                    channel = Channel.query.filter_by(title=row["channel_title"]).first()
                if not channel:
                    continue

                buttons_raw = row.get("buttons", "[]")
                if buttons_raw and not buttons_raw.strip().startswith("["):
                    parsed_buttons = []
                    for pair in buttons_raw.split(";"):
                        if "|" in pair:
                            text, url = pair.split("|", 1)
                            parsed_buttons.append({"text": text, "url": url})
                    buttons_raw = json.dumps(parsed_buttons, ensure_ascii=False)

                media_urls = [u.strip() for u in (row.get("media_urls") or "").split("|") if u.strip()]
                media = json.dumps([{"type": "photo", "url": u} for u in media_urls], ensure_ascii=False)

                post = Post(
                    channel_id=channel.id,
                    title=row.get("title") or "Без названия",
                    body_html=row.get("body_html") or "",
                    media=media,
                    buttons=buttons_raw or "[]",
                    options="{}",
                    status="draft",
                )
                ok, reason = validate_post(post)
                post.blacklist_check_status = "ok" if ok else "blocked"
                post.blacklist_reason = reason
                db.session.add(post)
                db.session.flush()
                created += 1

                if mode == "scheduled" and ok:
                    planned_at = row.get("planned_at", "").strip()
                    if planned_at:
                        local_dt = datetime.strptime(planned_at, "%Y-%m-%d %H:%M")
                        plan_utc = moscow_to_utc(local_dt)
                    else:
                        plan_utc, _ = calculate_next_slot(channel)
                    plan_utc = adjust_to_window(channel, plan_utc)
                    db.session.add(Publication(post_id=post.id, planned_at=plan_utc, ready_at=plan_utc, status="scheduled"))
                    post.status = "scheduled"
            db.session.commit()
            flash(f"Импортировано записей: {created}")
            return redirect(url_for("posts"))
        return render_template("import.html")

    with app.app_context():
        db.create_all()

    if os.getenv("DISABLE_SCHEDULER", "0") != "1":
        t = threading.Thread(target=run_scheduler, args=(app,), daemon=True)
        t.start()

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
