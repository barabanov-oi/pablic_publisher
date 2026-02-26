import csv
import io
import json
import time
from datetime import datetime

from flask import flash, redirect, render_template, request, url_for
from sqlalchemy import func
from sqlalchemy.exc import OperationalError

from app.extensions import db
from app.models import BlacklistRule, Channel, Post, Publication
from app.services.audit import log_action
from app.services.scheduling import adjust_to_window, calculate_next_slot
from app.services.telegram_client import normalize_chat_id, verify_channel_access
from app.services.validation import validate_post
from app.utils.timezone import local_to_utc_naive, now_utc_naive, utc_naive_to_local
from app.web import web_bp


def _commit_with_retry(max_attempts: int = 5, delay_seconds: float = 0.2) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            db.session.commit()
            return
        except OperationalError as exc:
            db.session.rollback()
            if "database is locked" not in str(exc).lower() or attempt == max_attempts:
                raise
            time.sleep(delay_seconds * attempt)


@web_bp.route("/")
def index():
    return render_template("index.html")


@web_bp.route("/channels", methods=["GET", "POST"])
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
        db.session.flush()
        log_action("channel", channel.id, "create", {"check": check_message})
        _commit_with_retry()
        flash(f"Канал добавлен. {check_message}")
        return redirect(url_for("channels"))

    items = Channel.query.order_by(Channel.created_at.desc()).all()
    return render_template("channels.html", channels=items)


@web_bp.route("/posts")
def posts():
    items = Post.query.order_by(Post.updated_at.desc()).all()
    return render_template("posts.html", posts=items)


@web_bp.route("/posts/new", methods=["GET", "POST"])
def post_new():
    channels_list = Channel.query.order_by(Channel.title.asc()).all()
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
    return render_template("post_form.html", channels=channels_list, post=None)


@web_bp.route("/posts/<int:post_id>/edit", methods=["GET", "POST"])
def post_edit(post_id: int):
    post = db.get_or_404(Post, post_id)
    channels_list = Channel.query.order_by(Channel.title.asc()).all()
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
    return render_template("post_form.html", channels=channels_list, post=post)


@web_bp.post("/posts/<int:post_id>/duplicate")
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


@web_bp.post("/posts/<int:post_id>/cancel")
def post_cancel(post_id: int):
    post = db.get_or_404(Post, post_id)
    post.status = "canceled"
    Publication.query.filter(
        Publication.post_id == post.id,
        Publication.status.in_(["scheduled", "retry", "processing"]),
    ).update({"status": "canceled"})
    log_action("post", post.id, "cancel")
    db.session.commit()
    flash("Пост отменён")
    return redirect(url_for("posts"))


@web_bp.post("/posts/<int:post_id>/schedule")
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
        planned_utc = local_to_utc_naive(planned_local, post.channel.timezone)
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


@web_bp.route("/publications")
def publications():
    items = Publication.query.order_by(Publication.ready_at.asc(), Publication.planned_at.asc(), Publication.id.asc()).all()
    return render_template("publications.html", publications=items)


@web_bp.post("/publications/<int:pub_id>/reschedule")
def publication_reschedule(pub_id: int):
    publication = db.get_or_404(Publication, pub_id)
    channel = publication.post.channel
    planned_text = request.form["planned_at"].strip()
    planned_local = datetime.strptime(planned_text, "%Y-%m-%dT%H:%M")
    planned_utc = adjust_to_window(
        channel,
        local_to_utc_naive(planned_local, channel.timezone),
    )
    publication.post.status = "scheduled"
    publication.planned_at = planned_utc
    publication.ready_at = planned_utc
    publication.status = "scheduled"
    publication.attempts = 0
    publication.last_error = None
    publication.locked_at = None
    publication.locked_by = None
    log_action("publication", publication.id, "reschedule", {"planned_at": planned_utc.isoformat()})
    _commit_with_retry()
    flash("Публикация перепланирована")
    return redirect(url_for("publications"))


@web_bp.post("/publications/<int:pub_id>/retry-now")
def publication_retry_now(pub_id: int):
    publication = db.get_or_404(Publication, pub_id)
    publication.post.status = "queued"
    publication.status = "retry"
    publication.ready_at = now_utc_naive()
    publication.attempts = 0
    publication.last_error = None
    publication.locked_at = None
    publication.locked_by = None
    log_action("publication", publication.id, "retry_now")
    _commit_with_retry()
    flash("Публикация поставлена на немедленную переотправку")
    return redirect(url_for("publications"))


@web_bp.route("/reports")
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
    return render_template(
        "reports.html",
        rows=rows,
        error_rows=error_rows,
        utc_to_moscow=lambda dt: utc_naive_to_local(dt, "Europe/Moscow"),
    )


@web_bp.route("/blacklist", methods=["GET", "POST"])
def blacklist():
    if request.method == "POST":
        rule = BlacklistRule(
            type=request.form["type"],
            pattern=request.form["pattern"],
            is_enabled=bool(request.form.get("is_enabled")),
        )
        db.session.add(rule)
        db.session.commit()
        flash("Правило blacklist добавлено")
        return redirect(url_for("blacklist"))
    rules = BlacklistRule.query.order_by(BlacklistRule.id.desc()).all()
    return render_template("blacklist.html", rules=rules)


@web_bp.route("/import", methods=["GET", "POST"])
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
                    plan_utc = local_to_utc_naive(local_dt, channel.timezone)
                else:
                    plan_utc, _ = calculate_next_slot(channel)
                plan_utc = adjust_to_window(channel, plan_utc)
                db.session.add(Publication(post_id=post.id, planned_at=plan_utc, ready_at=plan_utc, status="scheduled"))
                post.status = "scheduled"
        db.session.commit()
        flash(f"Импортировано записей: {created}")
        return redirect(url_for("posts"))
    return render_template("import.html")
