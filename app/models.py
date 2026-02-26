from datetime import datetime

from .extensions import db


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
    type = db.Column(db.String(32), nullable=False)
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
