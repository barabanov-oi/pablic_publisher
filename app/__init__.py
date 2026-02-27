import os
import logging
import sys
import threading

import click
from flask import Flask
from sqlalchemy import event

from app.config import CONFIG_MAP, DevelopmentConfig
from app.extensions import db
from app.web import web_bp
from app.worker import run_worker

# Ensure models are registered for SQLAlchemy metadata.
from app import models  # noqa: F401


def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def create_app(config_name: str | None = None) -> Flask:
    _configure_logging()
    app = Flask(__name__, template_folder="../templates", static_folder="../static")

    env_name = config_name or os.getenv("APP_ENV", "development")
    config_class = CONFIG_MAP.get(env_name, DevelopmentConfig)
    app.config.from_object(config_class)

    db.init_app(app)
    _configure_sqlite_engine(app)
    app.register_blueprint(web_bp)

    _register_endpoint_aliases(app)
    _register_cli_commands(app)
    _maybe_start_scheduler(app)

    return app


def _configure_sqlite_engine(app: Flask) -> None:
    db_uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
    if not db_uri.startswith("sqlite"):
        return

    with app.app_context():
        @event.listens_for(db.engine, "connect")
        def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()


def _should_start_scheduler(app: Flask) -> bool:
    if app.testing or app.config.get("DISABLE_SCHEDULER", False):
        return False

    if os.getenv("FLASK_RUN_FROM_CLI") == "true":
        return False

    if app.debug:
        return os.getenv("WERKZEUG_RUN_MAIN") == "true"

    return True


def _maybe_start_scheduler(app: Flask) -> None:
    if not _should_start_scheduler(app):
        logging.getLogger(__name__).info("[worker] Автозапуск воркера отключён")
        return

    worker_thread = threading.Thread(target=run_worker, args=(app,), daemon=True, name="publisher-worker")
    worker_thread.start()
    logging.getLogger(__name__).info("[worker] Воркер запущен в фоне: thread=%s", worker_thread.name)


def _register_endpoint_aliases(app: Flask) -> None:
    aliases = {
        "web.index": "index",
        "web.channels": "channels",
        "web.posts": "posts",
        "web.post_new": "post_new",
        "web.post_edit": "post_edit",
        "web.post_duplicate": "post_duplicate",
        "web.post_cancel": "post_cancel",
        "web.post_schedule": "post_schedule",
        "web.publications": "publications",
        "web.publication_reschedule": "publication_reschedule",
        "web.publication_retry_now": "publication_retry_now",
        "web.reports": "reports",
        "web.blacklist": "blacklist",
        "web.csv_import": "csv_import",
    }

    for rule in list(app.url_map.iter_rules()):
        if rule.endpoint not in aliases:
            continue
        app.add_url_rule(
            rule.rule,
            endpoint=aliases[rule.endpoint],
            view_func=app.view_functions[rule.endpoint],
            methods=rule.methods,
        )


def _register_cli_commands(app: Flask) -> None:
    @app.cli.command("init-db")
    def init_db_command() -> None:
        db.create_all()
        click.echo("Database initialized")

    @app.cli.command("worker")
    def worker_command() -> None:
        run_worker(app)
