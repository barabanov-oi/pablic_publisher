import os

import click
from flask import Flask

from app.config import CONFIG_MAP, DevelopmentConfig
from app.extensions import db
from app.web import web_bp
from app.worker import run_worker

# Ensure models are registered for SQLAlchemy metadata.
from app import models  # noqa: F401


def create_app(config_name: str | None = None) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")

    env_name = config_name or os.getenv("APP_ENV", "development")
    config_class = CONFIG_MAP.get(env_name, DevelopmentConfig)
    app.config.from_object(config_class)

    db.init_app(app)
    app.register_blueprint(web_bp)

    _register_endpoint_aliases(app)
    _register_cli_commands(app)

    return app


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
