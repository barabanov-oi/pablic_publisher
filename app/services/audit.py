import json
from typing import Any

from app.extensions import db
from app.models import AuditLog


def log_action(entity_type: str, entity_id: int, action: str, meta: dict[str, Any] | None = None) -> None:
    db.session.add(
        AuditLog(
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            meta=json.dumps(meta or {}, ensure_ascii=False),
        )
    )
