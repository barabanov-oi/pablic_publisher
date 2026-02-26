import os
from threading import Thread

from app import create_app
from app.worker import run_worker


app = create_app()


def _should_start_worker() -> bool:
    if os.getenv("DISABLE_SCHEDULER") == "1":
        return False

    # В debug-режиме у Flask есть процесс-родитель перезагрузчика.
    # Запускаем воркер только в рабочем процессе, чтобы избежать дублей.
    return os.getenv("WERKZEUG_RUN_MAIN") == "true" or not app.debug


def _start_worker_in_background() -> None:
    worker_thread = Thread(target=run_worker, args=(app,), daemon=True, name="publication-worker")
    worker_thread.start()


if __name__ == "__main__":
    if _should_start_worker():
        _start_worker_in_background()

    app.run(host="0.0.0.0", port=5000, debug=True)
