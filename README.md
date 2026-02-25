# pablic_publisher

MVP веб-сервис автопостинга в Telegram на Flask.

## Возможности

- мульти-канальная работа (через таблицу `channels`);
- черновики, планирование, отмена, дублирование постов;
- сущности `Post` и `Publication` с историей план/факт;
- scheduler без очередей (background thread), строгий порядок отправки;
- ретраи до 5 попыток с переносом на `+30 минут`;
- автодогон пропущенных публикаций (`ready_at <= now`);
- HTML, media, inline-кнопки через Bot API;
- blacklist (word/domain/regex), валидации длины/ссылок/медиа;
- CSV-импорт;
- простая веб-админка: каналы, посты, очередь, отчёты.

## Быстрый старт

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Открыть: `http://127.0.0.1:5000`

## Настройки

Переменные окружения:

- `DATABASE_URL` (по умолчанию `sqlite:///publisher.db`)
- `SECRET_KEY`
- `DISABLE_SCHEDULER=1` (если нужно отключить background-планировщик)

## Инициализация БД

Таблицы создаются автоматически при запуске. Также доступна команда:

```bash
flask --app app init-db
```
