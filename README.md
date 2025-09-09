# JEM Reminder - Telegram Bot

Telegram бот с веб-интерфейсом для управления группами и событиями с системой напоминаний.

## Функции

- Управление группами и участниками
- Создание и управление событиями
- Система оповещений (групповые и личные)
- Telegram Mini App интерфейс
- Роли пользователей (владелец, админ, участник)

## Установка

1. Клонируйте репозиторий
2. Создайте виртуальное окружение: `python -m venv env`
3. Активируйте окружение: `source env/bin/activate` (Linux/Mac) или `env\Scripts\activate` (Windows)
4. Установите зависимости: `pip install -r requirements.txt`
5. Создайте `.env` файл с настройками:

```env
BOT_TOKEN=your_bot_token_here
TEST_TELEGRAM_ID=your_telegram_id
```

6. Запустите бота: `python bot.py`
7. Запустите веб-сервер: `python -m uvicorn web.app:app --host 0.0.0.0 --port 8000`

## Структура проекта

- `bot.py` - Основной файл бота
- `web/` - Веб-интерфейс (FastAPI)
- `services/` - Бизнес-логика и работа с БД
- `database/` - Схема базы данных
- `data/` - Файлы базы данных

## Технологии

- Python 3.12+
- aiogram 3.x
- FastAPI
- SQLite
- HTML/CSS/JavaScript
