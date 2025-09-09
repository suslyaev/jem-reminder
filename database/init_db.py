import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / 'data' / 'bot_v2.db'
SCHEMA_PATH = Path(__file__).resolve().parent / 'schema.sql'


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH.as_posix()) as conn:
        with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
            conn.executescript(f.read())


if __name__ == '__main__':
    init_db()
    print(f'Database initialized at: {DB_PATH}')


