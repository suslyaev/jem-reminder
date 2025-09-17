import sqlite3
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / 'data' / 'bot_v2.db'
SCHEMA_PATH = Path(__file__).resolve().parent / 'schema.sql'


def check_table_exists(conn, table_name):
    """Проверяет, существует ли таблица"""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None


def check_column_exists(conn, table_name, column_name):
    """Проверяет, существует ли колонка в таблице"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return any(col[1] == column_name for col in columns)


def get_missing_columns(conn, table_name, expected_columns):
    """Возвращает список отсутствующих колонок"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [col[1] for col in cursor.fetchall()]
    return [col for col in expected_columns if col not in existing_columns]


def apply_migrations(conn):
    """Применяет миграции к существующей базе данных"""
    print("Проверяем и применяем миграции...")
    
    # Включаем foreign keys
    print("  - Включаем foreign keys...")
    conn.execute("PRAGMA foreign_keys = ON")

    # Базовая миграция: убедиться, что колонка 'type' есть в notification_settings
    if check_table_exists(conn, 'notification_settings') and not check_column_exists(conn, 'notification_settings', 'type'):
        print("  - Добавляем колонку 'type' в таблицу notification_settings...")
        cursor = conn.cursor()
        cursor.execute("""
            ALTER TABLE notification_settings 
            ADD COLUMN type TEXT NOT NULL DEFAULT 'group'
        """)
        cursor.execute("""
            UPDATE notification_settings 
            SET type = 'group' 
            WHERE type IS NULL OR type = ''
        """)
        print("  - Колонка 'type' добавлена успешно")
    
    # Применяем схему для создания недостающих таблиц и индексов
    print("  - Создаем недостающие таблицы и индексы по schema.sql...")
    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        sql_script = f.read()
        conn.executescript(sql_script)
    print("  - Схема синхронизирована")

    # Добавляем колонку allow_multi_roles_per_user в events, если отсутствует
    if check_table_exists(conn, 'events') and not check_column_exists(conn, 'events', 'allow_multi_roles_per_user'):
        print("  - Добавляем колонку 'allow_multi_roles_per_user' в таблицу events...")
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE events ADD COLUMN allow_multi_roles_per_user INTEGER NOT NULL DEFAULT 0")
        print("  - Колонка добавлена")

    # Добавляем аудиторские поля для событий, если отсутствуют
    # Шаблоны ролей на уровне группы
    try:
        if check_table_exists(conn, 'groups'):
            if not check_table_exists(conn, 'group_role_templates'):
                print("  - Создаем таблицу group_role_templates...")
                cursor = conn.cursor()
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS group_role_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id INTEGER NOT NULL,
                        role_name TEXT NOT NULL,
                        required INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT DEFAULT (datetime('now')),
                        UNIQUE(group_id, role_name),
                        FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_role_templates_group ON group_role_templates(group_id)")
                print("  - Таблица group_role_templates создана")
    except Exception as e:
        print("  - Ошибка при создании group_role_templates:", e)

    if check_table_exists(conn, 'events'):
        cursor = conn.cursor()
        if not check_column_exists(conn, 'events', 'created_by_user_id'):
            print("  - Добавляем колонку 'created_by_user_id' в таблицу events...")
            cursor.execute("ALTER TABLE events ADD COLUMN created_by_user_id INTEGER")
        if not check_column_exists(conn, 'events', 'updated_by_user_id'):
            print("  - Добавляем колонку 'updated_by_user_id' в таблицу events...")
            cursor.execute("ALTER TABLE events ADD COLUMN updated_by_user_id INTEGER")
        if not check_column_exists(conn, 'events', 'updated_at'):
            print("  - Добавляем колонку 'updated_at' в таблицу events...")
            cursor.execute("ALTER TABLE events ADD COLUMN updated_at TEXT")

    # Блокировка пользователей
    if check_table_exists(conn, 'users') and not check_column_exists(conn, 'users', 'blocked'):
        print("  - Добавляем колонку 'blocked' в таблицу users...")
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE users ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0")

    # Очистка номинальных членств суперадмина (если когда-то добавлялись автоматически)
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    if CFG_SA:
        with sqlite3.connect(DB_PATH.as_posix()) as c2:
            cur2 = c2.cursor()
            cur2.execute("SELECT id FROM users WHERE telegram_id = ?", (CFG_SA,))
            sa = cur2.fetchone()
            if sa:
                sa_uid = sa[0]
                print("  - Удаляем автодобавленные записи суперадмина из user_group_roles (если есть)...")
                cur2.execute("DELETE FROM user_group_roles WHERE user_id = ? AND role = 'superadmin'", (sa_uid,))
                c2.commit()
    
    print("Миграции применены успешно!")


def init_db() -> None:
    """Инициализирует базу данных или обновляет существующую"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    db_exists = DB_PATH.exists()
    
    with sqlite3.connect(DB_PATH.as_posix()) as conn:
        if not db_exists:
            print("Создаем новую базу данных...")
            with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
                conn.executescript(f.read())
            print(f"База данных создана: {DB_PATH}")
        else:
            print(f"База данных уже существует: {DB_PATH}")
            print("Проверяем необходимость миграций...")
            apply_migrations(conn)
        
        # Включаем foreign keys для всех операций
        conn.execute("PRAGMA foreign_keys = ON")


def check_db_status():
    """Проверяет статус базы данных и выводит информацию"""
    if not DB_PATH.exists():
        print(f"База данных не найдена: {DB_PATH}")
        return False
    
    with sqlite3.connect(DB_PATH.as_posix()) as conn:
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"База данных: {DB_PATH}")
        print(f"Таблицы ({len(tables)}):")
        for table in tables:
            print(f"  - {table[0]}")
        
        # Проверяем важные таблицы
        important_tables = ['users', 'groups', 'notification_settings', 'events']
        missing_tables = [t for t in important_tables if not check_table_exists(conn, t)]
        
        if missing_tables:
            print(f"❌ Отсутствуют важные таблицы: {missing_tables}")
            return False
        
        # Проверяем колонку type в notification_settings
        if check_table_exists(conn, 'notification_settings'):
            if check_column_exists(conn, 'notification_settings', 'type'):
                print("✅ Колонка 'type' присутствует в notification_settings")
            else:
                print("❌ Колонка 'type' отсутствует в notification_settings")
                return False
        
        print("✅ База данных в порядке")
        return True


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'check':
        check_db_status()
    else:
        init_db()
        print(f'База данных инициализирована/обновлена: {DB_PATH}')
        print("\nПроверяем статус...")
        check_db_status()


