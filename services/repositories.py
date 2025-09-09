import sqlite3
from pathlib import Path
from typing import Optional, List, Tuple

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / 'data' / 'bot_v2.db'


def get_conn():
    return sqlite3.connect(DB_PATH.as_posix())


class UserRepo:
    @staticmethod
    def upsert_user(telegram_id: int, username: Optional[str], phone: Optional[str], first_name: Optional[str], last_name: Optional[str]) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
            row = cur.fetchone()
            if row:
                user_id = row[0]
                cur.execute("UPDATE users SET username = ?, phone = ?, first_name = ?, last_name = ? WHERE id = ?",
                            (username, phone, first_name, last_name, user_id))
            else:
                cur.execute("INSERT INTO users (telegram_id, username, phone, first_name, last_name) VALUES (?,?,?,?,?)",
                            (telegram_id, username, phone, first_name, last_name))
                user_id = cur.lastrowid
            conn.commit()
            return user_id

    @staticmethod
    def get_by_telegram_id(telegram_id: int) -> Optional[Tuple]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, telegram_id, username, phone, first_name, last_name FROM users WHERE telegram_id = ?", (telegram_id,))
            return cur.fetchone()

    @staticmethod
    def get_by_id(user_id: int) -> Optional[Tuple]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, telegram_id, username, phone, first_name, last_name FROM users WHERE id = ?", (user_id,))
            return cur.fetchone()

    @staticmethod
    def get_by_username(username: str) -> Optional[Tuple]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, telegram_id, username, phone, first_name, last_name FROM users WHERE LOWER(username) = LOWER(?)", (username.lstrip('@'),))
            return cur.fetchone()

    @staticmethod
    def update_phone(user_id: int, phone: str) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE users SET phone = ? WHERE id = ?", (phone, user_id))
            conn.commit()

    @staticmethod
    def get_telegram_id_by_user_id(user_id: int) -> Optional[int]:
        """Get telegram_id by user_id."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT telegram_id FROM users WHERE id = ?", (user_id,))
            row = cur.fetchone()
            return row[0] if row else None


class GroupRepo:
    @staticmethod
    def get_by_chat_id(telegram_chat_id: str) -> Optional[Tuple]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, telegram_chat_id, title, owner_user_id FROM groups WHERE telegram_chat_id = ?", (telegram_chat_id,))
            return cur.fetchone()

    @staticmethod
    def create(telegram_chat_id: str, title: str, owner_user_id: Optional[int]) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO groups (telegram_chat_id, title, owner_user_id) VALUES (?,?,?)",
                        (telegram_chat_id, title, owner_user_id))
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def get_by_id(group_id: int) -> Optional[Tuple]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, telegram_chat_id, title, owner_user_id FROM groups WHERE id = ?", (group_id,))
            return cur.fetchone()

    @staticmethod
    def list_user_groups_with_roles(user_id: int) -> List[Tuple[int, str, str]]:
        """Return list of (group_id, title, role, telegram_chat_id) for the user."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT g.id, g.title, r.role, g.telegram_chat_id
                FROM user_group_roles r
                JOIN groups g ON g.id = r.group_id
                WHERE r.user_id = ? AND r.confirmed = 1
                ORDER BY g.title
                """,
                (user_id,)
            )
            return cur.fetchall()

    @staticmethod
    def list_group_admin_ids(group_id: int) -> List[int]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id FROM user_group_roles WHERE group_id = ? AND role = 'admin' AND confirmed = 1", (group_id,))
            return [r[0] for r in cur.fetchall()]

    @staticmethod
    def list_group_admins(group_id: int) -> List[Tuple[int, int, Optional[str]]]:
        """Return (user_id, telegram_id, username) for confirmed admins of a group."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id, u.telegram_id, u.username
                FROM user_group_roles r
                JOIN users u ON u.id = r.user_id
                WHERE r.group_id = ? AND r.role = 'admin' AND r.confirmed = 1
                ORDER BY u.username IS NULL, u.username
                """,
                (group_id,)
            )
            return cur.fetchall()

    @staticmethod
    def list_group_members(group_id: int) -> List[Tuple[int, Optional[str]]]:
        """Return confirmed members of group as (user_id, username). Includes owner/admin/member."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT u.id, u.username
                FROM user_group_roles r
                JOIN users u ON u.id = r.user_id
                WHERE r.group_id = ? AND r.confirmed = 1
                ORDER BY u.username IS NULL, u.username
                """,
                (group_id,)
            )
            return cur.fetchall()

    @staticmethod
    def list_group_members_detailed(group_id: int) -> List[Tuple[int, int, Optional[str], Optional[str], Optional[str], Optional[str], str]]:
        """Return detailed members info as (user_id, telegram_id, username, phone, first_name, last_name, role)."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT u.id, u.telegram_id, u.username, u.phone, u.first_name, u.last_name, r.role
                FROM user_group_roles r
                JOIN users u ON u.id = r.user_id
                WHERE r.group_id = ? AND r.confirmed = 1
                ORDER BY r.role = 'superadmin' DESC, r.role = 'owner' DESC, r.role = 'admin' DESC, u.username IS NULL, u.username
                """,
                (group_id,)
            )
            return cur.fetchall()

    @staticmethod
    def count_group_events(group_id: int) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(1) FROM events WHERE group_id = ?", (group_id,))
            row = cur.fetchone()
            return row[0] if row else 0


class RoleRepo:
    @staticmethod
    def add_role(user_id: int, group_id: int, role: str, confirmed: bool = True) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO user_group_roles (user_id, group_id, role, confirmed) VALUES (?,?,?,?)",
                        (user_id, group_id, role, 1 if confirmed else 0))
            conn.commit()

    @staticmethod
    def find_pending_admin_match(group_id: int, *, telegram_id: Optional[int], username: Optional[str], phone: Optional[str]) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            if telegram_id is not None:
                cur.execute("SELECT 1 FROM pending_admins WHERE group_id = ? AND identifier_type = 'id' AND identifier = ?",
                            (group_id, str(telegram_id)))
                if cur.fetchone():
                    return True
            if username:
                cur.execute("SELECT 1 FROM pending_admins WHERE group_id = ? AND identifier_type = 'username' AND identifier = ?",
                            (group_id, username.lstrip('@')))
                if cur.fetchone():
                    return True
            if phone:
                normalized = ''.join(filter(str.isdigit, phone))
                if normalized.startswith('8'):
                    normalized = '7' + normalized[1:]
                cur.execute("SELECT 1 FROM pending_admins WHERE group_id = ? AND identifier_type = 'phone' AND REPLACE(REPLACE(REPLACE(identifier,'+',''),'-',''),' ','') LIKE ?",
                            (group_id, f"%{normalized[-10:]}%",))
                if cur.fetchone():
                    return True
            return False

    @staticmethod
    def confirm_admin_if_pending(user_id: int, group_id: int) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            # Upon confirmation, add admin role and clear matching pending entries by group
            cur.execute("INSERT OR IGNORE INTO user_group_roles (user_id, group_id, role, confirmed) VALUES (?,?,?,1)",
                        (user_id, group_id, 'admin'))
            # Remove pending records that match this user by any known identifier
            # Delete by telegram id
            cur.execute("DELETE FROM pending_admins WHERE group_id = ? AND identifier_type = 'id' AND identifier = (SELECT CAST(telegram_id AS TEXT) FROM users WHERE id = ?)", (group_id, user_id))
            # Delete by username
            cur.execute("DELETE FROM pending_admins WHERE group_id = ? AND identifier_type = 'username' AND LOWER(identifier) = (SELECT LOWER(COALESCE(username,'')) FROM users WHERE id = ?)", (group_id, user_id))
            # Delete by phone (normalize last 10 digits)
            cur.execute("DELETE FROM pending_admins WHERE group_id = ? AND identifier_type = 'phone' AND REPLACE(REPLACE(REPLACE(identifier,'+',''),'-',''),' ','') LIKE '%' || (SELECT substr(REPLACE(REPLACE(REPLACE(COALESCE(phone,''),'+',''),'-',''),' ',''), -10) FROM users WHERE id = ?) || '%'", (group_id, user_id))
            conn.commit()

    @staticmethod
    def find_groups_for_pending(telegram_id: Optional[int], username: Optional[str], phone: Optional[str]) -> List[int]:
        group_ids: List[int] = []
        with get_conn() as conn:
            cur = conn.cursor()
            if telegram_id is not None:
                cur.execute("SELECT DISTINCT group_id FROM pending_admins WHERE identifier_type='id' AND identifier = ?", (str(telegram_id),))
                group_ids += [r[0] for r in cur.fetchall()]
            if username:
                cur.execute("SELECT DISTINCT group_id FROM pending_admins WHERE identifier_type='username' AND LOWER(identifier) = LOWER(?)", (username.lstrip('@'),))
                group_ids += [r[0] for r in cur.fetchall()]
            if phone:
                normalized = ''.join(filter(str.isdigit, phone))
                if normalized.startswith('8'):
                    normalized = '7' + normalized[1:]
                cur.execute("SELECT DISTINCT group_id FROM pending_admins WHERE identifier_type='phone' AND REPLACE(REPLACE(REPLACE(identifier,'+',''),'-',''),' ','') LIKE ?",
                            (f"%{normalized[-10:]}%",))
                group_ids += [r[0] for r in cur.fetchall()]
        # Unique
        return list(dict.fromkeys(group_ids))

    @staticmethod
    def has_role(user_id: int, group_id: int, roles: List[str]) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            q_marks = ','.join('?' for _ in roles)
            cur.execute(f"SELECT 1 FROM user_group_roles WHERE user_id = ? AND group_id = ? AND role IN ({q_marks}) LIMIT 1", (user_id, group_id, *roles))
            return cur.fetchone() is not None

    @staticmethod
    def get_user_role(user_id: int, group_id: int) -> Optional[str]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT role FROM user_group_roles WHERE user_id = ? AND group_id = ? AND confirmed = 1 LIMIT 1", (user_id, group_id))
            row = cur.fetchone()
            return row[0] if row else None

    @staticmethod
    def remove_admin(user_id: int, group_id: int) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM user_group_roles WHERE user_id = ? AND group_id = ? AND role = 'admin'", (user_id, group_id))
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def add_pending_admin(group_id: int, identifier: str, identifier_type: str, created_by_user: int) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO pending_admins (group_id, identifier, identifier_type, created_by_user) VALUES (?,?,?,?)",
                        (group_id, identifier, identifier_type, created_by_user))
            conn.commit()

    @staticmethod
    def list_pending_admins(group_id: int) -> List[Tuple]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, identifier, identifier_type, created_by_user, created_at FROM pending_admins WHERE group_id = ? ORDER BY created_at DESC", (group_id,))
            return cur.fetchall()

    @staticmethod
    def delete_pending(pending_id: int) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM pending_admins WHERE id = ?", (pending_id,))
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def has_any_pending_by_phone() -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pending_admins WHERE identifier_type = 'phone' LIMIT 1")
            return cur.fetchone() is not None


class NotificationRepo:
    @staticmethod
    def ensure_defaults(group_id: int) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM notification_settings WHERE group_id = ?", (group_id,))
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO notification_settings (group_id, time_before, time_unit, message_text, is_default) VALUES (?,?,?,?,1)",
                    (group_id, 1, 'days', 'Напоминание: скоро мероприятие!')
                )
                cur.execute(
                    "INSERT INTO notification_settings (group_id, time_before, time_unit, message_text, is_default) VALUES (?,?,?,?,1)",
                    (group_id, 2, 'hours', 'Напоминание: через 2 часа мероприятие!')
                )
                cur.execute(
                    "INSERT INTO notification_settings (group_id, time_before, time_unit, message_text, is_default) VALUES (?,?,?,?,1)",
                    (group_id, 3, 'days', 'Напоминание: через 3 дня мероприятие!')
                )
                conn.commit()

    @staticmethod
    def add_notification(group_id: int, time_before: int, time_unit: str, message_text: Optional[str], is_default: int = 0) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO notification_settings (group_id, time_before, time_unit, message_text, is_default) VALUES (?,?,?,?,?)",
                        (group_id, time_before, time_unit, message_text, is_default))
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def list_notifications(group_id: int) -> List[Tuple]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, time_before, time_unit, message_text, is_default FROM notification_settings WHERE group_id = ? ORDER BY time_before", (group_id,))
            return cur.fetchall()

    @staticmethod
    def delete_notification(notification_id: int) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM notification_settings WHERE id = ?", (notification_id,))
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def get_user_role_in_group(user_id: int, group_id: int) -> str:
        """Get user's role in group, returns 'member' if not found."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT role FROM user_group_roles 
                WHERE user_id = ? AND group_id = ? AND confirmed = 1
                ORDER BY role = 'superadmin' DESC, role = 'owner' DESC, role = 'admin' DESC
                LIMIT 1
            """, (user_id, group_id))
            row = cur.fetchone()
            return row[0] if row else 'member'


class EventRepo:
    @staticmethod
    def list_by_group(group_id: int) -> List[Tuple[int, str, str, Optional[int]]]:
        """
        Return events of group ordered by time asc.
        Returns: (id, name, time, responsible_user_id)
        """
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, name, time, responsible_user_id FROM events WHERE group_id = ? ORDER BY time ASC",
                (group_id,),
            )
            return cur.fetchall()

    @staticmethod
    def get_by_id(event_id: int) -> Optional[Tuple[int, str, str, int, Optional[int]]]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, name, time, group_id, responsible_user_id FROM events WHERE id = ?",
                (event_id,),
            )
            return cur.fetchone()

    @staticmethod
    def create(group_id: int, name: str, time_str: str, responsible_user_id: Optional[int] = None) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO events (name, time, group_id, responsible_user_id) VALUES (?,?,?,?)",
                (name, time_str, group_id, responsible_user_id),
            )
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def delete(event_id: int) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM events WHERE id = ?", (event_id,))
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def set_responsible(event_id: int, user_id: Optional[int]) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE events SET responsible_user_id = ? WHERE id = ?", (user_id, event_id))
            conn.commit()

    @staticmethod
    def update_name(event_id: int, name: str) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE events SET name = ? WHERE id = ?", (name, event_id))
            conn.commit()

    @staticmethod
    def update_time(event_id: int, time_str: str) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE events SET time = ? WHERE id = ?", (time_str, event_id))
            conn.commit()

    @staticmethod
    def list_by_group_between(group_id: int, start_iso: str, end_iso: str) -> List[Tuple]:
        """Return events in group between [start_iso, end_iso], ordered by time."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, name, time, responsible_user_id FROM events WHERE group_id = ? AND time >= ? AND time <= ? ORDER BY time",
                (group_id, start_iso, end_iso),
            )
            return cur.fetchall()


class EventNotificationRepo:
    @staticmethod
    def create_from_group_defaults(event_id: int, group_id: int) -> None:
        """Create event notifications based on ALL group notification settings."""
        with get_conn() as conn:
            cur = conn.cursor()
            # Get ALL group notifications (both default and custom)
            cur.execute("SELECT time_before, time_unit, message_text FROM notification_settings WHERE group_id = ?", (group_id,))
            all_notifications = cur.fetchall()
            
            # Create event notifications based on all group settings
            for time_before, time_unit, message_text in all_notifications:
                cur.execute(
                    "INSERT INTO event_notifications (event_id, time_before, time_unit, message_text) VALUES (?,?,?,?)",
                    (event_id, time_before, time_unit, message_text)
                )
            conn.commit()

    @staticmethod
    def list_by_event(event_id: int) -> List[Tuple]:
        """Return event notifications ordered by time_before."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, time_before, time_unit, message_text FROM event_notifications WHERE event_id = ? ORDER BY time_before", (event_id,))
            return cur.fetchall()

    @staticmethod
    def add_notification(event_id: int, time_before: int, time_unit: str, message_text: Optional[str] = None) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            # Check if notification already exists
            cur.execute("SELECT id FROM event_notifications WHERE event_id = ? AND time_before = ? AND time_unit = ? AND (message_text = ? OR (message_text IS NULL AND ? IS NULL))",
                        (event_id, time_before, time_unit, message_text, message_text))
            existing = cur.fetchone()
            if existing:
                return existing[0]  # Return existing ID instead of creating duplicate
            
            cur.execute("INSERT INTO event_notifications (event_id, time_before, time_unit, message_text) VALUES (?,?,?,?)",
                        (event_id, time_before, time_unit, message_text))
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def delete_notification(notification_id: int) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM event_notifications WHERE id = ?", (notification_id,))
            conn.commit()
            return cur.rowcount > 0


class PersonalEventNotificationRepo:
    @staticmethod
    def create_from_group_for_user(event_id: int, group_id: int, user_id: int) -> None:
        """Create personal notifications for ONE user from group's settings (idempotent)."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT time_before, time_unit, message_text FROM notification_settings WHERE group_id = ?", (group_id,))
            group_notifications = cur.fetchall()
            for time_before, time_unit, message_text in group_notifications:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO personal_event_notifications 
                    (user_id, event_id, time_before, time_unit, message_text) 
                    VALUES (?,?,?,?,?)
                    """,
                    (user_id, event_id, time_before, time_unit, message_text)
                )
            conn.commit()

    @staticmethod
    def create_from_group_for_all_users(event_id: int, group_id: int) -> None:
        """Create personal notifications for all group members based on group settings."""
        with get_conn() as conn:
            cur = conn.cursor()
            # Get all group members
            cur.execute("""
                SELECT DISTINCT u.id 
                FROM users u 
                JOIN user_group_roles ugr ON u.id = ugr.user_id 
                WHERE ugr.group_id = ? AND ugr.confirmed = 1
            """, (group_id,))
            group_members = cur.fetchall()
            
            # Get all group notification settings
            cur.execute("SELECT time_before, time_unit, message_text FROM notification_settings WHERE group_id = ?", (group_id,))
            group_notifications = cur.fetchall()
            
            # Create personal notifications for each user
            for user_id, in group_members:
                for time_before, time_unit, message_text in group_notifications:
                    # Use INSERT OR IGNORE to avoid duplicates
                    cur.execute("""
                        INSERT OR IGNORE INTO personal_event_notifications 
                        (user_id, event_id, time_before, time_unit, message_text) 
                        VALUES (?,?,?,?,?)
                    """, (user_id, event_id, time_before, time_unit, message_text))
            conn.commit()

    @staticmethod
    def list_by_user_and_event(user_id: int, event_id: int) -> List[Tuple]:
        """Return personal notifications for user and event, ordered by time_before."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, time_before, time_unit, message_text FROM personal_event_notifications WHERE user_id = ? AND event_id = ? ORDER BY time_before", (user_id, event_id))
            return cur.fetchall()

    @staticmethod
    def add_notification(user_id: int, event_id: int, time_before: int, time_unit: str, message_text: Optional[str] = None) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO personal_event_notifications (user_id, event_id, time_before, time_unit, message_text) VALUES (?,?,?,?,?)",
                        (user_id, event_id, time_before, time_unit, message_text))
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def delete_notification(notification_id: int, user_id: int) -> bool:
        """Delete personal notification, ensuring user owns it."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM personal_event_notifications WHERE id = ? AND user_id = ?", (notification_id, user_id))
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def delete_all_for_user_event(user_id: int, event_id: int) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM personal_event_notifications WHERE user_id = ? AND event_id = ?", (user_id, event_id))
            conn.commit()

    @staticmethod
    def list_by_user(user_id: int) -> List[Tuple]:
        """Return all personal notifications for user across all events."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT pen.id, pen.event_id, pen.time_before, pen.time_unit, pen.message_text, e.name, e.time, g.title
                FROM personal_event_notifications pen
                JOIN events e ON e.id = pen.event_id
                JOIN groups g ON g.id = e.group_id
                WHERE pen.user_id = ?
                ORDER BY e.time, pen.time_before
            """, (user_id,))
            return cur.fetchall()

    @staticmethod
    def list_personal_settings(user_id: int) -> List[Tuple]:
        """Return personal notification settings (without event_id) for user."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, time_before, time_unit, message_text
                FROM personal_event_notifications 
                WHERE user_id = ? AND event_id IS NULL
                ORDER BY time_before, time_unit
            """, (user_id,))
            return cur.fetchall()

    @staticmethod
    def add_personal_notification(user_id: int, time_before: int, time_unit: str, message_text: Optional[str]) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO personal_event_notifications (user_id, event_id, time_before, time_unit, message_text) 
                VALUES (?,?,?,?,?)
            """, (user_id, None, time_before, time_unit, message_text))
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def delete_personal_notification(user_id: int, notification_id: int) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM personal_event_notifications 
                WHERE id = ? AND user_id = ? AND event_id IS NULL
            """, (notification_id, user_id))
            conn.commit()


class DispatchLogRepo:
    @staticmethod
    def mark_sent(kind: str, *, user_id: Optional[int], group_id: Optional[int], event_id: int, time_before: int, time_unit: str) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR IGNORE INTO notification_dispatch_log
                (kind, user_id, group_id, event_id, time_before, time_unit)
                VALUES (?,?,?,?,?,?)
                """,
                (kind, user_id, group_id, event_id, time_before, time_unit)
            )
            conn.commit()

    @staticmethod
    def was_sent(kind: str, *, user_id: Optional[int], group_id: Optional[int], event_id: int, time_before: int, time_unit: str) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1 FROM notification_dispatch_log
                WHERE kind = ? AND COALESCE(user_id,0) = COALESCE(?,0) AND COALESCE(group_id,0) = COALESCE(?,0)
                  AND event_id = ? AND time_before = ? AND time_unit = ?
                LIMIT 1
                """,
                (kind, user_id, group_id, event_id, time_before, time_unit)
            )
            return cur.fetchone() is not None


class BookingRepo:
    @staticmethod
    def add_booking(user_id: int, event_id: int) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO bookings (user_id, event_id) VALUES (?,?)", (user_id, event_id))
            conn.commit()
            return cur.lastrowid

    @staticmethod
    def remove_booking(user_id: int, event_id: int) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM bookings WHERE user_id = ? AND event_id = ?", (user_id, event_id))
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def has_booking(user_id: int, event_id: int) -> bool:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM bookings WHERE user_id = ? AND event_id = ? LIMIT 1", (user_id, event_id))
            return cur.fetchone() is not None

    @staticmethod
    def list_event_bookings(event_id: int) -> List[Tuple[int]]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id FROM bookings WHERE event_id = ?", (event_id,))
            return cur.fetchall()

    @staticmethod
    def list_user_bookings(user_id: int) -> List[Tuple[int]]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT event_id FROM bookings WHERE user_id = ?", (user_id,))
            return cur.fetchall()

    @staticmethod
    def list_event_bookings_with_names(group_id: int, event_id: int) -> List[Tuple[int, str]]:
        """Return (user_id, name_to_show) using display_name if exists, else @username or user_id."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT b.user_id,
                       COALESCE(dn.display_name,
                                CASE WHEN u.username IS NOT NULL THEN '@' || u.username ELSE CAST(u.id AS TEXT) END) AS name_to_show
                FROM bookings b
                JOIN users u ON u.id = b.user_id
                LEFT JOIN user_display_names dn ON dn.group_id = ? AND dn.user_id = b.user_id
                WHERE b.event_id = ?
                ORDER BY name_to_show
                """,
                (group_id, event_id)
            )
            return cur.fetchall()


class DisplayNameRepo:
    @staticmethod
    def set_display_name(group_id: int, user_id: int, display_name: str) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO user_display_names (group_id, user_id, display_name) VALUES (?,?,?)\n                 ON CONFLICT(group_id, user_id) DO UPDATE SET display_name = excluded.display_name",
                (group_id, user_id, display_name)
            )
            conn.commit()

    @staticmethod
    def create_display_name_from_user_info(group_id: int, user_id: int) -> None:
        """Create display name from user's first_name and last_name"""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT first_name, last_name FROM users WHERE id = ?", (user_id,))
            row = cur.fetchone()
            if row:
                first_name, last_name = row
                display_name = f"{first_name or ''} {last_name or ''}".strip()
                if display_name:
                    cur.execute(
                        "INSERT INTO user_display_names (group_id, user_id, display_name) VALUES (?,?,?)\n                         ON CONFLICT(group_id, user_id) DO UPDATE SET display_name = excluded.display_name",
                        (group_id, user_id, display_name)
                    )
                    conn.commit()

    @staticmethod
    def get_display_name(group_id: int, user_id: int) -> Optional[str]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT display_name FROM user_display_names WHERE group_id = ? AND user_id = ?", (group_id, user_id))
            row = cur.fetchone()
            return row[0] if row else None


