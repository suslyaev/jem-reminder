import sqlite3
from typing import List, Tuple, Optional

class GroupManager:
    def __init__(self, db_path: str = 'events.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def create_group(self, chat_id: str, title: str, created_by: int) -> int:
        """Создает новую группу в базе данных"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO groups (chat_id, title, created_by) VALUES (?, ?, ?)",
                (chat_id, title, created_by)
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()
    
    def get_group_by_chat_id(self, chat_id: str) -> Optional[Tuple[int, str, int]]:
        """Получает информацию о группе по chat_id"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, title, created_by FROM groups WHERE chat_id = ?", (chat_id,))
            return cursor.fetchone()
        finally:
            conn.close()
    
    def get_group_by_id(self, group_id: int) -> Optional[Tuple[int, str, str, int]]:
        """Получает информацию о группе по ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, chat_id, title, created_by FROM groups WHERE id = ?", (group_id,))
            return cursor.fetchone()
        finally:
            conn.close()
    
    def get_all_groups(self) -> List[Tuple[int, str, str, int]]:
        """Получает список всех групп"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, chat_id, title, created_by FROM groups ORDER BY id")
            return cursor.fetchall()
        finally:
            conn.close()
    
    def add_group_admin(self, group_id: int, admin_id: int) -> bool:
        """Добавляет админа в группу"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO group_admins (group_id, admin_id) VALUES (?, ?)",
                (group_id, admin_id)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Админ уже существует
        finally:
            conn.close()
    
    def remove_group_admin(self, group_id: int, admin_id: int) -> bool:
        """Удаляет админа из группы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM group_admins WHERE group_id = ? AND admin_id = ?",
                (group_id, admin_id)
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def is_group_admin(self, group_id: int, user_id: int) -> bool:
        """Проверяет, является ли пользователь админом группы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT 1 FROM group_admins WHERE group_id = ? AND admin_id = ?",
                (group_id, user_id)
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()
    
    def is_group_creator(self, group_id: int, user_id: int) -> bool:
        """Проверяет, является ли пользователь создателем группы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT 1 FROM groups WHERE id = ? AND created_by = ?",
                (group_id, user_id)
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()
    
    def get_user_admin_groups(self, user_id: int) -> List[Tuple[int, str]]:
        """Получает список групп, где пользователь является админом"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT g.id, g.title 
                FROM groups g 
                LEFT JOIN group_admins ga ON g.id = ga.group_id 
                WHERE g.created_by = ? OR ga.admin_id = ?
                ORDER BY g.id
            """, (user_id, user_id))
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_group_admins(self, group_id: int) -> List[int]:
        """Получает список админов группы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT admin_id FROM group_admins WHERE group_id = ?
                UNION
                SELECT created_by FROM groups WHERE id = ?
            """, (group_id, group_id))
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def delete_group(self, group_id: int) -> bool:
        """Удаляет группу и всех её админов"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM group_admins WHERE group_id = ?", (group_id,))
            cursor.execute("DELETE FROM groups WHERE id = ?", (group_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
