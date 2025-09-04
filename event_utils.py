import sqlite3
from typing import List, Tuple, Optional
from datetime import datetime

class EventManager:
    def __init__(self, db_path: str = 'events.db'):
        self.db_path = db_path
    
    def _get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def create_event(self, name: str, time: datetime, group_id: int, responsible: Optional[str] = None) -> int:
        """Создает новое мероприятие"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO events (name, time, responsible, group_id) VALUES (?, ?, ?, ?)",
                (name, time.strftime('%Y-%m-%d %H:%M:%S'), responsible, group_id)
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()
    
    def get_event(self, event_id: int) -> Optional[Tuple[int, str, str, str, int]]:
        """Получает мероприятие по ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, name, time, responsible, group_id FROM events WHERE id = ?", (event_id,))
            return cursor.fetchone()
        finally:
            conn.close()
    
    def get_events_by_group(self, group_id: int) -> List[Tuple[int, str, str, str, int]]:
        """Получает все мероприятия группы"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT id, name, time, responsible, group_id FROM events WHERE group_id = ? ORDER BY time ASC",
                (group_id,)
            )
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_upcoming_events(self, group_id: int, days: int = 30) -> List[Tuple[int, str, str, str, int]]:
        """Получает предстоящие мероприятия группы на указанное количество дней"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            from datetime import timedelta
            now = datetime.now()
            future_date = now + timedelta(days=days)
            
            cursor.execute("""
                SELECT id, name, time, responsible, group_id 
                FROM events 
                WHERE group_id = ? AND time BETWEEN ? AND ? 
                ORDER BY time ASC
            """, (group_id, now.strftime('%Y-%m-%d %H:%M:%S'), future_date.strftime('%Y-%m-%d %H:%M:%S')))
            return cursor.fetchall()
        finally:
            conn.close()
    
    def update_event_responsible(self, event_id: int, responsible: Optional[str]) -> bool:
        """Обновляет ответственного за мероприятие"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE events SET responsible = ? WHERE id = ?",
                (responsible, event_id)
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def delete_event(self, event_id: int) -> bool:
        """Удаляет мероприятие"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM events WHERE id = ?", (event_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def get_all_events(self) -> List[Tuple[int, str, str, str, int]]:
        """Получает все мероприятия"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, name, time, responsible, group_id FROM events ORDER BY time ASC")
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_events_by_time_range(self, start_time: datetime, end_time: datetime) -> List[Tuple[int, str, str, str, int]]:
        """Получает мероприятия в указанном временном диапазоне"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, name, time, responsible, group_id 
                FROM events 
                WHERE time BETWEEN ? AND ? 
                ORDER BY time ASC
            """, (start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')))
            return cursor.fetchall()
        finally:
            conn.close()
