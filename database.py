# database.py — ИСПРАВЛЕННАЯ ВЕРСИЯ
#
# Исправления:
#   1. Убраны вложенные транзакции (BEGIN/COMMIT/ROLLBACK внутри `with conn`).
#      sqlite3 сам управляет транзакцией через контекстный менеджер;
#      явный BEGIN вызывал OperationalError и прерывал запись.
#   2. Метод book_slot_with_scheduler теперь правильно оборачивает всё
#      в единый conn.execute-блок без вложенных BEGIN.
#   3. cancel_booking_with_scheduler — аналогичное исправление.

import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict

from models import TimeSlot, Booking, SlotAdjuster
from slot_scheduler import Slot, SmartScheduler
from config import DATABASE_FILE, TIMEZONE, DEFAULT_SLOTS, SESSION_DURATION


class Database:
    def __init__(self):
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(DATABASE_FILE)
        # Включаем внешние ключи
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    registered_date TEXT
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS time_slots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    base_time TEXT NOT NULL,
                    adjusted_time TEXT NOT NULL,
                    day TEXT NOT NULL,
                    date TEXT NOT NULL,
                    week_start TEXT NOT NULL,
                    is_available INTEGER DEFAULT 1,
                    booked_by INTEGER,
                    booking_id INTEGER,
                    UNIQUE(date, base_time)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bookings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    slot_id INTEGER NOT NULL,
                    original_time TEXT NOT NULL,
                    adjusted_time TEXT NOT NULL,
                    booking_date TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    notified_15min INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (slot_id) REFERENCES time_slots (id)
                )
            ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_slots_week ON time_slots(week_start)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_slots_available ON time_slots(is_available)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id, status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_notified ON bookings(notified_15min)')

            conn.commit()

    # ============================
    # ПОЛЬЗОВАТЕЛИ
    # ============================

    def add_user(self, user_id: int, username: Optional[str],
                 first_name: str, last_name: Optional[str]):
        with self.get_connection() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO users
                (user_id, username, first_name, last_name, registered_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name,
                  datetime.now(TIMEZONE).isoformat()))
            conn.commit()

    def get_user(self, user_id: int) -> Optional[dict]:
        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT user_id, username, first_name, last_name, registered_date
                FROM users WHERE user_id = ?
            ''', (user_id,)).fetchone()
        if row:
            return dict(zip(
                ['user_id', 'username', 'first_name', 'last_name', 'registered_date'],
                row
            ))
        return None

    def get_all_users(self) -> List[dict]:
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT user_id, username, first_name, last_name, registered_date
                FROM users ORDER BY registered_date DESC
            ''').fetchall()
        return [
            dict(zip(['user_id', 'username', 'first_name', 'last_name', 'registered_date'], r))
            for r in rows
        ]

    # ============================
    # СЛОТЫ
    # ============================

    def add_time_slot(self, base_time: str, adjusted_time: str, day: str,
                      date: str, week_start: str, is_available: bool = True) -> int:
        with self.get_connection() as conn:
            cursor = conn.execute('''
                INSERT INTO time_slots
                (base_time, adjusted_time, day, date, week_start, is_available)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (base_time, adjusted_time, day, date, week_start, int(is_available)))
            conn.commit()
            return cursor.lastrowid

    def get_slot_by_id(self, slot_id: int) -> Optional[TimeSlot]:
        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT id, base_time, adjusted_time, day, date,
                       is_available, booked_by, week_start
                FROM time_slots WHERE id = ?
            ''', (slot_id,)).fetchone()
        if row:
            return TimeSlot(id=row[0], base_time=row[1], adjusted_time=row[2],
                            day=row[3], date=row[4], is_available=bool(row[5]),
                            booked_by=row[6], week_start=row[7])
        return None

    def update_slot_time(self, slot_id: int, new_time: str) -> bool:
        with self.get_connection() as conn:
            cur = conn.execute(
                'UPDATE time_slots SET adjusted_time = ? WHERE id = ?',
                (new_time, slot_id)
            )
            conn.commit()
            return cur.rowcount > 0

    def update_slot_availability(self, slot_id: int, is_available: bool,
                                 booked_by: Optional[int] = None) -> bool:
        with self.get_connection() as conn:
            cur = conn.execute('''
                UPDATE time_slots SET is_available = ?, booked_by = ?
                WHERE id = ?
            ''', (int(is_available), booked_by, slot_id))
            conn.commit()
            return cur.rowcount > 0

    def initialize_week_slots(self, week_start: str,
                               days: Optional[List[str]] = None) -> bool:
        if days is None:
            days = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница"]

        with self.get_connection() as conn:
            count = conn.execute(
                'SELECT COUNT(*) FROM time_slots WHERE week_start = ?',
                (week_start,)
            ).fetchone()[0]

            if count > 0:
                return False

            for day_offset, day_name in enumerate(days):
                current_date = (
                    datetime.strptime(week_start, "%Y-%m-%d")
                    + timedelta(days=day_offset)
                )
                date_str = current_date.strftime("%Y-%m-%d")
                for base_time in DEFAULT_SLOTS:
                    conn.execute('''
                        INSERT INTO time_slots
                        (base_time, adjusted_time, day, date, week_start, is_available)
                        VALUES (?, ?, ?, ?, ?, 1)
                    ''', (base_time, base_time, day_name, date_str, week_start))

            conn.commit()
            return True

    def get_week_slots(self, week_start: str) -> List[TimeSlot]:
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT id, base_time, adjusted_time, day, date,
                       is_available, booked_by, week_start
                FROM time_slots
                WHERE week_start = ?
                ORDER BY date, base_time
            ''', (week_start,)).fetchall()
        return [
            TimeSlot(id=r[0], base_time=r[1], adjusted_time=r[2],
                     day=r[3], date=r[4], is_available=bool(r[5]),
                     booked_by=r[6], week_start=r[7])
            for r in rows
        ]

    def get_available_slots(self, week_start: str) -> List[TimeSlot]:
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT id, base_time, adjusted_time, day, date,
                       is_available, booked_by, week_start
                FROM time_slots
                WHERE week_start = ? AND is_available = 1
                ORDER BY date, base_time
            ''', (week_start,)).fetchall()
        return [
            TimeSlot(id=r[0], base_time=r[1], adjusted_time=r[2],
                     day=r[3], date=r[4], is_available=bool(r[5]),
                     booked_by=r[6], week_start=r[7])
            for r in rows
        ]

    def get_slots_by_date(self, date_str: str) -> List[TimeSlot]:
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT id, base_time, adjusted_time, day, date,
                       is_available, booked_by, week_start
                FROM time_slots WHERE date = ? ORDER BY base_time
            ''', (date_str,)).fetchall()
        return [
            TimeSlot(id=r[0], base_time=r[1], adjusted_time=r[2],
                     day=r[3], date=r[4], is_available=bool(r[5]),
                     booked_by=r[6], week_start=r[7])
            for r in rows
        ]

    def delete_day_slots(self, date_str: str) -> bool:
        """Полное удаление всех слотов и записей на дату (для force-delete)."""
        with self.get_connection() as conn:
            conn.execute('''
                DELETE FROM bookings
                WHERE slot_id IN (SELECT id FROM time_slots WHERE date = ?)
            ''', (date_str,))
            conn.execute('DELETE FROM time_slots WHERE date = ?', (date_str,))
            conn.commit()
        return True

    def update_day_slots_preserve_bookings(
        self, date_str: str, day: str, week_start: str, new_times: List[str]
    ) -> dict:
        """
        Обновить слоты на день БЕЗ потери существующих записей.

        - Слоты, которые уже забронированы — оставляем нетронутыми.
        - Свободные слоты, которых нет в new_times — удаляем.
        - Новые времена, которых ещё нет — добавляем.
        - Возвращает dict с информацией об изменениях.
        """
        with self.get_connection() as conn:
            # Текущие слоты на день
            existing = conn.execute('''
                SELECT id, base_time, is_available, booked_by
                FROM time_slots WHERE date = ?
                ORDER BY base_time
            ''', (date_str,)).fetchall()

            booked_times = set()
            free_slot_ids_to_delete = []

            for row in existing:
                slot_id, base_time, is_available, booked_by = row
                if not is_available or booked_by is not None:
                    # Занятый слот — сохраняем, запоминаем его время
                    booked_times.add(base_time)
                else:
                    # Свободный слот — удалим если не в new_times
                    if base_time not in new_times:
                        free_slot_ids_to_delete.append(slot_id)

            # Удаляем ненужные свободные слоты
            for slot_id in free_slot_ids_to_delete:
                conn.execute('DELETE FROM time_slots WHERE id = ?', (slot_id,))

            # Существующие базовые времена (включая занятые)
            existing_base_times = {r[1] for r in existing}

            # Добавляем новые времена (которых ещё нет)
            added = []
            for t in new_times:
                if t not in existing_base_times:
                    conn.execute('''
                        INSERT INTO time_slots
                        (base_time, adjusted_time, day, date, week_start, is_available)
                        VALUES (?, ?, ?, ?, ?, 1)
                    ''', (t, t, day, date_str, week_start))
                    added.append(t)

            conn.commit()

        return {
            'preserved_bookings': list(booked_times),
            'deleted_free_slots': len(free_slot_ids_to_delete),
            'added_slots': added,
        }

    def reset_week_slots(self, week_start: str) -> bool:
        with self.get_connection() as conn:
            conn.execute('''
                UPDATE time_slots SET adjusted_time = base_time
                WHERE week_start = ?
            ''', (week_start,))
            conn.commit()
        return True

    # ============================
    # УМНЫЙ ПЛАНИРОВЩИК
    # ============================

    def get_all_slots_for_scheduling(self, week_start: str) -> List[Slot]:
        """Получить все слоты недели в формате, понятном SmartScheduler."""
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT id, base_time, adjusted_time, day, date, week_start,
                       CASE WHEN is_available = 0 THEN 1 ELSE 0 END AS is_booked,
                       booked_by
                FROM time_slots
                WHERE week_start = ?
                ORDER BY date, base_time
            ''', (week_start,)).fetchall()
        return [
            Slot(id=r[0], base_time=r[1], current_time=r[2],
                 day=r[3], date=r[4], week_start=r[5],
                 is_booked=bool(r[6]), booked_by=r[7])
            for r in rows
        ]

    def apply_slot_changes(self, changes: Dict[int, str]):
        """Применить изменения времён слотов к БД."""
        with self.get_connection() as conn:
            for slot_id, new_time in changes.items():
                conn.execute(
                    'UPDATE time_slots SET adjusted_time = ? WHERE id = ?',
                    (new_time, slot_id)
                )
            conn.commit()

    def book_slot_with_scheduler(
        self, user_id: int, slot_id: int, week_start: str, is_admin: bool = False
    ) -> Tuple[bool, Optional[str], Optional[Dict]]:
        """
        Забронировать слот через SmartScheduler.

        ИСПРАВЛЕНО: убраны вложенные BEGIN/COMMIT/ROLLBACK.
        sqlite3 с `with conn` сам откатывает при исключении.

        Возвращает: (success, booked_time_or_error, slot_info_dict)
        """
        all_slots = self.get_all_slots_for_scheduling(week_start)
        if not all_slots:
            return False, "Нет слотов на эту неделю", None

        scheduler = SmartScheduler(all_slots)
        success, booked_time, changes = scheduler.book_slot(slot_id, user_id, is_admin=is_admin)

        if not success:
            return False, booked_time, None

        try:
            with self.get_connection() as conn:
                # Обновляем времена и доступность всех слотов
                for slot in scheduler.slots:
                    conn.execute('''
                        UPDATE time_slots
                        SET adjusted_time = ?,
                            is_available  = ?,
                            booked_by     = ?
                        WHERE id = ?
                    ''', (
                        slot.current_time,
                        0 if slot.is_booked else 1,
                        slot.booked_by,
                        slot.id
                    ))

                # Создаём запись о бронировании
                booked_slot = scheduler.find_slot_by_id(slot_id)
                cursor = conn.execute('''
                    INSERT INTO bookings
                    (user_id, slot_id, original_time, adjusted_time, booking_date, status)
                    VALUES (?, ?, ?, ?, ?, 'active')
                ''', (
                    user_id,
                    slot_id,
                    booked_slot.base_time,
                    booked_time,
                    datetime.now(TIMEZONE).isoformat()
                ))
                booking_id = cursor.lastrowid

                # Привязываем booking_id к слоту
                conn.execute(
                    'UPDATE time_slots SET booking_id = ? WHERE id = ?',
                    (booking_id, slot_id)
                )

                conn.commit()

            slot_info = {
                'day': booked_slot.day,
                'date': booked_slot.date,
                'time': booked_time,
                'base_time': booked_slot.base_time,
            }
            return True, booked_time, slot_info

        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"book_slot_with_scheduler error: {e}")
            return False, f"Ошибка базы данных: {e}", None

    def cancel_booking_with_scheduler(self, user_id: int, booking_id: int) -> bool:
        """
        Отменить запись и пересчитать расписание.

        ИСПРАВЛЕНО: убраны вложенные BEGIN/COMMIT/ROLLBACK.
        """
        booking = self.get_booking_by_id(booking_id)
        if not booking or booking['user_id'] != user_id:
            return False

        week_start = booking['week_start']
        all_slots = self.get_all_slots_for_scheduling(week_start)
        scheduler = SmartScheduler(all_slots)

        success, changes = scheduler.cancel_booking(booking['slot_id'])
        if not success:
            return False

        try:
            with self.get_connection() as conn:
                for slot in scheduler.slots:
                    conn.execute('''
                        UPDATE time_slots
                        SET adjusted_time = ?,
                            is_available  = ?,
                            booked_by     = ?
                        WHERE id = ?
                    ''', (
                        slot.current_time,
                        0 if slot.is_booked else 1,
                        slot.booked_by,
                        slot.id
                    ))

                conn.execute(
                    "UPDATE bookings SET status = 'cancelled' WHERE id = ?",
                    (booking_id,)
                )
                conn.commit()
            return True

        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"cancel_booking_with_scheduler error: {e}")
            return False

    # ============================
    # БРОНИРОВАНИЯ
    # ============================

    def book_slot(self, user_id: int, slot_id: int) -> Tuple[bool, Optional[str]]:
        """Старый метод для обратной совместимости."""
        slot = self.get_slot_by_id(slot_id)
        if not slot:
            return False, "Слот не найден"
        return self.book_slot_with_scheduler(user_id, slot_id, slot.week_start)[:2]

    def get_user_active_booking(self, user_id: int) -> Optional[dict]:
        """
        Первая активная запись пользователя на текущей КАЛЕНДАРНОЙ неделе
        (пн-вс по дате слота), время которой ещё не прошло.
        Привязка идёт к дате слота, а не к week_start — это позволяет
        корректно работать со слотами на следующую неделю.
        """
        today = datetime.now(TIMEZONE).date()
        # Начало и конец текущей календарной недели (пн-вс)
        week_monday = today - timedelta(days=today.weekday())
        week_sunday = week_monday + timedelta(days=6)
        week_monday_str = week_monday.strftime("%Y-%m-%d")
        week_sunday_str = week_sunday.strftime("%Y-%m-%d")
        now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT b.id, ts.day, ts.adjusted_time, ts.date,
                       ts.base_time, ts.week_start
                FROM bookings b
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.user_id = ?
                  AND b.status  = 'active'
                  AND ts.date >= ?
                  AND ts.date <= ?
                  AND (ts.date || ' ' || ts.adjusted_time) > ?
                ORDER BY ts.date, ts.base_time
                LIMIT 1
            ''', (user_id, week_monday_str, week_sunday_str, now_str)).fetchone()

        if row:
            return {
                'booking_id': row[0], 'day': row[1], 'time': row[2],
                'date': row[3], 'base_time': row[4], 'week_start': row[5]
            }
        return None

    def get_user_active_bookings_count(self, user_id: int) -> int:
        """Количество активных предстоящих записей на текущей календарной неделе."""
        today = datetime.now(TIMEZONE).date()
        week_monday = today - timedelta(days=today.weekday())
        week_sunday = week_monday + timedelta(days=6)
        week_monday_str = week_monday.strftime("%Y-%m-%d")
        week_sunday_str = week_sunday.strftime("%Y-%m-%d")
        now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
        with self.get_connection() as conn:
            count = conn.execute('''
                SELECT COUNT(*) FROM bookings b
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.user_id = ?
                  AND b.status  = 'active'
                  AND ts.date >= ?
                  AND ts.date <= ?
                  AND (ts.date || ' ' || ts.adjusted_time) > ?
            ''', (user_id, week_monday_str, week_sunday_str, now_str)).fetchone()[0]
        return count

    def get_users_with_no_booking_this_week(self) -> List[dict]:
        """
        Получить всех пользователей, у которых НЕТ активной записи на текущей неделе
        (для уведомления о новых слотах).
        """
        today = datetime.now(TIMEZONE).date()
        week_start = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT u.user_id, u.first_name, u.username
                FROM users u
                WHERE u.user_id NOT IN (
                    SELECT b.user_id FROM bookings b
                    JOIN time_slots ts ON b.slot_id = ts.id
                    WHERE b.status = 'active'
                      AND ts.week_start = ?
                )
            ''', (week_start,)).fetchall()
        return [{'user_id': r[0], 'first_name': r[1], 'username': r[2]} for r in rows]

    def cancel_booking(self, user_id: int, booking_id: Optional[int] = None) -> bool:
        """Старый метод для обратной совместимости."""
        return self.cancel_booking_with_scheduler(user_id, booking_id)

    def get_booking_by_id(self, booking_id: int) -> Optional[dict]:
        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT b.id, b.user_id, b.slot_id, b.original_time,
                       b.adjusted_time, b.booking_date, b.status,
                       ts.day, ts.date, ts.week_start,
                       u.first_name, u.username
                FROM bookings b
                JOIN time_slots ts ON b.slot_id = ts.id
                JOIN users u ON b.user_id = u.user_id
                WHERE b.id = ?
            ''', (booking_id,)).fetchone()

        if row:
            return {
                'id': row[0], 'user_id': row[1], 'slot_id': row[2],
                'original_time': row[3], 'adjusted_time': row[4],
                'booking_date': row[5], 'status': row[6],
                'day': row[7], 'date': row[8], 'week_start': row[9],
                'user_name': row[10], 'username': row[11]
            }
        return None

    def get_booking_by_slot_id(self, slot_id: int) -> Optional[dict]:
        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT b.id, b.user_id, u.first_name, u.username
                FROM bookings b
                JOIN users u ON b.user_id = u.user_id
                WHERE b.slot_id = ? AND b.status = 'active'
                LIMIT 1
            ''', (slot_id,)).fetchone()

        if row:
            return {
                'booking_id': row[0], 'user_id': row[1],
                'user_name': row[2], 'username': row[3]
            }
        return None

    def get_all_bookings(self) -> List[dict]:
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT b.id, u.first_name, u.username, u.user_id,
                       ts.day, ts.date, ts.adjusted_time, ts.base_time,
                       b.booking_date
                FROM bookings b
                JOIN users u  ON b.user_id  = u.user_id
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.status = 'active'
                ORDER BY ts.date, ts.base_time
            ''').fetchall()
        return [
            {
                'booking_id': r[0], 'user_name': r[1], 'username': r[2],
                'user_id': r[3], 'day': r[4], 'date': r[5],
                'time': r[6], 'base_time': r[7], 'booked_at': r[8]
            }
            for r in rows
        ]

    def get_bookings_for_date(self, date_str: str) -> List[dict]:
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT b.id, u.first_name, u.username, u.user_id,
                       ts.day, ts.adjusted_time
                FROM bookings b
                JOIN users u  ON b.user_id  = u.user_id
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.status = 'active' AND ts.date = ?
                ORDER BY ts.base_time
            ''', (date_str,)).fetchall()
        return [
            {
                'booking_id': r[0], 'user_name': r[1], 'username': r[2],
                'user_id': r[3], 'day': r[4], 'time': r[5]
            }
            for r in rows
        ]

    # ============================
    # НАПОМИНАНИЯ
    # ============================

    def get_upcoming_sessions(self, minutes_before: int = 15) -> List[dict]:
        """
        Найти сессии, которые начнутся примерно через minutes_before минут
        (±3 минуты — защита от пропуска при проверке каждые 5 мин).
        Время рассчитывается по московскому часовому поясу.
        Уведомление отправляется только один раз (notified_15min = 0).
        """
        now = datetime.now(TIMEZONE)
        # Окно поиска: от (minutes_before - 3) до (minutes_before + 3) минут
        window_start = now + timedelta(minutes=minutes_before - 3)
        window_end   = now + timedelta(minutes=minutes_before + 3)

        # Для поиска по дате используем дату целевого времени
        target_date = (now + timedelta(minutes=minutes_before)).strftime("%Y-%m-%d")
        ws_hm = window_start.strftime("%H:%M")
        we_hm = window_end.strftime("%H:%M")

        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT b.id, b.user_id, u.first_name, u.username,
                       ts.day, ts.adjusted_time, ts.date
                FROM bookings b
                JOIN users u  ON b.user_id  = u.user_id
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.status = 'active'
                  AND ts.date          = ?
                  AND ts.adjusted_time >= ?
                  AND ts.adjusted_time <= ?
                  AND b.notified_15min = 0
            ''', (target_date, ws_hm, we_hm)).fetchall()

            sessions = [
                {
                    'booking_id': r[0], 'user_id': r[1], 'name': r[2],
                    'username': r[3], 'day': r[4], 'time': r[5], 'date': r[6]
                }
                for r in rows
            ]

            # Помечаем как уведомлённые сразу, чтобы не отправить дважды
            for s in sessions:
                conn.execute(
                    'UPDATE bookings SET notified_15min = 1 WHERE id = ?',
                    (s['booking_id'],)
                )
            conn.commit()

        return sessions

    def mark_notification_sent(self, booking_id: int) -> bool:
        with self.get_connection() as conn:
            cur = conn.execute(
                'UPDATE bookings SET notified_15min = 1 WHERE id = ?',
                (booking_id,)
            )
            conn.commit()
            return cur.rowcount > 0

    def reset_notifications(self, week_start: Optional[str] = None):
        with self.get_connection() as conn:
            if week_start:
                conn.execute('''
                    UPDATE bookings SET notified_15min = 0
                    WHERE slot_id IN (
                        SELECT id FROM time_slots WHERE week_start = ?
                    )
                ''', (week_start,))
            else:
                conn.execute('UPDATE bookings SET notified_15min = 0')
            conn.commit()

    # ============================
    # СТАТИСТИКА И ОЧИСТКА
    # ============================

    def get_all_active_bookings(self) -> List[dict]:
        return self.get_all_bookings()

    def get_week_statistics(self, week_start: str) -> dict:
        with self.get_connection() as conn:
            total = conn.execute(
                'SELECT COUNT(*) FROM time_slots WHERE week_start = ?',
                (week_start,)
            ).fetchone()[0]

            booked = conn.execute(
                'SELECT COUNT(*) FROM time_slots WHERE week_start = ? AND is_available = 0',
                (week_start,)
            ).fetchone()[0]

            clients = conn.execute('''
                SELECT COUNT(DISTINCT b.user_id)
                FROM bookings b
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE ts.week_start = ? AND b.status = 'active'
            ''', (week_start,)).fetchone()[0]

        return {
            'total_slots': total,
            'booked_slots': booked,
            'available_slots': total - booked,
            'unique_clients': clients,
            'week_start': week_start,
        }

    def cleanup_old_data(self, weeks_to_keep: int = 4) -> dict:
        cutoff = (
            datetime.now(TIMEZONE) - timedelta(weeks=weeks_to_keep)
        ).strftime("%Y-%m-%d")

        with self.get_connection() as conn:
            cur1 = conn.execute('''
                DELETE FROM bookings
                WHERE status IN ('cancelled', 'completed')
                  AND booking_date < ?
            ''', (cutoff,))
            cur2 = conn.execute(
                'DELETE FROM time_slots WHERE week_start < ?', (cutoff,)
            )
            conn.commit()

        return {
            'deleted_bookings': cur1.rowcount,
            'deleted_slots': cur2.rowcount,
        }
