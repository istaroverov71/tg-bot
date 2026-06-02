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

from models import TimeSlot, Booking
from slot_scheduler import Slot, SmartScheduler
from config import DATABASE_FILE, TIMEZONE, DEFAULT_SLOTS, SESSION_DURATION


class Database:
    def __init__(self):
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        # timeout=30: ждём освобождения блокировки до 30с (а не падаем сразу),
        # busy_timeout дублирует это на уровне SQLite. WAL разрешает читать во время
        # записи — вместе с BEGIN IMMEDIATE снижает риск "database is locked" (H1).
        conn = sqlite3.connect(DATABASE_FILE, timeout=30)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
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
                    notified_24h INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (slot_id) REFERENCES time_slots (id)
                )
            ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_slots_week ON time_slots(week_start)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_slots_available ON time_slots(is_available)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id, status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_notified ON bookings(notified_15min)')

            # Миграции (идемпотентные — безопасно запускать на существующей БД)
            existing_user_cols = [row[1] for row in cursor.execute("PRAGMA table_info(users)").fetchall()]
            if 'is_active' not in existing_user_cols:
                cursor.execute('ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1')

            existing_booking_cols = [row[1] for row in cursor.execute("PRAGMA table_info(bookings)").fetchall()]
            if 'notified_24h' not in existing_booking_cols:
                cursor.execute('ALTER TABLE bookings ADD COLUMN notified_24h INTEGER DEFAULT 0')

            conn.commit()

    # ============================
    # ПОЛЬЗОВАТЕЛИ
    # ============================

    def add_user(self, user_id: int, username: Optional[str],
                 first_name: str, last_name: Optional[str]):
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO users
                    (user_id, username, first_name, last_name, registered_date, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(user_id) DO UPDATE SET
                    username       = excluded.username,
                    first_name     = excluded.first_name,
                    last_name      = excluded.last_name,
                    is_active      = 1
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
        """Только активные пользователи (не заблокировавшие бота)."""
        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT user_id, username, first_name, last_name, registered_date
                FROM users
                WHERE is_active = 1
                ORDER BY registered_date DESC
            ''').fetchall()
        return [
            dict(zip(['user_id', 'username', 'first_name', 'last_name', 'registered_date'], r))
            for r in rows
        ]

    def deactivate_user(self, user_id: int) -> bool:
        """
        Пометить пользователя как неактивного (заблокировал бота).
        Такой пользователь исключается из всех рассылок и напоминаний.
        При следующем /start is_active автоматически вернётся в 1.
        """
        with self.get_connection() as conn:
            cur = conn.execute(
                'UPDATE users SET is_active = 0 WHERE user_id = ?',
                (user_id,)
            )
            conn.commit()
            return cur.rowcount > 0

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
        Добавить новые слоты к существующим на указанный день.

        - Существующие слоты (свободные и занятые) не удаляются.
        - Новые времена, которых ещё нет — добавляются.
        - Возвращает dict с информацией об изменениях.
        """
        with self.get_connection() as conn:
            # Существующие базовые времена (слоты не удаляем — только добавляем)
            existing = conn.execute('''
                SELECT base_time FROM time_slots WHERE date = ?
            ''', (date_str,)).fetchall()
            existing_base_times = {r[0] for r in existing}

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
            'preserved_bookings': [],
            'deleted_free_slots': 0,
            'added_slots': added,
        }

    def delete_free_slots(self, date_str: str, times: List[str]) -> dict:
        """
        Удалить конкретные свободные слоты по дате и списку времён.

        Для каждого времени:
          - Слот не найден          → попадает в 'not_found'
          - Есть активная запись    → попадает в 'booked' (не трогаем)
          - Свободен                → удаляем (сначала чистим hist. записи по FK)

        Возвращает: {'deleted': [...], 'booked': [...], 'not_found': [...]}
        """
        deleted = []
        booked = []
        not_found = []

        with self.get_connection() as conn:
            for t in times:
                row = conn.execute(
                    '''SELECT id, is_available FROM time_slots
                       WHERE date = ? AND (base_time = ? OR adjusted_time = ?)''',
                    (date_str, t, t),
                ).fetchone()

                if not row:
                    not_found.append(t)
                    continue

                slot_id, is_available = row

                # Проверяем активную запись (cancelled/completed — не считаются)
                active = conn.execute(
                    "SELECT COUNT(*) FROM bookings WHERE slot_id = ? AND status = 'active'",
                    (slot_id,),
                ).fetchone()[0]

                if active > 0:
                    booked.append(t)
                    continue

                # Удаляем исторические записи (cancelled/completed) — иначе FK не даст
                conn.execute(
                    "DELETE FROM bookings WHERE slot_id = ? AND status IN ('cancelled', 'completed')",
                    (slot_id,),
                )
                conn.execute('DELETE FROM time_slots WHERE id = ?', (slot_id,))
                deleted.append(t)

            conn.commit()

        return {'deleted': deleted, 'booked': booked, 'not_found': not_found}

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

        Конкурентность (C2): весь цикл «прочитать снимок недели → пересчитать
        планировщиком → записать» выполняется в ОДНОЙ транзакции BEGIN IMMEDIATE.
        Захват write-lock сериализует параллельные брони, что исключает:
          • lost update (затирание чужой записи устаревшим снимком),
          • двойную бронь одного слота.

        Календарная неделя (M1): обычный пользователь не может иметь больше одной
        НЕотменённой записи (active/completed) в пределах одной календарной недели
        (пн–вс) — даже если его сессия на этой неделе уже прошла. Записаться снова
        он сможет только на слоты следующей недели. Для администратора лимита нет.

        Возвращает: (success, booked_time_or_error, slot_info_dict)
        """
        conn = self.get_connection()
        try:
            conn.isolation_level = None         # управляем транзакцией вручную
            conn.execute("BEGIN IMMEDIATE")     # write-lock: сериализует конкурентные брони

            # Свежий снимок слотов недели — ВНУТРИ транзакции (фикс гонки C2)
            rows = conn.execute('''
                SELECT id, base_time, adjusted_time, day, date, week_start,
                       CASE WHEN is_available = 0 THEN 1 ELSE 0 END AS is_booked,
                       booked_by
                FROM time_slots
                WHERE week_start = ?
                ORDER BY date, base_time
            ''', (week_start,)).fetchall()

            if not rows:
                conn.execute("ROLLBACK")
                return False, "Нет слотов на эту неделю", None

            all_slots = [
                Slot(id=r[0], base_time=r[1], current_time=r[2],
                     day=r[3], date=r[4], week_start=r[5],
                     is_booked=bool(r[6]), booked_by=r[7])
                for r in rows
            ]

            # M1: одна запись в календарную неделю (пн–вс) для обычного пользователя.
            # Считаем все НЕотменённые записи (active + completed) → прошедшая сессия
            # тоже занимает неделю.
            if not is_admin:
                ws_dt = datetime.strptime(week_start, "%Y-%m-%d")
                week_sunday = (ws_dt + timedelta(days=6)).strftime("%Y-%m-%d")
                already = conn.execute('''
                    SELECT COUNT(*) FROM bookings b
                    JOIN time_slots ts ON b.slot_id = ts.id
                    WHERE b.user_id = ?
                      AND b.status != 'cancelled'
                      AND ts.date >= ?
                      AND ts.date <= ?
                ''', (user_id, week_start, week_sunday)).fetchone()[0]
                if already > 0:
                    conn.execute("ROLLBACK")
                    return False, (
                        "⚠️ У вас уже есть запись на этой неделе.\n\n"
                        "На одной неделе можно записаться только один раз. "
                        "Записаться снова можно будет на слоты следующей недели."
                    ), None

            scheduler = SmartScheduler(all_slots)
            success, booked_time, changes = scheduler.book_slot(slot_id, user_id, is_admin=is_admin)
            if not success:
                conn.execute("ROLLBACK")
                return False, booked_time, None

            booked_slot = scheduler.find_slot_by_id(slot_id)

            # Записываем рассчитанные позиции всех слотов недели
            for slot in scheduler.slots:
                conn.execute('''
                    UPDATE time_slots
                    SET adjusted_time = ?, is_available = ?, booked_by = ?
                    WHERE id = ?
                ''', (
                    slot.current_time,
                    0 if slot.is_booked else 1,
                    slot.booked_by,
                    slot.id
                ))

            # Создаём запись о бронировании
            cursor = conn.execute('''
                INSERT INTO bookings
                (user_id, slot_id, original_time, adjusted_time, booking_date, status)
                VALUES (?, ?, ?, ?, ?, 'active')
            ''', (
                user_id, slot_id, booked_slot.base_time, booked_time,
                datetime.now(TIMEZONE).isoformat()
            ))
            booking_id = cursor.lastrowid
            conn.execute(
                'UPDATE time_slots SET booking_id = ? WHERE id = ?',
                (booking_id, slot_id)
            )

            conn.execute("COMMIT")

            slot_info = {
                'day': booked_slot.day,
                'date': booked_slot.date,
                'time': booked_time,
                'base_time': booked_slot.base_time,
            }
            return True, booked_time, slot_info

        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            import logging
            logging.getLogger(__name__).error(f"book_slot_with_scheduler error: {e}")
            return False, f"Ошибка базы данных: {e}", None
        finally:
            conn.close()

    def cancel_booking_with_scheduler(self, user_id: int, booking_id: int) -> bool:
        """
        Отменить запись и пересчитать расписание.

        Конкурентность (C2): чтение снимка недели, пересчёт и запись выполняются
        в одной транзакции BEGIN IMMEDIATE — как и при бронировании.
        """
        if booking_id is None:
            return False

        conn = self.get_connection()
        try:
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")

            # Проверяем владельца и получаем слот/неделю внутри транзакции
            brow = conn.execute('''
                SELECT b.slot_id, b.user_id, ts.week_start
                FROM bookings b
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.id = ?
            ''', (booking_id,)).fetchone()

            if not brow or brow[1] != user_id:
                conn.execute("ROLLBACK")
                return False

            slot_id, _owner, week_start = brow

            rows = conn.execute('''
                SELECT id, base_time, adjusted_time, day, date, week_start,
                       CASE WHEN is_available = 0 THEN 1 ELSE 0 END AS is_booked,
                       booked_by
                FROM time_slots
                WHERE week_start = ?
                ORDER BY date, base_time
            ''', (week_start,)).fetchall()

            all_slots = [
                Slot(id=r[0], base_time=r[1], current_time=r[2],
                     day=r[3], date=r[4], week_start=r[5],
                     is_booked=bool(r[6]), booked_by=r[7])
                for r in rows
            ]

            scheduler = SmartScheduler(all_slots)
            success, changes = scheduler.cancel_booking(slot_id)
            if not success:
                conn.execute("ROLLBACK")
                return False

            for slot in scheduler.slots:
                conn.execute('''
                    UPDATE time_slots
                    SET adjusted_time = ?, is_available = ?, booked_by = ?
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
            conn.execute("COMMIT")
            return True

        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            import logging
            logging.getLogger(__name__).error(f"cancel_booking_with_scheduler error: {e}")
            return False
        finally:
            conn.close()

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
        Первая активная запись пользователя, время которой ещё не прошло.
        Без ограничения по неделе — находит записи на текущей И следующей неделе.
        """
        now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT b.id, ts.day, ts.adjusted_time, ts.date,
                       ts.base_time, ts.week_start
                FROM bookings b
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.user_id = ?
                  AND b.status  = 'active'
                  AND (ts.date || ' ' || ts.adjusted_time) > ?
                ORDER BY ts.date, ts.base_time
                LIMIT 1
            ''', (user_id, now_str)).fetchone()

        if row:
            return {
                'booking_id': row[0], 'day': row[1], 'time': row[2],
                'date': row[3], 'base_time': row[4], 'week_start': row[5]
            }
        return None

    def get_user_booking_in_week(self, user_id: int, week_monday_str: str,
                                 week_sunday_str: str) -> Optional[dict]:
        """
        Запись пользователя в пределах календарной недели [пн, вс].

        Учитывает все НЕотменённые записи (status != 'cancelled'), т.е. и активные,
        и уже прошедшие (completed). Нужна для правила «одна запись в календарную
        неделю» (M1): если клиент уже был на сессии этой недели — он считается
        занятым на эту неделю и может записаться только на следующую.

        Возвращает данные ближайшей такой записи или None.
        """
        with self.get_connection() as conn:
            row = conn.execute('''
                SELECT b.id, ts.day, ts.adjusted_time, ts.date,
                       ts.base_time, ts.week_start
                FROM bookings b
                JOIN time_slots ts ON b.slot_id = ts.id
                WHERE b.user_id = ?
                  AND b.status != 'cancelled'
                  AND ts.date >= ?
                  AND ts.date <= ?
                ORDER BY ts.date, ts.base_time
                LIMIT 1
            ''', (user_id, week_monday_str, week_sunday_str)).fetchone()
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

    def complete_past_sessions(self) -> int:
        """
        Перевести прошедшие активные сессии в статус 'completed'.
        Вызывается перед выборкой активных записей, чтобы пользователи,
        чьи сессии уже состоялись, снова попадали в список для уведомлений.
        Возвращает количество завершённых записей.
        """
        now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
        with self.get_connection() as conn:
            cur = conn.execute('''
                UPDATE bookings SET status = 'completed'
                WHERE status = 'active'
                  AND slot_id IN (
                      SELECT id FROM time_slots
                      WHERE (date || ' ' || adjusted_time) < ?
                  )
            ''', (now_str,))
            conn.commit()
            return cur.rowcount

    def get_users_without_booking_on_week(self, week_monday_str: str, week_sunday_str: str) -> List[dict]:
        """
        Получить пользователей без активной БУДУЩЕЙ записи на указанной неделе
        (пн-вс задаётся строками YYYY-MM-DD).

        Перед выборкой завершает прошедшие сессии.
        Используется для рассылки: если слоты добавляются на текущую неделю —
        уведомляем только тех, кто на неё не записан.
        """
        self.complete_past_sessions()
        now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

        with self.get_connection() as conn:
            rows = conn.execute('''
                SELECT u.user_id, u.first_name, u.username
                FROM users u
                WHERE u.is_active = 1
                  AND u.user_id NOT IN (
                    SELECT b.user_id FROM bookings b
                    JOIN time_slots ts ON b.slot_id = ts.id
                    WHERE b.status = 'active'
                      AND ts.date >= ?
                      AND ts.date <= ?
                      AND (ts.date || ' ' || ts.adjusted_time) > ?
                )
            ''', (week_monday_str, week_sunday_str, now_str)).fetchall()
        return [{'user_id': r[0], 'first_name': r[1], 'username': r[2]} for r in rows]

    # Обратная совместимость
    def get_users_with_no_booking_this_week(self) -> List[dict]:
        today = datetime.now(TIMEZONE).date()
        week_monday = today - timedelta(days=today.weekday())
        week_sunday = week_monday + timedelta(days=6)
        return self.get_users_without_booking_on_week(
            week_monday.strftime("%Y-%m-%d"),
            week_sunday.strftime("%Y-%m-%d"),
        )

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

    def _get_due_sessions(self, minutes_before: int, grace_minutes: int,
                          flag_column: str) -> List[dict]:
        """
        Найти сессии, до начала которых осталось <= minutes_before минут,
        но > (minutes_before - grace_minutes) минут.

        Ключевое свойство: напоминание НИКОГДА не отправляется раньше, чем
        за minutes_before минут до сессии (нет «за 18 минут» / «за 24ч 3мин»).
        grace_minutes — окно «подхвата»: на случай сбоя отправки (флаг сбрасывается
        и попытка повторяется) или кратковременного простоя бота.

        Сравнение идёт по полному datetime (date + time), поэтому окно корректно
        работает и при переходе через полночь. Время — московское.
        Найденные записи сразу помечаются flag_column=1 (защита от дублей).
        """
        # Защита: имя колонки подставляется в SQL, поэтому жёстко валидируем.
        if flag_column not in ('notified_15min', 'notified_24h'):
            raise ValueError(f"Недопустимая колонка флага: {flag_column}")

        now = datetime.now(TIMEZONE)
        upper = now + timedelta(minutes=minutes_before)                    # остаток <= minutes_before
        lower = now + timedelta(minutes=minutes_before - grace_minutes)    # остаток >  (minutes_before - grace)
        upper_str = upper.strftime("%Y-%m-%d %H:%M")
        lower_str = lower.strftime("%Y-%m-%d %H:%M")

        with self.get_connection() as conn:
            rows = conn.execute(f'''
                SELECT b.id, b.user_id, u.first_name, u.username,
                       ts.day, ts.adjusted_time, ts.date
                FROM bookings b
                JOIN users u       ON b.user_id  = u.user_id
                JOIN time_slots ts ON b.slot_id  = ts.id
                WHERE b.status = 'active'
                  AND (ts.date || ' ' || ts.adjusted_time) >  ?
                  AND (ts.date || ' ' || ts.adjusted_time) <= ?
                  AND b.{flag_column} = 0
            ''', (lower_str, upper_str)).fetchall()

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
                    f'UPDATE bookings SET {flag_column} = 1 WHERE id = ?',
                    (s['booking_id'],)
                )
            conn.commit()

        return sessions

    def get_upcoming_sessions(self, minutes_before: int = 15) -> List[dict]:
        """
        Сессии, до которых осталось не более minutes_before минут (по умолчанию 15)
        и которые ещё не начались. Напоминание не уходит раньше чем за 15 минут.
        Окно подхвата = minutes_before (нижняя граница = «сейчас»): если отправка
        сорвалась, повтор произойдёт на следующей проверке.
        Уведомление отправляется один раз (notified_15min).
        """
        return self._get_due_sessions(
            minutes_before, grace_minutes=minutes_before, flag_column='notified_15min'
        )

    def get_upcoming_sessions_24h(self) -> List[dict]:
        """
        Сессии, до которых осталось не более 24 часов (и не начались).
        Напоминание не уходит раньше чем за 24 часа. Окно подхвата — 30 минут:
        ловит сессию у отметки 24ч и не срабатывает для записей, сделанных
        менее чем за ~23.5 часа (для них напоминание за 24ч бессмысленно).
        Уведомление отправляется один раз (notified_24h).
        """
        return self._get_due_sessions(
            24 * 60, grace_minutes=30, flag_column='notified_24h'
        )

    def reset_24h_notification_for_booking(self, booking_id: int) -> bool:
        """Сбросить 24ч-флаг для повторной попытки при сбое отправки."""
        with self.get_connection() as conn:
            cur = conn.execute(
                'UPDATE bookings SET notified_24h = 0 WHERE id = ?', (booking_id,)
            )
            conn.commit()
            return cur.rowcount > 0

    def mark_notification_sent(self, booking_id: int) -> bool:
        with self.get_connection() as conn:
            cur = conn.execute(
                'UPDATE bookings SET notified_15min = 1 WHERE id = ?',
                (booking_id,)
            )
            conn.commit()
            return cur.rowcount > 0

    def reset_notification_for_booking(self, booking_id: int) -> bool:
        """Сбросить флаг уведомления для одной записи — повторная попытка при сбое отправки."""
        with self.get_connection() as conn:
            cur = conn.execute(
                'UPDATE bookings SET notified_15min = 0 WHERE id = ?',
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
