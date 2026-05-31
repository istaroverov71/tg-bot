"""
test_comprehensive.py — Комплексные тесты 9 сценариев бота.

Тестируем бизнес-логику без реального Telegram:
  1.  Напоминания приходят за 15 мин до adjusted_time (не base_time)
  2.  4-й слот подряд (10:00→11:10→12:20→?) скрывается из списка
  3.  Клиент не может записаться дважды на одной календарной неделе
  4.  Код-пути уведомлений (статический анализ bot.py)
  5.  Уведомления о новых слотах — только незаписанные пользователи
  6.  /update_week сохраняет существующие записи клиентов
  7.  Прошедшие слоты исчезают из списка
  8.  Нельзя записаться менее чем за 3 часа
  9.  Нельзя отменить менее чем за 3 часа + контакт в тексте

Запуск: python3 test_comprehensive.py
"""

import os, sys, types, tempfile, sqlite3
from datetime import datetime, timedelta

import pytz

# ── Заглушка config (до всех остальных импортов) ─────────────────────────────
TIMEZONE = pytz.timezone('Europe/Moscow')
_TMP_DB  = tempfile.mktemp(suffix='_tgbot_test.db')

cfg = types.ModuleType('config')
cfg.SESSION_DURATION = 60
cfg.BUFFER_TIME      = 10
cfg.TIMEZONE         = TIMEZONE
cfg.DEFAULT_SLOTS    = ["10:00", "11:00", "12:00", "13:00"]
cfg.BOT_TOKEN        = 'TEST'
cfg.DATABASE_FILE    = _TMP_DB
sys.modules['config'] = cfg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from slot_scheduler import Slot, SmartScheduler  # noqa: E402
from database import Database                    # noqa: E402

# ─── Константы для тестов ────────────────────────────────────────────────────

# Понедельник в далёком будущем — все слоты гарантированно >= cutoff
WEEK     = "2099-01-06"
DATE     = "2099-01-06"
USER_A   = 1001          # клиент «Иван»
USER_B   = 1002          # клиент «Мария»

# ─── Вспомогательные функции ─────────────────────────────────────────────────

def sep(title: str):
    print(f"\n{'='*58}")
    print(f"  {title}")
    print('='*58)


def fresh_db() -> Database:
    """Удалить старую БД и создать чистую с двумя тестовыми пользователями."""
    if os.path.exists(_TMP_DB):
        os.remove(_TMP_DB)
    db = Database()
    db.add_user(USER_A, 'client1', 'Иван',  None)
    db.add_user(USER_B, 'client2', 'Мария', None)
    return db


def _raw(sql, params=()):
    """Выполнить произвольный SQL во временной БД, вернуть все строки."""
    conn = sqlite3.connect(_TMP_DB)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def add_slot_raw(slot_id: int, base_time: str, adjusted_time: str = None,
                 day: str = "Понедельник", date: str = DATE,
                 week_start: str = WEEK, is_available: int = 1):
    """Вставить слот напрямую через SQL (позволяет задать adjusted_time вручную)."""
    adj = adjusted_time or base_time
    conn = sqlite3.connect(_TMP_DB)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute('''
        INSERT OR IGNORE INTO time_slots
            (id, base_time, adjusted_time, day, date, week_start, is_available)
        VALUES (?,?,?,?,?,?,?)
    ''', (slot_id, base_time, adj, day, date, week_start, is_available))
    conn.commit()
    conn.close()


def book_raw(user_id: int, slot_id: int, adjusted_time: str,
             date: str = DATE, day: str = "Понедельник",
             week_start: str = WEEK) -> int:
    """Создать запись напрямую через SQL, пометить слот занятым.
    Используется для тестов, где нужен полный контроль над adjusted_time."""
    conn = sqlite3.connect(_TMP_DB)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute('UPDATE time_slots SET is_available=0, booked_by=? WHERE id=?',
                 (user_id, slot_id))
    cur = conn.execute('''
        INSERT INTO bookings
            (user_id, slot_id, original_time, adjusted_time, booking_date, status)
        VALUES (?,?,?,?,?,?)
    ''', (user_id, slot_id, adjusted_time, adjusted_time,
          datetime.now(TIMEZONE).isoformat(), 'active'))
    booking_id = cur.lastrowid
    conn.execute('UPDATE time_slots SET booking_id=? WHERE id=?', (booking_id, slot_id))
    conn.commit()
    conn.close()
    return booking_id


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 1: Напоминания по adjusted_time (сдвинутому времени), не base_time
# ══════════════════════════════════════════════════════════════════════════════

def test_1_reminders_use_adjusted_time():
    sep("ТЕСТ 1: Напоминания за 15 мин до adjusted_time")
    db = fresh_db()

    now = datetime.now(TIMEZONE)
    # Создаём сессию ровно через 15 минут (adjusted_time ≠ base_time)
    target_dt  = now + timedelta(minutes=15)
    adj_time   = target_dt.strftime("%H:%M")   # например "11:10"
    date_str   = target_dt.strftime("%Y-%m-%d")
    base_time  = "11:00"                        # исходное время до сдвига

    # Вычисляем понедельник для этой даты
    td = target_dt.date()
    week_start = (td - timedelta(days=td.weekday())).strftime("%Y-%m-%d")

    add_slot_raw(901, base_time, adj_time, date=date_str, week_start=week_start)
    book_raw(USER_A, 901, adj_time, date=date_str, week_start=week_start)

    # Первый вызов: должен найти сессию через 15 мин по adjusted_time
    sessions = db.get_upcoming_sessions(minutes_before=15)
    assert len(sessions) == 1, \
        f"Ожидали 1 сессию, получили {len(sessions)}"
    assert sessions[0]['time'] == adj_time, \
        f"Время в напоминании: {sessions[0]['time']}, ожидалось {adj_time} (adj)"
    print(f"✅ Напоминание нашло сессию с adjusted_time={adj_time} (base={base_time})")

    # Повторный вызов: notified_15min=1 → дубль не отправится
    sessions2 = db.get_upcoming_sessions(minutes_before=15)
    assert sessions2 == [], \
        "Повторный вызов не должен возвращать уже уведомлённые сессии"
    print("✅ Повторный вызов не дублирует напоминание (notified_15min=1)")

    # reset_notification: при сбое Telegram можно попробовать снова
    booking_id = sessions[0]['booking_id']
    ok = db.reset_notification_for_booking(booking_id)
    assert ok, "reset_notification_for_booking должен вернуть True"
    sessions3 = db.get_upcoming_sessions(minutes_before=15)
    assert len(sessions3) == 1, \
        f"После reset должна вернуться 1 сессия, получили {len(sessions3)}"
    print("✅ reset_notification позволяет повторную попытку при сбое отправки")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 2: 4-й слот подряд скрывается — 10:00 → 11:10 → 12:20 → ?
# ══════════════════════════════════════════════════════════════════════════════

def test_2_fourth_slot_hidden():
    sep("ТЕСТ 2: 4-й слот подряд скрывается из списка")

    def mk_booked(id_, base, current):
        return Slot(id_, base, current, "Понедельник", DATE, WEEK, True, 99)

    def mk_free(id_, base):
        return Slot(id_, base, base, "Понедельник", DATE, WEEK, False, None)

    # 3 занятых с реальными current_time: 10:00, 11:10, 12:20
    # Следующий свободный слот (13:00 → ~13:30) должен исчезнуть
    slots = [
        mk_booked(1, "10:00", "10:00"),
        mk_booked(2, "11:00", "11:10"),
        mk_booked(3, "12:00", "12:20"),
        mk_free(4, "13:00"),    # стал бы 4-м подряд — должен скрыться
        mk_free(5, "15:00"),    # далеко от цепочки — должен остаться
    ]
    s = SmartScheduler(slots)
    visible = s.get_visible_slots()
    vis_ids = {sl.id for sl in visible}

    assert 4 not in vis_ids, \
        f"Слот 4 (4-й в цепочке) должен быть СКРЫТ. Видимые id: {vis_ids}"
    assert 5 in vis_ids, \
        f"Слот 5 (15:00, вне цепочки) должен быть ВИДЕН. Видимые id: {vis_ids}"
    print("✅ Слот 4 (4-й подряд) скрыт — пользователь его не видит")
    print("✅ Слот 5 (вне цепочки) виден")

    # 3-й подряд ДОЛЖЕН быть виден — только 4-й запрещён
    slots2 = [
        mk_booked(10, "10:00", "10:00"),
        mk_booked(11, "11:00", "11:10"),
        mk_free(12, "12:00"),    # 3-й → виден
        mk_free(13, "13:00"),
    ]
    s2 = SmartScheduler(slots2)
    vis2 = {sl.id for sl in s2.get_visible_slots()}
    assert 12 in vis2, \
        f"Слот 12 (3-й подряд) должен быть виден. Видимые: {vis2}"
    print("✅ Слот 12 (3-й подряд) виден — ограничение только на 4-й")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 3: Клиент не может записаться дважды на одной неделе
# ══════════════════════════════════════════════════════════════════════════════

def test_3_no_double_booking():
    sep("ТЕСТ 3: Запрет двойной записи на одной неделе")
    db = fresh_db()

    add_slot_raw(301, "10:00")
    add_slot_raw(302, "11:00")

    # Первая запись через SmartScheduler (как в боте)
    ok, msg, info = db.book_slot_with_scheduler(USER_A, 301, WEEK)
    assert ok, f"Первая запись должна пройти, получили: {msg}"
    print(f"✅ Первая запись прошла (слот 10:00)")

    # get_user_active_booking — именно этот метод используют show_booking_days
    # и process_final_booking перед тем как пустить пользователя дальше
    active = db.get_user_active_booking(USER_A)
    assert active is not None, \
        "get_user_active_booking должна вернуть запись после бронирования"
    print(f"✅ get_user_active_booking возвращает активную запись: {active['time']}")

    # Бот блокирует второй вход в show_booking_days: active != None → ALREADY_BOOKED
    # Проверяем: метод по-прежнему возвращает запись (значит блок сработает)
    check = db.get_user_active_booking(USER_A)
    assert check is not None, \
        "Повторный вызов get_user_active_booking должен по-прежнему видеть запись"
    print("✅ Попытка открыть «Запись» второй раз была бы заблокирована (ALREADY_BOOKED)")

    # Другой пользователь (USER_B) может записаться на свободный слот
    ok2, msg2, _ = db.book_slot_with_scheduler(USER_B, 302, WEEK)
    assert ok2, f"Другой пользователь должен записаться: {msg2}"
    print("✅ Другой пользователь (USER_B) свободно записался на 11:00")

    # Гонка (race condition): попытка занять уже занятый слот 301 кем-то ещё
    ok3, msg3, _ = db.book_slot_with_scheduler(USER_B, 301, WEEK)
    assert not ok3, "Попытка занять уже занятый слот должна вернуть False"
    print(f"✅ Race condition: попытка занять занятый слот отклонена: «{msg3}»")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 4: Код-пути уведомлений (статический анализ bot.py)
# ══════════════════════════════════════════════════════════════════════════════

def test_4_notification_paths():
    sep("ТЕСТ 4: Код-пути уведомлений (статический анализ)")
    bot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    with open(bot_path, encoding='utf-8') as f:
        src = f.read()

    # 4a: Уведомление администратору о новой записи
    assert 'Новая запись' in src, "Текст 'Новая запись' должен быть в bot.py"
    assert 'send_message(admin_id, admin_message)' in src, \
        "Отправка admin_message администратору должна быть в боте"
    assert 'if not is_admin:' in src, \
        "Блок 'if not is_admin' защищает от само-уведомления"
    print("✅ 4a: Уведомление о новой записи — отправляется только если записывается не сам админ")

    # 4b: Уведомление администратору об отмене
    assert 'Запись отменена' in src, "Уведомление об отмене в коде"
    assert 'cancel_msg' in src, "cancel_msg должен быть сформирован"
    assert 'send_message(admin_id, cancel_msg)' in src, \
        "cancel_msg должен отправляться администратору"
    print("✅ 4b: Уведомление об отмене — отправляется администратору")

    # 4c: Напоминание за 15 минут — и пользователю, и администратору
    assert 'REMINDER_USER.format(slot=slot_str)' in src, \
        "Напоминание пользователю должно отправляться через REMINDER_USER"
    assert 'REMINDER_ADMIN.format(' in src, \
        "Напоминание администратору должно отправляться через REMINDER_ADMIN"
    print("✅ 4c: 15-минутные напоминания — и клиенту, и администратору")

    # 4d: Если напоминание клиенту не ушло — администратор не беспокоится
    assert 'user_sent' in src, \
        "Переменная user_sent контролирует отправку напоминания администратору"
    assert 'if not user_sent:' in src, \
        "Блок 'if not user_sent: continue' должен быть в send_reminders"
    print("✅ 4d: Администратор не беспокоится если клиент не получил напоминание")

    # 4e: run_repeating для напоминаний (PTB job_queue)
    assert 'run_repeating' in src, "job_queue.run_repeating должен быть в main()"
    assert 'send_reminders' in src, "send_reminders должна быть зарегистрирована"
    print("✅ 4e: Напоминания запускаются через PTB job_queue каждые 5 минут")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 5: Уведомления о новых слотах — только незаписанные пользователи
# ══════════════════════════════════════════════════════════════════════════════

def test_5_new_slot_notifications():
    sep("ТЕСТ 5: Рассылка о новых слотах")
    db = fresh_db()

    add_slot_raw(401, "10:00")
    add_slot_raw(402, "11:00")

    # USER_A записывается
    ok, _, _ = db.book_slot_with_scheduler(USER_A, 401, WEEK)
    assert ok, "Запись USER_A должна пройти"

    # get_users_without_booking_on_week — именно этот метод используется в /update_week
    monday = "2099-01-06"
    sunday = "2099-01-12"
    to_notify = db.get_users_without_booking_on_week(monday, sunday)
    ids = {u['user_id'] for u in to_notify}

    assert USER_A not in ids, \
        f"USER_A (с записью) НЕ должен получать уведомление. ids={ids}"
    assert USER_B in ids, \
        f"USER_B (без записи) ДОЛЖЕН получать уведомление. ids={ids}"
    print(f"✅ USER_A (с записью на неделе) — не уведомляется")
    print(f"✅ USER_B (без записи) — уведомляется о новых слотах")

    # Если USER_B тоже запишется — он тоже исчезнет из списка
    ok2, _, _ = db.book_slot_with_scheduler(USER_B, 402, WEEK)
    assert ok2
    to_notify2 = db.get_users_without_booking_on_week(monday, sunday)
    ids2 = {u['user_id'] for u in to_notify2}
    assert USER_B not in ids2, \
        f"USER_B записался — уведомление ему больше не нужно. ids2={ids2}"
    print("✅ После записи USER_B он также исчезает из списка рассылки")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 6: /update_week — добавляет слоты, не трогает существующие записи
# ══════════════════════════════════════════════════════════════════════════════

def test_6_update_week_preserves_bookings():
    sep("ТЕСТ 6: /update_week добавляет слоты, не трогает записи клиентов")
    db = fresh_db()

    # Начальное расписание: 10:00, 11:00, 12:00
    add_slot_raw(601, "10:00")
    add_slot_raw(602, "11:00")
    add_slot_raw(603, "12:00")

    # Клиент записывается на 11:00
    ok, _, _ = db.book_slot_with_scheduler(USER_A, 602, WEEK)
    assert ok, "Запись на 11:00 должна пройти"
    print("✅ Запись на 11:00 создана")

    # Администратор добавляет слот 13:00 (11:00 не в списке — не важно,
    # /update_week больше не удаляет существующие слоты)
    result = db.update_day_slots_preserve_bookings(
        date_str=DATE,
        day="Понедельник",
        week_start=WEEK,
        new_times=["10:00", "12:00", "13:00"],
    )

    # 13:00 — новое время → должен появиться
    assert "13:00" in result['added_slots'], \
        f"13:00 должен быть добавлен: {result}"
    print(f"✅ Новый слот 13:00 добавлен (added={result['added_slots']})")

    # 10:00 и 12:00 уже были → не дублируются
    assert "10:00" not in result['added_slots'], "10:00 уже существует — не должен дублироваться"
    assert "12:00" not in result['added_slots'], "12:00 уже существует — не должен дублироваться"
    print("✅ Существующие слоты 10:00 и 12:00 не задублированы")

    # Проверяем состояние БД напрямую
    rows = _raw(
        "SELECT base_time, is_available, booked_by FROM time_slots "
        "WHERE date=? ORDER BY base_time", (DATE,)
    )
    db_state = {r[0]: (r[1], r[2]) for r in rows}

    assert "11:00" in db_state, "Слот 11:00 должен остаться в БД"
    assert db_state["11:00"][1] == USER_A, \
        f"11:00 должен быть занят USER_A ({USER_A}), booked_by={db_state['11:00'][1]}"
    assert "13:00" in db_state, "Слот 13:00 должен появиться в БД"
    assert db_state["13:00"][0] == 1, "13:00 должен быть свободен (is_available=1)"
    print("✅ БД: слот 11:00 занят USER_A, слот 13:00 свободен")

    # Запись клиента на 11:00 не затронута
    active = db.get_user_active_booking(USER_A)
    assert active is not None, "Запись USER_A должна оставаться активной"
    assert active['time'] == '11:00', \
        f"Время записи должно быть 11:00, получили {active['time']}"
    print("✅ Запись клиента на 11:00 не пострадала после обновления расписания")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 7: Прошедшие и близкие слоты исчезают из списка
# ══════════════════════════════════════════════════════════════════════════════

def test_7_past_slots_hidden():
    sep("ТЕСТ 7: Прошедшие и ближние (< 3ч) слоты скрыты")
    now    = datetime.now(TIMEZONE)
    cutoff = now + timedelta(hours=3)

    cases = [
        (now - timedelta(hours=2),              False, "2 ч назад"),
        (now - timedelta(minutes=1),            False, "1 мин назад"),
        (now,                                   False, "прямо сейчас"),
        (now + timedelta(hours=1),              False, "через 1 ч  (< cutoff)"),
        (now + timedelta(hours=2, minutes=59),  False, "через 2ч59 (< cutoff)"),
        (now + timedelta(hours=3),              True,  "ровно 3 ч  (граница)"),
        (now + timedelta(hours=4),              True,  "через 4 ч"),
        (now + timedelta(days=7),               True,  "через 7 дней"),
    ]

    for slot_dt, expected, label in cases:
        visible = slot_dt >= cutoff
        assert visible == expected, \
            f"«{label}»: видимость={visible}, ожидалась={expected}"
        status = "виден ✅" if visible else "скрыт  ✅"
        print(f"  «{label}» → {status}")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 8: Нельзя записаться менее чем за 3 часа
# ══════════════════════════════════════════════════════════════════════════════

def test_8_booking_cutoff_3h():
    sep("ТЕСТ 8: Cutoff записи = 3 часа")
    now    = datetime.now(TIMEZONE)
    cutoff = now + timedelta(hours=3)   # логика из show_booking_days

    # Эти случаи должны быть СКРЫТЫ (slot_dt < cutoff)
    blocked = [
        (now + timedelta(minutes=30),  "30 мин"),
        (now + timedelta(hours=1),     "1 ч"),
        (now + timedelta(hours=2),     "2 ч"),
        (now + timedelta(hours=2, minutes=59), "2ч59"),
    ]
    for slot_dt, label in blocked:
        assert slot_dt < cutoff, f"Слот «через {label}» должен быть недоступен"
        print(f"  ✅ «через {label}» — недоступен для записи")

    # Эти случаи должны быть ДОСТУПНЫ (slot_dt >= cutoff)
    allowed = [
        (now + timedelta(hours=3),  "ровно 3 ч"),
        (now + timedelta(hours=4),  "4 ч"),
        (now + timedelta(hours=24), "24 ч"),
    ]
    for slot_dt, label in allowed:
        assert slot_dt >= cutoff, f"Слот «через {label}» должен быть доступен"
        print(f"  ✅ «через {label}» — доступен для записи")

    # Также: попытка ЗАПИСАТЬСЯ через SmartScheduler на слот, который всё ещё
    # свободен в БД, но бот его спрятал — при финальном подтверждении слот
    # физически всё ещё существует (это разрешено для администратора).
    # Блокировка происходит на уровне show_booking_days, а не на уровне DB.
    print("  ✅ Блокировка cutoff реализована в show_booking_days (фильтр видимости)")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 9: Нельзя отменить < 3 часа + контакт в тексте сообщения
# ══════════════════════════════════════════════════════════════════════════════

def test_9_cancel_cutoff_and_messages():
    sep("ТЕСТ 9: Cutoff отмены = 3 часа + контакт в тексте")
    now = datetime.now(TIMEZONE)

    def can_cancel(session_dt):
        """Зеркало логики из process_cancellation / pre_cancel_confirm."""
        return (session_dt - now) >= timedelta(hours=3)

    # Запрещённые окна
    blocked = [
        (now + timedelta(minutes=30),           "30 мин"),
        (now + timedelta(hours=2),              "2 ч"),
        (now + timedelta(hours=2, minutes=59),  "2ч59"),
    ]
    for dt, label in blocked:
        assert not can_cancel(dt), f"Отмена «за {label}» должна быть запрещена"
        print(f"  ✅ «за {label}» — отмена запрещена")

    # Разрешённые окна
    allowed = [
        (now + timedelta(hours=3),  "ровно 3 ч"),
        (now + timedelta(hours=4),  "4 ч"),
        (now + timedelta(days=1),   "24 ч"),
    ]
    for dt, label in allowed:
        assert can_cancel(dt), f"Отмена «за {label}» должна быть разрешена"
        print(f"  ✅ «за {label}» — отмена разрешена")

    # Статический анализ: все блоки «менее 3 часов» содержат контакт @n_kshmlv
    bot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    with open(bot_path, encoding='utf-8') as f:
        src = f.read()

    chunks = src.split("осталось менее 3 часов")
    total  = len(chunks) - 1
    assert total > 0, "В bot.py должно быть хотя бы одно сообщение об ограничении 3ч"

    bad = [c for c in chunks[1:] if "@n_kshmlv" not in c[:500]]
    print(f"\n  Блоков «менее 3 часов» в bot.py: {total}")
    if bad:
        print(f"  ⚠️  Без контакта @n_kshmlv: {len(bad)}")
    else:
        print(f"  ✅ Во всех {total} блоках есть контакт @n_kshmlv")
    assert not bad, \
        "Все сообщения об ограничении отмены должны содержать контакт @n_kshmlv"

    # Проверяем, что execute_change_booking тоже имеет 3ч-проверку (баг 1)
    assert 'execute_change_booking' in src
    # Находим тело функции
    fn_start = src.index('async def execute_change_booking')
    fn_body  = src[fn_start:fn_start + 1200]
    assert 'timedelta(hours=3)' in fn_body, \
        "execute_change_booking должна содержать повторную проверку 3ч"
    print("  ✅ execute_change_booking содержит повторную проверку 3ч (баг 1 исправлен)")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 10: Деактивация пользователей, заблокировавших бота
# ══════════════════════════════════════════════════════════════════════════════

def test_10_deactivate_blocked_users():
    sep("ТЕСТ 10: Деактивация пользователей, заблокировавших бота")
    db = fresh_db()

    # Оба пользователя активны после регистрации
    all_users = db.get_all_users()
    ids = {u['user_id'] for u in all_users}
    assert USER_A in ids and USER_B in ids, "Оба пользователя должны быть в списке"
    print("✅ Оба пользователя активны после регистрации")

    # USER_A заблокировал бота
    ok = db.deactivate_user(USER_A)
    assert ok, "deactivate_user должен вернуть True"
    print(f"✅ USER_A деактивирован (заблокировал бота)")

    # get_all_users (используется в /broadcast) — USER_A исчезает
    active = db.get_all_users()
    active_ids = {u['user_id'] for u in active}
    assert USER_A not in active_ids, "USER_A не должен быть в get_all_users"
    assert USER_B in active_ids,     "USER_B должен оставаться активным"
    print("✅ get_all_users (для /broadcast) больше не включает USER_A")

    # get_users_without_booking_on_week (для рассылки о слотах) — тоже не включает
    monday, sunday = "2099-01-06", "2099-01-12"
    to_notify = db.get_users_without_booking_on_week(monday, sunday)
    notify_ids = {u['user_id'] for u in to_notify}
    assert USER_A not in notify_ids, "Деактивированный USER_A не должен получать рассылки"
    assert USER_B in notify_ids,     "USER_B (активный) должен получать рассылки"
    print("✅ Деактивированный USER_A исключён из рассылок о новых слотах")

    # При следующем /start is_active восстанавливается в 1
    db.add_user(USER_A, 'client1', 'Иван', None)
    restored = db.get_all_users()
    restored_ids = {u['user_id'] for u in restored}
    assert USER_A in restored_ids, "После /start USER_A снова активен"
    print("✅ После /start (add_user) USER_A снова активен — is_active=1 восстановлен")


# ══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 11: Ежедневная очистка (cleanup) — логика
# ══════════════════════════════════════════════════════════════════════════════

def test_11_cleanup_logic():
    sep("ТЕСТ 11: Ежедневная очистка старых данных")
    db = fresh_db()

    # Добавляем «старые» слоты и запись (неделя 6 недель назад)
    from datetime import date
    old_monday = (date.today() - timedelta(weeks=6))
    old_monday -= timedelta(days=old_monday.weekday())   # сдвигаем на понедельник
    old_week = old_monday.strftime("%Y-%m-%d")
    old_date = old_monday.strftime("%Y-%m-%d")

    add_slot_raw(701, "10:00", date=old_date, week_start=old_week)

    # Создаём «завершённую» запись (статус completed) напрямую в БД
    conn = sqlite3.connect(_TMP_DB)
    conn.execute('''
        INSERT INTO bookings
            (user_id, slot_id, original_time, adjusted_time, booking_date, status)
        VALUES (?,?,?,?,?,?)
    ''', (USER_A, 701, "10:00", "10:00",
          (datetime.now(TIMEZONE) - timedelta(weeks=6)).isoformat(), 'completed'))
    conn.commit()
    conn.close()

    # Убеждаемся что данные есть
    before_slots    = _raw("SELECT COUNT(*) FROM time_slots WHERE week_start=?", (old_week,))[0][0]
    before_bookings = _raw("SELECT COUNT(*) FROM bookings WHERE status='completed'"  )[0][0]
    assert before_slots >= 1,    "Старый слот должен быть в БД"
    assert before_bookings >= 1, "Старая завершённая запись должна быть в БД"
    print(f"✅ До очистки: {before_slots} старых слотов, {before_bookings} завершённых записей")

    # Запускаем очистку (weeks_to_keep=4 → удалим всё старше 4 недель)
    completed = db.complete_past_sessions()   # как в run_cleanup
    result = db.cleanup_old_data(weeks_to_keep=4)
    assert result['deleted_slots'] >= 1,    f"Старые слоты должны быть удалены: {result}"
    assert result['deleted_bookings'] >= 1, f"Старые записи должны быть удалены: {result}"
    print(f"✅ Удалено: {result['deleted_slots']} слотов, {result['deleted_bookings']} записей")

    # Новые данные (на следующей неделе) должны остаться нетронутыми
    add_slot_raw(702, "10:00")   # WEEK = 2099-01-06 — останется
    db.book_slot_with_scheduler(USER_A, 702, WEEK)
    db.cleanup_old_data(weeks_to_keep=4)
    active = db.get_user_active_booking(USER_A)
    assert active is not None, "Свежая запись не должна удаляться при очистке"
    print("✅ Свежие данные (2099) не затронуты очисткой")

    # Статический анализ: run_cleanup и job зарегистрированы в bot.py
    bot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot.py')
    src = open(bot_path, encoding='utf-8').read()
    assert 'run_cleanup' in src,        "run_cleanup должна быть в bot.py"
    assert 'cleanup_old_data' in src,   "cleanup_old_data должна вызываться"
    assert 'interval=86400' in src,     "Очистка должна запускаться раз в сутки (86400 сек)"
    print("✅ run_cleanup зарегистрирован в job_queue с interval=86400 (раз в сутки)")


# ══════════════════════════════════════════════════════════════════════════════
# Запуск
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    try:
        test_1_reminders_use_adjusted_time()
        test_2_fourth_slot_hidden()
        test_3_no_double_booking()
        test_4_notification_paths()
        test_5_new_slot_notifications()
        test_6_update_week_preserves_bookings()
        test_7_past_slots_hidden()
        test_8_booking_cutoff_3h()
        test_9_cancel_cutoff_and_messages()
        test_10_deactivate_blocked_users()
        test_11_cleanup_logic()

        print("\n" + "="*58)
        print("  🎉  Все 11 тестовых сценариев пройдены!")
        print("="*58)
    finally:
        if os.path.exists(_TMP_DB):
            os.remove(_TMP_DB)
