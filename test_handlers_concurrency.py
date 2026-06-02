"""
test_handlers_concurrency.py — Тесты хендлеров bot.py и конкурентности.

Покрывает то, чего не было в существующих тестах:
  • Хендлеры bot.py (start, handle_message, show_my_bookings, show_booking_days,
    process_final_booking, process_cancellation) — через фейковые Update/Query/Context.
  • Конкурентность book_slot_with_scheduler:
       - параллельная гонка за ОДИН слот → ровно одна успешная запись;
       - параллельные записи на РАЗНЫЕ слоты → обе успешны, нет lost update (C2).
  • M1: одна запись в КАЛЕНДАРНУЮ неделю, включая уже прошедшую сессию;
       запись на следующую неделю при этом разрешена.
  • H3: напоминание не отправляется раньше чем за 15 минут / 24 часа.

Запуск: python3 test_handlers_concurrency.py   или   pytest -q
"""

import os, sys, types, tempfile, sqlite3, asyncio, threading
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytz

# ── Заглушка config ДО импорта bot/database/… ───────────────────────────────
TIMEZONE = pytz.timezone('Europe/Moscow')
_TMP_DB  = tempfile.mktemp(suffix='_tgbot_handlers_test.db')
ADMIN_ID = 999999

cfg = types.ModuleType('config')
cfg.SESSION_DURATION   = 60
cfg.BUFFER_TIME        = 10
cfg.TIMEZONE           = TIMEZONE
cfg.DEFAULT_SLOTS      = ["10:00", "11:00", "12:00", "13:00", "14:00"]
cfg.BOT_TOKEN          = 'TEST'
cfg.DATABASE_FILE      = _TMP_DB
cfg.ADMIN_IDS          = [ADMIN_ID]
cfg.WELCOME_MESSAGE    = "WELCOME"
cfg.BOOKING_START      = "BOOKING_START"
cfg.ALREADY_BOOKED     = "ALREADY_BOOKED: {slot}"
cfg.NO_SLOTS_MESSAGE   = "NO_SLOTS"
cfg.MY_BOOKINGS_HEADER = "Ваши записи:\n\n"
cfg.NO_BOOKINGS        = "NO_BOOKINGS"
cfg.REMINDER_USER      = "RU: {slot}"
cfg.REMINDER_USER_24H  = "RU24: {slot}"
cfg.REMINDER_ADMIN     = "RA: {name} {username} {slot}"
cfg.ERROR_MESSAGE      = "ERROR"
cfg.BUTTON_BOOK        = "✍🏻 Запись"
cfg.BUTTON_MY_BOOKINGS = "📋 Мои записи"
cfg.BUTTON_CANCEL      = "❌ Отменить запись"
sys.modules['config'] = cfg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import Database          # noqa: E402
from slot_scheduler import Slot, SmartScheduler  # noqa: E402
import bot                             # noqa: E402

# Под pytest заглушка `config` общая для всех тест-файлов, и модуль `database`
# мог связать DATABASE_FILE с путём из другого файла. Берём фактический путь,
# который реально использует Database, чтобы прямой SQL шёл в ту же БД.
import database as _dbmod             # noqa: E402
_TMP_DB = _dbmod.DATABASE_FILE

USER_A, USER_B, USER_C = 1001, 1002, 1003
WEEK1 = "2099-01-06"   # понедельник
WEEK2 = "2099-01-13"   # следующий понедельник


# ─── Хелперы БД ──────────────────────────────────────────────────────────────

def fresh_db(extra_users=()) -> Database:
    """Чистая БД + тестовые пользователи. bot.db переключаем на неё же."""
    if os.path.exists(_TMP_DB):
        os.remove(_TMP_DB)
    db = Database()
    db.add_user(USER_A, 'ivan',  'Иван',  None)
    db.add_user(USER_B, 'maria', 'Мария', None)
    db.add_user(USER_C, 'petr',  'Пётр',  None)
    db.add_user(ADMIN_ID, 'admin', 'Админ', None)
    for uid in extra_users:
        db.add_user(uid, f'u{uid}', f'User{uid}', None)
    # Перенаправляем глобальные объекты bot на тестовую БД
    bot.db = db
    return db


def add_slot(db: Database, slot_id, base, adj=None, day="Понедельник",
             date=WEEK1, week_start=WEEK1, is_available=1):
    adj = adj or base
    conn = sqlite3.connect(_TMP_DB)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute('''
        INSERT OR IGNORE INTO time_slots
            (id, base_time, adjusted_time, day, date, week_start, is_available)
        VALUES (?,?,?,?,?,?,?)
    ''', (slot_id, base, adj, day, date, week_start, is_available))
    conn.commit(); conn.close()


def insert_booking(db, user_id, slot_id, adj, date, week_start,
                   status='active', day="Понедельник"):
    """Прямая вставка записи (для сценариев с прошедшими/готовыми сессиями)."""
    conn = sqlite3.connect(_TMP_DB)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute('UPDATE time_slots SET is_available=0, booked_by=? WHERE id=?',
                 (user_id, slot_id))
    cur = conn.execute('''
        INSERT INTO bookings
            (user_id, slot_id, original_time, adjusted_time, booking_date, status)
        VALUES (?,?,?,?,?,?)
    ''', (user_id, slot_id, adj, adj, datetime.now(TIMEZONE).isoformat(), status))
    bid = cur.lastrowid
    conn.execute('UPDATE time_slots SET booking_id=? WHERE id=?', (bid, slot_id))
    conn.commit(); conn.close()
    return bid


def monday_of(d: datetime) -> str:
    return (d.date() - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


# ─── Фейки Telegram ──────────────────────────────────────────────────────────

class FakeUser:
    def __init__(self, uid, username='u', first='Имя', last='Фам'):
        self.id = uid; self.username = username
        self.first_name = first; self.last_name = last


class FakeMessage:
    def __init__(self, text=None):
        self.text = text
        self.replies = []
    async def reply_text(self, text, **kw):
        self.replies.append((text, kw))
        return SimpleNamespace(message_id=1)


class FakeUpdate:
    """Имитация Update. Содержит и effective_user, и from_user — чтобы подходить
    как обычным хендлерам, так и фолбэку show_booking_days."""
    def __init__(self, user, text=None):
        self.effective_user = user
        self.from_user = user
        self.message = FakeMessage(text)
        self.callback_query = None


class FakeQuery:
    def __init__(self, user, data=''):
        self.from_user = user
        self.data = data
        self.message = FakeMessage()
        self.edits = []
        self.answered = False
    async def answer(self, *a, **k):
        self.answered = True
    async def edit_message_text(self, text, **kw):
        self.edits.append((text, kw))


class FakeBot:
    def __init__(self):
        self.sent = []
    async def send_message(self, chat_id, text, **kw):
        self.sent.append((chat_id, text, kw))


class FakeContext:
    def __init__(self, args=None):
        self.bot = FakeBot()
        self.user_data = {}
        self.args = args or []


def run(coro):
    return asyncio.run(coro)


def sep(t):
    print(f"\n{'='*60}\n  {t}\n{'='*60}")


# ══════════════════════════════════════════════════════════════════════════════
# ХЕНДЛЕРЫ
# ══════════════════════════════════════════════════════════════════════════════

def test_handler_start_registers_user():
    sep("Хендлер /start — регистрирует пользователя и шлёт приветствие")
    db = fresh_db()
    # Удаляем USER_A, чтобы проверить регистрацию заново
    conn = sqlite3.connect(_TMP_DB); conn.execute("DELETE FROM users WHERE user_id=?", (USER_A,)); conn.commit(); conn.close()

    upd = FakeUpdate(FakeUser(USER_A, 'ivan', 'Иван'))
    ctx = FakeContext()
    run(bot.start(upd, ctx))

    assert db.get_user(USER_A) is not None, "start должен зарегистрировать пользователя"
    assert upd.message.replies, "start должен ответить сообщением"
    assert upd.message.replies[0][0] == "WELCOME"
    print("✅ /start: пользователь сохранён, отправлено WELCOME")


def test_handler_handle_message_routes_my_bookings():
    sep("Хендлер handle_message — кнопка «Мои записи» → NO_BOOKINGS")
    fresh_db()
    upd = FakeUpdate(FakeUser(USER_A), text=cfg.BUTTON_MY_BOOKINGS)
    ctx = FakeContext()
    run(bot.handle_message(upd, ctx))
    assert any("NO_BOOKINGS" in r[0] for r in upd.message.replies), \
        f"Ожидали NO_BOOKINGS, получили {upd.message.replies}"
    print("✅ handle_message маршрутизирует «Мои записи» → NO_BOOKINGS (записей нет)")


def test_handler_handle_message_unknown_text():
    sep("Хендлер handle_message — произвольный текст → подсказка про кнопки")
    fresh_db()
    upd = FakeUpdate(FakeUser(USER_A), text="привет случайный текст")
    ctx = FakeContext()
    run(bot.handle_message(upd, ctx))
    assert any("кнопки" in r[0].lower() for r in upd.message.replies)
    print("✅ Неизвестный текст → подсказка использовать кнопки меню")


def test_handler_final_booking_success_and_admin_notify():
    sep("Хендлер process_final_booking — успешная запись + уведомление админу")
    db = fresh_db()
    add_slot(db, 501, "10:00", date=WEEK1, week_start=WEEK1)

    q = FakeQuery(FakeUser(USER_A, 'ivan', 'Иван'), data="final_confirm_501")
    ctx = FakeContext()
    run(bot.process_final_booking(q, ctx, 501))

    assert q.edits, "Должно быть отредактировано сообщение пользователю"
    assert "подтвержден" in q.edits[-1][0].lower(), f"Текст: {q.edits[-1][0]}"
    active = db.get_user_active_booking(USER_A)
    assert active is not None and active['time'] == "10:00"
    # Уведомление администратору ушло
    assert any(chat_id == ADMIN_ID for (chat_id, _t, _k) in ctx.bot.sent), \
        f"Админ должен получить уведомление, sent={ctx.bot.sent}"
    print("✅ Запись создана, пользователю — подтверждение, админу — уведомление")


def test_handler_final_booking_blocks_second_in_week():
    sep("Хендлер process_final_booking — M1: вторая запись в той же неделе блокируется")
    db = fresh_db()
    add_slot(db, 511, "10:00", date=WEEK1, week_start=WEEK1)
    add_slot(db, 512, "12:00", date=WEEK1, week_start=WEEK1)

    # Первая запись
    ok, msg, _ = db.book_slot_with_scheduler(USER_A, 511, WEEK1)
    assert ok, msg

    # Попытка второй записи в той же неделе через хендлер
    q = FakeQuery(FakeUser(USER_A), data="final_confirm_512")
    ctx = FakeContext()
    run(bot.process_final_booking(q, ctx, 512))

    assert q.edits and "ALREADY_BOOKED" in q.edits[-1][0], \
        f"Ожидали ALREADY_BOOKED, получили {q.edits}"
    # Второй записи в БД быть не должно
    cnt = sqlite3.connect(_TMP_DB).execute(
        "SELECT COUNT(*) FROM bookings WHERE user_id=? AND status='active'", (USER_A,)
    ).fetchone()[0]
    assert cnt == 1, f"Должна остаться 1 активная запись, найдено {cnt}"
    print("✅ Вторая запись в той же календарной неделе заблокирована (хендлер + БД)")


def test_handler_show_booking_days_skips_booked_week():
    sep("Хендлер show_booking_days — M1: пропускает занятую неделю, предлагает следующую")
    db = fresh_db()
    # Неделя 1 — у пользователя уже есть запись
    add_slot(db, 601, "10:00", date=WEEK1, week_start=WEEK1)
    insert_booking(db, USER_A, 601, "10:00", WEEK1, WEEK1, status='active')
    # Неделя 2 — есть свободные слоты
    add_slot(db, 602, "11:00", date=WEEK2, week_start=WEEK2)

    # Делаем current_week_start детерминированным
    bot.slot_manager = SimpleNamespace(current_week_start=WEEK1)

    q = FakeQuery(FakeUser(USER_A))
    ctx = FakeContext()
    run(bot.show_booking_days(q, ctx))

    # Должны предложить выбор дней (неделя 2), а не ALREADY_BOOKED
    assert q.message.replies, f"Ожидали ответ с днями, replies={q.message.replies}"
    assert q.message.replies[-1][0] == "BOOKING_START", \
        f"Ожидали BOOKING_START (предложение след. недели), получили {q.message.replies[-1][0]}"
    print("✅ Неделя с записью пропущена, предложены слоты следующей недели")


def test_handler_show_booking_days_already_booked_when_no_next():
    sep("Хендлер show_booking_days — M1: запись есть, следующей недели нет → ALREADY_BOOKED")
    db = fresh_db()
    add_slot(db, 611, "10:00", date=WEEK1, week_start=WEEK1)
    insert_booking(db, USER_A, 611, "10:00", WEEK1, WEEK1, status='active')
    bot.slot_manager = SimpleNamespace(current_week_start=WEEK1)

    q = FakeQuery(FakeUser(USER_A))
    ctx = FakeContext()
    run(bot.show_booking_days(q, ctx))

    assert q.message.replies and "ALREADY_BOOKED" in q.message.replies[-1][0], \
        f"Ожидали ALREADY_BOOKED, получили {q.message.replies}"
    print("✅ Нет свободной следующей недели → показано ALREADY_BOOKED")


def test_handler_cancellation_blocked_within_3h():
    sep("Хендлер process_cancellation — отмена < 3ч запрещена")
    db = fresh_db()
    now = datetime.now(TIMEZONE)
    soon = now + timedelta(hours=2)              # через 2 часа
    date_s = soon.strftime("%Y-%m-%d"); time_s = soon.strftime("%H:%M")
    wk = monday_of(soon)
    add_slot(db, 701, "10:00", adj=time_s, date=date_s, week_start=wk)
    bid = insert_booking(db, USER_A, 701, time_s, date_s, wk, status='active')

    q = FakeQuery(FakeUser(USER_A), data=f"cancel_confirm_{bid}")
    ctx = FakeContext()
    run(bot.process_cancellation(q, ctx, bid))

    assert q.edits and "Отмена невозможна" in q.edits[-1][0]
    status = sqlite3.connect(_TMP_DB).execute(
        "SELECT status FROM bookings WHERE id=?", (bid,)).fetchone()[0]
    assert status == 'active', "Запись не должна быть отменена в пределах 3ч"
    print("✅ Отмена менее чем за 3 часа отклонена, запись осталась активной")


def test_handler_cancellation_allowed_after_3h():
    sep("Хендлер process_cancellation — отмена >= 3ч проходит + уведомление админу")
    db = fresh_db()
    now = datetime.now(TIMEZONE)
    later = now + timedelta(hours=5)             # через 5 часов
    date_s = later.strftime("%Y-%m-%d"); time_s = later.strftime("%H:%M")
    wk = monday_of(later)
    add_slot(db, 711, "10:00", adj=time_s, date=date_s, week_start=wk)
    bid = insert_booking(db, USER_A, 711, time_s, date_s, wk, status='active')

    q = FakeQuery(FakeUser(USER_A, 'ivan', 'Иван'), data=f"cancel_confirm_{bid}")
    ctx = FakeContext()
    run(bot.process_cancellation(q, ctx, bid))

    assert q.edits and "отменена" in q.edits[-1][0].lower()
    status = sqlite3.connect(_TMP_DB).execute(
        "SELECT status FROM bookings WHERE id=?", (bid,)).fetchone()[0]
    assert status == 'cancelled', "Запись должна быть отменена"
    assert any(chat_id == ADMIN_ID for (chat_id, _t, _k) in ctx.bot.sent), \
        "Админ должен получить уведомление об отмене"
    print("✅ Отмена за 5 часов прошла, статус cancelled, админ уведомлён")


def test_handler_callback_routing_back_to_main():
    sep("Хендлер callback_handler — маршрутизация back_to_main")
    fresh_db()
    q = FakeQuery(FakeUser(USER_A), data="back_to_main")
    ctx = FakeContext()
    run(bot.callback_handler(_wrap_update(q), ctx))
    assert q.answered, "callback должен вызвать query.answer()"
    assert q.edits and q.edits[-1][0] == "WELCOME"
    print("✅ callback_handler: back_to_main → WELCOME, answer() вызван")


def _wrap_update(query):
    """callback_handler читает update.callback_query."""
    return SimpleNamespace(callback_query=query)


# ══════════════════════════════════════════════════════════════════════════════
# H2 / H4 / H5
# ══════════════════════════════════════════════════════════════════════════════

def test_h2_escape_markdown():
    sep("H2 — экранирование спецсимволов Markdown в пользовательских данных")
    assert bot._esc_md("a_b*c`[d") == "a\\_b\\*c\\`\\[d"
    assert bot._esc_md(None) == ""
    print("✅ _esc_md экранирует _ * ` [ и обрабатывает None")


def test_h5_myid_hides_admin_list_for_non_admin():
    sep("H5 — /myid не раскрывает список админов обычному пользователю")
    fresh_db()
    # Обычный пользователь
    upd = FakeUpdate(FakeUser(USER_A, 'ivan', 'Иван'))
    run(bot.show_my_id(upd, FakeContext()))
    assert upd.message.replies
    assert "админы" not in upd.message.replies[-1][0].lower(), \
        "Список админов не должен показываться обычному пользователю"
    print("✅ Обычный пользователь не видит список ADMIN_IDS")

    # Администратор
    upd2 = FakeUpdate(FakeUser(ADMIN_ID, 'admin', 'Админ'))
    run(bot.show_my_id(upd2, FakeContext()))
    assert "админы" in upd2.message.replies[-1][0].lower(), \
        "Администратор должен видеть список админов"
    print("✅ Администратор видит список админов")


def test_h4_send_with_retry_handles_retryafter():
    sep("H4 — _send_with_retry переживает флуд-лимит Telegram (RetryAfter)")
    from telegram.error import RetryAfter as _RA

    class FlakyBot:
        def __init__(self):
            self.calls = 0
        async def send_message(self, chat_id, text, **kw):
            self.calls += 1
            if self.calls == 1:
                raise _RA(0)   # первый раз — флуд-лимит
            return True

    fb = FlakyBot()
    ok = run(bot._send_with_retry(fb, 123, "hi"))
    assert ok and fb.calls == 2, f"Ожидали успех со 2-й попытки, calls={fb.calls}"
    print("✅ После RetryAfter повторная попытка отправки успешна")


# ══════════════════════════════════════════════════════════════════════════════
# M1: одна запись в КАЛЕНДАРНУЮ неделю (включая прошедшие сессии)
# ══════════════════════════════════════════════════════════════════════════════

def test_m1_past_session_blocks_same_week():
    sep("M1 — прошедшая сессия на неделе блокирует повторную запись на ту же неделю")
    db = fresh_db()
    add_slot(db, 801, "10:00", date=WEEK1, week_start=WEEK1)
    add_slot(db, 802, "12:00", date=WEEK1, week_start=WEEK1)
    # «Прошедшая» сессия: запись со статусом completed на той же неделе
    insert_booking(db, USER_A, 801, "10:00", WEEK1, WEEK1, status='completed')

    ok, msg, _ = db.book_slot_with_scheduler(USER_A, 802, WEEK1)
    assert not ok, "Запись должна быть заблокирована — на неделе уже была сессия"
    assert "уже есть запись" in msg.lower() or "неделе" in msg.lower(), msg
    print(f"✅ Повтор на ту же неделю после прошедшей сессии заблокирован: «{msg[:40]}…»")


def test_m1_next_week_allowed_after_session():
    sep("M1 — после сессии на неделе1 запись на неделю2 разрешена")
    db = fresh_db()
    add_slot(db, 811, "10:00", date=WEEK1, week_start=WEEK1)
    add_slot(db, 812, "11:00", date=WEEK2, week_start=WEEK2)
    insert_booking(db, USER_A, 811, "10:00", WEEK1, WEEK1, status='completed')

    ok, msg, info = db.book_slot_with_scheduler(USER_A, 812, WEEK2)
    assert ok, f"Запись на следующую неделю должна пройти: {msg}"
    print("✅ Запись на слоты следующей недели разрешена")


def test_m1_cancelled_does_not_block():
    sep("M1 — отменённая запись не занимает неделю")
    db = fresh_db()
    add_slot(db, 821, "10:00", date=WEEK1, week_start=WEEK1)
    add_slot(db, 822, "12:00", date=WEEK1, week_start=WEEK1)
    insert_booking(db, USER_A, 821, "10:00", WEEK1, WEEK1, status='cancelled')

    ok, msg, _ = db.book_slot_with_scheduler(USER_A, 822, WEEK1)
    assert ok, f"После отмены запись на той же неделе должна быть возможна: {msg}"
    print("✅ Отменённая запись не блокирует новую запись на той же неделе")


# ══════════════════════════════════════════════════════════════════════════════
# КОНКУРЕНТНОСТЬ
# ══════════════════════════════════════════════════════════════════════════════

def test_concurrent_same_slot_single_winner():
    sep("Конкурентность — гонка за ОДИН слот: ровно один победитель")
    extra = [2001, 2002, 2003, 2004, 2005]
    db = fresh_db(extra_users=extra)
    add_slot(db, 901, "10:00", date=WEEK1, week_start=WEEK1)

    results = []
    lock = threading.Lock()
    barrier = threading.Barrier(len(extra))

    def worker(uid):
        barrier.wait()  # стартуем максимально одновременно
        ok, msg, _ = db.book_slot_with_scheduler(uid, 901, WEEK1)
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=worker, args=(u,)) for u in extra]
    for t in threads: t.start()
    for t in threads: t.join()

    successes = sum(1 for r in results if r)
    assert successes == 1, f"Ожидали ровно 1 успешную запись, получили {successes}"

    # В БД: слот занят, ровно одна активная запись на него
    row = sqlite3.connect(_TMP_DB).execute(
        "SELECT is_available FROM time_slots WHERE id=901").fetchone()
    assert row[0] == 0, "Слот должен быть занят"
    cnt = sqlite3.connect(_TMP_DB).execute(
        "SELECT COUNT(*) FROM bookings WHERE slot_id=901 AND status='active'").fetchone()[0]
    assert cnt == 1, f"Должна быть ровно 1 запись на слот, найдено {cnt}"
    print(f"✅ Из {len(extra)} параллельных попыток победила ровно одна, дублей нет")


def test_concurrent_different_slots_no_lost_update():
    sep("Конкурентность — разные слоты параллельно: обе записи сохранены (нет lost update, C2)")
    extra = [3001, 3002]
    db = fresh_db(extra_users=extra)
    # Слоты далеко друг от друга — алгоритм сдвига их не связывает
    add_slot(db, 911, "10:00", date=WEEK1, week_start=WEEK1)
    add_slot(db, 912, "14:00", date=WEEK1, week_start=WEEK1)

    results = {}
    lock = threading.Lock()
    barrier = threading.Barrier(2)
    pairs = [(3001, 911), (3002, 912)]

    def worker(uid, sid):
        barrier.wait()
        ok, msg, _ = db.book_slot_with_scheduler(uid, sid, WEEK1)
        with lock:
            results[uid] = (ok, msg)

    threads = [threading.Thread(target=worker, args=p) for p in pairs]
    for t in threads: t.start()
    for t in threads: t.join()

    assert all(ok for ok, _ in results.values()), f"Обе записи должны пройти: {results}"
    booked = sqlite3.connect(_TMP_DB).execute(
        "SELECT COUNT(*) FROM time_slots WHERE id IN (911,912) AND is_available=0").fetchone()[0]
    assert booked == 2, f"Оба слота должны быть заняты, занято={booked} (lost update!)"
    active = sqlite3.connect(_TMP_DB).execute(
        "SELECT COUNT(*) FROM bookings WHERE status='active'").fetchone()[0]
    assert active == 2, f"Должно быть 2 активные записи, найдено {active}"
    print("✅ Параллельные записи на разные слоты обе сохранены — потери обновлений нет")


# ══════════════════════════════════════════════════════════════════════════════
# H3: тайминг напоминаний (не раньше чем за 15 минут / 24 часа)
# ══════════════════════════════════════════════════════════════════════════════

def _make_session_at(db, slot_id, dt: datetime, status='active'):
    date_s = dt.strftime("%Y-%m-%d"); time_s = dt.strftime("%H:%M")
    wk = monday_of(dt)
    add_slot(db, slot_id, time_s, adj=time_s, date=date_s, week_start=wk)
    insert_booking(db, USER_A, slot_id, time_s, date_s, wk, status=status)


def test_reminder_15min_not_earlier():
    sep("H3 — 15-мин напоминание: за 18/20 мин НЕ уходит, за 15 — уходит")
    now = datetime.now(TIMEZONE)

    # Сессия через 20 минут — не должна попасть в выборку
    db = fresh_db()
    _make_session_at(db, 1001001, now + timedelta(minutes=20))
    assert db.get_upcoming_sessions(15) == [], "За 20 минут напоминание не должно уходить"
    print("✅ За 20 минут — напоминание НЕ отправляется")

    # Сессия через 18 минут — тоже не должна (раньше был баг «за 18»)
    db = fresh_db()
    _make_session_at(db, 1001002, now + timedelta(minutes=18))
    assert db.get_upcoming_sessions(15) == [], "За 18 минут напоминание не должно уходить"
    print("✅ За 18 минут — напоминание НЕ отправляется (баг устранён)")

    # Сессия через 15 минут — должна попасть
    db = fresh_db()
    _make_session_at(db, 1001003, now + timedelta(minutes=15))
    found = db.get_upcoming_sessions(15)
    assert len(found) == 1, f"За 15 минут напоминание должно уйти, found={found}"
    print("✅ За 15 минут — напоминание отправляется")


def test_reminder_15min_dedup_and_retry():
    sep("H3 — 15-мин напоминание: защита от дублей и повтор после сброса флага")
    now = datetime.now(TIMEZONE)
    db = fresh_db()
    _make_session_at(db, 1002001, now + timedelta(minutes=14))

    first = db.get_upcoming_sessions(15)
    assert len(first) == 1
    assert db.get_upcoming_sessions(15) == [], "Повторно (notified=1) — не должно вернуться"
    db.reset_notification_for_booking(first[0]['booking_id'])
    assert len(db.get_upcoming_sessions(15)) == 1, "После сброса флага повтор разрешён"
    print("✅ Дубли отсекаются, при сбросе флага возможна повторная попытка")


def test_reminder_24h_not_earlier():
    sep("H3 — 24-ч напоминание: за 24ч03м и за 12ч НЕ уходит, за 24ч — уходит")
    now = datetime.now(TIMEZONE)

    # За 24ч + 3 минуты — не должно (раньше был баг «за 24ч 3мин»)
    db = fresh_db()
    _make_session_at(db, 1003001, now + timedelta(hours=24, minutes=3))
    assert db.get_upcoming_sessions_24h() == [], "За 24ч03м не должно уходить"
    print("✅ За 24 часа 3 минуты — НЕ отправляется (баг устранён)")

    # За 12 часов — не должно (это не «за сутки»; ложное срабатывание исключено)
    db = fresh_db()
    _make_session_at(db, 1003002, now + timedelta(hours=12))
    assert db.get_upcoming_sessions_24h() == [], "За 12 часов 24ч-напоминание не нужно"
    print("✅ За 12 часов — 24ч-напоминание НЕ отправляется")

    # За 24 часа (минус минута, чтобы попасть в окно) — должно уйти
    db = fresh_db()
    _make_session_at(db, 1003003, now + timedelta(hours=24) - timedelta(minutes=1))
    found = db.get_upcoming_sessions_24h()
    assert len(found) == 1, f"За ~24 часа напоминание должно уйти, found={found}"
    print("✅ За ~24 часа — напоминание отправляется")


# ══════════════════════════════════════════════════════════════════════════════

ALL_TESTS = [
    test_handler_start_registers_user,
    test_handler_handle_message_routes_my_bookings,
    test_handler_handle_message_unknown_text,
    test_handler_final_booking_success_and_admin_notify,
    test_handler_final_booking_blocks_second_in_week,
    test_handler_show_booking_days_skips_booked_week,
    test_handler_show_booking_days_already_booked_when_no_next,
    test_handler_cancellation_blocked_within_3h,
    test_handler_cancellation_allowed_after_3h,
    test_handler_callback_routing_back_to_main,
    test_h2_escape_markdown,
    test_h5_myid_hides_admin_list_for_non_admin,
    test_h4_send_with_retry_handles_retryafter,
    test_m1_past_session_blocks_same_week,
    test_m1_next_week_allowed_after_session,
    test_m1_cancelled_does_not_block,
    test_concurrent_same_slot_single_winner,
    test_concurrent_different_slots_no_lost_update,
    test_reminder_15min_not_earlier,
    test_reminder_15min_dedup_and_retry,
    test_reminder_24h_not_earlier,
]


if __name__ == '__main__':
    try:
        for t in ALL_TESTS:
            t()
        print("\n" + "=" * 60)
        print(f"  🎉  Все {len(ALL_TESTS)} тестов хендлеров/конкурентности пройдены!")
        print("=" * 60)
    finally:
        if os.path.exists(_TMP_DB):
            os.remove(_TMP_DB)
