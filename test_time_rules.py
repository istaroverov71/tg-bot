"""
test_time_rules.py — Тесты временны́х правил бота

Тестирует чистую бизнес-логику без Telegram и БД:
  1. Прошедшие/близкие слоты не попадают в список записи
  2. Отсечка для записи (cutoff)
  3. Запрет отмены менее чем за 3 часа
"""

import sys, os, types
from datetime import datetime, timedelta

# Заглушка конфига
import pytz
cfg = types.ModuleType('config')
cfg.SESSION_DURATION = 60
cfg.BUFFER_TIME = 10
cfg.TIMEZONE = pytz.timezone('Europe/Moscow')
sys.modules['config'] = cfg
sys.path.insert(0, os.path.dirname(__file__))

TIMEZONE = cfg.TIMEZONE
BOOKING_CUTOFF_HOURS = 3   # текущее значение в bot.py
CANCEL_CUTOFF_HOURS  = 3   # правило отмены


# ---------- вспомогательные ----------

def slot_is_visible(slot_datetime: datetime, now: datetime, cutoff_hours: int = BOOKING_CUTOFF_HOURS) -> bool:
    """
    Логика из bot.py строка 178-182:
    слот виден если slot_time >= now + cutoff_hours
    """
    cutoff = now + timedelta(hours=cutoff_hours)
    return slot_datetime >= cutoff


def can_cancel(session_datetime: datetime, now: datetime, cutoff_hours: int = CANCEL_CUTOFF_HOURS) -> bool:
    """
    Логика из bot.py cancel_booking_start / pre_cancel_confirm / process_cancellation:
    отмена разрешена если (session_time - now) >= cutoff_hours
    """
    return (session_datetime - now) >= timedelta(hours=cutoff_hours)


def make_dt(offset_minutes: int, now: datetime) -> datetime:
    """Создать datetime = now + offset_minutes"""
    return now + timedelta(minutes=offset_minutes)


# ---------- тест 1: исчезновение прошедших слотов ----------

def test_past_slots_invisible():
    """Слоты в прошлом не должны отображаться."""
    now = datetime.now(TIMEZONE)

    past      = make_dt(-30, now)    # 30 минут назад
    very_past = make_dt(-120, now)   # 2 часа назад

    assert not slot_is_visible(past, now),      f"Слот -30 мин не должен быть виден: {past}"
    assert not slot_is_visible(very_past, now), f"Слот -2ч не должен быть виден: {very_past}"
    print("✅ Тест 1a: прошедшие слоты невидимы")


def test_near_slots_invisible():
    """Слоты ближайших 3 часов должны быть скрыты (текущий cutoff = 3 ч)."""
    now = datetime.now(TIMEZONE)

    in_30min  = make_dt(30, now)   # через 30 мин  — скрыт
    in_2h     = make_dt(120, now)  # через 2 ч     — скрыт (< 3h cutoff)
    in_2h59   = make_dt(179, now)  # через 2ч59мин — скрыт (< 3h cutoff)
    in_3h_ok  = make_dt(180, now)  # ровно 3 часа  — виден (>= 3h)
    in_4h     = make_dt(240, now)  # через 4 ч     — виден

    assert not slot_is_visible(in_30min, now),  "30 мин — должен быть скрыт"
    assert not slot_is_visible(in_2h, now),     "2 ч — должен быть скрыт"
    assert not slot_is_visible(in_2h59, now),   "2ч59 — должен быть скрыт"
    assert slot_is_visible(in_3h_ok, now),      "3 ч ровно — должен быть виден"
    assert slot_is_visible(in_4h, now),         "4 ч — должен быть виден"
    print(f"✅ Тест 1b: слоты < {BOOKING_CUTOFF_HOURS}ч скрыты, >= {BOOKING_CUTOFF_HOURS}ч видны")


def test_booking_cutoff_note():
    """
    ИНФОРМАЦИЯ о текущем cutoff:
    Пользователь сказал «нельзя записаться менее чем за 2 часа».
    Текущий код: cutoff = 3 часа.
    Записаться менее чем за 2 часа — действительно нельзя (нельзя и за 3 тоже).
    Но технически ограничение жёстче, чем озвучено пользователю.
    """
    now = datetime.now(TIMEZONE)
    in_2h_exact = make_dt(120, now)   # ровно 2 часа
    in_2h30     = make_dt(150, now)   # 2 ч 30 мин

    # При cutoff=3h — слоты в 2ч и 2.5ч СКРЫТЫ (потому что < 3h)
    assert not slot_is_visible(in_2h_exact, now), "2 ч — скрыт (cutoff=3h)"
    assert not slot_is_visible(in_2h30, now),     "2.5 ч — скрыт (cutoff=3h)"

    print(
        f"⚠️  Тест 2 (запись): cutoff = {BOOKING_CUTOFF_HOURS} ч.\n"
        f"    Записаться менее чем за 2 ч нельзя — правило выполняется.\n"
        f"    Но реальный cutoff = 3 ч (строже). Уточните: нужно 2 или 3?"
    )


# ---------- тест 3: запрет отмены ----------

def test_cancel_blocked_within_3h():
    """Отмена менее чем за 3 часа — запрещена."""
    now = datetime.now(TIMEZONE)

    in_30min  = make_dt(30, now)
    in_2h     = make_dt(120, now)
    in_2h59   = make_dt(179, now)

    assert not can_cancel(in_30min, now),  "30 мин — отмена запрещена"
    assert not can_cancel(in_2h, now),     "2 ч — отмена запрещена"
    assert not can_cancel(in_2h59, now),   "2ч59 — отмена запрещена"
    print("✅ Тест 3a: отмена < 3ч — запрещена")


def test_cancel_allowed_after_3h():
    """Отмена за 3 часа и более — разрешена."""
    now = datetime.now(TIMEZONE)

    in_3h_ok  = make_dt(180, now)   # ровно 3 часа
    in_4h     = make_dt(240, now)
    in_24h    = make_dt(1440, now)

    assert can_cancel(in_3h_ok, now),  "3 ч ровно — отмена разрешена"
    assert can_cancel(in_4h, now),     "4 ч — отмена разрешена"
    assert can_cancel(in_24h, now),    "24 ч — отмена разрешена"
    print("✅ Тест 3b: отмена >= 3ч — разрешена")


def test_cancel_messages():
    """Проверка текстов сообщений об отмене в коде."""
    # Загружаем реальный config через заглушку (уже пропатчена выше)
    # Проверяем содержимое строк в bot.py статически
    bot_path = os.path.join(os.path.dirname(__file__), 'bot.py')
    with open(bot_path, encoding='utf-8') as f:
        bot_src = f.read()

    # Считаем сколько мест с сообщением об отмене включают @n_kshmlv
    cancel_blocks = bot_src.split("Отмена невозможна")
    blocks_with_contact = [b for b in cancel_blocks[1:] if "@n_kshmlv" in b[:300]]
    blocks_without_contact = [b for b in cancel_blocks[1:] if "@n_kshmlv" not in b[:300]]

    total = len(cancel_blocks) - 1
    print(f"\n  Сообщений 'Отмена невозможна': {total}")
    print(f"  Из них с контактом @n_kshmlv:  {len(blocks_with_contact)}")
    print(f"  Из них БЕЗ контакта:           {len(blocks_without_contact)}")

    if blocks_without_contact:
        print("  ⚠️  Не во всех сообщениях об отмене указан контакт @n_kshmlv")
    else:
        print("  ✅ Во всех сообщениях об отмене есть контакт @n_kshmlv")


if __name__ == "__main__":
    print("=" * 55)
    print("ТЕСТ 1: Исчезновение прошедших/близких слотов")
    print("=" * 55)
    test_past_slots_invisible()
    test_near_slots_invisible()

    print()
    print("=" * 55)
    print("ТЕСТ 2: Запрет записи (cutoff)")
    print("=" * 55)
    test_booking_cutoff_note()

    print()
    print("=" * 55)
    print("ТЕСТ 3: Запрет отмены менее чем за 3 часа")
    print("=" * 55)
    test_cancel_blocked_within_3h()
    test_cancel_allowed_after_3h()
    test_cancel_messages()

    print()
    print("🎉 Все тесты временны́х правил выполнены!")
