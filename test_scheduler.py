"""
test_scheduler_fixed.py — Тесты для исправленного алгоритма

Запуск (без Telegram и БД):
    python test_scheduler_fixed.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

# Патчим config, чтобы не нужен был pytz
import types
cfg = types.ModuleType('config')
cfg.SESSION_DURATION = 60
cfg.BUFFER_TIME = 10
import pytz
cfg.TIMEZONE = pytz.timezone('Europe/Moscow')
sys.modules['config'] = cfg

from slot_scheduler import Slot, SmartScheduler  # noqa: E402

DATE = "2024-03-18"
WEEK = "2024-03-18"

def make_slot(id_, base):
    return Slot(id_, base, base, "Понедельник", DATE, WEEK, False, None)

def make_booked(id_, base, user=99):
    return Slot(id_, base, base, "Понедельник", DATE, WEEK, True, user)


def test_a_forward_shift():
    """Сценарий А: 13:00 занят → 14:00 сдвигается на 14:10"""
    slots = [make_slot(1, "13:00"), make_slot(2, "14:00")]
    s = SmartScheduler(slots)
    s.slots[0].is_booked = True   # бронируем 13:00 напрямую

    visible = s.get_visible_slots()
    assert len(visible) == 1, f"Ожидали 1 слот, получили {len(visible)}"
    assert visible[0].current_time == "14:10", (
        f"Ожидали 14:10, получили {visible[0].current_time}"
    )
    print("✅ Сценарий А: 14:00 → 14:10")


def test_a_via_book_slot():
    """Сценарий А через book_slot: 13:00 → следующий 14:00 становится 14:10"""
    slots = [make_slot(1, "13:00"), make_slot(2, "14:00")]
    s = SmartScheduler(slots)
    ok, booked_time, changes = s.book_slot(1, 100)

    assert ok
    visible = s.get_visible_slots()
    assert len(visible) == 1
    assert visible[0].current_time == "14:10", (
        f"Ожидали 14:10, получили {visible[0].current_time}"
    )
    print("✅ Сценарий А (book_slot): 14:00 → 14:10")


def test_b_no_shift_if_gap_ok():
    """Сценарий Б: 13:00 занят, 15:00 свободен — 15:00 остаётся на месте"""
    slots = [make_slot(1, "13:00"), make_slot(2, "14:00"), make_slot(3, "15:00")]
    s = SmartScheduler(slots)
    s.book_slot(1, 100)  # бронируем 13:00

    slot2 = s.find_slot_by_id(2)
    slot3 = s.find_slot_by_id(3)
    s.get_visible_slots()  # обновляем current_time

    assert slot2.current_time == "14:10", f"slot2: {slot2.current_time}"
    # slot3 (15:00): от 14:10 до 15:00 = 50 мин < 70 мин (MIN_GAP)
    # значит 15:00 должен сдвинуться вперёд: 14:10 + 70 = 15:20
    assert slot3.current_time == "15:20", f"slot3: {slot3.current_time}"
    print(f"✅ Сценарий Б: slot2={slot2.current_time}, slot3={slot3.current_time}")


def test_c_backward_shift():
    """Сценарий В: 13:00 и 15:00 заняты → 14:00 сдвигается назад на 12:50"""
    slots = [
        make_slot(1, "12:00"),
        make_booked(2, "13:00"),  # занят
        make_slot(3, "14:00"),
        make_booked(4, "15:00"),  # занят
    ]
    s = SmartScheduler(slots)
    visible = s.get_visible_slots()

    slot1 = s.find_slot_by_id(1)
    slot3 = s.find_slot_by_id(3)

    # slot3 (14:00): prev=13:00, next=15:00
    # min_start = 13:00 + 60 + 10 = 14:10
    # max_start = 15:00 - 70 = 13:50
    # min_start > max_start → слот недоступен
    visible_ids = {s.id for s in visible}
    assert 3 not in visible_ids, f"Слот 3 должен быть недоступен (нет окна)"

    # slot1 (12:00): next=13:00
    # max_start = 13:00 - 70 = 11:50
    # base=12:00 > max_start=11:50 → сдвигаем к 11:50
    assert slot1.current_time == "11:50", f"slot1: {slot1.current_time}"
    print(f"✅ Сценарий В: slot1={slot1.current_time}, slot3 недоступен")


def test_d_chain():
    """Сценарий Г: каскадные сдвиги вперёд.

    Слоты: id=1→10:00, id=2→11:00 (занят), id=3→12:00, id=4→13:00, id=5→14:00
    После бронирования 11:00 (id=2):
      id=1 (10:00): только занятый справа (11:00) → max_start = 11:00 - 70 = 9:50
                    min(10:00, 9:50) = 9:50
      id=3 (12:00): только занятый слева (11:00)  → min_start = 11:00 + 70 = 12:10
                    max(12:00, 12:10) = 12:10
      id=4 (13:00): каскад от id=3 (12:10)        → min_start = 12:10 + 70 = 13:20
      id=5 (14:00): каскад от id=4 (13:20)        → min_start = 13:20 + 70 = 14:30
    """
    slots = [make_slot(i, f"{9+i}:00") for i in range(1, 6)]
    # Бронируем id=2 (base_time = "11:00")
    s = SmartScheduler(slots)
    s.book_slot(2, 100)

    visible = s.get_visible_slots()
    times = {v.id: v.current_time for v in visible}
    print(f"  Слот id=1 (10:00): {times.get(1)}")
    print(f"  Слот id=3 (12:00): {times.get(3)}")
    print(f"  Слот id=4 (13:00): {times.get(4)}")
    print(f"  Слот id=5 (14:00): {times.get(5)}")

    assert times.get(1) == "09:50", f"slot1: {times.get(1)}"
    # После бронирования 11:00 слот 12:00 сдвигается: 11:00 + 70 = 12:10
    assert times.get(3) == "12:10", f"slot3: {times.get(3)}"
    # Каскад: 12:10 + 70 = 13:20
    assert times.get(4) == "13:20", f"slot4: {times.get(4)}"
    # Каскад: 13:20 + 70 = 14:30
    assert times.get(5) == "14:30", f"slot5: {times.get(5)}"
    print("✅ Сценарий Г: каскадные сдвиги — OK")


def test_cancel_restores():
    """Отмена бронирования возвращает слоты к базовому времени"""
    slots = [make_slot(1, "13:00"), make_slot(2, "14:00")]
    s = SmartScheduler(slots)
    s.book_slot(1, 100)

    visible = s.get_visible_slots()
    assert visible[0].current_time == "14:10"

    s.cancel_booking(1)
    visible = s.get_visible_slots()
    times = sorted(v.current_time for v in visible)
    assert times == ["13:00", "14:00"], f"После отмены: {times}"
    print("✅ Отмена: слоты вернулись на базовое время")


if __name__ == "__main__":
    test_a_forward_shift()
    test_a_via_book_slot()
    test_b_no_shift_if_gap_ok()
    test_c_backward_shift()
    test_d_chain()
    test_cancel_restores()
    print("\n🎉 Все тесты пройдены!")
