# slot_scheduler.py — ИСПРАВЛЕННАЯ ВЕРСИЯ
#
# Алгоритм сдвига слотов:
#   Сессия = SESSION_DURATION мин (60), Буфер = BUFFER_TIME мин (10)
#   Минимальный интервал между началами = 60 + 10 = 70 мин
#
#   Сдвиг вперёд:
#     Если слот X занят, следующий свободный слот (X+1) сдвигается на:
#     X.base_time + SESSION_DURATION + BUFFER_TIME
#     Пример: 13:00 занят → 14:00 становится 14:10
#
#   Сдвиг назад:
#     Если и предыдущий (X-1), и следующий (X+1) слоты заняты,
#     текущий свободный (X) сдвигается назад:
#     max_start = (X+1).base_time - SESSION_DURATION - BUFFER_TIME
#     Если базовое время X > max_start → сдвиг:
#       new_time = (X-1).base_time + SESSION_DURATION + BUFFER_TIME
#     Пример: 13:00 и 15:00 заняты → 14:00 становится 12:50
#              (12:00 свободно, т.к. 13:00 + 60 = 14:00 > 12:50 — нет конфликта)

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
from config import TIMEZONE, SESSION_DURATION, BUFFER_TIME

# Минимальный интервал между началами двух сессий
MIN_GAP = SESSION_DURATION + BUFFER_TIME  # 70 минут


@dataclass
class Slot:
    """Класс для представления слота в алгоритме"""
    id: Optional[int]
    base_time: str       # Исходное время ("13:00")
    current_time: str    # Текущее отображаемое время ("14:10", "12:50")
    day: str
    date: str
    week_start: str
    is_booked: bool
    booked_by: Optional[int]

    def get_datetime(self) -> datetime:
        """Получить datetime для current_time (московское время через localize)"""
        dt_str = f"{self.date} {self.current_time}"
        return TIMEZONE.localize(datetime.strptime(dt_str, "%Y-%m-%d %H:%M"))

    def get_base_datetime(self) -> datetime:
        """Получить datetime для base_time (московское время через localize)"""
        dt_str = f"{self.date} {self.base_time}"
        return TIMEZONE.localize(datetime.strptime(dt_str, "%Y-%m-%d %H:%M"))

    def get_end_time(self) -> datetime:
        """Время окончания сессии (по current_time)"""
        return self.get_datetime() + timedelta(minutes=SESSION_DURATION)


class SmartScheduler:
    """
    Умный планировщик сессий с автоматическим смещением слотов.

    Правила:
    - SESSION_DURATION = 60 мин
    - BUFFER_TIME = 10 мин (перерыв между сессиями)
    - MIN_GAP = 70 мин между началами

    После каждого бронирования пересчитываются времена соседних свободных слотов.
    """

    def __init__(self, slots: List[Slot]):
        self.slots = sorted(slots, key=lambda s: (s.date, s.base_time))
        self.slots_by_id: Dict[int, Slot] = {
            s.id: s for s in self.slots if s.id is not None
        }

    def find_slot_by_id(self, slot_id: int) -> Optional[Slot]:
        return self.slots_by_id.get(slot_id)

    def get_booked_slots(self) -> List[Slot]:
        return [s for s in self.slots if s.is_booked]

    def get_available_slots(self) -> List[Slot]:
        return [s for s in self.slots if not s.is_booked]

    # ------------------------------------------------------------------
    # ОСНОВНОЙ АЛГОРИТМ
    # ------------------------------------------------------------------

    def calculate_slot_positions(self) -> Dict[int, str]:
        """
        Рассчитать отображаемое время для каждого свободного слота.

        Алгоритм (по каждому дню отдельно):
        1. Зафиксировать занятые слоты на базовом времени.
        2. Для каждого свободного слота найти ближайших занятых соседей
           слева (prev) и справа (next).
        3. Применить сдвиг вперёд/назад по правилам.
        4. Если слот не влезает в окно — пометить как недоступный (None).

        Возвращает {slot_id: "HH:MM"} только для доступных слотов.
        """
        # Группируем по дням
        slots_by_day: Dict[str, List[Slot]] = {}
        for slot in self.slots:
            slots_by_day.setdefault(slot.date, []).append(slot)

        result: Dict[int, str] = {}

        for date, day_slots in slots_by_day.items():
            # Сортируем по базовому времени внутри дня
            day_slots.sort(key=lambda s: s.base_time)
            day_result = self._calculate_day_positions(day_slots)
            result.update(day_result)

        return result

    def _calculate_day_positions(self, day_slots: List[Slot]) -> Dict[int, str]:
        """Рассчитать позиции для одного дня."""
        n = len(day_slots)
        # base times как datetime (только часы и минуты, дата не важна для сравнений)
        base_dt = [datetime.strptime(s.base_time, "%H:%M") for s in day_slots]
        # Результирующие времена; для занятых — base_time, для свободных — вычисляем
        result_dt: List[Optional[datetime]] = [None] * n

        # Фиксируем занятые: используем current_time (подтверждённое/сдвинутое время)
        # Баг 1: раньше стояло base_dt[i] — из-за этого следующий свободный слот
        # считал min_start от 11:00 (базы), а не от 11:10 (реального времени записи),
        # и разрыв между соседними сессиями получался 60 мин вместо 70.
        for i, slot in enumerate(day_slots):
            if slot.is_booked:
                result_dt[i] = datetime.strptime(slot.current_time, "%H:%M")

        # Вычисляем свободные
        for i, slot in enumerate(day_slots):
            if slot.is_booked:
                continue

            # Ближайший занятый СЛЕВА
            prev_idx: Optional[int] = None
            for j in range(i - 1, -1, -1):
                if day_slots[j].is_booked:
                    prev_idx = j
                    break

            # Ближайший занятый СПРАВА
            next_idx: Optional[int] = None
            for j in range(i + 1, n):
                if day_slots[j].is_booked:
                    next_idx = j
                    break

            base = base_dt[i]

            if prev_idx is None and next_idx is None:
                # Нет занятых соседей — остаёмся на месте
                result_dt[i] = base

            elif prev_idx is not None and next_idx is None:
                # Только занятый слева: возможен сдвиг ВПЕРЁД
                prev_end = result_dt[prev_idx] + timedelta(minutes=SESSION_DURATION)
                min_start = prev_end + timedelta(minutes=BUFFER_TIME)
                result_dt[i] = max(base, min_start)

            elif prev_idx is None and next_idx is not None:
                # Только занятый справа: возможен сдвиг НАЗАД
                next_start = result_dt[next_idx]
                max_start = next_start - timedelta(minutes=MIN_GAP)
                if max_start < datetime.strptime("00:00", "%H:%M"):
                    result_dt[i] = None  # недоступен
                else:
                    result_dt[i] = min(base, max_start)

            else:
                # Занятые и слева, и справа
                prev_end = result_dt[prev_idx] + timedelta(minutes=SESSION_DURATION)
                min_start = prev_end + timedelta(minutes=BUFFER_TIME)
                next_start = result_dt[next_idx]
                max_start = next_start - timedelta(minutes=MIN_GAP)

                if min_start > max_start:
                    # Нет места — слот недоступен
                    result_dt[i] = None
                else:
                    # Прижимаем к границам окна, если нужно
                    clamped = max(min_start, min(base, max_start))
                    result_dt[i] = clamped

        # Скрываем слоты, которые стали бы 4-м (или более) подряд в цепочке.
        # Для каждого свободного слота считаем занятые соседние слоты подряд:
        # непрерывно влево и вправо (разрыв ≤ MIN_GAP). Если сумма ≥ 3 — слот
        # скрываем: бронировать его всё равно нельзя, лучше не вводить в заблуждение.
        for i, slot in enumerate(day_slots):
            if slot.is_booked or result_dt[i] is None:
                continue

            left_booked = 0
            prev_t = result_dt[i]
            for j in range(i - 1, -1, -1):
                if result_dt[j] is None:
                    break
                gap = (prev_t - result_dt[j]).total_seconds() / 60
                if day_slots[j].is_booked and gap <= MIN_GAP:
                    left_booked += 1
                    prev_t = result_dt[j]
                else:
                    break

            right_booked = 0
            prev_t = result_dt[i]
            for j in range(i + 1, n):
                if result_dt[j] is None:
                    break
                gap = (result_dt[j] - prev_t).total_seconds() / 60
                if day_slots[j].is_booked and gap <= MIN_GAP:
                    right_booked += 1
                    prev_t = result_dt[j]
                else:
                    break

            if left_booked + right_booked >= 3:
                result_dt[i] = None  # 4-я в цепочке — скрываем

        # Формируем результат: только доступные (не None, не занятые)
        day_result: Dict[int, str] = {}
        for i, slot in enumerate(day_slots):
            if not slot.is_booked and result_dt[i] is not None and slot.id is not None:
                day_result[slot.id] = result_dt[i].strftime("%H:%M")

        return day_result

    # ------------------------------------------------------------------
    # ПУБЛИЧНЫЕ МЕТОДЫ
    # ------------------------------------------------------------------

    def get_visible_slots(self) -> List[Slot]:
        """
        Получить слоты, видимые пользователям (свободные с актуальным временем).
        Обновляет current_time каждого слота.
        """
        new_times = self.calculate_slot_positions()
        visible: List[Slot] = []

        for slot in self.slots:
            if not slot.is_booked and slot.id in new_times:
                slot.current_time = new_times[slot.id]
                visible.append(slot)

        return sorted(visible, key=lambda s: (s.date, s.current_time))

    def book_slot(
        self, slot_id: int, user_id: int, is_admin: bool = False
    ) -> Tuple[bool, Optional[str], Optional[Dict]]:
        """
        Забронировать слот и пересчитать расписание.

        Возвращает:
            (True, booked_time, changes_dict)  — при успехе
            (False, error_msg, None)            — при ошибке
        """
        target = self.find_slot_by_id(slot_id)
        if not target:
            return False, "Слот не найден", None
        if target.is_booked:
            return False, "К сожалению, этот слот только что заняли. Выберите другое время.", None

        # Бизнес-правило: максимум 3 сессии подряд (4-я подряд — запрещена).
        # "Подряд" = разрыв между началами двух соседних сессий <= MIN_GAP (70 мин).
        # Ищем четвёрку: если ВСЕ 3 разрыва <= MIN_GAP — 4-я запись создаёт цепочку из 4.
        # Для администратора проверка не применяется.
        # Примеры:
        #   10:00, 11:10, 12:20, 13:30 → разрывы 70,70,70 → ЗАПРЕЩЕНО (4-я в ряд)
        #   10:00, 11:10, 12:20         → разрывы 70,70    → РАЗРЕШЕНО  (3-я в ряд, ОК)
        #   10:00, 11:10, 13:00, 14:10 → разрыв 110 в середине → разорвана → РАЗРЕШЕНО

        if not is_admin:
            same_day_booked = sorted(
                [s for s in self.slots if s.is_booked and s.date == target.date],
                key=lambda s: s.base_time
            )

            # Обновляем current_time целевого слота до актуального расчётного значения.
            # Баг 5: ранее использовался base_time, что давало неверные разрывы.
            # Для занятых слотов current_time = подтверждённое adjusted_time из БД.
            # Для свободного target пересчитываем свежую позицию через SmartScheduler.
            fresh_positions = self.calculate_slot_positions()
            if target.id in fresh_positions:
                target.current_time = fresh_positions[target.id]

            # Строим гипотетический список: занятые + новый слот
            hypothetical = sorted(
                same_day_booked + [target],
                key=lambda s: s.base_time
            )
            hyp_times = [datetime.strptime(s.current_time, "%H:%M") for s in hypothetical]

            # Ищем четвёрку где ВСЕ 3 разрыва <= MIN_GAP
            for i in range(len(hyp_times) - 3):
                gap1 = (hyp_times[i+1] - hyp_times[i]).total_seconds() / 60
                gap2 = (hyp_times[i+2] - hyp_times[i+1]).total_seconds() / 60
                gap3 = (hyp_times[i+3] - hyp_times[i+2]).total_seconds() / 60
                if gap1 <= MIN_GAP and gap2 <= MIN_GAP and gap3 <= MIN_GAP:
                    slots_in_chain = {
                        hypothetical[i].base_time,
                        hypothetical[i+1].base_time,
                        hypothetical[i+2].base_time,
                        hypothetical[i+3].base_time,
                    }
                    if target.base_time in slots_in_chain:
                        return False, (
                            "К сожалению, этот слот уже занят или недоступен. Выберите другое время — нажмите «✍🏻 Запись» в главном меню."
                        ), None

        # Запоминаем время, которое видел пользователь при выборе (баг 9: ранее возвращался base_time)
        confirmed_time = target.current_time

        # Помечаем как занятый
        target.is_booked = True
        target.booked_by = user_id

        # Пересчитываем соседние слоты
        new_times = self.calculate_slot_positions()
        changes: Dict[int, Dict[str, str]] = {}

        for slot in self.slots:
            if slot.id in new_times:
                old = slot.current_time
                new = new_times[slot.id]
                if old != new:
                    changes[slot.id] = {"old": old, "new": new}
                slot.current_time = new

        return True, confirmed_time, changes

    def cancel_booking(
        self, slot_id: int
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Отменить бронирование и пересчитать расписание.

        Возвращает:
            (True, changes_dict)  — при успехе
            (False, None)         — если слот не найден или не занят
        """
        target = self.find_slot_by_id(slot_id)
        if not target or not target.is_booked:
            return False, None

        # Освобождаем
        target.is_booked = False
        target.booked_by = None
        target.current_time = target.base_time  # возвращаем базовое время

        # Пересчитываем
        new_times = self.calculate_slot_positions()
        changes: Dict[int, Dict[str, str]] = {}

        for slot in self.slots:
            if slot.id in new_times:
                old = slot.current_time
                new = new_times[slot.id]
                if old != new:
                    changes[slot.id] = {"old": old, "new": new}
                slot.current_time = new

        return True, changes
