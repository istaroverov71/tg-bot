# models.py - Модели данных
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List
from config import TIMEZONE, SESSION_DURATION, BUFFER_TIME

@dataclass
class TimeSlot:
    """Модель временного слота"""
    id: Optional[int]
    base_time: str  # Базовое время в формате "ЧЧ:ММ"
    adjusted_time: str  # Скорректированное время с учетом сдвигов
    day: str  # День недели
    date: str  # Дата в формате "ГГГГ-ММ-ДД"
    is_available: bool
    booked_by: Optional[int]
    week_start: str  # Начало недели для группировки
    
    def get_datetime(self) -> datetime:
        """Получить datetime объекта"""
        dt_str = f"{self.date} {self.adjusted_time}"
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
    
    def is_today(self) -> bool:
        """Проверка, является ли слот сегодняшним"""
        today = datetime.now(TIMEZONE).date()
        slot_date = datetime.strptime(self.date, "%Y-%m-%d").date()
        return slot_date == today
    
    def is_past(self) -> bool:
        """Проверка, прошел ли уже слот"""
        now = datetime.now(TIMEZONE)
        return self.get_datetime() < now

@dataclass
class Booking:
    """Модель бронирования"""
    id: Optional[int]
    user_id: int
    slot_id: int
    original_time: str
    adjusted_time: str
    booking_date: datetime
    status: str  # 'active', 'cancelled', 'completed'
    
class SlotAdjuster:
    """Класс для умного сдвига слотов"""
    
    def __init__(self, all_slots: List[TimeSlot]):
        self.slots = sorted(all_slots, key=lambda s: s.get_datetime())
    
    def find_adjacent_slots(self, target_slot: TimeSlot) -> dict:
        """Найти соседние слоты"""
        result = {
            'prev': None,
            'next': None,
            'prev_prev': None,
            'next_next': None
        }
        
        for i, slot in enumerate(self.slots):
            if slot.id == target_slot.id:
                if i > 0:
                    result['prev'] = self.slots[i-1]
                if i > 1:
                    result['prev_prev'] = self.slots[i-2]
                if i < len(self.slots) - 1:
                    result['next'] = self.slots[i+1]
                if i < len(self.slots) - 2:
                    result['next_next'] = self.slots[i+2]
                break
        
        return result
    
    def calculate_adjustment(self, target_slot: TimeSlot) -> Optional[str]:
        """
        Рассчитать необходимое смещение времени согласно алгоритму:
        
        Сценарий А (Сдвиг вперед):
        - Если выбран слот X, следующий слот (X+1) свободен -> X+1 сдвигается на X+1 + 10 минут
        
        Сценарий Б (Сдвиг назад):
        - Если выбран слот X, предыдущий слот (X-1) занят, следующий (X+1) занят, 
          а X-2 свободен -> X сдвигается на X - 10 минут
        """
        adjacent = self.find_adjacent_slots(target_slot)
        
        # Сценарий А: Сдвиг вперед
        if (adjacent['next'] and 
            adjacent['next'].is_available and 
            not target_slot.is_available):
            
            # Сдвигаем следующий слот на 10 минут вперед
            next_time = adjacent['next'].get_datetime()
            new_time = next_time + timedelta(minutes=BUFFER_TIME)
            return new_time.strftime("%H:%M")
        
        # Сценарий Б: Сдвиг назад (сложный случай)
        if (adjacent['prev'] and 
            not adjacent['prev'].is_available and  # предыдущий занят
            adjacent['next'] and 
            not adjacent['next'].is_available and  # следующий занят
            adjacent['prev_prev'] and 
            adjacent['prev_prev'].is_available):   # пред-предыдущий свободен
            
            # Сдвигаем текущий слот на 10 минут назад
            current_time = target_slot.get_datetime()
            new_time = current_time - timedelta(minutes=BUFFER_TIME)
            return new_time.strftime("%H:%M")
        
        return None
    
    def validate_slot_time(self, slot: TimeSlot) -> bool:
        """Проверить, не наступил ли слот"""
        if slot.is_today():
            now = datetime.now(TIMEZONE)
            slot_time = slot.get_datetime()
            # Нельзя записаться на слот, который уже начался или начнется менее чем через час
            return slot_time > now + timedelta(hours=1)
        return not slot.is_past()
    
    def get_available_slots_for_display(self) -> List[TimeSlot]:
        """Получить слоты для отображения (с учетом запрета на сегодня)"""
        available = []
        for slot in self.slots:
            if slot.is_available and self.validate_slot_time(slot):
                available.append(slot)
        return available
