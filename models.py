# models.py - Модели данных
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from config import TIMEZONE

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
