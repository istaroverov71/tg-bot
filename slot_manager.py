# slot_manager.py
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from models import TimeSlot, SlotAdjuster
from database import Database
from config import TIMEZONE, DEFAULT_SLOTS

class SlotManager:
    """Менеджер для работы со слотами и умным расписанием"""
    
    def __init__(self, db: Database):
        self.db = db
        self.current_week_start = self._get_week_start()
        
    def _get_week_start(self) -> str:
        """Получить дату начала текущей недели (понедельник)"""
        today = datetime.now(TIMEZONE).date()
        # Вычисляем понедельник текущей недели
        week_start = today - timedelta(days=today.weekday())
        return week_start.strftime("%Y-%m-%d")
    
    def _get_week_days(self) -> List[str]:
        """Получить список дней текущей недели"""
        days = []
        week_start = datetime.strptime(self.current_week_start, "%Y-%m-%d")
        for i in range(7):  # 7 дней недели
            day = week_start + timedelta(days=i)
            # Оставляем только будние дни (пн-пт) или все дни - по желанию
            if day.weekday() < 5:  # 0-4: пн-пт
                days.append(day.strftime("%Y-%m-%d"))
        return days
    
    def initialize_week_slots(self) -> bool:
        """
        Инициализировать слоты на текущую неделю, если их еще нет
        """
        # Проверяем, есть ли уже слоты на эту неделю
        existing_slots = self.db.get_week_slots(self.current_week_start)
        
        if existing_slots:
            return False  # Слоты уже существуют
        
        # Создаем новые слоты на неделю
        days_map = {
            "Понедельник": 0,
            "Вторник": 1,
            "Среда": 2,
            "Четверг": 3,
            "Пятница": 4
        }
        
        week_start_date = datetime.strptime(self.current_week_start, "%Y-%m-%d")
        
        for day_name, day_offset in days_map.items():
            current_date = week_start_date + timedelta(days=day_offset)
            date_str = current_date.strftime("%Y-%m-%d")
            
            for base_time in DEFAULT_SLOTS:
                self.db.add_time_slot(
                    base_time=base_time,
                    adjusted_time=base_time,
                    day=day_name,
                    date=date_str,
                    week_start=self.current_week_start,
                    is_available=True
                )
        
        return True
    
    def initialize_new_week(self) -> tuple:
        """
        Публичный метод для вызова из бота (обертка для initialize_week_slots)
        
        Returns:
            tuple: (success, message) - (bool, str)
        """
        try:
            # Обновляем начало недели на случай, если неделя сменилась
            self.current_week_start = self._get_week_start()
            
            success = self.initialize_week_slots()
            
            if success:
                return True, f"✅ Слоты на неделю {self.current_week_start} успешно созданы"
            else:
                return False, f"ℹ️ Слоты на неделю {self.current_week_start} уже существуют"
                
        except Exception as e:
            return False, f"❌ Ошибка при создании слотов: {str(e)}"
    
    def get_available_slots(self) -> List[TimeSlot]:
        """
        Получить все доступные слоты на текущую неделю
        с учетом запрета на сегодняшние слоты
        """
        # Убеждаемся, что слоты существуют
        self.initialize_week_slots()
        
        # Получаем все слоты на неделю
        all_slots = self.db.get_week_slots(self.current_week_start)
        
        # Фильтруем доступные слоты
        available_slots = []
        today = datetime.now(TIMEZONE).date()
        now = datetime.now(TIMEZONE)
        
        for slot in all_slots:
            if not slot.is_available:
                continue
                
            slot_date = datetime.strptime(slot.date, "%Y-%m-%d").date()
            
            # Проверяем запрет на "день в день"
            if slot_date == today:
                # Не показываем сегодняшние слоты, которые уже прошли
                # или наступят менее чем через 2 часа
                slot_time = datetime.strptime(f"{slot.date} {slot.adjusted_time}", "%Y-%m-%d %H:%M")
                slot_time = slot_time.replace(tzinfo=TIMEZONE)
                
                # Даем минимум 2 часа на подготовку
                if slot_time <= now + timedelta(hours=2):
                    continue
            
            available_slots.append(slot)
        
        return available_slots
    
    def check_user_week_booking(self, user_id: int) -> Optional[Dict]:
        """
        Проверить, есть ли у пользователя запись на этой неделе
        """
        return self.db.get_user_active_booking(user_id)
    
    def calculate_slot_adjustments(self, target_slot: TimeSlot) -> Dict[str, Optional[str]]:
        """
        Рассчитать все необходимые смещения слотов
        Возвращает словарь со смещениями для каждого слота
        """
        all_slots = self.db.get_week_slots(self.current_week_start)
        adjuster = SlotAdjuster(all_slots)
        
        adjustments = {}
        
        # Находим соседние слоты
        adjacent = adjuster.find_adjacent_slots(target_slot)
        
        # Проверяем сценарий А (сдвиг вперед)
        if (adjacent['next'] and 
            adjacent['next'].is_available and 
            not target_slot.is_available):
            
            # Сдвигаем следующий слот на 10 минут вперед
            next_time = adjacent['next'].get_datetime()
            new_time = next_time + timedelta(minutes=10)
            adjustments[adjacent['next'].id] = new_time.strftime("%H:%M")
        
        # Проверяем сценарий Б (сдвиг назад)
        if (adjacent['prev'] and 
            not adjacent['prev'].is_available and
            adjacent['next'] and 
            not adjacent['next'].is_available and
            adjacent['prev_prev'] and 
            adjacent['prev_prev'].is_available):
            
            # Сдвигаем текущий слот на 10 минут назад
            current_time = target_slot.get_datetime()
            new_time = current_time - timedelta(minutes=10)
            adjustments[target_slot.id] = new_time.strftime("%H:%M")
        
        return adjustments
    
    def get_slots_by_day(self, day: str) -> List[TimeSlot]:
        """
        Получить слоты для конкретного дня
        """
        all_slots = self.get_available_slots()
        return [slot for slot in all_slots if slot.day == day]
    
    def format_slots_for_display(self, slots: List[TimeSlot]) -> str:
        """
        Отформатировать слоты для отображения пользователю
        """
        if not slots:
            return "Нет доступных слотов"
        
        # Группируем по дням
        slots_by_day = {}
        for slot in slots:
            if slot.day not in slots_by_day:
                slots_by_day[slot.day] = []
            slots_by_day[slot.day].append(slot)
        
        # Форматируем вывод
        result = ""
        for day, day_slots in slots_by_day.items():
            result += f"\n📅 {day}:\n"
            times = [f"  {slot.adjusted_time}" for slot in day_slots]
            result += "\n".join(times)
        
        return result
    
    def reset_week_slots(self) -> None:
        """
        Сбросить все слоты недели к базовому времени
        (используется при отмене записи)
        """
        all_slots = self.db.get_week_slots(self.current_week_start)
        
        for slot in all_slots:
            if slot.is_available:
                # Возвращаем базовое время для свободных слотов
                self.db.update_slot_time(slot.id, slot.base_time)
    
    def get_next_available_slot(self, preferred_time: Optional[str] = None) -> Optional[TimeSlot]:
        """
        Найти следующий доступный слот
        (для предложения альтернатив, если выбранное время занято)
        """
        available_slots = self.get_available_slots()
        
        if not available_slots:
            return None
        
        # Сортируем по дате и времени
        available_slots.sort(key=lambda s: s.get_datetime())
        
        now = datetime.now(TIMEZONE)
        
        for slot in available_slots:
            slot_time = slot.get_datetime()
            if slot_time > now:
                return slot
        
        return None
