# keyboards.py - ПОЛНАЯ ИСПРАВЛЕННАЯ ВЕРСИЯ
from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Optional
from models import TimeSlot

class Keyboards:
    """Класс для создания клавиатур"""
    
    @staticmethod
    def get_main_keyboard():
        """Главная клавиатура с основными действиями"""
        keyboard = [
            [KeyboardButton("✍🏻 Запись")],
            [KeyboardButton("📋 Мои записи")],
            [KeyboardButton("❌ Отменить запись")]
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    @staticmethod
    def get_days_keyboard(days):
        """
        Клавиатура с днями недели.
        days — список строк (тогда label == callback) или
               список кортежей (label, day_name) для разных текста и значения callback.
        """
        keyboard = []
        # Нормализуем к кортежам (label, callback_value)
        items = []
        for d in days:
            if isinstance(d, tuple):
                items.append(d)
            else:
                items.append((d, d))

        # Сортируем по дате (label содержит дату вида "ПТ 13.03")
        # callback_value — полное название дня для фильтрации слотов
        for i in range(0, len(items), 2):
            row = []
            label, cb = items[i]
            row.append(InlineKeyboardButton(label, callback_data=f"day_{cb}"))
            if i + 1 < len(items):
                label2, cb2 = items[i + 1]
                row.append(InlineKeyboardButton(label2, callback_data=f"day_{cb2}"))
            keyboard.append(row)

        keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_main")])
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_slots_keyboard(slots: List[TimeSlot], day: str):
        """Клавиатура с временными слотами для конкретного дня"""
        keyboard = []
        
        # Фильтруем слоты для выбранного дня
        day_slots = [s for s in slots if s.day == day]
        
        if not day_slots:
            # Если нет слотов, показываем только кнопку назад
            keyboard.append([InlineKeyboardButton("🔙 Назад к дням", callback_data="back_to_days")])
            keyboard.append([InlineKeyboardButton("💌 Главное меню", callback_data="back_to_main")])
            return InlineKeyboardMarkup(keyboard)
        
        # Сортируем слоты по времени
        day_slots.sort(key=lambda x: x.current_time if hasattr(x, 'current_time') else x.adjusted_time)
        
        # Группируем слоты по 2 в ряд
        for i in range(0, len(day_slots), 2):
            row = []
            # Первый слот в ряду
            slot = day_slots[i]
            display_time = slot.current_time if hasattr(slot, 'current_time') else slot.adjusted_time
            button_text = f"{display_time}"
            row.append(InlineKeyboardButton(
                button_text, 
                callback_data=f"slot_{slot.id}"
            ))
            
            # Второй слот в ряду, если есть
            if i + 1 < len(day_slots):
                slot = day_slots[i + 1]
                display_time = slot.current_time if hasattr(slot, 'current_time') else slot.adjusted_time
                button_text = f"{display_time}"
                row.append(InlineKeyboardButton(
                    button_text, 
                    callback_data=f"slot_{slot.id}"
                ))
            
            keyboard.append(row)
        
        # Кнопка "Назад" к выбору дня
        keyboard.append([InlineKeyboardButton("🔙 Назад к дням", callback_data="back_to_days")])
        keyboard.append([InlineKeyboardButton("💌 Главное меню", callback_data="back_to_main")])
        
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_booking_confirmation_keyboard(slot_id: int):
        """Клавиатура предварительного подтверждения записи"""
        keyboard = [
            [
                InlineKeyboardButton("✔️ Продолжить", callback_data=f"confirm_{slot_id}"),
                InlineKeyboardButton("❌ Отмена", callback_data="back_to_slots")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_final_confirmation_keyboard(slot_id: int):
        """
        Клавиатура для финального подтверждения записи
        (ОТСУТСТВОВАЛ В ВАШЕЙ ВЕРСИИ)
        """
        keyboard = [
            [
                InlineKeyboardButton("✔️ Да, записаться", callback_data=f"final_confirm_{slot_id}"),
                InlineKeyboardButton("❌ Отмена", callback_data="back_to_days")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_cancel_confirmation_keyboard(booking_id: int):
        """Клавиатура подтверждения отмены записи"""
        keyboard = [
            [
                InlineKeyboardButton("✔️ Да, отменить", callback_data=f"cancel_confirm_{booking_id}"),
                InlineKeyboardButton("❌ Нет, оставить", callback_data="back_to_main")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_booking_actions_keyboard(booking_id: int):
        """
        Клавиатура действий с существующей записью
        (НОВЫЙ МЕТОД)
        """
        keyboard = [
            [InlineKeyboardButton("📝 Изменить время", callback_data="change_booking")],
            [InlineKeyboardButton("❌ Отменить запись", callback_data=f"cancel_{booking_id}")],
            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_main")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_admin_force_delete_keyboard(date_str: str):
        """
        Клавиатура для подтверждения принудительного удаления дня
        (НОВЫЙ МЕТОД)
        """
        keyboard = [
            [InlineKeyboardButton("⚠️ ДА, УДАЛИТЬ ВСЁ", callback_data=f"force_delete_day_{date_str}")],
            [InlineKeyboardButton("❌ Нет, отмена", callback_data="cancel_delete")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_back_keyboard(target: str = "main"):
        """
        Простая клавиатура с кнопкой назад
        (НОВЫЙ МЕТОД)
        """
        if target == "days":
            callback = "back_to_days"
        elif target == "slots":
            callback = "back_to_slots"
        else:
            callback = "back_to_main"
        
        keyboard = [
            [InlineKeyboardButton("🔙 Назад", callback_data=callback)]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_empty_state_keyboard():
        """
        Клавиатура для пустого состояния (нет слотов)
        (НОВЫЙ МЕТОД)
        """
        keyboard = [
            [InlineKeyboardButton("💌 В главное меню", callback_data="back_to_main")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_admin_quick_actions_keyboard():
        """
        Быстрые действия для администратора
        (НОВЫЙ МЕТОД)
        """
        keyboard = [
            [InlineKeyboardButton("📅 Просмотр слотов", callback_data="admin_view_slots")],
            [InlineKeyboardButton("📋 Все записи", callback_data="admin_view_bookings")],
            [InlineKeyboardButton("🗑 Удалить день", callback_data="admin_delete_day")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def get_yes_no_keyboard(yes_callback: str, no_callback: str = "back_to_main"):
        """
        Универсальная клавиатура Да/Нет
        (НОВЫЙ МЕТОД)
        """
        keyboard = [
            [
                InlineKeyboardButton("✔️ Да", callback_data=yes_callback),
                InlineKeyboardButton("❌ Нет", callback_data=no_callback)
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
