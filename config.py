# config.py — ИСПРАВЛЕННАЯ ВЕРСИЯ
#
# Исправления:
#   1. REMINDER_ADMIN: добавлены плейсхолдеры {name} и {username},
#      которые используются в bot.py при форматировании.
#   2. REMINDER_USER: уточнён комментарий — шаблон использует {slot},
#      а не {day}/{time}.

import os
from dotenv import load_dotenv
import pytz

load_dotenv()

# Telegram Bot Token
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Timezone
TIMEZONE = pytz.timezone('Europe/Moscow')

# Database file
DATABASE_FILE = 'bookings.db'

# Session duration in minutes
SESSION_DURATION = 60   # длительность сессии
BUFFER_TIME = 10        # буфер между сессиями

# Default time slots
DEFAULT_SLOTS = [
    "09:00", "10:00", "11:00", "12:00", "13:00",
    "14:00", "15:00", "16:00", "17:00", "18:00", "19:00"
]

# Admin IDs
ADMIN_IDS = [245217088]  # Замените на свой Telegram ID

# ======== Сообщения ========

WELCOME_MESSAGE = """
🌿 Добро пожаловать в бот психологической поддержки!

Я — практикующий психолог, и здесь вы можете записаться на консультацию.

Выберите действие:
"""

BOOKING_START = "🗓 Выберите день для записи:"

SLOT_SELECTION = "🕐 Выберите удобное время:"

BOOKING_CONFIRMED = """
✅ Запись подтверждена!

Ваше время: {slot}
Длительность: 60 минут

За 15 минут до начала я пришлю напоминание.

До встречи! 🌸
"""

ALREADY_BOOKED = """
🌸 У вас уже есть активная запись на этой неделе: {slot}

Вы можете изменить или отменить запись в разделе "Мои записи".
"""

NO_SLOTS_MESSAGE = """
🌱 На текущую неделю все слоты заняты.

Пожалуйста, загляните на следующей неделе или свяжитесь со мной напрямую.
"""

MY_BOOKINGS_HEADER = "📋 Ваши записи:\n\n"

NO_BOOKINGS = "У вас нет активных записей."

BOOKING_CANCELLED = """
✅ Запись отменена: {slot}

Если хотите записаться на другое время, нажмите "Запись"
"""

BOOKING_CHANGE_PROMPT = "Выберите новое время для записи:"

# ИСПРАВЛЕНО: используем {slot} — передаётся как "День в ЧЧ:ММ"
REMINDER_USER = """
🔔 Напоминание о сессии!

Ваша сессия начнётся через 15 минут: {slot}

Пожалуйста, подготовьте спокойное место и будьте на связи.
"""

# ИСПРАВЛЕНО: добавлены {name}, {username}, {slot}
REMINDER_ADMIN = """
🔔 Напоминание администратору:

Через 15 минут сессия с {name} (@{username})
Время: {slot}
"""

ERROR_MESSAGE = """
😔 Произошла небольшая ошибка. Пожалуйста, попробуйте ещё раз.
"""

# Кнопки главного меню
BUTTON_BOOK = "✍🏻 Запись"
BUTTON_MY_BOOKINGS = "📋 Мои записи"
BUTTON_CANCEL = "❌ Отменить запись"
BUTTON_BACK = "🔙 Назад"
