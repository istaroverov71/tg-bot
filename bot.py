# bot.py — ИСПРАВЛЕННАЯ ВЕРСИЯ
#
# Исправления:
#   1. send_reminders: APScheduler передаёт `context` через JobQueue PTB,
#      но в коде использовался args=[application.bot]. Исправлено на
#      application.job_queue.run_repeating(), что передаёт правильный context.
#   2. REMINDER_USER форматирование: в config.py шаблон использует {slot},
#      а вызов передавал day= и time= — несоответствие ключей → KeyError.
#      Исправлено: передаём slot=f"{day} в {time}".
#   3. В process_cancellation убран голый `except: pass` — теперь ошибки
#      уведомления логируются.
#   4. show_booking_days теперь корректно работает при вызове из callback
#      (query вместо update).

import logging
from datetime import datetime, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import (
    BOT_TOKEN, ADMIN_IDS, TIMEZONE,
    WELCOME_MESSAGE, BOOKING_START, BOOKING_CONFIRMED,
    ALREADY_BOOKED, NO_SLOTS_MESSAGE, MY_BOOKINGS_HEADER,
    NO_BOOKINGS, BOOKING_CANCELLED, BOOKING_CHANGE_PROMPT,
    REMINDER_USER, REMINDER_ADMIN, ERROR_MESSAGE,
    BUTTON_BOOK, BUTTON_MY_BOOKINGS, BUTTON_CANCEL,
)
from database import Database
from keyboards import Keyboards
from slot_manager import SlotManager
from slot_scheduler import SmartScheduler, Slot

# Аббревиатуры дней для отображения пользователю
_DAY_ABBR = {
    "Понедельник": "Пн", "Вторник": "Вт", "Среда": "Ср",
    "Четверг": "Чт ", "Пятница": "Пт ", "Суббота": "Сб ", "Воскресенье": "Вск ",
}

def _day_label(day_name: str, date_str: str) -> str:
    """Формирует метку вида 'Ср (11.03)' — единый формат по всему боту."""
    abbr = _DAY_ABBR.get(day_name, day_name[:2])
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{abbr}({d.strftime('%d.%m')})"
    except Exception:
        return abbr



logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

db = Database()
slot_manager = SlotManager(db)


# ========== ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    await update.message.reply_text(
        WELCOME_MESSAGE,
        reply_markup=Keyboards.get_main_keyboard(),
    )


async def show_my_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    is_admin = user.id in ADMIN_IDS
    message = (
        f"👤 **Ваша информация:**\n"
        f"• ID: `{user.id}`\n"
        f"• Имя: {user.first_name}\n"
        f"• Username: @{user.username or 'не указан'}\n"
        f"• Админ: {'✅ ДА' if is_admin else '❌ НЕТ'}\n\n"
        f"**Текущие админы в config.py:**\n"
    )
    for admin_id in (ADMIN_IDS or []):
        message += f"• `{admin_id}`\n"
    if not ADMIN_IDS:
        message += "• Список пуст\n"
    message += "\nЧтобы стать админом, добавьте свой ID в `ADMIN_IDS` в config.py"
    await update.message.reply_text(message, parse_mode='Markdown')


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == BUTTON_BOOK:
        await show_booking_days(update, context)
    elif text == BUTTON_MY_BOOKINGS:
        await show_my_bookings(update, context)
    elif text == BUTTON_CANCEL:
        await cancel_booking_start(update, context)
    else:
        await update.message.reply_text(
            "Пожалуйста, используйте кнопки меню.",
            reply_markup=Keyboards.get_main_keyboard(),
        )


async def show_booking_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать доступные дни. Принимает Update или CallbackQuery напрямую."""
    # Определяем тип объекта: Update, CallbackQuery или просто query-объект
    from telegram import Update as _Update
    from telegram import CallbackQuery as _CallbackQuery
    if isinstance(update, _Update):
        if update.callback_query:
            user_id = update.callback_query.from_user.id
            reply = update.callback_query.message.reply_text
        else:
            user_id = update.effective_user.id
            reply = update.message.reply_text
    elif isinstance(update, _CallbackQuery):
        # Передан сам CallbackQuery (из process_change_booking)
        user_id = update.from_user.id
        reply = update.message.reply_text
    else:
        # Fallback: пробуем как query-объект
        user_id = update.from_user.id
        reply = update.message.reply_text

    is_admin = user_id in ADMIN_IDS

    # Обычный пользователь: только одна запись в неделю
    if not is_admin:
        existing = db.get_user_active_booking(user_id)
        if existing:
            await reply(
                ALREADY_BOOKED.format(slot=f"{existing['day']} {existing['time']}"),
                reply_markup=Keyboards.get_main_keyboard(),
            )
            return

    week_start = slot_manager.current_week_start
    all_slots = db.get_all_slots_for_scheduling(week_start)

    if not all_slots:
        await reply(NO_SLOTS_MESSAGE, reply_markup=Keyboards.get_main_keyboard())
        return

    smart = SmartScheduler(all_slots)
    visible_slots = smart.get_visible_slots()

    # Пункт 4: скрываем прошедшие слоты и слоты ближайших 3 часов
    now_tz = datetime.now(TIMEZONE)
    cutoff = now_tz + timedelta(hours=3)
    visible_slots = [
        s for s in visible_slots
        if datetime.strptime(f"{s.date} {s.current_time}", "%Y-%m-%d %H:%M")
               .replace(tzinfo=TIMEZONE) >= cutoff
    ]

    if not visible_slots:
        await reply(NO_SLOTS_MESSAGE, reply_markup=Keyboards.get_main_keyboard())
        return

    # Строим метки вида "ПТ 13.03" — берём дату из первого слота каждого дня
    day_date_map = {}
    for s in visible_slots:
        if s.day not in day_date_map:
            day_date_map[s.day] = s.date
    # Сортируем по дате
    sorted_days = sorted(day_date_map.keys(), key=lambda d: day_date_map[d])
    days_for_kb = [(_day_label(d, day_date_map[d]), d) for d in sorted_days]

    context.user_data['visible_slots'] = visible_slots
    context.user_data['all_slots'] = all_slots

    await reply(BOOKING_START, reply_markup=Keyboards.get_days_keyboard(days_for_kb))


async def show_my_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    booking = db.get_user_active_booking(user_id)

    if booking:
        message = MY_BOOKINGS_HEADER
        message += f"📅 {_day_label(booking['day'], booking['date'])} в {booking['time']}\n"
        message += "\nВы можете изменить или отменить запись."
        keyboard = [
            [InlineKeyboardButton("✏️ Изменить время", callback_data="change_booking")],
            [InlineKeyboardButton("❌ Отменить запись",
                                  callback_data=f"cancel_{booking['booking_id']}")],
        ]
        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    else:
        await update.message.reply_text(
            NO_BOOKINGS,
            reply_markup=Keyboards.get_main_keyboard(),
        )


async def cancel_booking_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    booking = db.get_user_active_booking(user_id)
    if booking:
        # Пункт 5: запрет отмены менее чем за 3 часа (кроме админа)
        if user_id not in ADMIN_IDS:
            session_dt = datetime.strptime(
                f"{booking['date']} {booking['time']}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=TIMEZONE)
            if session_dt - datetime.now(TIMEZONE) < timedelta(hours=3):
                await update.message.reply_text(
                    f"❌ Отмена невозможна.\n\n"
                    f"Запись на {booking['day']} в {booking['time']} нельзя отменить "
                    f"менее чем за 3 часа до начала.\n\n"
                    f"Для отмены свяжитесь напрямую.",
                    reply_markup=Keyboards.get_main_keyboard(),
                )
                return
        await update.message.reply_text(
            f"Вы уверены, что хотите отменить запись на {booking['day']} в {booking['time']}?",
            reply_markup=Keyboards.get_cancel_confirmation_keyboard(booking['booking_id']),
        )
    else:
        await update.message.reply_text(
            "У вас нет активных записей.",
            reply_markup=Keyboards.get_main_keyboard(),
        )


# ========== ОБРАБОТЧИК CALLBACK-КНОПОК ==========

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    try:
        if data == "back_to_main":
            await query.edit_message_text(WELCOME_MESSAGE, reply_markup=None)
            await query.message.reply_text(
                "Главное меню:", reply_markup=Keyboards.get_main_keyboard()
            )

        elif data == "back_to_days":
            visible_slots = context.user_data.get('visible_slots', [])
            day_date_map = {}
            for s in visible_slots:
                if s.day not in day_date_map:
                    day_date_map[s.day] = s.date
            sorted_days = sorted(day_date_map.keys(), key=lambda d: day_date_map[d])
            days_for_kb = [(_day_label(d, day_date_map[d]), d) for d in sorted_days]
            await query.edit_message_text(
                BOOKING_START, reply_markup=Keyboards.get_days_keyboard(days_for_kb)
            )

        elif data.startswith("day_"):
            day = data[4:]
            visible_slots = context.user_data.get('visible_slots', [])
            await show_slots_for_day(query, context, day, visible_slots)

        elif data.startswith("slot_"):
            slot_id = int(data[5:])
            await show_booking_confirmation(query, context, slot_id)

        elif data.startswith("confirm_"):
            slot_id = int(data[8:])
            await process_booking(query, context, slot_id)

        elif data.startswith("final_confirm_"):
            slot_id = int(data[14:])
            await process_final_booking(query, context, slot_id)

        elif data.startswith("cancel_confirm_"):
            booking_id = int(data[15:])
            await process_cancellation(query, context, booking_id)

        elif data.startswith("cancel_"):
            # Кнопка "Отменить запись" из show_my_bookings
            booking_id = int(data[7:])
            await process_cancellation(query, context, booking_id)

        elif data.startswith("force_delete_day_"):
            date_str = data[17:]
            await force_delete_day(query, context, date_str)

        elif data == "cancel_delete":
            await query.edit_message_text("❌ Удаление отменено.")

        elif data == "change_booking":
            await process_change_booking(query, context)

    except Exception as e:
        logger.error(f"Error in callback_handler (data={data!r}): {e}", exc_info=True)
        await query.edit_message_text(ERROR_MESSAGE)


async def show_slots_for_day(query, context, day: str, visible_slots):
    day_slots = [s for s in visible_slots if s.day == day]

    if not day_slots:
        day_date_map = {}
        for s in visible_slots:
            if s.day not in day_date_map:
                day_date_map[s.day] = s.date
        sorted_days = sorted(day_date_map.keys(), key=lambda d: day_date_map[d])
        days_for_kb = [(_day_label(d, day_date_map[d]), d) for d in sorted_days]
        await query.edit_message_text(
            f"📅 {day}\n\n❌ На этот день нет свободных слотов.",
            reply_markup=Keyboards.get_days_keyboard(days_for_kb),
        )
        return

    day_slots.sort(key=lambda s: s.current_time)

    keyboard = [
        [InlineKeyboardButton(s.current_time, callback_data=f"slot_{s.id}")]
        for s in day_slots
    ]
    keyboard.append([InlineKeyboardButton("🔙 Назад к дням", callback_data="back_to_days")])

    await query.edit_message_text(
        f"📅 {day}\n\nДоступное время:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def show_booking_confirmation(query, context, slot_id: int):
    all_slots = context.user_data.get('all_slots', [])
    target_slot = next((s for s in all_slots if s.id == slot_id), None)

    if not target_slot:
        await query.edit_message_text("❌ Слот не найден")
        return

    await query.edit_message_text(
        f"📝 **Подтверждение записи**\n\n"
        f"Вы выбрали:\n"
        f"📅 {_day_label(target_slot.day, target_slot.date)}\n"
        f"⏰ {target_slot.current_time}\n\n"
        f"Сессия продлится 60 минут.\n\n"
        f"Подтверждаете запись?",
        parse_mode='Markdown',
        reply_markup=Keyboards.get_final_confirmation_keyboard(slot_id),
    )
    context.user_data['selected_slot'] = target_slot


async def process_booking(query, context, slot_id: int):
    """Промежуточный шаг — проверить запись и показать подтверждение."""
    user_id = query.from_user.id
    is_admin = user_id in ADMIN_IDS
    if not is_admin:
        existing = db.get_user_active_booking(user_id)
        if existing:
            await query.edit_message_text(
                ALREADY_BOOKED.format(slot=f"{existing['day']} {existing['time']}")
            )
            return
    await show_booking_confirmation(query, context, slot_id)


async def process_final_booking(query, context, slot_id: int):
    """Финальная запись с умным планировщиком и уведомлением админу."""
    user_id = query.from_user.id
    week_start = slot_manager.current_week_start
    is_admin = user_id in ADMIN_IDS

    # Обычный пользователь — проверяем ограничение одна запись в неделю
    if not is_admin:
        existing = db.get_user_active_booking(user_id)
        if existing:
            await query.edit_message_text(
                ALREADY_BOOKED.format(slot=f"{existing['day']} {existing['time']}")
            )
            return

    success, message, slot_info = db.book_slot_with_scheduler(user_id, slot_id, week_start)

    if success:
        _dl = _day_label(slot_info['day'], slot_info['date'])

        await query.edit_message_text(
            f"✅ **Запись подтверждена!**\n\n"
            f"📅 {_dl}\n"
            f"⏰ {slot_info['time']}\n\n"
            f"Сессия продлится 60 минут.\n"
            f"За 15 минут до начала я пришлю напоминание.\n\n"
            f"До встречи! 🌸",
            parse_mode='Markdown',
        )

        # Уведомление администратору — всегда, если записывается не сам админ
        if not is_admin:
            user = query.from_user
            first = user.first_name or ''
            last = user.last_name or ''
            uname = f"@{user.username}" if user.username else 'нет username'
            admin_message = (
                f"📝 Новая запись!\n\n"
                f"👤 Клиент: {first} {last}\n"
                f"📱 Username: {uname}\n"
                f"🆔 ID: {user.id}\n\n"
                f"📅 Слот: {_dl} в {slot_info['time']}"
            )
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(admin_id, admin_message)
                    logger.info(f"✅ Уведомление о записи отправлено админу {admin_id}")
                except Exception as e:
                    logger.error(f"❌ Не удалось уведомить админа {admin_id}: {e}", exc_info=True)

        # Чистим контекст
        context.user_data.pop('selected_slot', None)
        context.user_data.pop('visible_slots', None)
        context.user_data.pop('all_slots', None)
    else:
        await query.edit_message_text(f"❌ {message}")


async def process_cancellation(query, context, booking_id: int):
    user_id = query.from_user.id

    # Пункт 5: запрет отмены менее чем за 3 часа (кроме админа)
    if user_id not in ADMIN_IDS:
        booking_info = db.get_booking_by_id(booking_id)
        if booking_info:
            session_dt = datetime.strptime(
                f"{booking_info['date']} {booking_info['adjusted_time']}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=TIMEZONE)
            if session_dt - datetime.now(TIMEZONE) < timedelta(hours=3):
                await query.edit_message_text(
                    f"❌ Отмена невозможна.\n\n"
                    f"Запись на {booking_info['day']} в {booking_info['adjusted_time']} нельзя отменить "
                    f"менее чем за 3 часа до начала.\n\n"
                    f"Для отмены свяжитесь напрямую."
                )
                return

    if db.cancel_booking_with_scheduler(user_id, booking_id):
        await query.edit_message_text(
            "✅ **Запись успешно отменена.**\n\n"
            "Если хотите записаться снова, нажмите кнопку «Запись» в главном меню.",
            parse_mode='Markdown',
        )
        # Уведомление администратору
        for admin_id in ADMIN_IDS:
            try:
                user = query.from_user
                first = user.first_name or ''
                last = user.last_name or ''
                uname = f"@{user.username}" if user.username else 'нет username'
                cancel_msg = (
                    f"❌ Запись отменена\n\n"
                    f"👤 Клиент: {first} {last}\n"
                    f"📱 Username: {uname}\n"
                    f"🆔 ID: {user_id}"
                )
                await context.bot.send_message(admin_id, cancel_msg)
                logger.info(f"✅ Уведомление об отмене отправлено админу {admin_id}")
            except Exception as e:
                logger.error(f"❌ Не удалось уведомить админа {admin_id} об отмене: {e}", exc_info=True)
    else:
        await query.edit_message_text("❌ Не удалось отменить запись.")


async def process_change_booking(query, context):
    user_id = query.from_user.id
    booking = db.get_user_active_booking(user_id)

    if booking and db.cancel_booking_with_scheduler(user_id, booking['booking_id']):
        await query.edit_message_text(
            "✅ Текущая запись отменена.\n\nТеперь выберите новое время:"
        )
        await show_booking_days(query, context)
    else:
        await query.edit_message_text("❌ Не удалось изменить запись.")


async def force_delete_day(query, context, date_str: str):
    if db.delete_day_slots(date_str):
        await query.edit_message_text(
            f"✅ **День {date_str} полностью удалён.**\n\nВсе слоты и записи удалены.",
            parse_mode='Markdown',
        )
    else:
        await query.edit_message_text("❌ Ошибка при удалении.")


# ========== НАПОМИНАНИЯ ==========
# ИСПРАВЛЕНО: используем job_queue PTB вместо APScheduler с args=[bot],
# чтобы функция получала правильный context с context.bot.

async def send_reminders(context: ContextTypes.DEFAULT_TYPE):
    """
    Отправить напоминания о сессиях, которые начнутся через 15 минут.
    Вызывается через job_queue PTB — context.bot доступен автоматически.
    """
    logger.info("Checking reminders...")
    upcoming = db.get_upcoming_sessions(minutes_before=15)

    for session in upcoming:
        # ИСПРАВЛЕНО: REMINDER_USER использует {slot}, передаём правильный ключ
        slot_str = f"{session['day']} в {session['time']}"

        try:
            await context.bot.send_message(
                session['user_id'],
                REMINDER_USER.format(slot=slot_str),
            )
            logger.info(f"Reminder sent to user {session['user_id']}")
        except Exception as e:
            logger.error(f"Failed to send reminder to user {session['user_id']}: {e}")

        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    REMINDER_ADMIN.format(
                        name=session['name'],
                        username=session.get('username') or '—',
                        slot=slot_str,
                    ),
                )
            except Exception as e:
                logger.error(f"Failed to send reminder to admin {admin_id}: {e}")


# ========== АДМИН-КОМАНДЫ ==========

async def admin_update_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /update_week — установка слотов в формате:
      /update_week ПТ 16.05 10:00,11:00,14:00
    Несколько дней — каждый с новой строки в одном сообщении:
      /update_week ПТ 16.05 10:00,11:00
      СБ 17.05 12:00,15:00
    Поддерживаются все 7 дней: ПН ВТ СР ЧТ ПТ СБ ВСК
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    # Таблица аббревиатур → полное название
    DAY_ALIASES = {
        "пн": "Понедельник", "понедельник": "Понедельник",
        "вт": "Вторник",     "вторник": "Вторник",
        "ср": "Среда",       "среда": "Среда",
        "чт": "Четверг",     "четверг": "Четверг",
        "пт": "Пятница",     "пятница": "Пятница",
        "сб": "Суббота",     "суббота": "Суббота",
        "вск": "Воскресенье", "воскресенье": "Воскресенье",
    }

    if not context.args:
        await update.message.reply_text(
            "🕐 Ручная установка слотов\n\n"
            "Формат (один день):\n"
            "  /update_week ПТ 16.05 10:00,11:00,14:00\n\n"
            "Несколько дней — каждый с новой строки:\n"
            "  /update_week ПТ 16.05 10:00,11:00\n"
            "  СБ 17.05 12:00,15:00\n\n"
            "Дни: ПН ВТ СР ЧТ ПТ СБ ВСК\n"
            "Существующие записи сохраняются."
        )
        return

    # Разбираем текст сообщения построчно (Telegram разбивает переносы)
    raw_text = update.message.text or ""
    # Отрезаем команду /update_week и берём остаток
    first_arg_pos = raw_text.find(context.args[0])
    lines_raw = raw_text[first_arg_pos:].strip().splitlines()

    entries = []
    errors = []

    for line in lines_raw:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            errors.append(f"Строка '{line}': нужно ДД МД.ММ ЧЧ:ММ,...")
            continue

        day_key = parts[0].lower().rstrip(".")
        date_raw = parts[1]   # ДД.ММ
        times_raw = parts[2]  # ЧЧ:ММ,ЧЧ:ММ,...

        if day_key not in DAY_ALIASES:
            errors.append(f"Неизвестный день '{parts[0]}'. Используйте: ПН ВТ СР ЧТ ПТ СБ ВСК")
            continue
        day_name = DAY_ALIASES[day_key]

        try:
            year = datetime.now(TIMEZONE).year
            target_date_dt = datetime.strptime(f"{date_raw}.{year}", "%d.%m.%Y")
            target_date = target_date_dt.strftime("%Y-%m-%d")
        except ValueError:
            errors.append(f"Неверная дата '{date_raw}'. Формат: ДД.ММ (например 16.05)")
            continue

        # week_start = понедельник недели этой даты
        week_start = (target_date_dt - timedelta(days=target_date_dt.weekday())).strftime("%Y-%m-%d")

        valid_times = []
        time_ok = True
        for t in times_raw.split(","):
            t = t.strip()
            try:
                datetime.strptime(t, "%H:%M")
                valid_times.append(t)
            except ValueError:
                errors.append(f"Неверное время '{t}'. Формат: ЧЧ:ММ")
                time_ok = False
                break
        if not time_ok:
            continue

        entries.append({
            "day_name": day_name,
            "target_date": target_date,
            "week_start": week_start,
            "valid_times": valid_times,
        })

    if errors:
        await update.message.reply_text("❌ Ошибки:\n" + "\n".join(errors))
        if not entries:
            return

    if not entries:
        await update.message.reply_text("❌ Не найдено корректных строк с расписанием.")
        return

    # Обрабатываем каждый день
    summary = []
    notif_lines = []

    for entry in entries:
        day_name = entry["day_name"]
        target_date = entry["target_date"]
        week_start = entry["week_start"]
        valid_times = entry["valid_times"]

        result = db.update_day_slots_preserve_bookings(target_date, day_name, week_start, valid_times)

        label = _day_label(day_name, target_date)
        times_line = " / ".join(valid_times)
        preserved = ""
        if result["preserved_bookings"]:
            preserved = f"\n🔒 Сохранены записи: {', '.join(result['preserved_bookings'])}"
        summary.append(f"🖇️ {label}\n{times_line}{preserved}")

        slot_list = result["added_slots"] if result["added_slots"] else valid_times
        notif_lines.append((label, slot_list))
        logger.info(f"Admin {user_id} updated slots: {day_name} {target_date} {valid_times}")

    summary_text = "Расписание обновлено:\n\n" + "\n\n".join(summary)
    await update.message.reply_text(summary_text)

    # Рассылка пользователям без записи на текущую неделю
    users_to_notify = db.get_users_with_no_booking_this_week()
    users_to_notify = [u for u in users_to_notify if u["user_id"] not in ADMIN_IDS]
    logger.info(f"Рассылка о новых слотах: {len(users_to_notify)} пользователей")

    if users_to_notify:
        day_blocks = []
        for (_lbl, _times) in notif_lines:
            day_blocks.append(f"🖇️ {_lbl}\n{' / '.join(_times)}")
        notif_text = (
            "🗓 Новые слоты доступны!\n\n"
            + "\n\n".join(day_blocks)
            + "\n\nНажмите «✍🏻 Запись» чтобы забронировать время."
        )
        sent = 0
        for u in users_to_notify:
            try:
                await context.bot.send_message(u["user_id"], notif_text)
                sent += 1
            except Exception as e:
                logger.warning(f"Не удалось уведомить {u['user_id']}: {e}")
        await update.message.reply_text(
            f"📣 Уведомлено {sent} из {len(users_to_notify)} пользователей."
        )
    else:
        await update.message.reply_text(
            "ℹ️ Нет пользователей для уведомления (все уже записаны или ещё не использовали бот)."
        )


async def admin_view_slots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    week_start = slot_manager.current_week_start
    all_slots = db.get_all_slots_for_scheduling(week_start)

    if not all_slots:
        await update.message.reply_text("❌ Нет слотов на текущую неделю.")
        return

    smart = SmartScheduler(all_slots)
    visible_ids = {s.id for s in smart.get_visible_slots()}

    slots_by_day = {}
    for slot in all_slots:
        slots_by_day.setdefault(slot.day, []).append(slot)

    message = f"📋 **Слоты на неделю {week_start}**\n\n"
    # Все дни, которые есть в слотах, отсортированные по дате
    days_order = sorted(slots_by_day.keys(), key=lambda d: min(s.date for s in slots_by_day[d]))

    for day in days_order:
        if day not in slots_by_day:
            continue
        day_slots = sorted(slots_by_day[day], key=lambda s: s.base_time)
        booked = sum(1 for s in day_slots if s.is_booked)
        avail = len(day_slots) - booked
        message += f"📅 **{day}** ({avail} свободно, {booked} занято):\n"

        for slot in day_slots:
            if slot.is_booked:
                status = "❌"
                booking = db.get_booking_by_slot_id(slot.id)
                client = f" — {booking['user_name']}" if booking else ""
                message += f"  {status} {slot.base_time} → {slot.current_time}{client}\n"
            elif slot.id in visible_ids:
                status = "✅"
                message += f"  {status} {slot.base_time} → {slot.current_time}\n"
            else:
                message += f"  ⚠️ {slot.base_time} — недоступен\n"
        message += "\n"

    if len(message) > 4000:
        for chunk in [message[i:i+4000] for i in range(0, len(message), 4000)]:
            await update.message.reply_text(chunk, parse_mode='Markdown')
    else:
        await update.message.reply_text(message, parse_mode='Markdown')


async def admin_delete_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    if not context.args:
        await update.message.reply_text(
            "🗑 Использование: `/delete_day Понедельник`", parse_mode='Markdown'
        )
        return

    DAY_ALIASES_DEL = {
        "пн": "Понедельник", "понедельник": "Понедельник",
        "вт": "Вторник",     "вторник": "Вторник",
        "ср": "Среда",       "среда": "Среда",
        "чт": "Четверг",     "четверг": "Четверг",
        "пт": "Пятница",     "пятница": "Пятница",
        "сб": "Суббота",     "суббота": "Суббота",
        "вск": "Воскресенье", "воскресенье": "Воскресенье",
    }
    DAY_OFFSET = {
        "Понедельник": 0, "Вторник": 1, "Среда": 2, "Четверг": 3,
        "Пятница": 4, "Суббота": 5, "Воскресенье": 6,
    }
    day_key = context.args[0].lower().rstrip(".")
    if day_key not in DAY_ALIASES_DEL:
        await update.message.reply_text(
            "❌ Неизвестный день. Используйте: ПН ВТ СР ЧТ ПТ СБ ВСК"
        )
        return
    day = DAY_ALIASES_DEL[day_key]
    target_date = (
        datetime.strptime(slot_manager.current_week_start, "%Y-%m-%d")
        + timedelta(days=DAY_OFFSET[day])
    ).strftime("%Y-%m-%d")

    all_slots = db.get_all_slots_for_scheduling(slot_manager.current_week_start)
    day_slots = [s for s in all_slots if s.date == target_date]

    if not day_slots:
        await update.message.reply_text(f"❌ На {day} ({target_date}) нет слотов.")
        return

    has_bookings = any(s.is_booked for s in day_slots)
    if has_bookings:
        booked_clients = []
        for slot in day_slots:
            if slot.is_booked:
                booking = db.get_booking_by_slot_id(slot.id)
                if booking:
                    booked_clients.append(f"  • {booking['user_name']} в {slot.current_time}")
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"force_delete_day_{target_date}")],
            [InlineKeyboardButton("❌ Нет, отмена", callback_data="cancel_delete")],
        ]
        await update.message.reply_text(
            f"⚠️ **Внимание!**\n\nНа {day} есть записи:\n"
            + '\n'.join(booked_clients)
            + "\n\nВы уверены?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown',
        )
        return

    if db.delete_day_slots(target_date):
        await update.message.reply_text(f"✅ Все слоты на {day} ({target_date}) удалены.")
    else:
        await update.message.reply_text("❌ Ошибка при удалении.")


async def admin_view_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    bookings = db.get_all_bookings()
    if not bookings:
        await update.message.reply_text("📭 Нет активных записей.")
        return

    bookings.sort(key=lambda x: (x['date'], x['time']))
    message = "📋 ВСЕ АКТИВНЫЕ ЗАПИСИ:\n\n"
    current_date = None

    for b in bookings:
        booking_date = datetime.strptime(b['date'], "%Y-%m-%d")
        date_key = f"{b['day']}, {booking_date.strftime('%d.%m.%Y')}"
        if date_key != current_date:
            current_date = date_key
            message += f"\n📅 {date_key}:\n"
        message += f"  • {b['time']} — {b['user_name']} (@{b['username'] or '—'})\n"

    message += f"\n📊 Всего: {len(bookings)}"

    if len(message) > 4000:
        for chunk in [message[i:i+4000] for i in range(0, len(message), 4000)]:
            await update.message.reply_text(chunk)
    else:
        await update.message.reply_text(message)


async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    if not context.args:
        await update.message.reply_text(
            "📢 Использование: `/broadcast Ваше сообщение`", parse_mode='Markdown'
        )
        return

    text = ' '.join(context.args)
    users = db.get_all_users()
    await update.message.reply_text(f"⏳ Отправка {len(users)} пользователям...")

    sent = failed = 0
    for user in users:
        try:
            await context.bot.send_message(
                user['user_id'],
                f"📢 **Сообщение от администратора:**\n\n{text}",
                parse_mode='Markdown',
            )
            sent += 1
        except Exception as e:
            failed += 1
            logger.error(f"Broadcast failed for {user['user_id']}: {e}")

    await update.message.reply_text(
        f"✅ Рассылка завершена!\n• Отправлено: {sent}\n• Не доставлено: {failed}",
        parse_mode='Markdown',
    )


# ========== ЗАПУСК ==========

def main():
    application = Application.builder().token(BOT_TOKEN).build()

    # Пользовательские хендлеры
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("myid", show_my_id))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )
    application.add_handler(CallbackQueryHandler(callback_handler))

    # Админ-команды
    application.add_handler(CommandHandler("update_week", admin_update_week))
    application.add_handler(CommandHandler("view_slots", admin_view_slots))
    application.add_handler(CommandHandler("delete_day", admin_delete_day))
    application.add_handler(CommandHandler("view_all", admin_view_all))
    application.add_handler(CommandHandler("broadcast", admin_broadcast))

    # Обработчик ошибок
    async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.error(f"Update {update} caused error {context.error}", exc_info=True)
        if update and update.effective_message:
            await update.effective_message.reply_text(ERROR_MESSAGE)

    application.add_error_handler(error_handler)

    # ИСПРАВЛЕНО: напоминания через PTB job_queue (не APScheduler с args=[bot])
    # job_queue передаёт правильный context с context.bot автоматически
    application.job_queue.run_repeating(
        send_reminders,
        interval=300,   # каждые 5 минут
        first=10,       # первый запуск через 10 сек после старта
    )

    print("=" * 50)
    print("🤖 БОТ ЗАПУЩЕН")
    print("=" * 50)
    print(f"👑 Админ ID: {ADMIN_IDS[0] if ADMIN_IDS else 'не задан'}")
    print("📋 /start /myid /update_week /view_slots /delete_day /view_all /broadcast")
    print("=" * 50)

    application.run_polling()


if __name__ == '__main__':
    main()
