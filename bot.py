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

import asyncio
import logging
from datetime import datetime, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, RetryAfter
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
    WELCOME_MESSAGE, BOOKING_START,
    ALREADY_BOOKED, NO_SLOTS_MESSAGE, MY_BOOKINGS_HEADER,
    NO_BOOKINGS,
    REMINDER_USER, REMINDER_USER_24H, REMINDER_ADMIN, ERROR_MESSAGE,
    BUTTON_BOOK, BUTTON_MY_BOOKINGS, BUTTON_CANCEL,
)
from database import Database
from keyboards import Keyboards
from slot_manager import SlotManager
from slot_scheduler import SmartScheduler, Slot

# Аббревиатуры дней для отображения пользователю
_DAY_ABBR = {
    "Понедельник": "Пн ", "Вторник": "Вт ", "Среда": "Ср ",
    "Четверг": "Чт ", "Пятница": "Пт ", "Суббота": "Сб ", "Воскресенье": "Вск ",
}

def _esc_md(text) -> str:
    """Экранировать спецсимволы legacy-Markdown в пользовательском тексте.
    Без этого имя/username клиента с символами _ * ` [ ломает parse_mode='Markdown'
    (Telegram возвращает BadRequest: can't parse entities)."""
    if text is None:
        return ""
    text = str(text)
    for ch in ('\\', '_', '*', '`', '['):
        text = text.replace(ch, '\\' + ch)
    return text


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


async def _send_with_retry(bot, chat_id, text, **kwargs) -> bool:
    """
    Отправка сообщения с обработкой флуд-лимита Telegram (RetryAfter) — H4.
    При 429 ждём указанное API время и пробуем ещё раз.
    Возвращает True при успехе, False если доставить не удалось.
    Forbidden НЕ перехватывается здесь — он нужен вызывающему коду для деактивации
    пользователя (заблокировал бота).
    """
    for _attempt in range(3):
        try:
            await bot.send_message(chat_id, text, **kwargs)
            return True
        except RetryAfter as e:
            ra = getattr(e, 'retry_after', 1)
            # PTB ≥22 в будущем сделает retry_after timedelta — поддержим оба варианта
            if hasattr(ra, 'total_seconds'):
                ra = ra.total_seconds()
            await asyncio.sleep(float(ra) + 0.5)
    return False


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
    # H2: экранируем пользовательские поля, иначе имя/username с символами Markdown
    # (_ * ` [) ломают сообщение.
    message = (
        f"👤 **Ваша информация:**\n"
        f"• ID: `{user.id}`\n"
        f"• Имя: {_esc_md(user.first_name)}\n"
        f"• Username: @{_esc_md(user.username) or 'не указан'}\n"
        f"• Админ: {'✅ ДА' if is_admin else '❌ НЕТ'}\n"
    )
    # H5: список админских ID показываем только самим админам — не раскрываем посторонним.
    if is_admin:
        message += "\n**Текущие админы в config.py:**\n"
        for admin_id in (ADMIN_IDS or []):
            message += f"• `{admin_id}`\n"
        if not ADMIN_IDS:
            message += "• Список пуст\n"
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

    # Ищем ближайшие доступные слоты, проверяя текущую и следующие 2 недели.
    #
    # Старая логика смотрела на следующую неделю ТОЛЬКО если в БД вообще нет строк
    # для текущей недели. Это приводило к багу: в воскресенье вечером слоты текущей
    # недели ещё есть в БД (пусть все прошедшие), поэтому `if not all_slots` = False,
    # фильтр по времени срезал их все, и пользователь видел «все занято» —
    # хотя admin только что залил новые слоты на следующую неделю.
    #
    # Исправление: перебираем недели до тех пор, пока не найдём хотя бы один
    # видимый (будущий) слот.
    #
    # M1: одна запись в КАЛЕНДАРНУЮ неделю. Если у пользователя уже есть запись
    # на рассматриваемой неделе (в т.ч. уже прошедшая сессия) — пропускаем эту
    # неделю и предлагаем следующую. Так клиент, побывавший на сессии, сможет
    # записаться только на слоты следующей недели.
    now_tz = datetime.now(TIMEZONE)
    cutoff = now_tz + timedelta(hours=3)

    week_start = slot_manager.current_week_start
    all_slots: list = []
    visible_slots: list = []
    already_booked = None  # запись пользователя в пропущенной из-за лимита неделе

    for weeks_ahead in range(3):  # текущая неделя + 2 следующие
        candidate_week = (
            datetime.strptime(week_start, "%Y-%m-%d") + timedelta(weeks=weeks_ahead)
        ).strftime("%Y-%m-%d")
        candidate_slots = db.get_all_slots_for_scheduling(candidate_week)
        if not candidate_slots:
            continue

        if not is_admin:
            week_sunday = (
                datetime.strptime(candidate_week, "%Y-%m-%d") + timedelta(days=6)
            ).strftime("%Y-%m-%d")
            existing = db.get_user_booking_in_week(user_id, candidate_week, week_sunday)
            if existing:
                already_booked = already_booked or existing
                continue  # на этой неделе запись уже есть — пробуем следующую

        smart_candidate = SmartScheduler(candidate_slots)
        candidate_visible = smart_candidate.get_visible_slots()
        candidate_visible = [
            s for s in candidate_visible
            if TIMEZONE.localize(
                   datetime.strptime(f"{s.date} {s.current_time}", "%Y-%m-%d %H:%M")
               ) >= cutoff
        ]
        if candidate_visible:
            all_slots = candidate_slots
            visible_slots = candidate_visible
            break  # нашли неделю с будущими слотами — дальше не ищем

    if not visible_slots:
        if already_booked:
            await reply(
                ALREADY_BOOKED.format(
                    slot=f"{_day_label(already_booked['day'], already_booked['date'])} "
                         f"в {already_booked['time']}"
                ),
                reply_markup=Keyboards.get_main_keyboard(),
            )
        else:
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
            # Баг 4: используем pre_cancel_ чтобы сначала показать диалог «Вы уверены?»
            [InlineKeyboardButton("❌ Отменить запись",
                                  callback_data=f"pre_cancel_{booking['booking_id']}")],
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
            session_dt = TIMEZONE.localize(datetime.strptime(
                f"{booking['date']} {booking['time']}", "%Y-%m-%d %H:%M"
            ))
            if session_dt - datetime.now(TIMEZONE) < timedelta(hours=3):
                await update.message.reply_text(
                    f"❌ Отмена невозможна.\n\n"
                    f"Запись на {booking['day']} в {booking['time']} нельзя отменить "
                    f"менее чем за 3 часа до начала.\n\n"
                    f"Для отмены свяжитесь напрямую: @n_kshmlv",
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
            # Редактируем текущее сообщение — не создаём новое.
            # ReplyKeyboardMarkup «липкая»: она установлена через /start
            # и остаётся видна без повторной отправки.
            await query.edit_message_text(WELCOME_MESSAGE, reply_markup=None)

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

        elif data.startswith("final_confirm_"):
            slot_id = int(data[14:])
            await process_final_booking(query, context, slot_id)

        elif data.startswith("cancel_confirm_"):
            booking_id = int(data[15:])
            await process_cancellation(query, context, booking_id)

        # Баг 4: новый обработчик — показывает «Вы уверены?» перед отменой из «Мои записи»
        elif data.startswith("pre_cancel_"):
            booking_id = int(data[11:])
            await pre_cancel_confirm(query, booking_id)

        elif data.startswith("force_delete_day_"):
            date_str = data[17:]
            await force_delete_day(query, context, date_str)

        elif data == "cancel_delete":
            await query.edit_message_text("❌ Удаление отменено.")

        elif data == "change_booking":
            await process_change_booking(query, context)

        # Баг 1: новый обработчик — выполняет смену после подтверждения
        elif data.startswith("confirm_change_"):
            booking_id = int(data[15:])
            await execute_change_booking(query, context, booking_id)

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

    # Баг 7: фолбэк — загружаем слот из БД если контекст был потерян (рестарт бота)
    if not target_slot:
        slot_db = db.get_slot_by_id(slot_id)
        if not slot_db:
            await query.edit_message_text(
                "К сожалению, этот слот уже занят или недоступен. Выберите другое время — нажмите «✍🏻 Запись» в главном меню."
            )
            return
        # Перезагружаем всю неделю и пересчитываем через SmartScheduler
        fresh_slots = db.get_all_slots_for_scheduling(slot_db.week_start)
        smart_fresh = SmartScheduler(fresh_slots)
        visible_fresh = smart_fresh.get_visible_slots()
        target_slot = next((s for s in visible_fresh if s.id == slot_id), None)
        if not target_slot:
            await query.edit_message_text(
                "К сожалению, этот слот уже занят или недоступен. Выберите другое время — нажмите «✍🏻 Запись» в главном меню."
            )
            return
        # Восстанавливаем контекст для последующих шагов
        context.user_data['all_slots'] = fresh_slots
        context.user_data['visible_slots'] = visible_fresh

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


async def process_final_booking(query, context, slot_id: int):
    """Финальная запись с умным планировщиком и уведомлением админу."""
    user_id = query.from_user.id
    is_admin = user_id in ADMIN_IDS

    # Берём week_start из самого слота — он может быть на следующей неделе.
    # Если контекст потерян (рестарт бота) — подтягиваем неделю слота из БД.
    all_slots = context.user_data.get('all_slots', [])
    target_slot = next((s for s in all_slots if s.id == slot_id), None)
    if target_slot:
        week_start = target_slot.week_start
    else:
        slot_db = db.get_slot_by_id(slot_id)
        week_start = slot_db.week_start if slot_db else slot_manager.current_week_start

    # M1: одна запись в КАЛЕНДАРНУЮ неделю слота (учитываются и прошедшие сессии).
    # Это «дружелюбная» проверка ради красивого сообщения; финальную гарантию
    # даёт сама book_slot_with_scheduler внутри транзакции.
    if not is_admin:
        week_sunday = (
            datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)
        ).strftime("%Y-%m-%d")
        existing = db.get_user_booking_in_week(user_id, week_start, week_sunday)
        if existing:
            dl = _day_label(existing['day'], existing['date'])
            await query.edit_message_text(
                ALREADY_BOOKED.format(slot=f"{dl} в {existing['time']}")
            )
            return

    success, message, slot_info = db.book_slot_with_scheduler(user_id, slot_id, week_start, is_admin=is_admin)

    if success:
        # slot_info['time'] теперь содержит то время, которое видел пользователь (баг 9 исправлен)
        display_time = slot_info['time']
        _dl = _day_label(slot_info['day'], slot_info['date'])

        await query.edit_message_text(
            f"✔️ **Запись подтверждена!**\n\n"
            f"📅 {_dl}\n"
            f"⏰ {display_time}\n\n"
            f"Сессия продлится 60 минут.\n"
            f"За 15 минут до начала я пришлю напоминание.\n\n"
            f"До встречи! 🫂🤍",
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
                f"📱 Username: {uname}\n\n"
                f"📅 Слот: {_dl} в {display_time}"
            )
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(admin_id, admin_message)
                    logger.info(f"✔️ Уведомление о записи отправлено админу {admin_id}")
                except Exception as e:
                    logger.error(f"❌ Не удалось уведомить админа {admin_id}: {e}", exc_info=True)

        # Чистим контекст
        context.user_data.pop('selected_slot', None)
        context.user_data.pop('visible_slots', None)
        context.user_data.pop('all_slots', None)
    else:
        # Баг 2: используем реальное сообщение из планировщика/БД,
        # чтобы пользователь видел правильную причину (гонка, лимит подряд и т.д.)
        await query.edit_message_text(message)


async def process_cancellation(query, context, booking_id: int):
    user_id = query.from_user.id

    # Пункт 5: запрет отмены менее чем за 3 часа (кроме админа)
    if user_id not in ADMIN_IDS:
        booking_info = db.get_booking_by_id(booking_id)
        if booking_info:
            session_dt = TIMEZONE.localize(datetime.strptime(
                f"{booking_info['date']} {booking_info['adjusted_time']}", "%Y-%m-%d %H:%M"
            ))
            if session_dt - datetime.now(TIMEZONE) < timedelta(hours=3):
                await query.edit_message_text(
                    f"❌ Отмена невозможна.\n\n"
                    f"Запись на {booking_info['day']} в {booking_info['adjusted_time']} нельзя отменить "
                    f"менее чем за 3 часа до начала.\n\n"
                    f"Для отмены свяжитесь напрямую: @n_kshmlv"
                )
                return

    # Получаем инфо о записи ДО отмены чтобы включить в уведомление
    cancel_info = db.get_booking_by_id(booking_id)

    if db.cancel_booking_with_scheduler(user_id, booking_id):
        await query.edit_message_text(
            "✔️ **Запись успешно отменена.**\n\n"
            "Если хотите записаться снова, нажмите кнопку «Запись» в главном меню.",
            parse_mode='Markdown',
        )
        # Уведомление администратору с информацией о слоте
        for admin_id in ADMIN_IDS:
            try:
                user = query.from_user
                first = user.first_name or ''
                last = user.last_name or ''
                uname = f"@{user.username}" if user.username else 'нет username'
                slot_info_str = ""
                if cancel_info:
                    dl = _day_label(cancel_info['day'], cancel_info['date'])
                    slot_info_str = f"\n📅 Слот: {dl} в {cancel_info['adjusted_time']}"
                cancel_msg = (
                    f"❌ Запись отменена\n\n"
                    f"👤 Клиент: {first} {last}\n"
                    f"📱 Username: {uname}"
                    f"{slot_info_str}"
                )
                await context.bot.send_message(admin_id, cancel_msg)
                logger.info(f"✔️ Уведомление об отмене отправлено админу {admin_id}")
            except Exception as e:
                logger.error(f"❌ Не удалось уведомить админа {admin_id} об отмене: {e}", exc_info=True)
    else:
        await query.edit_message_text("❌ Не удалось отменить запись.")


async def pre_cancel_confirm(query, booking_id: int):
    """
    Баг 4: показывает диалог «Вы уверены?» перед отменой записи из раздела «Мои записи».
    Также проверяет 3-часовое правило, чтобы пользователь сразу получил понятный ответ.
    """
    user_id = query.from_user.id
    booking_info = db.get_booking_by_id(booking_id)
    if not booking_info:
        await query.edit_message_text("❌ Запись не найдена.")
        return

    if user_id not in ADMIN_IDS:
        session_dt = TIMEZONE.localize(datetime.strptime(
            f"{booking_info['date']} {booking_info['adjusted_time']}", "%Y-%m-%d %H:%M"
        ))
        if session_dt - datetime.now(TIMEZONE) < timedelta(hours=3):
            await query.edit_message_text(
                f"❌ Отмена невозможна.\n\n"
                f"До сессии {booking_info['day']} в {booking_info['adjusted_time']} "
                f"осталось менее 3 часов.\n\n"
                f"Для отмены свяжитесь напрямую: @n_kshmlv"
            )
            return

    dl = _day_label(booking_info['day'], booking_info['date'])
    await query.edit_message_text(
        f"Вы уверены, что хотите отменить запись?\n\n"
        f"📅 {dl} в {booking_info['adjusted_time']}",
        reply_markup=Keyboards.get_cancel_confirmation_keyboard(booking_id),
    )


async def process_change_booking(query, context):
    """
    Баг 1: добавлена проверка 3-часового правила и диалог подтверждения
    перед тем, как отменить текущую запись и открыть выбор нового времени.
    """
    user_id = query.from_user.id
    booking = db.get_user_active_booking(user_id)

    if not booking:
        await query.edit_message_text("❌ Активная запись не найдена.")
        return

    # Проверяем 3-часовое правило (кроме администратора)
    if user_id not in ADMIN_IDS:
        session_dt = TIMEZONE.localize(datetime.strptime(
            f"{booking['date']} {booking['time']}", "%Y-%m-%d %H:%M"
        ))
        if session_dt - datetime.now(TIMEZONE) < timedelta(hours=3):
            await query.edit_message_text(
                f"❌ Изменить запись невозможно.\n\n"
                f"До сессии {booking['day']} в {booking['time']} "
                f"осталось менее 3 часов.\n\n"
                f"Для изменения свяжитесь напрямую: @n_kshmlv"
            )
            return

    dl = _day_label(booking['day'], booking['date'])
    keyboard = [
        [InlineKeyboardButton(
            "✔️ Да, изменить",
            callback_data=f"confirm_change_{booking['booking_id']}"
        )],
        [InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")],
    ]
    await query.edit_message_text(
        f"📝 Текущая запись:\n📅 {dl} в {booking['time']}\n\n"
        f"Она будет отменена, и вы сможете выбрать новое время.\n\nПродолжить?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def execute_change_booking(query, context, booking_id: int):
    """Выполняет отмену и открывает выбор нового времени после подтверждения пользователя."""
    user_id = query.from_user.id
    booking = db.get_user_active_booking(user_id)

    if not booking or booking['booking_id'] != booking_id:
        await query.edit_message_text("❌ Не удалось изменить запись.")
        return

    # Баг 1: повторная проверка 3ч — пользователь мог подтвердить кнопку
    # спустя долгое время после того, как process_change_booking показал диалог.
    if user_id not in ADMIN_IDS:
        session_dt = TIMEZONE.localize(datetime.strptime(
            f"{booking['date']} {booking['time']}", "%Y-%m-%d %H:%M"
        ))
        if session_dt - datetime.now(TIMEZONE) < timedelta(hours=3):
            await query.edit_message_text(
                f"❌ Изменить запись больше нельзя.\n\n"
                f"До сессии {booking['day']} в {booking['time']} "
                f"осталось менее 3 часов.\n\n"
                f"Для изменения свяжитесь напрямую: @n_kshmlv"
            )
            return

    if db.cancel_booking_with_scheduler(user_id, booking_id):
        await query.edit_message_text(
            "✅ Текущая запись отменена.\n\nТеперь выберите новое время:"
        )

        # Уведомляем администратора об отмене в рамках переноса
        user = query.from_user
        first = user.first_name or ''
        last  = user.last_name  or ''
        uname = f"@{user.username}" if user.username else 'нет username'
        dl    = _day_label(booking['day'], booking['date'])
        change_msg = (
            f"✏️ Перенос записи (старый слот отменён)\n\n"
            f"👤 Клиент: {first} {last}\n"
            f"📱 Username: {uname}\n\n"
            f"📅 Отменён: {dl} в {booking['time']}\n"
            f"⏳ Клиент выбирает новое время..."
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(admin_id, change_msg)
            except Exception as e:
                logger.error(f"❌ Не удалось уведомить админа {admin_id} о переносе: {e}")

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
    Отправить напоминания:
      — за 24 часа: клиенту (текст об оплате и правиле отмены)
      — за 15 минут: клиенту + администратору
    Вызывается через job_queue PTB каждую минуту (окно в БД гарантирует «не раньше»).
    """
    logger.info("Checking reminders...")

    # ── Напоминания за 24 часа (только клиенту) ──────────────────────────────
    upcoming_24h = db.get_upcoming_sessions_24h()
    for session in upcoming_24h:
        slot_str = f"{session['day']} в {session['time']}"
        try:
            await context.bot.send_message(
                session['user_id'],
                REMINDER_USER_24H.format(slot=slot_str),
            )
            logger.info(f"24h reminder sent to user {session['user_id']}")
        except Forbidden:
            db.deactivate_user(session['user_id'])
            logger.warning(f"User {session['user_id']} blocked the bot — deactivated (24h reminder)")
        except Exception as e:
            logger.error(f"Failed to send 24h reminder to user {session['user_id']}: {e}")
            db.reset_24h_notification_for_booking(session['booking_id'])

    # ── Напоминания за 15 минут (клиенту + администратору) ───────────────────
    upcoming = db.get_upcoming_sessions(minutes_before=15)

    for session in upcoming:
        slot_str = f"{session['day']} в {session['time']}"

        try:
            await context.bot.send_message(
                session['user_id'],
                REMINDER_USER.format(slot=slot_str),
            )
            logger.info(f"Reminder sent to user {session['user_id']}")
        except Forbidden:
            db.deactivate_user(session['user_id'])
            logger.warning(f"User {session['user_id']} blocked the bot — deactivated")
        except Exception as e:
            logger.error(f"Failed to send reminder to user {session['user_id']}: {e}")
            db.reset_notification_for_booking(session['booking_id'])

        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    REMINDER_ADMIN.format(
                        name=session['name'],
                        username=f"@{session['username']}" if session.get('username') else 'нет username',
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
            # Баг 4: если дата уже прошла — берём следующий год.
            # Актуально при вводе январских слотов в декабре.
            if target_date_dt.date() < datetime.now(TIMEZONE).date():
                target_date_dt = datetime.strptime(f"{date_raw}.{year + 1}", "%d.%m.%Y")
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

    # Определяем недели добавленных слотов и для каждой ищем незаписанных.
    # Правило: уведомление получает тот, у кого нет записи на ЦЕЛЕВУЮ неделю слотов.
    # Если в одной команде добавлены слоты на разные недели — объединяем получателей
    # без дублей.
    added_weeks = {}  # week_start -> sunday_str
    for e in entries:
        ws = e["week_start"]
        if ws not in added_weeks:
            ws_dt = datetime.strptime(ws, "%Y-%m-%d")
            added_weeks[ws] = (ws_dt + timedelta(days=6)).strftime("%Y-%m-%d")

    seen_ids: set = set()
    users_to_notify = []
    for monday_str, sunday_str in added_weeks.items():
        for u in db.get_users_without_booking_on_week(monday_str, sunday_str):
            if u["user_id"] not in seen_ids:
                seen_ids.add(u["user_id"])
                users_to_notify.append(u)

    users_to_notify = [u for u in users_to_notify if u["user_id"] not in ADMIN_IDS]
    logger.info(f"Рассылка о новых слотах (недели: {list(added_weeks.keys())}): {len(users_to_notify)} пользователей")

    # --- Диагностика рассылки (видна только администратору) ---
    diag_lines = [f"🔍 Диагностика рассылки:"]
    diag_lines.append(f"• Недели слотов: {list(added_weeks.keys())}")
    diag_lines.append(f"• Кандидатов на уведомление: {len(users_to_notify)}")
    for u in users_to_notify:
        diag_lines.append(f"  – {u['first_name']} (@{u['username']}) id={u['user_id']}")
    await update.message.reply_text("\n".join(diag_lines))
    # --- конец диагностики ---

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
        blocked = 0
        failed_details = []
        for u in users_to_notify:
            try:
                if await _send_with_retry(context.bot, u["user_id"], notif_text):
                    sent += 1
                    logger.info(f"✅ Уведомление отправлено {u['user_id']} ({u['first_name']})")
                else:
                    failed_details.append(f"{u['first_name']} (id={u['user_id']}): флуд-лимит, не доставлено")
            except Forbidden:
                blocked += 1
                db.deactivate_user(u["user_id"])
                logger.warning(f"User {u['user_id']} ({u['first_name']}) заблокировал бота — деактивирован")
            except Exception as e:
                failed_details.append(f"{u['first_name']} (id={u['user_id']}): {e}")
                logger.warning(f"Не удалось уведомить {u['user_id']}: {e}")
            # H4: троттлинг ~20 сообщений/с — держимся ниже флуд-лимита Telegram
            await asyncio.sleep(0.05)
        result_msg = f"📣 Уведомлено {sent} из {len(users_to_notify)} пользователей."
        if blocked:
            result_msg += f"\n🚫 Заблокировали бота: {blocked} (деактивированы)"
        if failed_details:
            result_msg += "\n\n❌ Ошибки:\n" + "\n".join(failed_details)
        await update.message.reply_text(result_msg)
    else:
        await update.message.reply_text(
            "ℹ️ Нет пользователей для уведомления (все уже записаны или ещё не использовали бот)."
        )


async def admin_view_slots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показать расписание для администратора.
    Баг 6 исправлен: отображает текущую + следующие 2 недели,
    показывает только слоты, время которых ещё не прошло.
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    now_tz = datetime.now(TIMEZONE)
    week_start = slot_manager.current_week_start
    message = "📋 **Расписание (предстоящие слоты)**\n\n"
    found_any = False

    for weeks_ahead in range(3):  # текущая неделя + 2 следующие
        candidate_week = (
            datetime.strptime(week_start, "%Y-%m-%d") + timedelta(weeks=weeks_ahead)
        ).strftime("%Y-%m-%d")

        all_slots = db.get_all_slots_for_scheduling(candidate_week)
        if not all_slots:
            continue

        # Пересчитываем позиции через SmartScheduler (обновляет current_time)
        smart = SmartScheduler(all_slots)
        visible_ids = {s.id for s in smart.get_visible_slots()}

        # Фильтр: только слоты, время которых ещё не прошло
        future_slots = [
            s for s in all_slots
            if TIMEZONE.localize(
                   datetime.strptime(f"{s.date} {s.current_time}", "%Y-%m-%d %H:%M")
               ) >= now_tz
        ]
        if not future_slots:
            continue

        found_any = True
        ws_dt = datetime.strptime(candidate_week, "%Y-%m-%d")
        we_dt = ws_dt + timedelta(days=6)
        message += f"📆 **{ws_dt.strftime('%d.%m')} – {we_dt.strftime('%d.%m')}**\n"

        slots_by_day: dict = {}
        for slot in future_slots:
            slots_by_day.setdefault(slot.day, []).append(slot)

        days_order = sorted(slots_by_day.keys(), key=lambda d: min(s.date for s in slots_by_day[d]))

        for day in days_order:
            day_slots = sorted(slots_by_day[day], key=lambda s: s.current_time)
            booked = sum(1 for s in day_slots if s.is_booked)
            avail = sum(1 for s in day_slots if not s.is_booked and s.id in visible_ids)
            date_label = _day_label(day, day_slots[0].date)
            message += f"\n📅 **{date_label}** — {avail} своб., {booked} зан.:\n"

            for slot in day_slots:
                if slot.is_booked:
                    booking = db.get_booking_by_slot_id(slot.id)
                    # H2: имя клиента экранируем — сообщение идёт с parse_mode='Markdown'
                    client = f" — {_esc_md(booking['user_name'])}" if booking else ""
                    message += f"  ❌ {slot.base_time} → {slot.current_time}{client}\n"
                elif slot.id in visible_ids:
                    message += f"  ✅ {slot.base_time} → {slot.current_time}\n"
                else:
                    message += f"  ⚠️ {slot.base_time} — недоступен\n"

        message += "\n"

    if not found_any:
        await update.message.reply_text("📭 Нет предстоящих слотов на ближайшие 3 недели.")
        return

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

    # Ищем день в текущей и следующих 2 неделях — берём ближайший с слотами
    week_start_base = slot_manager.current_week_start
    target_date = None
    day_slots = []

    for weeks_ahead in range(3):
        candidate_week = (
            datetime.strptime(week_start_base, "%Y-%m-%d") + timedelta(weeks=weeks_ahead)
        ).strftime("%Y-%m-%d")
        candidate_date = (
            datetime.strptime(candidate_week, "%Y-%m-%d")
            + timedelta(days=DAY_OFFSET[day])
        ).strftime("%Y-%m-%d")

        candidate_slots = db.get_all_slots_for_scheduling(candidate_week)
        candidate_day_slots = [s for s in candidate_slots if s.date == candidate_date]
        if candidate_day_slots:
            target_date = candidate_date
            day_slots = candidate_day_slots
            break

    if not target_date:
        await update.message.reply_text(f"❌ На {day} нет слотов в ближайшие 3 недели.")
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


async def admin_delete_slot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /delete_slot ПТ 16.05 10:00,14:00
    Удалить конкретные свободные слоты. Слоты с активными записями не удаляются.
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    if not context.args or len(context.args) < 3:
        await update.message.reply_text(
            "🗑 Удаление конкретных слотов\n\n"
            "Формат:\n"
            "  /delete_slot ПТ 16.05 10:00,14:00\n\n"
            "Дни: ПН ВТ СР ЧТ ПТ СБ ВСК\n"
            "Занятые слоты удалить нельзя."
        )
        return

    DAY_ALIASES = {
        "пн": "Понедельник", "понедельник": "Понедельник",
        "вт": "Вторник",     "вторник": "Вторник",
        "ср": "Среда",       "среда": "Среда",
        "чт": "Четверг",     "четверг": "Четверг",
        "пт": "Пятница",     "пятница": "Пятница",
        "сб": "Суббота",     "суббота": "Суббота",
        "вск": "Воскресенье", "воскресенье": "Воскресенье",
    }

    day_key = context.args[0].lower().rstrip(".")
    if day_key not in DAY_ALIASES:
        await update.message.reply_text(
            "❌ Неизвестный день. Используйте: ПН ВТ СР ЧТ ПТ СБ ВСК"
        )
        return
    day_name = DAY_ALIASES[day_key]

    date_raw = context.args[1]
    times_raw = context.args[2]

    try:
        year = datetime.now(TIMEZONE).year
        target_date_dt = datetime.strptime(f"{date_raw}.{year}", "%d.%m.%Y")
        if target_date_dt.date() < datetime.now(TIMEZONE).date():
            target_date_dt = datetime.strptime(f"{date_raw}.{year + 1}", "%d.%m.%Y")
        target_date = target_date_dt.strftime("%Y-%m-%d")
    except ValueError:
        await update.message.reply_text(
            f"❌ Неверная дата '{date_raw}'. Формат: ДД.ММ (например 16.05)"
        )
        return

    times = []
    for t in times_raw.split(","):
        t = t.strip()
        try:
            datetime.strptime(t, "%H:%M")
            times.append(t)
        except ValueError:
            await update.message.reply_text(
                f"❌ Неверное время '{t}'. Формат: ЧЧ:ММ"
            )
            return

    result = db.delete_free_slots(target_date, times)
    label = _day_label(day_name, target_date)

    lines = []
    if result['deleted']:
        lines.append(f"✅ Удалены: {', '.join(result['deleted'])}")
    if result['booked']:
        lines.append(f"❌ Заняты, не удалены: {', '.join(result['booked'])}")
    if result['not_found']:
        lines.append(f"⚠️ Не найдены: {', '.join(result['not_found'])}")

    await update.message.reply_text(f"🗑 {label}\n\n" + "\n".join(lines))


async def admin_view_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет прав для этой команды.")
        return

    now_tz = datetime.now(TIMEZONE)
    all_bookings = db.get_all_bookings()
    # Показываем только предстоящие записи (прошедшие — уже состоялись)
    bookings = [
        b for b in all_bookings
        if TIMEZONE.localize(
               datetime.strptime(f"{b['date']} {b['time']}", "%Y-%m-%d %H:%M")
           ) >= now_tz
    ]
    if not bookings:
        await update.message.reply_text("📭 Нет предстоящих записей.")
        return

    bookings.sort(key=lambda x: (x['date'], x['time']))
    message = "📋 ПРЕДСТОЯЩИЕ ЗАПИСИ:\n\n"
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

    sent = failed = blocked = 0
    for user in users:
        try:
            if await _send_with_retry(
                context.bot,
                user['user_id'],
                f"📢 **Сообщение от администратора:**\n\n{text}",
                parse_mode='Markdown',
            ):
                sent += 1
            else:
                failed += 1
        except Forbidden:
            blocked += 1
            db.deactivate_user(user['user_id'])
            logger.warning(f"Broadcast: user {user['user_id']} заблокировал бота — деактивирован")
        except Exception as e:
            failed += 1
            logger.error(f"Broadcast failed for {user['user_id']}: {e}")
        # H4: троттлинг ~20 сообщений/с — держимся ниже флуд-лимита Telegram
        await asyncio.sleep(0.05)

    report = f"✅ Рассылка завершена!\n• Отправлено: {sent}"
    if blocked:
        report += f"\n• Заблокировали бота: {blocked} (деактивированы)"
    if failed:
        report += f"\n• Другие ошибки: {failed}"
    await update.message.reply_text(report, parse_mode='Markdown')


# ========== ОЧИСТКА СТАРЫХ ДАННЫХ ==========

async def run_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """
    Ежедневная задача:
      1. Переводит прошедшие активные сессии в статус 'completed'
         (без этого они мешают cleanup и статистике).
      2. Удаляет записи и слоты старше 4 недель (cancelled/completed).
    """
    completed = db.complete_past_sessions()
    result = db.cleanup_old_data(weeks_to_keep=4)
    logger.info(
        f"Daily cleanup: завершено прошедших сессий={completed}, "
        f"удалено записей={result['deleted_bookings']}, "
        f"слотов={result['deleted_slots']}"
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
    application.add_handler(CommandHandler("delete_slot", admin_delete_slot))
    application.add_handler(CommandHandler("view_all", admin_view_all))
    application.add_handler(CommandHandler("broadcast", admin_broadcast))

    # Обработчик ошибок
    async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.error(f"Update {update} caused error {context.error}", exc_info=True)
        if update and update.effective_message:
            await update.effective_message.reply_text(ERROR_MESSAGE)

    application.add_error_handler(error_handler)

    # Напоминания: каждую минуту.
    # Частый опрос нужен, чтобы напоминание уходило близко к отметке «за 15 минут»
    # (а не где-то в диапазоне). Окно в БД гарантирует «не раньше» нужного времени.
    application.job_queue.run_repeating(
        send_reminders,
        interval=60,
        first=10,
    )

    # Ежедневная очистка: завершить прошедшие сессии + удалить старые данные
    application.job_queue.run_repeating(
        run_cleanup,
        interval=86400,   # раз в сутки
        first=60,         # первый запуск через 60 сек после старта
    )

    print("=" * 50)
    print("🤖 БОТ ЗАПУЩЕН")
    print("=" * 50)
    print(f"👑 Админ ID: {ADMIN_IDS[0] if ADMIN_IDS else 'не задан'}")
    print("📋 /start /myid /update_week /view_slots /delete_day /delete_slot /view_all /broadcast")
    print("=" * 50)

    application.run_polling()


if __name__ == '__main__':
    main()
