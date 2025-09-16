import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import ChatMemberUpdated
from aiogram.enums import ChatMemberStatus
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import dateparser
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

from services.repositories import UserRepo, GroupRepo, RoleRepo, NotificationRepo, EventRepo, EventNotificationRepo, PersonalEventNotificationRepo, DispatchLogRepo
from config import BOT_TOKEN, SUPERADMIN_ID


logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Роли → русские названия
ROLE_RU = {
    'owner': 'Владелец',
    'admin': 'Админ',
    'member': 'Участник',
    'superadmin': 'Суперадмин',
}

# Память ожиданий (простая in-memory)
AWAITING_NOTIF_ADD: dict[int, dict] = {}  # user_id -> {gid, edit_chat_id, edit_message_id, prompt_message_id}
AWAITING_EVENT_CREATE: dict[int, dict] = {}  # user_id -> {gid, step, name?, time?, edit_chat_id, edit_message_id, prompt_message_id}
AWAITING_EVENT_EDIT: dict[int, dict] = {}    # user_id -> {eid, gid, mode: 'rename'|'retime', prompt_message_id, edit_chat_id, edit_message_id}
AWAITING_ADMIN_INPUT: dict[int, dict] = {}  # user_id -> {gid, mode: 'add'|'remove', type?: 'id'|'username'|'phone', edit_chat_id, edit_message_id, prompt_message_id}
AWAITING_EVENT_NOTIF: dict[int, dict] = {}  # user_id -> {eid, gid, edit_chat_id, edit_message_id, prompt_message_id}
AWAITING_PERSONAL_NOTIF: dict[int, dict] = {}  # user_id -> {eid, gid, edit_chat_id, edit_message_id, prompt_message_id}

# Единое сообщение-меню на пользователя
MENU_STATE: dict[int, dict] = {}  # user_id -> {chat_id, message_id}

async def set_menu_message(user_id: int, chat_id: int, text: str, markup: types.InlineKeyboardMarkup | None):
    state = MENU_STATE.get(user_id)
    if state and state.get('chat_id') == chat_id:
        # Пытаемся редактировать текущее меню
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=state['message_id'], reply_markup=markup)
            return
        except Exception:
            pass
    # Если не получилось — отправляем новое и запоминаем
    msg = await bot.send_message(chat_id, text, reply_markup=markup)
    MENU_STATE[user_id] = {'chat_id': chat_id, 'message_id': msg.message_id}

async def safe_answer(callback: types.CallbackQuery) -> None:
    try:
        await callback.answer()
    except Exception:
        # Ignore expired/invalid query id errors
        pass

async def refresh_personal_notifications_view(message: types.Message, event_id: int, group_id: int, user_id: int):
    """Refresh the personal notifications view for an event."""
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    
    # Get event info
    ev = EventRepo.get_by_id(event_id)
    if not ev:
        await message.answer("Мероприятие не найдено")
        return
    
    _id, name, time_str, group_id, resp_uid = ev
    
    # Get personal notifications
    notifs = PersonalEventNotificationRepo.list_by_user_and_event(user_id, event_id)
    
    kb = InlineKeyboardBuilder()
    lines = [f"📱 Мои напоминания для \"{name}\""]
    lines.append(f"Время: {format_event_time_display(time_str)}")
    lines.append("")
    
    if notifs:
        lines.append("Настроенные напоминания:")
        for notif_id, time_before, time_unit, message_text in notifs:
            time_display = format_duration_ru(time_before, time_unit)
            notification_time = calculate_notification_time(time_str, time_before, time_unit)
            lines.append(f"• {time_display} - Через {time_display} начало мероприятия")
            lines.append(f"  📅 Отправится: {notification_time}")
            kb.row(types.InlineKeyboardButton(text=f"❌ {time_display}", callback_data=f"evt_personal_notif_del:{notif_id}:{event_id}:{group_id}"))
    else:
        lines.append("Нет настроенных напоминаний")
    
    lines.append("")
    lines.append("Добавить напоминание:")
    
    # Add quick buttons
    kb.row(
        types.InlineKeyboardButton(text="1 день", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:1:days"),
        types.InlineKeyboardButton(text="2 дня", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:2:days"),
        types.InlineKeyboardButton(text="3 дня", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:3:days")
    )
    kb.row(
        types.InlineKeyboardButton(text="1 час", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:1:hours"),
        types.InlineKeyboardButton(text="2 часа", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:2:hours"),
        types.InlineKeyboardButton(text="30 мин", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:30:minutes")
    )
    kb.row(types.InlineKeyboardButton(text="Произвольное", callback_data=f"evt_personal_notif_add_free:{event_id}:{group_id}"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"evt_open:{event_id}:{group_id}"))
    
    text = "\n".join(lines)
    await bot.edit_message_text(text, chat_id=message.chat.id, message_id=message.message_id, reply_markup=kb.as_markup())

async def refresh_event_notifications_view(message: types.Message, event_id: int, group_id: int, user_id: int):
    """Refresh the event notifications view."""
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    
    # Get event info
    ev = EventRepo.get_by_id(event_id)
    if not ev:
        await message.answer("Мероприятие не найдено")
        return
    
    _id, name, time_str, group_id, resp_uid = ev
    
    # Get event notifications
    notifs = EventNotificationRepo.list_by_event(event_id)
    
    kb = InlineKeyboardBuilder()
    lines = [f"🔔 Групповые оповещения для \"{name}\""]
    lines.append(f"Время: {format_event_time_display(time_str)}")
    lines.append("")
    
    if notifs:
        lines.append("Настроенные оповещения:")
        for notif_id, time_before, time_unit, message_text in notifs:
            time_display = format_duration_ru(time_before, time_unit)
            notification_time = calculate_notification_time(time_str, time_before, time_unit)
            lines.append(f"• {time_display} - Через {time_display} начало мероприятия")
            lines.append(f"  📅 Отправится: {notification_time}")
            # Only show delete button if user can edit
            if can_edit_event_notifications(user_id, event_id):
                kb.row(types.InlineKeyboardButton(text=f"❌ {time_display}", callback_data=f"evt_notif_del:{notif_id}:{event_id}:{group_id}"))
    else:
        lines.append("Нет настроенных оповещений")
    
    # Only show add buttons if user can edit
    if can_edit_event_notifications(user_id, event_id):
        lines.append("")
        lines.append("Добавить оповещение:")
        
        # Add quick buttons
        kb.row(
            types.InlineKeyboardButton(text="1 день", callback_data=f"evt_notif_add:{event_id}:{group_id}:1:days"),
            types.InlineKeyboardButton(text="2 дня", callback_data=f"evt_notif_add:{event_id}:{group_id}:2:days"),
            types.InlineKeyboardButton(text="3 дня", callback_data=f"evt_notif_add:{event_id}:{group_id}:3:days")
        )
        kb.row(
            types.InlineKeyboardButton(text="1 час", callback_data=f"evt_notif_add:{event_id}:{group_id}:1:hours"),
            types.InlineKeyboardButton(text="2 часа", callback_data=f"evt_notif_add:{event_id}:{group_id}:2:hours"),
            types.InlineKeyboardButton(text="30 мин", callback_data=f"evt_notif_add:{event_id}:{group_id}:30:minutes")
        )
        kb.row(types.InlineKeyboardButton(text="Произвольное", callback_data=f"evt_notif_add_free:{event_id}:{group_id}"))
    
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"evt_open:{event_id}:{group_id}"))
    
    text = "\n".join(lines)
    await bot.edit_message_text(text, chat_id=message.chat.id, message_id=message.message_id, reply_markup=kb.as_markup())

# Variants that refresh by explicit chat/message ids to avoid constructing Message objects
async def refresh_personal_notifications_view_ids(edit_chat_id: int, edit_message_id: int, event_id: int, group_id: int, user_id: int):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    ev = EventRepo.get_by_id(event_id)
    if not ev:
        return
    _id, name, time_str, group_id, resp_uid = ev
    notifs = PersonalEventNotificationRepo.list_by_user_and_event(user_id, event_id)
    kb = InlineKeyboardBuilder()
    lines = [f"📱 Личные оповещения для \"{name}\""]
    lines.append(f"Время: {format_event_time_display(time_str)}")
    lines.append("")
    if notifs:
        lines.append("Настроенные напоминания:")
        for notif_id, time_before, time_unit, message_text in notifs:
            time_display = format_duration_ru(time_before, time_unit)
            notification_time = calculate_notification_time(time_str, time_before, time_unit)
            lines.append(f"• {time_display} - Через {time_display} начало мероприятия")
            lines.append(f"  📅 Отправится: {notification_time}")
            kb.row(types.InlineKeyboardButton(text=f"❌ {time_display}", callback_data=f"evt_personal_notif_del:{notif_id}:{event_id}:{group_id}"))
    else:
        lines.append("Нет настроенных напоминаний")
    lines.append("")
    lines.append("Добавить напоминание:")
    kb.row(
        types.InlineKeyboardButton(text="1 день", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:1:days"),
        types.InlineKeyboardButton(text="2 дня", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:2:days"),
        types.InlineKeyboardButton(text="3 дня", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:3:days")
    )
    kb.row(
        types.InlineKeyboardButton(text="1 час", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:1:hours"),
        types.InlineKeyboardButton(text="2 часа", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:2:hours"),
        types.InlineKeyboardButton(text="30 мин", callback_data=f"evt_personal_notif_add:{event_id}:{group_id}:30:minutes")
    )
    kb.row(types.InlineKeyboardButton(text="Произвольное", callback_data=f"evt_personal_notif_add_free:{event_id}:{group_id}"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"evt_open:{event_id}:{group_id}"))
    text = "\n".join(lines)
    await bot.edit_message_text(text, chat_id=edit_chat_id, message_id=edit_message_id, reply_markup=kb.as_markup())

async def refresh_event_notifications_view_ids(edit_chat_id: int, edit_message_id: int, event_id: int, group_id: int, user_id: int):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    ev = EventRepo.get_by_id(event_id)
    if not ev:
        return
    _id, name, time_str, group_id, resp_uid = ev
    notifs = EventNotificationRepo.list_by_event(event_id)
    kb = InlineKeyboardBuilder()
    lines = [f"🔔 Групповые оповещения для \"{name}\""]
    lines.append(f"Время: {format_event_time_display(time_str)}")
    lines.append("")
    if notifs:
        lines.append("Настроенные оповещения:")
        for notif_id, time_before, time_unit, message_text in notifs:
            time_display = format_duration_ru(time_before, time_unit)
            notification_time = calculate_notification_time(time_str, time_before, time_unit)
            lines.append(f"• {time_display} - Через {time_display} начало мероприятия")
            lines.append(f"  📅 Отправится: {notification_time}")
            if can_edit_event_notifications(user_id, event_id):
                kb.row(types.InlineKeyboardButton(text=f"❌ {time_display}", callback_data=f"evt_notif_del:{notif_id}:{event_id}:{group_id}"))
    else:
        lines.append("Нет настроенных оповещений")
    if can_edit_event_notifications(user_id, event_id):
        lines.append("")
        lines.append("Добавить оповещение:")
        kb.row(
            types.InlineKeyboardButton(text="1 день", callback_data=f"evt_notif_add:{event_id}:{group_id}:1:days"),
            types.InlineKeyboardButton(text="2 дня", callback_data=f"evt_notif_add:{event_id}:{group_id}:2:days"),
            types.InlineKeyboardButton(text="3 дня", callback_data=f"evt_notif_add:{event_id}:{group_id}:3:days")
        )
        kb.row(
            types.InlineKeyboardButton(text="1 час", callback_data=f"evt_notif_add:{event_id}:{group_id}:1:hours"),
            types.InlineKeyboardButton(text="2 часа", callback_data=f"evt_notif_add:{event_id}:{group_id}:2:hours"),
            types.InlineKeyboardButton(text="30 мин", callback_data=f"evt_notif_add:{event_id}:{group_id}:30:minutes")
        )
        kb.row(types.InlineKeyboardButton(text="Произвольное", callback_data=f"evt_notif_add_free:{event_id}:{group_id}"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"evt_open:{event_id}:{group_id}"))
    text = "\n".join(lines)
    await bot.edit_message_text(text, chat_id=edit_chat_id, message_id=edit_message_id, reply_markup=kb.as_markup())

# Утилиты форматирования и парсинга сроков
def calculate_notification_time(event_time_str: str, time_before: int, time_unit: str) -> str:
    """Calculate when a notification will be sent based on event time and notification settings."""
    from datetime import datetime, timedelta
    
    try:
        # Parse event time
        event_dt = datetime.strptime(event_time_str, '%Y-%m-%d %H:%M:%S')
        
        # Calculate notification time (BEFORE the event)
        if time_unit == 'minutes':
            notification_dt = event_dt - timedelta(minutes=time_before)
        elif time_unit == 'hours':
            notification_dt = event_dt - timedelta(hours=time_before)
        elif time_unit == 'days':
            notification_dt = event_dt - timedelta(days=time_before)
        elif time_unit == 'weeks':
            notification_dt = event_dt - timedelta(weeks=time_before)
        elif time_unit == 'months':
            # Approximate months as 30 days
            notification_dt = event_dt - timedelta(days=time_before * 30)
        else:
            return "Неизвестная единица времени"
        
        # Debug info
        print(f"DEBUG: Event time: {event_dt}, Notification time: {notification_dt}, Time before: {time_before} {time_unit}")
        
        # Format the result
        return notification_dt.strftime('%d.%m.%Y %H:%M')
    except Exception as e:
        return f"Ошибка: {str(e)}"

def format_duration_ru(amount: int, unit: str) -> str:
    # Для minutes попытаемся разложить на составные
    if unit == 'minutes':
        total = amount
        parts = []
        m_in_hour = 60
        m_in_day = 60 * 24
        m_in_week = m_in_day * 7
        m_in_month = m_in_day * 30
        months = total // m_in_month
        total %= m_in_month
        weeks = total // m_in_week
        total %= m_in_week
        days = total // m_in_day
        total %= m_in_day
        hours = total // m_in_hour
        minutes = total % m_in_hour
        if months:
            parts.append(f"{months} мес")
        if weeks:
            parts.append(f"{weeks} нед")
        if days:
            parts.append(f"{days} дн")
        if hours:
            parts.append(f"{hours} ч")
        if minutes:
            parts.append(f"{minutes} мин")
        return ' '.join(parts) if parts else '0 мин'
    names = {
        'months': 'мес',
        'weeks': 'нед',
        'days': 'дн',
        'hours': 'ч',
        'minutes': 'мин',
    }
    return f"{amount} {names.get(unit, unit)}"

def parse_duration_ru(text: str) -> int:
    """Парсит русское описание длительности и возвращает сумму в минутах."""
    import re
    tokens = re.split(r"[\s,]+", text.lower())
    tokens = [t for t in tokens if t]
    if not tokens:
        return 0
    units_map = {
        'месяц': 'months', 'месяца': 'months', 'месяцев': 'months', 'мес': 'months',
        'неделя': 'weeks', 'недели': 'weeks', 'недель': 'weeks', 'нед': 'weeks', 'неделями': 'weeks',
        'день': 'days', 'дня': 'days', 'дней': 'days', 'дн': 'days', 'днями': 'days',
        'час': 'hours', 'часа': 'hours', 'часов': 'hours', 'ч': 'hours', 'часами': 'hours',
        'минута': 'minutes', 'минуты': 'minutes', 'минут': 'minutes', 'мин': 'minutes', 'минутами': 'minutes',
    }
    m_in = {
        'months': 30 * 24 * 60,
        'weeks': 7 * 24 * 60,
        'days': 24 * 60,
        'hours': 60,
        'minutes': 1,
    }
    total = 0
    i = 0
    while i < len(tokens):
        try:
            num = int(tokens[i])
            i += 1
            if i < len(tokens):
                unit_token = tokens[i]
                unit = units_map.get(unit_token)
                if unit:
                    total += num * m_in[unit]
                    i += 1
                else:
                    # если не распознали единицу, считаем минутами
                    total += num
            else:
                total += num
        except ValueError:
            # пропускаем лишние слова
            i += 1
    return total


def parse_ru_datetime(text: str) -> datetime | None:
    """Парсит русскоязычное описание даты/времени в datetime (локальное время)."""
    dt = dateparser.parse(
        text,
        languages=["ru"],
        settings={
            'PREFER_DATES_FROM': 'future',
            'DATE_ORDER': 'DMY',
            'TIMEZONE': 'UTC',
            'RETURN_AS_TIMEZONE_AWARE': False,
        },
    )
    return dt

def format_event_time_display(time_str: str) -> str:
    """Пытается привести время к виду ДД.ММ.ГГГГ ЧЧ:ММ:СС для отображения."""
    # Попытка распарсить ISO "YYYY-MM-DD HH:MM:SS" или другие
    try_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"]
    for fmt in try_formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            pass
    # Как fallback попробуем dateparser
    dt2 = dateparser.parse(time_str, languages=["ru"], settings={'DATE_ORDER': 'DMY'})
    if dt2:
        return dt2.strftime("%d.%m.%Y %H:%M:%S")
    return time_str

def can_edit_event_notifications(user_id: int, event_id: int) -> bool:
    """Owner/admin/superadmin only (responsible no longer allowed)."""
    if user_id == SUPERADMIN_ID:
        return True
    ev = EventRepo.get_by_id(event_id)
    if not ev:
        return False
    _id, _name, _time_str, group_id, _resp_uid = ev
    role = RoleRepo.get_user_role(user_id, group_id)
    return role in ['owner', 'admin']


@dp.my_chat_member()
async def on_chat_member_update(event: ChatMemberUpdated):
    logging.info(f"my_chat_member update received: chat={event.chat.id}, status={event.new_chat_member.status}, actor={getattr(event.from_user,'id',None)}")
    # Only react when the bot is added to a group
    if event.new_chat_member.user.id != bot.id:
        return

    if event.new_chat_member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR):
        logging.info("Bot added to a chat (member/admin), proceeding with registration...")
        chat_id = str(event.chat.id)
        title = event.chat.title or 'Без названия'
        adder = event.from_user

        # Upsert the user who added the bot (if known)
        owner_user_id = None
        if adder is not None:
            owner_user_id = UserRepo.upsert_user(
                telegram_id=adder.id,
                username=adder.username,
                phone=None,
                first_name=adder.first_name,
                last_name=adder.last_name,
            )

        # Ensure group exists
        existing = GroupRepo.get_by_chat_id(chat_id)
        if not existing:
            group_id = GroupRepo.create(chat_id, title, owner_user_id)
            # If we know adder, grant owner role
            if owner_user_id is not None:
                RoleRepo.add_role(owner_user_id, group_id, 'owner', confirmed=True)
            # Add superadmin to the group as superadmin
            try:
                sa_row = UserRepo.get_by_telegram_id(SUPERADMIN_ID)
                if sa_row:
                    RoleRepo.add_role(sa_row[0], group_id, 'superadmin', confirmed=True)
            except Exception:
                pass
            # Default notifications
            NotificationRepo.ensure_defaults(group_id)
            await bot.send_message(event.chat.id, "Группа зарегистрирована. Настройки уведомлений по умолчанию созданы.")
        else:
            logging.info("Group already registered, skipping")


@dp.message(CommandStart())
async def start(message: types.Message):
    user = message.from_user
    user_id = UserRepo.upsert_user(
        telegram_id=user.id,
        username=user.username,
        phone=None,
        first_name=user.first_name,
        last_name=user.last_name,
    )

    # Сформировать корневое меню (единое сообщение)
    groups = GroupRepo.list_user_groups_with_roles(user_id)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    lines = []
    if user.id == SUPERADMIN_ID:
        lines.append(f"Роль: {ROLE_RU['superadmin']}")
    if not groups:
        lines.append("У вас пока нет групп. Добавьте бота в группу или дождитесь подтверждения доступа.")
    else:
        lines.append("Ваши группы:")
        for gid, title, role, chat_id in groups:
            role_ru = ROLE_RU.get(role, role)
            kb.button(text=f"{title} (роль: {role_ru})", callback_data=f"grp_menu:{gid}")
        kb.adjust(1)
    await set_menu_message(user_id, message.chat.id, "\n".join(lines), kb.as_markup())

    # Check if user matches any pending admins and confirm in those groups
    try:
        group_ids = RoleRepo.find_groups_for_pending(telegram_id=user.id, username=user.username, phone=None)
        for gid in group_ids:
            RoleRepo.confirm_admin_if_pending(user_id, gid)
        if group_ids:
            await message.answer(f"Ваш доступ админа подтвержден в группах: {', '.join(map(str, group_ids))}")
        else:
            # If nothing confirmed via id/username, suggest sharing phone number only if there are pending by phone
            if RoleRepo.has_any_pending_by_phone():
                kb = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="Поделиться телефоном", request_contact=True)]],
                    resize_keyboard=True,
                    one_time_keyboard=True,
                    selective=True,
                )
                await message.answer(
                    "Если вас добавляли администратором по телефону — поделитесь номером для подтверждения доступа.",
                    reply_markup=kb,
                )
    except Exception as e:
        logging.exception(f"Failed to confirm pending admin: {e}")


# Обработчики кнопок
@dp.callback_query(lambda c: c.data and c.data.startswith('grp_events:'))
async def cb_group_events(callback: types.CallbackQuery):
    print(f"DEBUG: cb_group_events called with data: {callback.data}")
    gid = int(callback.data.split(':', 1)[1])
    await safe_answer(callback)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    # Resolve current internal user
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    events = EventRepo.list_by_group(gid)
    kb = InlineKeyboardBuilder()
    lines = [f"Мероприятия (ID группы {gid})"]
    if events:
        for eid, name, time_str, resp_uid in events:
            who: str
            time_disp = format_event_time_display(time_str)
            if resp_uid:
                u = UserRepo.get_by_id(resp_uid)
                if u:
                    _iid, _tid, _uname, _phone, _first, _last = u
                    if _uname:
                        who = f"ответственный: @{_uname}"
                    elif _first or _last:
                        who = f"ответственный: {(_first or '').strip()} {(_last or '').strip()}".strip()
                    else:
                        who = f"ответственный: {_tid}"
                else:
                    who = f"ответственный: {resp_uid}"
            else:
                who = "без ответственного"
            lines.append(f"• {name}\n{time_disp} | {who}")
            # Only an Open button in the list; booking is managed inside the event card
            kb.button(text=f"Открыть: {name}", callback_data=f"evt_open:{eid}:{gid}")
        kb.adjust(1)
    else:
        lines.append("Пока нет мероприятий")
    # Кнопки действий (создание) и назад
    # Hide "+ Создать" for plain members
    role = RoleRepo.get_user_role(internal_user_id, gid) if internal_user_id is not None else None
    if role in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID:
        kb.row(types.InlineKeyboardButton(text="+ Создать", callback_data=f"evt_create:{gid}"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_menu:{gid}"))
    await set_menu_message(callback.from_user.id, callback.message.chat.id, "\n".join(lines), kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_open:'))
async def cb_event_open(callback: types.CallbackQuery):
    print(f"DEBUG: cb_event_open called with data: {callback.data}")
    try:
        _, eid, gid = callback.data.split(':')
        eid_i = int(eid)
        gid_i = int(gid)
        print(f"DEBUG: Parsed eid_i={eid_i}, gid_i={gid_i}")
        await callback.answer()
        print(f"DEBUG: After callback.answer()")
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        print(f"DEBUG: Before EventRepo.get_by_id")
        ev = EventRepo.get_by_id(eid_i)
        print(f"DEBUG: Event data: {ev}")
        if not ev:
            print(f"DEBUG: Event not found")
            await callback.message.answer("Мероприятие не найдено")
            return
        _id, name, time_str, group_id, resp_uid = ev
        # Resolve current user internal id
        urow = UserRepo.get_by_telegram_id(callback.from_user.id)
        internal_user_id = urow[0] if urow else None
        kb = InlineKeyboardBuilder()
        
        # Role-based controls
        role = RoleRepo.get_user_role(internal_user_id, gid_i) if internal_user_id is not None else None
        if role in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID:
            kb.row(
                types.InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"evt_rename:{eid_i}:{gid_i}"),
                types.InlineKeyboardButton(text="🕒 Изм. дату/время", callback_data=f"evt_retime:{eid_i}:{gid_i}")
            )
            kb.row(
                types.InlineKeyboardButton(text="Назначить ответственного", callback_data=f"evt_assign:{eid_i}:{gid_i}"),
                types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"evt_delete:{eid_i}:{gid_i}")
            )
        # Owner or Superadmin can send an immediate notify to the group and DM responsible
        if (role == "owner") or (callback.from_user.id == SUPERADMIN_ID):
            kb.row(types.InlineKeyboardButton(text="📣 Отправить оповещение", callback_data=f"evt_notify_now:{eid_i}:{gid_i}"))
        # Booking/unbooking in event card for participants
        if not resp_uid:
            kb.row(types.InlineKeyboardButton(text="Забронировать", callback_data=f"evt_book_toggle:{eid_i}:{gid_i}"))
        else:
            # Allow unassign button for owners/admins/superadmin or the responsible themselves
            if internal_user_id is not None and (internal_user_id == resp_uid or role in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID):
                kb.row(types.InlineKeyboardButton(text="❌ Убрать ответственного", callback_data=f"evt_unassign:{eid_i}:{gid_i}"))
        
        # Group notifications only for owner/admin/superadmin
        if internal_user_id and can_edit_event_notifications(internal_user_id, eid_i):
            kb.row(types.InlineKeyboardButton(text="🔔 Групповые оповещения", callback_data=f"evt_notifications:{eid_i}:{gid_i}"))
        # Personal notifications for responsible OR owner/admin/superadmin
        if internal_user_id and (
            (resp_uid and internal_user_id == resp_uid)
            or (role in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID)
        ):
            kb.row(types.InlineKeyboardButton(text="📱 Личные оповещения", callback_data=f"evt_personal_notifications:{eid_i}:{gid_i}"))

        kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_events:{gid_i}"))
        
        # Prepare display text for responsible person
        if resp_uid:
            u = UserRepo.get_by_id(resp_uid)
            if u:
                _iid, _tid, _uname, _phone, _first, _last = u
                if _uname:
                    resp_text = f"@{_uname}"
                elif _first or _last:
                    resp_text = f"{(_first or '').strip()} {(_last or '').strip()}".strip()
                else:
                    resp_text = str(_tid)
            else:
                resp_text = str(resp_uid)
        else: 
            resp_text = 'не назначен'
        text = f"{name}\nВремя: {format_event_time_display(time_str)}\nОтветственный: {resp_text}"
        await set_menu_message(callback.from_user.id, callback.message.chat.id, text, kb.as_markup())
    except Exception as e:
        print(f"DEBUG: Error in cb_event_open: {e}")
        import traceback
        traceback.print_exc()

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_delete:'))
async def cb_event_delete(callback: types.CallbackQuery):
    try:
        _, eid, gid = callback.data.split(':')
        print(f"DELETE EVENT: eid={eid}, gid={gid}")
        result = EventRepo.delete(int(eid))
        print(f"DELETE RESULT: {result}")
        await callback.answer("Удалено")
    except Exception as e:
        print(f"DELETE ERROR: {e}")
        await callback.answer(f"Ошибка: {e}")
    # refresh list
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    gid_i = int(gid)
    events = EventRepo.list_by_group(gid_i)
    kb = InlineKeyboardBuilder()
    lines = [f"Мероприятия (ID группы {gid_i})"]
    if events:
        for eid, name, time_str, resp_uid in events:
            if resp_uid:
                u = UserRepo.get_by_id(resp_uid)
                if u:
                    _iid, _tid, _uname, _phone, _first, _last = u
                    if _uname:
                        who = f"ответственный: @{_uname}"
                    elif _first or _last:
                        who = f"ответственный: {(_first or '').strip()} {(_last or '').strip()}".strip()
                    else:
                        who = f"ответственный: {_tid}"
                else:
                    who = f"ответственный: {resp_uid}"
            else:
                who = "без ответственного"
            lines.append(f"• {name}\n{time_str} | {who}")
            kb.button(text=f"Открыть: {name}", callback_data=f"evt_open:{eid}:{gid_i}")
        kb.adjust(1)
    else:
        lines.append("Пока нет мероприятий")
    kb.row(types.InlineKeyboardButton(text="+ Создать", callback_data=f"evt_create:{gid_i}"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_menu:{gid_i}"))
    await set_menu_message(callback.from_user.id, callback.message.chat.id, "\n".join(lines), kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_assign:'))
async def cb_event_assign(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await safe_answer(callback)
    prompt = await callback.message.answer("Введите @username или ID Telegram ответственного пользователя")
    AWAITING_EVENT_CREATE[callback.from_user.id] = {
        'mode': 'assign', 'eid': eid_i, 'gid': gid_i,
        'edit_chat_id': callback.message.chat.id,
        'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_unassign:'))
async def cb_event_unassign(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    EventRepo.set_responsible(eid_i, None)
    await callback.answer("Ответственный снят")
    # refresh card directly
    ev = EventRepo.get_by_id(eid_i)
    if ev:
        _id, name, time_str, group_id, resp_uid = ev
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        # Current user role
        urow = UserRepo.get_by_telegram_id(callback.from_user.id)
        internal_user_id = urow[0] if urow else None
        role = RoleRepo.get_user_role(internal_user_id, gid_i) if internal_user_id is not None else None
        # Admin controls
        if role in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID:
            kb.row(
                types.InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"evt_rename:{eid_i}:{gid_i}"),
                types.InlineKeyboardButton(text="🕒 Изм. дату/время", callback_data=f"evt_retime:{eid_i}:{gid_i}")
            )
            kb.row(
                types.InlineKeyboardButton(text="Назначить ответственного", callback_data=f"evt_assign:{eid_i}:{gid_i}"),
                types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"evt_delete:{eid_i}:{gid_i}")
            )
        # Booking controls
        if not resp_uid:
            kb.row(types.InlineKeyboardButton(text="Забронировать", callback_data=f"evt_book_toggle:{eid_i}:{gid_i}"))
        else:
            if internal_user_id is not None and (internal_user_id == resp_uid or role in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID):
                kb.row(types.InlineKeyboardButton(text="❌ Убрать ответственного", callback_data=f"evt_unassign:{eid_i}:{gid_i}"))
        # Group notifications for admins
        if internal_user_id and can_edit_event_notifications(internal_user_id, eid_i):
            kb.row(types.InlineKeyboardButton(text="🔔 Групповые оповещения", callback_data=f"evt_notifications:{eid_i}:{gid_i}"))
        # Personal notifications for responsible OR owner/admin/superadmin
        if internal_user_id and (
            (resp_uid and internal_user_id == resp_uid)
            or (role in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID)
        ):
            kb.row(types.InlineKeyboardButton(text="📱 Личные оповещения", callback_data=f"evt_personal_notifications:{eid_i}:{gid_i}"))
        kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_events:{gid_i}"))
        
        # Prepare display text for responsible person
        if resp_uid:
            u = UserRepo.get_by_id(resp_uid)
            if u:
                _iid, _tid, _uname, _phone, _first, _last = u
                if _uname:
                    resp_text = f"@{_uname}"
                elif _first or _last:
                    resp_text = f"{(_first or '').strip()} {(_last or '').strip()}".strip()
                else:
                    resp_text = str(_tid)
            else:
                resp_text = str(resp_uid)
        else:
            resp_text = 'не назначен'
        text = f"{name}\nВремя: {format_event_time_display(time_str)}\nОтветственный: {resp_text}"
        await set_menu_message(callback.from_user.id, callback.message.chat.id, text, kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_rename:'))
async def cb_event_rename_prompt(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    await safe_answer(callback)
    prompt = await callback.message.answer("Введите новое название мероприятия")
    AWAITING_EVENT_EDIT[callback.from_user.id] = {
        'eid': int(eid), 'gid': int(gid), 'mode': 'rename',
        'edit_chat_id': callback.message.chat.id, 'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_retime:'))
async def cb_event_retime_prompt(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    await safe_answer(callback)
    prompt = await callback.message.answer(
        "Введите новую дату/время (напр. '22 сентября 8 утра', '15.09.2025 00:00', '15.09.2025 00:00:00')"
    )
    AWAITING_EVENT_EDIT[callback.from_user.id] = {
        'eid': int(eid), 'gid': int(gid), 'mode': 'retime',
        'edit_chat_id': callback.message.chat.id, 'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_notifications:'))
async def cb_event_notifications(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await safe_answer(callback)
    
    # Check permissions
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    if not internal_user_id or not can_edit_event_notifications(internal_user_id, eid_i):
        await callback.message.answer("У вас нет прав для редактирования оповещений этого мероприятия")
        return
    
    # Use the new refresh function
    await refresh_event_notifications_view(callback.message, eid_i, gid_i, internal_user_id)

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_personal_notifications:'))
async def cb_event_personal_notifications(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await safe_answer(callback)
    
    # Get user
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    if not urow:
        await callback.message.answer("Пользователь не найден")
        return
    internal_user_id = urow[0]
    
    # Personal notifications are now handled automatically by set_responsible
    # No need to seed them manually here
    # Use the new refresh function
    await refresh_personal_notifications_view(callback.message, eid_i, gid_i, internal_user_id)

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_notif_add:'))
async def cb_event_notif_add(callback: types.CallbackQuery):
    _, eid, gid, amount, unit = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    amount_i = int(amount)
    await safe_answer(callback)
    
    # Check permissions
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    if not internal_user_id or not can_edit_event_notifications(internal_user_id, eid_i):
        await callback.message.answer("У вас нет прав для редактирования оповещений этого мероприятия")
        return
    
    # Add notification with standard text
    EventNotificationRepo.add_notification(eid_i, amount_i, unit, f"Через {format_duration_ru(amount_i, unit)} начало мероприятия")
    await callback.answer("Добавлено")
    # Refresh view by updating the message directly
    await refresh_event_notifications_view(callback.message, eid_i, gid_i, internal_user_id)

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_notif_del:'))
async def cb_event_notif_del(callback: types.CallbackQuery):
    _, notif_id, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await safe_answer(callback)
    
    # Check permissions
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    if not internal_user_id or not can_edit_event_notifications(internal_user_id, eid_i):
        await callback.message.answer("У вас нет прав для редактирования оповещений этого мероприятия")
        return
    
    # Delete notification
    EventNotificationRepo.delete_notification(int(notif_id))
    await callback.answer("Удалено")
    # Refresh view by updating the message directly
    await refresh_event_notifications_view(callback.message, eid_i, gid_i, internal_user_id)

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_notif_add_free:'))
async def cb_event_notif_add_free(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await safe_answer(callback)
    
    # Check permissions
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    if not internal_user_id or not can_edit_event_notifications(internal_user_id, eid_i):
        await callback.message.answer("У вас нет прав для редактирования оповещений этого мероприятия")
        return
    
    prompt = await callback.message.answer(
        "Введите срок до мероприятия (например: '1 неделя', '2 дня', '1 день и 3 часа', '40 минут', '1 час 6 минут')\n"
        "Или укажите точную дату/время оповещения: '18.09.2025 22:30', '2025-09-18 22:30'"
    )
    AWAITING_EVENT_NOTIF[callback.from_user.id] = {
        'eid': eid_i,
        'gid': gid_i,
        'edit_chat_id': callback.message.chat.id,
        'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_personal_notif_add:'))
async def cb_personal_notif_add(callback: types.CallbackQuery):
    _, eid, gid, amount, unit = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    amount_i = int(amount)
    await safe_answer(callback)
    
    # Get user
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    if not urow:
        await callback.message.answer("Пользователь не найден")
        return
    internal_user_id = urow[0]
    
    # Add personal notification with standard text
    PersonalEventNotificationRepo.add_notification(internal_user_id, eid_i, amount_i, unit, f"Через {format_duration_ru(amount_i, unit)} начало мероприятия")
    await callback.answer("Добавлено")
    # Refresh view by updating the message directly
    await refresh_personal_notifications_view(callback.message, eid_i, gid_i, internal_user_id)

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_personal_notif_del:'))
async def cb_personal_notif_del(callback: types.CallbackQuery):
    _, notif_id, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await safe_answer(callback)
    
    # Get user
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    if not urow:
        await callback.message.answer("Пользователь не найден")
        return
    internal_user_id = urow[0]
    
    # Delete personal notification
    PersonalEventNotificationRepo.delete_notification(int(notif_id), internal_user_id)
    await callback.answer("Удалено")
    # Refresh view by updating the message directly
    await refresh_personal_notifications_view(callback.message, eid_i, gid_i, internal_user_id)

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_personal_notif_add_free:'))
async def cb_personal_notif_add_free(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await safe_answer(callback)
    
    # Get user
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    if not urow:
        await callback.message.answer("Пользователь не найден")
        return
    
    prompt = await callback.message.answer(
        "Введите срок до мероприятия (например: '1 неделя', '2 дня', '1 день и 3 часа', '40 минут', '1 час 6 минут')\n"
        "Или укажите точную дату/время оповещения: '18.09.2025 22:30', '2025-09-18 22:30'"
    )
    AWAITING_PERSONAL_NOTIF[callback.from_user.id] = {
        'eid': eid_i,
        'gid': gid_i,
        'edit_chat_id': callback.message.chat.id,
        'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_create:'))
async def cb_event_create(callback: types.CallbackQuery):
    _, gid = callback.data.split(':')
    gid_i = int(gid)
    await callback.answer()
    prompt = await callback.message.answer("Введите название мероприятия")
    AWAITING_EVENT_CREATE[callback.from_user.id] = {
        'mode': 'create', 'gid': gid_i, 'step': 'name',
        'edit_chat_id': callback.message.chat.id,
        'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }

def build_notifies_ui(gid: int):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    group = GroupRepo.get_by_id(gid)
    group_title = group[2] if group else f"Группа {gid}"
    header = f"Оповещения: {group_title} (ID {gid})"
    notifies = NotificationRepo.list_notifications(gid)
    kb = InlineKeyboardBuilder()
    if notifies:
        for nid, time_before, time_unit, message_text, is_default in notifies:
            pretty = format_duration_ru(time_before if time_unit=='minutes' else time_before, time_unit)
            kb.row(
                types.InlineKeyboardButton(text=pretty, callback_data="noop"),
                types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"notif_del:{nid}:{gid}")
            )
    else:
        kb.row(types.InlineKeyboardButton(text="Нет настроек", callback_data="noop"))
    kb.row(
        types.InlineKeyboardButton(text="+ 3 дня", callback_data=f"notif_add:{gid}:3:days"),
        types.InlineKeyboardButton(text="+ 1 день", callback_data=f"notif_add:{gid}:1:days"),
        types.InlineKeyboardButton(text="+ 2 часа", callback_data=f"notif_add:{gid}:2:hours")
    )
    kb.row(types.InlineKeyboardButton(text="+ Ввести вручную…", callback_data=f"notif_add_free:{gid}"))
    # Назад к меню группы
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_menu:{gid}"))
    return header, kb.as_markup()


def build_admins_ui(gid: int):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    admins = GroupRepo.list_group_admins(gid)
    pending = RoleRepo.list_pending_admins(gid)
    lines = [f"Администраторы группы {gid}"]
    if admins:
        lines.append("Подтвержденные:")
        for uid, tid, uname in admins:
            uname_disp = f"@{uname}" if uname else str(tid)
            kb.row(
                types.InlineKeyboardButton(text=uname_disp, callback_data="noop"),
                types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"adm_del:{uid}:{gid}")
            )
    else:
        kb.row(types.InlineKeyboardButton(text="Нет подтвержденных админов", callback_data="noop"))
    if pending:
        lines.append("")
        lines.append("Ожидающие подтверждения:")
        for pid, ident, ident_type, created_by, created_at in pending:
            kb.row(
                types.InlineKeyboardButton(text=f"{ident_type}: {ident}", callback_data="noop"),
                types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"padm_del:{pid}:{gid}")
            )
    # Add controls
    kb.row(types.InlineKeyboardButton(text="+ Добавить по ID", callback_data=f"adm_add_id:{gid}"))
    kb.row(types.InlineKeyboardButton(text="+ Добавить по @username", callback_data=f"adm_add_username:{gid}"))
    kb.row(types.InlineKeyboardButton(text="+ Добавить по телефону", callback_data=f"adm_add_phone:{gid}"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_menu:{gid}"))
    return "\n".join(lines), kb.as_markup()

@dp.callback_query(lambda c: c.data and c.data.startswith('grp_notifies:'))
async def cb_group_notifies(callback: types.CallbackQuery):
    gid = int(callback.data.split(':', 1)[1])
    await callback.answer()
    header, markup = build_notifies_ui(gid)
    await set_menu_message(callback.from_user.id, callback.message.chat.id, header, markup)


@dp.callback_query(lambda c: c.data and c.data.startswith('grp_remind:'))
async def cb_group_remind(callback: types.CallbackQuery):
    gid = int(callback.data.split(':', 1)[1])
    await callback.answer()
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="1 неделя", callback_data=f"grp_remind_period:{gid}:7days"),
        types.InlineKeyboardButton(text="1 месяц", callback_data=f"grp_remind_period:{gid}:1month"),
    )
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_menu:{gid}"))
    await set_menu_message(callback.from_user.id, callback.message.chat.id, "Выберите период для напоминаний:", kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith('grp_remind_period:'))
async def cb_group_remind_period(callback: types.CallbackQuery):
    _, gid, period = callback.data.split(':')
    gid_i = int(gid)
    await callback.answer()
    # Permissions: owner/admin/superadmin only
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    role = RoleRepo.get_user_role(internal_user_id, gid_i) if internal_user_id is not None else None
    if not (callback.from_user.id == SUPERADMIN_ID or role in ("owner", "admin")):
        await callback.message.answer("Недостаточно прав")
        return
    # Compute range
    now = datetime.utcnow()
    if period == '7days':
        end = now + timedelta(days=7)
    else:
        end = now + timedelta(days=30)
    start_iso = now.strftime('%Y-%m-%d %H:%M:%S')
    end_iso = end.strftime('%Y-%m-%d %H:%M:%S')
    events = EventRepo.list_by_group_between(gid_i, start_iso, end_iso)
    # Resolve target chat: send to the group's chat, not to the user's PM
    grp = GroupRepo.get_by_id(gid_i)
    if not grp:
        await callback.message.answer("Группа не найдена")
        return
    try:
        target_chat_id = int(grp[1])  # telegram_chat_id
    except Exception:
        target_chat_id = grp[1]
    if not events:
        await callback.message.answer("В выбранный период мероприятий нет")
        return
    # Send messages with booking button
    for eid, name, time_str, resp_uid in events:
        # Resolve responsible display
        if resp_uid:
            u = UserRepo.get_by_id(resp_uid)
            if u:
                _iid, _tid, _uname, _phone, _first, _last = u
                if _uname:
                    who = f"@{_uname}"
                elif _first or _last:
                    who = f"{(_first or '').strip()} {(_last or '').strip()}".strip()
                else:
                    who = str(_tid)
            else:
                who = str(resp_uid)
        else:
            who = None
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        label = who if who else "Забронировать"
        kb.row(types.InlineKeyboardButton(text=label, callback_data=f"evt_book_toggle:{eid}:{gid_i}"))
        text = f"• {name}\n{format_event_time_display(time_str)}"
        await bot.send_message(target_chat_id, text, reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith('role_book:'))
async def cb_role_book(callback: types.CallbackQuery):
    try:
        _, eid, gid, role_name = callback.data.split(':', 3)
        eid_i = int(eid); gid_i = int(gid)
        await callback.answer()
    except Exception as e:
        return await callback.answer(f"Ошибка: {e}")
    # Ensure user exists in our DB
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    if not urow:
        UserRepo.upsert_user(callback.from_user.id, callback.from_user.username, None, callback.from_user.first_name, callback.from_user.last_name)
        urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    user_id = urow[0]
    # Add member role if missing for visibility
    try:
        if not RoleRepo.get_user_role(user_id, gid_i):
            RoleRepo.add_role(user_id, gid_i, 'member', True)
    except Exception:
        pass
    # Assign role via web repo logic
    from services.repositories import EventRoleAssignmentRepo
    # Check multi-role flag and existing assignments for this user
    try:
        ev = EventRepo.get_by_id(eid_i)
        allow_multi = ev[5] if ev and len(ev) > 5 else 0
        if not allow_multi:
            existing = [uid for _r, uid in EventRoleAssignmentRepo.list_for_event(eid_i) if uid == user_id]
            if existing:
                return await callback.answer("Допустима только 1 бронь в этом мероприятии", show_alert=True)
    except Exception:
        pass
    if EventRoleAssignmentRepo.assign(eid_i, role_name, user_id):
        await refresh_role_keyboard(callback.message, gid_i, eid_i)
    else:
        await callback.answer("Уже занято", show_alert=False)

@dp.callback_query(lambda c: c.data and c.data.startswith('role_unbook:'))
async def cb_role_unbook(callback: types.CallbackQuery):
    try:
        _, eid, gid, role_name = callback.data.split(':', 3)
        eid_i = int(eid); gid_i = int(gid)
        await callback.answer()
    except Exception as e:
        return await callback.answer(f"Ошибка: {e}")
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    user_id = urow[0] if urow else None
    if not user_id:
        return await callback.answer("Нет пользователя", show_alert=False)
    from services.repositories import EventRoleAssignmentRepo, RoleRepo
    # Admins/owners can unassign any user; find current assignee for this role
    try:
        role = RoleRepo.get_user_role(user_id, gid_i)
        is_admin = role in ['admin', 'owner', 'superadmin'] if role else False
    except Exception:
        is_admin = False
    target_uid = user_id
    if is_admin:
        try:
            for r, uid in EventRoleAssignmentRepo.list_for_event(eid_i):
                if r == role_name:
                    target_uid = uid
                    break
        except Exception:
            pass
    if EventRoleAssignmentRepo.unassign(eid_i, role_name, target_uid):
        await refresh_role_keyboard(callback.message, gid_i, eid_i)
    else:
        await callback.answer("Нельзя снять чужую бронь", show_alert=False)

async def refresh_role_keyboard(message: types.Message, gid: int, eid: int):
    # Rebuild keyboard for roles
    from services.repositories import EventRoleRequirementRepo, EventRoleAssignmentRepo, DisplayNameRepo
    reqs = EventRoleRequirementRepo.list_for_event(eid)
    asgs = EventRoleAssignmentRepo.list_for_event(eid)
    asg_map = {}
    for r, uid in asgs:
        asg_map.setdefault(r, []).append(uid)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    for rname, _req in sorted(reqs, key=lambda x: x[0].lower()):
        assigned = asg_map.get(rname, [])
        if assigned:
            uid = assigned[0]
            dn = DisplayNameRepo.get_display_name(gid, uid)
            label = dn if dn else f"ID:{uid}"
            kb.row(types.InlineKeyboardButton(text=f"✅ {rname}: {label}", callback_data=f"role_unbook:{eid}:{gid}:{rname}"))
        else:
            kb.row(types.InlineKeyboardButton(text=f"🟢 {rname}: Забронировать", callback_data=f"role_book:{eid}:{gid}:{rname}"))
    try:
        await bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=kb.as_markup())
    except Exception:
        pass

@dp.callback_query(lambda c: c.data and c.data.startswith('evt_notify_now:'))
async def cb_evt_notify_now(callback: types.CallbackQuery):
    _, eid, gid = callback.data.split(':')
    eid_i = int(eid)
    gid_i = int(gid)
    await callback.answer()
    # Permissions: owner or superadmin
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    role = RoleRepo.get_user_role(internal_user_id, gid_i) if internal_user_id is not None else None
    if not (callback.from_user.id == SUPERADMIN_ID or role == "owner"):
        await callback.answer("Недостаточно прав", show_alert=False)
        return
    ev = EventRepo.get_by_id(eid_i)
    if not ev:
        await callback.answer("Мероприятие не найдено", show_alert=False)
        return
    _id, name, time_str, group_id, resp_uid = ev
    grp = GroupRepo.get_by_id(group_id)
    if not grp:
        await callback.answer("Группа не найдена", show_alert=False)
        return
    chat_id = grp[1]
    # Resolve responsible label
    label = "Забронировать"
    if resp_uid:
        u = UserRepo.get_by_id(resp_uid)
        if u:
            _iid, _tid, _uname, _phone, _first, _last = u
            if _uname:
                label = f"@{_uname}"
            elif _first or _last:
                label = f"{(_first or '').strip()} {(_last or '').strip()}".strip()
            else:
                label = str(_tid)
        else:
            label = str(resp_uid)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text=label, callback_data=f"evt_book_toggle:{eid_i}:{gid_i}"))
    # Build text per spec with responsible line
    lines = [
        f"Напоминание по мероприятию \"{name}\".",
        f"{format_event_time_display(time_str)}",
    ]
    if resp_uid:
        lines.append(f"Ответственный - {label}")
    else:
        lines.append("Ответственный еще не назначен")
    text = "\n".join(lines)
    # Send to group chat
    try:
        await bot.send_message(int(chat_id), text, reply_markup=kb.as_markup())
    except Exception:
        try:
            await bot.send_message(chat_id, text, reply_markup=kb.as_markup())
        except Exception:
            pass
    # DM to responsible
    if resp_uid:
        u = UserRepo.get_by_id(resp_uid)
        if u:
            _iid, _tid, _uname, _phone, _first, _last = u
            try:
                await bot.send_message(_tid, f"Напоминание: {name} — {format_event_time_display(time_str)}. Вы указаны ответственным.")
            except Exception:
                pass
    await callback.answer("Оповещение отправлено")

    # If this action happened in the private event card, refresh the card to show "❌ Убрать ответственного"
    try:
        if getattr(callback.message.chat, 'type', None) == 'private':
            # rebuild event card similar to cb_event_open
            ev_full = EventRepo.get_by_id(eid_i)
            if ev_full:
                _id3, name3, time_str3, group_id3, resp_uid3 = ev_full
                # Resolve current user internal id
                urow2 = UserRepo.get_by_telegram_id(callback.from_user.id)
                internal_user_id2 = urow2[0] if urow2 else None
                kb2 = InlineKeyboardBuilder()
                role2 = RoleRepo.get_user_role(internal_user_id2, gid_i) if internal_user_id2 is not None else None
                if role2 in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID:
                    kb2.row(
                        types.InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"evt_rename:{eid_i}:{gid_i}"),
                        types.InlineKeyboardButton(text="🕒 Изм. дату/время", callback_data=f"evt_retime:{eid_i}:{gid_i}")
                    )
                    kb2.row(
                        types.InlineKeyboardButton(text="Назначить ответственного", callback_data=f"evt_assign:{eid_i}:{gid_i}"),
                        types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"evt_delete:{eid_i}:{gid_i}")
                    )
                if not resp_uid3:
                    kb2.row(types.InlineKeyboardButton(text="Забронировать", callback_data=f"evt_book_toggle:{eid_i}:{gid_i}"))
                else:
                    if internal_user_id2 is not None and (internal_user_id2 == resp_uid3 or role2 in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID):
                        kb2.row(types.InlineKeyboardButton(text="❌ Убрать ответственного", callback_data=f"evt_unassign:{eid_i}:{gid_i}"))
                if internal_user_id2 and can_edit_event_notifications(internal_user_id2, eid_i):
                    kb2.row(types.InlineKeyboardButton(text="🔔 Групповые оповещения", callback_data=f"evt_notifications:{eid_i}:{gid_i}"))
                # Personal notifications for responsible OR owner/admin/superadmin
                if internal_user_id2 and (
                    (resp_uid3 and internal_user_id2 == resp_uid3)
                    or (role2 in ("owner", "admin") or callback.from_user.id == SUPERADMIN_ID)
                ):
                    kb2.row(types.InlineKeyboardButton(text="📱 Личные оповещения", callback_data=f"evt_personal_notifications:{eid_i}:{gid_i}"))
                kb2.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_events:{gid_i}"))
                # Prepare display text
                if resp_uid3:
                    u3 = UserRepo.get_by_id(resp_uid3)
                    if u3:
                        _iid3, _tid3, _uname3, _phone3, _first3, _last3 = u3
                        if _uname3:
                            resp_text3 = f"@{_uname3}"
                        elif _first3 or _last3:
                            resp_text3 = f"{(_first3 or '').strip()} {(_last3 or '').strip()}".strip()
                        else:
                            resp_text3 = str(_tid3)
                    else:
                        resp_text3 = str(resp_uid3)
                else:
                    resp_text3 = 'не назначен'
                text3 = f"{name3}\nВремя: {format_event_time_display(time_str3)}\nОтветственный: {resp_text3}"
                await set_menu_message(callback.from_user.id, callback.message.chat.id, text3, kb2.as_markup())
    except Exception:
        pass

@dp.callback_query(lambda c: c.data and c.data.startswith('grp_menu:'))
async def cb_group_menu(callback: types.CallbackQuery):
    gid = int(callback.data.split(':', 1)[1])
    await callback.answer()
    # callback.from_user.id — это Telegram ID, нужно получить внутренний user_id
    urow = UserRepo.get_by_telegram_id(callback.from_user.id)
    internal_user_id = urow[0] if urow else None
    role = RoleRepo.get_user_role(internal_user_id, gid) if internal_user_id is not None else None
    group = GroupRepo.get_by_id(gid)
    title = group[2] if group else f"Группа {gid}"
    role_ru = ROLE_RU.get(role or 'member', role or 'member')

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="Мероприятия", callback_data=f"grp_events:{gid}")
    if role == "owner":
        kb.button(text="Оповещения", callback_data=f"grp_notifies:{gid}")
        kb.button(text="Администраторы", callback_data=f"grp_admins:{gid}")
        kb.button(text="Напомнить", callback_data=f"grp_remind:{gid}")
    kb.adjust(2)
    await set_menu_message(callback.from_user.id, callback.message.chat.id, f"{title} (ID {gid})\nРоль - {role_ru}", kb.as_markup())

@dp.callback_query(lambda c: c.data and c.data.startswith('grp_admins:'))
async def cb_group_admins(callback: types.CallbackQuery):
    gid = int(callback.data.split(':', 1)[1])
    await callback.answer()
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    admins = GroupRepo.list_group_admins(gid)
    pending = RoleRepo.list_pending_admins(gid)
    lines = [f"Администраторы группы {gid}"]
    if admins:
        lines.append("Подтвержденные:")
        for uid, tid, uname in admins:
            uname_disp = f"@{uname}" if uname else str(tid)
            kb.row(
                types.InlineKeyboardButton(text=uname_disp, callback_data="noop"),
                types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"adm_del:{uid}:{gid}")
            )
    else:
        kb.row(types.InlineKeyboardButton(text="Нет подтвержденных админов", callback_data="noop"))
    if pending:
        lines.append("")
        lines.append("Ожидающие подтверждения:")
        for pid, ident, ident_type, created_by, created_at in pending:
            kb.row(
                types.InlineKeyboardButton(text=f"{ident_type}: {ident}", callback_data="noop"),
                types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"padm_del:{pid}:{gid}")
            )
    # Add controls
    kb.row(types.InlineKeyboardButton(text="+ Добавить по ID", callback_data=f"adm_add_id:{gid}"))
    kb.row(types.InlineKeyboardButton(text="+ Добавить по @username", callback_data=f"adm_add_username:{gid}"))
    kb.row(types.InlineKeyboardButton(text="+ Добавить по телефону", callback_data=f"adm_add_phone:{gid}"))
    kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_menu:{gid}"))
    await set_menu_message(callback.from_user.id, callback.message.chat.id, "\n".join(lines), kb.as_markup())

@dp.callback_query(lambda c: c.data and (c.data.startswith('adm_add_id:') or c.data.startswith('adm_add_username:') or c.data.startswith('adm_add_phone:')))
async def cb_admin_add_prompt(callback: types.CallbackQuery):
    await callback.answer()
    data = callback.data
    if data.startswith('adm_add_id:'):
        _, gid = data.split(':')
        what = ('id', 'введите ID Telegram пользователя (будет добавлен как ожидающий)')
    elif data.startswith('adm_add_username:'):
        _, gid = data.split(':')
        what = ('username', 'введите @username пользователя')
    else:
        _, gid = data.split(':')
        what = ('phone', 'введите телефон пользователя (в любом формате)')
    gid_i = int(gid)
    prompt = await callback.message.answer(f"Добавление администратора: {what[1]}")
    AWAITING_ADMIN_INPUT[callback.from_user.id] = {
        'gid': gid_i,
        'mode': 'add',
        'type': what[0],
        'edit_chat_id': callback.message.chat.id,
        'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }

@dp.callback_query(lambda c: c.data and c.data.startswith('adm_del:'))
async def cb_admin_delete(callback: types.CallbackQuery):
    _, uid, gid = callback.data.split(':')
    RoleRepo.remove_admin(int(uid), int(gid))
    await callback.answer("Удалено")
    # refresh view without constructing fake CallbackQuery
    header, markup = build_admins_ui(int(gid))
    await set_menu_message(callback.from_user.id, callback.message.chat.id, header, markup)

@dp.callback_query(lambda c: c.data and c.data.startswith('padm_del:'))
async def cb_pending_admin_delete(callback: types.CallbackQuery):
    _, pid, gid = callback.data.split(':')
    RoleRepo.delete_pending(int(pid))
    await callback.answer("Удалено")
    # refresh view without constructing fake CallbackQuery
    header, markup = build_admins_ui(int(gid))
    await set_menu_message(callback.from_user.id, callback.message.chat.id, header, markup)


# Notification actions
@dp.callback_query(lambda c: c.data and c.data.startswith('notif_add:'))
async def cb_notif_add(callback: types.CallbackQuery):
    _, gid, amount, unit = callback.data.split(':')
    gid_i = int(gid)
    amount_i = int(amount)
    # Add without custom message (None) and not default
    NotificationRepo.add_notification(gid_i, amount_i, unit, None, is_default=0)
    await callback.answer("Добавлено")
    # Refresh view in place
    header, markup = build_notifies_ui(gid_i)
    await set_menu_message(callback.from_user.id, callback.message.chat.id, header, markup)


@dp.callback_query(lambda c: c.data and c.data.startswith('notif_del:'))
async def cb_notif_del(callback: types.CallbackQuery):
    _, notif_id, gid = callback.data.split(':')
    NotificationRepo.delete_notification(int(notif_id))
    await callback.answer("Удалено")
    # Refresh view in place
    header, markup = build_notifies_ui(int(gid))
    await set_menu_message(callback.from_user.id, callback.message.chat.id, header, markup)


@dp.callback_query(lambda c: c.data and c.data.startswith('notif_add_free:'))
async def cb_notif_add_free(callback: types.CallbackQuery):
    _, gid = callback.data.split(':')
    gid_i = int(gid)
    await callback.answer()
    prompt = await callback.message.answer(
        "Введите срок до мероприятия (пример: '1 неделя', '2 дня', '1 день и 3 часа', '40 минут', '1 час 6 минут')"
    )
    AWAITING_NOTIF_ADD[callback.from_user.id] = {
        'gid': gid_i,
        'edit_chat_id': callback.message.chat.id,
        'edit_message_id': callback.message.message_id,
        'prompt_message_id': prompt.message_id,
    }


@dp.message()
async def on_freeform_input(message: types.Message):
    # Обрабатываем произвольный ввод для добавления оповещений
    uid = message.from_user.id
    # 1) Ожидание произвольного срока для уведомлений
    ctx = AWAITING_NOTIF_ADD.get(uid)
    if ctx is not None:
        text = (message.text or '').strip()
        minutes = parse_duration_ru(text)
        if minutes <= 0:
            await message.answer("Не удалось распознать срок. Попробуйте снова, например: '2 дня', '1 день 3 часа', '45 минут'")
            return
        gid = ctx['gid']
        NotificationRepo.add_notification(gid, minutes, 'minutes', None, is_default=0)
        # Обновляем исходное сообщение с меню
        header, markup = build_notifies_ui(gid)
        try:
            await bot.edit_message_text(header, chat_id=ctx['edit_chat_id'], message_id=ctx['edit_message_id'], reply_markup=markup)
        except Exception:
            pass
        # Удаляем подсказку и ввод пользователя
        try:
            await bot.delete_message(chat_id=ctx['edit_chat_id'], message_id=ctx['prompt_message_id'])
        except Exception:
            pass
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass
        # Очистить ожидание
        AWAITING_NOTIF_ADD.pop(uid, None)
        return

    # 2) Ожидания для создания/назначения мероприятия
    ectx = AWAITING_EVENT_CREATE.get(uid)
    if ectx is not None:
        mode = ectx.get('mode')
        if mode == 'create':
            step = ectx.get('step')
            if step == 'name':
                ectx['name'] = (message.text or '').strip()
                ectx['step'] = 'time'
                # rewrite prompt to next step and delete user's message
                try:
                    await bot.edit_message_text("Введите дату/время мероприятия (свободный формат)", chat_id=ectx['edit_chat_id'], message_id=ectx['prompt_message_id'])
                except Exception:
                    pass
                try:
                    await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
                except Exception:
                    pass
                return
            elif step == 'time':
                raw_time = (message.text or '').strip()
                gid = ectx['gid']
                # parse russian datetime
                dt = parse_ru_datetime(raw_time)
                if not dt:
                    # keep prompt, ask again
                    try:
                        await bot.edit_message_text("Не понял дату/время. Примеры: '22 сентября 8 утра', '22/09/2025 11 часов'", chat_id=ectx['edit_chat_id'], message_id=ectx['prompt_message_id'])
                    except Exception:
                        pass
                    try:
                        await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
                    except Exception:
                        pass
                    return
                time_store = dt.strftime('%Y-%m-%d %H:%M:%S')
                event_id = EventRepo.create(gid, ectx.get('name','Без названия'), time_store)
                # Auto-create event notifications based on group settings
                EventNotificationRepo.create_from_group_defaults(event_id, gid)
                # refresh events list
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                events = EventRepo.list_by_group(gid)
                kb = InlineKeyboardBuilder()
                lines = [f"Мероприятия (ID группы {gid})"]
                if events:
                    for eid, name, time_str, resp_uid in events:
                        # format time nicely
                        time_disp = format_event_time_display(time_str)
                        if resp_uid:
                            u = UserRepo.get_by_id(resp_uid)
                            if u:
                                _iid, _tid, _uname, _phone, _first, _last = u
                                if _uname:
                                    who = f"ответственный: @{_uname}"
                                elif _first or _last:
                                    who = f"ответственный: {(_first or '').strip()} {(_last or '').strip()}".strip()
                                else:
                                    who = f"ответственный: {_tid}"
                            else:
                                who = f"ответственный: {resp_uid}"
                        else:
                            who = "без ответственного"
                        lines.append(f"• {name}\n{time_disp} | {who}")
                        kb.button(text=f"Открыть: {name}", callback_data=f"evt_open:{eid}:{gid}")
                    kb.adjust(1)
                else:
                    lines.append("Пока нет мероприятий")
                kb.row(types.InlineKeyboardButton(text="+ Создать", callback_data=f"evt_create:{gid}"))
                kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_menu:{gid}"))
                try:
                    await bot.edit_message_text("\n".join(lines), chat_id=ectx['edit_chat_id'], message_id=ectx['edit_message_id'], reply_markup=kb.as_markup())
                except Exception:
                    pass
                # Cleanup prompt and user input
                try:
                    await bot.delete_message(chat_id=ectx['edit_chat_id'], message_id=ectx['prompt_message_id'])
                except Exception:
                    pass
                try:
                    await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
                except Exception:
                    pass
                AWAITING_EVENT_CREATE.pop(uid, None)
                return
        elif mode == 'assign':
            val = (message.text or '').strip()
            user_row = None
            if val.startswith('@'):
                user_row = UserRepo.get_by_username(val)
            else:
                try:
                    tid = int(val)
                except ValueError:
                    tid = None
                if tid is not None:
                    user_row = UserRepo.get_by_telegram_id(tid)
            if not user_row:
                # delete user's input and update prompt to retry
                try:
                    await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
                except Exception:
                    pass
                try:
                    await bot.edit_message_text("Пользователь не найден. Введите @username или ID Telegram ещё раз", chat_id=ectx['edit_chat_id'], message_id=ectx['prompt_message_id'])
                except Exception:
                    pass
                return
            user_id_internal = user_row[0]
            EventRepo.set_responsible(ectx['eid'], user_id_internal)
            # refresh event view
            ev = EventRepo.get_by_id(ectx['eid'])
            if ev:
                _id, name, time_str, group_id, resp_uid = ev
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                kb = InlineKeyboardBuilder()
                # Role of current user
                urow2 = UserRepo.get_by_telegram_id(message.from_user.id)
                internal_user_id2 = urow2[0] if urow2 else None
                role2 = RoleRepo.get_user_role(internal_user_id2, group_id) if internal_user_id2 is not None else None
                # Admin controls
                if role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID:
                    kb.row(
                        types.InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"evt_rename:{_id}:{group_id}"),
                        types.InlineKeyboardButton(text="🕒 Изм. дату/время", callback_data=f"evt_retime:{_id}:{group_id}")
                    )
                    kb.row(
                        types.InlineKeyboardButton(text="Назначить ответственного", callback_data=f"evt_assign:{_id}:{group_id}"),
                        types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"evt_delete:{_id}:{group_id}")
                    )
                # Booking controls
                if not resp_uid:
                    kb.row(types.InlineKeyboardButton(text="Забронировать", callback_data=f"evt_book_toggle:{_id}:{group_id}"))
                else:
                    if internal_user_id2 is not None and (internal_user_id2 == resp_uid or role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID):
                        kb.row(types.InlineKeyboardButton(text="❌ Убрать ответственного", callback_data=f"evt_unassign:{_id}:{group_id}"))
                # Group notifications for admins
                if internal_user_id2 and can_edit_event_notifications(internal_user_id2, _id):
                    kb.row(types.InlineKeyboardButton(text="🔔 Групповые оповещения", callback_data=f"evt_notifications:{_id}:{group_id}"))
                # Personal notifications for responsible OR owner/admin/superadmin
                if internal_user_id2 and (
                    (resp_uid and internal_user_id2 == resp_uid)
                    or (role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID)
                ):
                    kb.row(types.InlineKeyboardButton(text="📱 Личные оповещения", callback_data=f"evt_personal_notifications:{_id}:{group_id}"))
                kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_events:{group_id}"))
                if resp_uid:
                    u = UserRepo.get_by_id(resp_uid)
                    if u:
                        _iid, _tid, _uname, _phone, _first, _last = u
                        if _uname:
                            resp_text2 = f"@{_uname}"
                        elif _first or _last:
                            resp_text2 = f"{(_first or '').strip()} {(_last or '').strip()}".strip()
                        else:
                            resp_text2 = str(_tid)
                    else:
                        resp_text2 = str(resp_uid)
                else:
                    resp_text2 = 'не назначен'
                text = f"{name}\nВремя: {time_str}\nОтветственный: {resp_text2}"
                try:
                    await bot.edit_message_text(text, chat_id=ectx['edit_chat_id'], message_id=ectx['edit_message_id'], reply_markup=kb.as_markup())
                except Exception:
                    pass
            # Cleanup prompt and user input
            try:
                await bot.delete_message(chat_id=ectx['edit_chat_id'], message_id=ectx['prompt_message_id'])
            except Exception:
                pass
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            except Exception:
                pass
            AWAITING_EVENT_CREATE.pop(uid, None)
            return

    # 3) Ожидания для добавления администраторов
    actx = AWAITING_ADMIN_INPUT.get(uid)
    if actx is not None:
        gid = actx['gid']
        mode = actx['mode']
        if mode == 'add':
            id_type = actx['type']
            value = (message.text or '').strip()
            if id_type == 'id':
                try:
                    int(value)
                except ValueError:
                    await message.answer("Ожидался числовой ID")
                    return
                # Всегда добавляем как ожидающего, даже если пользователя нет в базе
                creator_row = UserRepo.get_by_telegram_id(uid)
                created_by = creator_row[0] if creator_row else None
                RoleRepo.add_pending_admin(gid, value, 'id', created_by_user=created_by or 0)
            elif id_type == 'username':
                if not value.startswith('@'):
                    await message.answer("Ожидался @username")
                    return
                creator_row = UserRepo.get_by_telegram_id(uid)
                created_by = creator_row[0] if creator_row else None
                RoleRepo.add_pending_admin(gid, value.lstrip('@'), 'username', created_by_user=created_by or 0)
            else:
                creator_row = UserRepo.get_by_telegram_id(uid)
                created_by = creator_row[0] if creator_row else None
                RoleRepo.add_pending_admin(gid, value, 'phone', created_by_user=created_by or 0)
            # refresh admins view
            try:
                await bot.delete_message(chat_id=actx['edit_chat_id'], message_id=actx['prompt_message_id'])
            except Exception:
                pass
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            except Exception:
                pass
            # show updated admins without constructing fake Message/CallbackQuery
            header, markup = build_admins_ui(gid)
            try:
                await bot.edit_message_text(header, chat_id=actx['edit_chat_id'], message_id=actx['edit_message_id'], reply_markup=markup)
            except Exception:
                pass
            AWAITING_ADMIN_INPUT.pop(uid, None)
            return

    # 4) Обработка контакта для подтверждения по телефону
    if message.contact and message.from_user and message.contact.user_id == message.from_user.id:
        phone = message.contact.phone_number
        # Обновим телефон юзера и попробуем подтвердить доступы
        urow = UserRepo.get_by_telegram_id(message.from_user.id)
        if urow:
            UserRepo.update_phone(urow[0], phone)
            groups = RoleRepo.find_groups_for_pending(telegram_id=None, username=None, phone=phone)
            for gid in groups:
                RoleRepo.confirm_admin_if_pending(urow[0], gid)
            if groups:
                await message.answer(
                    f"Телефон получен. Ваш доступ админа подтвержден в группах: {', '.join(map(str, groups))}",
                    reply_markup=ReplyKeyboardRemove(),
                )
            else:
                await message.answer("Телефон получен, но неподтвержденных доступов не найдено.", reply_markup=ReplyKeyboardRemove())
        else:
            await message.answer("Спасибо, получили телефон.", reply_markup=ReplyKeyboardRemove())

    # 5) Ожидания редактирования мероприятия (переименование / изменение времени)
    eedit = AWAITING_EVENT_EDIT.get(uid)
    if eedit is not None:
        mode = eedit['mode']
        eid = eedit['eid']
        gid = eedit['gid']
        if mode == 'rename':
            new_name = (message.text or '').strip()
            if not new_name:
                try:
                    await bot.edit_message_text("Название не может быть пустым. Введите новое название.", chat_id=eedit['edit_chat_id'], message_id=eedit['prompt_message_id'])
                except Exception:
                    pass
                try:
                    await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
                except Exception:
                    pass
                return
            EventRepo.update_name(eid, new_name)
            # refresh card directly
            ev = EventRepo.get_by_id(eid)
            if ev:
                _id, name, time_str, group_id, resp_uid = ev
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                kb = InlineKeyboardBuilder()
                # Role of current user
                urow2 = UserRepo.get_by_telegram_id(message.from_user.id)
                internal_user_id2 = urow2[0] if urow2 else None
                role2 = RoleRepo.get_user_role(internal_user_id2, gid) if internal_user_id2 is not None else None
                # Admin controls
                if role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID:
                    kb.row(
                        types.InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"evt_rename:{eid}:{gid}"),
                        types.InlineKeyboardButton(text="🕒 Изм. дату/время", callback_data=f"evt_retime:{eid}:{gid}")
                    )
                    kb.row(
                        types.InlineKeyboardButton(text="Назначить ответственного", callback_data=f"evt_assign:{eid}:{gid}"),
                        types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"evt_delete:{eid}:{gid}")
                    )
                # Booking controls
                if not resp_uid:
                    kb.row(types.InlineKeyboardButton(text="Забронировать", callback_data=f"evt_book_toggle:{eid}:{gid}"))
                else:
                    if internal_user_id2 is not None and (internal_user_id2 == resp_uid or role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID):
                        kb.row(types.InlineKeyboardButton(text="❌ Убрать ответственного", callback_data=f"evt_unassign:{eid}:{gid}"))
                # Group notifications for admins
                if internal_user_id2 and can_edit_event_notifications(internal_user_id2, eid):
                    kb.row(types.InlineKeyboardButton(text="🔔 Групповые оповещения", callback_data=f"evt_notifications:{eid}:{gid}"))
                # Personal notifications for responsible OR owner/admin/superadmin
                if internal_user_id2 and (
                    (resp_uid and internal_user_id2 == resp_uid)
                    or (role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID)
                ):
                    kb.row(types.InlineKeyboardButton(text="📱 Личные оповещения", callback_data=f"evt_personal_notifications:{eid}:{gid}"))
                kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_events:{gid}"))
                # Personal notifications for responsible OR owner/admin/superadmin
                urow2 = UserRepo.get_by_telegram_id(message.from_user.id)
                internal_user_id2 = urow2[0] if urow2 else None
                role2 = RoleRepo.get_user_role(internal_user_id2, gid) if internal_user_id2 is not None else None
                if internal_user_id2 and (
                    (resp_uid and internal_user_id2 == resp_uid)
                    or (role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID)
                ):
                    kb.row(types.InlineKeyboardButton(text="📱 Личные оповещения", callback_data=f"evt_personal_notifications:{eid}:{gid}"))
                # Personal notifications for responsible OR owner/admin/superadmin
                urow2 = UserRepo.get_by_telegram_id(message.from_user.id)
                internal_user_id2 = urow2[0] if urow2 else None
                role2 = RoleRepo.get_user_role(internal_user_id2, gid) if internal_user_id2 is not None else None
                if internal_user_id2 and (
                    (resp_uid and internal_user_id2 == resp_uid)
                    or (role2 in ("owner", "admin") or message.from_user.id == SUPERADMIN_ID)
                ):
                    kb.row(types.InlineKeyboardButton(text="📱 Личные оповещения", callback_data=f"evt_personal_notifications:{eid}:{gid}"))
                if resp_uid:
                    u = UserRepo.get_by_id(resp_uid)
                    if u:
                        _iid, _tid, _uname, _phone, _first, _last = u
                        if _uname:
                            resp_text = f"@{_uname}"
                        elif _first or _last:
                            resp_text = f"{(_first or '').strip()} {(_last or '').strip()}".strip()
                        else:
                            resp_text = str(_tid)
                    else:
                        resp_text = str(resp_uid)
                else:
                    resp_text = 'не назначен'
                text = f"{name}\nВремя: {format_event_time_display(time_str)}\nОтветственный: {resp_text}"
                try:
                    await bot.edit_message_text(text, chat_id=eedit['edit_chat_id'], message_id=eedit['edit_message_id'], reply_markup=kb.as_markup())
                except Exception:
                    pass
            # cleanup
            try:
                await bot.delete_message(chat_id=eedit['edit_chat_id'], message_id=eedit['prompt_message_id'])
            except Exception:
                pass
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            except Exception:
                pass
            AWAITING_EVENT_EDIT.pop(uid, None)
            return
        elif mode == 'retime':
            raw = (message.text or '').strip()
            # Try explicit formats first
            dt = None
            for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    dt = datetime.strptime(raw, fmt)
                    break
                except Exception:
                    pass
            if not dt:
                dt = parse_ru_datetime(raw)
            if not dt:
                try:
                    await bot.edit_message_text("Не понял дату/время. Примеры: '22 сентября 8 утра', '15.09.2025 00:00'", chat_id=eedit['edit_chat_id'], message_id=eedit['prompt_message_id'])
                except Exception:
                    pass
                try:
                    await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
                except Exception:
                    pass
                return
            EventRepo.update_time(eid, dt.strftime('%Y-%m-%d %H:%M:%S'))
            # refresh card directly
            ev = EventRepo.get_by_id(eid)
            if ev:
                _id, name, time_str, group_id, resp_uid = ev
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                kb = InlineKeyboardBuilder()
                kb.row(
                    types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"evt_delete:{eid}:{gid}"),
                    types.InlineKeyboardButton(text="Назначить ответственного", callback_data=f"evt_assign:{eid}:{gid}")
                )
                kb.row(
                    types.InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"evt_rename:{eid}:{gid}"),
                    types.InlineKeyboardButton(text="🕒 Изм. дату/время", callback_data=f"evt_retime:{eid}:{gid}")
                )
                if resp_uid:
                    kb.row(
                        types.InlineKeyboardButton(text="❌ Убрать ответственного", callback_data=f"evt_unassign:{eid}:{gid}")
                    )
                kb.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"grp_events:{gid}"))
                if resp_uid:
                    u = UserRepo.get_by_id(resp_uid)
                    if u:
                        _iid, _tid, _uname, _phone, _first, _last = u
                        if _uname:
                            resp_text = f"@{_uname}"
                        elif _first or _last:
                            resp_text = f"{(_first or '').strip()} {(_last or '').strip()}".strip()
                        else:
                            resp_text = str(_tid)
                    else:
                        resp_text = str(resp_uid)
                else:
                    resp_text = 'не назначен'
                text = f"{name}\nВремя: {format_event_time_display(time_str)}\nОтветственный: {resp_text}"
                try:
                    await bot.edit_message_text(text, chat_id=eedit['edit_chat_id'], message_id=eedit['edit_message_id'], reply_markup=kb.as_markup())
                except Exception:
                    pass
            # cleanup
            try:
                await bot.delete_message(chat_id=eedit['edit_chat_id'], message_id=eedit['prompt_message_id'])
            except Exception:
                pass
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            except Exception:
                pass
            AWAITING_EVENT_EDIT.pop(uid, None)
            return

    # 4) Ожидания для добавления оповещений мероприятия
    enctx = AWAITING_EVENT_NOTIF.get(uid)
    if enctx is not None:
        text = (message.text or '').strip()
        # Try relative duration first
        minutes = parse_duration_ru(text)
        if minutes <= 0:
            # Try absolute date/time of notification
            eid = enctx['eid']
            gid = enctx['gid']
            ev = EventRepo.get_by_id(eid)
            if not ev:
                await message.answer("Мероприятие не найдено")
                return
            _id, _name, time_str, _group_id, _resp = ev
            nt = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
                try:
                    nt = datetime.strptime(text, fmt)
                    break
                except Exception:
                    pass
            if nt is None:
                nt = dateparser.parse(text, languages=["ru"], settings={'DATE_ORDER': 'DMY'})
            if nt is None:
                await message.answer("Не удалось распознать срок. Укажите, например: '2 дня' или '18.09.2025 22:30'")
                return
            # parse event time to compute minutes before
            evt_dt = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
                try:
                    evt_dt = datetime.strptime(time_str, fmt)
                    break
                except Exception:
                    pass
            if evt_dt is None:
                evt_dt = dateparser.parse(time_str, languages=["ru"], settings={'DATE_ORDER': 'DMY'})
            if evt_dt is None:
                await message.answer("Не удалось распознать время мероприятия")
                return
            delta = evt_dt - nt
            minutes = int(delta.total_seconds() // 60)
            if minutes <= 0:
                await message.answer("Время оповещения должно быть раньше времени мероприятия")
                return
        eid = enctx['eid']
        gid = enctx['gid']
        EventNotificationRepo.add_notification(eid, minutes, 'minutes', f"Через {format_duration_ru(minutes, 'minutes')} начало мероприятия")
        # Refresh notifications view (no fake CallbackQuery)
        urow = UserRepo.get_by_telegram_id(uid)
        internal_user_id = urow[0] if urow else None
        if internal_user_id:
            await refresh_event_notifications_view_ids(enctx['edit_chat_id'], enctx['edit_message_id'], eid, gid, internal_user_id)
        # Cleanup prompt and user input
        try:
            await bot.delete_message(chat_id=enctx['edit_chat_id'], message_id=enctx['prompt_message_id'])
        except Exception:
            pass
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass
        AWAITING_EVENT_NOTIF.pop(uid, None)
        return

    # 5) Ожидания для добавления личных напоминаний о мероприятии
    pnctx = AWAITING_PERSONAL_NOTIF.get(uid)
    if pnctx is not None:
        text = (message.text or '').strip()
        minutes = parse_duration_ru(text)
        if minutes <= 0:
            await message.answer("Не удалось распознать срок. Попробуйте снова, например: '2 дня', '1 день 3 часа', '45 минут'")
            return
        eid = pnctx['eid']
        gid = pnctx['gid']
        
        # Get user
        urow = UserRepo.get_by_telegram_id(uid)
        if not urow:
            await message.answer("Пользователь не найден")
            return
        internal_user_id = urow[0]
        
        PersonalEventNotificationRepo.add_notification(internal_user_id, eid, minutes, 'minutes', f"Через {format_duration_ru(minutes, 'minutes')} начало мероприятия")
        # Refresh personal notifications view (no fake CallbackQuery)
        await refresh_personal_notifications_view_ids(pnctx['edit_chat_id'], pnctx['edit_message_id'], eid, gid, internal_user_id)
        # Cleanup prompt and user input
        try:
            await bot.delete_message(chat_id=pnctx['edit_chat_id'], message_id=pnctx['prompt_message_id'])
        except Exception:
            pass
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
        except Exception:
            pass
        AWAITING_PERSONAL_NOTIF.pop(uid, None)
        return


def _register_group_if_needed_from_message(message: types.Message) -> None:
    if message.chat.type not in ("group", "supergroup"):
        return
    chat_id = str(message.chat.id)
    title = message.chat.title or 'Без названия'
    existing = GroupRepo.get_by_chat_id(chat_id)
    if existing:
        return
    user = message.from_user
    owner_user_id = UserRepo.upsert_user(
        telegram_id=user.id,
        username=user.username,
        phone=None,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    group_id = GroupRepo.create(chat_id, title, owner_user_id)
    RoleRepo.add_role(owner_user_id, group_id, 'owner', confirmed=True)
    # Add superadmin to the group as superadmin
    try:
        sa_row = UserRepo.get_by_telegram_id(SUPERADMIN_ID)
        if sa_row:
            RoleRepo.add_role(sa_row[0], group_id, 'superadmin', confirmed=True)
    except Exception:
        pass
    NotificationRepo.ensure_defaults(group_id)
    logging.info(f"Auto-registered group {chat_id} by message from user {user.id}")

# --- Generic logging handlers + auto-register on first message ---
@dp.message()
async def log_any_message(message: types.Message):
    logging.info(
        f"message: chat_type={message.chat.type}, chat_id={message.chat.id}, user_id={message.from_user.id}, "
        f"username={message.from_user.username}, text={message.text!r}"
    )
    try:
        _register_group_if_needed_from_message(message)
    except Exception as e:
        logging.exception(f"Failed to auto-register group from message: {e}")

@dp.chat_member()
async def log_chat_member(event: ChatMemberUpdated):
    logging.info(
        f"chat_member: chat_id={event.chat.id}, actor_id={getattr(event.from_user,'id',None)}, "
        f"old_status={getattr(event.old_chat_member,'status',None)}, new_status={event.new_chat_member.status}"
    )

@dp.callback_query()
async def log_callback(callback: types.CallbackQuery):
    logging.info(
        f"callback: chat_id={callback.message.chat.id if callback.message else None}, user_id={callback.from_user.id}, data={callback.data}"
    )


async def main():
    from database.init_db import init_db
    init_db()
    # Start scheduler for notifications
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
    except Exception:
        # APScheduler might not be installed; skip silently
        await dp.start_polling(bot)
        return

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    async def tick_send_due():
        msk = ZoneInfo("Europe/Moscow")
        now = datetime.now(msk)
        now_iso = now.strftime('%Y-%m-%d %H:%M:%S')
        # Scan upcoming events in next 2 days to compute due notifications
        # (simple approach; can be optimized with SQL later)
        for gid_row in range(1):
            pass
        # Load all events
        # We don't have a repo to list all events; reuse groups and list_by_group
        # Iterate groups
        groups = []
        try:
            # Fetch all groups
            from services.repositories import get_conn
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, telegram_chat_id FROM groups")
                groups = cur.fetchall()
        except Exception:
            groups = []
        for gid, chat_id in groups:
            events = EventRepo.list_by_group(gid)
            for eid, name, time_str, resp_uid in events:
                # Parse event time (support with and without seconds)
                evt_dt = None
                for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                    try:
                        evt_dt = datetime.strptime(time_str, fmt).replace(tzinfo=msk)
                        break
                    except Exception:
                        pass
                if evt_dt is None:
                    try:
                        evt_dt = datetime.fromisoformat(time_str).replace(tzinfo=msk)
                    except Exception:
                        # Skip if cannot parse
                        continue
                # Event notifications (group-level sends)
                for _, time_before, time_unit, message_text in EventNotificationRepo.list_by_event(eid):
                    # compute notify time
                    delta_minutes = 0
                    if time_unit == 'minutes':
                        delta_minutes = time_before
                    elif time_unit == 'hours':
                        delta_minutes = time_before * 60
                    elif time_unit == 'days':
                        delta_minutes = time_before * 1440
                    elif time_unit == 'weeks':
                        delta_minutes = time_before * 10080
                    elif time_unit == 'months':
                        delta_minutes = time_before * 43200
                    notify_dt = evt_dt - timedelta(minutes=delta_minutes)
                    # if it's due within the last minute window
                    if notify_dt <= now < (notify_dt + timedelta(minutes=1)):
                        # Debug log
                        print(f"[TICK] Group due: gid={gid}, eid={eid}, notify={notify_dt}, now={now}, tb={time_before}{time_unit}")
                        if not DispatchLogRepo.was_sent('event', user_id=None, group_id=gid, event_id=eid, time_before=time_before, time_unit=time_unit):
                            # Build group message per spec
                            # Build roles block (assigned/unassigned)
                            try:
                                from services.repositories import EventRoleRequirementRepo, EventRoleAssignmentRepo, DisplayNameRepo
                                role_reqs = EventRoleRequirementRepo.list_for_event(eid)
                                role_asg = EventRoleAssignmentRepo.list_for_event(eid)
                                asg_map = {}
                                for rname, uid in role_asg:
                                    asg_map.setdefault(rname, []).append(uid)
                                role_lines = []
                                for rname, _req in sorted(role_reqs, key=lambda x: x[0].lower()):
                                    assigned_uids = asg_map.get(rname, [])
                                    if assigned_uids:
                                        names = []
                                        for uid in assigned_uids:
                                            dn = DisplayNameRepo.get_display_name(gid, uid)
                                            if dn:
                                                names.append(dn)
                                            else:
                                                u = UserRepo.get_by_id(uid)
                                                if u and u[2]:
                                                    names.append(f"@{u[2]}")
                                                else:
                                                    names.append(str(uid))
                                        role_lines.append(f"✅ {rname}: {', '.join(names)}")
                                    else:
                                        role_lines.append(f"🟡 {rname}: свободно")
                            except Exception:
                                role_lines = []

                            # Build message
                            lines = [
                                f"📅 Мероприятие: \"{name}\"",
                                f"🕒 {format_event_time_display(time_str)}",
                            ]
                            if role_lines:
                                lines.append("")
                                lines.append("Роли:")
                                lines.extend(role_lines)
                            else:
                                lines.append("")
                                lines.append("Роли не заданы")
                            if message_text:
                                lines.append("")
                                lines.append(str(message_text))
                            text = "\n".join(lines)
                            # Build inline keyboard with per-role actions
                            from aiogram.utils.keyboard import InlineKeyboardBuilder
                            kb_ev = InlineKeyboardBuilder()
                            try:
                                from services.repositories import EventRoleRequirementRepo, EventRoleAssignmentRepo, DisplayNameRepo
                                reqs = EventRoleRequirementRepo.list_for_event(eid)
                                asgs = EventRoleAssignmentRepo.list_for_event(eid)
                                asg_map = {}
                                for r, uid in asgs:
                                    asg_map.setdefault(r, []).append(uid)
                                for rname, _req in sorted(reqs, key=lambda x: x[0].lower()):
                                    assigned = asg_map.get(rname, [])
                                    if assigned:
                                        # Show first assignee name (or count)
                                        uid = assigned[0]
                                        dn = DisplayNameRepo.get_display_name(gid, uid)
                                        label = dn if dn else f"ID:{uid}"
                                        btn_text = f"✅ {rname}: {label}"
                                        # Allow unbook intent; handler will verify ownership
                                        kb_ev.row(types.InlineKeyboardButton(text=btn_text, callback_data=f"role_unbook:{eid}:{gid}:{rname}"))
                                    else:
                                        kb_ev.row(types.InlineKeyboardButton(text=f"🟢 {rname}: Забронировать", callback_data=f"role_book:{eid}:{gid}:{rname}"))
                            except Exception:
                                pass
                            try:
                                await bot.send_message(int(chat_id), text, reply_markup=kb_ev.as_markup())
                            except Exception:
                                try:
                                    await bot.send_message(chat_id, text, reply_markup=kb_ev.as_markup())
                                except Exception:
                                    pass
                            DispatchLogRepo.mark_sent('event', user_id=None, group_id=gid, event_id=eid, time_before=time_before, time_unit=time_unit)
                # Personal notifications (DM to users)
                from services.repositories import get_conn
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT user_id, time_before, time_unit, message_text FROM personal_event_notifications WHERE event_id = ?", (eid,))
                    personals = cur.fetchall()
                for user_id, time_before, time_unit, message_text in personals:
                    # compute notify time
                    delta_minutes = 0
                    if time_unit == 'minutes':
                        delta_minutes = time_before
                    elif time_unit == 'hours':
                        delta_minutes = time_before * 60
                    elif time_unit == 'days':
                        delta_minutes = time_before * 1440
                    elif time_unit == 'weeks':
                        delta_minutes = time_before * 10080
                    elif time_unit == 'months':
                        delta_minutes = time_before * 43200
                    notify_dt = evt_dt - timedelta(minutes=delta_minutes)
                    if notify_dt <= now < (notify_dt + timedelta(minutes=1)):
                        print(f"[TICK] Personal due: eid={eid}, uid={user_id}, notify={notify_dt}, now={now}, tb={time_before}{time_unit}")
                        if not DispatchLogRepo.was_sent('personal', user_id=user_id, group_id=None, event_id=eid, time_before=time_before, time_unit=time_unit):
                            u = UserRepo.get_by_id(user_id)
                            if u:
                                _iid, _tid, _uname, _phone, _first, _last = u
                                # Build personal message per spec
                                grp_row = GroupRepo.get_by_id(gid)
                                group_title = grp_row[2] if grp_row else f"Группа {gid}"
                                # Find user's roles for this event
                                try:
                                    from services.repositories import EventRoleAssignmentRepo
                                    user_roles = [r for r, uid in EventRoleAssignmentRepo.list_for_event(eid) if uid == user_id]
                                except Exception:
                                    user_roles = []
                                role_info = f"Роли: {', '.join(user_roles)}" if user_roles else "Роль не указана"
                                lines = [
                                    f"🔔 Личное напоминание в группе \"{group_title}\"",
                                    f"📅 Мероприятие: \"{name}\"",
                                    f"🕒 {format_event_time_display(time_str)}",
                                    role_info,
                                ]
                                if message_text:
                                    lines.append("")
                                    lines.append(str(message_text))
                                text = "\n".join(lines)
                                try:
                                    await bot.send_message(_tid, text)
                                    DispatchLogRepo.mark_sent('personal', user_id=user_id, group_id=None, event_id=eid, time_before=time_before, time_unit=time_unit)
                                except Exception:
                                    pass

    scheduler.add_job(tick_send_due, 'interval', minutes=1, id='notify_tick')
    scheduler.start()
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
