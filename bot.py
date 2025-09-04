import logging
import asyncio
from pytz import timezone
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, ChatMemberUpdated
from aiogram.enums import ChatMemberStatus
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, timedelta

from config import BOT_TOKEN, SUPERADMIN_ID
from group_utils import GroupManager
from event_utils import EventManager

# Настраиваем бота и логирование
logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Инициализируем менеджеры
group_manager = GroupManager()
event_manager = EventManager()

# Планировщик задач
scheduler = AsyncIOScheduler(timezone=timezone('Europe/Moscow'))

# Получение списка команд
async def get_commands():
    return '''/list_events - Полный список событий
/remind_events - События на месяц, отправляются в рабочий чат
/add_event ДД.ММ.ГГГГ ЧЧ:ММ Название ГРУППА_ID - Добавить событие
/remove_event ID - Удалить событие по ID
/update - Обновить оповещалки после перезагрузки
/my_groups - Показать группы где вы админ
/add_admin ГРУППА_ID ПОЛЬЗОВАТЕЛЬ_ID - Добавить админа в группу
/remove_admin ГРУППА_ID ПОЛЬЗОВАТЕЛЬ_ID - Удалить админа из группы
/register_group - Зарегистрировать текущую группу в системе
/add_group CHAT_ID Название - Добавить группу вручную (только суперадмин)
/help - Напомнить все команды'''

# Проверка прав пользователя
def is_superadmin(user_id: int) -> bool:
    return user_id == SUPERADMIN_ID

def is_group_admin(user_id: int, group_id: int) -> bool:
    is_admin = group_manager.is_group_admin(group_id, user_id)
    is_creator = group_manager.is_group_creator(group_id, user_id)
    print(f"DEBUG: is_group_admin({user_id}, {group_id}): admin={is_admin}, creator={is_creator}")
    return is_admin or is_creator

def can_manage_group(user_id: int, group_id: int) -> bool:
    return is_superadmin(user_id) or group_manager.is_group_creator(group_id, user_id)

# Удаление заданий шедулера по ID мероприятия
async def delete_scheduler(event_id):
    try:
        scheduler.remove_job(f"{event_id}_3d")
        scheduler.remove_job(f"{event_id}_2d")
        scheduler.remove_job(f"{event_id}_2h")
        return True
    except:
        print(f'Не удалось удалить задания на оповещения по мероприятию {event_id}')
        return False

# Добавление заданий шедулера по ID и времени мероприятия
async def add_scheduler(event_id, event_time, group_id):
    try:
        scheduler.add_job(send_reminder, DateTrigger(run_date=event_time - timedelta(days=3)), 
                         args=[event_id, "3 дня", group_id], id=f"{event_id}_3d")
        scheduler.add_job(send_reminder, DateTrigger(run_date=event_time - timedelta(days=2)), 
                         args=[event_id, "2 дня", group_id], id=f"{event_id}_2d")
        scheduler.add_job(send_reminder, DateTrigger(run_date=event_time - timedelta(hours=2)), 
                         args=[event_id, "2 часа", group_id], id=f"{event_id}_2h")
    except:
        print(f'Не удалось добавить задания на оповещения по мероприятию {event_id}')
        return False

# Обновление оповещений
async def update_notifications():
    try:
        # Запрашиваем предстоящие мероприятия из базы данных
        events = event_manager.get_all_events()

        if not events:
            return 'Не найдено мероприятий для обновления оповещений.'

        # Удаляем по каждому мероприятию оповещения и создаем новые
        for event_id, name, time, responsible, group_id in events:
            time_formatted = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

            # Удаляем все задания, связанные с мероприятием
            await delete_scheduler(event_id)
            
            # Создаем новые задания по мероприятиям
            await add_scheduler(event_id, time_formatted, group_id)

        mess_res = 'Задания по всем мероприятиям успешно обновлены.'
    except Exception as e:
        mess_res = f'Не удалось обновить задания по мероприятиям: {e}'
    return mess_res

# Обработчик добавления бота в группу
@dp.chat_member()
async def on_chat_member_update(event: ChatMemberUpdated):
    print(f"DEBUG: Chat member update event received: {event.new_chat_member.status}")
    print(f"DEBUG: Chat ID: {event.chat.id}, Title: {event.chat.title}")
    print(f"DEBUG: From user ID: {event.from_user.id}")
    print(f"DEBUG: Bot user ID: {event.new_chat_member.user.id}")
    
    # Проверяем, что это событие касается нашего бота
    if event.new_chat_member.user.id != bot.id:
        print(f"DEBUG: Event not for our bot, ignoring")
        return
    
    if event.new_chat_member.status == ChatMemberStatus.MEMBER:
        print(f"DEBUG: Bot added to group: {event.chat.title}")
        # Бот добавлен в группу
        chat_id = str(event.chat.id)
        title = event.chat.title or "Неизвестная группа"
        user_id = event.from_user.id
        
        # Проверяем, существует ли группа
        existing_group = group_manager.get_group_by_chat_id(chat_id)
        print(f"DEBUG: Existing group check: {existing_group}")
        
        if not existing_group:
            print(f"DEBUG: Creating new group: {title}")
            # Создаем новую группу
            group_id = group_manager.create_group(chat_id, title, user_id)
            print(f"DEBUG: Group created with ID: {group_id}")
            # Добавляем пользователя как админа
            group_manager.add_group_admin(group_id, user_id)
            print(f"DEBUG: User {user_id} added as admin to group {group_id}")
            
            await bot.send_message(
                event.chat.id,
                f"Группа '{title}' успешно добавлена в систему!\n"
                f"ID группы: {group_id}\n"
                f"Создатель: @{event.from_user.username or event.from_user.first_name}"
            )
        else:
            print(f"DEBUG: Group already exists: {existing_group}")
    else:
        print(f"DEBUG: Bot status changed to: {event.new_chat_member.status}")

# Команда start
@dp.message(Command("start"))
async def start_mess(message: Message):
    user_id = message.from_user.id
    print(f"DEBUG: Start command from user {user_id}")
    print(f"DEBUG: Is superadmin: {is_superadmin(user_id)}")
    
    user_groups = group_manager.get_user_admin_groups(user_id)
    print(f"DEBUG: User admin groups: {user_groups}")
    
    if is_superadmin(user_id) or user_groups:
        await message.answer("Привет! Список команд:\n"+str(await get_commands()))
    else:
        await message.answer("Привет! У вас нет прав администратора.")

# Команда help
@dp.message(Command("help"))
async def help_mess(message: Message):
    if is_superadmin(message.from_user.id) or group_manager.get_user_admin_groups(message.from_user.id):
        await message.answer(str(await get_commands()))
    else:
        await message.answer("У вас нет прав для просмотра команд.")

# Команда my_groups
@dp.message(Command("my_groups"))
async def my_groups(message: Message):
    user_id = message.from_user.id
    if is_superadmin(user_id):
        # Суперадмин видит все группы
        groups = group_manager.get_all_groups()
        if groups:
            response = "Все группы в системе:\n"
            for group_id, title, created_by in groups:
                response += f"ID: {group_id} | {title}\n"
        else:
            response = "В системе нет групп."
    else:
        # Обычный пользователь видит только свои группы
        groups = group_manager.get_user_admin_groups(user_id)
        if groups:
            response = "Группы где вы являетесь админом:\n"
            for group_id, title in groups:
                response += f"ID: {group_id} | {title}\n"
        else:
            response = "Вы не являетесь админом ни в одной группе."
    
    await message.answer(response)

# Команда add_admin
@dp.message(Command("add_admin"))
async def add_admin(message: Message):
    user_id = message.from_user.id
    if not (is_superadmin(user_id) or group_manager.get_user_admin_groups(user_id)):
        await message.answer("У вас нет прав для добавления админов.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            await message.answer("Использование: /add_admin ГРУППА_ID ПОЛЬЗОВАТЕЛЬ_ID")
            return
        
        group_id = int(parts[1])
        new_admin_id = int(parts[2])
        
        # Проверяем права
        if not (is_superadmin(user_id) or can_manage_group(user_id, group_id)):
            await message.answer("У вас нет прав для управления этой группой.")
            return
        
        # Проверяем существование группы
        group = group_manager.get_group_by_id(group_id)
        if not group:
            await message.answer("Группа с таким ID не найдена.")
            return
        
        # Добавляем админа
        if group_manager.add_group_admin(group_id, new_admin_id):
            await message.answer(f"Пользователь {new_admin_id} успешно добавлен как админ в группу '{group[2]}'")
        else:
            await message.answer("Пользователь уже является админом этой группы.")
            
    except (IndexError, ValueError):
        await message.answer("Использование: /add_admin ГРУППА_ID ПОЛЬЗОВАТЕЛЬ_ID")

# Команда для регистрации группы (работает только в группах)
@dp.message(Command("register_group"))
async def register_group(message: Message):
    # Проверяем, что команда отправлена из группы
    if message.chat.type not in ['group', 'supergroup']:
        await message.answer("Эта команда работает только в группах.")
        return
    
    user_id = message.from_user.id
    chat_id = str(message.chat.id)
    title = message.chat.title or "Неизвестная группа"
    
    print(f"DEBUG: register_group called by user {user_id} in chat {chat_id}")
    
    # Проверяем, существует ли группа
    existing_group = group_manager.get_group_by_chat_id(chat_id)
    if existing_group:
        await message.answer(f"Группа '{title}' уже зарегистрирована в системе!\nID группы: {existing_group[0]}")
        return
    
    # Создаем новую группу
    group_id = group_manager.create_group(chat_id, title, user_id)
    print(f"DEBUG: Group created with ID: {group_id}")
    
    # Добавляем пользователя как админа
    group_manager.add_group_admin(group_id, user_id)
    print(f"DEBUG: User {user_id} added as admin to group {group_id}")
    
    await message.answer(
        f"Группа '{title}' успешно зарегистрирована в системе!\n"
        f"ID группы: {group_id}\n"
        f"Создатель: @{message.from_user.username or message.from_user.first_name}\n\n"
        f"Теперь вы можете управлять мероприятиями в этой группе!"
    )

# Команда для ручного добавления группы (для отладки)
@dp.message(Command("add_group"))
async def add_group_manual(message: Message):
    user_id = message.from_user.id
    if not is_superadmin(user_id):
        await message.answer("Только суперадмин может добавлять группы вручную.")
        return
    
    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) != 3:
            await message.answer("Использование: /add_group CHAT_ID Название_группы")
            return
        
        chat_id = parts[1]
        title = parts[2]
        
        # Проверяем, существует ли группа
        existing_group = group_manager.get_group_by_chat_id(chat_id)
        if existing_group:
            await message.answer(f"Группа с chat_id {chat_id} уже существует.")
            return
        
        # Создаем новую группу
        group_id = group_manager.create_group(chat_id, title, user_id)
        # Добавляем пользователя как админа
        group_manager.add_group_admin(group_id, user_id)
        
        await message.answer(f"Группа '{title}' успешно добавлена в систему!\nID группы: {group_id}")
        
    except Exception as e:
        await message.answer(f"Ошибка при добавлении группы: {e}")

# Команда remove_admin
@dp.message(Command("remove_admin"))
async def remove_admin(message: Message):
    user_id = message.from_user.id
    if not (is_superadmin(user_id) or group_manager.get_user_admin_groups(user_id)):
        await message.answer("У вас нет прав для удаления админов.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            await message.answer("Использование: /remove_admin ГРУППА_ID ПОЛЬЗОВАТЕЛЬ_ID")
            return
        
        group_id = int(parts[1])
        admin_id = int(parts[2])
        
        # Проверяем права
        if not (is_superadmin(user_id) or can_manage_group(user_id, group_id)):
            await message.answer("У вас нет прав для управления этой группой.")
            return
        
        # Проверяем существование группы
        group = group_manager.get_group_by_id(group_id)
        if not group:
            await message.answer("Группа с таким ID не найдена.")
            return
        
        # Нельзя удалить создателя группы
        if group_manager.is_group_creator(group_id, admin_id):
            await message.answer("Нельзя удалить создателя группы.")
            return
        
        # Удаляем админа
        if group_manager.remove_group_admin(group_id, admin_id):
            await message.answer(f"Пользователь {admin_id} успешно удален из админов группы '{group[2]}'")
        else:
            await message.answer("Пользователь не является админом этой группы.")
            
    except (IndexError, ValueError):
        await message.answer("Использование: /remove_admin ГРУППА_ID ПОЛЬЗОВАТЕЛЬ_ID")

# Добавление события
@dp.message(Command("add_event"))
async def add_event(message: Message):
    user_id = message.from_user.id
    if not (is_superadmin(user_id) or group_manager.get_user_admin_groups(user_id)):
        await message.answer("У вас нет прав для добавления мероприятий.")
        return
    
    try:
        event_data = message.text.split()[1:]
        if len(event_data) < 3:
            await message.answer("Использование: /add_event ДД.ММ.ГГГГ ЧЧ:ММ Название ГРУППА_ID")
            return
        
        date_time_str = ' '.join(event_data[:2])
        event_name = ' '.join(event_data[2:-1])
        group_id = int(event_data[-1])
        
        event_time = datetime.strptime(date_time_str, '%d.%m.%Y %H:%M')

        if event_time < datetime.now():
            await message.answer("Нельзя добавить мероприятие в прошлом.")
            return

        # Проверяем права на группу
        if not (is_superadmin(user_id) or is_group_admin(user_id, group_id)):
            await message.answer("У вас нет прав для добавления мероприятий в эту группу.")
            return

        # Проверяем существование группы
        group = group_manager.get_group_by_id(group_id)
        if not group:
            await message.answer("Группа с таким ID не найдена.")
            return

        event_id = event_manager.create_event(event_name, event_time, group_id)

        # Планируем напоминания
        await add_scheduler(event_id, event_time, group_id)

        await message.answer(f"Мероприятие '{event_name}' добавлено в группу '{group[2]}' на {event_time.strftime('%d.%m.%Y %H:%M')}")

    except (IndexError, ValueError) as e:
        await message.answer(f"Ошибка: {e}\nИспользование: /add_event ДД.ММ.ГГГГ ЧЧ:ММ Название ГРУППА_ID")

# Удаление события
@dp.message(Command("remove_event"))
async def remove_event(message: Message):
    user_id = message.from_user.id
    if not (is_superadmin(user_id) or group_manager.get_user_admin_groups(user_id)):
        await message.answer("У вас нет прав для удаления мероприятий.")
        return
    
    try:
        event_id = int(message.text.split()[1])
        
        # Получаем информацию о мероприятии
        event = event_manager.get_event(event_id)
        if not event:
            await message.answer("Мероприятие с таким ID не найдено.")
            return
        
        # Проверяем права на группу
        if not (is_superadmin(user_id) or is_group_admin(user_id, event[4])):
            await message.answer("У вас нет прав для удаления мероприятий из этой группы.")
            return
        
        if event_manager.delete_event(event_id):
            # Удаляем все задания, связанные с мероприятием
            await delete_scheduler(event_id)
            await message.answer(f"Мероприятие с ID {event_id} удалено.")
        else:
            await message.answer("Не удалось удалить мероприятие.")
            
    except (IndexError, ValueError):
        await message.answer("Использование: /remove_event ID")

# Обновление всех оповещений
@dp.message(Command("update"))
async def update_notifications_cmd(message: Message):
    user_id = message.from_user.id
    if not (is_superadmin(user_id) or group_manager.get_user_admin_groups(message.from_user.id)):
        await message.answer("У вас нет прав для обновления оповещений.")
        return
    
    mess_res = await update_notifications()
    await message.answer(mess_res)

# Список мероприятий
@dp.message(Command("remind_events"))
async def remind_upcoming_events(message: Message):
    user_id = message.from_user.id
    
    # Определяем группу
    group_id = None
    
    if message.chat.type in ['group', 'supergroup']:
        # Сообщение из группы - проверяем, указан ли ID группы
        try:
            parts = message.text.split()
            if len(parts) >= 2:
                # Если указан ID группы, используем его
                group_id = int(parts[1])
            else:
                # Иначе используем текущую группу
                group = group_manager.get_group_by_chat_id(str(message.chat.id))
                if not group:
                    await message.answer("Эта группа не зарегистрирована в системе.")
                    return
                group_id = group[0]  # ID группы в системе
        except (IndexError, ValueError):
            # Если не удалось распарсить ID, используем текущую группу
            group = group_manager.get_group_by_chat_id(str(message.chat.id))
            if not group:
                await message.answer("Эта группа не зарегистрирована в системе.")
                return
            group_id = group[0]  # ID группы в системе
    else:
        # Личное сообщение - проверяем, указан ли ID группы
        try:
            parts = message.text.split()
            if len(parts) >= 2:
                group_id = int(parts[1])
            else:
                await message.answer("Пожалуйста, укажите ID группы для просмотра мероприятий.\n"
                                   "Использование: /remind_events ГРУППА_ID")
                return
        except (IndexError, ValueError):
            await message.answer("Пожалуйста, укажите корректный ID группы.\n"
                               "Использование: /remind_events ГРУППА_ID")
            return
    
    # Проверяем существование группы
    group = group_manager.get_group_by_id(group_id)
    if not group:
        await message.answer(f"Группа с ID {group_id} не найдена в системе.")
        return
    
    # Проверяем права
    if not (is_superadmin(user_id) or is_group_admin(user_id, group_id)):
        await message.answer("У вас нет прав для просмотра мероприятий этой группы.")
        return
    
    events = event_manager.get_upcoming_events(group_id, 30)

    if not events:
        await message.answer(f"В группе '{group[2]}' на ближайший месяц нет мероприятий.")
        return
    
    # Получаем chat_id целевой группы для отправки напоминаний
    target_chat_id = group[1]  # group[1] = chat_id группы в Telegram
    
    for event_id, name, time, responsible, _ in events:
        builder = InlineKeyboardBuilder()
        if responsible:
            builder.button(text=responsible, callback_data=f"unbook_{event_id}")
        else:
            builder.button(text="Забронировать", callback_data=f"book_{event_id}")

        markup = builder.as_markup()
        msg_text = f"Мероприятие: {name}\nДата и время: {datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')}\nГруппа: {group[2]}"  # group[2] = название группы

        # Отправляем напоминание в целевую группу
        try:
            await bot.send_message(target_chat_id, msg_text, reply_markup=markup)
        except Exception as e:
            print(f"Не удалось отправить напоминание в группу {target_chat_id}: {e}")
            # Если не удалось отправить в целевую группу, отправляем в текущий чат
            await message.answer(f"Не удалось отправить в группу {group[2]}: {msg_text}", reply_markup=markup)
    
    # Отправляем финальное подтверждение
    await message.answer(f"✅ Отправлено {len(events)} напоминаний в группу '{group[2]}'")

# Список всех мероприятий
@dp.message(Command("list_events"))
async def list_events(message: Message):
    user_id = message.from_user.id
    if not (is_superadmin(user_id) or group_manager.get_user_admin_groups(user_id)):
        await message.answer("У вас нет прав для получения списка мероприятий.")
        return

    if is_superadmin(user_id):
        # Суперадмин видит все мероприятия
        events = event_manager.get_all_events()
        if not events:
            await message.answer("Не найдено мероприятий для отображения.")
            return

        event_list_message = "Все мероприятия:\n"
        for event_id, name, time, responsible, group_id in events:
            time_formatted = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
            responsible_text = responsible if responsible else "Свободное"
            group = group_manager.get_group_by_id(group_id)
            group_name = group[2] if group else f"Группа {group_id}"  # group[2] = название группы
            event_list_message += f"\nID: {event_id} | {name}\nДата и время: {time_formatted}\nОтветственный: @{responsible_text}\nГруппа: {group_name}\n"
    else:
        # Обычный админ видит только мероприятия своих групп
        user_groups = group_manager.get_user_admin_groups(user_id)
        if not user_groups:
            await message.answer("У вас нет групп для просмотра мероприятий.")
            return
        
        event_list_message = "Мероприятия в ваших группах:\n"
        for group_id, group_title in user_groups:
            events = event_manager.get_events_by_group(group_id)
            if events:
                event_list_message += f"\n--- {group_title} ---\n"
                for event_id, name, time, responsible, _ in events:
                    time_formatted = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
                    responsible_text = responsible if responsible else "Свободное"
                    event_list_message += f"ID: {event_id} | {name}\nДата и время: {time_formatted}\nОтветственный: @{responsible_text}\n"
            else:
                event_list_message += f"\n--- {group_title} ---\nНет мероприятий\n"

    await message.answer(event_list_message)

# Обработка бронирования
@dp.callback_query(lambda c: c.data.startswith('book_'))
async def handle_booking(callback_query: types.CallbackQuery):
    event_id = int(callback_query.data.split('_')[1])
    event = event_manager.get_event(event_id)
    
    if not event:
        await callback_query.answer("Мероприятие не найдено.")
        return
    
    if event[3]:  # responsible
        await callback_query.answer("Это мероприятие уже забронировано.")
        return
    
    # Проверяем права на группу
    user_id = callback_query.from_user.id
    if not (is_superadmin(user_id) or is_group_admin(user_id, event[4])):
        await callback_query.answer("У вас нет прав для бронирования мероприятий в этой группе.")
        return
    
    # Бронируем мероприятие
    user = (
        callback_query.from_user.username or
        f"{callback_query.from_user.first_name} {callback_query.from_user.last_name}".strip() or
        f"id_{callback_query.from_user.id}"
    )
    
    if event_manager.update_event_responsible(event_id, user):
        builder = InlineKeyboardBuilder()
        builder.button(text=user, callback_data=f"unbook_{event_id}")
        markup = builder.as_markup()
        
        await bot.edit_message_reply_markup(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            reply_markup=markup
        )
        await callback_query.answer("Вы забронировали мероприятие.")
    else:
        await callback_query.answer("Не удалось забронировать мероприятие.")

# Обработка отмены бронирования
@dp.callback_query(lambda c: c.data.startswith('unbook_'))
async def handle_unbooking(callback_query: types.CallbackQuery):
    event_id = int(callback_query.data.split('_')[1])
    event = event_manager.get_event(event_id)
    
    if not event:
        await callback_query.answer("Мероприятие не найдено.")
        return
    
    # Проверяем права на группу
    user_id = callback_query.from_user.id
    if not (is_superadmin(user_id) or is_group_admin(user_id, event[4])):
        await callback_query.answer("У вас нет прав для отмены бронирования в этой группе.")
        return
    
    if event_manager.update_event_responsible(event_id, None):
        builder = InlineKeyboardBuilder()
        builder.button(text="Забронировать", callback_data=f"book_{event_id}")
        markup = builder.as_markup()
        
        await bot.edit_message_reply_markup(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            reply_markup=markup
        )
        await callback_query.answer("Мероприятие снова доступно для бронирования.")
    else:
        await callback_query.answer("Не удалось отменить бронирование.")

# Напоминание о мероприятии
async def send_reminder(event_id, time_before, group_id):
    event = event_manager.get_event(event_id)
    if not event:
        return
    
    name, time, responsible = event[1], event[2], event[3]
    group = group_manager.get_group_by_id(group_id)
    group_title = group[2] if group else f"Группа {group_id}"  # group[2] = название группы
    
    message = f"Напоминание: {time_before} до мероприятия '{name}'.\n"
    message += f"Дата и время: {datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')}\n"
    message += f"Группа: {group_title}\n"
    message += f"Ответственный: @{responsible}" if responsible else "Ответственного пока нет("
    
    builder = InlineKeyboardBuilder()
    if responsible:
        builder.button(text=responsible, callback_data=f"unbook_{event_id}")
    else:
        builder.button(text="Забронировать", callback_data=f"book_{event_id}")
    
    markup = builder.as_markup()
    
    # Отправляем напоминание в группу
    try:
        # Получаем chat_id группы для отправки сообщения
        group_info = group_manager.get_group_by_id(group_id)
        print(f"DEBUG: group_info for group_id {group_id}: {group_info}")
        if group_info:
            # group_info[0] = id группы в системе
            # group_info[1] = chat_id в Telegram (столбец chat_id)
            # group_info[2] = название группы
            chat_id = group_info[1]  # chat_id из базы (правильный столбец)
            group_title = group_info[2]  # название группы
            print(f"DEBUG: group_info[0] (id): {group_info[0]}")
            print(f"DEBUG: group_info[1] (chat_id): {chat_id}")
            print(f"DEBUG: group_info[2] (title): {group_title}")
            print(f"DEBUG: Sending reminder to chat_id: {chat_id} for group_id: {group_id}")
            await bot.send_message(chat_id, message, reply_markup=markup)
        else:
            print(f"Группа {group_id} не найдена для отправки напоминания")
    except Exception as e:
        print(f"Не удалось отправить напоминание в группу {group_id}: {e}")

async def main():
    # Создаем и запускаем планировщик
    scheduler.start()
    await update_notifications()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())