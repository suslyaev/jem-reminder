from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pathlib import Path
from datetime import datetime, timedelta
import json
import base64
import hmac
import hashlib

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.repositories import UserRepo, GroupRepo, EventRepo, RoleRepo, PersonalEventNotificationRepo, NotificationRepo, BookingRepo, DisplayNameRepo, EventNotificationRepo

# Import test configuration from .env
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
TEST_TELEGRAM_ID = os.getenv('TEST_TELEGRAM_ID')
if TEST_TELEGRAM_ID:
    TEST_TELEGRAM_ID = int(TEST_TELEGRAM_ID)

SUPERADMIN_ID = os.getenv('SUPERADMIN_ID')
if SUPERADMIN_ID:
    SUPERADMIN_ID = int(SUPERADMIN_ID)

BASE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = BASE_DIR / 'web' / 'templates'
STATIC_DIR = BASE_DIR / 'web' / 'static'

env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR.as_posix()),
    autoescape=select_autoescape(['html', 'xml'])
)

# Add filter for safe tg_id handling
def safe_tg_id(tg_id):
    """Return tg_id if it's valid, otherwise return None"""
    if tg_id and tg_id != 'None':
        return tg_id
    return None

env.filters['safe_tg_id'] = safe_tg_id

app = FastAPI(title="JEM Reminder")
app.mount('/static', StaticFiles(directory=STATIC_DIR.as_posix()), name='static')


def render(name: str, **ctx) -> HTMLResponse:
    tpl = env.get_template(name)
    return HTMLResponse(tpl.render(**ctx))


def _require_user(tg_id: str | int | None, request: Request = None):
    # Try to get user data from Telegram Mini App first
    if request:
        telegram_user = get_telegram_user_data(request)
        if telegram_user:
            tg_id = str(telegram_user['id'])
    
    # Handle string "None" from templates
    if tg_id == "None":
        tg_id = None
    
    # Use test ID if no tg_id provided and test mode is enabled
    if tg_id is None and TEST_TELEGRAM_ID:
        tg_id = str(TEST_TELEGRAM_ID)
        print(f"Using test Telegram ID: {tg_id}")
    
    if tg_id is None:
        raise HTTPException(status_code=401, detail="tg_id required")
    
    # Convert to int for database operations
    try:
        tg_id_int = int(tg_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid tg_id format")
    
    urow = UserRepo.get_by_telegram_id(tg_id_int)
    if not urow:
        raise HTTPException(status_code=403, detail="Unknown user")
    return urow


def _require_admin(user_id: int, group_id: int):
    role = RoleRepo.get_user_role(user_id, group_id)
    if role not in ("owner", "admin", "superadmin"):
        raise HTTPException(status_code=403, detail="Admin permissions required")
    return role


def _role_label(role: str | None) -> str:
    mapping = {
        'superadmin': 'Суперадмин',
        'owner': 'Владелец',
        'admin': 'Админ',
        'member': 'Участник',
        'participant': 'Участник',
        None: 'Участник',
    }
    return mapping.get(role, role.capitalize() if role else 'Участник')


def _normalize_dt_local(val: str | None) -> str | None:
    if not val:
        return None
    if 'T' in val:
        val = val.replace('T', ' ')
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(val, fmt)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    return val


def _format_time_display(time_str: str) -> tuple[str, str]:
    """Return (display_ddmmyyyy_hhmmss, input_yyyy-mm-ddThh:mm). Handle strings with or without seconds."""
    # Build input value (strip seconds if present)
    parts = time_str.split(' ')
    if len(parts) == 2 and parts[1].count(':') >= 2:
        # strip seconds
        hhmm = ':'.join(parts[1].split(':')[:2])
        input_val = f"{parts[0]}T{hhmm}"
    else:
        input_val = time_str.replace(' ', 'T')
    display_val = time_str
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(time_str, fmt)
            display_val = dt.strftime("%d.%m.%Y %H:%M:%S")
            break
        except ValueError:
            continue
    return display_val, input_val

def _calculate_notification_time(event_time_str: str, time_before: int, time_unit: str) -> str:
    """Calculate when notification will be sent based on event time and notification settings."""
    try:
        # Parse the event time
        event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
        
        # Convert time_before and time_unit to minutes
        if time_unit == 'minutes':
            minutes_before = time_before
        elif time_unit == 'hours':
            minutes_before = time_before * 60
        elif time_unit == 'days':
            minutes_before = time_before * 24 * 60
        elif time_unit == 'weeks':
            minutes_before = time_before * 7 * 24 * 60
        elif time_unit == 'months':
            minutes_before = time_before * 30 * 24 * 60
        else:
            return "неизвестно"
        
        # Calculate notification time
        notification_time = event_time - timedelta(minutes=minutes_before)
        
        # Format as DD.MM.YYYY HH:MM:SS
        return notification_time.strftime('%d.%m.%Y %H:%M:%S')
    except Exception:
        return "неизвестно"

def _is_notification_in_past(event_time_str: str, time_before: int, time_unit: str) -> bool:
    """Check if notification time is in the past."""
    try:
        # Parse the event time
        event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
        
        # Convert time_before and time_unit to minutes
        if time_unit == 'minutes':
            minutes_before = time_before
        elif time_unit == 'hours':
            minutes_before = time_before * 60
        elif time_unit == 'days':
            minutes_before = time_before * 24 * 60
        elif time_unit == 'weeks':
            minutes_before = time_before * 7 * 24 * 60
        elif time_unit == 'months':
            minutes_before = time_before * 30 * 24 * 60
        else:
            return True  # If we can't parse, consider it invalid (in past)
        
        # Calculate notification time
        notification_time = event_time - timedelta(minutes=minutes_before)
        
        # Check if notification time is in the past
        now = datetime.now(event_time.tzinfo)
        return notification_time < now
    except Exception:
        return True  # If we can't parse, consider it invalid (in past)


def get_telegram_user_data(request: Request) -> dict | None:
    """Extract user data from Telegram Mini App parameters."""
    try:
        # Get initData from query parameters
        init_data = request.query_params.get('tgWebAppData')
        if not init_data:
            return None
            
        # Parse initData (it's URL-encoded)
        from urllib.parse import parse_qs, unquote
        parsed_data = parse_qs(unquote(init_data))
        
        # Get user data
        user_data_str = parsed_data.get('user', [None])[0]
        if not user_data_str:
            return None
            
        user_data = json.loads(user_data_str)
        
        return {
            'id': user_data.get('id'),
            'username': user_data.get('username'),
            'first_name': user_data.get('first_name'),
            'last_name': user_data.get('last_name'),
            'language_code': user_data.get('language_code'),
        }
    except Exception as e:
        print(f"Error parsing Telegram user data: {e}")
        return None


@app.get('/', response_class=HTMLResponse)
async def index(request: Request, tg_id: str | None = None):
    # Try to get user data from Telegram Mini App first
    telegram_user = get_telegram_user_data(request)
    
    if telegram_user:
        tg_id = str(telegram_user['id'])
        print(f"Got Telegram user data: {telegram_user}")
    elif tg_id is None and TEST_TELEGRAM_ID:
        # Use test ID if no tg_id provided and test mode is enabled
        tg_id = str(TEST_TELEGRAM_ID)
        print(f"Using test Telegram ID for index: {tg_id}")
    elif tg_id == "None":
        # Handle string "None" from templates
        tg_id = None
    
    if tg_id is None:
        # No user data available
        return render('welcome.html', message="Установите TEST_TELEGRAM_ID в файле .env для тестирования", user_info=None, request=request)
    
    # Convert to int for database operations
    try:
        tg_id_int = int(tg_id)
    except (ValueError, TypeError):
        return render('welcome.html', message="Неверный ID пользователя", user_info=None, request=request)
    
    urow = UserRepo.get_by_telegram_id(tg_id_int)
    if not urow:
        # Create user_info object for display using Telegram data or fallback
        user_info = {
            'telegram_id': tg_id_int,
            'username': telegram_user.get('username') if telegram_user else None,
            'first_name': telegram_user.get('first_name') if telegram_user else None,
            'last_name': telegram_user.get('last_name') if telegram_user else None
        }
        return render('welcome.html', message="Пользователь не найден. Запустите бота сначала.", user_info=user_info, request=request)
    user_id = urow[0]
    # urow: (id, telegram_id, username, phone, first_name, last_name)
    username = urow[2]
    phone = urow[3]
    first_name = urow[4]
    last_name = urow[5]
    display = first_name or username or str(tg_id_int)
    if first_name and last_name:
        display = f"{first_name} {last_name}"
    groups = GroupRepo.list_user_groups_with_roles(user_id)
    groups_with_counts = []
    for gid, title, role, chat_id in groups:
        groups_with_counts.append((gid, title, role, GroupRepo.count_group_events(gid), _role_label(role), chat_id))
    return render('index.html', groups=groups_with_counts, user_info={
        'display': display,
        'username': username,
        'telegram_id': tg_id_int,
        'phone': phone,
    }, request=request)


@app.get('/group/{gid}', response_class=HTMLResponse)
async def group_view(request: Request, gid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    if role is None:
        events = EventRepo.list_by_group(gid)
        has_any = False
        for eid, _, _, _ in events:
            if BookingRepo.has_booking(user_id, eid):
                has_any = True
                break
        if not has_any:
            raise HTTPException(status_code=403, detail="Access denied")
    else:
        events = EventRepo.list_by_group(gid)
    group = GroupRepo.get_by_id(gid)
    display_name = DisplayNameRepo.get_display_name(gid, user_id)
    booked_ids = {eid for (eid, _, _, _) in events if BookingRepo.has_booking(user_id, eid)}
    is_admin = (role in ("owner", "admin", "superadmin")) if role else False
    bookings_map = {eid: BookingRepo.list_event_bookings_with_names(gid, eid) for (eid, _, _, _) in events}
    member_rows = GroupRepo.list_group_members(gid)
    member_name_map: dict[int, str] = {}
    for mid, uname in member_rows:
        dn = DisplayNameRepo.get_display_name(gid, mid)
        member_name_map[mid] = dn if dn else (f"@{uname}" if uname else str(mid))
    
    # Filter out superadmin if current user is not superadmin
    from config import SUPERADMIN_ID
    is_superadmin = urow[1] == SUPERADMIN_ID  # urow[1] is telegram_id
    if not is_superadmin:
        # Remove superadmin from member options
        filtered_member_rows = []
        for mid, uname in member_rows:
            # Check if this user is superadmin
            user_role = RoleRepo.get_user_role(mid, gid)
            if user_role != 'superadmin':
                filtered_member_rows.append((mid, uname))
        member_rows = filtered_member_rows
    
    member_options = [(mid, member_name_map[mid]) for mid, _ in member_rows]
    event_items = []
    for eid, name, time_str, resp_uid in events:
        disp, input_val = _format_time_display(time_str)
        event_items.append({
            'id': eid,
            'name': name,
            'time_display': disp,
            'time_input': input_val,
            'responsible_user_id': resp_uid,
            'responsible_name': member_name_map.get(resp_uid) if resp_uid is not None else None,
            'has_any_bookings': len(bookings_map.get(eid, [])) > 0,
        })
    event_count = GroupRepo.count_group_events(gid)
    return render('group.html', group=group, role=_role_label(role or 'participant'), is_admin=is_admin, events=event_items, booked_ids=booked_ids, display_name=display_name, bookings_map=bookings_map, member_options=member_options, event_count=event_count, request=request)


# --- Event CRUD ---
@app.post('/group/{gid}/events/create')
async def create_event(request: Request, gid: int, name: str = Form(...), time: str = Form(...), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    norm_time = _normalize_dt_local(time)
    new_eid = EventRepo.create(gid, name, norm_time or time)
    # Create event notifications from group defaults
    EventNotificationRepo.create_from_group_defaults(new_eid, gid)
    # Also create personal defaults for all confirmed members
    PersonalEventNotificationRepo.create_from_group_for_all_users(new_eid, gid)
    return RedirectResponse(url=f"/group/{gid}?ok=created", status_code=303)


@app.post('/group/{gid}/events/create-multiple')
async def create_events_multiple(request: Request, gid: int, items: str | None = Form(None), name: list[str] | None = Form(None), time: list[str] | None = Form(None), tg_id: str | None = None):
    """
    Supports two payload formats:
    1) Repeated fields: name=.., time=.. (multiple)
    2) Single textarea 'items' with lines: "YYYY-MM-DD HH:MM | Name"
    """
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    created_any = False
    if name and time:
        for t, n in zip(time, name):
            t = _normalize_dt_local((t or '').strip())
            n = (n or '').strip()
            if not t and not n:
                continue
            if t and n:
                eid = EventRepo.create(gid, n, t)
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                PersonalEventNotificationRepo.create_from_group_for_all_users(eid, gid)
                created_any = True
            elif t:
                eid = EventRepo.create(gid, f"Событие {t}", t)
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                PersonalEventNotificationRepo.create_from_group_for_all_users(eid, gid)
                created_any = True
            elif n:
                continue
    elif items:
        for line in items.splitlines():
            if not line.strip():
                continue
            if '|' in line:
                time_part, name_part = line.split('|', 1)
                eid = EventRepo.create(gid, name_part.strip(), _normalize_dt_local(time_part.strip()) or time_part.strip())
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                PersonalEventNotificationRepo.create_from_group_for_all_users(eid, gid)
                created_any = True
            else:
                t = _normalize_dt_local(line.strip()) or line.strip()
                eid = EventRepo.create(gid, f"Событие {t}", t)
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                PersonalEventNotificationRepo.create_from_group_for_all_users(eid, gid)
                created_any = True
    ok = 'created' if created_any else 'noop'
    return RedirectResponse(url=f"/group/{gid}?ok={ok}", status_code=303)


@app.post('/group/{gid}/events/{eid}/update')
async def update_event(request: Request, gid: int, eid: int, name: str | None = Form(None), time: str | None = Form(None), responsible_user_id: int | None = Form(None), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    if name is not None and name != "":
        EventRepo.update_name(eid, name)
    if time is not None and time != "":
        EventRepo.update_time(eid, _normalize_dt_local(time) or time)
    if responsible_user_id is not None:
        EventRepo.set_responsible(eid, responsible_user_id if responsible_user_id != 0 else None)
        # If assigning a responsible user, ensure their personal notifications are created from group settings
        if responsible_user_id and responsible_user_id != 0:
            PersonalEventNotificationRepo.create_from_group_for_user(eid, gid, responsible_user_id)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?ok=updated", status_code=303)


@app.post('/group/{gid}/events/{eid}/update-from-card')
async def update_event_from_card(request: Request, gid: int, eid: int, name: str | None = Form(None), time: str | None = Form(None), responsible_user_id: int | None = Form(None), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    if name is not None and name != "":
        EventRepo.update_name(eid, name)
    if time is not None and time != "":
        EventRepo.update_time(eid, _normalize_dt_local(time) or time)
    if responsible_user_id is not None:
        EventRepo.set_responsible(eid, responsible_user_id if responsible_user_id != 0 else None)
        # If assigning a responsible user, ensure their personal notifications are created from group settings
        if responsible_user_id and responsible_user_id != 0:
            PersonalEventNotificationRepo.create_from_group_for_user(eid, gid, responsible_user_id)
    return RedirectResponse(url=f"/group/{gid}?ok=event_updated", status_code=303)


# --- Settings: notifications & admins ---
@app.get('/group/{gid}/settings', response_class=HTMLResponse)
async def group_settings(request: Request, gid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    # Allow all users to see settings, but restrict admin functions
    try:
        role = _require_admin(user_id, gid)
    except:
        # If not admin, get user role for display purposes
        role = RoleRepo.get_user_role(user_id, gid)
    group = GroupRepo.get_by_id(gid)
    notifications = NotificationRepo.list_notifications(gid)
    # Load personal notification templates for this group
    personal_notifications = NotificationRepo.list_personal_notifications(gid)
    pending = RoleRepo.list_pending_admins(gid)
    admins = GroupRepo.list_group_admins(gid)
    members = GroupRepo.list_group_members_detailed(gid)
    current_display_name = DisplayNameRepo.get_display_name(gid, user_id)
    
    # Filter out superadmin if current user is not superadmin
    # Check if current user is superadmin by telegram_id
    is_superadmin = urow[1] == SUPERADMIN_ID  # urow[1] is telegram_id
    if not is_superadmin:
        members = [m for m in members if m[6] != 'superadmin']
    
    # Get display names for all members, create if not exists
    member_display_names = {}
    for uid, _, _, _, _, _, _ in members:
        display_name = DisplayNameRepo.get_display_name(gid, uid)
        if not display_name:
            # Create display name from first_name + last_name
            DisplayNameRepo.create_display_name_from_user_info(gid, uid)
            display_name = DisplayNameRepo.get_display_name(gid, uid)
        member_display_names[uid] = display_name
    role_map = {'superadmin': 'СУПЕРАДМИН', 'owner': 'ВЛАДЕЛЕЦ', 'admin': 'АДМИН', 'member': 'УЧАСТНИК'}
    event_count = GroupRepo.count_group_events(gid)
    notifications_count = len(notifications)
    personal_notifications_count = len(personal_notifications)
    return render('group_settings.html', group=group, role=role, notifications=notifications, personal_notifications=personal_notifications, pending=pending, admins=admins, members=members, current_display_name=current_display_name, member_display_names=member_display_names, role_map=role_map, event_count=event_count, notifications_count=notifications_count, personal_notifications_count=personal_notifications_count, request=request)


@app.post('/group/{gid}/settings/notifications/add')
async def add_group_notification(request: Request, gid: int, time_before: int, time_unit: str, message_text: str | None = None, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    NotificationRepo.add_notification(gid, time_before, time_unit, message_text)
    return RedirectResponse(url=f"/group/{gid}/settings?tg_id={tg_id}", status_code=303)

@app.get('/group/{gid}/settings/notifications/add-text')
async def add_group_notification_text_get(request: Request, gid: int, tg_id: str | None = None):
    print(f"GET add_group_notification_text: gid={gid}, tg_id={tg_id}")
    return RedirectResponse(f"/group/{gid}/settings?ok=method_error", status_code=303)

@app.post('/group/{gid}/settings/notifications/add-text')
async def add_group_notification_text(request: Request, gid: int, notification_text: str = Form(...), message_text: str = Form(""), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    
    # Parse notification text using the same logic as bot
    from bot import parse_duration_ru
    
    text = notification_text.strip()
    minutes = parse_duration_ru(text)
    
    if minutes <= 0:
        return RedirectResponse(f"/group/{gid}/settings?ok=parse_error", status_code=303)
    
    # Convert minutes to appropriate unit
    if minutes < 60:
        time_before = minutes
        time_unit = 'minutes'
    elif minutes < 1440:  # 24 hours
        time_before = minutes // 60
        time_unit = 'hours'
    else:
        time_before = minutes // 1440
        time_unit = 'days'
    
    # Use provided message_text or extract from notification_text
    final_message_text = message_text.strip() if message_text.strip() else None
    
    if not final_message_text:
        # Extract message from text (remove time expressions) as fallback
        import re
        pattern_global = re.compile(r'(?:за|через)?\s*(\d+)\s*(день|дн(?:я|ей|ь)|нед(?:еля|ели|ель)|мес(?:яц|яца|яцев)|час(?:а|ов)?|ч|мин(?:ут|уты|ут)?|м)', re.IGNORECASE)
        cleaned = pattern_global.sub('', text)
        cleaned = re.sub(r'[\s,]+', ' ', cleaned).strip()
        if cleaned.lower().startswith('напомнить'):
            cleaned = cleaned[9:].strip()
        final_message_text = cleaned if cleaned else None
    
    NotificationRepo.add_notification(gid, time_before, time_unit, final_message_text)
    return RedirectResponse(f"/group/{gid}/settings?ok=notification_added", status_code=303)

@app.post('/group/{gid}/settings/personal-notifications/add-text')
async def add_personal_notification_text(request: Request, gid: int, notification_text: str = Form(...), message_text: str = Form(""), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    
    # Parse notification text using the same logic as bot
    from bot import parse_duration_ru
    
    text = notification_text.strip()
    minutes = parse_duration_ru(text)
    
    if minutes <= 0:
        return RedirectResponse(f"/group/{gid}/settings?ok=parse_error", status_code=303)
    
    # Convert minutes to appropriate unit
    if minutes < 60:
        time_before = minutes
        time_unit = 'minutes'
    elif minutes < 1440:  # 24 hours
        time_before = minutes // 60
        time_unit = 'hours'
    else:
        time_before = minutes // 1440
        time_unit = 'days'
    
    # Use provided message_text or extract from notification_text
    final_message_text = message_text.strip() if message_text.strip() else None
    
    if not final_message_text:
        # Extract message from text (remove time expressions) as fallback
        import re
        pattern_global = re.compile(r'(?:за|через)?\s*(\d+)\s*(день|дн(?:я|ей|ь)|нед(?:еля|ели|ель)|мес(?:яц|яца|яцев)|час(?:а|ов)?|ч|мин(?:ут|уты|ут)?|м)', re.IGNORECASE)
        cleaned = pattern_global.sub('', text)
        cleaned = re.sub(r'[\s,]+', ' ', cleaned).strip()
        if cleaned.lower().startswith('напомнить'):
            cleaned = cleaned[9:].strip()
        final_message_text = cleaned if cleaned else None
    
    # Add personal notification template
    NotificationRepo.add_notification(gid, time_before, time_unit, final_message_text, notification_type='personal')
    return RedirectResponse(f"/group/{gid}/settings?ok=personal_notification_added", status_code=303)

@app.post('/group/{gid}/settings/personal-notifications/{nid}/delete')
async def delete_personal_notification(request: Request, gid: int, nid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    NotificationRepo.delete_notification(nid)
    return RedirectResponse(f"/group/{gid}/settings?ok=personal_notification_deleted", status_code=303)

@app.post('/group/{gid}/send-message')
async def send_message_to_user(request: Request, gid: int, recipient_id: int = Form(...), message: str = Form(...), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    
    # Check if user has permission to send messages (admin or owner)
    try:
        _require_admin(user_id, gid)
    except:
        return {"success": False, "error": "Недостаточно прав для отправки сообщений"}
    
    # Get recipient's telegram_id
    recipient_telegram_id = UserRepo.get_telegram_id_by_user_id(recipient_id)
    if not recipient_telegram_id:
        return {"success": False, "error": "Получатель не найден"}
    
    try:
        # Import bot here to avoid circular imports
        from bot import bot
        await bot.send_message(recipient_telegram_id, message)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": f"Ошибка отправки: {str(e)}"}

@app.post('/group/{gid}/send-group-message')
async def send_message_to_group(request: Request, gid: int, message: str = Form(...), tg_id: str | None = None):
    print(f"send-group-message: tg_id={tg_id}, gid={gid}, message={message}")
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    print(f"User found: id={user_id}")
    
    # Check if user has permission to send messages (admin or owner)
    role = _require_admin(user_id, gid)
    print(f"User role: {role}")
    
    # Get group's telegram_chat_id
    group = GroupRepo.get_by_id(gid)
    if not group or not group[1]:  # group[1] is telegram_chat_id
        return {"success": False, "error": "Группа не найдена"}
    
    try:
        # Import bot here to avoid circular imports
        from bot import bot
        # Convert string chat_id to int for bot
        chat_id = int(group[1])
        print(f"Attempting to send message to chat_id: {chat_id}, message: {message}")
        await bot.send_message(chat_id, message)
        return {"success": True}
    except Exception as e:
        print(f"Error sending message: {str(e)}")
        return {"success": False, "error": f"Ошибка отправки: {str(e)}"}


@app.post('/group/{gid}/settings/notifications/{nid}/delete')
async def delete_group_notification(request: Request, gid: int, nid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    NotificationRepo.delete_notification(nid)
    return RedirectResponse(url=f"/group/{gid}/settings?tg_id={tg_id}", status_code=303)


@app.get('/group/{gid}/events/{eid}/settings', response_class=HTMLResponse)
async def event_settings(request: Request, gid: int, eid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    
    # Check if user has access (admin or participant with booking)
    role = RoleRepo.get_user_role(user_id, gid)
    if role not in ['admin', 'owner', 'superadmin'] and not BookingRepo.has_booking(user_id, eid):
        raise HTTPException(status_code=403, detail="Access denied")
    
    group = GroupRepo.get_by_id(gid)
    event = EventRepo.get_by_id(eid)
    member_rows = GroupRepo.list_group_members(gid)
    event_notifications = EventNotificationRepo.list_by_event(eid)
    personal_notifications = PersonalEventNotificationRepo.list_by_user_and_event(user_id, eid)
    
    # Create member options with display names (same as in group.html)
    member_name_map: dict[int, str] = {}
    for mid, uname in member_rows:
        dn = DisplayNameRepo.get_display_name(gid, mid)
        member_name_map[mid] = dn if dn else (f"@{uname}" if uname else str(mid))
    
    # Filter out superadmin if current user is not superadmin
    from config import SUPERADMIN_ID
    is_superadmin = urow[1] == SUPERADMIN_ID  # urow[1] is telegram_id
    if not is_superadmin:
        # Remove superadmin from member options
        filtered_member_rows = []
        for mid, uname in member_rows:
            # Check if this user is superadmin
            user_role = RoleRepo.get_user_role(mid, gid)
            if user_role != 'superadmin':
                filtered_member_rows.append((mid, uname))
        member_rows = filtered_member_rows
    
    member_options = [(mid, member_name_map[mid]) for mid, _ in member_rows]
    
    # Format event time display
    event_time_display, _ = _format_time_display(event[2])
    
    # Get responsible person name
    responsible_name = member_name_map.get(event[4]) if event[4] else None
    
    return render('event_settings.html', group=group, event=event, members=member_options, event_notifications=event_notifications, personal_notifications=personal_notifications, role=role, responsible_name=responsible_name, event_time_display=event_time_display, _calculate_notification_time=_calculate_notification_time, request=request)


@app.post('/group/{gid}/events/{eid}/notifications/add')
async def add_event_notification(request: Request, gid: int, eid: int, time_before: int, time_unit: str, message_text: str | None = None, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    EventNotificationRepo.add_notification(eid, time_before, time_unit, message_text)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/notifications/add-text')
async def add_event_notification_text(request: Request, gid: int, eid: int, notification_text: str = Form(...), message_text: str = Form(""), tg_id: str | None = None):
    print(f"add_event_notification_text called: gid={gid}, eid={eid}, tg_id={tg_id}")
    print(f"notification_text='{notification_text}', message_text='{message_text}'")
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    
    # Get event data
    event = EventRepo.get_by_id(eid)
    if not event:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=event_not_found", status_code=303)
    
    # Parse notification text using the same logic as bot
    try:
        from bot import parse_duration_ru
        print(f"Successfully imported parse_duration_ru for event notifications")
    except Exception as e:
        print(f"Error importing parse_duration_ru: {e}")
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    text = notification_text.strip()
    print(f"Parsing event notification text: '{text}'")
    minutes = parse_duration_ru(text)
    print(f"Parsed minutes: {minutes}")
    
    if minutes <= 0:
        print("Minutes <= 0, returning parse_error")
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    # Convert minutes to appropriate unit
    if minutes < 60:
        time_before = minutes
        time_unit = 'minutes'
    elif minutes < 1440:  # 24 hours
        time_before = minutes // 60
        time_unit = 'hours'
    else:
        time_before = minutes // 1440
        time_unit = 'days'
    
    # Use provided message_text if available
    message_text_tail = message_text.strip() if message_text and message_text.strip() else None
    
    # Check if notification time is in the past
    if _is_notification_in_past(event[2], time_before, time_unit):
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=notification_in_past", status_code=303)
    
    EventNotificationRepo.add_notification(eid, time_before, time_unit, message_text_tail)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=notification_added", status_code=303)


@app.get('/group/{gid}/events/{eid}/notifications/add-absolute')
async def add_event_notification_absolute_get(request: Request, gid: int, eid: int, tg_id: str | None = None):
    print(f"GET add_event_notification_absolute: gid={gid}, eid={eid}, tg_id={tg_id}")
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=method_error", status_code=303)

@app.post('/group/{gid}/events/{eid}/notifications/add-absolute')
async def add_event_notification_absolute(request: Request, gid: int, eid: int, date: str = Form(...), time: str = Form(...), message_text: str = Form(None), tg_id: str | None = None):
    print(f"POST add_event_notification_absolute: gid={gid}, eid={eid}, tg_id={tg_id}, date={date}, time={time}, message_text={message_text}")
    """Add event notification by exact datetime. Stored as delta in minutes."""
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    event = EventRepo.get_by_id(eid)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    # Combine date and time
    at = f"{date}T{time}"
    
    event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M")
    at_norm = _normalize_dt_local(at)
    try:
        at_dt = datetime.strptime(at_norm, "%Y-%m-%d %H:%M") if at_norm else None
    except Exception:
        at_dt = None
    if not at_dt:
        raise HTTPException(status_code=400, detail="Invalid datetime")
    delta_seconds = int((event_time - at_dt).total_seconds())
    if delta_seconds <= 0:
        # If not before the event, clamp to 1 minute before
        delta_seconds = 60
    # Store in minutes by default
    minutes = max(delta_seconds // 60, 1)
    EventNotificationRepo.add_notification(eid, minutes, 'minutes', message_text)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?tg_id={tg_id}", status_code=303)


@app.post('/group/{gid}/events/{eid}/notifications/{nid}/delete')
async def delete_event_notification(request: Request, gid: int, eid: int, nid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    EventNotificationRepo.delete_notification(nid)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/personal-notifications/add-text')
async def add_personal_event_notification_text(request: Request, gid: int, eid: int, notification_text: str = Form(...), message_text: str = Form(""), tg_id: str | None = None):
    print(f"add_personal_event_notification_text called: gid={gid}, eid={eid}, tg_id={tg_id}")
    print(f"notification_text='{notification_text}', message_text='{message_text}'")
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    
    # Get event data
    event = EventRepo.get_by_id(eid)
    if not event:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=event_not_found", status_code=303)
    
    # Parse notification text using the same logic as bot
    try:
        from bot import parse_duration_ru
        print(f"Successfully imported parse_duration_ru for personal notifications")
    except Exception as e:
        print(f"Error importing parse_duration_ru: {e}")
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    text = notification_text.strip()
    minutes = parse_duration_ru(text)
    
    if minutes <= 0:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    # Ограничение: максимум 30 дней (43200 минут)
    if minutes > 43200:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    # Обработка пустого сообщения
    if message_text and message_text.strip():
        message_text = message_text.strip()
    else:
        message_text = None
    
    # Convert minutes to appropriate unit
    if minutes < 60:
        time_before = minutes
        time_unit = 'minutes'
    elif minutes < 1440:  # 24 hours
        time_before = minutes // 60
        time_unit = 'hours'
    else:
        time_before = minutes // 1440
        time_unit = 'days'
    
    # Use provided message_text if available
    message_text_tail = message_text.strip() if message_text and message_text.strip() else None
    
    # Check if notification time is in the past
    if _is_notification_in_past(event[2], time_before, time_unit):
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=notification_in_past", status_code=303)
    
    PersonalEventNotificationRepo.add_notification(user_id, eid, time_before, time_unit, message_text_tail)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=personal_notification_added", status_code=303)

@app.post('/group/{gid}/events/{eid}/personal-notifications/add-absolute')
async def add_personal_event_notification_absolute(request: Request, gid: int, eid: int, date: str = Form(...), time: str = Form(...), message_text: str = Form(None), tg_id: str | None = None):
    """Add personal event notification by exact datetime. Stored as delta in minutes."""
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    
    # Combine date and time
    at = f"{date}T{time}"
    
    # Convert datetime to minutes before event
    from datetime import datetime
    event = EventRepo.get_by_id(eid)
    if not event:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=error", status_code=303)
    
    event_time = datetime.fromisoformat(event[2].replace('Z', '+00:00'))
    notification_time = datetime.fromisoformat(at)
    
    # Calculate difference in minutes
    delta = event_time - notification_time
    minutes_before = int(delta.total_seconds() / 60)
    
    if minutes_before <= 0:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    # Convert to appropriate unit
    if minutes_before < 60:
        time_before = minutes_before
        time_unit = 'minutes'
    elif minutes_before < 1440:  # 24 hours
        time_before = minutes_before // 60
        time_unit = 'hours'
    else:
        time_before = minutes_before // 1440
        time_unit = 'days'
    
    PersonalEventNotificationRepo.add_notification(user_id, eid, time_before, time_unit, message_text)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=personal_notification_added", status_code=303)

@app.post('/group/{gid}/events/{eid}/personal-notifications/{nid}/delete')
async def delete_personal_event_notification(request: Request, gid: int, eid: int, nid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    PersonalEventNotificationRepo.delete_personal_notification(user_id, nid)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=personal_notification_deleted", status_code=303)

@app.post('/group/{gid}/events/{eid}/delete')
async def delete_event(request: Request, gid: int, eid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    
    # Delete the event (CASCADE will handle notifications)
    print(f"DELETE EVENT: eid={eid}, gid={gid}, user_id={user_id}")
    result = EventRepo.delete(eid)
    print(f"DELETE RESULT: {result}")
    
    return RedirectResponse(f"/group/{gid}?ok=event_deleted", status_code=303)

@app.post('/group/{gid}/events/{eid}/book')
async def book_event(request: Request, gid: int, eid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    
    print(f"BOOK EVENT: eid={eid}, gid={gid}, user_id={user_id}")
    
    # Check if event exists and is not already booked
    event = EventRepo.get_by_id(eid)
    print(f"EVENT: {event}")
    if not event or event[4]:  # event doesn't exist or already has responsible person
        print(f"BOOKING ERROR: event={event}, has_responsible={event[4] if event else 'no event'}")
        return RedirectResponse(f"/group/{gid}?ok=booking_error", status_code=303)
    
    # Book the event
    print(f"UPDATING EVENT: setting responsible to {user_id}")
    EventRepo.update_event(eid, event[1], event[2], event[3], user_id)
    BookingRepo.add_booking(user_id, eid)
    
    # Create personal notifications for this user
    PersonalEventNotificationRepo.create_from_group_for_user(eid, gid, user_id)
    
    print(f"BOOKING SUCCESS")
    return RedirectResponse(f"/group/{gid}?ok=event_booked", status_code=303)


@app.get('/group/{gid}/events/{eid}', response_class=HTMLResponse)
async def event_detail(request: Request, gid: int, eid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    if role is None and not BookingRepo.has_booking(user_id, eid):
        raise HTTPException(status_code=403, detail="Access denied")
    group = GroupRepo.get_by_id(gid)
    event = EventRepo.get_by_id(eid)
    bookings = BookingRepo.list_event_bookings_with_names(gid, eid)
    event_time_display, _ = _format_time_display(event[2])
    return render('event_detail.html', group=group, event=event, event_time_display=event_time_display, bookings=bookings, role=_role_label(role or 'participant'), request=request)


@app.post('/group/{gid}/events/{eid}/unbook')
async def unbook_event(request: Request, gid: int, eid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    BookingRepo.remove_booking(user_id, eid)
    return RedirectResponse(url=f"/group/{gid}?ok=unbooked", status_code=303)


@app.post('/group/{gid}/display-name/set')
async def set_display_name(request: Request, gid: int, display_name: str = Form(...), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    DisplayNameRepo.set_display_name(gid, user_id, display_name)
    return RedirectResponse(url=f"/group/{gid}?ok=name_saved", status_code=303)

@app.post('/group/{gid}/settings/member/{uid}/display-name')
async def update_member_display_name(request: Request, gid: int, uid: int, display_name: str = Form(...), tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    # No additional encoding needed, FastAPI already handles UTF-8
    DisplayNameRepo.set_display_name(gid, uid, display_name)
    return RedirectResponse(f"/group/{gid}/settings?ok=member_name_saved", status_code=303)

@app.post('/group/{gid}/settings/member/{uid}/make-admin')
async def make_member_admin(request: Request, gid: int, uid: int, tg_id: str | None = None):
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    # Add as pending admin
    RoleRepo.add_pending_admin(gid, str(uid), 'id', user_id)
    return RedirectResponse(f"/group/{gid}/settings?ok=member_made_admin", status_code=303)


@app.post('/group/{gid}/delete')
async def delete_group(request: Request, gid: int, tg_id: str | None = None):
    """Delete group and all associated data"""
    print(f"DELETE GROUP: gid={gid}, tg_id={tg_id}")
    urow = _require_user(tg_id, request)
    user_id = urow[0]
    
    # Check if user is owner or superadmin
    role = RoleRepo.get_user_role(user_id, gid)
    if role not in ['owner', 'superadmin']:
        raise HTTPException(status_code=403, detail="Only group owner or superadmin can delete group")
    
    # Delete group - CASCADE will handle all associated data
    try:
        # Delete group itself - CASCADE will automatically delete:
        # - notification_settings (group and personal templates)
        # - user_group_roles (roles)
        # - pending_admins (pending admin requests)
        # - events (and their notifications via CASCADE)
        # - user_display_names (display names)
        GroupRepo.delete_group(gid)
        
        print(f"Group {gid} deleted successfully by user {user_id}")
        
    except Exception as e:
        print(f"Error deleting group {gid}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting group: {str(e)}")
    
    # Redirect to main page
    return RedirectResponse(url="/?ok=group_deleted", status_code=303)


