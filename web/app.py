from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
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
from services.repositories import UserRepo, GroupRepo, EventRepo, RoleRepo, PersonalEventNotificationRepo, NotificationRepo, BookingRepo, DisplayNameRepo, EventNotificationRepo, DispatchLogRepo

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

# Add session middleware
app.add_middleware(SessionMiddleware, secret_key="your-secret-key-change-in-production")

app.mount('/static', StaticFiles(directory=STATIC_DIR.as_posix()), name='static')


def render(name: str, **ctx) -> HTMLResponse:
    tpl = env.get_template(name)
    return HTMLResponse(tpl.render(**ctx))


def _require_user(request: Request):
    """Get user from session or authenticate from Telegram Mini App."""
    # First, try to get user from session
    session_user = get_user_from_session(request)
    if session_user:
        print(f"User found in session: {session_user['id']}")
        # Get or create user in database
        user_row = UserRepo.get_by_telegram_id(int(session_user['id']))
        if user_row:
            return user_row
    
    # If no session, try to authenticate from Telegram Mini App
    telegram_user = get_telegram_user_data(request)
    if telegram_user:
        print(f"Authenticating from Telegram Mini App: {telegram_user['id']}")
        # Set session
        set_user_session(request, telegram_user)
        # Get or create user in database
        user_row = UserRepo.get_by_telegram_id(telegram_user['id'])
        if user_row:
            return user_row
    
    # If test mode is enabled, use test user
    if TEST_TELEGRAM_ID:
        print(f"Using test Telegram ID: {TEST_TELEGRAM_ID}")
        # Set test session
        test_user_data = {
            'id': str(TEST_TELEGRAM_ID),
            'username': 'test_user',
            'first_name': 'Test',
            'last_name': 'User'
        }
        set_user_session(request, test_user_data)
        # Get or create user in database
        user_row = UserRepo.get_by_telegram_id(int(TEST_TELEGRAM_ID))
        if user_row:
            return user_row
    
    # No authentication found
    raise HTTPException(status_code=401, detail="Authentication required")


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
    
    # Handle datetime-local input format (YYYY-MM-DDTHH:MM)
    if 'T' in val:
        # Try to parse as datetime-local format first
        try:
            dt = datetime.fromisoformat(val)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            # Fallback to old method
            val = val.replace('T', ' ')
    
    # Try different formats
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
        print(f"tgWebAppData from query: {init_data}")
        if not init_data:
            print("No tgWebAppData found in query parameters")
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


def set_user_session(request: Request, user_data: dict):
    """Set user data in session."""
    request.session['user_id'] = str(user_data['id'])
    request.session['username'] = user_data.get('username', '')
    request.session['first_name'] = user_data.get('first_name', '')
    request.session['last_name'] = user_data.get('last_name', '')
    request.session['is_authenticated'] = True
    print(f"Session set for user: {user_data['id']}")


def get_user_from_session(request: Request) -> dict | None:
    """Get user data from session."""
    if request.session.get('is_authenticated'):
        return {
            'id': request.session.get('user_id'),
            'username': request.session.get('username', ''),
            'first_name': request.session.get('first_name', ''),
            'last_name': request.session.get('last_name', ''),
        }
    return None


def clear_user_session(request: Request):
    """Clear user session."""
    request.session.clear()
    print("User session cleared")


@app.get('/', response_class=HTMLResponse)
async def index(request: Request):
    # Сначала пытаемся получить данные из Telegram
    telegram_user = get_telegram_user_data(request)
    if telegram_user:
        print(f"Authenticating from Telegram Mini App: {telegram_user['id']}")
        # Устанавливаем сессию
        set_user_session(request, telegram_user)
        # Получаем пользователя из БД
        user_row = UserRepo.get_by_telegram_id(telegram_user['id'])
        if user_row:
            user_id = user_row[0]
            # user_row: (id, telegram_id, username, phone, first_name, last_name)
            username = user_row[2]
            phone = user_row[3]
            first_name = user_row[4]
            last_name = user_row[5]
            telegram_id = user_row[1]
            display = first_name or username or str(telegram_id)
            if first_name and last_name:
                display = f"{first_name} {last_name}"
            groups = GroupRepo.list_user_groups_with_roles(user_id)
            groups_with_counts = []
            for gid, title, role, chat_id in groups:
                groups_with_counts.append((gid, title, role, GroupRepo.count_group_events(gid), _role_label(role), chat_id))
            return render('index.html', groups=groups_with_counts, user_info={
                'display': display,
                'username': username,
                'telegram_id': telegram_id,
                'phone': phone,
            }, request=request)
    
    # Если данные из Telegram не пришли, пробуем из переменных окружения
    if TEST_TELEGRAM_ID:
        print(f"Using test Telegram ID: {TEST_TELEGRAM_ID}")
        # Устанавливаем тестовую сессию
        test_user_data = {
            'id': str(TEST_TELEGRAM_ID),
            'username': 'test_user',
            'first_name': 'Test',
            'last_name': 'User'
        }
        set_user_session(request, test_user_data)
        # Получаем пользователя из БД
        user_row = UserRepo.get_by_telegram_id(int(TEST_TELEGRAM_ID))
        if user_row:
            user_id = user_row[0]
            # user_row: (id, telegram_id, username, phone, first_name, last_name)
            username = user_row[2]
            phone = user_row[3]
            first_name = user_row[4]
            last_name = user_row[5]
            telegram_id = user_row[1]
            display = first_name or username or str(telegram_id)
            if first_name and last_name:
                display = f"{first_name} {last_name}"
            groups = GroupRepo.list_user_groups_with_roles(user_id)
            groups_with_counts = []
            for gid, title, role, chat_id in groups:
                groups_with_counts.append((gid, title, role, GroupRepo.count_group_events(gid), _role_label(role), chat_id))
            return render('index.html', groups=groups_with_counts, user_info={
                'display': display,
                'username': username,
                'telegram_id': telegram_id,
                'phone': phone,
            }, request=request)
    
    # Если ничего не получилось, показываем стартовую страницу
    return render('welcome.html', message="Требуется авторизация", user_info=None, request=request)


@app.get('/group/{gid}', response_class=HTMLResponse)
async def group_view(request: Request, gid: int, tab: str = None, page: int = 1, per_page: int = 10):
    urow = _require_user(request)
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
    responsible_ids = {eid for (eid, _, _, responsible_user_id) in events if responsible_user_id == user_id}
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
    # Разделяем мероприятия на активные и архивные
    from datetime import datetime
    now = datetime.now()
    
    all_active_events = []
    all_archived_events = []
    
    for eid, name, time_str, resp_uid in events:
        disp, input_val = _format_time_display(time_str)
        event_data = {
            'id': eid,
            'name': name,
            'time_display': disp,
            'time_input': input_val,
            'responsible_user_id': resp_uid,
            'responsible_name': member_name_map.get(resp_uid) if resp_uid is not None else None,
            'has_any_bookings': len(bookings_map.get(eid, [])) > 0,
        }
        
        # Проверяем, прошло ли мероприятие
        try:
            # Парсим время мероприятия
            event_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
            if event_time < now:
                all_archived_events.append(event_data)
            else:
                all_active_events.append(event_data)
        except ValueError:
            # Если не удалось распарсить время, считаем активным
            all_active_events.append(event_data)
    
    # Применяем пагинацию
    def paginate_events(events_list, current_page, items_per_page):
        total_items = len(events_list)
        total_pages = (total_items + items_per_page - 1) // items_per_page
        start_idx = (current_page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        paginated_events = events_list[start_idx:end_idx]
        
        return {
            'events': paginated_events,
            'total_items': total_items,
            'total_pages': total_pages,
            'current_page': current_page,
            'items_per_page': items_per_page,
            'has_prev': current_page > 1,
            'has_next': current_page < total_pages
        }
    
    # Пагинация для активных мероприятий
    active_pagination = paginate_events(all_active_events, page, per_page)
    active_events = active_pagination['events']
    
    # Пагинация для архивных мероприятий
    archived_pagination = paginate_events(all_archived_events, page, per_page)
    archived_events = archived_pagination['events']
    
    event_count = GroupRepo.count_group_events(gid)
    return render('group.html', group=group, role=_role_label(role or 'participant'), is_admin=is_admin, active_events=active_events, archived_events=archived_events, active_pagination=active_pagination, archived_pagination=archived_pagination, booked_ids=booked_ids, responsible_ids=responsible_ids, display_name=display_name, bookings_map=bookings_map, member_options=member_options, event_count=event_count, active_tab=tab or 'active', current_page=page, per_page=per_page, request=request)


# --- Event CRUD ---
@app.post('/group/{gid}/events/create')
async def create_event(request: Request, gid: int, name: str = Form(...), time: str = Form(...)):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    norm_time = _normalize_dt_local(time)
    new_eid = EventRepo.create(gid, name, norm_time or time)
    # Create event notifications from group defaults
    EventNotificationRepo.create_from_group_defaults(new_eid, gid)
    # Personal notifications will be created when responsible person is assigned
    return RedirectResponse(url=f"/group/{gid}?ok=created", status_code=303)


@app.post('/group/{gid}/events/create-multiple')
async def create_events_multiple(request: Request, gid: int, items: str | None = Form(None), name: list[str] | None = Form(None), time: list[str] | None = Form(None)):
    """
    Supports two payload formats:
    1) Repeated fields: name=.., time=.. (multiple)
    2) Single textarea 'items' with lines: "YYYY-MM-DD HH:MM | Name"
    """
    urow = _require_user(request)
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
                # Personal notifications will be created when responsible person is assigned
                created_any = True
            elif t:
                eid = EventRepo.create(gid, f"Событие {t}", t)
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                # Personal notifications will be created when responsible person is assigned
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
                # Personal notifications will be created when responsible person is assigned
                created_any = True
            else:
                t = _normalize_dt_local(line.strip()) or line.strip()
                eid = EventRepo.create(gid, f"Событие {t}", t)
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                # Personal notifications will be created when responsible person is assigned
                created_any = True
    ok = 'created' if created_any else 'noop'
    return RedirectResponse(url=f"/group/{gid}?ok={ok}", status_code=303)


@app.post('/group/{gid}/events/{eid}/update')
async def update_event(request: Request, gid: int, eid: int, name: str | None = Form(None), time: str | None = Form(None), responsible_user_id: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    if name is not None and name != "":
        EventRepo.update_name(eid, name)
    if time is not None and time != "":
        EventRepo.update_time(eid, _normalize_dt_local(time) or time)
    if responsible_user_id is not None:
        # Get current responsible user before updating
        event = EventRepo.get_by_id(eid)
        old_responsible = event[4] if event else None
        
        # Update responsible user
        new_responsible = responsible_user_id if responsible_user_id != 0 else None
        EventRepo.set_responsible(eid, new_responsible)
        
        # Update personal notifications
        PersonalEventNotificationRepo.update_user_for_event(eid, old_responsible, new_responsible, gid)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?ok=updated", status_code=303)


@app.post('/group/{gid}/events/{eid}/update-from-card')
async def update_event_from_card(request: Request, gid: int, eid: int, name: str | None = Form(None), time: str | None = Form(None), responsible_user_id: int | None = Form(None), tab: str | None = Form(None), page: int | None = Form(None), per_page: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    if name is not None and name != "":
        EventRepo.update_name(eid, name)
    if time is not None and time != "":
        EventRepo.update_time(eid, _normalize_dt_local(time) or time)
    if responsible_user_id is not None:
        # Get current responsible user before updating
        event = EventRepo.get_by_id(eid)
        old_responsible = event[4] if event else None
        
        # Update responsible user
        new_responsible = responsible_user_id if responsible_user_id != 0 else None
        EventRepo.set_responsible(eid, new_responsible)
        
        # Update personal notifications
        PersonalEventNotificationRepo.update_user_for_event(eid, old_responsible, new_responsible, gid)
    # Build redirect URL with all parameters
    params = []
    if tab:
        params.append(f"tab={tab}")
    if page:
        params.append(f"page={page}")
    if per_page:
        params.append(f"per_page={per_page}")
    
    param_string = "&".join(params)
    param_string = f"?ok=event_updated&{param_string}" if param_string else "?ok=event_updated"
    
    return RedirectResponse(url=f"/group/{gid}{param_string}", status_code=303)


# --- Settings: notifications & admins ---
@app.get('/group/{gid}/settings', response_class=HTMLResponse)
async def group_settings(request: Request, gid: int):
    urow = _require_user(request)
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
async def add_group_notification(request: Request, gid: int, time_before: int, time_unit: str, message_text: str | None = None):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    NotificationRepo.add_notification(gid, time_before, time_unit, message_text)
    return RedirectResponse(url=f"/group/{gid}/settings?tg_id={tg_id}", status_code=303)

@app.get('/group/{gid}/settings/notifications/add-text')
async def add_group_notification_text_get(request: Request, gid: int):
    tg_id = request.query_params.get('tg_id')
    print(f"GET add_group_notification_text: gid={gid}, tg_id={tg_id}")
    return RedirectResponse(f"/group/{gid}/settings?ok=method_error", status_code=303)

@app.post('/group/{gid}/settings/notifications/add-text')
async def add_group_notification_text(request: Request, gid: int, notification_text: str = Form(...), message_text: str = Form(""), tab: str | None = Form(None)):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
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
    tab_param = f"&tab={tab}" if tab else "&tab=group"
    return RedirectResponse(f"/group/{gid}/settings?ok=notification_added&tg_id={tg_id}{tab_param}", status_code=303)

@app.post('/group/{gid}/settings/personal-notifications/add-text')
async def add_personal_notification_text(request: Request, gid: int, notification_text: str = Form(...), message_text: str = Form(""), tab: str | None = Form(None)):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
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
    tab_param = f"&tab={tab}" if tab else "&tab=personal"
    return RedirectResponse(f"/group/{gid}/settings?ok=personal_notification_added&tg_id={tg_id}{tab_param}", status_code=303)

@app.post('/group/{gid}/settings/personal-notifications/{nid}/delete')
async def delete_personal_notification(request: Request, gid: int, nid: int, tab: str | None = Form(None)):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    NotificationRepo.delete_notification(nid)
    tab_param = f"&tab={tab}" if tab else "&tab=personal"
    return RedirectResponse(f"/group/{gid}/settings?ok=personal_notification_deleted&tg_id={tg_id}{tab_param}", status_code=303)

@app.post('/group/{gid}/send-message')
async def send_message_to_user(request: Request, gid: int, recipient_id: int = Form(...), message: str = Form(...)):
    urow = _require_user(request)
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
async def send_message_to_group(request: Request, gid: int, message: str = Form(...)):
    tg_id = request.query_params.get('tg_id')
    print(f"send-group-message: tg_id={tg_id}, gid={gid}, message={message}")
    urow = _require_user(request)
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
async def delete_group_notification(request: Request, gid: int, nid: int, tab: str | None = Form(None)):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    NotificationRepo.delete_notification(nid)
    tab_param = f"&tab={tab}" if tab else ""
    return RedirectResponse(url=f"/group/{gid}/settings?ok=notification_deleted&tg_id={tg_id}{tab_param}", status_code=303)


@app.get('/group/{gid}/events/{eid}/settings', response_class=HTMLResponse)
async def event_settings(request: Request, gid: int, eid: int):
    urow = _require_user(request)
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
    
    # Get sent status for notifications
    event_notifications_sent = DispatchLogRepo.get_sent_status_for_event_notifications(eid)
    personal_notifications_sent = DispatchLogRepo.get_sent_status_for_personal_notifications(eid, user_id)
    
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
    
    # Get current user info for personal notifications
    current_user_telegram_id = urow[1]  # urow[1] is telegram_id
    current_user_display_name = member_name_map.get(user_id, f"@{urow[2]}" if urow[2] else str(current_user_telegram_id))
    
    return render('event_settings.html', group=group, event=event, members=member_options, event_notifications=event_notifications, personal_notifications=personal_notifications, event_notifications_sent=event_notifications_sent, personal_notifications_sent=personal_notifications_sent, role=role, responsible_name=responsible_name, event_time_display=event_time_display, current_user_telegram_id=current_user_telegram_id, current_user_display_name=current_user_display_name, _calculate_notification_time=_calculate_notification_time, request=request)


@app.post('/group/{gid}/events/{eid}/notifications/add')
async def add_event_notification(request: Request, gid: int, eid: int, time_before: int, time_unit: str, message_text: str | None = None):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    EventNotificationRepo.add_notification(eid, time_before, time_unit, message_text)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/notifications/add-text')
async def add_event_notification_text(request: Request, gid: int, eid: int, notification_text: str = Form(...), message_text: str = Form("")):
    tg_id = request.query_params.get('tg_id')
    print(f"add_event_notification_text called: gid={gid}, eid={eid}, tg_id={tg_id}")
    print(f"notification_text='{notification_text}', message_text='{message_text}'")
    urow = _require_user(request)
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
async def add_event_notification_absolute_get(request: Request, gid: int, eid: int):
    tg_id = request.query_params.get('tg_id')
    print(f"GET add_event_notification_absolute: gid={gid}, eid={eid}, tg_id={tg_id}")
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=method_error", status_code=303)

@app.post('/group/{gid}/events/{eid}/notifications/add-absolute')
async def add_event_notification_absolute(request: Request, gid: int, eid: int, datetime_str: str = Form(...), message_text: str = Form(None)):
    tg_id = request.query_params.get('tg_id')
    print(f"POST add_event_notification_absolute: gid={gid}, eid={eid}, tg_id={tg_id}, datetime={datetime_str}, message_text={message_text}")
    """Add event notification by exact datetime. Stored as delta in minutes."""
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    event = EventRepo.get_by_id(eid)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M")
    at_norm = _normalize_dt_local(datetime_str)
    try:
        at_dt = datetime.strptime(at_norm, "%Y-%m-%d %H:%M") if at_norm else None
    except Exception as e:
        print(f"Error parsing datetime: {e}, at_norm: {at_norm}")
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
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?ok=notification_added&tg_id={tg_id}", status_code=303)


@app.post('/group/{gid}/events/{eid}/notifications/{nid}/delete')
async def delete_event_notification(request: Request, gid: int, eid: int, nid: int):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    EventNotificationRepo.delete_notification(nid)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?ok=notification_deleted&tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/personal-notifications/add-text')
async def add_personal_event_notification_text(request: Request, gid: int, eid: int, notification_text: str = Form(...), message_text: str = Form("")):
    tg_id = request.query_params.get('tg_id')
    print(f"add_personal_event_notification_text called: gid={gid}, eid={eid}, tg_id={tg_id}")
    print(f"notification_text='{notification_text}', message_text='{message_text}'")
    urow = _require_user(request)
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
    
    # Add notification (repository handles duplicate checking internally)
    PersonalEventNotificationRepo.add_notification(user_id, eid, time_before, time_unit, message_text_tail)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=personal_notification_added&tab=personal&tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/personal-notifications/add-absolute')
async def add_personal_event_notification_absolute(request: Request, gid: int, eid: int, datetime_str: str = Form(...), message_text: str = Form(None)):
    """Add personal event notification by exact datetime. Stored as delta in minutes."""
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    
    # Convert datetime to minutes before event
    event = EventRepo.get_by_id(eid)
    if not event:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=error", status_code=303)
    
    event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M")
    at_norm = _normalize_dt_local(datetime_str)
    try:
        notification_time = datetime.strptime(at_norm, "%Y-%m-%d %H:%M") if at_norm else None
    except Exception as e:
        print(f"Error parsing datetime: {e}, at_norm: {at_norm}")
        notification_time = None
    if not notification_time:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
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
    
    # Add notification (repository handles duplicate checking internally)
    PersonalEventNotificationRepo.add_notification(user_id, eid, time_before, time_unit, message_text)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=personal_notification_added&tab=personal&tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/personal-notifications/{nid}/delete')
async def delete_personal_event_notification(request: Request, gid: int, eid: int, nid: int):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    PersonalEventNotificationRepo.delete_notification(nid, user_id)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=personal_notification_deleted&tab=personal&tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/delete')
async def delete_event(request: Request, gid: int, eid: int, tab: str | None = Form(None), page: int | None = Form(None), per_page: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    
    # Delete the event (CASCADE will handle notifications)
    print(f"DELETE EVENT: eid={eid}, gid={gid}, user_id={user_id}")
    result = EventRepo.delete(eid)
    print(f"DELETE RESULT: {result}")
    
    # Build redirect URL with all parameters
    params = []
    if tab:
        params.append(f"tab={tab}")
    if page:
        params.append(f"page={page}")
    if per_page:
        params.append(f"per_page={per_page}")
    
    param_string = "&".join(params)
    param_string = f"?ok=event_deleted&{param_string}" if param_string else "?ok=event_deleted"
    
    return RedirectResponse(f"/group/{gid}{param_string}", status_code=303)

@app.post('/group/{gid}/events/{eid}/book')
async def book_event(request: Request, gid: int, eid: int, tab: str | None = Form(None), page: int | None = Form(None), per_page: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    
    print(f"=== BOOK EVENT START ===")
    print(f"BOOK EVENT: eid={eid}, gid={gid}, user_id={user_id}")
    
    # Check if event exists
    event = EventRepo.get_by_id(eid)
    print(f"EVENT BEFORE: {event}")
    if not event:
        print(f"BOOKING ERROR: event not found")
        return RedirectResponse(f"/group/{gid}?ok=booking_error", status_code=303)
    
    # Check if user is already responsible
    if event[4] == user_id:  # user is already responsible
        print(f"BOOKING ERROR: user {user_id} is already responsible for event {eid}")
        return RedirectResponse(f"/group/{gid}?ok=booking_error", status_code=303)
    
    # Book the event - use set_responsible to create notifications
    print(f"UPDATING EVENT: setting responsible to {user_id}")
    EventRepo.set_responsible(eid, user_id)
    
    # Verify the update
    event_after = EventRepo.get_by_id(eid)
    print(f"EVENT AFTER: {event_after}")
    
    BookingRepo.add_booking(user_id, eid)
    
    print(f"BOOKING SUCCESS")
    # Build redirect URL with all parameters
    params = []
    if tab:
        params.append(f"tab={tab}")
    if page:
        params.append(f"page={page}")
    if per_page:
        params.append(f"per_page={per_page}")
    
    param_string = "&".join(params)
    param_string = f"?ok=event_booked&{param_string}" if param_string else "?ok=event_booked"
    
    return RedirectResponse(f"/group/{gid}{param_string}", status_code=303)


@app.get('/group/{gid}/events/{eid}', response_class=HTMLResponse)
async def event_detail(request: Request, gid: int, eid: int):
    urow = _require_user(request)
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
async def unbook_event(request: Request, gid: int, eid: int, tab: str | None = Form(None), page: int | None = Form(None), per_page: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    
    print(f"=== UNBOOK EVENT START ===")
    print(f"UNBOOK EVENT: eid={eid}, gid={gid}, user_id={user_id}")
    
    # Get event and check if user is responsible
    event = EventRepo.get_by_id(eid)
    print(f"EVENT BEFORE: {event}")
    if event and event[4] == user_id:  # user is responsible
        print(f"REMOVING RESPONSIBILITY: user {user_id} is responsible for event {eid}")
        # Remove responsibility (set to None) - use set_responsible to handle notifications
        EventRepo.set_responsible(eid, None)
        
        # Verify the update
        event_after = EventRepo.get_by_id(eid)
        print(f"EVENT AFTER: {event_after}")
    else:
        print(f"USER NOT RESPONSIBLE: user {user_id} is not responsible for event {eid}")
    
    BookingRepo.remove_booking(user_id, eid)
    # Build redirect URL with all parameters
    params = []
    if tab:
        params.append(f"tab={tab}")
    if page:
        params.append(f"page={page}")
    if per_page:
        params.append(f"per_page={per_page}")
    
    param_string = "&".join(params)
    param_string = f"?ok=unbooked&{param_string}" if param_string else "?ok=unbooked"
    
    return RedirectResponse(url=f"/group/{gid}{param_string}", status_code=303)


@app.post('/group/{gid}/display-name/set')
async def set_display_name(request: Request, gid: int, display_name: str = Form(...)):
    urow = _require_user(request)
    user_id = urow[0]
    DisplayNameRepo.set_display_name(gid, user_id, display_name)
    return RedirectResponse(url=f"/group/{gid}?ok=name_saved", status_code=303)

@app.post('/group/{gid}/settings/member/{uid}/display-name')
async def update_member_display_name(request: Request, gid: int, uid: int, display_name: str = Form(...)):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    # No additional encoding needed, FastAPI already handles UTF-8
    DisplayNameRepo.set_display_name(gid, uid, display_name)
    return RedirectResponse(f"/group/{gid}/settings?ok=member_name_saved", status_code=303)

@app.post('/group/{gid}/settings/member/{uid}/make-admin')
async def make_member_admin(request: Request, gid: int, uid: int):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    # Add as pending admin
    RoleRepo.add_pending_admin(gid, str(uid), 'id', user_id)
    return RedirectResponse(f"/group/{gid}/settings?ok=member_made_admin", status_code=303)


@app.post('/group/{gid}/delete')
async def delete_group(request: Request, gid: int):
    """Delete group and all associated data"""
    tg_id = request.query_params.get('tg_id')
    print(f"DELETE GROUP: gid={gid}, tg_id={tg_id}")
    urow = _require_user(request)
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


