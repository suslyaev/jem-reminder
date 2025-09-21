from fastapi import FastAPI, Request, HTTPException, Form
from typing import List
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
from services.repositories import UserRepo, GroupRepo, EventRepo, RoleRepo, PersonalEventNotificationRepo, NotificationRepo, BookingRepo, DisplayNameRepo, EventNotificationRepo, DispatchLogRepo, EventTemplateRepo, TemplateRoleRequirementRepo, TemplateGenerationRepo, TemplateGenerator, EventRoleRequirementRepo, EventRoleAssignmentRepo, get_conn
from services.repositories import AuditLogRepo

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
            # Blocked users are not allowed to use the app
            try:
                if len(user_row) >= 7 and user_row[6]:
                    raise HTTPException(status_code=403, detail="User is blocked")
            except Exception:
                pass
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
            try:
                if len(user_row) >= 7 and user_row[6]:
                    raise HTTPException(status_code=403, detail="User is blocked")
            except Exception:
                pass
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
    # Allow by group role OR by global superadmin telegram id
    role = RoleRepo.get_user_role(user_id, group_id)
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    if role in ("owner", "admin", "superadmin"):
        return role
    # Check telegram id
    u = UserRepo.get_by_id(user_id)
    if u and CFG_SA and u[1] == CFG_SA:
        return "superadmin"
    raise HTTPException(status_code=403, detail="Admin permissions required")


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
            try:
                if len(user_row) >= 7 and user_row[6]:
                    clear_user_session(request)
                    return render('welcome.html', message="Доступ запрещён", user_info=None, request=request)
            except Exception:
                pass
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
            # If superadmin, show all groups; else only groups with roles
            is_super = (user_row[1] == SUPERADMIN_ID) if SUPERADMIN_ID else False
            if is_super:
                groups_all = GroupRepo.list_all()
                # Show actual role in group for superadmin (may be None if not a member)
                groups = [(gid, title, RoleRepo.get_user_role(user_id, gid), chat_id) for (gid, title, chat_id) in groups_all]
            else:
                groups = GroupRepo.list_user_groups_with_roles(user_id)
            groups_with_counts = []
            for gid, title, role, chat_id in groups:
                role_label = _role_label(role) if role else 'Отсутствует'
                groups_with_counts.append((gid, title, role, GroupRepo.count_group_events(gid), role_label, chat_id))
            # If superadmin, also load users list for admin panel
            users = []
            if is_super:
                users = UserRepo.list_with_groups()
            # Load audit if superadmin and tab=audit
            audit_rows = []
            audit_total = 0
            audit_page = int(request.query_params.get('page') or 1)
            audit_per_page = int(request.query_params.get('per_page') or 50)
            audit_filters = {'group_id': request.query_params.get('group_id'), 'event_id': request.query_params.get('event_id')}
            if is_super and (request.query_params.get('tab') == 'audit'):
                try:
                    from services.repositories import AuditLogRepo
                    # Load groups list
                    groups_all = GroupRepo.list_all()
                    audit_groups = [(gid, title) for (gid, title, _chat) in groups_all]
                    # Load events for selected group
                    audit_events = []
                    gflt = int(audit_filters['group_id']) if audit_filters['group_id'] else None
                    eflt = int(audit_filters['event_id']) if audit_filters['event_id'] else None
                    if gflt:
                        try:
                            evs = EventRepo.list_by_group(gflt)
                            audit_events = [(eid, name) for (eid, name, _t, _r) in evs]
                        except Exception:
                            audit_events = []
                    (audit_rows, audit_total) = AuditLogRepo.list(page=audit_page, per_page=audit_per_page, group_id=gflt, event_id=eflt)
                    # Enrich rows for display
                    def _fmt_ts(s: str) -> str:
                        from datetime import datetime as _dt
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                            try:
                                return _dt.strptime(s, fmt).strftime("%d.%m.%Y %H:%M:%S")
                            except Exception:
                                continue
                        return s
                    audit_items = []
                    action_map = {
                        'event_created': 'Создание',
                        'event_name_updated': 'Наименование',
                        'event_time_updated': 'Дата и время',
                        'event_responsible_updated': 'Ответственный',
                        'event_deleted': 'Удаление',
                        'event_booked': 'Бронь',
                        'event_unbooked': 'Отмена брони',
                        'role_booked': 'Бронь',
                        'role_unbooked': 'Отмена брони',
                        'role_requirement_added': 'Роль добавлена',
                        'role_requirement_removed': 'Роль удалена',
                        'notify_group': 'Групповое сообщение',
                        'notify_personal': 'Личное сообщение',
                        'group_notification_created': 'Групповое оповещение',
                        'group_notification_deleted': 'Групповое оповещение',
                        'personal_notification_created': 'Личное оповещение',
                        'personal_notification_deleted': 'Личное оповещение',
                        'member_display_name_updated': 'Имя участника',
                        'member_role_updated': 'Роль участника',
                        'member_removed': 'Удаление участника',
                        'member_added': 'Добавление участника',
                        'group_deleted': 'Удаление группы',
                    }
                    for (_id, created_at, uid, action, gid_a, eid_a, oldv, newv) in audit_rows:
                        # user label
                        user_label = str(uid) if uid else '—'
                        user_tid = None
                        try:
                            u = UserRepo.get_by_id(uid) if uid else None
                            if u:
                                _iid, _tid, _uname, _phone, _first, _last, *_rest = u
                                user_tid = _tid
                                disp = (_first or '')
                                if _last:
                                    disp = f"{disp} {_last}".strip()
                                if not disp:
                                    disp = f"@{_uname}" if _uname else (str(_tid) if _tid else str(uid))
                                user_label = f"{disp} ({_tid})" if _tid else disp
                        except Exception:
                            pass
                        # group
                        group_title = None
                        try:
                            g = GroupRepo.get_by_id(gid_a) if gid_a else None
                            group_title = g[2] if g else None
                        except Exception:
                            group_title = None
                        # event
                        event_name = None
                        try:
                            ev = EventRepo.get_by_id(eid_a) if eid_a else None
                            event_name = ev[1] if ev else None
                        except Exception:
                            event_name = None
                        audit_items.append({
                            'id': _id,
                            'ts': _fmt_ts(created_at),
                            'action': action,
                            'action_ru': action_map.get(action, action),
                            'user_label': user_label,
                            'group_id': gid_a,
                            'group_title': group_title,
                            'event_id': eid_a,
                            'event_name': event_name,
                            'old': oldv,
                            'new': newv,
                        })
                except Exception:
                    audit_rows, audit_total = [], 0
                    audit_groups, audit_events = [], []
                    audit_items = []
            return render('index.html', groups=groups_with_counts, user_info={
                'display': display,
                'username': username,
                'telegram_id': telegram_id,
                'phone': phone,
            }, users=users, is_superadmin=is_super, audit_rows=audit_rows, audit_items=(audit_items if (request.query_params.get('tab') == 'audit') else []), audit_total=audit_total, audit_page=audit_page, audit_per_page=audit_per_page, audit_filters=audit_filters, audit_groups=(audit_groups if (request.query_params.get('tab') == 'audit') else []), audit_events=(audit_events if (request.query_params.get('tab') == 'audit') else []), request=request)
    
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
            try:
                if len(user_row) >= 7 and user_row[6]:
                    clear_user_session(request)
                    return render('welcome.html', message="Доступ запрещён", user_info=None, request=request)
            except Exception:
                pass
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
            # If superadmin, show all groups; else only groups with roles
            is_super = (user_row[1] == SUPERADMIN_ID) if SUPERADMIN_ID else False
            if is_super:
                groups_all = GroupRepo.list_all()
                groups = [(gid, title, RoleRepo.get_user_role(user_id, gid), chat_id) for (gid, title, chat_id) in groups_all]
            else:
                groups = GroupRepo.list_user_groups_with_roles(user_id)
            groups_with_counts = []
            for gid, title, role, chat_id in groups:
                role_label = _role_label(role) if role else 'Отсутствует'
                groups_with_counts.append((gid, title, role, GroupRepo.count_group_events(gid), role_label, chat_id))
            # If superadmin, also load users list for admin panel
            users = []
            if is_super:
                users = UserRepo.list_with_groups()
            audit_rows = []
            audit_total = 0
            audit_page = int(request.query_params.get('page') or 1)
            audit_per_page = int(request.query_params.get('per_page') or 50)
            audit_filters = {'group_id': request.query_params.get('group_id'), 'event_id': request.query_params.get('event_id')}
            if is_super and (request.query_params.get('tab') == 'audit'):
                try:
                    from services.repositories import AuditLogRepo
                    groups_all = GroupRepo.list_all()
                    audit_groups = [(gid, title) for (gid, title, _chat) in groups_all]
                    audit_events = []
                    gflt = int(audit_filters['group_id']) if audit_filters['group_id'] else None
                    eflt = int(audit_filters['event_id']) if audit_filters['event_id'] else None
                    if gflt:
                        try:
                            evs = EventRepo.list_by_group(gflt)
                            audit_events = [(eid, name) for (eid, name, _t, _r) in evs]
                        except Exception:
                            audit_events = []
                    (audit_rows, audit_total) = AuditLogRepo.list(page=audit_page, per_page=audit_per_page, group_id=gflt, event_id=eflt)
                    def _fmt_ts(s: str) -> str:
                        from datetime import datetime as _dt
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                            try:
                                return _dt.strptime(s, fmt).strftime("%d.%m.%Y %H:%M:%S")
                            except Exception:
                                continue
                        return s
                    audit_items = []
                    action_map = {
                        'event_created': 'Создание',
                        'event_name_updated': 'Наименование',
                        'event_time_updated': 'Дата и время',
                        'event_responsible_updated': 'Ответственный',
                        'event_deleted': 'Удаление',
                        'event_booked': 'Бронь',
                        'event_unbooked': 'Отмена брони',
                        'role_booked': 'Бронь',
                        'role_unbooked': 'Отмена брони',
                        'role_requirement_added': 'Роль добавлена',
                        'role_requirement_removed': 'Роль удалена',
                        'notify_group': 'Групповое сообщение',
                        'notify_personal': 'Личное сообщение',
                        'group_notification_created': 'Групповое оповещение',
                        'group_notification_deleted': 'Групповое оповещение',
                        'personal_notification_created': 'Личное оповещение',
                        'personal_notification_deleted': 'Личное оповещение',
                        'member_display_name_updated': 'Имя участника',
                        'member_role_updated': 'Роль участника',
                        'member_removed': 'Удаление участника',
                        'member_added': 'Добавление участника',
                        'group_deleted': 'Удаление группы',
                    }
                    for (_id, created_at, uid, action, gid_a, eid_a, oldv, newv) in audit_rows:
                        user_label = str(uid) if uid else '—'
                        try:
                            u = UserRepo.get_by_id(uid) if uid else None
                            if u:
                                _iid, _tid, _uname, _phone, _first, _last, *_rest = u
                                disp = (_first or '')
                                if _last:
                                    disp = f"{disp} {_last}".strip()
                                if not disp:
                                    disp = f"@{_uname}" if _uname else (str(_tid) if _tid else str(uid))
                                user_label = f"{disp} ({_tid})" if _tid else disp
                        except Exception:
                            pass
                        group_title = None
                        try:
                            g = GroupRepo.get_by_id(gid_a) if gid_a else None
                            group_title = g[2] if g else None
                        except Exception:
                            group_title = None
                        event_name = None
                        try:
                            ev = EventRepo.get_by_id(eid_a) if eid_a else None
                            event_name = ev[1] if ev else None
                        except Exception:
                            event_name = None
                        audit_items.append({
                            'id': _id,
                            'ts': _fmt_ts(created_at),
                            'action': action,
                            'action_ru': action_map.get(action, action),
                            'user_label': user_label,
                            'group_id': gid_a,
                            'group_title': group_title,
                            'event_id': eid_a,
                            'event_name': event_name,
                            'old': oldv,
                            'new': newv,
                        })
                except Exception:
                    audit_rows, audit_total = [], 0
                    audit_groups, audit_events = [], []
                    audit_items = []
            return render('index.html', groups=groups_with_counts, user_info={
                'display': display,
                'username': username,
                'telegram_id': telegram_id,
                'phone': phone,
            }, users=users, is_superadmin=is_super, audit_rows=audit_rows, audit_items=(audit_items if (request.query_params.get('tab') == 'audit') else []), audit_total=audit_total, audit_page=audit_page, audit_per_page=audit_per_page, audit_filters=audit_filters, audit_groups=(audit_groups if (request.query_params.get('tab') == 'audit') else []), audit_events=(audit_events if (request.query_params.get('tab') == 'audit') else []), request=request)
    
    # Если ничего не получилось, показываем стартовую страницу
    return render('welcome.html', message="Требуется авторизация", user_info=None, request=request)


@app.get('/group/{gid}', response_class=HTMLResponse)
async def group_view(request: Request, gid: int, tab: str = None, page: int = 1, per_page: int = 10):
    urow = _require_user(request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    # Superadmin bypass for access
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    is_superadmin_req = (urow[1] == CFG_SA) if CFG_SA else False
    if role is None and not is_superadmin_req:
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
    # For superadmin not in group, show role as None (Отсутствует), но сохраняем права администратора
    effective_role = role if role else None
    # Global superadmin should also see admin UI
    is_admin = (role in ("owner", "admin", "superadmin")) or is_superadmin_req
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

    # Build audit labels for events
    audit_labels = {}
    try:
        for eid, _, _, _ in events:
            c_uid, c_at, u_uid, u_at = EventRepo.get_audit(eid)
            def _fmt_dt_ru(dt_str: str | None) -> str:
                if not dt_str:
                    return '—'
                from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                # Parse as naive then treat as UTC (SQLite datetime('now') is UTC), convert to Europe/Moscow
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                    try:
                        d_naive = _dt.strptime(dt_str, fmt)
                        d_utc = d_naive.replace(tzinfo=_tz.utc)
                        try:
                            from zoneinfo import ZoneInfo
                            tz_msk = ZoneInfo('Europe/Moscow')
                            d_local = d_utc.astimezone(tz_msk)
                        except Exception:
                            # Fallback fixed offset +3
                            d_local = d_utc + _td(hours=3)
                        return d_local.strftime("%d.%m.%Y %H:%M:%S")
                    except Exception:
                        continue
                return dt_str
            def label_for(uid: int | None) -> str:
                if not uid:
                    return '—'
                dn = DisplayNameRepo.get_display_name(gid, uid)
                if dn:
                    return dn
                u = UserRepo.get_by_id(uid)
                if u and u[2]:
                    return f"@{u[2]}"
                return str(uid)
            audit_labels[eid] = {
                'created_by': label_for(c_uid),
                'created_at': _fmt_dt_ru(c_at),
                'updated_by': label_for(u_uid),
                'updated_at': _fmt_dt_ru(u_at),
            }
    except Exception:
        audit_labels = {}
    # Разделяем мероприятия на активные и архивные
    from datetime import datetime
    now = datetime.now()
    
    all_active_events = []
    all_archived_events = []
    
    for eid, name, time_str, resp_uid in events:
        disp, input_val = _format_time_display(time_str)
        # Load roles and assignments for this event
        try:
            role_requirements = EventRoleRequirementRepo.list_for_event(eid)
        except Exception:
            role_requirements = []
        try:
            role_assignments = EventRoleAssignmentRepo.list_for_event(eid)
        except Exception:
            role_assignments = []
        # Map role -> assigned user_id (first assignment if multiple present)
        assignments_map = {}
        for rname, uid in role_assignments:
            if rname not in assignments_map:
                assignments_map[rname] = uid
        # Build label for assigned users (display name -> username -> telegram_id)
        def _user_label(uid: int | None) -> str:
            if not uid:
                return ''
            dn = DisplayNameRepo.get_display_name(gid, uid)
            if dn:
                return dn
            u = UserRepo.get_by_id(uid)
            if u:
                # Some versions return 7 fields (including blocked). Accept extra.
                _iid, _tid, _uname, _phone, _first, _last, *_rest = u
                if _uname:
                    return f"@{_uname}"
                if _first or _last:
                    return f"{(_first or '').strip()} {(_last or '').strip()}".strip()
                if _tid:
                    return str(_tid)
            return str(uid)
        assignments_label_map = { r: _user_label(uid) for r, uid in assignments_map.items() }
        # Whether current user already has any role in this event
        current_user_has_role = any(uid == user_id for _, uid in role_assignments)
        # Read allow_multi_roles_per_user flag
        allow_multi_roles_per_user = 0
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT allow_multi_roles_per_user FROM events WHERE id = ?", (eid,))
                row = cur.fetchone()
                allow_multi_roles_per_user = row[0] if row else 0
        except Exception:
            allow_multi_roles_per_user = 0
        event_data = {
            'id': eid,
            'name': name,
            'time_display': disp,
            'time_input': input_val,
            'responsible_user_id': resp_uid,
            'responsible_name': member_name_map.get(resp_uid) if resp_uid is not None else None,
            'has_any_bookings': len(bookings_map.get(eid, [])) > 0,
            'role_requirements': role_requirements,
            'role_assignments': assignments_map,
            'role_assignment_labels': assignments_label_map,
            'allow_multi_roles_per_user': allow_multi_roles_per_user,
            'current_user_has_role': 1 if current_user_has_role else 0,
        }
        
        # Проверяем, прошло ли мероприятие
        try:
            # Парсим время мероприятия (поддерживаем формат с секундами)
            try:
                event_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # Если формат без секунд, пробуем старый формат
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
    # Role label: show localized role if present; otherwise show "Отсутствует"
    role_label = _role_label(role) if role else 'Отсутствует'
    return render('group.html', group=group, role=role_label, is_admin=is_admin, active_events=active_events, archived_events=archived_events, active_pagination=active_pagination, archived_pagination=archived_pagination, booked_ids=booked_ids, responsible_ids=responsible_ids, display_name=display_name, bookings_map=bookings_map, member_options=member_options, member_name_map=member_name_map, event_count=event_count, active_tab=tab or 'active', current_page=page, per_page=per_page, request=request, current_user_id=user_id, audit_labels=audit_labels)


# --- Event CRUD ---
@app.post('/group/{gid}/events/create')
async def create_event(request: Request, gid: int, name: str = Form(...), time: str = Form(...)):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    norm_time = _normalize_dt_local(time)
    new_eid = EventRepo.create(gid, name, norm_time or time, created_by_user_id=user_id)
    try:
        AuditLogRepo.add('event_created', user_id=user_id, group_id=gid, event_id=new_eid, new_value=name)
    except Exception:
        pass
    # Create event notifications from group defaults
    EventNotificationRepo.create_from_group_defaults(new_eid, gid)
    # Apply group role templates
    try:
        from services.repositories import GroupRoleTemplateRepo
        for rname, req in GroupRoleTemplateRepo.list(gid):
            EventRoleRequirementRepo.set_for_event(new_eid, rname, int(req or 1))
    except Exception:
        pass
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
                eid = EventRepo.create(gid, n, t, created_by_user_id=user_id)
                try:
                    AuditLogRepo.add('event_created', user_id=user_id, group_id=gid, event_id=eid, new_value=n)
                except Exception:
                    pass
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                # Apply group role templates
                try:
                    from services.repositories import GroupRoleTemplateRepo
                    for rname, req in GroupRoleTemplateRepo.list(gid):
                        EventRoleRequirementRepo.set_for_event(eid, rname, int(req or 1))
                except Exception:
                    pass
                # Personal notifications will be created when responsible person is assigned
                created_any = True
            elif t:
                eid = EventRepo.create(gid, f"Событие {t}", t, created_by_user_id=user_id)
                try:
                    AuditLogRepo.add('event_created', user_id=user_id, group_id=gid, event_id=eid, new_value=f"Событие {t}")
                except Exception:
                    pass
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                try:
                    from services.repositories import GroupRoleTemplateRepo
                    for rname, req in GroupRoleTemplateRepo.list(gid):
                        EventRoleRequirementRepo.set_for_event(eid, rname, int(req or 1))
                except Exception:
                    pass
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
                eid = EventRepo.create(gid, name_part.strip(), _normalize_dt_local(time_part.strip()) or time_part.strip(), created_by_user_id=user_id)
                try:
                    AuditLogRepo.add('event_created', user_id=user_id, group_id=gid, event_id=eid, new_value=name_part.strip())
                except Exception:
                    pass
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                try:
                    from services.repositories import GroupRoleTemplateRepo
                    for rname, req in GroupRoleTemplateRepo.list(gid):
                        EventRoleRequirementRepo.set_for_event(eid, rname, int(req or 1))
                except Exception:
                    pass
                # Personal notifications will be created when responsible person is assigned
                created_any = True
            else:
                t = _normalize_dt_local(line.strip()) or line.strip()
                eid = EventRepo.create(gid, f"Событие {t}", t, created_by_user_id=user_id)
                try:
                    AuditLogRepo.add('event_created', user_id=user_id, group_id=gid, event_id=eid, new_value=f"Событие {t}")
                except Exception:
                    pass
                EventNotificationRepo.create_from_group_defaults(eid, gid)
                try:
                    from services.repositories import GroupRoleTemplateRepo
                    for rname, req in GroupRoleTemplateRepo.list(gid):
                        EventRoleRequirementRepo.set_for_event(eid, rname, int(req or 1))
                except Exception:
                    pass
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
        try:
            old = EventRepo.get_by_id(eid)
            old_name = old[1] if old else None
        except Exception:
            old_name = None
        EventRepo.update_name(eid, name, updated_by_user_id=user_id)
        try:
            if (old_name or '') != (name or ''):
                AuditLogRepo.add('event_name_updated', user_id=user_id, group_id=gid, event_id=eid, old_value=old_name, new_value=name)
        except Exception:
            pass
    if time is not None and time != "":
        try:
            old = EventRepo.get_by_id(eid)
            old_time = old[2] if old else None
        except Exception:
            old_time = None
        new_time = _normalize_dt_local(time) or time
        EventRepo.update_time(eid, new_time, updated_by_user_id=user_id)
        try:
            if (old_time or '') != (new_time or ''):
                AuditLogRepo.add('event_time_updated', user_id=user_id, group_id=gid, event_id=eid, old_value=old_time, new_value=new_time)
        except Exception:
            pass
    if responsible_user_id is not None:
        # Get current responsible user before updating
        event = EventRepo.get_by_id(eid)
        old_responsible = event[4] if event else None
        
        # Update responsible user
        new_responsible = responsible_user_id if responsible_user_id != 0 else None
        EventRepo.set_responsible(eid, new_responsible)
        try:
            if (old_responsible or None) != (new_responsible or None):
                AuditLogRepo.add('event_responsible_updated', user_id=user_id, group_id=gid, event_id=eid, old_value=str(old_responsible or ''), new_value=str(new_responsible or ''))
        except Exception:
            pass
        
        # Update personal notifications
        PersonalEventNotificationRepo.update_user_for_event(eid, old_responsible, new_responsible, gid)
    return RedirectResponse(url=f"/group/{gid}/events/{eid}/settings?ok=updated", status_code=303)


@app.post('/group/{gid}/events/{eid}/update-from-card')
async def update_event_from_card(request: Request, gid: int, eid: int, name: str | None = Form(None), time: str | None = Form(None), responsible_user_id: int | None = Form(None), tab: str | None = Form(None), page: int | None = Form(None), per_page: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    if name is not None and name != "":
        try:
            old = EventRepo.get_by_id(eid)
            old_name = old[1] if old else None
        except Exception:
            old_name = None
        EventRepo.update_name(eid, name, updated_by_user_id=user_id)
        try:
            if (old_name or '') != (name or ''):
                AuditLogRepo.add('event_name_updated', user_id=user_id, group_id=gid, event_id=eid, old_value=old_name, new_value=name)
        except Exception:
            pass
    if time is not None and time != "":
        try:
            old = EventRepo.get_by_id(eid)
            old_time = old[2] if old else None
        except Exception:
            old_time = None
        new_time = _normalize_dt_local(time) or time
        EventRepo.update_time(eid, new_time, updated_by_user_id=user_id)
        try:
            if (old_time or '') != (new_time or ''):
                AuditLogRepo.add('event_time_updated', user_id=user_id, group_id=gid, event_id=eid, old_value=old_time, new_value=new_time)
        except Exception:
            pass
    if responsible_user_id is not None:
        # Get current responsible user before updating
        event = EventRepo.get_by_id(eid)
        old_responsible = event[4] if event else None
        
        # Update responsible user
        new_responsible = responsible_user_id if responsible_user_id != 0 else None
        EventRepo.set_responsible(eid, new_responsible)
        try:
            if (old_responsible or None) != (new_responsible or None):
                AuditLogRepo.add('event_responsible_updated', user_id=user_id, group_id=gid, event_id=eid, old_value=str(old_responsible or ''), new_value=str(new_responsible or ''))
        except Exception:
            pass
        
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
    # Allow all users to see settings; restrict admin functions by role / global superadmin
    role = RoleRepo.get_user_role(user_id, gid)
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    is_super = (urow[1] == CFG_SA) if CFG_SA else False
    group = GroupRepo.get_by_id(gid)
    notifications = NotificationRepo.list_notifications(gid) if (role in ['admin', 'owner', 'superadmin'] or is_super) else []
    # Load group role templates
    from services.repositories import GroupRoleTemplateRepo
    role_templates = GroupRoleTemplateRepo.list(gid) if (role in ['admin', 'owner', 'superadmin'] or is_super) else []
    # Load personal notification templates for this group
    personal_notifications = NotificationRepo.list_personal_notifications(gid) if (role in ['admin', 'owner', 'superadmin'] or is_super) else []
    pending = RoleRepo.list_pending_admins(gid) if (role in ['admin', 'owner', 'superadmin'] or is_super) else []
    admins = GroupRepo.list_group_admins(gid) if (role in ['admin', 'owner', 'superadmin'] or is_super) else []
    members = GroupRepo.list_group_members_detailed(gid) if (role in ['admin', 'owner', 'superadmin'] or is_super) else []
    current_display_name = DisplayNameRepo.get_display_name(gid, user_id)
    
    # Filter out superadmin if current user is not superadmin
    # Check if current user is superadmin by telegram_id
    is_superadmin = is_super
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
    effective_role = 'superadmin' if is_super else role
    return render('group_settings.html', group=group, role=effective_role, notifications=notifications, personal_notifications=personal_notifications, pending=pending, admins=admins, members=members, current_display_name=current_display_name, member_display_names=member_display_names, role_map=role_map, event_count=event_count, notifications_count=notifications_count, personal_notifications_count=personal_notifications_count, role_templates=role_templates, request=request)


@app.post('/group/{gid}/settings/notifications/add')
async def add_group_notification(request: Request, gid: int, time_before: int, time_unit: str, message_text: str | None = None):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    NotificationRepo.add_notification(gid, time_before, time_unit, message_text)
    try:
        AuditLogRepo.add('group_notification_created', user_id=user_id, group_id=gid, new_value=f"{time_before} {time_unit} | {message_text or ''}")
    except Exception:
        pass
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
    try:
        AuditLogRepo.add('group_notification_created', user_id=user_id, group_id=gid, new_value=f"{time_before} {time_unit} | {final_message_text or ''}")
    except Exception:
        pass
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
    try:
        AuditLogRepo.add('personal_notification_created', user_id=user_id, group_id=gid, new_value=f"{time_before} {time_unit} | {final_message_text or ''}")
    except Exception:
        pass
    tab_param = f"&tab={tab}" if tab else "&tab=personal"
    return RedirectResponse(f"/group/{gid}/settings?ok=personal_notification_added&tg_id={tg_id}{tab_param}", status_code=303)

@app.post('/group/{gid}/settings/personal-notifications/{nid}/delete')
async def delete_personal_notification(request: Request, gid: int, nid: int, tab: str | None = Form(None)):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    NotificationRepo.delete_notification(nid)
    try:
        AuditLogRepo.add('personal_notification_deleted', user_id=user_id, group_id=gid, old_value=str(nid))
    except Exception:
        pass
    tab_param = f"&tab={tab}" if tab else "&tab=personal"
    return RedirectResponse(f"/group/{gid}/settings?ok=personal_notification_deleted&tg_id={tg_id}{tab_param}", status_code=303)

@app.post('/group/{gid}/settings/role-templates/save')
async def save_role_templates(request: Request, gid: int, role_name: list[str] = Form(None)):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    from services.repositories import GroupRoleTemplateRepo
    items = []
    role_name = role_name or []
    for n in role_name:
        n = (n or '').strip()
        if n:
            items.append((n, 1))
    GroupRoleTemplateRepo.replace_all(gid, items)
    return RedirectResponse(f"/group/{gid}/settings?ok=roles_template_saved&tg_id={tg_id}", status_code=303)

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
        try:
            AuditLogRepo.add('notify_personal', user_id=user_id, group_id=gid, new_value=message)
        except Exception:
            pass
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
        try:
            AuditLogRepo.add('notify_group', user_id=user_id, group_id=gid, new_value=message)
        except Exception:
            pass
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
    try:
        NotificationRepo.delete_notification(nid)
        AuditLogRepo.add('group_notification_deleted', user_id=user_id, group_id=gid, old_value=str(nid))
    except Exception:
        pass
    tab_param = f"&tab={tab}" if tab else ""
    return RedirectResponse(url=f"/group/{gid}/settings?ok=notification_deleted&tg_id={tg_id}{tab_param}", status_code=303)
@app.post('/admin/users/{uid}/block')
async def admin_block_user(request: Request, uid: int, blocked: int = Form(...)):
    urow = _require_user(request)
    # Only superadmin can block users
    if urow[1] != SUPERADMIN_ID:
        raise HTTPException(status_code=403, detail="Only superadmin")
    try:
        UserRepo.set_blocked(uid, bool(int(blocked) == 0))  # toggle based on sent value
        # Preserve tab if provided
        tab = request.query_params.get('tab')
        tg_q = request.query_params.get('tg_id')
        qs = []
        if tg_q:
            qs.append(f"tg_id={tg_q}")
        qs.append("ok=user_blocked")
        if tab:
            qs.append(f"tab={tab}")
        return RedirectResponse(url=f"/?{'&'.join(qs)}", status_code=303)
    except Exception:
        tab = request.query_params.get('tab')
        tg_q = request.query_params.get('tg_id')
        qs = []
        if tg_q:
            qs.append(f"tg_id={tg_q}")
        qs.append("ok=error")
        if tab:
            qs.append(f"tab={tab}")
        return RedirectResponse(url=f"/?{'&'.join(qs)}", status_code=303)

@app.post('/admin/users/{uid}/delete')
async def admin_delete_user(request: Request, uid: int):
    urow = _require_user(request)
    # Only superadmin can delete users
    if urow[1] != SUPERADMIN_ID:
        raise HTTPException(status_code=403, detail="Only superadmin")
    try:
        UserRepo.delete_user(uid)
        tab = request.query_params.get('tab')
        tg_q = request.query_params.get('tg_id')
        qs = []
        if tg_q:
            qs.append(f"tg_id={tg_q}")
        qs.append("ok=user_deleted")
        if tab:
            qs.append(f"tab={tab}")
        return RedirectResponse(url=f"/?{'&'.join(qs)}", status_code=303)
    except Exception:
        tab = request.query_params.get('tab')
        tg_q = request.query_params.get('tg_id')
        qs = []
        if tg_q:
            qs.append(f"tg_id={tg_q}")
        qs.append("ok=error")
        if tab:
            qs.append(f"tab={tab}")
        return RedirectResponse(url=f"/?{'&'.join(qs)}", status_code=303)

@app.post('/admin/send-message')
async def admin_send_message(request: Request, recipient_id: int = Form(...), message: str = Form(...)):
    urow = _require_user(request)
    # Only superadmin can send arbitrary messages here
    if urow[1] != SUPERADMIN_ID:
        raise HTTPException(status_code=403, detail="Only superadmin")
    # Resolve recipient telegram_id
    tid = UserRepo.get_telegram_id_by_user_id(recipient_id)
    if not tid:
        return {"success": False, "error": "Получатель не найден"}
    try:
        from bot import bot
        await bot.send_message(tid, message)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get('/group/{gid}/events/{eid}/settings', response_class=HTMLResponse)
async def event_settings(request: Request, gid: int, eid: int):
    urow = _require_user(request)
    user_id = urow[0]
    
    # Check if user has access (admin or participant with booking)
    role = RoleRepo.get_user_role(user_id, gid)
    # Global superadmin bypass
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    is_super = (urow[1] == CFG_SA) if CFG_SA else False
    # Allow: admins/owner/superadmin OR any group member OR anyone who has a booking on this event
    if (role is None and not BookingRepo.has_booking(user_id, eid)) and not is_super:
        raise HTTPException(status_code=403, detail="Access denied")
    
    group = GroupRepo.get_by_id(gid)
    event = EventRepo.get_by_id(eid)
    member_rows = GroupRepo.list_group_members(gid)
    event_notifications = EventNotificationRepo.list_by_event(eid)
    # Personal notifications: show all for owners/superadmin; otherwise only current user's
    try:
        from config import SUPERADMIN_ID as CFG_SUPER
    except Exception:
        CFG_SUPER = None
    try:
        user_role = RoleRepo.get_user_role(user_id, gid)
        is_superadmin = (urow[1] == CFG_SUPER) if CFG_SUPER else False
        def _label_for(uid: int) -> tuple[str, int | None]:
            dn = DisplayNameRepo.get_display_name(gid, uid)
            tg_id = None
            if dn:
                u = UserRepo.get_by_id(uid)
                tg_id = u[1] if u else None
                return dn, tg_id
            u = UserRepo.get_by_id(uid)
            if u:
                tg_id = u[1]
                if u[2]:
                    return f"@{u[2]}", tg_id
            return str(uid), tg_id
        if user_role in ['owner'] or is_superadmin:
            # Include recipient id and label so template shows correct destination
            personal_rows = PersonalEventNotificationRepo.list_all_for_event(eid)
            personal_notifications = []
            for (nid, uid, tb, tu, msg) in personal_rows:
                label, tgid = _label_for(uid)
                personal_notifications.append((nid, tb, tu, msg, uid, label, tgid))
        else:
            mine = PersonalEventNotificationRepo.list_by_user_and_event(user_id, eid)
            # mine rows: (nid, tb, tu, msg)
            personal_notifications = []
            for (nid, tb, tu, msg) in mine:
                label, tgid = _label_for(user_id)
                personal_notifications.append((nid, tb, tu, msg, user_id, label, tgid))
    except Exception:
        personal_notifications = PersonalEventNotificationRepo.list_by_user_and_event(user_id, eid)
    
    # Get sent status for notifications
    event_notifications_sent = DispatchLogRepo.get_sent_status_for_event_notifications(eid)
    # Build sent map per-notification for display: {(nid): is_sent}
    # For admins/superadmin, compute per user; for regular user, compute for self only
    personal_notifications_sent_map = {}
    try:
        is_superadmin = (urow[1] == CFG_SUPER) if CFG_SUPER else False
        if user_role in ['owner'] or is_superadmin:
            # Load all personal notifications with user_id to compute sent per row
            all_personals = PersonalEventNotificationRepo.list_all_for_event(eid)
            for nid, uid, tb, tu, msg in all_personals:
                sent = DispatchLogRepo.was_sent('personal', user_id=uid, group_id=None, event_id=eid, time_before=tb, time_unit=tu)
                personal_notifications_sent_map[nid] = sent
        else:
            # Only current user's
            mine = PersonalEventNotificationRepo.list_by_user_and_event(user_id, eid)
            for nid, tb, tu, msg in mine:
                sent = DispatchLogRepo.was_sent('personal', user_id=user_id, group_id=None, event_id=eid, time_before=tb, time_unit=tu)
                personal_notifications_sent_map[nid] = sent
    except Exception:
        personal_notifications_sent_map = {}
    
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
    
    # Template info (if this event was generated from a template)
    template_info = None
    template_row = None
    template_roles = []
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT template_id, occurrence_key FROM template_generated_events WHERE event_id = ?", (eid,))
            row = cur.fetchone()
            if row:
                template_info = {'template_id': row[0], 'occurrence_key': row[1]}
                # load template
                template_row = EventTemplateRepo.get(row[0])
                try:
                    template_roles = TemplateRoleRequirementRepo.list(row[0])
                except Exception:
                    template_roles = []
    except Exception:
        template_info = None
        template_row = None
        template_roles = []

    # Load event-specific role list to edit for one-time events as well
    event_roles = EventRoleRequirementRepo.list_for_event(eid)

    # Audit labels for this event
    try:
        c_uid, c_at, u_uid, u_at = EventRepo.get_audit(eid)
        def _label_for(uid: int | None) -> str:
            if not uid:
                return '—'
            dn = DisplayNameRepo.get_display_name(gid, uid)
            if dn:
                return dn
            u = UserRepo.get_by_id(uid)
            if u and u[2]:
                return f"@{u[2]}"
            return str(uid)
        # Format dates into DD.MM.YYYY HH:MM:SS
        def _fmt_dt_ru(dt_str: str | None) -> str:
            if not dt_str:
                return '—'
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    d = datetime.strptime(dt_str, fmt)
                    return d.strftime("%d.%m.%Y %H:%M:%S")
                except Exception:
                    continue
            return dt_str
        audit_info = {
            'created_by': _label_for(c_uid),
            'created_at': _fmt_dt_ru(c_at),
            'updated_by': _label_for(u_uid),
            'updated_at': _fmt_dt_ru(u_at),
        }
    except Exception:
        audit_info = {'created_by': '—', 'updated_by': '—', 'updated_at': '—'}

    # If global superadmin, force role label to superadmin for UI sections
    effective_role = 'superadmin' if is_super else role
    return render('event_settings.html', group=group, event=event, members=member_options, event_notifications=event_notifications, personal_notifications=personal_notifications, event_notifications_sent=event_notifications_sent, personal_notifications_sent_map=personal_notifications_sent_map, role=effective_role, responsible_name=responsible_name, event_time_display=event_time_display, current_user_telegram_id=current_user_telegram_id, current_user_display_name=current_user_display_name, _calculate_notification_time=_calculate_notification_time, request=request, template_info=template_info, template_row=template_row, template_roles=template_roles, event_roles=event_roles, audit_info=audit_info)


@app.post('/group/{gid}/events/{eid}/notifications/add')
async def add_event_notification(request: Request, gid: int, eid: int, time_before: int, time_unit: str, message_text: str | None = None):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    EventNotificationRepo.add_notification(eid, time_before, time_unit, message_text)
    try:
        AuditLogRepo.add('group_notification_created', user_id=user_id, group_id=gid, event_id=eid, new_value=f"{time_before} {time_unit} | {message_text or ''}")
    except Exception:
        pass
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
    try:
        AuditLogRepo.add('group_notification_created', user_id=user_id, group_id=gid, event_id=eid, new_value=f"{time_before} {time_unit} | {message_text_tail or ''}")
    except Exception:
        pass
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=notification_added", status_code=303)


@app.post('/group/{gid}/events/{eid}/convert-to-template')
async def convert_event_to_template(request: Request, gid: int, eid: int, kind: str = Form('recurring'), repeat_every: int = Form(1), repeat_unit: str = Form('week'), planning_horizon_days: int = Form(60), allow_multi_roles_per_user: int = Form(0)):
    urow = _require_user(request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    if role not in ['admin', 'owner', 'superadmin']:
        raise HTTPException(status_code=403, detail="Access denied")

    event = EventRepo.get_by_id(eid)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    # event tuple now includes allow_multi_roles_per_user at index 5
    _, name, time_str, group_id, _resp, _allow_multi = event

    tz = 'Europe/Moscow'
    # Map repeat_unit -> freq
    unit_map = {'day': 'daily', 'week': 'weekly', 'month': 'monthly'}
    freq = unit_map.get(repeat_unit, 'weekly') if kind != 'one_time' else None
    interval = repeat_every if kind != 'one_time' else None
    template_id = EventTemplateRepo.create(group_id, name, None, 'recurring' if kind != 'one_time' else 'one_time', time_str, tz, planning_horizon_days, allow_multi_roles_per_user, freq=freq, interval=interval, byweekday=None)

    # Link current event as generated occurrence for its datetime to avoid duplicate creation
    try:
        TemplateGenerationRepo.mark_generated(template_id, time_str, eid)
    except Exception:
        pass

    # copy current event role reqs into template defaults
    try:
        reqs = EventRoleRequirementRepo.list_for_event(eid)
        for role_name, required in reqs:
            TemplateRoleRequirementRepo.upsert(template_id, role_name, required)
    except Exception:
        pass

    try:
        TemplateGenerator.generate_for_template(template_id, created_by_user_id=user_id)
    except Exception:
        pass

    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=template_created", status_code=303)


from typing import Optional

@app.get('/group/{gid}/audit')
async def group_audit(request: Request, gid: int):
    urow = _require_user(request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    # Allow owners/admins and global superadmin
    is_allowed = role in ['owner', 'admin', 'superadmin'] if role else False
    try:
        is_super = (urow[1] == SUPERADMIN_ID) if SUPERADMIN_ID else False
        if is_super:
            is_allowed = True
    except Exception:
        pass
    if not is_allowed:
        raise HTTPException(status_code=403, detail="Access denied")

    page = int(request.query_params.get('page') or 1)
    per_page = int(request.query_params.get('per_page') or 50)
    event_id = request.query_params.get('event_id')
    eflt = int(event_id) if event_id else None

    # Data for filters
    try:
        evs = EventRepo.list_by_group(gid)
        audit_events = [(eid, name) for (eid, name, _t, _r) in evs]
    except Exception:
        audit_events = []

    # Load audit rows
    try:
        (audit_rows, audit_total) = AuditLogRepo.list(page=page, per_page=per_page, group_id=gid, event_id=eflt)
    except Exception:
        audit_rows, audit_total = [], 0

    # Formatting helpers
    def _fmt_ts(s: str) -> str:
        from datetime import datetime as _dt
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return _dt.strptime(s, fmt).strftime("%d.%m.%Y %H:%M:%S")
            except Exception:
                continue
        return s

    action_map = {
        'event_created': 'Создание',
        'event_name_updated': 'Наименование',
        'event_time_updated': 'Дата и время',
        'event_responsible_updated': 'Ответственный',
        'event_deleted': 'Удаление',
        'event_booked': 'Бронь',
        'event_unbooked': 'Отмена брони',
        'role_booked': 'Бронь',
        'role_unbooked': 'Отмена брони',
        'role_requirement_added': 'Роль добавлена',
        'role_requirement_removed': 'Роль удалена',
        'notify_group': 'Групповое сообщение',
        'notify_personal': 'Личное сообщение',
        'group_notification_created': 'Групповое оповещение',
        'group_notification_deleted': 'Групповое оповещение',
        'personal_notification_created': 'Личное оповещение',
        'personal_notification_deleted': 'Личное оповещение',
        'member_display_name_updated': 'Имя участника',
        'member_role_updated': 'Роль участника',
        'member_removed': 'Удаление участника',
        'member_added': 'Добавление участника',
        'group_deleted': 'Удаление группы',
    }

    audit_items = []
    for (_id, created_at, uid, action, gid_a, eid_a, oldv, newv) in audit_rows:
        # user label
        user_label = str(uid) if uid else '—'
        try:
            u = UserRepo.get_by_id(uid) if uid else None
            if u:
                _iid, _tid, _uname, _phone, _first, _last, *_rest = u
                disp = (_first or '')
                if _last:
                    disp = f"{disp} {_last}".strip()
                if not disp:
                    disp = f"@{_uname}" if _uname else (str(_tid) if _tid else str(uid))
                user_label = f"{disp} ({_tid})" if _tid else disp
        except Exception:
            pass
        # event
        event_name = None
        try:
            ev = EventRepo.get_by_id(eid_a) if eid_a else None
            event_name = ev[1] if ev else None
        except Exception:
            event_name = None
        audit_items.append({
            'id': _id,
            'ts': _fmt_ts(created_at),
            'action': action,
            'action_ru': action_map.get(action, action),
            'user_label': user_label,
            'event_id': eid_a,
            'event_name': event_name,
            'old': oldv,
            'new': newv,
        })

    return render('group_audit.html', request=request, gid=gid, audit_items=audit_items, audit_total=audit_total, audit_page=page, audit_per_page=per_page, audit_events=audit_events, event_filter=(eflt or ''), group=GroupRepo.get_by_id(gid))

@app.post('/group/{gid}/settings/admins/pending/{pid}/delete')
async def delete_pending_invite(request: Request, gid: int, pid: int):
    urow = _require_user(request)
    user_id = urow[0]
    # Allow owner/admin/superadmin or global superadmin
    role = RoleRepo.get_user_role(user_id, gid)
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    is_super = (urow[1] == CFG_SA) if CFG_SA else False
    if role not in ['owner', 'admin', 'superadmin'] and not is_super:
        raise HTTPException(status_code=403, detail="Access denied")
    ok = 'pending_deleted'
    try:
        RoleRepo.delete_pending(pid)
    except Exception:
        ok = 'error'
    tg_q = request.query_params.get('tg_id')
    qs = []
    if tg_q:
        qs.append(f"tg_id={tg_q}")
    qs.append(f"ok={ok}")
    return RedirectResponse(url=f"/group/{gid}/settings?{'&'.join(qs)}", status_code=303)

@app.post('/group/{gid}/events/{eid}/template/update')
async def update_template_from_event(request: Request, gid: int, eid: int,
                                     repeat_every: Optional[int] = Form(None),
                                     repeat_unit: Optional[str] = Form(None),
                                     planning_horizon_days: Optional[int] = Form(None),
                                     allow_multi_roles_per_user: Optional[int] = Form(None),
                                     regenerate: Optional[int] = Form(0),
                                     role_names: Optional[List[str]] = Form(None),
                                     roles_only: Optional[int] = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    if role not in ['admin', 'owner', 'superadmin']:
        raise HTTPException(status_code=403, detail="Access denied")

    # get template by event
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT template_id FROM template_generated_events WHERE event_id = ?", (eid,))
        row = cur.fetchone()
        if not row:
            return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=method_error", status_code=303)
        template_id = row[0]

    # Update only provided basic fields (periodicity) - skip if roles_only
    if not roles_only and (repeat_every is not None or repeat_unit is not None or planning_horizon_days is not None or allow_multi_roles_per_user is not None):
        # Load current template to fill missing fields
        tpl = EventTemplateRepo.get(template_id)
        # tpl indices: 0 id,1 group_id,2 name,3 descr,4 kind,5 base_time,6 timezone,7 planning_horizon_days,8 allow_multi,9 freq,10 interval
        current_horizon = tpl[7] if tpl else 60
        current_allow_multi = tpl[8] if tpl else 0
        current_freq = tpl[9] if tpl else 'weekly'
        current_interval = tpl[10] if tpl else 1
        unit_map = {'day': 'daily', 'week': 'weekly', 'month': 'monthly'}
        new_freq = unit_map.get(repeat_unit, current_freq)
        new_interval = repeat_every if repeat_every is not None else current_interval
        new_horizon = planning_horizon_days if planning_horizon_days is not None else current_horizon
        new_allow = (1 if allow_multi_roles_per_user else 0) if allow_multi_roles_per_user is not None else current_allow_multi
        EventTemplateRepo.update_basic(template_id, planning_horizon_days=new_horizon, allow_multi_roles_per_user=new_allow, freq=new_freq, interval=new_interval)
        # Set base_time to current event time so generation anchors from this event
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                # Read this event time
                cur.execute("SELECT time FROM events WHERE id = ?", (eid,))
                row = cur.fetchone()
                if row and row[0]:
                    cur.execute("UPDATE event_templates SET base_time = ? WHERE id = ?", (row[0], template_id))
                    conn.commit()
        except Exception:
            pass
    elif allow_multi_roles_per_user is not None:
        # Allow updating only the multi-roles flag (when saving roles without touching periodicity)
        EventTemplateRepo.set_allow_multi_roles(template_id, allow_multi_roles_per_user)

    # Replace template roles if provided (one name per line; quantity not used)
    try:
        if role_names is not None:
            # role_names can be multi-value from repeated inputs
            names_flat = []
            if isinstance(role_names, list):
                for n in role_names:
                    if n and n.strip():
                        names_flat.append(n.strip())
            else:
                names_flat = [x.strip() for x in str(role_names).split('\n') if x.strip()]
            names = names_flat
            items = [(n, 1) for n in names]
            TemplateRoleRequirementRepo.replace_all(template_id, items)
            # Also sync roles to this specific event so the group list reflects changes immediately
            try:
                EventRoleRequirementRepo.replace_for_event(eid, names)
            except Exception:
                pass
        elif roles_only:
            # If roles_only flag is set but no role_names provided, clear all roles
            TemplateRoleRequirementRepo.replace_all(template_id, [])
            # Also clear roles for this specific event
            try:
                EventRoleRequirementRepo.replace_for_event(eid, [])
            except Exception:
                pass
    except Exception:
        pass

    if regenerate:
        try:
            # If this event has its own roles, make them the template defaults before generation
            try:
                evt_roles = EventRoleRequirementRepo.list_for_event(eid)
                if evt_roles:
                    TemplateRoleRequirementRepo.replace_all(template_id, [(rname, rreq) for rname, rreq in evt_roles])
            except Exception:
                pass
            TemplateGenerator.generate_for_template(template_id, created_by_user_id=user_id)
        except Exception:
            pass

    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=updated", status_code=303)


@app.get('/group/{gid}/analytics', response_class=HTMLResponse)
async def group_analytics(request: Request, gid: int, start: str | None = None, end: str | None = None, user: int | None = None):
    urow = _require_user(request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    is_superadmin_req = (urow[1] == CFG_SA) if CFG_SA else False
    if role is None and not is_superadmin_req:
        events = EventRepo.list_by_group(gid)
        if not any(BookingRepo.has_booking(user_id, eid) for eid, _, _, _ in events):
            raise HTTPException(status_code=403, detail="Access denied")

    group = GroupRepo.get_by_id(gid)
    member_rows = GroupRepo.list_group_members(gid)
    members = []
    for mid, uname in member_rows:
        dn = DisplayNameRepo.get_display_name(gid, mid)
        label = dn if dn else (f"@{uname}" if uname else str(mid))
        members.append({ 'id': mid, 'label': label })

    from datetime import datetime as _dt
    def parse_dt(s: str | None):
        if not s:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M"):
            try:
                return _dt.strptime(s, fmt)
            except Exception:
                continue
        return None
    dt_start = parse_dt(start)
    dt_end = parse_dt(end)

    events = EventRepo.list_by_group(gid)
    from collections import Counter
    total_by_day = Counter()
    responsible_set = set()
    roles_counter = Counter()
    for eid, name, time_str, resp_uid in events:
        try:
            try:
                t = _dt.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                t = _dt.strptime(time_str, "%Y-%m-%d %H:%M")
        except Exception:
            continue
        if dt_start and t < dt_start:
            continue
        if dt_end and t > dt_end:
            continue
        if user and user != 0:
            if resp_uid != user:
                booked = BookingRepo.list_event_bookings_with_names(gid, eid)
                if not any(uid == user for uid, _name in booked):
                    continue
        day_key = t.strftime('%Y-%m-%d')
        total_by_day[day_key] += 1
        if resp_uid:
            responsible_set.add(resp_uid)
        try:
            role_reqs = EventRoleRequirementRepo.list_for_event(eid)
            for rname, req in role_reqs:
                roles_counter[rname] += int(req or 0)
        except Exception:
            pass

    daily = sorted(total_by_day.items())
    roles = sorted(roles_counter.items(), key=lambda x: (-x[1], x[0]))
    stats = {
        'events_total': sum(total_by_day.values()),
        'unique_responsibles': len(responsible_set),
    }

    return render('group_analytics.html', group=group, members=members, daily=daily, roles=roles, stats=stats, request=request, gid=gid, start=start or '', end=end or '', user=user or 0)

@app.post('/group/{gid}/events/{eid}/roles/update')
async def update_event_roles(request: Request, gid: int, eid: int, allow_multi_roles_per_user: int = Form(0), role_names: List[str] = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    if role not in ['admin', 'owner', 'superadmin']:
        raise HTTPException(status_code=403, detail="Access denied")
    # Update event flag
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE events SET allow_multi_roles_per_user = ? WHERE id = ?", (1 if allow_multi_roles_per_user else 0, eid))
            conn.commit()
    except Exception:
        pass
    # Replace roles if provided
    if role_names is not None:
        names = [n.strip() for n in role_names if n and n.strip()]
        try:
            # Read old roles for audit diff
            try:
                old_roles = [r for r, _ in EventRoleRequirementRepo.list_for_event(eid)]
            except Exception:
                old_roles = []
            EventRoleRequirementRepo.replace_for_event(eid, names)
            # Audit additions/removals
            try:
                old_set = set(old_roles)
                new_set = set(names)
                added = sorted(list(new_set - old_set))
                removed = sorted(list(old_set - new_set))
                for r in added:
                    AuditLogRepo.add('role_requirement_added', user_id=user_id, group_id=gid, event_id=eid, new_value=r)
                for r in removed:
                    AuditLogRepo.add('role_requirement_removed', user_id=user_id, group_id=gid, event_id=eid, old_value=r)
            except Exception:
                pass
        except Exception:
            pass
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=updated", status_code=303)
@app.post('/admin/audit/{aid}/delete')
async def admin_audit_delete(request: Request, aid: int):
    urow = _require_user(request)
    # Only superadmin
    if urow[1] != SUPERADMIN_ID:
        raise HTTPException(status_code=403, detail="Only superadmin")
    try:
        AuditLogRepo.delete(aid)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}
    urow = _require_user(request)
    user_id = urow[0]
    role = RoleRepo.get_user_role(user_id, gid)
    if role not in ['admin', 'owner', 'superadmin']:
        raise HTTPException(status_code=403, detail="Access denied")

    # Update event flag
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE events SET allow_multi_roles_per_user = ? WHERE id = ?", (1 if allow_multi_roles_per_user else 0, eid))
            conn.commit()
    except Exception:
        pass

    # Replace event roles
    try:
        names = []
        if isinstance(role_names, list):
            for n in role_names:
                if n and n.strip():
                    names.append(n.strip())
        else:
            names = [x.strip() for x in str(role_names or '').split('\n') if x.strip()]
        # Allow empty roles list - no forced "Ответственный"
        EventRoleRequirementRepo.replace_for_event(eid, names)
    except Exception:
        pass

    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=updated", status_code=303)


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
    
    try:
        event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M:%S")
    except ValueError:
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


@app.post('/group/{gid}/events/{eid}/notify-now')
async def trigger_group_notification_now(request: Request, gid: int, eid: int):
    """Create a group event notification scheduled for 'now' so bot will send it on the next tick."""
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    # Admins/owners/superadmins only
    _require_admin(user_id, gid)
    event = EventRepo.get_by_id(eid)
    if not event:
        return {"success": False, "error": "Мероприятие не найдено"}
    # event[2] is time string "%Y-%m-%d %H:%M" (seconds optional handled elsewhere)
    try:
        try:
            event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M")
        except ValueError:
            event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return {"success": False, "error": "Некорректная дата мероприятия"}

    now = datetime.now()
    # Compute minutes_before so that notification time ~ now (next tick)
    delta_seconds = int((event_time - now).total_seconds())
    minutes_before = max(1, delta_seconds // 60)
    try:
        EventNotificationRepo.add_notification(eid, minutes_before, 'minutes', None)
        try:
            AuditLogRepo.add('group_notification_created', user_id=user_id, group_id=gid, event_id=eid, new_value=f"{minutes_before} minutes")
        except Exception:
            pass
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post('/group/{gid}/events/{eid}/notifications/{nid}/delete')
async def delete_event_notification(request: Request, gid: int, eid: int, nid: int):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    _require_admin(user_id, gid)
    EventNotificationRepo.delete_notification(nid)
    try:
        AuditLogRepo.add('group_notification_deleted', user_id=user_id, group_id=gid, event_id=eid, old_value=str(nid))
    except Exception:
        pass
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
    try:
        AuditLogRepo.add('personal_notification_created', user_id=user_id, group_id=gid, event_id=eid, new_value=f"{time_before} {time_unit} | {message_text_tail or ''}")
    except Exception:
        pass
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
    
    try:
        event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        event_time = datetime.strptime(event[2], "%Y-%m-%d %H:%M")
    at_norm = _normalize_dt_local(datetime_str)
    try:
        notification_time = datetime.strptime(at_norm, "%Y-%m-%d %H:%M") if at_norm else None
    except Exception as e:
        print(f"Error parsing datetime: {e}, at_norm: {at_norm}")
        notification_time = None
    if not notification_time:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    # Calculate difference in minutes (same logic as group notifications)
    delta_seconds = int((event_time - notification_time).total_seconds())
    if delta_seconds <= 0:
        return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=parse_error", status_code=303)
    
    # Store in minutes by default (same as group notifications)
    minutes = max(delta_seconds // 60, 1)
    
    # Add notification (repository handles duplicate checking internally)
    PersonalEventNotificationRepo.add_notification(user_id, eid, minutes, 'minutes', message_text)
    return RedirectResponse(f"/group/{gid}/events/{eid}/settings?ok=personal_notification_added&tab=personal&tg_id={tg_id}", status_code=303)

@app.post('/group/{gid}/events/{eid}/personal-notifications/{nid}/delete')
async def delete_personal_event_notification(request: Request, gid: int, eid: int, nid: int):
    tg_id = request.query_params.get('tg_id')
    urow = _require_user(request)
    user_id = urow[0]
    # Allow owner/superadmin to delete any personal notification
    try:
        user_role = RoleRepo.get_user_role(user_id, gid)
        if user_role in ['owner'] or urow[1] == SUPERADMIN_ID:
            PersonalEventNotificationRepo.admin_delete_notification(nid)
        else:
            PersonalEventNotificationRepo.delete_notification(nid, user_id)
    except Exception:
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
    try:
        if result:
            AuditLogRepo.add('event_deleted', user_id=user_id, group_id=gid, event_id=eid)
    except Exception:
        pass
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
    try:
        AuditLogRepo.add('event_booked', user_id=user_id, group_id=gid, event_id=eid)
    except Exception:
        pass
    
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
    try:
        AuditLogRepo.add('event_unbooked', user_id=user_id, group_id=gid, event_id=eid)
    except Exception:
        pass
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


@app.post('/group/{gid}/events/{eid}/roles/{role_name}/book')
async def book_role(request: Request, gid: int, eid: int, role_name: str, tab: str | None = Form(None), page: int | None = Form(None), per_page: int | None = Form(None), selected_user_id: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]

    # Determine target user (admins can assign others)
    user_role = RoleRepo.get_user_role(user_id, gid)
    is_admin = user_role in ['admin', 'owner', 'superadmin']
    target_user_id = selected_user_id if (is_admin and selected_user_id) else user_id

    # Validate role exists for this event
    reqs = {r: req for r, req in EventRoleRequirementRepo.list_for_event(eid)}
    if role_name not in reqs:
        return RedirectResponse(f"/group/{gid}?ok=booking_error", status_code=303)

    # Check if role already assigned
    assigned = {r: uid for r, uid in EventRoleAssignmentRepo.list_for_event(eid)}
    if role_name in assigned and assigned[role_name]:
        return RedirectResponse(f"/group/{gid}?ok=booking_error", status_code=303)

    # Check allow_multi_roles_per_user
    allow_multi = 0
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT allow_multi_roles_per_user FROM events WHERE id = ?", (eid,))
        row = cur.fetchone()
        allow_multi = row[0] if row else 0

    if not allow_multi:
        # ensure user has no other role in this event
        for r, uid in assigned.items():
            if uid == target_user_id:
                return RedirectResponse(f"/group/{gid}?ok=booking_error", status_code=303)

    ok = 'event_booked'
    # Check whether user had any role before assignment
    had_any_role_before = any(uid == target_user_id for uid in assigned.values())
    if EventRoleAssignmentRepo.assign(eid, role_name, target_user_id):
        # Ensure personal notifications exist (idempotent) and booking recorded
        try:
            evt = EventRepo.get_by_id(eid)
            group_id = evt[3] if evt else gid
            PersonalEventNotificationRepo.create_from_personal_templates(eid, group_id, target_user_id)
        except Exception:
            pass
        try:
            BookingRepo.add_booking(target_user_id, eid)
        except Exception:
            pass
        try:
            AuditLogRepo.add('role_booked', user_id=user_id, group_id=gid, event_id=eid, new_value=role_name)
        except Exception:
            pass
        ok = 'event_booked'
    else:
        ok = 'booking_error'

    params = []
    if tab:
        params.append(f"tab={tab}")
    if page:
        params.append(f"page={page}")
    if per_page:
        params.append(f"per_page={per_page}")
    param_string = "&".join(params)
    param_string = f"?ok={ok}&{param_string}" if param_string else f"?ok={ok}"
    return RedirectResponse(f"/group/{gid}{param_string}", status_code=303)


@app.post('/group/{gid}/events/{eid}/roles/{role_name}/unbook')
async def unbook_role(request: Request, gid: int, eid: int, role_name: str, tab: str | None = Form(None), page: int | None = Form(None), per_page: int | None = Form(None)):
    urow = _require_user(request)
    user_id = urow[0]

    ok = 'unbooked'
    # Determine permissions (treat global superadmin as admin even if not a group member)
    role = RoleRepo.get_user_role(user_id, gid)
    is_admin = role in ['admin', 'owner', 'superadmin'] if role else False
    try:
        is_super = (urow[1] == SUPERADMIN_ID) if SUPERADMIN_ID else False
        if is_super:
            is_admin = True
    except Exception:
        pass

    # Determine which user to unassign
    target_uid = user_id
    if is_admin:
        try:
            assigned = {r: uid for r, uid in EventRoleAssignmentRepo.list_for_event(eid)}
            if role_name in assigned:
                target_uid = assigned[role_name]
        except Exception:
            pass

    # Attempt unassign
    if not EventRoleAssignmentRepo.unassign(eid, role_name, target_uid):
        ok = 'booking_error'
    else:
        # After unassign, delete personal notifications only if user has no other roles in this event
        try:
            remaining = {r: uid for r, uid in EventRoleAssignmentRepo.list_for_event(eid)}
            if all(uid != target_uid for uid in remaining.values()):
                PersonalEventNotificationRepo.delete_by_user_and_event(target_uid, eid)
                # Defensive: verify removal; if still present, attempt once more
                try:
                    leftovers = PersonalEventNotificationRepo.list_by_user_and_event(target_uid, eid)
                    if leftovers:
                        PersonalEventNotificationRepo.delete_by_user_and_event(target_uid, eid)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            AuditLogRepo.add('role_unbooked', user_id=user_id, group_id=gid, event_id=eid, old_value=role_name)
        except Exception:
            pass

    params = []
    if tab:
        params.append(f"tab={tab}")
    if page:
        params.append(f"page={page}")
    if per_page:
        params.append(f"per_page={per_page}")
    param_string = "&".join(params)
    param_string = f"?ok={ok}&{param_string}" if param_string else f"?ok={ok}"
    return RedirectResponse(f"/group/{gid}{param_string}", status_code=303)

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
    try:
        old = DisplayNameRepo.get_display_name(gid, uid)
    except Exception:
        old = None
    DisplayNameRepo.set_display_name(gid, uid, display_name)
    try:
        AuditLogRepo.add('member_display_name_updated', user_id=user_id, group_id=gid, old_value=old or '', new_value=display_name)
    except Exception:
        pass
    return RedirectResponse(f"/group/{gid}/settings?ok=member_name_saved", status_code=303)

@app.post('/group/{gid}/settings/member/{uid}/role')
async def update_member_role(request: Request, gid: int, uid: int, new_role: str = Form(...)):
    urow = _require_user(request)
    actor_id = urow[0]
    # Only owner or superadmin can change roles
    actor_role = RoleRepo.get_user_role(actor_id, gid)
    from config import SUPERADMIN_ID
    is_super = (urow[1] == SUPERADMIN_ID)
    if actor_role not in ['owner'] and not is_super:
        raise HTTPException(status_code=403, detail="Only owner or superadmin can change roles")

    new_role = (new_role or '').strip().lower()
    if new_role not in ['member', 'admin', 'owner']:
        return RedirectResponse(f"/group/{gid}/settings?ok=member_role_error", status_code=303)

    # Apply role change
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            # Helper: clear all group roles for user
            cur.execute("DELETE FROM user_group_roles WHERE group_id = ? AND user_id = ?", (gid, uid))
            if new_role == 'owner':
                # Set as owner in groups table
                # Find previous owner
                cur.execute("SELECT owner_user_id FROM groups WHERE id = ?", (gid,))
                row = cur.fetchone()
                prev_owner = row[0] if row else None
                cur.execute("UPDATE groups SET owner_user_id = ? WHERE id = ?", (uid, gid))
                # Set new owner's role
                cur.execute("INSERT OR IGNORE INTO user_group_roles (user_id, group_id, role, confirmed) VALUES (?,?,?,1)", (uid, gid, 'owner'))
                # Demote previous owner to admin if exists and different
                if prev_owner and prev_owner != uid:
                    cur.execute("DELETE FROM user_group_roles WHERE group_id = ? AND user_id = ?", (gid, prev_owner))
                    cur.execute("INSERT OR IGNORE INTO user_group_roles (user_id, group_id, role, confirmed) VALUES (?,?,?,1)", (prev_owner, gid, 'admin'))
            else:
                # Set member or admin
                cur.execute("INSERT OR IGNORE INTO user_group_roles (user_id, group_id, role, confirmed) VALUES (?,?,?,1)", (uid, gid, new_role))
            conn.commit()
        ok = 'member_role_saved'
    except Exception:
        ok = 'member_role_error'
    try:
        AuditLogRepo.add('member_role_updated', user_id=actor_id, group_id=gid, event_id=None, old_value='', new_value=new_role)
    except Exception:
        pass
    return RedirectResponse(f"/group/{gid}/settings?ok={ok}", status_code=303)

@app.post('/group/{gid}/settings/member/{uid}/remove')
async def remove_member(request: Request, gid: int, uid: int):
    urow = _require_user(request)
    actor_id = urow[0]
    # Only owner or superadmin can remove
    actor_role = RoleRepo.get_user_role(actor_id, gid)
    from config import SUPERADMIN_ID
    is_super = (urow[1] == SUPERADMIN_ID)
    if actor_role not in ['owner'] and not is_super:
        raise HTTPException(status_code=403, detail="Only owner or superadmin can remove members")

    # Do not allow removing current owner
    grp = GroupRepo.get_by_id(gid)
    if grp and grp[3] == uid:  # owner_user_id
        return RedirectResponse(f"/group/{gid}/settings?ok=member_remove_owner_error", status_code=303)

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM user_group_roles WHERE group_id = ? AND user_id = ?", (gid, uid))
            conn.commit()
        ok = 'member_removed'
        try:
            AuditLogRepo.add('member_removed', user_id=actor_id, group_id=gid, old_value=str(uid))
        except Exception:
            pass
    except Exception:
        ok = 'member_remove_error'
    return RedirectResponse(f"/group/{gid}/settings?ok={ok}", status_code=303)

@app.post('/group/{gid}/settings/members/add')
async def add_group_member(request: Request, gid: int, identifier_type: str = Form(...), identifier: str = Form(...)):
    urow = _require_user(request)
    user_id = urow[0]
    # Owners, admins, or superadmins can add members
    role = RoleRepo.get_user_role(user_id, gid)
    from config import SUPERADMIN_ID
    is_super = (urow[1] == SUPERADMIN_ID)
    if role not in ['owner', 'admin'] and not is_super:
        raise HTTPException(status_code=403, detail="Only owner, admin or superadmin can add members")

    identifier_type = (identifier_type or '').strip().lower()
    value = (identifier or '').strip()
    target_user_id = None
    ok_code = 'member_added'

    try:
        if identifier_type == 'id':
            # Telegram ID
            try:
                tid = int(value)
            except Exception:
                return RedirectResponse(f"/group/{gid}/settings?ok=member_not_found", status_code=303)
            # For Telegram ID, always create pending invite first
            RoleRepo.add_pending_admin(gid, str(tid), 'member_id', user_id)
            ok_code = 'member_pending'
            target_user_id = None
        elif identifier_type == 'username':
            uname = value.lstrip('@')
            # For username, always create pending invite first
            RoleRepo.add_pending_admin(gid, uname, 'member_username', user_id)
            ok_code = 'member_pending'
            target_user_id = None
        elif identifier_type == 'phone':
            # Find by last 10 digits
            normalized = ''.join(filter(str.isdigit, value))
            if normalized.startswith('8'):
                normalized = '7' + normalized[1:]
            from services.repositories import get_conn
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM users WHERE REPLACE(REPLACE(REPLACE(COALESCE(phone,''),'+',''),'-',''),' ','') LIKE ? ORDER BY id DESC LIMIT 1", (f"%{normalized[-10:]}%",))
                row = cur.fetchone()
                if row:
                    target_user_id = row[0]
                else:
                    # create pending member by phone (store last 10 digits only)
                    RoleRepo.add_pending_admin(gid, normalized[-10:], 'member_phone', user_id)
                    ok_code = 'member_pending'
        else:
            # fallback: create a member pending invite with raw identifier
            RoleRepo.add_pending_admin(gid, value, 'member_unknown', user_id)
            ok_code = 'member_pending'

        if target_user_id:
            # Add as confirmed member (only when we resolved a user directly, e.g., phone match)
            RoleRepo.add_role(target_user_id, gid, 'member', confirmed=True)
            ok_code = 'member_added'
    except Exception:
        ok_code = 'member_error'

    try:
        if target_user_id:
            AuditLogRepo.add('member_added', user_id=user_id, group_id=gid, new_value=str(target_user_id))
    except Exception:
        pass

    return RedirectResponse(f"/group/{gid}/settings?ok={ok_code}", status_code=303)

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
    
    # Check if user is owner or global superadmin
    role = RoleRepo.get_user_role(user_id, gid)
    try:
        from config import SUPERADMIN_ID as CFG_SA
    except Exception:
        CFG_SA = None
    is_global_superadmin = (urow[1] == CFG_SA) if CFG_SA else False  # urow[1] is telegram_id
    if role != 'owner' and not is_global_superadmin:
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
        try:
            AuditLogRepo.add('group_deleted', user_id=user_id, group_id=gid)
        except Exception:
            pass
        
        print(f"Group {gid} deleted successfully by user {user_id}")
        
    except Exception as e:
        print(f"Error deleting group {gid}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting group: {str(e)}")
    
    # Redirect to main page
    return RedirectResponse(url="/?ok=group_deleted", status_code=303)


