-- Schema for bot_v2

PRAGMA foreign_keys = ON;

-- Users known to the bot
CREATE TABLE IF NOT EXISTS users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id     INTEGER UNIQUE NOT NULL,
    username        TEXT,
    phone           TEXT,
    first_name      TEXT,
    last_name       TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone);

-- Groups where the bot is present
CREATE TABLE IF NOT EXISTS groups (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_chat_id   TEXT UNIQUE NOT NULL,
    title              TEXT,
    owner_user_id      INTEGER,
    created_at         TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_groups_chat_id ON groups(telegram_chat_id);

-- Roles of users inside groups
-- role: 'superadmin' | 'owner' | 'admin' | 'member'
CREATE TABLE IF NOT EXISTS user_group_roles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    group_id    INTEGER NOT NULL,
    role        TEXT NOT NULL,
    confirmed   INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, group_id, role),
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_roles_group ON user_group_roles(group_id);

-- Pending admins to be confirmed by /start (identifier_type: 'id'|'username'|'phone')
CREATE TABLE IF NOT EXISTS pending_admins (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id         INTEGER NOT NULL,
    identifier       TEXT NOT NULL,
    identifier_type  TEXT NOT NULL,
    created_by_user  INTEGER,
    created_at       TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY(created_by_user) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_group ON pending_admins(group_id);

-- Notification settings per group
-- time_unit: 'months'|'weeks'|'days'|'hours'|'minutes'
-- type: 'group'|'personal' - тип шаблона уведомлений
CREATE TABLE IF NOT EXISTS notification_settings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id      INTEGER NOT NULL,
    time_before   INTEGER NOT NULL,
    time_unit     TEXT NOT NULL,
    message_text  TEXT,
    is_default    INTEGER NOT NULL DEFAULT 0,
    type          TEXT NOT NULL DEFAULT 'group',
    created_at    TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_notifications_group ON notification_settings(group_id);

-- Events
CREATE TABLE IF NOT EXISTS events (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT NOT NULL,
    time                 TEXT NOT NULL,
    group_id             INTEGER NOT NULL,
    responsible_user_id  INTEGER,
    created_at           TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY(responsible_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_events_group_time ON events(group_id, time);

-- Event-specific notification settings
-- time_unit: 'months'|'weeks'|'days'|'hours'|'minutes'
CREATE TABLE IF NOT EXISTS event_notifications (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      INTEGER NOT NULL,
    time_before   INTEGER NOT NULL,
    time_unit     TEXT NOT NULL,
    message_text  TEXT,
    created_at    TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_event_notifications_event ON event_notifications(event_id);

-- Personal event notifications for users
-- time_unit: 'months'|'weeks'|'days'|'hours'|'minutes'
CREATE TABLE IF NOT EXISTS personal_event_notifications (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id       INTEGER NOT NULL,
    event_id      INTEGER NOT NULL,
    time_before   INTEGER NOT NULL,
    time_unit     TEXT NOT NULL,
    message_text  TEXT,
    created_at    TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE,
    UNIQUE(user_id, event_id, time_before, time_unit)
);

CREATE INDEX IF NOT EXISTS idx_personal_event_notifications_user ON personal_event_notifications(user_id);
CREATE INDEX IF NOT EXISTS idx_personal_event_notifications_event ON personal_event_notifications(event_id);


-- Dispatch log to avoid duplicate sends
CREATE TABLE IF NOT EXISTS notification_dispatch_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL, -- 'personal' | 'event'
    user_id      INTEGER,       -- for personal
    group_id     INTEGER,       -- for event (group chat)
    event_id     INTEGER NOT NULL,
    time_before  INTEGER NOT NULL,
    time_unit    TEXT NOT NULL,
    sent_at      TEXT DEFAULT (datetime('now'))
);

-- Enforce uniqueness separately per kind
CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_event
ON notification_dispatch_log(kind, group_id, event_id, time_before, time_unit)
WHERE kind = 'event';

CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_personal
ON notification_dispatch_log(kind, user_id, event_id, time_before, time_unit)
WHERE kind = 'personal';

-- Event bookings by users
CREATE TABLE IF NOT EXISTS bookings (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL,
    event_id   INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, event_id),
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bookings_event ON bookings(event_id);
CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id);

-- Custom display names per group for users
CREATE TABLE IF NOT EXISTS user_display_names (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id    INTEGER NOT NULL,
    user_id     INTEGER NOT NULL,
    display_name TEXT NOT NULL,
    UNIQUE(group_id, user_id),
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);


