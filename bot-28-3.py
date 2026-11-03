#!/usr/bin/env python3
"""
HI2 CHECKER BOT — Premium Edition
Multi-user · Owner Approval · Credit System · TXT Upload · Admin Panel

Setup:
  1. Copy .env.example → .env and fill in your values
  2. pip install -r requirements.txt
  3. python bot.py
"""

import asyncio
import logging
import os
import re
import sqlite3
import string
import tempfile
import time
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path
from threading import Lock

import requests
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN    = os.environ.get("BOT_TOKEN", "8994170789:AAHhfJWxFsMVk_G46MgG3h8embYXQj5li88")
OWNER_ID     = int(os.environ.get("OWNER_ID", "8609127164"))

# DB path resolved at runtime — tries multiple locations
def _get_db_path() -> str:
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    candidates = [
        os.path.expanduser("~/hi2checker.db"),   # home dir
        "/tmp/hi2checker.db",                    # /tmp
        os.path.join(tempfile.gettempdir(), "hi2checker.db"),  # system temp
    ]
    for path in candidates:
        try:
            dirpath = os.path.dirname(path) or "."
            os.makedirs(dirpath, exist_ok=True)
            with open(path, "a"):
                pass
            return path
        except OSError:
            continue
    # absolute last resort — in-memory (lost on restart, but bot runs)
    return ":memory:"
MAX_UPLOAD_MB       = int(os.environ.get("MAX_UPLOAD_SIZE_MB", "2"))
MAX_CONCURRENT_TASKS = int(os.environ.get("MAX_CONCURRENT_TASKS", "1"))
LOG_LEVEL    = os.environ.get("LOG_LEVEL", "INFO")

ALLOWED_DOMAINS = ("hi2.in", "telegmail.com")

_SITEKEY = "6LfEUPkgAAAAAKTgbMoewQkWBEQhO2VPL4QviKct"
_CO      = "aHR0cHM6Ly9oaTIuaW46NDQz"
_UA      = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

STATUS_EMOJI = {
    "available":    "✅",
    "taken":        "❌",
    "error":        "⚠️",
    "rate_limited": "🕐",
    "invalid":      "🚫",
    "duplicate":    "🔁",
    "skipped":      "⏭️",
}

# Conversation states
(
    AWAIT_SINGLE_EMAIL,
    AWAIT_CREDIT_USER,
    AWAIT_CREDIT_AMOUNT,
    AWAIT_REMOVE_USER,
    AWAIT_REMOVE_AMOUNT,
    AWAIT_BAN_USER,
    AWAIT_SEARCH_USER,
    AWAIT_BROADCAST,
) = range(8)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
)
log = logging.getLogger("hi2bot")

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════════════

_db_lock = Lock()
_DB_PATH: str = ""   # set once on first call


@contextmanager
def get_db():
    global _DB_PATH
    if not _DB_PATH:
        _DB_PATH = _get_db_path()
        log.info("Database path: %s", _DB_PATH)
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id   INTEGER PRIMARY KEY,
            username      TEXT,
            full_name     TEXT,
            status        TEXT NOT NULL DEFAULT 'PENDING',
            credits       INTEGER NOT NULL DEFAULT 0,
            total_checks  INTEGER NOT NULL DEFAULT 0,
            registered_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS credit_transactions (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL REFERENCES users(telegram_id),
            amount     INTEGER NOT NULL,
            reason     TEXT,
            actor_id   INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id           TEXT PRIMARY KEY,
            user_id      INTEGER NOT NULL REFERENCES users(telegram_id),
            filename     TEXT,
            total        INTEGER NOT NULL DEFAULT 0,
            processed    INTEGER NOT NULL DEFAULT 0,
            available    INTEGER NOT NULL DEFAULT 0,
            taken        INTEGER NOT NULL DEFAULT 0,
            errors       INTEGER NOT NULL DEFAULT 0,
            skipped      INTEGER NOT NULL DEFAULT 0,
            credits_used INTEGER NOT NULL DEFAULT 0,
            status       TEXT NOT NULL DEFAULT 'RUNNING',
            created_at   TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS task_results (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id   TEXT NOT NULL REFERENCES tasks(id),
            email     TEXT NOT NULL,
            status    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id   INTEGER,
            action     TEXT NOT NULL,
            target_id  INTEGER,
            detail     TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        INSERT OR IGNORE INTO settings (key, value) VALUES ('checking_enabled', '1');
        INSERT OR IGNORE INTO settings (key, value) VALUES ('maintenance_mode', '0');
        """)
    log.info("Database initialised: %s", _DB_PATH)


def get_setting(key: str) -> str:
    with get_db() as db:
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else ""


def set_setting(key: str, value: str):
    with get_db() as db:
        db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))


def register_user(tg_id: int, username: str | None, full_name: str) -> bool:
    """Returns True if newly registered (was not in DB)."""
    with get_db() as db:
        existing = db.execute(
            "SELECT telegram_id FROM users WHERE telegram_id=?", (tg_id,)
        ).fetchone()
        if existing:
            db.execute(
                "UPDATE users SET username=?, full_name=? WHERE telegram_id=?",
                (username, full_name, tg_id),
            )
            return False
        db.execute(
            "INSERT INTO users(telegram_id,username,full_name,status,credits) VALUES(?,?,?,'PENDING',0)",
            (tg_id, username, full_name),
        )
        return True


def get_user(tg_id: int) -> sqlite3.Row | None:
    with get_db() as db:
        return db.execute("SELECT * FROM users WHERE telegram_id=?", (tg_id,)).fetchone()


def set_user_status(tg_id: int, status: str, actor_id: int):
    with get_db() as db:
        db.execute("UPDATE users SET status=? WHERE telegram_id=?", (status, tg_id))
    _audit(actor_id, f"SET_STATUS_{status}", tg_id)


def get_pending_users() -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM users WHERE status='PENDING' ORDER BY registered_at"
        ).fetchall()


def get_all_users(page: int = 0, per_page: int = 10) -> tuple[list, int]:
    with get_db() as db:
        total = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        rows  = db.execute(
            "SELECT * FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?",
            (per_page, page * per_page),
        ).fetchall()
        return rows, total


def add_credits(user_id: int, amount: int, reason: str, actor_id: int) -> int:
    """Returns new balance."""
    with _db_lock:
        with get_db() as db:
            db.execute(
                "UPDATE users SET credits = credits + ? WHERE telegram_id=?",
                (amount, user_id),
            )
            db.execute(
                "INSERT INTO credit_transactions(user_id,amount,reason,actor_id) VALUES(?,?,?,?)",
                (user_id, amount, reason, actor_id),
            )
            new_bal = db.execute(
                "SELECT credits FROM users WHERE telegram_id=?", (user_id,)
            ).fetchone()["credits"]
    _audit(actor_id, "ADD_CREDITS", user_id, f"amount={amount} reason={reason}")
    return new_bal


def remove_credits(user_id: int, amount: int, reason: str, actor_id: int) -> int:
    """Returns new balance. Clamps to 0."""
    with _db_lock:
        with get_db() as db:
            cur_bal = db.execute(
                "SELECT credits FROM users WHERE telegram_id=?", (user_id,)
            ).fetchone()["credits"]
            actual = min(amount, cur_bal)
            db.execute(
                "UPDATE users SET credits = credits - ? WHERE telegram_id=?",
                (actual, user_id),
            )
            db.execute(
                "INSERT INTO credit_transactions(user_id,amount,reason,actor_id) VALUES(?,?,?,?)",
                (user_id, -actual, reason, actor_id),
            )
            new_bal = db.execute(
                "SELECT credits FROM users WHERE telegram_id=?", (user_id,)
            ).fetchone()["credits"]
    _audit(actor_id, "REMOVE_CREDITS", user_id, f"amount={actual} reason={reason}")
    return new_bal


def deduct_one_credit(user_id: int) -> bool:
    """Atomically deduct 1 credit. Returns True if successful."""
    with _db_lock:
        with get_db() as db:
            row = db.execute(
                "SELECT credits FROM users WHERE telegram_id=?", (user_id,)
            ).fetchone()
            if not row or row["credits"] < 1:
                return False
            db.execute(
                "UPDATE users SET credits = credits - 1, total_checks = total_checks + 1 "
                "WHERE telegram_id=?",
                (user_id,),
            )
            db.execute(
                "INSERT INTO credit_transactions(user_id,amount,reason) VALUES(?,?,?)",
                (user_id, -1, "check"),
            )
            return True


def refund_credit(user_id: int, reason: str = "refund"):
    with _db_lock:
        with get_db() as db:
            db.execute(
                "UPDATE users SET credits = credits + 1, total_checks = CASE WHEN total_checks > 0 THEN total_checks - 1 ELSE 0 END WHERE telegram_id=?",
                (user_id,),
            )
            db.execute(
                "INSERT INTO credit_transactions(user_id,amount,reason) VALUES(?,?,?)",
                (user_id, 1, reason),
            )


def get_credit_history(user_id: int, limit: int = 10) -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM credit_transactions WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()


def create_task(user_id: int, filename: str, total: int) -> str:
    task_id = str(uuid.uuid4())[:12]
    with get_db() as db:
        db.execute(
            "INSERT INTO tasks(id,user_id,filename,total) VALUES(?,?,?,?)",
            (task_id, user_id, filename, total),
        )
    return task_id


def update_task(task_id: str, **kwargs):
    if not kwargs:
        return
    cols = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [task_id]
    with get_db() as db:
        db.execute(f"UPDATE tasks SET {cols} WHERE id=?", vals)


def finish_task(task_id: str, status: str):
    with get_db() as db:
        db.execute(
            "UPDATE tasks SET status=?, finished_at=datetime('now') WHERE id=?",
            (status, task_id),
        )


def save_result(task_id: str, email: str, status: str):
    with get_db() as db:
        db.execute(
            "INSERT INTO task_results(task_id,email,status) VALUES(?,?,?)",
            (task_id, email, status),
        )


def get_task(task_id: str) -> sqlite3.Row | None:
    with get_db() as db:
        return db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()


def get_task_results(task_id: str) -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM task_results WHERE task_id=?", (task_id,)
        ).fetchall()


def get_user_tasks(user_id: int, limit: int = 5) -> list:
    with get_db() as db:
        return db.execute(
            "SELECT * FROM tasks WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()


def get_bot_stats() -> dict:
    with get_db() as db:
        users    = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        approved = db.execute("SELECT COUNT(*) FROM users WHERE status='APPROVED'").fetchone()[0]
        pending  = db.execute("SELECT COUNT(*) FROM users WHERE status='PENDING'").fetchone()[0]
        tasks    = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        checks   = db.execute("SELECT SUM(processed) FROM tasks").fetchone()[0] or 0
        avail    = db.execute("SELECT SUM(available) FROM tasks").fetchone()[0] or 0
    return {
        "users": users, "approved": approved, "pending": pending,
        "tasks": tasks, "checks": checks, "available": avail,
    }


def _audit(actor_id: int, action: str, target_id: int = None, detail: str = None):
    try:
        with get_db() as db:
            db.execute(
                "INSERT INTO audit_logs(actor_id,action,target_id,detail) VALUES(?,?,?,?)",
                (actor_id, action, target_id, detail),
            )
    except Exception as e:
        log.warning("Audit log failed: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  RECAPTCHA SOLVER  (preserved from original bot.py — unchanged)
# ══════════════════════════════════════════════════════════════════════════════

_ver_lock  = Lock()
_ver_cache = {"version": None, "ts": 0.0}

_tok_lock  = Lock()
_tok_cache = {"token": None, "ts": 0.0}


def _get_version() -> str:
    with _ver_lock:
        if _ver_cache["version"] and time.time() - _ver_cache["ts"] < 3600:
            return _ver_cache["version"]
    try:
        r = requests.get(
            "https://www.google.com/recaptcha/api.js",
            headers={"User-Agent": _UA},
            timeout=8,
        )
        m = re.search(r"/releases/([\w-]+)/", r.text)
        if m:
            with _ver_lock:
                _ver_cache["version"] = m.group(1)
                _ver_cache["ts"] = time.time()
            return m.group(1)
    except Exception:
        pass
    return "rAqPVhe2JMK6mSJKi8r_vw"


def _get_token() -> str | None:
    with _tok_lock:
        if _tok_cache["token"] and time.time() - _tok_cache["ts"] < 90:
            return _tok_cache["token"]

    version = _get_version()
    cb = "".join(__import__("random").choices(string.ascii_lowercase + string.digits, k=10))

    sess = requests.Session()
    sess.headers.update({"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"})

    anchor_url = (
        f"https://www.google.com/recaptcha/api2/anchor"
        f"?ar=1&k={_SITEKEY}&co={_CO}&hl=en&v={version}&size=invisible&cb={cb}"
    )
    try:
        r1 = sess.get(anchor_url, timeout=8)
    except Exception:
        return None

    challenge = None
    for pat in (r'"rresp","([^"]+)"', r'id="recaptcha-token" value="([^"]+)"'):
        m = re.search(pat, r1.text)
        if m:
            challenge = m.group(1)
            break
    if not challenge:
        return None

    reload_url = f"https://www.google.com/recaptcha/api2/reload?k={_SITEKEY}"
    payload = (
        f"v={version}&reason=q&c={urllib.parse.quote(challenge)}"
        f"&k={_SITEKEY}&co={_CO}&hl=en&size=invisible"
        f"&chr=%5B89%2C64%2C27%5D&vh=13599012192&bg=!GgA4oQEAABkEBQAAAA"
    )
    try:
        r2 = sess.post(
            reload_url,
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": anchor_url,
            },
            timeout=8,
        )
    except Exception:
        return None

    for pat in (r'"rresp","([^"]+)"', r'\["rresp","([^"]+)"'):
        m = re.search(pat, r2.text)
        if m:
            tok = m.group(1)
            with _tok_lock:
                _tok_cache["token"] = tok
                _tok_cache["ts"] = time.time()
            return tok
    return None


def _invalidate_token():
    with _tok_lock:
        _tok_cache["token"] = None
        _tok_cache["ts"] = 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  HI2 CHECKER  (preserved from original bot.py — unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def hi2_check(username: str, domain: str) -> str:
    """Returns: available | taken | rate_limited | error"""
    token = _get_token()
    if not token:
        return "error"

    try:
        r = requests.post(
            "https://hi2.in/api/custom",
            data={"domain": domain, "prefix": username, "recaptcha": token},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin":       "https://hi2.in",
                "Referer":      "https://hi2.in/",
                "User-Agent":   _UA,
            },
            timeout=10,
        )

        if r.status_code == 429:
            _invalidate_token()
            return "rate_limited"

        if r.status_code == 200:
            data = r.json()
            email_returned = data.get("email", "").lower()
            _invalidate_token()
            if email_returned == f"{username}@{domain}".lower():
                return "available"
            return "taken"

        if r.status_code == 400:
            try:
                err = r.json().get("error", "").lower()
                if "already taken" in err:
                    _invalidate_token()
                    return "taken"
                if "recaptcha" in err:
                    _invalidate_token()
                    return "error"
            except Exception:
                pass
            _invalidate_token()
            return "taken"

        return "error"

    except Exception:
        return "error"


# ══════════════════════════════════════════════════════════════════════════════
#  PARSING  (preserved from original bot.py)
# ══════════════════════════════════════════════════════════════════════════════

_EMAIL_RE  = re.compile(r"^[^@\s]+@([^@\s]+)$")
_PREFIX_RE = re.compile(r"^[a-zA-Z0-9._+\-]+$")


def parse_line(line: str) -> tuple[str, str] | None:
    line = line.strip()
    if not line:
        return None
    if "@" not in line:
        if _PREFIX_RE.match(line):
            return line.lower(), "hi2.in"
        return None
    m = _EMAIL_RE.match(line)
    if not m:
        return None
    domain = m.group(1).lower()
    if domain not in ALLOWED_DOMAINS:
        return None
    return line.split("@", 1)[0].lower(), domain


# ══════════════════════════════════════════════════════════════════════════════
#  ACTIVE TASKS REGISTRY  (for cancellation)
# ══════════════════════════════════════════════════════════════════════════════

_active_tasks: dict[int, dict] = {}   # user_id → {task_id, cancelled}
_active_lock = Lock()


def register_active_task(user_id: int, task_id: str):
    with _active_lock:
        _active_tasks[user_id] = {"task_id": task_id, "cancelled": False}


def cancel_user_task(user_id: int):
    with _active_lock:
        if user_id in _active_tasks:
            _active_tasks[user_id]["cancelled"] = True


def is_cancelled(user_id: int) -> bool:
    with _active_lock:
        t = _active_tasks.get(user_id)
        return t["cancelled"] if t else False


def clear_active_task(user_id: int):
    with _active_lock:
        _active_tasks.pop(user_id, None)


def has_active_task(user_id: int) -> bool:
    with _active_lock:
        return user_id in _active_tasks


# ══════════════════════════════════════════════════════════════════════════════
#  GUARDS & DECORATORS
# ══════════════════════════════════════════════════════════════════════════════

def owner_only(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id != OWNER_ID:
            if update.message:
                await update.message.reply_text("⛔ Access denied.")
            elif update.callback_query:
                await update.callback_query.answer("⛔ Access denied.", show_alert=True)
            return
        return await func(update, ctx)
    return wrapper


async def _check_access(update: Update) -> sqlite3.Row | None:
    """Returns user row if approved, else sends error and returns None."""
    u = update.effective_user
    if not u:
        return None

    row = get_user(u.id)
    msg = update.message or (update.callback_query.message if update.callback_query else None)

    if not row:
        if msg:
            await msg.reply_text("⚠️ Please send /start to register.")
        return None

    if row["status"] == "PENDING":
        txt = (
            "⏳ *Your account is pending approval.*\n"
            "The owner will review your request shortly."
        )
        if msg:
            await msg.reply_text(txt, parse_mode="Markdown")
        return None

    if row["status"] in ("REJECTED", "BANNED"):
        txt = (
            "⛔ *Access denied.*\n"
            f"Status: `{row['status']}`"
        )
        if msg:
            await msg.reply_text(txt, parse_mode="Markdown")
        return None

    return row


# ══════════════════════════════════════════════════════════════════════════════
#  UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

HEADER = "🔮 *HI2 CHECKER*"
DIV    = "━━━━━━━━━━━━━━━━━━━━━━"


def _fmt_user(u) -> str:
    name = u["full_name"] or u["username"] or str(u["telegram_id"])
    return f"`{u['telegram_id']}` — {name} [{u['status']}]"


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📧 Single Check", callback_data="single_check"),
            InlineKeyboardButton("📂 Upload TXT",   callback_data="upload_txt"),
        ],
        [
            InlineKeyboardButton("📊 My Stats",    callback_data="my_stats"),
            InlineKeyboardButton("🗂 History",     callback_data="my_history"),
        ],
        [
            InlineKeyboardButton("💜 My Credits",  callback_data="my_credits"),
            InlineKeyboardButton("📨 Contact Owner", callback_data="contact_owner"),
        ],
        [
            InlineKeyboardButton("❓ Help",        callback_data="help"),
            InlineKeyboardButton("🔄 Refresh",     callback_data="refresh_home"),
        ],
    ])


def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 All Users",       callback_data="adm_users:0"),
            InlineKeyboardButton("⏳ Pending",          callback_data="adm_pending"),
        ],
        [
            InlineKeyboardButton("💜 Give Credits",    callback_data="adm_give"),
            InlineKeyboardButton("➖ Remove Credits",  callback_data="adm_remove"),
        ],
        [
            InlineKeyboardButton("🔍 Search User",     callback_data="adm_search"),
            InlineKeyboardButton("🚫 Ban / Unban",     callback_data="adm_ban"),
        ],
        [
            InlineKeyboardButton("📊 Bot Stats",       callback_data="adm_stats"),
            InlineKeyboardButton("🗒 Audit Logs",      callback_data="adm_audit"),
        ],
        [
            InlineKeyboardButton("⚙️ Toggle Checking", callback_data="adm_toggle_check"),
            InlineKeyboardButton("🔧 Maintenance",     callback_data="adm_toggle_maint"),
        ],
        [
            InlineKeyboardButton("📢 Broadcast",       callback_data="adm_broadcast"),
            InlineKeyboardButton("🔙 Home",             callback_data="refresh_home"),
        ],
    ])


def back_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Back to Home", callback_data="refresh_home")
    ]])


def _home_text(row) -> str:
    name = row["full_name"] or row["username"] or str(row["telegram_id"])
    return (
        f"{HEADER}\n"
        f"{DIV}\n"
        f"👤 *{name}*\n"
        f"🆔 `{row['telegram_id']}`\n"
        f"✅ Status: `{row['status']}`\n"
        f"💜 Credits: `{row['credits']}`\n"
        f"🔢 Total Checks: `{row['total_checks']}`\n"
        f"{DIV}\n"
        f"_Select an option below_"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    full_name = (u.full_name or "").strip()
    is_new = register_user(u.id, u.username, full_name)
    row    = get_user(u.id)

    # Owner always goes straight to home
    if u.id == OWNER_ID:
        await update.message.reply_text(
            f"{HEADER}\n{DIV}\n"
            f"👑 Welcome, *Owner*!\n"
            f"💜 Credits: `{row['credits']}`  🔢 Checks: `{row['total_checks']}`\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(),
        )
        return

    if is_new:
        # Notify owner
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Approve", callback_data=f"approve:{u.id}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"reject:{u.id}"),
        ]])
        uname = f"@{u.username}" if u.username else "N/A"
        try:
            await ctx.bot.send_message(
                OWNER_ID,
                f"🔔 *New Access Request*\n{DIV}\n"
                f"👤 Name: {full_name}\n"
                f"🆔 ID: `{u.id}`\n"
                f"👥 Username: {uname}\n"
                f"{DIV}",
                parse_mode="Markdown",
                reply_markup=kb,
            )
        except Exception:
            pass
        _audit(u.id, "REGISTER", u.id)
        await update.message.reply_text(
            f"{HEADER}\n{DIV}\n"
            f"👋 Hi *{full_name}*!\n\n"
            f"⏳ Your access request has been sent to the owner.\n"
            f"You will be notified once approved.\n"
            f"{DIV}",
            parse_mode="Markdown",
        )
        return

    # Existing user
    status = row["status"]
    if status == "APPROVED":
        if get_setting("maintenance_mode") == "1":
            await update.message.reply_text(
                f"{HEADER}\n{DIV}\n"
                "🔧 *Maintenance Mode*\n"
                "The bot is temporarily unavailable. Please try again later.\n"
                f"{DIV}",
                parse_mode="Markdown",
            )
            return
        await update.message.reply_text(
            _home_text(row),
            parse_mode="Markdown",
            reply_markup=main_menu_kb(),
        )
    elif status == "PENDING":
        await update.message.reply_text(
            f"{HEADER}\n{DIV}\n"
            "⏳ *Still pending approval.*\n"
            "The owner hasn't reviewed your request yet.\n"
            f"{DIV}",
            parse_mode="Markdown",
        )
    elif status in ("REJECTED", "BANNED"):
        await update.message.reply_text(
            f"{HEADER}\n{DIV}\n"
            f"⛔ Access denied — `{status}`\n"
            f"{DIV}",
            parse_mode="Markdown",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  /admin
# ══════════════════════════════════════════════════════════════════════════════

@owner_only
async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    checking = "✅ ON" if get_setting("checking_enabled") == "1" else "❌ OFF"
    maint    = "🔧 ON" if get_setting("maintenance_mode")  == "1" else "✅ OFF"
    stats    = get_bot_stats()
    await update.message.reply_text(
        f"{HEADER} — 👑 *Admin Panel*\n{DIV}\n"
        f"👥 Total Users: `{stats['users']}`  ✅ Approved: `{stats['approved']}`  ⏳ Pending: `{stats['pending']}`\n"
        f"🔢 Total Checks: `{stats['checks']}`  ✅ Available found: `{stats['available']}`\n"
        f"⚙️ Checking: {checking}  |  🔧 Maintenance: {maint}\n"
        f"{DIV}",
        parse_mode="Markdown",
        reply_markup=admin_menu_kb(),
    )


# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACK ROUTER
# ══════════════════════════════════════════════════════════════════════════════

async def callback_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data
    uid   = query.from_user.id

    # ── home refresh ──
    if data == "refresh_home":
        row = get_user(uid)
        if not row:
            await query.edit_message_text("Please send /start to register.")
            return
        if uid == OWNER_ID:
            checking = "✅ ON" if get_setting("checking_enabled") == "1" else "❌ OFF"
            maint    = "🔧 ON" if get_setting("maintenance_mode")  == "1" else "✅ OFF"
            s = get_bot_stats()
            await query.edit_message_text(
                f"{HEADER}\n{DIV}\n"
                f"👑 *Owner Panel*\n"
                f"💜 Credits: `{row['credits']}`  🔢 Checks: `{row['total_checks']}`\n"
                f"⚙️ Checking: {checking}  |  🔧 Maint: {maint}\n"
                f"👥 Users: `{s['users']}`  ⏳ Pending: `{s['pending']}`\n"
                f"{DIV}",
                parse_mode="Markdown",
                reply_markup=main_menu_kb(),
            )
        else:
            await query.edit_message_text(
                _home_text(row),
                parse_mode="Markdown",
                reply_markup=main_menu_kb(),
            )
        return

    # ── owner approval ──
    if data.startswith("approve:") or data.startswith("reject:"):
        if uid != OWNER_ID:
            await query.answer("⛔ Owner only.", show_alert=True)
            return
        action, target_str = data.split(":")
        target = int(target_str)
        new_status = "APPROVED" if action == "approve" else "REJECTED"
        set_user_status(target, new_status, uid)
        icon = "✅" if new_status == "APPROVED" else "❌"
        await query.edit_message_text(
            f"{icon} User `{target}` has been *{new_status}*.",
            parse_mode="Markdown",
        )
        # Notify user
        try:
            if new_status == "APPROVED":
                await ctx.bot.send_message(
                    target,
                    f"{HEADER}\n{DIV}\n"
                    "✅ *Your account has been approved!*\n"
                    "Send /start to access the bot.\n"
                    f"{DIV}",
                    parse_mode="Markdown",
                )
            else:
                await ctx.bot.send_message(
                    target,
                    f"{HEADER}\n{DIV}\n"
                    "❌ *Your access request was rejected.*\n"
                    f"{DIV}",
                    parse_mode="Markdown",
                )
        except Exception:
            pass
        return

    # ══ USER CALLBACKS ══
    row = get_user(uid)
    is_owner = (uid == OWNER_ID)

    if not is_owner:
        if not row or row["status"] != "APPROVED":
            await query.answer("⛔ No access.", show_alert=True)
            return
        if get_setting("maintenance_mode") == "1":
            await query.answer("🔧 Bot under maintenance.", show_alert=True)
            return

    # ── single check ──
    if data == "single_check":
        if not is_owner and row["credits"] < 1:
            await query.edit_message_text(
                f"{HEADER}\n{DIV}\n"
                "💜 *Insufficient Credits*\n"
                "You need at least 1 credit to check.\n"
                "Contact the owner to get credits.\n"
                f"{DIV}",
                parse_mode="Markdown",
                reply_markup=back_home_kb(),
            )
            return
        ctx.user_data["single_check_msg"] = query.message.message_id
        await query.edit_message_text(
            f"{HEADER}\n{DIV}\n"
            "📧 *Single Email Check*\n\n"
            "Send the email address to check:\n"
            "_Example: `john@hi2.in` or just `john`_\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_single")
            ]]),
        )
        ctx.user_data["awaiting"] = "single_email"
        return

    if data == "cancel_single":
        ctx.user_data.pop("awaiting", None)
        row = get_user(uid)
        await query.edit_message_text(
            _home_text(row),
            parse_mode="Markdown",
            reply_markup=main_menu_kb(),
        )
        return

    # ── upload txt ──
    if data == "upload_txt":
        if not is_owner and row["credits"] < 1:
            await query.edit_message_text(
                f"{HEADER}\n{DIV}\n"
                "💜 *Insufficient Credits*\n"
                "You need credits to check emails.\n"
                f"{DIV}",
                parse_mode="Markdown",
                reply_markup=back_home_kb(),
            )
            return
        await query.edit_message_text(
            f"{HEADER}\n{DIV}\n"
            "📂 *TXT File Upload*\n\n"
            f"Upload a `.txt` file (max {MAX_UPLOAD_MB}MB).\n"
            "One email or username per line.\n"
            f"Supported: `hi2.in` · `telegmail.com`\n"
            f"{DIV}\n"
            f"💜 Your credits: `{row['credits'] if row else '∞'}`",
            parse_mode="Markdown",
            reply_markup=back_home_kb(),
        )
        ctx.user_data["awaiting"] = "txt_file"
        return

    # ── cancel running task ──
    if data == "cancel_task":
        if has_active_task(uid):
            cancel_user_task(uid)
            await query.answer("⏹ Cancellation requested…", show_alert=False)
        else:
            await query.answer("No active task.", show_alert=True)
        return

    # ── my stats ──
    if data == "my_stats":
        r = get_user(uid)
        tasks = get_user_tasks(uid, 50)
        avail = sum(t["available"] for t in tasks)
        taken = sum(t["taken"]     for t in tasks)
        errs  = sum(t["errors"]    for t in tasks)
        await query.edit_message_text(
            f"{HEADER} — 📊 *My Statistics*\n{DIV}\n"
            f"💜 Credits: `{r['credits']}`\n"
            f"🔢 Total Checks: `{r['total_checks']}`\n"
            f"✅ Available: `{avail}`\n"
            f"❌ Taken: `{taken}`\n"
            f"⚠️ Errors: `{errs}`\n"
            f"📁 Tasks: `{len(tasks)}`\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=back_home_kb(),
        )
        return

    # ── my history ──
    if data == "my_history":
        tasks = get_user_tasks(uid, 5)
        if not tasks:
            text = f"{HEADER} — 🗂 *History*\n{DIV}\nNo tasks yet.\n{DIV}"
        else:
            lines = []
            for t in tasks:
                lines.append(
                    f"📁 `{t['id']}` · {t['status']}\n"
                    f"   {t['processed']}/{t['total']} · ✅{t['available']} ❌{t['taken']} ⚠️{t['errors']}\n"
                    f"   🕐 {t['created_at'][:16]}"
                )
            text = (
                f"{HEADER} — 🗂 *Recent History*\n{DIV}\n"
                + "\n\n".join(lines)
                + f"\n{DIV}"
            )
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_home_kb())
        return

    # ── my credits ──
    if data == "my_credits":
        r   = get_user(uid)
        txs = get_credit_history(uid, 8)
        lines = []
        for tx in txs:
            sign = "+" if tx["amount"] > 0 else ""
            lines.append(f"{sign}{tx['amount']}  _{tx['reason'] or '-'}_  `{tx['created_at'][:16]}`")
        hist = "\n".join(lines) if lines else "_No transactions yet_"
        await query.edit_message_text(
            f"{HEADER} — 💜 *My Credits*\n{DIV}\n"
            f"Balance: `{r['credits']}`\n\n"
            f"*Recent transactions:*\n{hist}\n{DIV}",
            parse_mode="Markdown",
            reply_markup=back_home_kb(),
        )
        return

    # ── help ──
    if data == "help":
        await query.edit_message_text(
            f"{HEADER} — ❓ *Help*\n{DIV}\n"
            "📧 *Single Check* — Check one email address\n"
            "📂 *Upload TXT* — Bulk check via file upload\n"
            "📊 *Stats* — View your checking statistics\n"
            "🗂 *History* — Recent task history\n"
            "💜 *Credits* — Balance & transaction log\n\n"
            f"*Supported domains:* `hi2.in` · `telegmail.com`\n"
            f"*Cost:* 1 credit per email checked\n"
            f"*Refunded:* errors & rate limits\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=back_home_kb(),
        )
        return

    # ── contact owner ──
    if data == "contact_owner":
        await query.edit_message_text(
            f"{HEADER}\n{DIV}\n"
            "📨 To request credits or support, contact the bot owner directly on Telegram.\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=back_home_kb(),
        )
        return

    # ══ ADMIN CALLBACKS ══
    if not is_owner:
        return

    if data == "adm_pending":
        rows = get_pending_users()
        if not rows:
            await query.edit_message_text(
                f"{HEADER}\n{DIV}\n⏳ No pending users.\n{DIV}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Admin", callback_data="adm_back")
                ]]),
            )
            return
        btns = []
        for r in rows[:10]:
            name = r["full_name"] or r["username"] or str(r["telegram_id"])
            btns.append([
                InlineKeyboardButton(f"✅ {name[:20]}", callback_data=f"approve:{r['telegram_id']}"),
                InlineKeyboardButton("❌ Reject",        callback_data=f"reject:{r['telegram_id']}"),
            ])
        btns.append([InlineKeyboardButton("🔙 Admin", callback_data="adm_back")])
        await query.edit_message_text(
            f"{HEADER}\n{DIV}\n⏳ *Pending Users ({len(rows)})*\n{DIV}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(btns),
        )
        return

    if data.startswith("adm_users:"):
        page = int(data.split(":")[1])
        rows, total = get_all_users(page, 8)
        lines = [_fmt_user(r) for r in rows]
        pages = (total + 7) // 8
        nav   = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"adm_users:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if (page + 1) < pages:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"adm_users:{page+1}"))
        kb = InlineKeyboardMarkup([nav, [InlineKeyboardButton("🔙 Admin", callback_data="adm_back")]])
        await query.edit_message_text(
            f"{HEADER} — 👥 *Users ({total})*\n{DIV}\n"
            + "\n".join(lines)
            + f"\n{DIV}",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    if data == "adm_stats":
        s = get_bot_stats()
        checking = "✅ ON" if get_setting("checking_enabled") == "1" else "❌ OFF"
        maint    = "🔧 ON" if get_setting("maintenance_mode")  == "1" else "✅ OFF"
        await query.edit_message_text(
            f"{HEADER} — 📊 *Bot Statistics*\n{DIV}\n"
            f"👥 Total Users: `{s['users']}`\n"
            f"✅ Approved: `{s['approved']}`\n"
            f"⏳ Pending: `{s['pending']}`\n"
            f"📁 Total Tasks: `{s['tasks']}`\n"
            f"🔢 Total Checks: `{s['checks']}`\n"
            f"✅ Available Found: `{s['available']}`\n"
            f"⚙️ Checking: {checking}\n"
            f"🔧 Maintenance: {maint}\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Admin", callback_data="adm_back")
            ]]),
        )
        return

    if data == "adm_audit":
        with get_db() as db:
            logs = db.execute(
                "SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT 10"
            ).fetchall()
        if not logs:
            text = f"{HEADER}\n{DIV}\n🗒 No audit logs yet.\n{DIV}"
        else:
            lines = [
                f"`{l['created_at'][:16]}` *{l['action']}*  target:`{l['target_id']}`  _{l['detail'] or ''}_"
                for l in logs
            ]
            text = f"{HEADER} — 🗒 *Audit Logs*\n{DIV}\n" + "\n".join(lines) + f"\n{DIV}"
        await query.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Admin", callback_data="adm_back")
            ]]),
        )
        return

    if data == "adm_toggle_check":
        cur = get_setting("checking_enabled")
        new = "0" if cur == "1" else "1"
        set_setting("checking_enabled", new)
        _audit(uid, f"TOGGLE_CHECKING_{new}")
        status = "✅ ENABLED" if new == "1" else "❌ DISABLED"
        await query.edit_message_text(
            f"{HEADER}\n{DIV}\n⚙️ *Checking is now {status}*\n{DIV}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Admin", callback_data="adm_back")
            ]]),
        )
        return

    if data == "adm_toggle_maint":
        cur = get_setting("maintenance_mode")
        new = "0" if cur == "1" else "1"
        set_setting("maintenance_mode", new)
        _audit(uid, f"TOGGLE_MAINTENANCE_{new}")
        status = "🔧 ON" if new == "1" else "✅ OFF"
        await query.edit_message_text(
            f"{HEADER}\n{DIV}\n🔧 *Maintenance Mode: {status}*\n{DIV}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Admin", callback_data="adm_back")
            ]]),
        )
        return

    if data in ("adm_give", "adm_remove", "adm_search", "adm_ban", "adm_broadcast"):
        prompts = {
            "adm_give":      ("💜 *Give Credits*\n\nSend the Telegram ID of the user:", "give_uid"),
            "adm_remove":    ("➖ *Remove Credits*\n\nSend the Telegram ID:", "remove_uid"),
            "adm_search":    ("🔍 *Search User*\n\nSend the Telegram ID:", "search_uid"),
            "adm_ban":       ("🚫 *Ban/Unban User*\n\nSend the Telegram ID:", "ban_uid"),
            "adm_broadcast": ("📢 *Broadcast*\n\nSend the message to broadcast:", "broadcast_msg"),
        }
        txt, key = prompts[data]
        ctx.user_data["adm_action"] = key
        await query.edit_message_text(
            f"{HEADER} — 👑 Admin\n{DIV}\n{txt}\n{DIV}\n_Send /cancel to abort_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="adm_back")
            ]]),
        )
        return

    if data == "adm_back":
        checking = "✅ ON" if get_setting("checking_enabled") == "1" else "❌ OFF"
        maint    = "🔧 ON" if get_setting("maintenance_mode")  == "1" else "✅ OFF"
        s = get_bot_stats()
        await query.edit_message_text(
            f"{HEADER} — 👑 *Admin Panel*\n{DIV}\n"
            f"👥 Users: `{s['users']}`  ✅ Approved: `{s['approved']}`  ⏳ Pending: `{s['pending']}`\n"
            f"🔢 Checks: `{s['checks']}`  ✅ Available: `{s['available']}`\n"
            f"⚙️ Checking: {checking}  |  🔧 Maintenance: {maint}\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=admin_menu_kb(),
        )
        return

    if data == "noop":
        return


# ══════════════════════════════════════════════════════════════════════════════
#  TEXT MESSAGE HANDLER  (single check + admin flows + bulk paste)
# ══════════════════════════════════════════════════════════════════════════════

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text.strip()

    if text.startswith("/"):
        return

    # ── admin action flows ──
    if uid == OWNER_ID:
        action = ctx.user_data.get("adm_action")
        if action:
            await _handle_admin_text(update, ctx, action, text)
            return

    # ── single check ──
    if ctx.user_data.get("awaiting") == "single_email":
        ctx.user_data.pop("awaiting")
        row = await _check_access(update)
        if not row and uid != OWNER_ID:
            return

        parsed = parse_line(text)
        if not parsed:
            await update.message.reply_text(
                f"🚫 Invalid: `{text}`\nUse `email@hi2.in` or bare `username`",
                parse_mode="Markdown",
                reply_markup=back_home_kb(),
            )
            return

        username, domain = parsed
        email = f"{username}@{domain}"

        # check global setting
        if get_setting("checking_enabled") != "1" and uid != OWNER_ID:
            await update.message.reply_text(
                "⚙️ Checking is currently disabled. Try again later.",
                reply_markup=back_home_kb(),
            )
            return

        user_row = get_user(uid)
        if uid != OWNER_ID and user_row["credits"] < 1:
            await update.message.reply_text(
                f"{HEADER}\n{DIV}\n💜 Insufficient credits.\n{DIV}",
                parse_mode="Markdown",
                reply_markup=back_home_kb(),
            )
            return

        # deduct credit before check
        if uid != OWNER_ID:
            if not deduct_one_credit(uid):
                await update.message.reply_text(
                    "💜 Insufficient credits.",
                    reply_markup=back_home_kb(),
                )
                return

        msg = await update.message.reply_text(
            f"⏳ Checking `{email}`…", parse_mode="Markdown"
        )
        status = hi2_check(username, domain)

        # refund if non-billable
        if status in ("rate_limited", "error") and uid != OWNER_ID:
            refund_credit(uid, f"refund_{status}")

        icon = STATUS_EMOJI.get(status, "❓")
        bal  = get_user(uid)["credits"] if uid != OWNER_ID else "∞"
        await msg.edit_text(
            f"{HEADER}\n{DIV}\n"
            f"{icon} `{email}` — *{status}*\n"
            f"💜 Credits remaining: `{bal}`\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=back_home_kb(),
        )
        _audit(uid, "SINGLE_CHECK", detail=f"{email}={status}")
        return

    # ── legacy bulk paste ──
    if uid == OWNER_ID:
        await _run_bulk_text(update, ctx, text)


async def _handle_admin_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE, action: str, text: str):
    uid = update.effective_user.id
    ctx.user_data.pop("adm_action", None)

    if action == "give_uid":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text("⚠️ Invalid ID.")
            return
        row = get_user(target)
        if not row:
            await update.message.reply_text(f"⚠️ User `{target}` not found.", parse_mode="Markdown")
            return
        ctx.user_data["adm_credit_target"] = target
        ctx.user_data["adm_action"] = "give_amount"
        name = row["full_name"] or row["username"] or str(target)
        await update.message.reply_text(
            f"💜 Give credits to *{name}* (`{target}`)\n\nHow many credits?",
            parse_mode="Markdown",
        )
        return

    if action == "give_amount":
        try:
            amount = int(text)
            assert amount > 0
        except (ValueError, AssertionError):
            await update.message.reply_text("⚠️ Enter a positive integer.")
            return
        target  = ctx.user_data.pop("adm_credit_target", None)
        if not target:
            await update.message.reply_text("⚠️ Session expired. Try again.")
            return
        new_bal = add_credits(target, amount, "owner_gift", uid)
        await update.message.reply_text(
            f"✅ Added *{amount}* credits to `{target}`.\n💜 New balance: `{new_bal}`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Admin", callback_data="adm_back")
            ]]),
        )
        try:
            await ctx.bot.send_message(
                target,
                f"{HEADER}\n{DIV}\n"
                f"💜 *{amount} credits have been added to your account!*\n"
                f"New balance: `{new_bal}`\n{DIV}",
                parse_mode="Markdown",
            )
        except Exception:
            pass
        return

    if action == "remove_uid":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text("⚠️ Invalid ID.")
            return
        row = get_user(target)
        if not row:
            await update.message.reply_text(f"⚠️ User `{target}` not found.", parse_mode="Markdown")
            return
        ctx.user_data["adm_credit_target"] = target
        ctx.user_data["adm_action"] = "remove_amount"
        await update.message.reply_text(
            f"➖ Remove credits from `{target}` (balance: `{row['credits']}`)\n\nHow many?",
            parse_mode="Markdown",
        )
        return

    if action == "remove_amount":
        try:
            amount = int(text)
            assert amount > 0
        except (ValueError, AssertionError):
            await update.message.reply_text("⚠️ Enter a positive integer.")
            return
        target  = ctx.user_data.pop("adm_credit_target", None)
        if not target:
            await update.message.reply_text("⚠️ Session expired.")
            return
        new_bal = remove_credits(target, amount, "owner_removal", uid)
        await update.message.reply_text(
            f"➖ Removed credits from `{target}`.\n💜 New balance: `{new_bal}`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Admin", callback_data="adm_back")
            ]]),
        )
        return

    if action == "search_uid":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text("⚠️ Invalid ID.")
            return
        row = get_user(target)
        if not row:
            await update.message.reply_text(f"⚠️ User `{target}` not found.", parse_mode="Markdown")
            return
        txs   = get_credit_history(target, 5)
        tasks = get_user_tasks(target, 3)
        tx_lines = "\n".join(
            f"  {'+' if t['amount']>0 else ''}{t['amount']} _{t['reason']}_  `{t['created_at'][:16]}`"
            for t in txs
        ) or "  _none_"
        task_lines = "\n".join(
            f"  `{t['id']}` {t['status']} {t['processed']}/{t['total']}  ✅{t['available']}"
            for t in tasks
        ) or "  _none_"
        await update.message.reply_text(
            f"{HEADER} — 🔍 *User Details*\n{DIV}\n"
            f"🆔 `{row['telegram_id']}`\n"
            f"👤 {row['full_name'] or '-'}\n"
            f"👥 @{row['username'] or '-'}\n"
            f"📋 Status: `{row['status']}`\n"
            f"💜 Credits: `{row['credits']}`\n"
            f"🔢 Checks: `{row['total_checks']}`\n"
            f"🕐 Registered: `{row['registered_at'][:16]}`\n\n"
            f"*Last transactions:*\n{tx_lines}\n\n"
            f"*Last tasks:*\n{task_lines}\n"
            f"{DIV}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"approve:{row['telegram_id']}"),
                    InlineKeyboardButton("❌ Reject",  callback_data=f"reject:{row['telegram_id']}"),
                ],
                [InlineKeyboardButton("🔙 Admin", callback_data="adm_back")],
            ]),
        )
        return

    if action == "ban_uid":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text("⚠️ Invalid ID.")
            return
        row = get_user(target)
        if not row:
            await update.message.reply_text(f"⚠️ User `{target}` not found.", parse_mode="Markdown")
            return
        if row["status"] == "BANNED":
            set_user_status(target, "APPROVED", uid)
            await update.message.reply_text(
                f"✅ User `{target}` has been *UNBANNED*.", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin", callback_data="adm_back")]]),
            )
        else:
            set_user_status(target, "BANNED", uid)
            await update.message.reply_text(
                f"🚫 User `{target}` has been *BANNED*.", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin", callback_data="adm_back")]]),
            )
        return

    if action == "broadcast_msg":
        with get_db() as db:
            all_approved = db.execute(
                "SELECT telegram_id FROM users WHERE status='APPROVED'"
            ).fetchall()
        sent = 0
        failed = 0
        for r in all_approved:
            try:
                await ctx.bot.send_message(
                    r["telegram_id"],
                    f"📢 *Broadcast from Owner:*\n{DIV}\n{text}\n{DIV}",
                    parse_mode="Markdown",
                )
                sent += 1
            except Exception:
                failed += 1
        _audit(uid, "BROADCAST", detail=f"sent={sent} failed={failed}")
        await update.message.reply_text(
            f"📢 Broadcast complete.\n✅ Sent: `{sent}`  ❌ Failed: `{failed}`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin", callback_data="adm_back")]]),
        )
        return


# ══════════════════════════════════════════════════════════════════════════════
#  TXT FILE UPLOAD HANDLER
# ══════════════════════════════════════════════════════════════════════════════

async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    row = get_user(uid)

    if uid != OWNER_ID:
        if not row or row["status"] != "APPROVED":
            await update.message.reply_text("⛔ Access denied.")
            return
        if get_setting("maintenance_mode") == "1":
            await update.message.reply_text("🔧 Bot under maintenance.")
            return

    if has_active_task(uid):
        await update.message.reply_text(
            "⚠️ You already have a running task. Cancel it first.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⏹ Cancel Task", callback_data="cancel_task")
            ]]),
        )
        return

    doc = update.message.document
    if not doc.file_name or not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text("⚠️ Please upload a `.txt` file.")
        return

    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    if doc.file_size > max_bytes:
        await update.message.reply_text(
            f"⚠️ File too large. Max allowed: {MAX_UPLOAD_MB}MB."
        )
        return

    if get_setting("checking_enabled") != "1" and uid != OWNER_ID:
        await update.message.reply_text("⚙️ Checking is currently disabled.")
        return

    # Download file
    status_msg = await update.message.reply_text("⏳ Downloading file…")
    try:
        tg_file = await doc.get_file()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            tmp_path = tmp.name
        await tg_file.download_to_drive(tmp_path)
    except Exception as e:
        await status_msg.edit_text(f"⚠️ Download failed: {e}")
        return

    # Parse
    try:
        raw = Path(tmp_path).read_text(encoding="utf-8", errors="ignore")
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass

    lines = raw.splitlines()
    seen = set()
    jobs = []
    invalid_count = 0
    dup_count = 0

    for line in lines:
        s = line.strip()
        if not s:
            continue
        parsed = parse_line(s)
        if not parsed:
            invalid_count += 1
            continue
        uname, domain = parsed
        key = f"{uname}@{domain}"
        if key in seen:
            dup_count += 1
            continue
        seen.add(key)
        jobs.append((key, uname, domain))

    total_unique = len(jobs)
    if total_unique == 0:
        await status_msg.edit_text(
            "⚠️ No valid hi2.in / telegmail.com addresses found in the file.",
            reply_markup=back_home_kb(),
        )
        return

    user_credits = row["credits"] if uid != OWNER_ID else float("inf")
    can_process  = min(total_unique, int(user_credits)) if uid != OWNER_ID else total_unique

    await status_msg.edit_text(
        f"{HEADER} — 📂 *File Parsed*\n{DIV}\n"
        f"📄 File: `{doc.file_name}`\n"
        f"📊 Valid unique: `{total_unique}`\n"
        f"🚫 Invalid: `{invalid_count}`   🔁 Duplicates: `{dup_count}`\n"
        f"💜 Your credits: `{user_credits if uid != OWNER_ID else '∞'}`\n"
        f"⚡ Will process: `{can_process}`\n"
        f"{DIV}\n"
        f"_Reply YES to start, anything else to cancel_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Start", callback_data=f"confirm_txt:{total_unique}"),
            InlineKeyboardButton("❌ Cancel", callback_data="refresh_home"),
        ]]),
    )
    # Store job data in user_data for confirm callback
    ctx.user_data["pending_jobs"]    = jobs
    ctx.user_data["pending_filename"] = doc.file_name


# ── confirm TXT callback ──
async def callback_confirm_txt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid   = query.from_user.id

    if has_active_task(uid):
        await query.answer("Already running a task.", show_alert=True)
        return

    jobs     = ctx.user_data.pop("pending_jobs", [])
    filename = ctx.user_data.pop("pending_filename", "file.txt")
    if not jobs:
        await query.edit_message_text("⚠️ Session expired. Please re-upload.")
        return

    row = get_user(uid)
    user_credits = row["credits"] if uid != OWNER_ID else float("inf")
    can_process  = min(len(jobs), int(user_credits)) if uid != OWNER_ID else len(jobs)

    task_id = create_task(uid, filename, len(jobs))
    register_active_task(uid, task_id)

    prog_msg = await query.edit_message_text(
        f"{HEADER} — ⏳ *Processing*\n{DIV}\n"
        f"📄 `{filename}`\n"
        f"Progress: `0 / {can_process}`\n"
        f"💜 Credits: `{user_credits if uid != OWNER_ID else '∞'}`\n"
        f"{DIV}\nStatus: *STARTING…*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⏹ Cancel", callback_data="cancel_task")
        ]]),
    )

    # Run in background
    asyncio.create_task(
        _run_txt_task(uid, task_id, jobs, can_process, filename, prog_msg, ctx)
    )


async def _run_txt_task(
    uid: int,
    task_id: str,
    jobs: list,
    budget: int,
    filename: str,
    prog_msg,
    ctx: ContextTypes.DEFAULT_TYPE,
):
    processed = 0
    available_count = 0
    taken_count = 0
    error_count = 0
    last_edit   = time.time()
    stop_reason = "DONE"

    loop = asyncio.get_event_loop()

    for idx, (email, uname, domain) in enumerate(jobs):
        # check cancel
        if is_cancelled(uid):
            stop_reason = "CANCELLED"
            break

        # check global toggle
        if get_setting("checking_enabled") != "1" and uid != OWNER_ID:
            stop_reason = "CHECKING_DISABLED"
            break

        # check budget
        if uid != OWNER_ID:
            if not deduct_one_credit(uid):
                stop_reason = "INSUFFICIENT_CREDITS"
                break
        
        processed += 1

        # Run blocking check in executor
        status = await loop.run_in_executor(None, hi2_check, uname, domain)

        if status in ("rate_limited", "error"):
            if uid != OWNER_ID:
                refund_credit(uid, f"refund_{status}")
            error_count += 1
        elif status == "available":
            available_count += 1
        elif status == "taken":
            taken_count += 1

        save_result(task_id, email, status)
        update_task(
            task_id,
            processed=processed,
            available=available_count,
            taken=taken_count,
            errors=error_count,
            credits_used=(processed - error_count) if uid != OWNER_ID else 0,
        )

        # Update progress every 3s or every 5 items
        now = time.time()
        if now - last_edit >= 3 or processed % 5 == 0:
            remaining_credits = get_user(uid)["credits"] if uid != OWNER_ID else "∞"
            try:
                await prog_msg.edit_text(
                    f"{HEADER} — ⏳ *Processing*\n{DIV}\n"
                    f"📄 `{filename}`\n"
                    f"Progress: `{processed} / {budget}`\n"
                    f"💜 Credits: `{remaining_credits}`\n\n"
                    f"✅ Available: `{available_count}`\n"
                    f"❌ Taken: `{taken_count}`\n"
                    f"⚠️ Errors: `{error_count}`\n"
                    f"{DIV}\nStatus: *RUNNING…*",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⏹ Cancel", callback_data="cancel_task")
                    ]]),
                )
                last_edit = now
            except Exception:
                pass

        if processed >= budget:
            break

    # ── task finished ──
    finish_task(task_id, stop_reason)
    clear_active_task(uid)

    remaining_credits = get_user(uid)["credits"] if uid != OWNER_ID else "∞"
    not_processed = len(jobs) - processed

    status_icons = {
        "DONE":                "✅ COMPLETE",
        "CANCELLED":           "⏹ CANCELLED",
        "INSUFFICIENT_CREDITS":"💜 STOPPED — NO CREDITS",
        "CHECKING_DISABLED":   "⚙️ STOPPED — CHECKING DISABLED",
    }
    status_str = status_icons.get(stop_reason, stop_reason)

    summary = (
        f"{HEADER} — 📋 *Task Summary*\n{DIV}\n"
        f"📄 File: `{filename}`\n"
        f"📊 Total entries: `{len(jobs)}`\n"
        f"⚡ Processed: `{processed}`\n"
        f"⏭️ Remaining: `{not_processed}`\n"
        f"💜 Credits remaining: `{remaining_credits}`\n\n"
        f"✅ Available: `{available_count}`\n"
        f"❌ Taken: `{taken_count}`\n"
        f"⚠️ Errors: `{error_count}`\n"
        f"{DIV}\n"
        f"Status: *{status_str}*"
    )

    try:
        await prog_msg.edit_text(
            summary,
            parse_mode="Markdown",
            reply_markup=back_home_kb(),
        )
    except Exception:
        pass

    _audit(uid, "TASK_FINISH", detail=f"task={task_id} status={stop_reason} processed={processed}")

    # ── export files ──
    results = get_task_results(task_id)

    if available_count > 0:
        avail_lines = "\n".join(r["email"] for r in results if r["status"] == "available")
        avail_bytes = avail_lines.encode()
        try:
            await ctx.bot.send_document(
                uid,
                document=avail_bytes,
                filename=f"available_{task_id}.txt",
                caption=f"✅ *Available emails — {available_count} found*",
                parse_mode="Markdown",
            )
        except Exception:
            pass

    # Full report
    full_lines = [f"{STATUS_EMOJI.get(r['status'],'?')} {r['email']} — {r['status']}" for r in results]
    if not_processed > 0:
        remaining_emails = [j[0] for j in jobs[processed:]]
        for em in remaining_emails:
            full_lines.append(f"⏭️ {em} — not_processed")
    full_bytes = "\n".join(full_lines).encode()
    try:
        await ctx.bot.send_document(
            uid,
            document=full_bytes,
            filename=f"report_{task_id}.txt",
            caption="📋 *Full Check Report*",
            parse_mode="Markdown",
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
#  OWNER BULK PASTE  (legacy from original bot.py)
# ══════════════════════════════════════════════════════════════════════════════

async def _run_bulk_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    uid   = update.effective_user.id
    lines = text.splitlines()

    if len(lines) == 1 and "@" not in text:
        await update.message.reply_text(
            "Single username? Use `/check john` or paste full emails.",
            parse_mode="Markdown",
        )
        return

    seen    = set()
    jobs    = []
    results = []

    for line in lines:
        s = line.strip()
        if not s:
            continue
        parsed = parse_line(s)
        if not parsed:
            results.append({"email": s, "status": "invalid"})
            continue
        uname, domain = parsed
        key = f"{uname}@{domain}"
        if key in seen:
            results.append({"email": key, "status": "duplicate"})
            continue
        seen.add(key)
        jobs.append((key, uname, domain))

    if not jobs:
        await update.message.reply_text("No valid hi2.in / telegmail.com addresses found.")
        return

    if len(jobs) > 200:
        await update.message.reply_text("⛔ Max 200 emails per paste.")
        return

    msg = await update.message.reply_text(f"⏳ Checking {len(jobs)} email(s)…")

    with ThreadPoolExecutor(max_workers=min(10, len(jobs))) as pool:
        future_map = {
            pool.submit(hi2_check, u, d): em
            for em, u, d in jobs
        }
        for future in as_completed(future_map):
            em = future_map[future]
            try:
                status = future.result()
            except Exception:
                status = "error"
            results.append({"email": em, "status": status})

    order = {line.strip(): idx for idx, line in enumerate(lines) if line.strip()}
    results.sort(key=lambda x: order.get(x["email"], 10**9))

    counts = {k: 0 for k in STATUS_EMOJI}
    lines_out = []
    available = []
    for r in results:
        st  = r["status"]
        em  = r["email"]
        ico = STATUS_EMOJI.get(st, "❓")
        lines_out.append(f"{ico} `{em}` — {st}")
        counts[st] = counts.get(st, 0) + 1
        if st == "available":
            available.append(em)

    total   = len(results)
    summary = (
        f"\n*{total} checked* — "
        f"{counts['available']} ✅  {counts['taken']} ❌"
    )
    if counts["error"]:      summary += f"  {counts['error']} ⚠️"
    if counts["rate_limited"]: summary += f"  {counts['rate_limited']} 🕐"

    body = "\n".join(lines_out)
    if len(body) + len(summary) > 3800:
        keep = 60
        body = "\n".join(lines_out[:keep]) + f"\n…+{total-keep} more"

    text_out = body + summary
    keyboard = None
    if available:
        key = str(uuid.uuid4())[:8]
        ctx.user_data[f"copy_{key}"] = available
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                f"📋 Copy {len(available)} available",
                callback_data=f"copy_bulk:{key}",
            )
        ]])

    await msg.edit_text(text_out, parse_mode="Markdown", reply_markup=keyboard)


async def callback_copy_bulk(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != OWNER_ID:
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    await query.answer()
    key    = query.data[10:]
    emails = ctx.user_data.pop(f"copy_{key}", [])
    if not emails:
        await query.message.reply_text("Cache expired — check again.")
        return
    chunk = "\n".join(emails)
    await query.message.reply_text(
        f"📋 *Available ({len(emails)}):*\n```\n{chunk}\n```",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════════════════════════════════════
#  /check command  (kept from original)
# ══════════════════════════════════════════════════════════════════════════════

@owner_only
async def cmd_check(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: `/check john@hi2.in`", parse_mode="Markdown")
        return
    raw    = " ".join(ctx.args).strip()
    parsed = parse_line(raw)
    if not parsed:
        await update.message.reply_text(
            f"⚠️ Invalid: `{raw}`", parse_mode="Markdown"
        )
        return
    username, domain = parsed
    msg = await update.message.reply_text(
        f"⏳ Checking `{username}@{domain}`…", parse_mode="Markdown"
    )
    status = hi2_check(username, domain)
    icon   = STATUS_EMOJI.get(status, "❓")
    await msg.edit_text(
        f"{icon} `{username}@{domain}` — *{status}*",
        parse_mode="Markdown",
    )


# ══════════════════════════════════════════════════════════════════════════════
#  /cancel command
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ctx.user_data.pop("awaiting", None)
    ctx.user_data.pop("adm_action", None)
    if has_active_task(uid):
        cancel_user_task(uid)
        await update.message.reply_text("⏹ Cancellation requested for your running task.")
    else:
        await update.message.reply_text("Nothing to cancel.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if not BOT_TOKEN or BOT_TOKEN == "":
        print("❌  BOT_TOKEN is not set.")
        return
    if OWNER_ID == 0:
        print("❌  OWNER_ID is not set.")
        return

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("admin",  cmd_admin))
    app.add_handler(CommandHandler("check",  cmd_check))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("help",   lambda u, c: cmd_start(u, c)))

    app.add_handler(CallbackQueryHandler(callback_confirm_txt, pattern=r"^confirm_txt:"))
    app.add_handler(CallbackQueryHandler(callback_copy_bulk,   pattern=r"^copy_bulk:"))
    app.add_handler(CallbackQueryHandler(callback_router))

    app.add_handler(MessageHandler(filters.Document.MimeType("text/plain"), handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("✅  HI2 CHECKER BOT is running…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
