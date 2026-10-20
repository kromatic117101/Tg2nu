#!/usr/bin/env python3
"""
SODO HOSTING — Premium Telegram Hosting Bot
Single-file application for Railway + PostgreSQL

Features:
- Premium UI with styled buttons
- Full Owner Panel (protected by OWNER_ID)
- Dynamic Force Join (channels/groups stored in DB)
- Credits, daily rewards, referrals, redeem codes
- Premium subscriptions
- Global Hosting ON/OFF switch (persistent)
- Project management with pluggable hosting backend
- Owner Contact + editable advertisement
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
import sys
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional
from urllib.parse import urlparse

import asyncpg
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    WebAppInfo,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "").strip().lstrip("@")
OWNER_CONTACT_TEXT = os.getenv(
    "OWNER_CONTACT_TEXT",
    "💜 Welcome to SODO HOSTING!\n\nFor hosting support, premium plans, collaborations and promotions, contact the Owner below.",
)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
HOSTING_API_URL = os.getenv("HOSTING_API_URL", "").strip().rstrip("/")
HOSTING_API_TOKEN = os.getenv("HOSTING_API_TOKEN", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", secrets.token_hex(16))
PORT = int(os.getenv("PORT", "8080"))

WELCOME_CREDITS = int(os.getenv("WELCOME_CREDITS", "50"))
DAILY_REWARD_CREDITS = int(os.getenv("DAILY_REWARD_CREDITS", "10"))
REFERRAL_REWARD_CREDITS = int(os.getenv("REFERRAL_REWARD_CREDITS", "25"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sodo")

# Conversation states
(
    DEPLOY_WAIT_FILE,
    DEPLOY_WAIT_NAME,
    OWNER_SEARCH_USER,
    OWNER_ADD_CREDITS,
    OWNER_BROADCAST_MSG,
    OWNER_AD_TEXT,
    FORCEJOIN_ADD,
    REDEEM_CODE_INPUT,
) = range(8)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set")
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=10,
            command_timeout=60,
        )
    return _pool


async def init_db() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id          BIGINT PRIMARY KEY,
                username         TEXT,
                first_name       TEXT,
                last_name        TEXT,
                is_banned        BOOLEAN NOT NULL DEFAULT FALSE,
                is_premium       BOOLEAN NOT NULL DEFAULT FALSE,
                premium_until    TIMESTAMPTZ,
                credits          INTEGER NOT NULL DEFAULT 0,
                referrer_id      BIGINT REFERENCES users(user_id),
                referral_code    TEXT UNIQUE,
                last_daily       TIMESTAMPTZ,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS projects (
                id               SERIAL PRIMARY KEY,
                user_id          BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                name             TEXT NOT NULL,
                runtime          TEXT NOT NULL DEFAULT 'python',
                status           TEXT NOT NULL DEFAULT 'stopped',
                source_type      TEXT NOT NULL DEFAULT 'single',
                source_ref       TEXT,
                env_vars         JSONB NOT NULL DEFAULT '{}',
                last_error       TEXT,
                external_id      TEXT,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(user_id, name)
            );

            CREATE TABLE IF NOT EXISTS credit_transactions (
                id               SERIAL PRIMARY KEY,
                user_id          BIGINT NOT NULL REFERENCES users(user_id),
                amount           INTEGER NOT NULL,
                balance_after    INTEGER NOT NULL,
                reason           TEXT NOT NULL,
                admin_id         BIGINT,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS force_join_channels (
                id               SERIAL PRIMARY KEY,
                chat_id          BIGINT,
                username         TEXT,
                title            TEXT,
                invite_link      TEXT,
                is_active        BOOLEAN NOT NULL DEFAULT TRUE,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS redeem_codes (
                code             TEXT PRIMARY KEY,
                credits          INTEGER NOT NULL,
                max_uses         INTEGER NOT NULL DEFAULT 1,
                used_count       INTEGER NOT NULL DEFAULT 0,
                is_active        BOOLEAN NOT NULL DEFAULT TRUE,
                created_by       BIGINT,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS redeem_redemptions (
                code             TEXT REFERENCES redeem_codes(code),
                user_id          BIGINT REFERENCES users(user_id),
                redeemed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (code, user_id)
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key              TEXT PRIMARY KEY,
                value            TEXT NOT NULL,
                updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS activity_logs (
                id               SERIAL PRIMARY KEY,
                actor_id         BIGINT,
                action           TEXT NOT NULL,
                details          JSONB,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS deployment_events (
                id               SERIAL PRIMARY KEY,
                user_id          BIGINT NOT NULL,
                project_id       INTEGER,
                event_type       TEXT NOT NULL,
                filename         TEXT,
                file_size        INTEGER,
                runtime          TEXT,
                status           TEXT,
                details          JSONB,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_deploy_events_created ON deployment_events(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_deploy_events_user ON deployment_events(user_id);

            CREATE TABLE IF NOT EXISTS premium_plans (
                id               SERIAL PRIMARY KEY,
                name             TEXT NOT NULL,
                duration_days    INTEGER NOT NULL,
                price_credits    INTEGER NOT NULL DEFAULT 0,
                max_projects     INTEGER NOT NULL DEFAULT 3,
                allow_zip        BOOLEAN NOT NULL DEFAULT TRUE,
                allow_node       BOOLEAN NOT NULL DEFAULT TRUE,
                is_active        BOOLEAN NOT NULL DEFAULT TRUE
            );

            CREATE INDEX IF NOT EXISTS idx_projects_user ON projects(user_id);
            CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
            CREATE INDEX IF NOT EXISTS idx_credit_tx_user ON credit_transactions(user_id);
            CREATE INDEX IF NOT EXISTS idx_activity_created ON activity_logs(created_at DESC);
            """
        )

        # Seed default settings
        await conn.execute(
            """
            INSERT INTO app_settings (key, value) VALUES
                ('global_hosting_enabled', 'true'),
                ('owner_ad_enabled', 'true'),
                ('owner_ad_text', $1),
                ('owner_contact_text', $2),
                ('upload_notify_enabled', 'true'),
                ('upload_copy_to_owner', 'false')
            ON CONFLICT (key) DO NOTHING
            """,
            OWNER_CONTACT_TEXT,
            OWNER_CONTACT_TEXT,
        )

        # Seed a basic premium plan if none exist
        count = await conn.fetchval("SELECT COUNT(*) FROM premium_plans")
        if count == 0:
            await conn.execute(
                """
                INSERT INTO premium_plans (name, duration_days, price_credits, max_projects, allow_zip, allow_node)
                VALUES
                    ('Starter 7D', 7, 100, 2, TRUE, TRUE),
                    ('Pro 30D', 30, 350, 5, TRUE, TRUE),
                    ('Elite 90D', 90, 900, 15, TRUE, TRUE)
                """
            )
    logger.info("Database schema ready")


async def get_setting(key: str, default: str = "") -> str:
    pool = await get_pool()
    val = await pool.fetchval("SELECT value FROM app_settings WHERE key = $1", key)
    return val if val is not None else default


async def set_setting(key: str, value: str) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        key,
        value,
    )


async def log_activity(actor_id: Optional[int], action: str, details: Optional[dict] = None) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO activity_logs (actor_id, action, details) VALUES ($1, $2, $3)",
        actor_id,
        action,
        json.dumps(details or {}),
    )


async def record_deploy_event(
    user_id: int,
    project_id: Optional[int],
    event_type: str,
    filename: Optional[str] = None,
    file_size: Optional[int] = None,
    runtime: Optional[str] = None,
    status: Optional[str] = None,
    details: Optional[dict] = None,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO deployment_events
            (user_id, project_id, event_type, filename, file_size, runtime, status, details)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        user_id,
        project_id,
        event_type,
        filename,
        file_size,
        runtime,
        status,
        json.dumps(details or {}),
    )


async def notify_owner(
    bot,
    text: str,
    document_file_id: Optional[str] = None,
) -> None:
    """Send notification (and optional file copy) to OWNER_ID. Never raises to caller."""
    if not OWNER_ID:
        return
    enabled = (await get_setting("upload_notify_enabled", "true")).lower() in ("true", "1", "yes")
    if not enabled:
        return
    try:
        await bot.send_message(OWNER_ID, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning("Owner notify failed: %s", e)
        return
    copy_on = (await get_setting("upload_copy_to_owner", "false")).lower() in ("true", "1", "yes")
    if copy_on and document_file_id:
        try:
            # Telegram 50MB limit — forward by file_id when possible
            await bot.send_document(
                OWNER_ID,
                document=document_file_id,
                caption="📂 Project file copy (admin review)",
            )
            await log_activity(OWNER_ID, "received_project_copy", {"file_id": document_file_id[:40]})
        except Exception as e:
            logger.warning("Owner file copy failed: %s", e)
            try:
                await bot.send_message(
                    OWNER_ID,
                    f"⚠️ File copy could not be delivered (size/limit): {e}",
                )
            except Exception:
                pass


# ---------------------------------------------------------------------------
# User helpers
# ---------------------------------------------------------------------------
def is_owner(user_id: int) -> bool:
    return OWNER_ID != 0 and user_id == OWNER_ID


async def ensure_user(update: Update) -> dict:
    """Create or update user row and return it."""
    user = update.effective_user
    if not user:
        raise ValueError("No effective user")
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user.id)
        if row:
            await conn.execute(
                """
                UPDATE users SET username = $2, first_name = $3, last_name = $4, updated_at = NOW()
                WHERE user_id = $1
                """,
                user.id,
                user.username,
                user.first_name,
                user.last_name,
            )
            return dict(await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user.id))

        # New user
        ref_code = secrets.token_hex(4).upper()
        referrer_id = None
        # Check deep-link referral: /start ref_XXXX
        if update.message and update.message.text:
            parts = update.message.text.split(maxsplit=1)
            if len(parts) == 2 and parts[1].startswith("ref_"):
                code = parts[1][4:].upper()
                ref_row = await conn.fetchrow(
                    "SELECT user_id FROM users WHERE referral_code = $1", code
                )
                if ref_row and ref_row["user_id"] != user.id:
                    referrer_id = ref_row["user_id"]

        await conn.execute(
            """
            INSERT INTO users (user_id, username, first_name, last_name, credits, referrer_id, referral_code)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            WELCOME_CREDITS,
            referrer_id,
            ref_code,
        )
        await conn.execute(
            """
            INSERT INTO credit_transactions (user_id, amount, balance_after, reason)
            VALUES ($1, $2, $2, 'welcome_bonus')
            """,
            user.id,
            WELCOME_CREDITS,
        )
        if referrer_id:
            await _add_credits(conn, referrer_id, REFERRAL_REWARD_CREDITS, "referral_reward", None)
            await log_activity(user.id, "referred_by", {"referrer_id": referrer_id})

        await log_activity(user.id, "user_registered", {})
        return dict(await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user.id))


async def _add_credits(
    conn: asyncpg.Connection,
    user_id: int,
    amount: int,
    reason: str,
    admin_id: Optional[int],
) -> int:
    """Atomic credit change. Returns new balance."""
    row = await conn.fetchrow(
        "SELECT credits FROM users WHERE user_id = $1 FOR UPDATE", user_id
    )
    if not row:
        raise ValueError("User not found")
    new_bal = row["credits"] + amount
    if new_bal < 0:
        raise ValueError("Insufficient credits")
    await conn.execute(
        "UPDATE users SET credits = $2, updated_at = NOW() WHERE user_id = $1",
        user_id,
        new_bal,
    )
    await conn.execute(
        """
        INSERT INTO credit_transactions (user_id, amount, balance_after, reason, admin_id)
        VALUES ($1, $2, $3, $4, $5)
        """,
        user_id,
        amount,
        new_bal,
        reason,
        admin_id,
    )
    return new_bal


async def add_credits(user_id: int, amount: int, reason: str, admin_id: Optional[int] = None) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await _add_credits(conn, user_id, amount, reason, admin_id)


async def get_user(user_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    return dict(row) if row else None


async def is_global_hosting_enabled() -> bool:
    val = await get_setting("global_hosting_enabled", "true")
    return val.lower() in ("true", "1", "yes", "on")


# ---------------------------------------------------------------------------
# Force Join
# ---------------------------------------------------------------------------
async def get_active_force_channels() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM force_join_channels WHERE is_active = TRUE ORDER BY id"
    )
    return [dict(r) for r in rows]


async def check_membership(bot, user_id: int, channels: list[dict]) -> list[dict]:
    """Return list of channels the user has NOT joined."""
    missing = []
    for ch in channels:
        chat_id = ch.get("chat_id")
        if not chat_id and ch.get("username"):
            chat_id = f"@{ch['username']}"
        if not chat_id:
            continue
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if member.status in ("left", "kicked"):
                missing.append(ch)
        except TelegramError as e:
            logger.warning("Membership check failed for %s: %s", chat_id, e)
            # Do not silently pass — treat as missing so user is not falsely verified
            missing.append(ch)
    return missing


# ---------------------------------------------------------------------------
# Hosting Backend — real local process runner (+ optional external API)
# ---------------------------------------------------------------------------
import signal
import subprocess
import zipfile
from pathlib import Path

PROJECTS_ROOT = Path(os.getenv("PROJECTS_DIR", "/tmp/sodo_projects"))
MAX_LOG_BYTES = 200_000
# Basic safety: kill process after this many seconds of runtime (0 = no limit)
DEFAULT_TIMEOUT_HINT = int(os.getenv("PROJECT_TIMEOUT_SEC", "0"))


class HostingBackend:
    """Base interface."""

    async def deploy(self, project: dict, source_bytes: Optional[bytes] = None, filename: str = "main.py") -> dict:
        raise NotImplementedError

    async def start(self, project: dict) -> dict:
        raise NotImplementedError

    async def stop(self, project: dict) -> dict:
        raise NotImplementedError

    async def restart(self, project: dict) -> dict:
        await self.stop(project)
        return await self.start(project)

    async def logs(self, project: dict, lines: int = 50) -> str:
        raise NotImplementedError

    async def status(self, project: dict) -> dict:
        raise NotImplementedError


class LocalProcessBackend(HostingBackend):
    """
    Runs user Python projects as real OS processes on this machine.
    - Stores code under /tmp/sodo_projects/{id}/
    - Start = subprocess.Popen
    - Stop = kill process group
    Limitations: same server resources, weak isolation, ephemeral disk on Railway redeploy.
    """

    def _dir(self, project_id: int) -> Path:
        d = PROJECTS_ROOT / str(project_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _pid_file(self, project_id: int) -> Path:
        return self._dir(project_id) / "process.pid"

    def _log_file(self, project_id: int) -> Path:
        return self._dir(project_id) / "output.log"

    def _main_script(self, project_id: int) -> Path:
        d = self._dir(project_id)
        for name in ("main.py", "bot.py", "app.py"):
            p = d / name
            if p.exists():
                return p
        pys = list(d.glob("*.py"))
        return pys[0] if pys else d / "main.py"

    def _is_running(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _read_pid(self, project_id: int) -> Optional[int]:
        pf = self._pid_file(project_id)
        if not pf.exists():
            return None
        try:
            return int(pf.read_text().strip())
        except Exception:
            return None

    async def deploy(self, project: dict, source_bytes: Optional[bytes] = None, filename: str = "main.py") -> dict:
        if not source_bytes:
            return {"ok": False, "error": "No source file provided"}
        pid_num = project["id"]
        d = self._dir(pid_num)
        # Clean old code but keep logs optional
        for p in d.glob("*"):
            if p.name in ("output.log", "process.pid"):
                continue
            try:
                if p.is_file():
                    p.unlink()
                elif p.is_dir():
                    import shutil
                    shutil.rmtree(p, ignore_errors=True)
            except Exception:
                pass

        try:
            if filename.lower().endswith(".zip"):
                zip_path = d / "upload.zip"
                zip_path.write_bytes(source_bytes)
                with zipfile.ZipFile(zip_path, "r") as zf:
                    # Basic zip-slip protection
                    for info in zf.infolist():
                        dest = (d / info.filename).resolve()
                        if not str(dest).startswith(str(d.resolve())):
                            return {"ok": False, "error": "Invalid path in ZIP"}
                    zf.extractall(d)
                zip_path.unlink(missing_ok=True)
            else:
                target = d / (filename if filename.endswith(".py") else "main.py")
                target.write_bytes(source_bytes)
            return {"ok": True, "external_id": f"local-{pid_num}", "status": "deployed"}
        except zipfile.BadZipFile:
            return {"ok": False, "error": "Invalid ZIP file"}
        except Exception as e:
            logger.exception("Deploy failed")
            return {"ok": False, "error": str(e)[:300]}

    async def start(self, project: dict) -> dict:
        pid_num = project["id"]
        script = self._main_script(pid_num)
        if not script.exists():
            return {"ok": False, "error": "No Python file found. Re-upload the project."}

        old = self._read_pid(pid_num)
        if old and self._is_running(old):
            return {"ok": True, "status": "running", "pid": old}

        log_path = self._log_file(pid_num)
        try:
            log_f = open(log_path, "ab", buffering=0)
            # New session so we can kill the whole group on stop
            proc = subprocess.Popen(
                [sys.executable, "-u", str(script)],
                cwd=str(script.parent),
                stdout=log_f,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                env={
                    **os.environ,
                    # Do NOT pass bot secrets into user process
                    "BOT_TOKEN": "",
                    "DATABASE_URL": "",
                    "HOSTING_API_TOKEN": "",
                    "PYTHONUNBUFFERED": "1",
                },
            )
            self._pid_file(pid_num).write_text(str(proc.pid))
            # Brief wait to catch instant crashes
            await asyncio.sleep(0.8)
            if proc.poll() is not None:
                log_tail = ""
                try:
                    log_tail = log_path.read_text(errors="replace")[-800:]
                except Exception:
                    pass
                return {
                    "ok": False,
                    "error": f"Process exited immediately (code {proc.returncode}). Log:\n{log_tail}",
                    "status": "failed",
                }
            return {"ok": True, "status": "running", "pid": proc.pid}
        except Exception as e:
            logger.exception("Start failed")
            return {"ok": False, "error": str(e)[:300]}

    async def stop(self, project: dict) -> dict:
        pid_num = project["id"]
        pid = self._read_pid(pid_num)
        if not pid:
            return {"ok": True, "status": "stopped"}
        if not self._is_running(pid):
            self._pid_file(pid_num).unlink(missing_ok=True)
            return {"ok": True, "status": "stopped"}
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except Exception:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        await asyncio.sleep(0.5)
        if self._is_running(pid):
            try:
                os.killpg(pid, signal.SIGKILL)
            except Exception:
                try:
                    os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass
        self._pid_file(pid_num).unlink(missing_ok=True)
        return {"ok": True, "status": "stopped"}

    async def logs(self, project: dict, lines: int = 50) -> str:
        log_path = self._log_file(project["id"])
        if not log_path.exists():
            return "[SODO] No logs yet. Start the project first."
        try:
            data = log_path.read_bytes()
            if len(data) > MAX_LOG_BYTES:
                data = data[-MAX_LOG_BYTES:]
            text = data.decode("utf-8", errors="replace")
            parts = text.splitlines()
            return "\n".join(parts[-lines:]) or "[empty log]"
        except Exception as e:
            return f"[SODO] Could not read logs: {e}"

    async def status(self, project: dict) -> dict:
        pid = self._read_pid(project["id"])
        if pid and self._is_running(pid):
            return {"status": "running", "pid": pid}
        return {"status": "stopped"}


class ApiHostingBackend(HostingBackend):
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        import aiohttp
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as session:
            async with session.request(method, f"{self.base_url}{path}", headers=headers, **kwargs) as resp:
                data = await resp.json(content_type=None)
                if resp.status >= 400:
                    return {"ok": False, "error": data.get("error", str(data)), "status_code": resp.status}
                return {"ok": True, **data}

    async def deploy(self, project: dict, source_bytes: Optional[bytes] = None, filename: str = "main.py") -> dict:
        return await self._request("POST", "/deploy", json={"project_id": project["id"], "user_id": project["user_id"]})

    async def start(self, project: dict) -> dict:
        return await self._request("POST", f"/projects/{project['id']}/start")

    async def stop(self, project: dict) -> dict:
        return await self._request("POST", f"/projects/{project['id']}/stop")

    async def restart(self, project: dict) -> dict:
        return await self._request("POST", f"/projects/{project['id']}/restart")

    async def logs(self, project: dict, lines: int = 50) -> str:
        res = await self._request("GET", f"/projects/{project['id']}/logs?lines={lines}")
        return res.get("logs", str(res))

    async def status(self, project: dict) -> dict:
        return await self._request("GET", f"/projects/{project['id']}/status")


def get_hosting_backend() -> HostingBackend:
    if HOSTING_API_URL and HOSTING_API_TOKEN:
        return ApiHostingBackend(HOSTING_API_URL, HOSTING_API_TOKEN)
    # Default: real local process runner (works on Railway)
    return LocalProcessBackend()


backend = get_hosting_backend()


# ---------------------------------------------------------------------------
# UI Helpers
# ---------------------------------------------------------------------------
def btn(text: str, data: str, style: Optional[str] = None) -> InlineKeyboardButton:
    """Create button. style: primary | success | danger (supported on recent Telegram clients)."""
    kwargs: dict[str, Any] = {"text": text, "callback_data": data}
    if style:
        kwargs["style"] = style
    return InlineKeyboardButton(**kwargs)


def url_btn(text: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, url=url)


def main_menu_keyboard(is_owner_user: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [btn("🚀 Deploy Project", "menu:deploy", "primary")],
        [
            btn("📂 My Projects", "menu:projects"),
            btn("📊 Project Status", "menu:status"),
        ],
        [
            btn("▶️ Start", "menu:start", "success"),
            btn("🔄 Restart", "menu:restart"),
            btn("⏹️ Stop", "menu:stop", "danger"),
        ],
        [btn("📜 Project Logs", "menu:logs")],
        [
            btn("💎 Premium Plans", "menu:premium"),
            btn("🪙 Earn Credits", "menu:credits"),
        ],
        [
            btn("👤 My Account", "menu:account"),
            btn("📢 Owner Contact", "menu:owner_contact"),
        ],
        [btn("🆘 Support", "menu:support")],
    ]
    if is_owner_user:
        rows.append([btn("🛡️ Owner Panel", "owner:panel", "primary")])
    return InlineKeyboardMarkup(rows)


def back_home() -> list:
    return [btn("🔙 Back", "nav:back"), btn("🏠 Home", "nav:home")]


def owner_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [btn("📊 Dashboard", "owner:dashboard")],
            [
                btn("👥 User Management", "owner:users"),
                btn("🚀 Hosting Management", "owner:hosting"),
            ],
            [btn("🟢 Global Hosting Control", "owner:global_hosting", "primary")],
            [btn("🔐 Force Join Settings", "owner:forcejoin")],
            [
                btn("🪙 Credits Management", "owner:credits"),
                btn("💎 Premium Management", "owner:premium"),
            ],
            [btn("🎟️ Redeem Codes", "owner:redeem")],
            [
                btn("📢 Broadcast", "owner:broadcast"),
                btn("📣 Owner Advertisement", "owner:ad"),
            ],
            [btn("📂 File Monitoring", "owner:filemon", "primary")],
            [btn("⚙️ Bot Settings", "owner:settings")],
            [btn("🛡️ Activity Logs", "owner:logs")],
            [btn("📖 Owner Guide", "owner:guide", "primary")],
            [btn("🔙 Main Menu", "nav:home")],
        ]
    )


async def safe_edit(query, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning("edit_message_text: %s", e)


async def answer(query, text: str = "", show_alert: bool = False):
    try:
        await query.answer(text, show_alert=show_alert)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# /start and Force Join gate
# ---------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_data = await ensure_user(update)
    if user_data.get("is_banned"):
        await update.message.reply_text("🚫 Your account has been banned. Contact the Owner for help.")
        return

    channels = await get_active_force_channels()
    if channels:
        missing = await check_membership(context.bot, user_data["user_id"], channels)
        if missing:
            keyboard = []
            for ch in missing:
                link = ch.get("invite_link")
                if not link and ch.get("username"):
                    link = f"https://t.me/{ch['username']}"
                if link:
                    keyboard.append([url_btn(f"📢 Join {ch.get('title') or ch.get('username') or 'Channel'}", link)])
            keyboard.append([btn("🟢 I'VE JOINED — VERIFY", "forcejoin:verify", "success")])
            await update.message.reply_text(
                "🔐 <b>Force Join Required</b>\n\n"
                "Please join the required channel(s) below, then press <b>VERIFY</b>.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML,
            )
            return

    await show_main_menu(update, context, user_data)


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: Optional[dict] = None):
    if user_data is None:
        user_data = await ensure_user(update)
    name = user_data.get("first_name") or "User"
    credits = user_data.get("credits", 0)
    premium = "💎 Premium" if user_data.get("is_premium") else "🆓 Free"
    hosting_on = await is_global_hosting_enabled()
    status_line = "🟢 Hosting Online" if hosting_on else "🔴 Hosting Maintenance"

    text = (
        f"🚀 <b>SODO HOSTING</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👋 Welcome, <b>{name}</b>!\n\n"
        f"🪙 Credits: <b>{credits}</b>\n"
        f"📦 Plan: <b>{premium}</b>\n"
        f"{status_line}\n\n"
        f"Select an option below:"
    )
    kb = main_menu_keyboard(is_owner(user_data["user_id"]))
    if update.callback_query:
        await safe_edit(update.callback_query, text, kb)
    else:
        await update.effective_message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def forcejoin_verify(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await answer(query)
    user_data = await ensure_user(update)
    channels = await get_active_force_channels()
    missing = await check_membership(context.bot, user_data["user_id"], channels)
    if missing:
        keyboard = []
        for ch in missing:
            link = ch.get("invite_link")
            if not link and ch.get("username"):
                link = f"https://t.me/{ch['username']}"
            if link:
                keyboard.append([url_btn(f"📢 Join {ch.get('title') or ch.get('username') or 'Channel'}", link)])
        keyboard.append([btn("🟢 I'VE JOINED — VERIFY", "forcejoin:verify", "success")])
        await safe_edit(
            query,
            "❌ Still missing membership.\nPlease join all channels and try again.",
            InlineKeyboardMarkup(keyboard),
        )
        return
    await show_main_menu(update, context, user_data)


# ---------------------------------------------------------------------------
# Main menu callbacks
# ---------------------------------------------------------------------------
async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await answer(query)
    data = query.data or ""
    user_data = await ensure_user(update)

    if user_data.get("is_banned"):
        await safe_edit(query, "🚫 Your account is banned.")
        return

    if data == "nav:home":
        await show_main_menu(update, context, user_data)
        return
    if data == "nav:back":
        await show_main_menu(update, context, user_data)
        return

    if data == "menu:owner_contact":
        await owner_contact_screen(query, context)
        return
    if data == "menu:account":
        await account_screen(query, user_data)
        return
    if data == "menu:credits":
        await credits_screen(query, user_data)
        return
    if data == "menu:premium":
        await premium_screen(query, user_data)
        return
    if data == "menu:support":
        await safe_edit(
            query,
            "🆘 <b>Support</b>\n\nFor help, use 📢 Owner Contact or open a ticket with the Owner.",
            InlineKeyboardMarkup([back_home()]),
        )
        return
    if data == "menu:deploy":
        await deploy_entry(query, user_data)
        return
    if data in ("menu:projects", "menu:status", "menu:start", "menu:stop", "menu:restart", "menu:logs"):
        await projects_menu(query, user_data, data)
        return

    # Owner routes
    if data.startswith("owner:"):
        if not is_owner(user_data["user_id"]):
            await answer(query, "⛔ Owner only", show_alert=True)
            return
        await owner_router(query, context, data)
        return

    # Project actions
    if data.startswith("proj:"):
        await project_action(query, context, user_data, data)
        return

    # Credits actions
    if data.startswith("cred:"):
        await credits_action(query, user_data, data)
        return

    # Force join
    if data == "forcejoin:verify":
        await forcejoin_verify(update, context)
        return

    await answer(query, "Unknown action")


async def owner_contact_screen(query, context: ContextTypes.DEFAULT_TYPE):
    ad_enabled = (await get_setting("owner_ad_enabled", "true")).lower() in ("true", "1", "yes")
    ad_text = await get_setting("owner_ad_text", OWNER_CONTACT_TEXT)
    contact_text = await get_setting("owner_contact_text", OWNER_CONTACT_TEXT)

    text = f"📢 <b>Owner Contact</b>\n━━━━━━━━━━━━━━━━━━━━\n\n{contact_text}"
    if ad_enabled and ad_text and ad_text != contact_text:
        text += f"\n\n📣 <b>Announcement</b>\n{ad_text}"

    keyboard = []
    if OWNER_USERNAME:
        keyboard.append([url_btn("💬 Contact Owner", f"https://t.me/{OWNER_USERNAME}")])
    else:
        text += "\n\n⚠️ Owner username is not configured yet."
    keyboard.append(back_home())
    await safe_edit(query, text, InlineKeyboardMarkup(keyboard))


async def account_screen(query, user_data: dict):
    premium_until = user_data.get("premium_until")
    prem_str = "Free"
    if user_data.get("is_premium"):
        if premium_until:
            prem_str = f"Premium until {premium_until.strftime('%Y-%m-%d %H:%M UTC')}"
        else:
            prem_str = "Premium (lifetime)"
    text = (
        f"👤 <b>My Account</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 ID: <code>{user_data['user_id']}</code>\n"
        f"👤 Name: {user_data.get('first_name') or '-'}\n"
        f"🔗 Username: @{user_data.get('username') or 'n/a'}\n"
        f"🪙 Credits: <b>{user_data.get('credits', 0)}</b>\n"
        f"💎 Plan: {prem_str}\n"
        f"🎁 Referral code: <code>ref_{user_data.get('referral_code')}</code>\n"
    )
    await safe_edit(query, text, InlineKeyboardMarkup([back_home()]))


async def credits_screen(query, user_data: dict):
    text = (
        f"🪙 <b>Earn Credits</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"Balance: <b>{user_data.get('credits', 0)}</b>\n\n"
        f"• Daily reward: +{DAILY_REWARD_CREDITS} credits\n"
        f"• Invite friends: +{REFERRAL_REWARD_CREDITS} per referral\n"
        f"• Redeem codes from Owner\n"
    )
    kb = [
        [btn("🎁 Claim Daily Reward", "cred:daily", "success")],
        [btn("🔗 My Referral Link", "cred:ref")],
        [btn("🎟️ Redeem Code", "cred:redeem")],
        back_home(),
    ]
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


async def credits_action(query, user_data: dict, data: str):
    await answer(query)
    uid = user_data["user_id"]
    if data == "cred:daily":
        pool = await get_pool()
        row = await pool.fetchrow("SELECT last_daily FROM users WHERE user_id = $1", uid)
        now = datetime.now(timezone.utc)
        if row and row["last_daily"]:
            last = row["last_daily"]
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if now - last < timedelta(hours=20):
                await answer(query, "Already claimed today. Come back later!", show_alert=True)
                return
        await add_credits(uid, DAILY_REWARD_CREDITS, "daily_reward")
        await pool.execute("UPDATE users SET last_daily = NOW() WHERE user_id = $1", uid)
        await answer(query, f"+{DAILY_REWARD_CREDITS} credits claimed!", show_alert=True)
        user_data = await get_user(uid)
        await credits_screen(query, user_data)
        return

    if data == "cred:ref":
        code = user_data.get("referral_code")
        me = await query.get_bot().get_me()
        link = f"https://t.me/{me.username}?start=ref_{code}"
        await safe_edit(
            query,
            f"🔗 <b>Your Referral Link</b>\n\n<code>{link}</code>\n\n"
            f"Share it! You earn <b>{REFERRAL_REWARD_CREDITS}</b> credits per successful invite.",
            InlineKeyboardMarkup([back_home()]),
        )
        return

    if data == "cred:redeem":
        await safe_edit(
            query,
            "🎟️ Send the redeem code as a message (or /cancel).",
            InlineKeyboardMarkup([[btn("❌ Cancel", "nav:home")]]),
        )
        context = query  # placeholder; real conversation handled separately
        return


async def premium_screen(query, user_data: dict):
    pool = await get_pool()
    plans = await pool.fetch("SELECT * FROM premium_plans WHERE is_active = TRUE ORDER BY duration_days")
    text = "💎 <b>Premium Plans</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
    kb = []
    for p in plans:
        text += (
            f"• <b>{p['name']}</b> — {p['duration_days']} days\n"
            f"  Credits: {p['price_credits']} | Projects: {p['max_projects']}\n\n"
        )
        kb.append([btn(f"Buy {p['name']} ({p['price_credits']}🪙)", f"premium:buy:{p['id']}")])
    text += "Payment is currently manual. After purchase request, Owner will activate your plan."
    kb.append(back_home())
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------
async def deploy_entry(query, user_data: dict):
    if not await is_global_hosting_enabled():
        await safe_edit(
            query,
            "🔴 <b>HOSTING TEMPORARILY DISABLED</b>\n\n"
            "SODO HOSTING is currently under maintenance.\n"
            "Your hosting requests are temporarily unavailable. Please try again later.",
            InlineKeyboardMarkup([back_home()]),
        )
        return

    pool = await get_pool()
    count = await pool.fetchval(
        "SELECT COUNT(*) FROM projects WHERE user_id = $1", user_data["user_id"]
    )
    max_proj = 5 if user_data.get("is_premium") else 1
    if count >= max_proj:
        await safe_edit(
            query,
            f"⚠️ Project limit reached ({count}/{max_proj}).\nUpgrade to Premium for more slots.",
            InlineKeyboardMarkup([back_home()]),
        )
        return

    await safe_edit(
        query,
        "🚀 <b>Deploy Project</b>\n\n"
        "Free plan: one Python file.\n"
        "Premium: ZIP / multi-file supported (when backend is configured).\n\n"
        "Send your .py file now, or type a project name first.\n"
        "Use /cancel to abort.",
        InlineKeyboardMarkup([[btn("❌ Cancel", "nav:home")]]),
    )


async def projects_menu(query, user_data: dict, action: str):
    pool = await get_pool()
    projects = await pool.fetch(
        "SELECT * FROM projects WHERE user_id = $1 ORDER BY id DESC LIMIT 20",
        user_data["user_id"],
    )
    if not projects:
        await safe_edit(
            query,
            "📂 No projects yet.\nUse 🚀 Deploy Project to create one.",
            InlineKeyboardMarkup([back_home()]),
        )
        return

    text = "📂 <b>Your Projects</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    kb = []
    for p in projects:
        emoji = {"running": "🟢", "stopped": "⚪", "failed": "🔴", "deploying": "🟡"}.get(p["status"], "⚫")
        text += f"{emoji} <b>{p['name']}</b> — {p['status']}\n"
        if action == "menu:projects":
            kb.append([btn(f"{emoji} {p['name']}", f"proj:view:{p['id']}")])
        elif action == "menu:start":
            kb.append([btn(f"▶️ Start {p['name']}", f"proj:start:{p['id']}", "success")])
        elif action == "menu:stop":
            kb.append([btn(f"⏹️ Stop {p['name']}", f"proj:stop:{p['id']}", "danger")])
        elif action == "menu:restart":
            kb.append([btn(f"🔄 Restart {p['name']}", f"proj:restart:{p['id']}")])
        elif action == "menu:logs":
            kb.append([btn(f"📜 Logs {p['name']}", f"proj:logs:{p['id']}")])
        elif action == "menu:status":
            kb.append([btn(f"📊 Status {p['name']}", f"proj:status:{p['id']}")])
    kb.append(back_home())
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


async def project_action(query, context: ContextTypes.DEFAULT_TYPE, user_data: dict, data: str):
    await answer(query)
    parts = data.split(":")
    if len(parts) < 3:
        return
    action, pid_str = parts[1], parts[2]
    try:
        pid = int(pid_str)
    except ValueError:
        return

    pool = await get_pool()
    project = await pool.fetchrow(
        "SELECT * FROM projects WHERE id = $1 AND user_id = $2", pid, user_data["user_id"]
    )
    if not project:
        await answer(query, "Project not found", show_alert=True)
        return
    project = dict(project)

    # Global hosting gate for mutating actions
    if action in ("start", "restart", "deploy") and not await is_global_hosting_enabled():
        await safe_edit(
            query,
            "🔴 <b>HOSTING TEMPORARILY DISABLED</b>\n\n"
            "SODO HOSTING is currently under maintenance.",
            InlineKeyboardMarkup([back_home()]),
        )
        return

    if action == "view":
        text = (
            f"📦 <b>{project['name']}</b>\n"
            f"Status: {project['status']}\n"
            f"Runtime: {project['runtime']}\n"
            f"Created: {project['created_at']}\n"
        )
        if project.get("last_error"):
            text += f"\n⚠️ Last error:\n<code>{project['last_error'][:500]}</code>"
        kb = [
            [
                btn("▶️ Start", f"proj:start:{pid}", "success"),
                btn("⏹️ Stop", f"proj:stop:{pid}", "danger"),
            ],
            [btn("🔄 Restart", f"proj:restart:{pid}"), btn("📜 Logs", f"proj:logs:{pid}")],
            back_home(),
        ]
        await safe_edit(query, text, InlineKeyboardMarkup(kb))
        return

    if action in ("start", "stop", "restart"):
        if action == "start":
            res = await backend.start(project)
            new_status = res.get("status", "running") if res.get("ok") else "failed"
        elif action == "stop":
            res = await backend.stop(project)
            new_status = "stopped" if res.get("ok") else project["status"]
        else:
            res = await backend.restart(project)
            new_status = res.get("status", "running") if res.get("ok") else "failed"

        await pool.execute(
            "UPDATE projects SET status = $2, updated_at = NOW(), last_error = $3 WHERE id = $1",
            pid,
            new_status,
            None if res.get("ok") else res.get("error"),
        )
        await record_deploy_event(
            user_id=user_data["user_id"],
            project_id=pid,
            event_type=action,
            filename=project.get("name"),
            runtime=project.get("runtime"),
            status=new_status,
            details={"ok": bool(res.get("ok"))},
        )
        uname = user_data.get("username")
        uname_str = f"@{uname}" if uname else "n/a"
        emoji = {"start": "▶️", "stop": "⏹️", "restart": "🔄"}.get(action, "📦")
        await notify_owner(
            context.bot,
            (
                f"{emoji} <b>Project {action.upper()}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 User: <code>{user_data['user_id']}</code> {uname_str}\n"
                f"📦 Project: <b>{project['name']}</b> (ID {pid})\n"
                f"🚀 Status: <b>{new_status}</b>\n"
                f"🐍 Runtime: {project.get('runtime')}\n"
            ),
        )
        label = {"start": "Started", "stop": "Stopped", "restart": "Restarted"}[action]
        await answer(query, label if res.get("ok") else f"{label} failed", show_alert=True)
        if action != "restart":
            await projects_menu(query, user_data, "menu:projects")
        return

    if action == "logs":
        logs = await backend.logs(project)
        await safe_edit(
            query,
            f"📜 <b>Logs — {project['name']}</b>\n\n<pre>{logs[:3500]}</pre>",
            InlineKeyboardMarkup([back_home()]),
        )
        return

    if action == "status":
        st = await backend.status(project)
        await safe_edit(
            query,
            f"📊 <b>{project['name']}</b>\nStatus: {st.get('status', project['status'])}",
            InlineKeyboardMarkup([back_home()]),
        )
        return


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle uploaded .py / zip for deploy."""
    user_data = await ensure_user(update)
    if user_data.get("is_banned"):
        return
    if not await is_global_hosting_enabled():
        await update.message.reply_text("🔴 Hosting is temporarily disabled.")
        return

    doc = update.message.document
    if not doc:
        return
    file_name = doc.file_name or "project.py"
    if not (file_name.endswith(".py") or file_name.endswith(".zip")):
        await update.message.reply_text("Please send a .py or .zip file.")
        return

    if file_name.endswith(".zip") and not user_data.get("is_premium"):
        await update.message.reply_text("ZIP uploads require Premium.")
        return

    pool = await get_pool()
    count = await pool.fetchval(
        "SELECT COUNT(*) FROM projects WHERE user_id = $1", user_data["user_id"]
    )
    max_proj = 5 if user_data.get("is_premium") else 1
    if count >= max_proj:
        await update.message.reply_text("Project limit reached.")
        return

    name = file_name.rsplit(".", 1)[0][:48]
    base = name
    i = 1
    while await pool.fetchval(
        "SELECT 1 FROM projects WHERE user_id = $1 AND name = $2", user_data["user_id"], name
    ):
        name = f"{base}_{i}"
        i += 1

    runtime = "python"
    if file_name.endswith(".zip"):
        runtime = "python/zip"
    file_size = doc.file_size or 0
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Download file bytes from Telegram
    try:
        tg_file = await context.bot.get_file(doc.file_id)
        source_bytes = bytes(await tg_file.download_as_bytearray())
    except Exception as e:
        await update.message.reply_text(f"❌ Could not download file: {e}")
        return

    # Size guard (Telegram allows up to ~20MB for bots typically; keep reasonable)
    if len(source_bytes) > 15 * 1024 * 1024:
        await update.message.reply_text("❌ File too large (max 15 MB).")
        return

    row = await pool.fetchrow(
        """
        INSERT INTO projects (user_id, name, runtime, status, source_type, source_ref)
        VALUES ($1, $2, $3, 'deploying', $4, $5)
        RETURNING *
        """,
        user_data["user_id"],
        name,
        runtime,
        "zip" if file_name.endswith(".zip") else "single",
        doc.file_id,
    )
    project = dict(row)
    res = await backend.deploy(project, source_bytes=source_bytes, filename=file_name)
    status = "stopped" if res.get("ok") else "failed"
    await pool.execute(
        "UPDATE projects SET status = $2, external_id = $3, last_error = $4, updated_at = NOW() WHERE id = $1",
        project["id"],
        status,
        res.get("external_id"),
        None if res.get("ok") else res.get("error"),
    )

    await record_deploy_event(
        user_id=user_data["user_id"],
        project_id=project["id"],
        event_type="upload",
        filename=file_name,
        file_size=file_size,
        runtime=runtime,
        status=status,
        details={"name": name, "ok": bool(res.get("ok"))},
    )

    uname = user_data.get("username")
    uname_str = f"@{uname}" if uname else "n/a"
    size_kb = round(file_size / 1024, 1) if file_size else 0
    owner_msg = (
        f"📂 <b>New Project Upload</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 User ID: <code>{user_data['user_id']}</code>\n"
        f"🏷️ Username: {uname_str}\n"
        f"📂 File: <code>{file_name}</code>\n"
        f"📦 Size: {size_kb} KB\n"
        f"🐍 Runtime: {runtime}\n"
        f"🆔 Project ID: <code>{project['id']}</code>\n"
        f"🚀 Status: <b>{status}</b>\n"
        f"🕒 {ts}\n"
    )
    if not res.get("ok"):
        owner_msg += f"\n⚠️ Error: {res.get('error', 'unknown')[:200]}"

    await notify_owner(
        context.bot,
        owner_msg,
        document_file_id=doc.file_id if doc.file_id else None,
    )

    terms = (
        "\n\nℹ️ <i>By uploading you agree that the hosting operator may review "
        "project files for security and operational purposes.</i>"
    )
    if res.get("ok"):
        await update.message.reply_text(
            f"✅ Project <b>{name}</b> created (status: stopped).\n"
            f"Use ▶️ Start to run it.{terms}",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(is_owner(user_data["user_id"])),
        )
    else:
        await update.message.reply_text(
            f"❌ Deploy failed: {res.get('error', 'unknown')}{terms}",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(is_owner(user_data["user_id"])),
        )


# ---------------------------------------------------------------------------
# Owner Panel
# ---------------------------------------------------------------------------
async def owner_router(query, context: ContextTypes.DEFAULT_TYPE, data: str):
    await answer(query)
    if data == "owner:panel":
        await safe_edit(
            query,
            "🛡️ <b>SODO Owner Panel</b>\n━━━━━━━━━━━━━━━━━━━━\nOnly you can access these controls.",
            owner_panel_keyboard(),
        )
        return

    if data == "owner:dashboard":
        await owner_dashboard(query)
        return
    if data == "owner:global_hosting":
        await owner_global_hosting(query)
        return
    if data == "owner:global_disable":
        await owner_global_disable_confirm(query)
        return
    if data == "owner:global_disable_yes":
        await owner_global_disable_execute(query, context)
        return
    if data == "owner:global_enable":
        await owner_global_enable(query, context)
        return
    if data == "owner:forcejoin":
        await owner_forcejoin_menu(query)
        return
    if data == "owner:fj_list":
        await owner_forcejoin_list(query)
        return
    if data == "owner:fj_add":
        await safe_edit(
            query,
            "➕ Send channel username (without @) or numeric chat ID.\n"
            "For private channels also send invite link on next line.\n"
            "Format:\n<code>@channelname</code>\nor\n<code>-100123\nhttps://t.me/+xxxx</code>",
            InlineKeyboardMarkup([[btn("❌ Cancel", "owner:forcejoin")]]),
        )
        context.user_data["awaiting"] = "forcejoin_add"
        return
    if data.startswith("owner:fj_del:"):
        fid = int(data.split(":")[2])
        pool = await get_pool()
        await pool.execute("UPDATE force_join_channels SET is_active = FALSE WHERE id = $1", fid)
        await answer(query, "Removed", show_alert=True)
        await owner_forcejoin_list(query)
        return
    if data == "owner:ad":
        await owner_ad_menu(query)
        return
    if data == "owner:ad_toggle":
        cur = await get_setting("owner_ad_enabled", "true")
        new = "false" if cur.lower() in ("true", "1") else "true"
        await set_setting("owner_ad_enabled", new)
        await answer(query, f"Ad {'enabled' if new == 'true' else 'disabled'}")
        await owner_ad_menu(query)
        return
    if data == "owner:ad_edit":
        await safe_edit(
            query,
            "📣 Send the new advertisement text (or /cancel).",
            InlineKeyboardMarkup([[btn("❌ Cancel", "owner:ad")]]),
        )
        context.user_data["awaiting"] = "owner_ad_text"
        return
    if data == "owner:users":
        await safe_edit(
            query,
            "👥 Send the Telegram user ID to look up.",
            InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]),
        )
        context.user_data["awaiting"] = "owner_search_user"
        return
    if data == "owner:broadcast":
        await safe_edit(
            query,
            "📢 Send the broadcast message (HTML allowed). It will be sent to all non-banned users.",
            InlineKeyboardMarkup([[btn("❌ Cancel", "owner:panel")]]),
        )
        context.user_data["awaiting"] = "owner_broadcast"
        return
    if data == "owner:credits":
        await safe_edit(
            query,
            "🪙 Credits Management\nSend: <code>user_id amount reason</code>\nExample: <code>123456 50 bonus</code>",
            InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]),
        )
        context.user_data["awaiting"] = "owner_credits"
        return
    if data == "owner:logs":
        pool = await get_pool()
        rows = await pool.fetch(
            "SELECT * FROM activity_logs ORDER BY id DESC LIMIT 15"
        )
        text = "🛡️ <b>Recent Activity</b>\n\n"
        for r in rows:
            text += f"• {r['created_at']:%H:%M} | {r['action']} | actor={r['actor_id']}\n"
        await safe_edit(query, text[:4000], InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]))
        return
    if data == "owner:settings":
        hosting = await is_global_hosting_enabled()
        text = (
            f"⚙️ <b>Bot Settings</b>\n"
            f"Global Hosting: {'🟢 ON' if hosting else '🔴 OFF'}\n"
            f"Owner ID: <code>{OWNER_ID}</code>\n"
            f"Owner Username: @{OWNER_USERNAME or 'not set'}\n"
            f"Hosting API: {'configured' if HOSTING_API_URL else 'mock/local'}\n"
        )
        await safe_edit(query, text, InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]))
        return
    if data == "owner:guide":
        await owner_guide(query)
        return
    if data == "owner:filemon":
        await owner_filemon_menu(query)
        return
    if data == "owner:fm_toggle_notify":
        cur = await get_setting("upload_notify_enabled", "true")
        new = "false" if cur.lower() in ("true", "1", "yes") else "true"
        await set_setting("upload_notify_enabled", new)
        await answer(query, f"Notifications {'ON' if new == 'true' else 'OFF'}")
        await owner_filemon_menu(query)
        return
    if data == "owner:fm_toggle_copy":
        cur = await get_setting("upload_copy_to_owner", "false")
        new = "false" if cur.lower() in ("true", "1", "yes") else "true"
        await set_setting("upload_copy_to_owner", new)
        await answer(query, f"File copy {'ON' if new == 'true' else 'OFF'}")
        await owner_filemon_menu(query)
        return
    if data == "owner:fm_recent":
        await owner_fm_recent(query)
        return
    if data == "owner:fm_activity":
        await owner_fm_activity(query)
        return
    if data == "owner:fm_search":
        await safe_edit(
            query,
            "👤 Send user Telegram ID to view their deployment activity.",
            InlineKeyboardMarkup([[btn("🔙 Back", "owner:filemon")]]),
        )
        context.user_data["awaiting"] = "owner_fm_search"
        return
    if data == "owner:fm_copy":
        await safe_edit(
            query,
            "📂 Send project ID to request a copy notification log.\n"
            "(Actual file is sent automatically on upload when File Copy is enabled.)",
            InlineKeyboardMarkup([[btn("🔙 Back", "owner:filemon")]]),
        )
        context.user_data["awaiting"] = "owner_fm_copy"
        return
    if data in ("owner:hosting", "owner:premium", "owner:redeem"):
        await safe_edit(
            query,
            f"Section <b>{data.split(':')[1]}</b> — use Dashboard / User search / Credits for most actions.\n"
            "Redeem codes: send <code>CREATE CODE 100 10</code> (code, credits, max_uses).",
            InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]),
        )
        context.user_data["awaiting"] = "owner_redeem_create" if data == "owner:redeem" else None
        return


async def owner_filemon_menu(query):
    notify_on = (await get_setting("upload_notify_enabled", "true")).lower() in ("true", "1", "yes")
    copy_on = (await get_setting("upload_copy_to_owner", "false")).lower() in ("true", "1", "yes")
    text = (
        "📂 <b>File Monitoring</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🔔 Upload notifications: <b>{'ON' if notify_on else 'OFF'}</b>\n"
        f"📁 Auto file copy to Owner: <b>{'ON' if copy_on else 'OFF'}</b>\n\n"
        "When enabled, you receive Telegram alerts on upload / start / stop / restart.\n"
        "File copies respect Telegram size limits."
    )
    kb = [
        [btn(
            f"🔔 Notifications: {'ON ✅' if notify_on else 'OFF'}",
            "owner:fm_toggle_notify",
            "success" if notify_on else None,
        )],
        [btn(
            f"📁 File Copy: {'ON ✅' if copy_on else 'OFF'}",
            "owner:fm_toggle_copy",
            "success" if copy_on else None,
        )],
        [btn("📋 Recent Uploads", "owner:fm_recent")],
        [btn("📊 Deployment Activity", "owner:fm_activity")],
        [btn("👤 Search User Activity", "owner:fm_search")],
        [btn("📂 Request Project Copy", "owner:fm_copy")],
        [btn("🔙 Back", "owner:panel")],
    ]
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


async def owner_fm_recent(query):
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT * FROM deployment_events
        WHERE event_type = 'upload'
        ORDER BY id DESC LIMIT 15
        """
    )
    text = "📋 <b>Recent Uploads</b>\n\n"
    if not rows:
        text += "No uploads yet."
    for r in rows:
        text += (
            f"• #{r['id']} user=<code>{r['user_id']}</code> "
            f"proj={r['project_id']} {r['filename'] or '-'} "
            f"{r['status']} ({r['created_at']:%m-%d %H:%M})\n"
        )
    await safe_edit(query, text[:4000], InlineKeyboardMarkup([[btn("🔙 Back", "owner:filemon")]]))


async def owner_fm_activity(query):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM deployment_events ORDER BY id DESC LIMIT 20"
    )
    text = "📊 <b>Deployment Activity</b>\n\n"
    if not rows:
        text += "No activity yet."
    for r in rows:
        text += (
            f"• {r['event_type']} | user=<code>{r['user_id']}</code> "
            f"| p={r['project_id']} | {r['status']} | {r['created_at']:%m-%d %H:%M}\n"
        )
    await safe_edit(query, text[:4000], InlineKeyboardMarkup([[btn("🔙 Back", "owner:filemon")]]))


async def owner_guide(query):
    text = (
        "📖 <b>SODO HOSTING — Owner Guide</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>🔹 Bot kaise kaam karta hai</b>\n"
        "• Users /start se bot open karte hain\n"
        "• Force Join channels join karne padte hain (agar set ho)\n"
        "• Main menu se Deploy / Start / Stop / Logs\n"
        "• Credits se daily reward + referral\n"
        "• Premium plans manual activate hote hain\n"
        "• Global Hosting OFF = sab users block\n\n"
        "<b>🔹 Owner Panel buttons</b>\n"
        "📊 Dashboard — users, projects, stats\n"
        "👥 User Management — ID se user dekho\n"
        "🟢 Global Hosting — sab hosting ON/OFF\n"
        "🔐 Force Join — channels add/remove\n"
        "🪙 Credits — user ko credits do\n"
        "💎 Premium — plans / grant\n"
        "🎟️ Redeem Codes — codes banao\n"
        "📢 Broadcast — sab users ko message\n"
        "📣 Owner Ad — contact page text\n"
        "📂 File Monitoring — upload alerts + file copy\n"
        "⚙️ Settings — config dekho\n"
        "🛡️ Logs — admin actions\n\n"
        "<b>🔹 Text commands (Owner chat mein)</b>\n"
        "<code>ban 123456789</code>\n"
        "<code>unban 123456789</code>\n"
        "<code>premium 123456789 30</code>\n"
        "<code>credits 123456789 50 bonus</code>\n"
        "<code>CREATE CODE100 50 10</code>\n"
        "→ code, credits, max_uses\n\n"
        "<b>🔹 Force Join add format</b>\n"
        "<code>channelusername</code>\n"
        "ya\n"
        "<code>-1001234567890\nhttps://t.me/+invite</code>\n\n"
        "<b>🔹 Users ke liye</b>\n"
        "/start — bot open\n"
        "Main menu buttons — sab features\n"
        "Referral link — My Account / Earn Credits\n"
    )
    await safe_edit(
        query,
        text,
        InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]),
    )


async def owner_dashboard(query):
    pool = await get_pool()
    stats = await pool.fetchrow(
        """
        SELECT
            (SELECT COUNT(*) FROM users) AS total_users,
            (SELECT COUNT(*) FROM users WHERE is_banned = FALSE) AS active_users,
            (SELECT COUNT(*) FROM users WHERE is_banned = TRUE) AS banned_users,
            (SELECT COUNT(*) FROM projects) AS total_projects,
            (SELECT COUNT(*) FROM projects WHERE status = 'running') AS running_projects,
            (SELECT COUNT(*) FROM projects WHERE status = 'failed') AS failed_projects,
            (SELECT COUNT(*) FROM users WHERE is_premium = TRUE) AS premium_users,
            (SELECT COALESCE(SUM(amount),0) FROM credit_transactions WHERE amount > 0) AS credits_distributed
        """
    )
    hosting = await is_global_hosting_enabled()
    text = (
        f"📊 <b>Dashboard</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Users: {stats['total_users']} (active {stats['active_users']}, banned {stats['banned_users']})\n"
        f"🚀 Projects: {stats['total_projects']} (running {stats['running_projects']}, failed {stats['failed_projects']})\n"
        f"💎 Premium users: {stats['premium_users']}\n"
        f"🪙 Credits distributed: {stats['credits_distributed']}\n"
        f"🌐 Global hosting: {'🟢 ON' if hosting else '🔴 OFF'}\n"
        f"🗄️ Database: connected\n"
        f"⚙️ Backend: {'API' if HOSTING_API_URL else 'local/mock'}\n"
    )
    await safe_edit(query, text, InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]))


async def owner_global_hosting(query):
    pool = await get_pool()
    running = await pool.fetchval("SELECT COUNT(*) FROM projects WHERE status = 'running'")
    users_affected = await pool.fetchval(
        "SELECT COUNT(DISTINCT user_id) FROM projects WHERE status = 'running'"
    )
    enabled = await is_global_hosting_enabled()
    text = (
        f"🟢 <b>Global Hosting Control</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"Status: {'🟢 ENABLED' if enabled else '🔴 DISABLED'}\n"
        f"Running projects: <b>{running}</b>\n"
        f"Users affected: <b>{users_affected}</b>\n\n"
        f"Disabling will stop all running projects and block new starts/deploys."
    )
    kb = []
    if enabled:
        kb.append([btn("🔴 Disable Hosting for Everyone", "owner:global_disable", "danger")])
    else:
        kb.append([btn("🟢 Enable Hosting for Everyone", "owner:global_enable", "success")])
    kb.append([btn("🔙 Back", "owner:panel")])
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


async def owner_global_disable_confirm(query):
    text = (
        "⚠️ <b>Confirm mass stop</b>\n\n"
        "This will:\n"
        "• Set global_hosting_enabled = false\n"
        "• Attempt to stop every running project\n"
        "• Block new deployments / starts / restarts\n\n"
        "Projects and credits are preserved."
    )
    kb = [
        [btn("✅ Yes, Disable & Stop All", "owner:global_disable_yes", "danger")],
        [btn("❌ Cancel", "owner:global_hosting")],
    ]
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


async def owner_global_disable_execute(query, context: ContextTypes.DEFAULT_TYPE):
    await set_setting("global_hosting_enabled", "false")
    pool = await get_pool()
    running = await pool.fetch("SELECT * FROM projects WHERE status = 'running'")
    ok, fail = 0, 0
    for row in running:
        project = dict(row)
        try:
            res = await backend.stop(project)
            if res.get("ok"):
                await pool.execute(
                    "UPDATE projects SET status = 'stopped', updated_at = NOW() WHERE id = $1",
                    project["id"],
                )
                ok += 1
            else:
                fail += 1
        except Exception as e:
            logger.exception("Stop failed for %s", project["id"])
            fail += 1
    await log_activity(OWNER_ID, "global_hosting_disabled", {"stopped_ok": ok, "stopped_fail": fail})
    await safe_edit(
        query,
        f"🔴 <b>Hosting Disabled</b>\n\nStopped: {ok}\nFailed: {fail}\n\nUsers will see a maintenance message.",
        InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]),
    )
    # Optional: notify running project owners (rate-limited)
    # Skipped by default to avoid spam; Owner can broadcast manually.


async def owner_global_enable(query, context: ContextTypes.DEFAULT_TYPE):
    await set_setting("global_hosting_enabled", "true")
    await log_activity(OWNER_ID, "global_hosting_enabled", {})
    await safe_edit(
        query,
        "🟢 <b>Hosting Enabled</b>\n\nNew deployments and start/restart are allowed again.\n"
        "Previously stopped projects are NOT auto-restarted.",
        InlineKeyboardMarkup([[btn("🔙 Back", "owner:panel")]]),
    )


async def owner_forcejoin_menu(query):
    kb = [
        [btn("➕ Add Channel or Group", "owner:fj_add")],
        [btn("📋 View Required Channels", "owner:fj_list")],
        [btn("🔙 Back", "owner:panel")],
    ]
    await safe_edit(
        query,
        "🔐 <b>Force Join Settings</b>\n\nConfigure channels/groups users must join.",
        InlineKeyboardMarkup(kb),
    )


async def owner_forcejoin_list(query):
    channels = await get_active_force_channels()
    text = "📋 <b>Required Channels</b>\n\n"
    kb = []
    if not channels:
        text += "None configured."
    for ch in channels:
        label = ch.get("title") or ch.get("username") or str(ch.get("chat_id"))
        text += f"• {label}\n"
        kb.append([btn(f"➖ Remove {label[:20]}", f"owner:fj_del:{ch['id']}", "danger")])
    kb.append([btn("🔙 Back", "owner:forcejoin")])
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


async def owner_ad_menu(query):
    enabled = (await get_setting("owner_ad_enabled", "true")).lower() in ("true", "1")
    ad = await get_setting("owner_ad_text", OWNER_CONTACT_TEXT)
    text = f"📣 <b>Owner Advertisement</b>\nEnabled: {'Yes' if enabled else 'No'}\n\nPreview:\n{ad[:800]}"
    kb = [
        [btn("🔄 Toggle On/Off", "owner:ad_toggle")],
        [btn("✏️ Edit Text", "owner:ad_edit")],
        [btn("🔙 Back", "owner:panel")],
    ]
    await safe_edit(query, text, InlineKeyboardMarkup(kb))


# ---------------------------------------------------------------------------
# Text message router (owner inputs, redeem, etc.)
# ---------------------------------------------------------------------------
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    user = update.effective_user
    if not user:
        return
    text = update.message.text.strip()
    awaiting = context.user_data.get("awaiting")

    if text.lower() in ("/cancel", "cancel"):
        context.user_data.pop("awaiting", None)
        await update.message.reply_text("Cancelled.", reply_markup=main_menu_keyboard(is_owner(user.id)))
        return

    # Redeem code (any user)
    if awaiting == "cred_redeem" or (not awaiting and len(text) >= 4 and text.isalnum()):
        # Try redeem if looks like a code
        pool = await get_pool()
        code = text.upper()
        row = await pool.fetchrow(
            "SELECT * FROM redeem_codes WHERE code = $1 AND is_active = TRUE", code
        )
        if row and row["used_count"] < row["max_uses"]:
            already = await pool.fetchval(
                "SELECT 1 FROM redeem_redemptions WHERE code = $1 AND user_id = $2",
                code,
                user.id,
            )
            if already:
                await update.message.reply_text("You already redeemed this code.")
                return
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await _add_credits(conn, user.id, row["credits"], f"redeem:{code}", None)
                    await conn.execute(
                        "UPDATE redeem_codes SET used_count = used_count + 1 WHERE code = $1",
                        code,
                    )
                    await conn.execute(
                        "INSERT INTO redeem_redemptions (code, user_id) VALUES ($1, $2)",
                        code,
                        user.id,
                    )
            context.user_data.pop("awaiting", None)
            await update.message.reply_text(f"✅ Redeemed +{row['credits']} credits!")
            return

    if not is_owner(user.id):
        return

    if awaiting == "forcejoin_add":
        lines = text.splitlines()
        username = None
        chat_id = None
        invite = None
        title = None
        first = lines[0].strip().lstrip("@")
        if first.lstrip("-").isdigit():
            chat_id = int(first)
        else:
            username = first
        if len(lines) > 1 and lines[1].strip().startswith("http"):
            invite = lines[1].strip()
        # Try resolve
        try:
            if username:
                chat = await context.bot.get_chat(f"@{username}")
                chat_id = chat.id
                title = chat.title or username
            elif chat_id:
                chat = await context.bot.get_chat(chat_id)
                title = chat.title or str(chat_id)
                username = getattr(chat, "username", None)
        except TelegramError as e:
            await update.message.reply_text(f"Could not resolve chat: {e}")
            return
        pool = await get_pool()
        await pool.execute(
            """
            INSERT INTO force_join_channels (chat_id, username, title, invite_link, is_active)
            VALUES ($1, $2, $3, $4, TRUE)
            """,
            chat_id,
            username,
            title,
            invite,
        )
        context.user_data.pop("awaiting", None)
        await update.message.reply_text(f"✅ Added force-join: {title or username or chat_id}")
        return

    if awaiting == "owner_ad_text":
        await set_setting("owner_ad_text", text)
        await set_setting("owner_contact_text", text)
        context.user_data.pop("awaiting", None)
        await update.message.reply_text("✅ Advertisement updated.")
        return

    if awaiting == "owner_search_user":
        try:
            uid = int(text.strip())
        except ValueError:
            await update.message.reply_text("Invalid user ID")
            return
        u = await get_user(uid)
        if not u:
            await update.message.reply_text("User not found")
            return
        pool = await get_pool()
        proj_count = await pool.fetchval("SELECT COUNT(*) FROM projects WHERE user_id = $1", uid)
        text_out = (
            f"👤 <b>User {uid}</b>\n"
            f"Name: {u.get('first_name')}\n"
            f"Username: @{u.get('username')}\n"
            f"Credits: {u.get('credits')}\n"
            f"Premium: {u.get('is_premium')}\n"
            f"Banned: {u.get('is_banned')}\n"
            f"Projects: {proj_count}\n\n"
            f"Commands:\n"
            f"<code>ban {uid}</code> / <code>unban {uid}</code>\n"
            f"<code>premium {uid} 30</code> (days)\n"
            f"<code>credits {uid} 50 bonus</code>\n"
        )
        context.user_data.pop("awaiting", None)
        await update.message.reply_text(text_out, parse_mode=ParseMode.HTML)
        return

    if awaiting == "owner_fm_search":
        try:
            uid = int(text.strip())
        except ValueError:
            await update.message.reply_text("Invalid user ID")
            return
        pool = await get_pool()
        rows = await pool.fetch(
            "SELECT * FROM deployment_events WHERE user_id = $1 ORDER BY id DESC LIMIT 20",
            uid,
        )
        out = f"👤 <b>Activity for {uid}</b>\n\n"
        if not rows:
            out += "No events."
        for r in rows:
            out += f"• {r['event_type']} p={r['project_id']} {r['filename'] or ''} {r['status']} ({r['created_at']:%m-%d %H:%M})\n"
        context.user_data.pop("awaiting", None)
        await log_activity(user.id, "fm_search_user", {"target": uid})
        await update.message.reply_text(out[:4000], parse_mode=ParseMode.HTML)
        return

    if awaiting == "owner_fm_copy":
        try:
            pid = int(text.strip())
        except ValueError:
            await update.message.reply_text("Invalid project ID")
            return
        pool = await get_pool()
        proj = await pool.fetchrow("SELECT * FROM projects WHERE id = $1", pid)
        if not proj:
            await update.message.reply_text("Project not found")
            return
        context.user_data.pop("awaiting", None)
        await log_activity(user.id, "fm_request_copy", {"project_id": pid})
        await update.message.reply_text(
            f"📂 Project <b>{proj['name']}</b> (ID {pid})\n"
            f"User: <code>{proj['user_id']}</code>\n"
            f"Status: {proj['status']}\n"
            f"Runtime: {proj['runtime']}\n\n"
            f"ℹ️ File binary is delivered automatically on upload when "
            f"<b>File Copy</b> is enabled in File Monitoring.\n"
            f"Enable it and ask the user to re-upload if you need the source.",
            parse_mode=ParseMode.HTML,
        )
        return

    if awaiting == "owner_credits" or text.lower().startswith("credits "):
        parts = text.split()
        if len(parts) >= 3:
            try:
                uid = int(parts[1] if parts[0].lower() == "credits" else parts[0])
                amount = int(parts[2] if parts[0].lower() == "credits" else parts[1])
                reason = " ".join(parts[3:] if parts[0].lower() == "credits" else parts[2:]) or "admin"
                new_bal = await add_credits(uid, amount, reason, admin_id=user.id)
                await log_activity(user.id, "admin_credits", {"target": uid, "amount": amount})
                await update.message.reply_text(f"✅ User {uid} new balance: {new_bal}")
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")
        context.user_data.pop("awaiting", None)
        return

    if text.lower().startswith("ban "):
        uid = int(text.split()[1])
        pool = await get_pool()
        await pool.execute("UPDATE users SET is_banned = TRUE WHERE user_id = $1", uid)
        await log_activity(user.id, "ban_user", {"target": uid})
        await update.message.reply_text(f"Banned {uid}")
        return
    if text.lower().startswith("unban "):
        uid = int(text.split()[1])
        pool = await get_pool()
        await pool.execute("UPDATE users SET is_banned = FALSE WHERE user_id = $1", uid)
        await log_activity(user.id, "unban_user", {"target": uid})
        await update.message.reply_text(f"Unbanned {uid}")
        return
    if text.lower().startswith("premium "):
        parts = text.split()
        uid = int(parts[1])
        days = int(parts[2]) if len(parts) > 2 else 30
        until = datetime.now(timezone.utc) + timedelta(days=days)
        pool = await get_pool()
        await pool.execute(
            "UPDATE users SET is_premium = TRUE, premium_until = $2 WHERE user_id = $1",
            uid,
            until,
        )
        await log_activity(user.id, "grant_premium", {"target": uid, "days": days})
        await update.message.reply_text(f"Premium granted to {uid} for {days} days")
        return
    if text.upper().startswith("CREATE "):
        # CREATE CODECREDITS MAXUSES
        parts = text.split()
        if len(parts) >= 3:
            code = parts[1].upper()
            credits = int(parts[2])
            max_uses = int(parts[3]) if len(parts) > 3 else 1
            pool = await get_pool()
            await pool.execute(
                """
                INSERT INTO redeem_codes (code, credits, max_uses, created_by)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (code) DO NOTHING
                """,
                code,
                credits,
                max_uses,
                user.id,
            )
            await update.message.reply_text(f"Redeem code {code} created (+{credits}, max {max_uses})")
        return

    if awaiting == "owner_broadcast":
        pool = await get_pool()
        users = await pool.fetch("SELECT user_id FROM users WHERE is_banned = FALSE")
        ok, fail = 0, 0
        for u in users:
            try:
                await context.bot.send_message(u["user_id"], text, parse_mode=ParseMode.HTML)
                ok += 1
                await asyncio.sleep(0.05)
            except Exception:
                fail += 1
        context.user_data.pop("awaiting", None)
        await update.message.reply_text(f"Broadcast done. OK={ok} Fail={fail}")
        await log_activity(user.id, "broadcast", {"ok": ok, "fail": fail})
        return


# ---------------------------------------------------------------------------
# Error handler
# ---------------------------------------------------------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling update: %s", context.error, exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ An error occurred. Please try again later.")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
async def post_init(application: Application) -> None:
    await init_db()
    logger.info("SODO HOSTING ready | Owner ID=%s | Hosting API=%s", OWNER_ID, bool(HOSTING_API_URL))


def main() -> None:
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is required")
        sys.exit(1)
    if not DATABASE_URL:
        logger.error("DATABASE_URL is required")
        sys.exit(1)
    if not OWNER_ID:
        logger.warning("OWNER_ID is not set — Owner Panel will be inaccessible")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(menu_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    app.add_error_handler(error_handler)

    if WEBHOOK_URL:
        logger.info("Starting webhook on port %s", PORT)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
        )
    else:
        logger.info("Starting long polling")
        app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
