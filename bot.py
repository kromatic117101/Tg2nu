# ============================================================
#  SODO HOSTING — Single-file Telegram Bot
#  python sodo_hosting_single.py
# ============================================================
from __future__ import annotations

import asyncio
import enum
import io
import json
import logging
import math
import os
import re
import secrets
import shutil
import signal
import string
import sys
import uuid
import zipfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncGenerator

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger, Boolean, DateTime, Enum as SAEnum, ForeignKey, Integer,
    String, Text, UniqueConstraint, func, select,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.pool import NullPool
from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update,
)
from telegram.error import TelegramError
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters,
)

load_dotenv()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("sodo")


# ============================================================
#  CONFIG
# ============================================================
@dataclass(frozen=True)
class Config:
    bot_token: str
    owner_id: int
    database_url: str
    hosting_api_url: str | None
    hosting_api_token: str | None
    welcome_credits: int
    daily_reward_credits: int
    referral_reward_credits: int
    free_max_projects: int
    free_max_file_mb: int
    max_log_lines: int
    storage_path: str

    @classmethod
    def from_env(cls) -> "Config":
        tok = os.environ.get("BOT_TOKEN", "")
        if not tok:
            raise ValueError("BOT_TOKEN required")
        oid = os.environ.get("OWNER_ID", "")
        if not oid:
            raise ValueError("OWNER_ID required")
        db = os.environ.get("DATABASE_URL", "")
        if not db:
            raise ValueError("DATABASE_URL required")
        return cls(
            bot_token=tok,
            owner_id=int(oid),
            database_url=db,
            hosting_api_url=os.environ.get("HOSTING_API_URL"),
            hosting_api_token=os.environ.get("HOSTING_API_TOKEN"),
            welcome_credits=int(os.environ.get("WELCOME_CREDITS", "50")),
            daily_reward_credits=int(os.environ.get("DAILY_REWARD_CREDITS", "10")),
            referral_reward_credits=int(os.environ.get("REFERRAL_REWARD_CREDITS", "25")),
            free_max_projects=int(os.environ.get("FREE_PLAN_MAX_PROJECTS", "1")),
            free_max_file_mb=int(os.environ.get("FREE_PLAN_MAX_FILE_SIZE_MB", "1")),
            max_log_lines=int(os.environ.get("MAX_LOG_LINES", "100")),
            storage_path=os.environ.get("STORAGE_BASE_PATH", "/tmp/sodo_projects"),
        )


# ============================================================
#  DATABASE
# ============================================================
_engine = None
_sm = None


def _async_url(url: str) -> str:
    return re.sub(r"^postgres(ql)?://", "postgresql+asyncpg://", url)


def init_db(url: str) -> None:
    global _engine, _sm
    _engine = create_async_engine(_async_url(url), echo=False, poolclass=NullPool)
    _sm = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)


async def create_tables() -> None:
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if _sm is None:
        raise RuntimeError("Call init_db() first")
    async with _sm() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ============================================================
#  MODELS
# ============================================================
class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str] = mapped_column(String(256))
    last_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    ban_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    banned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    referral_code_used: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_active_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    wallet: Mapped["CreditWallet"] = relationship("CreditWallet", back_populates="user", uselist=False, cascade="all, delete-orphan")
    projects: Mapped[list["Project"]] = relationship("Project", back_populates="owner", cascade="all, delete-orphan")
    referral_link: Mapped["ReferralLink | None"] = relationship("ReferralLink", back_populates="user", uselist=False, cascade="all, delete-orphan")


class ProjectStatus(str, enum.Enum):
    pending = "pending"; running = "running"; stopped = "stopped"
    failed = "failed"; suspended = "suspended"


class Runtime(str, enum.Enum):
    python = "python"; nodejs = "nodejs"


class DeploymentStatus(str, enum.Enum):
    queued = "queued"; building = "building"; running = "running"
    success = "success"; failed = "failed"; cancelled = "cancelled"


class Project(Base):
    __tablename__ = "projects"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(128))
    runtime: Mapped[Runtime] = mapped_column(SAEnum(Runtime, name="runtime_enum"), default=Runtime.python)
    status: Mapped[ProjectStatus] = mapped_column(SAEnum(ProjectStatus, name="pstatus_enum"), default=ProjectStatus.pending)
    entry_file: Mapped[str | None] = mapped_column(String(256), nullable=True)
    storage_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    github_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_suspended: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    process_pid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    env_vars: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    owner: Mapped["User"] = relationship("User", back_populates="projects")
    deployments: Mapped[list["Deployment"]] = relationship("Deployment", back_populates="project", cascade="all, delete-orphan", order_by="Deployment.created_at.desc()")
    log_entries: Mapped[list["LogEntry"]] = relationship("LogEntry", back_populates="project", cascade="all, delete-orphan")


class Deployment(Base):
    __tablename__ = "deployments"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"))
    triggered_by: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[DeploymentStatus] = mapped_column(SAEnum(DeploymentStatus, name="dstatus_enum"), default=DeploymentStatus.queued)
    log_output: Mapped[str] = mapped_column(Text, default="", server_default="")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    project: Mapped["Project"] = relationship("Project", back_populates="deployments")


class LogEntry(Base):
    __tablename__ = "log_entries"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"))
    deployment_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    content: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    project: Mapped["Project"] = relationship("Project", back_populates="log_entries")


class RequiredChannel(Base):
    __tablename__ = "required_channels"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    display_name: Mapped[str] = mapped_column(String(256))
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    invite_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class TransactionType(str, enum.Enum):
    welcome = "welcome"; daily = "daily"; referral = "referral"
    redeem = "redeem"; admin_add = "admin_add"; admin_deduct = "admin_deduct"
    purchase = "purchase"; refund = "refund"


class CreditWallet(Base):
    __tablename__ = "credit_wallets"
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    balance: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    user: Mapped["User"] = relationship("User", back_populates="wallet")
    transactions: Mapped[list["CreditTransaction"]] = relationship("CreditTransaction", back_populates="wallet", cascade="all, delete-orphan", order_by="CreditTransaction.created_at.desc()")


class CreditTransaction(Base):
    __tablename__ = "credit_transactions"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("credit_wallets.user_id", ondelete="CASCADE"))
    amount: Mapped[int] = mapped_column(BigInteger)
    type: Mapped[TransactionType] = mapped_column(SAEnum(TransactionType, name="txtype_enum"))
    description: Mapped[str] = mapped_column(Text)
    actor_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    wallet: Mapped["CreditWallet"] = relationship("CreditWallet", back_populates="transactions")


class ReferralLink(Base):
    __tablename__ = "referral_links"
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    user: Mapped["User"] = relationship("User", back_populates="referral_link")


class ReferralRecord(Base):
    __tablename__ = "referral_records"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    referrer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    referred_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), unique=True)
    rewarded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Plan(Base):
    __tablename__ = "plans"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)
    display_name: Mapped[str] = mapped_column(String(128))
    max_projects: Mapped[int] = mapped_column(Integer, default=5)
    max_file_size_mb: Mapped[int] = mapped_column(Integer, default=10)
    max_storage_mb: Mapped[int] = mapped_column(Integer, default=100)
    allows_zip: Mapped[bool] = mapped_column(Boolean, default=True)
    allows_github: Mapped[bool] = mapped_column(Boolean, default=True)
    allows_nodejs: Mapped[bool] = mapped_column(Boolean, default=True)
    allows_multi_file: Mapped[bool] = mapped_column(Boolean, default=True)
    allows_env_vars: Mapped[bool] = mapped_column(Boolean, default=True)
    duration_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price_credits: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    subscriptions: Mapped[list["Subscription"]] = relationship("Subscription", back_populates="plan")


class Subscription(Base):
    __tablename__ = "subscriptions"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    plan_id: Mapped[int] = mapped_column(Integer, ForeignKey("plans.id"))
    granted_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    plan: Mapped["Plan"] = relationship("Plan", back_populates="subscriptions")


class RedeemCode(Base):
    __tablename__ = "redeem_codes"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(64), unique=True)
    credit_amount: Mapped[int] = mapped_column(Integer)
    max_uses: Mapped[int | None] = mapped_column(Integer, nullable=True)
    used_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by: Mapped[int] = mapped_column(BigInteger)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    records: Mapped[list["RedeemRecord"]] = relationship("RedeemRecord", back_populates="code_ref", cascade="all, delete-orphan")


class RedeemRecord(Base):
    __tablename__ = "redeem_records"
    __table_args__ = (UniqueConstraint("code_id", "user_id"),)
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code_id: Mapped[int] = mapped_column(Integer, ForeignKey("redeem_codes.id", ondelete="CASCADE"))
    user_id: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    code_ref: Mapped["RedeemCode"] = relationship("RedeemCode", back_populates="records")


class BroadcastStatus(str, enum.Enum):
    pending = "pending"; running = "running"; completed = "completed"; cancelled = "cancelled"


class BroadcastJob(Base):
    __tablename__ = "broadcast_jobs"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    created_by: Mapped[int] = mapped_column(BigInteger)
    message_text: Mapped[str] = mapped_column(Text)
    photo_file_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[BroadcastStatus] = mapped_column(SAEnum(BroadcastStatus, name="bcstatus_enum"), default=BroadcastStatus.pending)
    total_users: Mapped[int] = mapped_column(Integer, default=0)
    sent_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AppSettings(Base):
    __tablename__ = "app_settings"
    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class AdminActivityLog(Base):
    __tablename__ = "admin_activity_logs"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    actor_id: Mapped[int] = mapped_column(BigInteger)
    action: Mapped[str] = mapped_column(String(128))
    target_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ============================================================
#  UTILS
# ============================================================
STATUS_EMOJI = {
    "running": "🟢", "stopped": "🔴", "pending": "🟡", "failed": "🔴",
    "suspended": "⚫", "queued": "🟡", "building": "🔵", "success": "🟢",
}
HEADER = (
    "╔══════════════════════════════════╗\n"
    "║   🚀  <b>SODO HOSTING</b>  ⚡           ║\n"
    "║   Premium Telegram Hosting       ║\n"
    "╚══════════════════════════════════╝"
)


def ts(dt: datetime | None) -> str:
    if not dt:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def truncate(text: str, n: int = 3000) -> str:
    return text if len(text) <= n else "...\n" + text[-(n - 4):]


def safe_name(s: str) -> str:
    return (re.sub(r"[^\w\-\. ]", "", s).strip())[:64] or "project"


def gen_code(n: int = 10) -> str:
    return "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(n))


def paginate(items: list[Any], page: int, size: int = 5) -> tuple[list[Any], int, int]:
    total = max(1, math.ceil(len(items) / size))
    page = max(0, min(page, total - 1))
    return items[page * size:(page + 1) * size], total, page


def nav_buttons(page: int, total: int, prefix: str) -> list[InlineKeyboardButton]:
    btns = []
    if page > 0:
        btns.append(InlineKeyboardButton("◀️", callback_data=f"{prefix}:{page-1}"))
    if total > 1:
        btns.append(InlineKeyboardButton(f"{page+1}/{total}", callback_data="noop"))
    if page < total - 1:
        btns.append(InlineKeyboardButton("▶️", callback_data=f"{prefix}:{page+1}"))
    return btns


def validate_py_file(data: bytes, filename: str, max_mb: int) -> tuple[bool, str]:
    if len(data) > max_mb * 1024 * 1024:
        return False, f"File too large. Max {max_mb}MB."
    if not filename.endswith(".py"):
        return False, "Only .py files allowed on free plan."
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False, "File must be valid UTF-8."
    return True, ""


def validate_zip(data: bytes, max_mb: int) -> tuple[bool, str]:
    if len(data) > max_mb * 1024 * 1024:
        return False, f"Archive too large. Max {max_mb}MB."
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            total = 0
            for info in zf.infolist():
                if ".." in info.filename or info.filename.startswith("/"):
                    return False, "Archive contains unsafe paths."
                total += info.file_size
                if total > 200 * 1024 * 1024:
                    return False, "Uncompressed size > 200MB."
            if len(zf.namelist()) > 100:
                return False, "Too many files (max 100)."
    except zipfile.BadZipFile:
        return False, "Invalid ZIP file."
    return True, ""


def validate_github_url(url: str) -> tuple[bool, str]:
    if re.match(r"^https://github\.com/[\w\-\.]+/[\w\-\.]+(/.+)?$", url):
        return True, ""
    return False, "Must be: https://github.com/owner/repo"


def find_entry(d: Path, runtime: str) -> str | None:
    if runtime == "python":
        for f in ("main.py", "app.py", "bot.py", "run.py"):
            if (d / f).exists():
                return f
        files = list(d.glob("*.py"))
        return files[0].name if files else None
    for f in ("index.js", "main.js", "app.js", "server.js"):
        if (d / f).exists():
            return f
    files = list(d.glob("*.js"))
    return files[0].name if files else None


# ============================================================
#  SERVICES
# ============================================================

# ----- User -----
async def svc_get_or_create_user(session, tid, username, first_name, last_name, ref_code=None):
    from sqlalchemy.orm import selectinload
    stmt = select(User).where(User.id == tid).options(
        selectinload(User.wallet), selectinload(User.referral_link))
    user = (await session.execute(stmt)).scalar_one_or_none()
    if user:
        user.username = username; user.first_name = first_name
        user.last_name = last_name; user.last_active_at = datetime.now(timezone.utc)
        return user, False
    user = User(id=tid, username=username, first_name=first_name,
                last_name=last_name, referral_code_used=ref_code)
    user.wallet = CreditWallet(user_id=tid, balance=0)
    user.referral_link = ReferralLink(user_id=tid, code=gen_code())
    session.add(user)
    return user, True


async def svc_get_user(session, tid):
    from sqlalchemy.orm import selectinload
    stmt = select(User).where(User.id == tid).options(
        selectinload(User.wallet), selectinload(User.referral_link))
    return (await session.execute(stmt)).scalar_one_or_none()


async def svc_ban(session, tid, reason):
    u = await svc_get_user(session, tid)
    if not u: return False
    u.is_banned = True; u.ban_reason = reason; u.banned_at = datetime.now(timezone.utc)
    return True


async def svc_unban(session, tid):
    u = await svc_get_user(session, tid)
    if not u: return False
    u.is_banned = False; u.ban_reason = None; u.banned_at = None
    return True


async def svc_user_stats(session):
    total = (await session.scalar(select(func.count(User.id)))) or 0
    banned = (await session.scalar(select(func.count(User.id)).where(User.is_banned == True))) or 0
    return {"total": total, "banned": banned, "active": total - banned}


async def svc_search_users(session, q, limit=10):
    try:
        stmt = select(User).where(User.id == int(q)).limit(limit)
    except ValueError:
        stmt = select(User).where(User.username.ilike(f"%{q.lstrip('@')}%")).limit(limit)
    return list((await session.execute(stmt)).scalars().all())


async def svc_all_user_ids(session):
    return [r[0] for r in (await session.execute(select(User.id).where(User.is_banned == False))).all()]


# ----- Credits -----
async def svc_locked_wallet(session, uid):
    return (await session.execute(
        select(CreditWallet).where(CreditWallet.user_id == uid).with_for_update()
    )).scalar_one_or_none()


async def svc_get_balance(session, uid):
    w = await svc_locked_wallet(session, uid)
    return w.balance if w else 0


async def svc_add_credits(session, uid, amount, tx_type, desc, actor=None):
    if amount <= 0: raise ValueError("amount must be positive")
    w = await svc_locked_wallet(session, uid)
    if w is None:
        w = CreditWallet(user_id=uid, balance=0); session.add(w)
    w.balance += amount
    session.add(CreditTransaction(user_id=uid, amount=amount, type=tx_type, description=desc, actor_id=actor))
    return w.balance


async def svc_deduct_credits(session, uid, amount, tx_type, desc, actor=None):
    if amount <= 0: raise ValueError("amount must be positive")
    w = await svc_locked_wallet(session, uid)
    if w is None or w.balance < amount: raise ValueError("Insufficient balance")
    w.balance -= amount
    session.add(CreditTransaction(user_id=uid, amount=-amount, type=tx_type, description=desc, actor_id=actor))
    return w.balance


async def svc_claim_daily(session, uid, reward):
    today_start = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)
    existing = (await session.execute(
        select(CreditTransaction).where(
            CreditTransaction.user_id == uid,
            CreditTransaction.type == TransactionType.daily,
            CreditTransaction.created_at >= today_start,
        )
    )).scalar_one_or_none()
    if existing: return False
    await svc_add_credits(session, uid, reward, TransactionType.daily, "Daily reward")
    return True


async def svc_get_txs(session, uid, limit=20, offset=0):
    return list((await session.execute(
        select(CreditTransaction).where(CreditTransaction.user_id == uid)
        .order_by(CreditTransaction.created_at.desc()).limit(limit).offset(offset)
    )).scalars().all())


async def svc_total_credits(session):
    return int((await session.scalar(
        select(func.sum(CreditTransaction.amount)).where(CreditTransaction.amount > 0)
    )) or 0)


# ----- Force Join -----
async def svc_active_channels(session):
    return list((await session.execute(
        select(RequiredChannel).where(RequiredChannel.is_enabled == True).order_by(RequiredChannel.id)
    )).scalars().all())


async def svc_check_membership(bot: Bot, session, uid):
    channels = await svc_active_channels(session)
    statuses = []
    for ch in channels:
        try:
            m = await bot.get_chat_member(chat_id=ch.chat_id, user_id=uid)
            is_member = m.status in ("member", "administrator", "creator")
        except TelegramError as e:
            log.warning("Force join check %s: %s", ch.chat_id, e)
            is_member = False
        statuses.append((ch, is_member))
    return statuses


async def svc_all_joined(bot, session, uid):
    return all(ok for _, ok in await svc_check_membership(bot, session, uid))


async def svc_add_channel(session, chat_id, display_name, username=None, invite_url=None):
    ch = RequiredChannel(chat_id=chat_id, display_name=display_name,
                         username=username, invite_url=invite_url, is_enabled=True)
    session.add(ch); return ch


async def svc_remove_channel(session, ch_id):
    ch = (await session.execute(select(RequiredChannel).where(RequiredChannel.id == ch_id))).scalar_one_or_none()
    if not ch: return False
    await session.delete(ch); return True


async def svc_list_channels(session):
    return list((await session.execute(select(RequiredChannel).order_by(RequiredChannel.id))).scalars().all())


# ----- Referral -----
async def svc_get_ref_link(session, uid):
    return (await session.execute(select(ReferralLink).where(ReferralLink.user_id == uid))).scalar_one_or_none()


async def svc_find_ref_by_code(session, code):
    return (await session.execute(select(ReferralLink).where(ReferralLink.code == code))).scalar_one_or_none()


async def svc_process_referral(session, referrer_id, referred_id, reward):
    if referrer_id == referred_id: return False
    if (await session.execute(select(ReferralRecord).where(ReferralRecord.referred_id == referred_id))).scalar_one_or_none():
        return False
    session.add(ReferralRecord(referrer_id=referrer_id, referred_id=referred_id, rewarded_at=datetime.now(timezone.utc)))
    await svc_add_credits(session, referrer_id, reward, TransactionType.referral, f"Referral bonus")
    return True


async def svc_ref_stats(session, uid):
    total = (await session.scalar(select(func.count(ReferralRecord.id)).where(ReferralRecord.referrer_id == uid))) or 0
    return {"total": total}


async def svc_leaderboard(session, limit=10):
    stmt = (
        select(ReferralRecord.referrer_id, func.count(ReferralRecord.id).label("cnt"))
        .group_by(ReferralRecord.referrer_id)
        .order_by(func.count(ReferralRecord.id).desc()).limit(limit)
    )
    return [(r.referrer_id, r.cnt) for r in (await session.execute(stmt)).all()]


# ----- Subscription -----
async def svc_active_sub(session, uid):
    from sqlalchemy.orm import selectinload
    now = datetime.now(timezone.utc)
    for sub in (await session.execute(
        select(Subscription).where(Subscription.user_id == uid, Subscription.is_active == True)
        .options(selectinload(Subscription.plan)).order_by(Subscription.created_at.desc())
    )).scalars().all():
        if sub.expires_at is None or sub.expires_at > now:
            return sub
        sub.is_active = False
    return None


async def svc_is_premium(session, uid):
    return (await svc_active_sub(session, uid)) is not None


async def svc_get_limits(session, uid):
    sub = await svc_active_sub(session, uid)
    if sub and sub.plan:
        p = sub.plan
        return {
            "max_projects": p.max_projects, "max_file_size_mb": p.max_file_size_mb,
            "allows_zip": p.allows_zip, "allows_github": p.allows_github,
            "allows_nodejs": p.allows_nodejs, "allows_env_vars": p.allows_env_vars,
            "is_premium": True, "plan_name": p.display_name, "expires_at": sub.expires_at,
        }
    return {
        "max_projects": 1, "max_file_size_mb": 1,
        "allows_zip": False, "allows_github": False,
        "allows_nodejs": False, "allows_env_vars": False,
        "is_premium": False, "plan_name": "Free", "expires_at": None,
    }


async def svc_grant_premium(session, uid, plan_id, granted_by, days=None):
    sub = await svc_active_sub(session, uid)
    if sub: sub.is_active = False
    now = datetime.now(timezone.utc)
    new_sub = Subscription(
        user_id=uid, plan_id=plan_id, granted_by=granted_by, starts_at=now,
        expires_at=(now + timedelta(days=days)) if days else None, is_active=True,
    )
    session.add(new_sub); return new_sub


async def svc_revoke_premium(session, uid):
    sub = await svc_active_sub(session, uid)
    if not sub: return False
    sub.is_active = False; return True


async def svc_extend_premium(session, uid, days):
    sub = await svc_active_sub(session, uid)
    if not sub: return False
    now = datetime.now(timezone.utc)
    base = sub.expires_at if (sub.expires_at and sub.expires_at > now) else now
    sub.expires_at = base + timedelta(days=days); return True


async def svc_all_plans(session):
    return list((await session.execute(select(Plan).where(Plan.is_active == True).order_by(Plan.id))).scalars().all())


# ----- Projects -----
async def svc_user_projects(session, uid):
    return list((await session.execute(
        select(Project).where(Project.owner_id == uid).order_by(Project.created_at.desc())
    )).scalars().all())


async def svc_get_project(session, pid, owner_id=None):
    stmt = select(Project).where(Project.id == pid)
    if owner_id: stmt = stmt.where(Project.owner_id == owner_id)
    return (await session.execute(stmt)).scalar_one_or_none()


async def svc_create_project(session, owner_id, name, runtime=Runtime.python,
                              storage_path=None, entry_file=None, github_url=None, env_vars_json=None):
    p = Project(owner_id=owner_id, name=name, runtime=runtime, status=ProjectStatus.pending,
                storage_path=storage_path, entry_file=entry_file, github_url=github_url, env_vars=env_vars_json)
    session.add(p); return p


async def svc_add_log(session, pid, content, dep_id=None):
    session.add(LogEntry(project_id=pid, deployment_id=dep_id, content=content))


async def svc_get_logs(session, pid, limit=50):
    return list(reversed((await session.execute(
        select(LogEntry).where(LogEntry.project_id == pid)
        .order_by(LogEntry.created_at.desc()).limit(limit)
    )).scalars().all()))


async def svc_count_user_projects(session, uid):
    return (await session.scalar(select(func.count(Project.id)).where(Project.owner_id == uid))) or 0


async def svc_all_projects(session, limit=50, offset=0):
    from sqlalchemy.orm import selectinload
    return list((await session.execute(
        select(Project).options(selectinload(Project.owner))
        .order_by(Project.created_at.desc()).limit(limit).offset(offset)
    )).scalars().all())


async def svc_count_projects(session):
    return (await session.scalar(select(func.count(Project.id)))) or 0


async def svc_count_running(session):
    return (await session.scalar(select(func.count(Project.id)).where(Project.status == ProjectStatus.running))) or 0


async def svc_count_failed_deps(session):
    return (await session.scalar(select(func.count(Deployment.id)).where(Deployment.status == DeploymentStatus.failed))) or 0


# ----- Hosting -----
def _child_env(project: Project) -> dict:
    env = dict(os.environ)
    for k in ("BOT_TOKEN", "DATABASE_URL", "OWNER_ID"):
        env.pop(k, None)
    env["SODO_PROJECT_ID"] = str(project.id)
    if project.env_vars:
        try:
            env.update({str(k): str(v) for k, v in json.loads(project.env_vars).items()})
        except Exception:
            pass
    return env


async def svc_start_project(session, project: Project, storage_base: str) -> bool:
    if not project.storage_path or not project.entry_file:
        return False
    entry = Path(storage_base) / project.storage_path / project.entry_file
    if not entry.exists():
        await svc_add_log(session, project.id, f"Entry not found: {entry}")
        return False
    dep = Deployment(project_id=project.id, triggered_by=project.owner_id,
                     status=DeploymentStatus.running, started_at=datetime.now(timezone.utc))
    session.add(dep); await session.flush()
    try:
        cmd = ([sys.executable, str(entry)] if project.runtime == Runtime.python
               else ["node", str(entry)])
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=str(entry.parent), env=_child_env(project),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        project.process_pid = proc.pid
        project.status = ProjectStatus.running
        dep.status = DeploymentStatus.success
        dep.completed_at = datetime.now(timezone.utc)
        await svc_add_log(session, project.id, f"Process launched PID={proc.pid}", dep.id)
        log_path = Path(storage_base) / project.storage_path / "runtime.log"
        asyncio.create_task(_drain(proc, log_path))
        return True
    except Exception as e:
        log.error("Start failed %s: %s", project.id, e)
        project.status = ProjectStatus.failed
        dep.status = DeploymentStatus.failed
        dep.error_message = str(e)
        dep.completed_at = datetime.now(timezone.utc)
        await svc_add_log(session, project.id, f"Launch error: {e}", dep.id)
        return False


async def _drain(proc, log_path: Path):
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "ab") as f:
            while True:
                line = await proc.stdout.readline()
                if not line: break
                f.write(line)
    except Exception as e:
        log.debug("Drain error: %s", e)


async def svc_stop_project(session, project: Project) -> bool:
    if project.process_pid:
        try:
            os.kill(project.process_pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    project.status = ProjectStatus.stopped; project.process_pid = None
    await svc_add_log(session, project.id, "Project stopped.")
    return True


async def svc_restart_project(session, project: Project, storage_base: str) -> bool:
    await svc_stop_project(session, project)
    await asyncio.sleep(0.5)
    return await svc_start_project(session, project, storage_base)


# ----- Redeem -----
async def svc_create_code(session, created_by, amount, max_uses=1, expires_at=None):
    c = RedeemCode(code=gen_code(12), credit_amount=amount, max_uses=max_uses,
                   expires_at=expires_at, created_by=created_by, is_active=True)
    session.add(c); return c


async def svc_redeem_code(session, uid, code_str):
    code = (await session.execute(
        select(RedeemCode).where(RedeemCode.code == code_str.upper(), RedeemCode.is_active == True)
    )).scalar_one_or_none()
    if not code: return False, "Invalid or inactive code."
    now = datetime.now(timezone.utc)
    if code.expires_at and code.expires_at < now: return False, "Code expired."
    if code.max_uses is not None and code.used_count >= code.max_uses: return False, "Code fully redeemed."
    if (await session.execute(
        select(RedeemRecord).where(RedeemRecord.code_id == code.id, RedeemRecord.user_id == uid)
    )).scalar_one_or_none():
        return False, "Already redeemed."
    session.add(RedeemRecord(code_id=code.id, user_id=uid))
    code.used_count += 1
    await svc_add_credits(session, uid, code.credit_amount, TransactionType.redeem, f"Code {code_str.upper()}")
    return True, f"✅ {code.credit_amount} credits added!"


async def svc_list_codes(session):
    return list((await session.execute(
        select(RedeemCode).where(RedeemCode.is_active == True).order_by(RedeemCode.created_at.desc())
    )).scalars().all())


# ----- Broadcast -----
async def svc_run_broadcast(bot: Bot, job_id: int):
    async with _sm() as session:
        job = (await session.execute(select(BroadcastJob).where(BroadcastJob.id == job_id))).scalar_one_or_none()
        if not job: return
        uids = await svc_all_user_ids(session)
        job.status = BroadcastStatus.running; job.total_users = len(uids)
        job.started_at = datetime.now(timezone.utc)
        await session.commit()
    sent = failed = 0
    for uid in uids:
        try:
            if job.photo_file_id:
                await bot.send_photo(chat_id=uid, photo=job.photo_file_id, caption=job.message_text, parse_mode="HTML")
            else:
                await bot.send_message(chat_id=uid, text=job.message_text, parse_mode="HTML")
            sent += 1
        except TelegramError:
            failed += 1
        if (sent + failed) % 25 == 0:
            await asyncio.sleep(1)
    async with _sm() as session:
        job = (await session.execute(select(BroadcastJob).where(BroadcastJob.id == job_id))).scalar_one_or_none()
        if job:
            job.sent_count = sent; job.failed_count = failed
            job.status = BroadcastStatus.completed; job.completed_at = datetime.now(timezone.utc)
        await session.commit()


# ----- Admin log -----
async def svc_log_action(session, actor_id, action, target_type=None, target_id=None, detail=None):
    session.add(AdminActivityLog(actor_id=actor_id, action=action,
                                 target_type=target_type, target_id=target_id, detail=detail))


async def svc_recent_admin_logs(session, limit=20):
    return list((await session.execute(
        select(AdminActivityLog).order_by(AdminActivityLog.created_at.desc()).limit(limit)
    )).scalars().all())


# ============================================================
#  DECORATORS
# ============================================================
import functools


def owner_only(fn):
    @functools.wraps(fn)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *a, **kw):
        cfg: Config = context.bot_data["config"]
        u = update.effective_user
        if not u or u.id != cfg.owner_id:
            if update.callback_query:
                await update.callback_query.answer("⛔ Not authorized.", show_alert=True)
            elif update.message:
                await update.message.reply_text("⛔ Not authorized.")
            return
        return await fn(update, context, *a, **kw)
    return wrapper


def not_banned(fn):
    @functools.wraps(fn)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *a, **kw):
        u = update.effective_user
        if not u: return
        async with get_session() as s:
            db_u = await svc_get_user(s, u.id)
            if db_u and db_u.is_banned:
                txt = f"🚫 <b>Account banned.</b>\n<i>{db_u.ban_reason or 'Contact support.'}</i>"
                if update.callback_query:
                    await update.callback_query.answer("🚫 Banned.", show_alert=True)
                elif update.message:
                    await update.message.reply_text(txt, parse_mode="HTML")
                return
        return await fn(update, context, *a, **kw)
    return wrapper


def require_joined(fn):
    @functools.wraps(fn)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *a, **kw):
        u = update.effective_user
        if not u: return
        async with get_session() as s:
            chs = await svc_active_channels(s)
            if not chs:
                return await fn(update, context, *a, **kw)
            joined = await svc_all_joined(context.bot, s, u.id)
        if joined:
            return await fn(update, context, *a, **kw)
        await _show_join_screen(update, context)
    return wrapper


# ============================================================
#  KEYBOARDS
# ============================================================
def kb_main_menu(is_premium=False, is_owner=False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🚀 Deploy Project", callback_data="deploy"),
         InlineKeyboardButton("📂 My Projects", callback_data="projects")],
        [InlineKeyboardButton("▶️ Start", callback_data="pp:start"),
         InlineKeyboardButton("🔄 Restart", callback_data="pp:restart"),
         InlineKeyboardButton("⏹️ Stop", callback_data="pp:stop")],
        [InlineKeyboardButton("📊 Status", callback_data="pp:status"),
         InlineKeyboardButton("📜 Logs", callback_data="pp:logs")],
        [InlineKeyboardButton("💎 Premium Plans", callback_data="premium"),
         InlineKeyboardButton("🪙 Earn Credits", callback_data="credits")],
        [InlineKeyboardButton("👤 My Account", callback_data="account"),
         InlineKeyboardButton("🆘 Support", callback_data="support")],
    ]
    if is_owner:
        rows.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(rows)


def kb_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Dashboard", callback_data="admin:dash"),
         InlineKeyboardButton("👥 Users", callback_data="admin:users")],
        [InlineKeyboardButton("🚀 Hosting", callback_data="admin:hosting"),
         InlineKeyboardButton("💎 Premium", callback_data="admin:premium")],
        [InlineKeyboardButton("🔐 Force Join", callback_data="admin:fj"),
         InlineKeyboardButton("🎟️ Redeem", callback_data="admin:redeem")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin:broadcast"),
         InlineKeyboardButton("⚙️ Settings", callback_data="admin:settings")],
        [InlineKeyboardButton("🛡️ Activity Logs", callback_data="admin:activity"),
         InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")],
    ])


def kb_fj_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Channel", callback_data="admin:fj:add"),
         InlineKeyboardButton("➖ Remove Channel", callback_data="admin:fj:remove_list")],
        [InlineKeyboardButton("📋 List Channels", callback_data="admin:fj:list")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
    ])


def kb_user_actions(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Credits", callback_data=f"au:{uid}:add"),
         InlineKeyboardButton("➖ Credits", callback_data=f"au:{uid}:deduct")],
        [InlineKeyboardButton("💎 Grant Premium", callback_data=f"au:{uid}:premium"),
         InlineKeyboardButton("🔧 Revoke Premium", callback_data=f"au:{uid}:revoke_premium")],
        [InlineKeyboardButton("🚫 Ban", callback_data=f"au:{uid}:ban"),
         InlineKeyboardButton("✅ Unban", callback_data=f"au:{uid}:unban")],
        [InlineKeyboardButton("📂 Projects", callback_data=f"au:{uid}:projects"),
         InlineKeyboardButton("🔙 Back", callback_data="admin:users")],
    ])


_CANCEL_KB = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_deploy")]])


# ============================================================
#  HANDLERS — helpers
# ============================================================
async def _show_join_screen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    async with get_session() as s:
        statuses = await svc_check_membership(context.bot, s, u.id)
        missing = [(ch, ok) for ch, ok in statuses if not ok]
    btns = []
    for ch, _ in missing:
        url = f"https://t.me/{ch.username}" if ch.username else ch.invite_url
        if url:
            btns.append([InlineKeyboardButton(f"📢 Join {ch.display_name}", url=url)])
    btns.append([InlineKeyboardButton("🟢 ✅ I'VE JOINED — VERIFY", callback_data="verify_join")])
    txt = (
        "🔐 <b>SODO HOSTING — ACCESS VERIFICATION</b>\n\n"
        "Join all required channels then tap <b>✅ VERIFY</b>."
    )
    msg = update.message or (update.callback_query.message if update.callback_query else None)
    if msg:
        await msg.reply_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(btns))


async def _send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        premium = await svc_is_premium(s, u.id)
    txt = f"{HEADER}\n\n👋 Welcome, <b>{u.first_name}</b>!\n\nSelect an option:"
    kb = kb_main_menu(premium, u.id == cfg.owner_id)
    if update.callback_query:
        await update.callback_query.edit_message_text(txt, parse_mode="HTML", reply_markup=kb)
    else:
        await update.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)


# ============================================================
#  HANDLERS — /start and force join
# ============================================================
async def h_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u: return
    ref_code = None
    if context.args and context.args[0].startswith("ref_"):
        ref_code = context.args[0][4:]
    async with get_session() as s:
        db_u, created = await svc_get_or_create_user(
            s, u.id, u.username, u.first_name, u.last_name, ref_code)
        if db_u.is_banned:
            await update.message.reply_text(
                f"🚫 <b>Account Banned</b>\n\n<i>{db_u.ban_reason or 'Contact support.'}</i>",
                parse_mode="HTML")
            return
        cfg: Config = context.bot_data["config"]
        if created:
            await svc_add_credits(s, u.id, cfg.welcome_credits, TransactionType.welcome, "Welcome bonus")
            if ref_code:
                link = await svc_find_ref_by_code(s, ref_code)
                if link and link.user_id != u.id:
                    await svc_process_referral(s, link.user_id, u.id, cfg.referral_reward_credits)
        chs = await svc_active_channels(s)
        if chs:
            statuses = await svc_check_membership(context.bot, s, u.id)
            missing = [ch for ch, ok in statuses if not ok]
            if missing:
                btns = []
                for ch in missing:
                    url = f"https://t.me/{ch.username}" if ch.username else ch.invite_url
                    if url:
                        btns.append([InlineKeyboardButton(f"📢 Join {ch.display_name}", url=url)])
                btns.append([InlineKeyboardButton("🟢 ✅ I'VE JOINED — VERIFY", callback_data="verify_join")])
                await update.message.reply_text(
                    "🔐 <b>SODO HOSTING — ACCESS VERIFICATION</b>\n\nJoin required channels then verify.",
                    parse_mode="HTML", reply_markup=InlineKeyboardMarkup(btns))
                return
        premium = await svc_is_premium(s, u.id)
    greeting = "🎉 Welcome to <b>SODO HOSTING</b>!" if created else f"👋 Welcome back, <b>{u.first_name}</b>!"
    await update.message.reply_text(
        f"{HEADER}\n\n{greeting}\n\n⚡ Deploy and manage your projects from Telegram.",
        parse_mode="HTML",
        reply_markup=kb_main_menu(premium, u.id == context.bot_data["config"].owner_id),
    )


async def h_verify_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user
    async with get_session() as s:
        db_u = await svc_get_user(s, u.id)
        if db_u and db_u.is_banned:
            await q.edit_message_text("🚫 Your account is banned."); return
        statuses = await svc_check_membership(context.bot, s, u.id)
        missing = [ch for ch, ok in statuses if not ok]
        if missing:
            await q.answer(f"❌ Still not joined: {', '.join(ch.display_name for ch in missing)}", show_alert=True)
            return
        premium = await svc_is_premium(s, u.id)
    await q.edit_message_text("✅ <b>Verified!</b> Welcome to SODO HOSTING!", parse_mode="HTML")
    cfg: Config = context.bot_data["config"]
    await q.message.reply_text(
        f"{HEADER}\n\n👋 Welcome, <b>{u.first_name}</b>!",
        parse_mode="HTML",
        reply_markup=kb_main_menu(premium, u.id == cfg.owner_id),
    )


async def h_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    await _send_main_menu(update, context)


# ============================================================
#  HANDLERS — Deploy (ConversationHandler)
# ============================================================
_DTYPE, _NAME, _UPLOAD, _RUNTIME, _GITHUB, _ENVVARS, _CONFIRM = range(7)


async def h_deploy_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if q: await q.answer()
    u = update.effective_user
    async with get_session() as s:
        limits = await svc_get_limits(s, u.id)
        count = await svc_count_user_projects(s, u.id)
    if count >= limits["max_projects"]:
        txt = (f"⚠️ <b>Project Limit Reached</b>\n\nPlan allows {limits['max_projects']} project(s), you have {count}."
               + ("\n\nUpgrade to premium for more." if not limits["is_premium"] else ""))
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]])
        if q: await q.edit_message_text(txt, parse_mode="HTML", reply_markup=kb)
        else: await update.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)
        return ConversationHandler.END
    context.user_data["deploy"] = {"limits": limits}
    if not limits["is_premium"]:
        context.user_data["deploy"].update({"type": "single", "runtime": "python"})
        txt = "🚀 <b>Deploy Project</b>\n\n📋 Free plan: single <code>.py</code> file.\n\nProject name:"
        if q: await q.edit_message_text(txt, parse_mode="HTML", reply_markup=_CANCEL_KB)
        else: await update.message.reply_text(txt, parse_mode="HTML", reply_markup=_CANCEL_KB)
        return _NAME
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Single Python File", callback_data="dt:single")],
        [InlineKeyboardButton("📦 ZIP Archive", callback_data="dt:zip")],
        [InlineKeyboardButton("🐙 GitHub Repository", callback_data="dt:github")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_deploy")],
    ])
    if q: await q.edit_message_text("🚀 <b>Deploy Project</b>\n\nChoose type:", parse_mode="HTML", reply_markup=kb)
    else: await update.message.reply_text("🚀 <b>Deploy Project</b>\n\nChoose type:", parse_mode="HTML", reply_markup=kb)
    return _DTYPE


async def h_dtype(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    context.user_data["deploy"]["type"] = q.data.split(":")[1]
    await q.edit_message_text("✏️ Project name:", reply_markup=_CANCEL_KB)
    return _NAME


async def h_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = safe_name(update.message.text or "")
    if not name:
        await update.message.reply_text("❌ Invalid name. Try again:")
        return _NAME
    context.user_data["deploy"]["name"] = name
    dtype = context.user_data["deploy"]["type"]
    limits = context.user_data["deploy"]["limits"]
    if dtype == "github":
        await update.message.reply_text(
            "🐙 <b>GitHub URL</b>\n<code>https://github.com/owner/repo</code>",
            parse_mode="HTML", reply_markup=_CANCEL_KB)
        return _GITHUB
    if dtype == "zip" and limits.get("allows_nodejs"):
        await update.message.reply_text("⚙️ Runtime:", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🐍 Python", callback_data="rt:python"),
            InlineKeyboardButton("🟩 Node.js", callback_data="rt:nodejs"),
        ]]))
        return _RUNTIME
    context.user_data["deploy"]["runtime"] = "python"
    ext = "ZIP archive" if dtype == "zip" else "<code>.py</code> file"
    await update.message.reply_text(f"📤 Send your {ext}:", parse_mode="HTML", reply_markup=_CANCEL_KB)
    return _UPLOAD


async def h_runtime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    context.user_data["deploy"]["runtime"] = q.data.split(":")[1]
    await q.edit_message_text("📤 Send your archive or file:", reply_markup=_CANCEL_KB)
    return _UPLOAD


async def h_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    url = (update.message.text or "").strip()
    ok, err = validate_github_url(url)
    if not ok:
        await update.message.reply_text(f"❌ {err}\n\nTry again:")
        return _GITHUB
    context.user_data["deploy"]["github_url"] = url
    limits = context.user_data["deploy"]["limits"]
    if limits.get("allows_nodejs"):
        await update.message.reply_text("⚙️ Runtime:", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🐍 Python", callback_data="rt:python"),
            InlineKeyboardButton("🟩 Node.js", callback_data="rt:nodejs"),
        ]]))
        return _RUNTIME
    context.user_data["deploy"]["runtime"] = "python"
    if limits.get("allows_env_vars"):
        await update.message.reply_text(
            "🔧 Env vars (KEY=value per line) or /skip:",
            reply_markup=_CANCEL_KB)
        return _ENVVARS
    return await _show_deploy_confirm(update, context)


async def h_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    doc = update.message.document
    if not doc:
        await update.message.reply_text("❌ Please send a file.")
        return _UPLOAD
    dtype = context.user_data["deploy"]["type"]
    max_mb = context.user_data["deploy"]["limits"]["max_file_size_mb"]
    data = bytes(await (await doc.get_file()).download_as_bytearray())
    ok, err = (validate_zip(data, max_mb) if dtype == "zip"
               else validate_py_file(data, doc.file_name or "", max_mb))
    if not ok:
        await update.message.reply_text(f"❌ {err}")
        return _UPLOAD
    context.user_data["deploy"]["file_bytes"] = data
    context.user_data["deploy"]["filename"] = doc.file_name or ("main.py" if dtype == "single" else "project.zip")
    if context.user_data["deploy"]["limits"].get("allows_env_vars"):
        await update.message.reply_text(
            "🔧 Env vars (KEY=value per line) or /skip:", reply_markup=_CANCEL_KB)
        return _ENVVARS
    return await _show_deploy_confirm(update, context)


async def h_envvars(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    txt = (update.message.text or "").strip()
    env = {}
    if txt.lower() not in ("/skip", "skip"):
        for line in txt.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    context.user_data["deploy"]["env_vars"] = env
    return await _show_deploy_confirm(update, context)


async def _show_deploy_confirm(update, context) -> int:
    d = context.user_data["deploy"]
    lines = [
        "📋 <b>Deploy Summary</b>",
        f"🏷️ Name: <b>{d.get('name')}</b>",
        f"📦 Type: <b>{d.get('type','single').title()}</b>",
        f"⚙️ Runtime: <b>{d.get('runtime','python')}</b>",
    ]
    if d.get("github_url"):
        lines.append(f"🐙 Repo: <code>{d['github_url']}</code>")
    if d.get("env_vars"):
        lines.append(f"🔧 Env vars: {len(d['env_vars'])}")
    lines.append("\nReady to deploy?")
    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🚀 Deploy Now", callback_data="deploy_go"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_deploy"),
        ]]),
    )
    return _CONFIRM


async def h_deploy_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    if q.data == "cancel_deploy":
        context.user_data.pop("deploy", None)
        await q.edit_message_text("❌ Cancelled.")
        return ConversationHandler.END
    await q.edit_message_text("⏳ <b>Deploying…</b>", parse_mode="HTML")
    u = update.effective_user
    d = context.user_data.pop("deploy", {})
    cfg: Config = context.bot_data["config"]
    dtype = d.get("type", "single"); runtime = d.get("runtime", "python")
    project_dir_id = uuid.uuid4()
    pdir = Path(cfg.storage_path) / str(project_dir_id)
    pdir.mkdir(parents=True, exist_ok=True)
    entry_file = None; github_url = None
    try:
        if dtype == "single":
            fname = d.get("filename", "main.py")
            (pdir / fname).write_bytes(d["file_bytes"]); entry_file = fname
        elif dtype == "zip":
            with zipfile.ZipFile(io.BytesIO(d["file_bytes"])) as zf:
                zf.extractall(str(pdir))
            entry_file = find_entry(pdir, runtime)
        elif dtype == "github":
            github_url = d.get("github_url")
            proc = await asyncio.create_subprocess_exec(
                "git", "clone", "--depth=1", github_url, str(pdir),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, err = await proc.communicate()
            if proc.returncode != 0:
                await q.edit_message_text(f"❌ Git clone failed:\n<code>{err.decode()[:400]}</code>", parse_mode="HTML")
                return ConversationHandler.END
            entry_file = find_entry(pdir, runtime)
        if not entry_file:
            await q.edit_message_text("❌ No entry file found (main.py or index.js).")
            return ConversationHandler.END
        env_json = json.dumps(d.get("env_vars", {})) if d.get("env_vars") else None
        async with get_session() as s:
            p = await svc_create_project(s, u.id, d.get("name", "project"), Runtime(runtime),
                                          str(project_dir_id), entry_file, github_url, env_json)
            await s.flush()
            started = await svc_start_project(s, p, cfg.storage_path)
            pid_str = str(p.id)
        if started:
            await q.edit_message_text(
                f"✅ <b>Deployed!</b>\n\n🏷️ {d.get('name')}\n🆔 <code>{pid_str[:8]}…</code>\n🟢 Running",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📂 My Projects", callback_data="projects"),
                    InlineKeyboardButton("🏠 Menu", callback_data="main_menu"),
                ]]),
            )
        else:
            await q.edit_message_text(
                "⚠️ Project saved but failed to start. Try starting from My Projects.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📂 Projects", callback_data="projects")]]))
    except Exception as e:
        log.exception("Deploy error uid=%d", u.id)
        await q.edit_message_text(f"❌ Deployment error: {str(e)[:300]}")
    return ConversationHandler.END


async def h_deploy_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("deploy", None)
    q = update.callback_query
    if q: await q.answer(); await q.edit_message_text("❌ Cancelled.")
    elif update.message: await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END


# ============================================================
#  HANDLERS — Projects
# ============================================================
async def h_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user
    parts = q.data.split(":"); page = int(parts[1]) if len(parts) > 1 else 0
    async with get_session() as s:
        projs = await svc_user_projects(s, u.id)
    if not projs:
        await q.edit_message_text(
            "📂 <b>My Projects</b>\n\nNo projects yet.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🚀 Deploy", callback_data="deploy"),
                InlineKeyboardButton("🔙 Back", callback_data="main_menu"),
            ]]))
        return
    page_items, total, page = paginate(projs, page)
    lines = [f"📂 <b>My Projects</b> ({len(projs)})\n"]
    btns = []
    for p in page_items:
        e = STATUS_EMOJI.get(p.status.value, "❓")
        lines.append(f"{e} {p.name}")
        btns.append([InlineKeyboardButton(f"{e} {p.name}", callback_data=f"proj:{p.id}")])
    nav = nav_buttons(page, total, "projects")
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
    await q.edit_message_text("\n".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(btns))


async def h_project_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user
    pid = uuid.UUID(q.data.split(":")[1])
    async with get_session() as s:
        p = await svc_get_project(s, pid, u.id)
    if not p: await q.answer("Not found.", show_alert=True); return
    e = STATUS_EMOJI.get(p.status.value, "❓")
    txt = (f"📁 <b>{p.name}</b>\n\n🆔 <code>{str(p.id)[:8]}…</code>\n"
           f"⚙️ Runtime: {p.runtime.value.upper()}\n📊 Status: {e} {p.status.value}\n"
           f"📅 Created: {ts(p.created_at)}\n"
           + (f"🐙 Repo: <code>{p.github_url}</code>\n" if p.github_url else "")
           + ("⚠️ <i>Suspended</i>\n" if p.is_suspended else ""))
    s = str(p.id)
    await q.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start", callback_data=f"pa:start:{s}"),
         InlineKeyboardButton("🔄 Restart", callback_data=f"pa:restart:{s}"),
         InlineKeyboardButton("⏹️ Stop", callback_data=f"pa:stop:{s}")],
        [InlineKeyboardButton("📜 Logs", callback_data=f"pa:logs:{s}"),
         InlineKeyboardButton("🗑️ Delete", callback_data=f"pa:del_confirm:{s}")],
        [InlineKeyboardButton("🔙 Back", callback_data="projects")],
    ]))


async def h_project_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user; action = q.data.split(":")[1]
    async with get_session() as s:
        projs = await svc_user_projects(s, u.id)
    if not projs: await q.answer("No projects.", show_alert=True); return
    btns = [[InlineKeyboardButton(
        f"{STATUS_EMOJI.get(p.status.value,'❓')} {p.name}",
        callback_data=f"pa:{action}:{p.id}")] for p in projs]
    btns.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
    await q.edit_message_text(f"Select project to <b>{action}</b>:",
                              parse_mode="HTML", reply_markup=InlineKeyboardMarkup(btns))


async def h_project_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user
    parts = q.data.split(":"); action = parts[1]; pid = uuid.UUID(parts[2])
    cfg: Config = context.bot_data["config"]
    if action == "del_confirm":
        await q.edit_message_text(
            "⚠️ <b>Delete Project?</b>\n\nThis is irreversible.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑️ Yes", callback_data=f"pa:delete:{pid}"),
                InlineKeyboardButton("❌ No", callback_data=f"proj:{pid}"),
            ]]))
        return
    if action == "logs":
        async with get_session() as s:
            p = await svc_get_project(s, pid, u.id)
            if not p: await q.answer("Not found.", show_alert=True); return
            entries = await svc_get_logs(s, pid, 20)
        db_logs = "\n".join(e.content for e in entries)
        rlog_path = Path(cfg.storage_path) / (p.storage_path or "") / "runtime.log"
        rlog = ""
        if rlog_path.exists():
            with open(rlog_path, "rb") as f:
                f.seek(max(0, os.path.getsize(rlog_path) - 3000))
                rlog = f.read().decode("utf-8", errors="replace")
        txt = (f"📜 <b>Logs: {p.name}</b>\n\n"
               f"<b>System:</b>\n<pre>{db_logs or '—'}</pre>\n\n"
               f"<b>Runtime:</b>\n<pre>{truncate(rlog or '(no output yet)', 1500)}</pre>")
        await q.edit_message_text(txt, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{pid}")]]))
        return
    async with get_session() as s:
        p = await svc_get_project(s, pid, u.id)
        if not p: await q.answer("Not found.", show_alert=True); return
        if p.is_suspended: await q.answer("Project suspended by admin.", show_alert=True); return
        if action == "start":
            ok = await svc_start_project(s, p, cfg.storage_path); msg = "🟢 Started." if ok else "❌ Failed."
        elif action == "stop":
            await svc_stop_project(s, p); msg = "🔴 Stopped."
        elif action == "restart":
            ok = await svc_restart_project(s, p, cfg.storage_path); msg = "🔄 Restarted." if ok else "❌ Failed."
        elif action == "status":
            e = STATUS_EMOJI.get(p.status.value, "❓")
            msg = f"📊 {e} {p.status.value}" + (f" (PID {p.process_pid})" if p.process_pid else "")
        elif action == "delete":
            ppath = Path(cfg.storage_path) / (p.storage_path or "")
            await svc_stop_project(s, p); await s.delete(p)
            shutil.rmtree(ppath, ignore_errors=True)
            await q.edit_message_text("🗑️ Deleted.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📂 Projects", callback_data="projects")]]))
            return
        else: msg = "Unknown action."
    await q.edit_message_text(f"<b>{p.name}</b>\n\n{msg}", parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{pid}")]]))


# ============================================================
#  HANDLERS — Credits
# ============================================================
async def h_credits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user
    async with get_session() as s:
        bal = await svc_get_balance(s, u.id)
        stats = await svc_ref_stats(s, u.id)
        link = await svc_get_ref_link(s, u.id)
    bot_info = await context.bot.get_me()
    ref_url = f"https://t.me/{bot_info.username}?start=ref_{link.code}" if link else "—"
    await q.edit_message_text(
        f"🪙 <b>Credits & Referrals</b>\n\n💰 Balance: <b>🪙 {bal:,}</b>\n"
        f"👥 Referrals: <b>{stats['total']}</b>\n\n🔗 Referral link:\n<code>{ref_url}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 Daily Reward", callback_data="cr:daily"),
             InlineKeyboardButton("🎟️ Redeem Code", callback_data="cr:redeem_start")],
            [InlineKeyboardButton("📋 History", callback_data="cr:history:0"),
             InlineKeyboardButton("🏆 Leaderboard", callback_data="cr:leaderboard")],
            [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
        ]))


async def h_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user; cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        ok = await svc_claim_daily(s, u.id, cfg.daily_reward_credits)
        bal = await svc_get_balance(s, u.id)
    if ok:
        await q.answer(f"✅ +{cfg.daily_reward_credits} credits! Balance: {bal:,}", show_alert=True)
    else:
        await q.answer("⏰ Already claimed today. Come back tomorrow!", show_alert=True)


async def h_tx_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user; page = int(q.data.split(":")[-1])
    async with get_session() as s:
        txs = await svc_get_txs(s, u.id, 100)
    items, total, page = paginate(txs, page, 8)
    lines = ["📋 <b>Transaction History</b>\n"]
    for t in items:
        sign = "+" if t.amount > 0 else ""
        lines.append(f"{sign}{t.amount:,} 🪙 — {t.type.value} — {ts(t.created_at)}")
    nav = nav_buttons(page, total, "cr:history")
    btns = ([nav] if nav else []) + [[InlineKeyboardButton("🔙 Back", callback_data="credits")]]
    await q.edit_message_text("\n".join(lines) or "No transactions.", parse_mode="HTML",
                              reply_markup=InlineKeyboardMarkup(btns))


async def h_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    async with get_session() as s:
        board = await svc_leaderboard(s)
    medals = ["🥇","🥈","🥉"] + ["🏅"]*10
    lines = ["🏆 <b>Referral Leaderboard</b>\n"]
    for i, (uid, cnt) in enumerate(board):
        lines.append(f"{medals[i]} User {uid} — {cnt}")
    await q.edit_message_text("\n".join(lines) or "No data.", parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="credits")]]))


_CR_REDEEM = 0

async def h_redeem_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    await q.edit_message_text("🎟️ Enter your code:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="credits")]]))
    return _CR_REDEEM

async def h_redeem_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    code = (update.message.text or "").strip()
    async with get_session() as s:
        ok, msg = await svc_redeem_code(s, update.effective_user.id, code)
    await update.message.reply_text(msg,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="credits")]]))
    return ConversationHandler.END

async def h_redeem_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query: await update.callback_query.answer()
    return ConversationHandler.END


# ============================================================
#  HANDLERS — Premium / Account / Support
# ============================================================
async def h_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user
    async with get_session() as s:
        plans = await svc_all_plans(s); limits = await svc_get_limits(s, u.id)
    lines = ["💎 <b>Premium Plans</b>\n"]
    if limits["is_premium"]:
        lines.append(f"✅ Current: <b>{limits['plan_name']}</b>")
        if limits["expires_at"]: lines.append(f"📅 Expires: {ts(limits['expires_at'])}")
        lines.append("")
    for p in plans:
        dur = f"{p.duration_days}d" if p.duration_days else "Permanent"
        lines += [
            f"<b>{p.display_name}</b>",
            f"  📁 {p.max_projects} projects | 📦 {p.max_file_size_mb}MB",
            f"  {'✅' if p.allows_zip else '❌'}ZIP  {'✅' if p.allows_github else '❌'}GitHub  {'✅' if p.allows_nodejs else '❌'}Node.js",
            f"  ⏱️ {dur} | 🪙 {p.price_credits:,}", "",
        ]
    lines.append("💬 Contact the owner to purchase.")
    await q.edit_message_text("\n".join(lines), parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🆘 Support", callback_data="support"),
            InlineKeyboardButton("🔙 Back", callback_data="main_menu"),
        ]]))


async def h_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    u = update.effective_user
    async with get_session() as s:
        bal = await svc_get_balance(s, u.id); limits = await svc_get_limits(s, u.id)
        count = await svc_count_user_projects(s, u.id); ref_stats = await svc_ref_stats(s, u.id)
        link = await svc_get_ref_link(s, u.id)
    bot_info = await context.bot.get_me()
    ref_url = f"https://t.me/{bot_info.username}?start=ref_{link.code}" if link else "—"
    plan_emoji = "💎" if limits["is_premium"] else "⚪"
    plan_line = f"{plan_emoji} {limits['plan_name']}"
    if limits["expires_at"]: plan_line += f" (expires {ts(limits['expires_at'])})"
    txt = (
        f"👤 <b>My Account</b>\n\n🆔 <code>{u.id}</code>\n👤 {u.full_name}\n"
        + (f"🔗 @{u.username}\n" if u.username else "")
        + f"\n{plan_line}\n🪙 Balance: <b>🪙 {bal:,}</b>\n"
        f"📁 Projects: {count}/{limits['max_projects']}\n"
        f"👥 Referrals: {ref_stats['total']}\n\n🔗 <code>{ref_url}</code>"
    )
    await q.edit_message_text(txt, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🪙 Credits", callback_data="credits"),
            InlineKeyboardButton("💎 Plans", callback_data="premium"),
        ], [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))


async def h_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    cfg: Config = context.bot_data["config"]
    await q.edit_message_text(
        f"🆘 <b>Support</b>\n\nContact the owner (ID: <code>{cfg.owner_id}</code>).\n"
        f"Include your ID: <code>{update.effective_user.id}</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]]))


# ============================================================
#  HANDLERS — Admin Panel
# ============================================================
@owner_only
async def h_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    await q.edit_message_text("⚙️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_menu())


@owner_only
async def h_admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    async with get_session() as s:
        ustats = await svc_user_stats(s); tp = await svc_count_projects(s)
        running = await svc_count_running(s); failed = await svc_count_failed_deps(s)
        tc = await svc_total_credits(s)
        pm = (await s.scalar(select(func.count(Subscription.id)).where(Subscription.is_active == True))) or 0
    txt = (
        f"📊 <b>Dashboard</b>\n\n"
        f"👥 Users: <b>{ustats['total']}</b> (Active: {ustats['active']} | Banned: {ustats['banned']})\n"
        f"💎 Premium: <b>{pm}</b>\n\n"
        f"🚀 Projects: <b>{tp}</b>  🟢 Running: <b>{running}</b>  🔴 Failed: <b>{failed}</b>\n\n"
        f"🪙 Credits distributed: <b>{tc:,}</b>"
    )
    await q.edit_message_text(txt, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))


@owner_only
async def h_admin_activity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    async with get_session() as s:
        logs = await svc_recent_admin_logs(s)
    lines = ["🛡️ <b>Activity Log</b>\n"]
    for lg in logs:
        t = f" → {lg.target_type}:{lg.target_id}" if lg.target_id else ""
        lines.append(f"[{ts(lg.created_at)}] {lg.action}{t}")
        if lg.detail: lines.append(f"  <i>{lg.detail[:80]}</i>")
    await q.edit_message_text("\n".join(lines) or "No activity.", parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))


@owner_only
async def h_admin_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    cfg: Config = context.bot_data["config"]
    txt = (
        f"⚙️ <b>Bot Settings</b>\n\n"
        f"👤 Owner ID: <code>{cfg.owner_id}</code>\n"
        f"🎁 Welcome: {cfg.welcome_credits}🪙\n"
        f"🎯 Daily: {cfg.daily_reward_credits}🪙\n"
        f"👥 Referral: {cfg.referral_reward_credits}🪙\n"
        f"📁 Free slots: {cfg.free_max_projects}\n"
        f"📦 Free max file: {cfg.free_max_file_mb}MB\n"
        f"📂 Storage: <code>{cfg.storage_path}</code>\n\n"
        f"<i>Edit env vars in Railway to change these.</i>"
    )
    await q.edit_message_text(txt, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))


# ----- Admin: Users -----
_AU_SEARCH, _AU_AMOUNT, _AU_BAN_REASON, _AU_CONFIRM = range(4)


@owner_only
async def h_admin_users_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    await q.edit_message_text("👥 Enter Telegram ID or @username:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))
    return _AU_SEARCH


@owner_only
async def h_admin_users_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    async with get_session() as s:
        users = await svc_search_users(s, (update.message.text or "").strip())
    if not users:
        await update.message.reply_text("❌ No users found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))
        return ConversationHandler.END
    btns = [[InlineKeyboardButton(
        f"{'🚫 ' if u.is_banned else ''}{u.first_name} (@{u.username or '—'}) [{u.id}]",
        callback_data=f"admin:view:{u.id}")] for u in users[:10]]
    btns.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
    await update.message.reply_text(f"Found {len(users)} user(s):", reply_markup=InlineKeyboardMarkup(btns))
    return ConversationHandler.END


@owner_only
async def h_admin_view_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    uid = int(q.data.split(":")[-1])
    async with get_session() as s:
        u = await svc_get_user(s, uid)
        if not u: await q.answer("Not found.", show_alert=True); return
        bal = await svc_get_balance(s, uid)
        sub = await svc_active_sub(s, uid)
        pc = len(await svc_user_projects(s, uid))
    plan = f"💎 {sub.plan.display_name}" if sub and sub.plan else "⚪ Free"
    txt = (
        f"👤 <b>User {uid}</b>\n\n"
        f"📝 {u.first_name} {u.last_name or ''}\n"
        + (f"🔗 @{u.username}\n" if u.username else "")
        + f"📅 Joined: {ts(u.created_at)}\n"
        f"{'🚫 BANNED' if u.is_banned else '✅ Active'}"
        + (f" — {u.ban_reason}" if u.is_banned else "")
        + f"\n\n{plan}\n🪙 {bal:,}\n📁 {pc} project(s)"
    )
    await q.edit_message_text(txt, parse_mode="HTML", reply_markup=kb_user_actions(uid))


@owner_only
async def h_admin_user_action_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    parts = q.data.split(":"); uid, action = int(parts[1]), parts[2]
    context.user_data["admin_action"] = {"uid": uid, "action": action}
    if action in ("add", "deduct"):
        label = "add to" if action == "add" else "deduct from"
        await q.edit_message_text(f"💰 Amount to {label} user <code>{uid}</code>:", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]))
        return _AU_AMOUNT
    if action == "ban":
        await q.edit_message_text(f"🚫 Ban reason for user <code>{uid}</code>:", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]))
        return _AU_BAN_REASON
    if action == "premium":
        await q.edit_message_text(
            f"💎 Grant premium to <code>{uid}</code>\nFormat: <code>plan_id [days]</code>\nEx: <code>1 30</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_action")]]))
        return _AU_AMOUNT
    if action in ("unban", "revoke_premium"):
        await q.edit_message_text(
            f"Confirm {'unban' if action == 'unban' else 'revoke premium'} for user <code>{uid}</code>?",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Confirm", callback_data="au_do"),
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"),
            ]]))
        return _AU_CONFIRM
    if action == "projects":
        async with get_session() as s:
            projs = await svc_user_projects(s, uid)
        lines = [f"📂 <b>Projects for {uid}</b>\n"]
        for p in projs:
            lines.append(f"{STATUS_EMOJI.get(p.status.value,'❓')} {p.name}")
        await q.edit_message_text("\n".join(lines) or "No projects.", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"admin:view:{uid}")]]))
        return ConversationHandler.END
    return ConversationHandler.END


@owner_only
async def h_admin_user_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    data = context.user_data.get("admin_action", {}); uid = data["uid"]; action = data["action"]
    txt = (update.message.text or "").strip(); cfg: Config = context.bot_data["config"]
    if action in ("add", "deduct"):
        try:
            amount = int(txt)
            if amount <= 0: raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Enter a positive integer."); return _AU_AMOUNT
        async with get_session() as s:
            try:
                if action == "add":
                    nb = await svc_add_credits(s, uid, amount, TransactionType.admin_add, "Admin add", cfg.owner_id)
                else:
                    nb = await svc_deduct_credits(s, uid, amount, TransactionType.admin_deduct, "Admin deduct", cfg.owner_id)
                await svc_log_action(s, cfg.owner_id, f"credits_{action}", "user", str(uid), f"amount={amount}")
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}"); return ConversationHandler.END
        await update.message.reply_text(f"✅ {'+'if action=='add' else '-'}{amount:,} for {uid}. Balance: {nb:,}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 User", callback_data=f"admin:view:{uid}")]]))
    elif action == "premium":
        parts = txt.split()
        try:
            plan_id = int(parts[0]); days = int(parts[1]) if len(parts) > 1 else None
        except (ValueError, IndexError):
            await update.message.reply_text("❌ Format: plan_id [days]"); return _AU_AMOUNT
        async with get_session() as s:
            await svc_grant_premium(s, uid, plan_id, cfg.owner_id, days)
            await svc_log_action(s, cfg.owner_id, "grant_premium", "user", str(uid))
        await update.message.reply_text(f"✅ Premium granted to {uid}.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 User", callback_data=f"admin:view:{uid}")]]))
    context.user_data.pop("admin_action", None)
    return ConversationHandler.END


@owner_only
async def h_admin_user_ban_reason(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    reason = (update.message.text or "").strip()
    data = context.user_data.pop("admin_action", {}); uid = data["uid"]; cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        await svc_ban(s, uid, reason)
        await svc_log_action(s, cfg.owner_id, "ban_user", "user", str(uid), reason)
    await update.message.reply_text(f"🚫 User {uid} banned.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 User", callback_data=f"admin:view:{uid}")]]))
    return ConversationHandler.END


@owner_only
async def h_admin_user_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    data = context.user_data.pop("admin_action", {}); uid = data["uid"]; action = data["action"]
    cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        if action == "unban": await svc_unban(s, uid)
        elif action == "revoke_premium": await svc_revoke_premium(s, uid)
        await svc_log_action(s, cfg.owner_id, action, "user", str(uid))
    await q.edit_message_text(f"✅ Done.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 User", callback_data=f"admin:view:{uid}")]]))
    return ConversationHandler.END


async def h_admin_cancel_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("admin_action", None)
    q = update.callback_query
    if q: await q.answer(); await q.edit_message_text("⚙️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_menu())
    return ConversationHandler.END


# ----- Admin: Force Join -----
_FJ_INPUT = 0


@owner_only
async def h_fj_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    await q.edit_message_text("🔐 <b>Force Join Settings</b>", parse_mode="HTML", reply_markup=kb_fj_menu())


@owner_only
async def h_fj_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    async with get_session() as s:
        chs = await svc_list_channels(s)
    lines = ["📋 <b>Required Channels</b>\n"]
    for ch in chs:
        lines.append(f"{'✅' if ch.is_enabled else '❌'} <b>{ch.display_name}</b> [{ch.chat_id}]"
                     + (f" @{ch.username}" if ch.username else ""))
    await q.edit_message_text("\n".join(lines) or "None configured.", parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:fj")]]))


@owner_only
async def h_fj_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    await q.edit_message_text(
        "➕ <b>Add Channel</b>\n\nFormat: <code>CHAT_ID Display Name [@username]</code>\n"
        "Example: <code>-1001234567890 My Channel @mychan</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin:fj")]]))
    return _FJ_INPUT


@owner_only
async def h_fj_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    parts = (update.message.text or "").strip().split(None, 3)
    if len(parts) < 2:
        await update.message.reply_text("❌ Need at least CHAT_ID and Display Name. Try again:"); return _FJ_INPUT
    try:
        chat_id = int(parts[0])
    except ValueError:
        await update.message.reply_text("❌ Chat ID must be an integer. Try again:"); return _FJ_INPUT
    display_name = parts[1]; username = None
    for p in parts[2:]:
        if p.startswith("@"): username = p.lstrip("@")
        else: display_name += " " + p
    cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        await svc_add_channel(s, chat_id, display_name, username)
        await svc_log_action(s, cfg.owner_id, "fj_add", "channel", str(chat_id), display_name)
    await update.message.reply_text(f"✅ <b>{display_name}</b> added.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:fj")]]))
    return ConversationHandler.END


@owner_only
async def h_fj_remove_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    async with get_session() as s:
        chs = await svc_list_channels(s)
    if not chs: await q.answer("No channels.", show_alert=True); return
    btns = [[InlineKeyboardButton(f"🗑️ {ch.display_name}", callback_data=f"admin:fj:del:{ch.id}")] for ch in chs]
    btns.append([InlineKeyboardButton("🔙 Back", callback_data="admin:fj")])
    await q.edit_message_text("Select channel to remove:", reply_markup=InlineKeyboardMarkup(btns))


@owner_only
async def h_fj_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    ch_id = int(q.data.split(":")[-1]); cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        ok = await svc_remove_channel(s, ch_id)
        if ok: await svc_log_action(s, cfg.owner_id, "fj_remove", "channel", str(ch_id))
    await q.edit_message_text("✅ Removed." if ok else "❌ Not found.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:fj")]]))


async def h_fj_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if q: await q.answer(); await q.edit_message_text("🔐 <b>Force Join Settings</b>", parse_mode="HTML", reply_markup=kb_fj_menu())
    elif update.message: await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ----- Admin: Broadcast -----
_BC_MSG, _BC_PREVIEW = range(2)


@owner_only
async def h_bc_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    await q.edit_message_text(
        "📢 <b>Broadcast</b>\n\nSend the message (supports HTML).",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]]))
    return _BC_MSG


@owner_only
async def h_bc_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    txt = update.message.text or ""; photo = None
    if update.message.photo:
        photo = update.message.photo[-1].file_id; txt = update.message.caption or ""
    context.user_data["broadcast"] = {"text": txt, "photo": photo}
    await update.message.reply_text(
        f"📢 <b>Preview</b>\n\n{'[Photo]\n' if photo else ''}{txt[:500]}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Send to All", callback_data="bc_send"),
            InlineKeyboardButton("❌ Cancel", callback_data="admin_panel"),
        ]]))
    return _BC_PREVIEW


@owner_only
async def h_bc_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    data = context.user_data.pop("broadcast", {}); cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        job = BroadcastJob(created_by=cfg.owner_id, message_text=data["text"],
                           photo_file_id=data.get("photo"), status=BroadcastStatus.pending)
        s.add(job); await s.flush(); jid = job.id
        await svc_log_action(s, cfg.owner_id, "broadcast_start")
    asyncio.create_task(svc_run_broadcast(context.bot, jid))
    await q.edit_message_text("📢 Broadcast queued!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin", callback_data="admin_panel")]]))
    return ConversationHandler.END


async def h_bc_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("broadcast", None)
    if update.callback_query: await update.callback_query.answer()
    return ConversationHandler.END


# ----- Admin: Redeem Codes -----
_RD_INPUT = 0


@owner_only
async def h_redeem_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    async with get_session() as s:
        codes = await svc_list_codes(s)
    lines = ["🎟️ <b>Redeem Codes</b>\n"]
    for c in codes[:15]:
        uses = f"{c.used_count}/{c.max_uses}" if c.max_uses else f"{c.used_count}/∞"
        lines.append(f"<code>{c.code}</code> — {c.credit_amount}🪙 — {uses}")
    await q.edit_message_text("\n".join(lines) or "No codes.", parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Generate", callback_data="admin:redeem:gen")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ]))


@owner_only
async def h_redeem_gen_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query; await q.answer()
    await q.edit_message_text(
        "🎟️ Format: <code>credits [max_uses]</code>\n"
        "Ex: <code>100</code> or <code>50 10</code> or <code>200 0</code> (unlimited)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin:redeem")]]))
    return _RD_INPUT


@owner_only
async def h_redeem_gen_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    parts = (update.message.text or "").split()
    try:
        amount = int(parts[0]); max_uses = int(parts[1]) if len(parts) > 1 else 1
        if max_uses == 0: max_uses = None
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Format: credits [max_uses]"); return _RD_INPUT
    cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        code = await svc_create_code(s, cfg.owner_id, amount, max_uses)
        await svc_log_action(s, cfg.owner_id, "create_redeem_code", detail=f"amount={amount}")
    uses_str = str(max_uses) if max_uses else "unlimited"
    await update.message.reply_text(
        f"✅ <code>{code.code}</code>\n💰 {amount}🪙 | {uses_str} use(s)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:redeem")]]))
    return ConversationHandler.END


async def h_redeem_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query: await update.callback_query.answer()
    return ConversationHandler.END


# ----- Admin: Hosting -----
@owner_only
async def h_admin_hosting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    parts = q.data.split(":"); page = int(parts[-1]) if len(parts) > 2 and parts[-1].isdigit() else 0
    async with get_session() as s:
        projs = await svc_all_projects(s, 200); total = await svc_count_projects(s)
    items, tp, page = paginate(projs, page, 8)
    btns = [[InlineKeyboardButton(
        f"{STATUS_EMOJI.get(p.status.value,'❓')} {p.name} [{p.owner_id}]",
        callback_data=f"admin:aproj:{p.id}")] for p in items]
    nav = nav_buttons(page, tp, "admin:hosting:p")
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
    await q.edit_message_text(f"🚀 <b>All Projects</b> ({total})", parse_mode="HTML",
                              reply_markup=InlineKeyboardMarkup(btns))


@owner_only
async def h_admin_project_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    pid = uuid.UUID(q.data.split(":")[-1])
    async with get_session() as s:
        p = await svc_get_project(s, pid)
    if not p: await q.answer("Not found.", show_alert=True); return
    e = STATUS_EMOJI.get(p.status.value, "❓"); s = str(p.id)
    txt = (f"🚀 <b>{p.name}</b>\n\nOwner: {p.owner_id}\n"
           f"Status: {e} {p.status.value}\nRuntime: {p.runtime.value}\n"
           f"PID: {p.process_pid or '—'}\nCreated: {ts(p.created_at)}\n"
           + ("⚠️ Suspended\n" if p.is_suspended else ""))
    await q.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start", callback_data=f"admin:apa:start:{s}"),
         InlineKeyboardButton("🔄 Restart", callback_data=f"admin:apa:restart:{s}"),
         InlineKeyboardButton("⏹️ Stop", callback_data=f"admin:apa:stop:{s}")],
        [InlineKeyboardButton("🔇 Suspend", callback_data=f"admin:apa:suspend:{s}"),
         InlineKeyboardButton("✅ Unsuspend", callback_data=f"admin:apa:unsuspend:{s}")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin:hosting")],
    ]))


@owner_only
async def h_admin_proj_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    parts = q.data.split(":"); action = parts[2]; pid = uuid.UUID(parts[3])
    cfg: Config = context.bot_data["config"]
    async with get_session() as s:
        p = await svc_get_project(s, pid)
        if not p: await q.answer("Not found.", show_alert=True); return
        if action == "start": ok = await svc_start_project(s, p, cfg.storage_path); msg = "Started." if ok else "Failed."
        elif action == "stop": await svc_stop_project(s, p); msg = "Stopped."
        elif action == "restart": ok = await svc_restart_project(s, p, cfg.storage_path); msg = "Restarted." if ok else "Failed."
        elif action == "suspend": p.is_suspended = True; await svc_stop_project(s, p); msg = "Suspended."
        elif action == "unsuspend": p.is_suspended = False; msg = "Unsuspended."
        else: msg = "?"
        await svc_log_action(s, cfg.owner_id, f"admin_{action}", "project", str(pid))
    await q.edit_message_text(f"✅ {msg}",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"admin:aproj:{pid}")]]))


@owner_only
async def h_admin_premium_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    async with get_session() as s:
        plans = await svc_all_plans(s)
    lines = ["💎 <b>Premium Management</b>\n<b>Plans:</b>"]
    for p in plans:
        dur = f"{p.duration_days}d" if p.duration_days else "Permanent"
        lines.append(f"  ID:{p.id} — {p.display_name} — {dur} — {p.price_credits}🪙")
    lines += ["", "Use <b>👥 Users</b> → search → Grant/Revoke Premium to manage users."]
    await q.edit_message_text("\n".join(lines), parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Go to Users", callback_data="admin:users")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ]))


# ============================================================
#  ERROR HANDLER
# ============================================================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Update error:", exc_info=context.error)
    if isinstance(update, Update):
        msg = update.message or (update.callback_query.message if update.callback_query else None)
        if msg:
            try:
                await msg.reply_text("❌ An internal error occurred. Please try again.")
            except Exception:
                pass


# ============================================================
#  POST_INIT — seed plans
# ============================================================
async def post_init(app: Application):
    cfg: Config = app.bot_data["config"]
    await create_tables()
    async with get_session() as s:
        if not (await s.execute(select(Plan).limit(1))).scalar_one_or_none():
            s.add(Plan(name="premium_basic", display_name="⭐ Premium Basic",
                       max_projects=5, max_file_size_mb=10, max_storage_mb=100,
                       allows_zip=True, allows_github=True, allows_nodejs=True,
                       allows_multi_file=True, allows_env_vars=True, duration_days=30,
                       price_credits=500, is_active=True))
            s.add(Plan(name="premium_pro", display_name="🔥 Premium Pro",
                       max_projects=20, max_file_size_mb=50, max_storage_mb=500,
                       allows_zip=True, allows_github=True, allows_nodejs=True,
                       allows_multi_file=True, allows_env_vars=True, duration_days=None,
                       price_credits=2000, is_active=True))
    Path(cfg.storage_path).mkdir(parents=True, exist_ok=True)
    log.info("SODO HOSTING started. Owner: %d", cfg.owner_id)


# ============================================================
#  BUILD APPLICATION
# ============================================================
def build_app() -> Application:
    cfg = Config.from_env()
    init_db(cfg.database_url)
    app = ApplicationBuilder().token(cfg.bot_token).post_init(post_init).build()
    app.bot_data["config"] = cfg
    app.add_error_handler(error_handler)

    # --- Deploy ConversationHandler ---
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(h_deploy_entry, pattern="^deploy$")],
        states={
            _DTYPE: [CallbackQueryHandler(h_dtype, pattern="^dt:")],
            _NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_name)],
            _UPLOAD: [MessageHandler(filters.Document.ALL, h_upload)],
            _RUNTIME: [CallbackQueryHandler(h_runtime, pattern="^rt:")],
            _GITHUB: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_github)],
            _ENVVARS: [MessageHandler(filters.TEXT, h_envvars)],
            _CONFIRM: [CallbackQueryHandler(h_deploy_confirm, pattern="^deploy_(go|cancel)$")],
        },
        fallbacks=[CallbackQueryHandler(h_deploy_cancel, pattern="^cancel_deploy$"),
                   CommandHandler("cancel", h_deploy_cancel)],
        per_user=True, per_chat=True,
    ))

    # --- Credits redeem ConversationHandler ---
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(h_redeem_start, pattern="^cr:redeem_start$")],
        states={_CR_REDEEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_redeem_input)]},
        fallbacks=[CallbackQueryHandler(h_redeem_cancel, pattern="^credits$"),
                   CommandHandler("cancel", h_redeem_cancel)],
        per_user=True, per_chat=True,
    ))

    # --- Admin: Users ConversationHandler ---
    app.add_handler(ConversationHandler(
        entry_points=[
            CallbackQueryHandler(h_admin_users_entry, pattern="^admin:users$"),
            CallbackQueryHandler(h_admin_user_action_entry, pattern=r"^au:\d+:"),
        ],
        states={
            _AU_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_admin_users_search)],
            _AU_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_admin_user_amount)],
            _AU_BAN_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_admin_user_ban_reason)],
            _AU_CONFIRM: [CallbackQueryHandler(h_admin_user_confirm, pattern="^au_do$")],
        },
        fallbacks=[CallbackQueryHandler(h_admin_cancel_action, pattern="^cancel_action$"),
                   CommandHandler("cancel", h_admin_cancel_action)],
        per_user=True, per_chat=True,
    ))

    # --- Admin: Force Join ConversationHandler ---
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(h_fj_add_start, pattern="^admin:fj:add$")],
        states={_FJ_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_fj_input)]},
        fallbacks=[CallbackQueryHandler(h_fj_cancel, pattern="^admin:fj$"),
                   CommandHandler("cancel", h_fj_cancel)],
        per_user=True, per_chat=True,
    ))

    # --- Admin: Broadcast ConversationHandler ---
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(h_bc_entry, pattern="^admin:broadcast$")],
        states={
            _BC_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_bc_msg),
                      MessageHandler(filters.PHOTO, h_bc_msg)],
            _BC_PREVIEW: [CallbackQueryHandler(h_bc_confirm, pattern="^bc_send$")],
        },
        fallbacks=[CallbackQueryHandler(h_bc_cancel, pattern="^admin_panel$"),
                   CommandHandler("cancel", h_bc_cancel)],
        per_user=True, per_chat=True,
    ))

    # --- Admin: Redeem Codes ConversationHandler ---
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(h_redeem_gen_start, pattern="^admin:redeem:gen$")],
        states={_RD_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_redeem_gen_input)]},
        fallbacks=[CallbackQueryHandler(h_redeem_back, pattern="^admin:redeem$"),
                   CommandHandler("cancel", h_redeem_back)],
        per_user=True, per_chat=True,
    ))

    # --- Static callbacks ---
    for pattern, handler in [
        ("^start$|^/start", h_start),
        ("^verify_join$", h_verify_join),
        ("^main_menu$", h_main_menu),
        (r"^projects(:\d+)?$", h_projects),
        (r"^proj:[0-9a-f\-]+$", h_project_detail),
        (r"^pp:", h_project_pick),
        (r"^pa:", h_project_action),
        ("^credits$", h_credits),
        ("^cr:daily$", h_daily),
        (r"^cr:history:\d+$", h_tx_history),
        ("^cr:leaderboard$", h_leaderboard),
        ("^premium$", h_premium),
        ("^account$", h_account),
        ("^support$", h_support),
        ("^admin_panel$", h_admin_panel),
        ("^admin:dash$", h_admin_dashboard),
        ("^admin:activity$", h_admin_activity),
        ("^admin:settings$", h_admin_settings),
        ("^admin:fj$", h_fj_menu),
        ("^admin:fj:list$", h_fj_list),
        ("^admin:fj:remove_list$", h_fj_remove_list),
        (r"^admin:fj:del:\d+$", h_fj_delete),
        ("^admin:redeem$", h_redeem_menu),
        (r"^admin:hosting", h_admin_hosting),
        (r"^admin:aproj:[0-9a-f\-]+$", h_admin_project_detail),
        (r"^admin:apa:", h_admin_proj_action),
        ("^admin:premium$", h_admin_premium_menu),
        (r"^admin:view:\d+$", h_admin_view_user),
        ("^cancel_action$", h_admin_cancel_action),
        ("^noop$", lambda u, c: u.callback_query.answer()),
    ]:
        app.add_handler(CallbackQueryHandler(handler, pattern=pattern))

    app.add_handler(CommandHandler("start", h_start))
    return app


# ============================================================
#  ENTRY POINT
# ============================================================
def main():
    app = build_app()
    log.info("Polling started…")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
