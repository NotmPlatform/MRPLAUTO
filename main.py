import html
import io
import logging
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import psycopg
from fastapi import FastAPI, HTTPException, Request
from psycopg.rows import dict_row
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BASE_URL = os.getenv("BASE_URL", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
PORT = int(os.getenv("PORT", "8080"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
MODERATION_CHAT_ID = int(os.getenv("MODERATION_CHAT_ID", "0"))
PARTNERSHIP_CHAT_ID = int(os.getenv("PARTNERSHIP_CHAT_ID", str(MODERATION_CHAT_ID or 0)))
ADMIN_USER_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_USER_IDS", "").split(",")
    if x.strip().isdigit()
}
PAYMENT_TEXT = os.getenv(
    "PAYMENT_TEXT",
    "ÐÐ¿Ð»Ð°ÑÐ¸ÑÐµ ÑÑÐ°ÑÑÐ¸Ðµ Ð¿Ð¾ Ð²Ð°ÑÐ¸Ð¼ ÑÐµÐºÐ²Ð¸Ð·Ð¸ÑÐ°Ð¼. ÐÐ¾ÑÐ»Ðµ Ð¾Ð¿Ð»Ð°ÑÑ Ð½Ð°Ð¶Ð¼Ð¸ÑÐµ ÐºÐ½Ð¾Ð¿ÐºÑ Â«Ð¯ Ð¾Ð¿Ð»Ð°ÑÐ¸Ð»Â».",
)
TIMEZONE_LABEL = os.getenv("TIMEZONE_LABEL", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not BASE_URL:
    raise RuntimeError("BASE_URL is required")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

(
    PROFILE_NAME,
    PROFILE_AGE,
    PROFILE_GENDER,
    PROFILE_CITY,
    PROFILE_PHONE,
) = range(5)
PARTNER_PROPOSAL, PARTNER_PHONE = range(20, 22)
(
    EVENT_TITLE,
    EVENT_DATE,
    EVENT_TIME,
    EVENT_LOCATION,
    EVENT_PRICE,
    EVENT_DESCRIPTION,
    EVENT_LIMIT,
    EVENT_BALANCE,
    EVENT_PHOTO,
) = range(100, 109)
EDIT_EVENT_VALUE = 140

ACTIVE_REGISTRATION_STATUSES = ("waiting_payment", "waiting_moderation", "approved")
PARTICIPANT_STATUSES = ("approved",)
GENDER_MAP = {
    "ÐÑÐ¶ÑÐºÐ¾Ð¹": "male",
    "ÐÐµÐ½ÑÐºÐ¸Ð¹": "female",
}
GENDER_LABELS = {
    "male": "Ð",
    "female": "Ð",
}
GENDER_FULL_LABELS = {
    "male": "ÐÑÐ¶ÑÐºÐ¾Ð¹",
    "female": "ÐÐµÐ½ÑÐºÐ¸Ð¹",
}


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db() -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                age INTEGER,
                gender TEXT,
                city TEXT,
                phone TEXT,
                profile_completed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id BIGSERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                event_date DATE NOT NULL,
                event_time TEXT NOT NULL,
                location TEXT NOT NULL,
                price NUMERIC(10, 2) NOT NULL DEFAULT 0,
                description TEXT,
                total_limit INTEGER NOT NULL,
                gender_balance_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                male_limit INTEGER,
                female_limit INTEGER,
                photo_file_id TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS registrations (
                id BIGSERIAL PRIMARY KEY,
                event_id BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
                name_snapshot TEXT NOT NULL,
                age_snapshot INTEGER NOT NULL,
                gender_snapshot TEXT NOT NULL,
                city_snapshot TEXT NOT NULL,
                phone_snapshot TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'waiting_payment',
                payment_status TEXT NOT NULL DEFAULT 'not_paid',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                moderated_at TIMESTAMPTZ
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS partner_inquiries (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                username TEXT,
                telegram_name TEXT,
                proposal_text TEXT NOT NULL,
                contact_phone TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS photo_file_id TEXT;")
        cur.execute(
            "ALTER TABLE events ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_reg_event_status ON registrations(event_id, status);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_reg_tg_event ON registrations(telegram_id, event_id);"
        )
        conn.commit()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def normalize_phone(phone: str) -> Optional[str]:
    raw = (phone or "").strip().replace(" ", "")
    digits = re.sub(r"\D", "", raw)
    if len(digits) < 10 or len(digits) > 15:
        return None
    if raw.startswith("8") and len(digits) == 11:
        return "+7" + digits[1:]
    if raw.startswith("+"):
        return "+" + digits
    return "+" + digits


def recalc_gender_limits(total_limit: int, enabled: bool) -> tuple[Optional[int], Optional[int]]:
    if not enabled:
        return None, None
    return total_limit // 2, total_limit // 2


def upsert_user_profile(
    telegram_id: int,
    username: Optional[str],
    full_name: str,
    age: int,
    gender: str,
    city: str,
    phone: str,
) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (telegram_id, username, full_name, age, gender, city, phone, profile_completed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (telegram_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                full_name = EXCLUDED.full_name,
                age = EXCLUDED.age,
                gender = EXCLUDED.gender,
                city = EXCLUDED.city,
                phone = EXCLUDED.phone,
                profile_completed = TRUE,
                updated_at = NOW();
            """,
            (telegram_id, username, full_name, age, gender, city, phone),
        )
        conn.commit()


def get_user_profile(telegram_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
        return cur.fetchone()


def create_partner_inquiry(telegram_id: int, username: Optional[str], telegram_name: str, proposal_text: str, contact_phone: str) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO partner_inquiries (telegram_id, username, telegram_name, proposal_text, contact_phone)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (telegram_id, username, telegram_name, proposal_text, contact_phone),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row["id"])


def create_event(
    title: str,
    event_date: str,
    event_time: str,
    location: str,
    price: Decimal,
    description: str,
    total_limit: int,
    gender_balance_enabled: bool,
    photo_file_id: Optional[str],
) -> int:
    male_limit, female_limit = recalc_gender_limits(total_limit, gender_balance_enabled)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (
                title, event_date, event_time, location, price, description, total_limit,
                gender_balance_enabled, male_limit, female_limit, photo_file_id, status, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'upcoming', NOW())
            RETURNING id;
            """,
            (
                title,
                event_date,
                event_time,
                location,
                price,
                description,
                total_limit,
                gender_balance_enabled,
                male_limit,
                female_limit,
                photo_file_id,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row["id"])


def update_event_field(event_id: int, field: str, value) -> None:
    allowed = {
        "title",
        "event_date",
        "event_time",
        "location",
        "price",
        "description",
        "total_limit",
        "photo_file_id",
    }
    if field not in allowed:
        raise ValueError("Unsupported field")
    with get_conn() as conn, conn.cursor() as cur:
        if field == "total_limit":
            cur.execute("SELECT gender_balance_enabled FROM events WHERE id = %s", (event_id,))
            row = cur.fetchone()
            if row and row["gender_balance_enabled"]:
                male_limit, female_limit = recalc_gender_limits(int(value), True)
                cur.execute(
                    """
                    UPDATE events
                    SET total_limit = %s, male_limit = %s, female_limit = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (int(value), male_limit, female_limit, event_id),
                )
            else:
                cur.execute(
                    "UPDATE events SET total_limit = %s, updated_at = NOW() WHERE id = %s",
                    (int(value), event_id),
                )
        else:
            cur.execute(
                f"UPDATE events SET {field} = %s, updated_at = NOW() WHERE id = %s",
                (value, event_id),
            )
        conn.commit()


def toggle_event_gender_balance(event_id: int) -> tuple[bool, str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT total_limit, gender_balance_enabled FROM events WHERE id = %s", (event_id,))
        row = cur.fetchone()
        if not row:
            return False, "ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾."
        total_limit = int(row["total_limit"])
        enabled = bool(row["gender_balance_enabled"])
        if not enabled and total_limit % 2 != 0:
            return False, "ÐÐµÐ»ÑÐ·Ñ Ð²ÐºÐ»ÑÑÐ¸ÑÑ 50/50 Ð¿ÑÐ¸ Ð½ÐµÑÐµÑÐ½Ð¾Ð¼ Ð»Ð¸Ð¼Ð¸ÑÐµ. Ð¡Ð½Ð°ÑÐ°Ð»Ð° ÑÐ´ÐµÐ»Ð°Ð¹ÑÐµ Ð»Ð¸Ð¼Ð¸Ñ ÑÐµÑÐ½ÑÐ¼."
        new_enabled = not enabled
        male_limit, female_limit = recalc_gender_limits(total_limit, new_enabled)
        cur.execute(
            """
            UPDATE events
            SET gender_balance_enabled = %s,
                male_limit = %s,
                female_limit = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (new_enabled, male_limit, female_limit, event_id),
        )
        conn.commit()
        return True, "50/50 Ð²ÐºÐ»ÑÑÐµÐ½." if new_enabled else "50/50 Ð²ÑÐºÐ»ÑÑÐµÐ½."


def list_events(limit: int = 30):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM events
            ORDER BY
                CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                event_date ASC,
                created_at ASC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def get_event(event_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM events WHERE id = %s", (event_id,))
        return cur.fetchone()


def get_active_event():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM events
            WHERE status = 'active'
            ORDER BY event_date ASC, created_at ASC
            LIMIT 1
            """
        )
        return cur.fetchone()


def set_event_status(event_id: int, new_status: str) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        if new_status == "active":
            cur.execute("UPDATE events SET status = 'upcoming', updated_at = NOW() WHERE status = 'active'")
        cur.execute(
            "UPDATE events SET status = %s, updated_at = NOW() WHERE id = %s",
            (new_status, event_id),
        )
        conn.commit()


def get_event_stats(event_id: int) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status IN ('waiting_payment', 'waiting_moderation', 'approved')) AS active_total,
                COUNT(*) FILTER (WHERE status = 'waiting_payment') AS waiting_payment_count,
                COUNT(*) FILTER (WHERE status = 'waiting_moderation') AS waiting_moderation_count,
                COUNT(*) FILTER (WHERE status = 'approved' AND payment_status = 'paid') AS confirmed_paid_count,
                COUNT(*) FILTER (WHERE status = 'approved') AS approved_count,
                COUNT(*) FILTER (WHERE status IN ('waiting_payment', 'waiting_moderation', 'approved') AND gender_snapshot = 'male') AS active_male,
                COUNT(*) FILTER (WHERE status IN ('waiting_payment', 'waiting_moderation', 'approved') AND gender_snapshot = 'female') AS active_female
            FROM registrations
            WHERE event_id = %s
            """,
            (event_id,),
        )
        row = cur.fetchone() or {}
        return {k: int(v or 0) for k, v in row.items()}


def get_latest_registration_for_user_event(telegram_id: int, event_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM registrations
            WHERE telegram_id = %s AND event_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (telegram_id, event_id),
        )
        return cur.fetchone()


def count_active_registrations(event_id: int) -> int:
    return get_event_stats(event_id)["active_total"]


def count_active_registrations_by_gender(event_id: int, gender: str) -> int:
    stats = get_event_stats(event_id)
    return stats["active_male"] if gender == "male" else stats["active_female"]


def list_confirmed_participants(event_id: int, gender: Optional[str] = None):
    with get_conn() as conn, conn.cursor() as cur:
        if gender:
            cur.execute(
                """
                SELECT name_snapshot, gender_snapshot, phone_snapshot
                FROM registrations
                WHERE event_id = %s
                  AND status = 'approved'
                  AND payment_status = 'paid'
                  AND gender_snapshot = %s
                ORDER BY gender_snapshot, name_snapshot
                """,
                (event_id, gender),
            )
        else:
            cur.execute(
                """
                SELECT name_snapshot, gender_snapshot, phone_snapshot
                FROM registrations
                WHERE event_id = %s
                  AND status = 'approved'
                  AND payment_status = 'paid'
                ORDER BY gender_snapshot, name_snapshot
                """,
                (event_id,),
            )
        return cur.fetchall()


def check_slot_available(event_row, gender: str) -> tuple[bool, str]:
    total_used = count_active_registrations(event_row["id"])
    if total_used >= event_row["total_limit"]:
        return False, "Ð¡Ð²Ð¾Ð±Ð¾Ð´Ð½ÑÑ Ð¼ÐµÑÑ Ð½Ð° ÑÑÐ¾ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð±Ð¾Ð»ÑÑÐµ Ð½ÐµÑ."

    if event_row["gender_balance_enabled"]:
        used_for_gender = count_active_registrations_by_gender(event_row["id"], gender)
        limit_for_gender = event_row["male_limit"] if gender == "male" else event_row["female_limit"]
        if limit_for_gender is not None and used_for_gender >= limit_for_gender:
            label = GENDER_FULL_LABELS.get(gender, "ÑÑÐ¾Ð¹ ÐºÐ°ÑÐµÐ³Ð¾ÑÐ¸Ð¸")
            return False, f"ÐÐµÑÑÐ° Ð´Ð»Ñ ÐºÐ°ÑÐµÐ³Ð¾ÑÐ¸Ð¸ Â«{label}Â» ÑÐ¶Ðµ Ð·Ð°ÐºÐ¾Ð½ÑÐ¸Ð»Ð¸ÑÑ."

    return True, ""


def create_registration_from_profile(event_row, user_row) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO registrations (
                event_id, telegram_id, name_snapshot, age_snapshot, gender_snapshot,
                city_snapshot, phone_snapshot, status, payment_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'waiting_payment', 'not_paid')
            RETURNING id;
            """,
            (
                event_row["id"],
                user_row["telegram_id"],
                user_row["full_name"],
                user_row["age"],
                user_row["gender"],
                user_row["city"],
                user_row["phone"],
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row["id"])


def get_registration(registration_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            WHERE r.id = %s
            """,
            (registration_id,),
        )
        return cur.fetchone()


def update_registration_status(registration_id: int, status: str, payment_status: Optional[str] = None) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        if payment_status is None:
            cur.execute(
                """
                UPDATE registrations
                SET status = %s,
                    moderated_at = CASE WHEN %s IN ('approved', 'rejected') THEN NOW() ELSE moderated_at END
                WHERE id = %s
                """,
                (status, status, registration_id),
            )
        else:
            cur.execute(
                """
                UPDATE registrations
                SET status = %s,
                    payment_status = %s,
                    moderated_at = CASE WHEN %s IN ('approved', 'rejected') THEN NOW() ELSE moderated_at END
                WHERE id = %s
                """,
                (status, payment_status, status, registration_id),
            )
        conn.commit()


def format_price(value) -> str:
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        return str(int(value)) if value == value.to_integral() else str(value)
    return str(value)


def render_event_text(event_row, include_stats: bool = False) -> str:
    balance = "ÐÐºÐ»ÑÑÐµÐ½" if event_row["gender_balance_enabled"] else "ÐÑÐºÐ»ÑÑÐµÐ½"
    extra = ""
    if event_row["gender_balance_enabled"]:
        extra = f"\nÐÐ°Ð»Ð°Ð½Ñ Ð/Ð: {event_row['male_limit']}/{event_row['female_limit']}"
    description = html.escape(event_row["description"] or "")
    lines = [
        f"<b>{html.escape(event_row['title'])}</b>",
        f"ÐÐ°ÑÐ°: {event_row['event_date']}",
        f"ÐÑÐµÐ¼Ñ: {html.escape(event_row['event_time'])}",
        f"ÐÐµÑÑÐ¾: {html.escape(event_row['location'])}",
        f"Ð¦ÐµÐ½Ð°: {format_price(event_row['price'])}",
        f"Ð¡ÑÐ°ÑÑÑ: {html.escape(event_row['status'])}",
        f"ÐÐ¸Ð¼Ð¸Ñ: {event_row['total_limit']}",
        f"50/50: {balance}{extra}",
    ]
    if description:
        lines.append(f"ÐÐ¿Ð¸ÑÐ°Ð½Ð¸Ðµ: {description}")
    if include_stats:
        stats = get_event_stats(event_row["id"])
        lines.extend(
            [
                "",
                "<b>Ð¡ÑÐ°ÑÐ¸ÑÑÐ¸ÐºÐ°</b>",
                f"ÐÐºÑÐ¸Ð²Ð½ÑÑ Ð·Ð°ÑÐ²Ð¾Ðº: {stats['active_total']}",
                f"ÐÐ¶Ð¸Ð´Ð°ÑÑ Ð¾Ð¿Ð»Ð°ÑÑ: {stats['waiting_payment_count']}",
                f"ÐÐ° Ð¼Ð¾Ð´ÐµÑÐ°ÑÐ¸Ð¸: {stats['waiting_moderation_count']}",
                f"ÐÐ¾Ð´ÑÐ²ÐµÑÐ¶Ð´ÐµÐ½Ð¾: {stats['approved_count']}",
                f"ÐÐ¾Ð´ÑÐ²ÐµÑÐ¶Ð´ÐµÐ½Ð¾ Ð¸ Ð¾Ð¿Ð»Ð°ÑÐµÐ½Ð¾: {stats['confirmed_paid_count']}",
                f"Ð / Ð Ð² Ð°ÐºÑÐ¸Ð²Ð½ÑÑ: {stats['active_male']} / {stats['active_female']}",
            ]
        )
    return "\n".join(lines)


def render_profile_text(user_row) -> str:
    return (
        "<b>ÐÐ°ÑÐ° Ð°Ð½ÐºÐµÑÐ°</b>\n"
        f"ÐÐ¼Ñ: {html.escape(user_row['full_name'])}\n"
        f"ÐÐ¾Ð·ÑÐ°ÑÑ: {user_row['age']}\n"
        f"ÐÐ¾Ð»: {GENDER_FULL_LABELS.get(user_row['gender'], user_row['gender'])}\n"
        f"ÐÐ¾ÑÐ¾Ð´: {html.escape(user_row['city'])}\n"
        f"Ð¢ÐµÐ»ÐµÑÐ¾Ð½: {html.escape(user_row['phone'])}"
    )


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Ð¥Ð¾ÑÑ ÑÑÐ°ÑÑÐ²Ð¾Ð²Ð°ÑÑ"], ["ÐÐ°ÑÑÐ½ÐµÑÑÑÐ²Ð¾"]],
        resize_keyboard=True,
    )


def event_admin_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Ð¡Ð´ÐµÐ»Ð°ÑÑ Ð°ÐºÑÐ¸Ð²Ð½ÑÐ¼", callback_data=f"activate:{event_id}"),
                InlineKeyboardButton("ÐÐ°ÐºÑÑÑÑ Ð½Ð°Ð±Ð¾Ñ", callback_data=f"close:{event_id}"),
            ],
            [
                InlineKeyboardButton("Ð ÐµÐ´Ð°ÐºÑÐ¸ÑÐ¾Ð²Ð°ÑÑ", callback_data=f"edit_event:{event_id}"),
                InlineKeyboardButton("Ð£ÑÐ°ÑÑÐ½Ð¸ÐºÐ¸", callback_data=f"participants_menu:{event_id}"),
            ],
        ]
    )


def participants_export_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("ÐÑÐµ", callback_data=f"export:all:{event_id}")],
            [
                InlineKeyboardButton("ÐÑÐ¶ÑÐ¸Ð½Ñ", callback_data=f"export:male:{event_id}"),
                InlineKeyboardButton("ÐÐµÐ½ÑÐ¸Ð½Ñ", callback_data=f"export:female:{event_id}"),
            ],
        ]
    )


async def send_event_message(target_message, event_row, include_stats: bool = False, reply_markup=None):
    text = render_event_text(event_row, include_stats=include_stats)
    photo_id = event_row.get("photo_file_id")
    if photo_id:
        try:
            await target_message.reply_photo(
                photo=photo_id,
                caption=text[:1024],
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
            if len(text) > 1024:
                await target_message.reply_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
            return
        except Exception as exc:
            logger.warning("Failed to send event photo: %s", exc)
    await target_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "ÐÑÐ¸Ð²ÐµÑ. ÐÐ´ÐµÑÑ Ð¼Ð¾Ð¶Ð½Ð¾ Ð±ÑÑÑÑÐ¾ Ð·Ð°Ð¿Ð¸ÑÐ°ÑÑÑÑ Ð½Ð° Ð°ÐºÑÑÐ°Ð»ÑÐ½Ð¾Ðµ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð¸Ð»Ð¸ Ð¾ÑÐ¿ÑÐ°Ð²Ð¸ÑÑ Ð¿ÑÐµÐ´Ð»Ð¾Ð¶ÐµÐ½Ð¸Ðµ Ð¿Ð¾ Ð¿Ð°ÑÑÐ½ÐµÑÑÑÐ²Ñ."
    )
    await update.effective_message.reply_text(text, reply_markup=main_menu_keyboard())


async def participate_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    event_row = get_active_event()
    if not event_row:
        await update.effective_message.reply_text(
            "Ð¡ÐµÐ¹ÑÐ°Ñ Ð½ÐµÑ Ð°ÐºÑÐ¸Ð²Ð½Ð¾Ð³Ð¾ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ Ð´Ð»Ñ Ð·Ð°Ð¿Ð¸ÑÐ¸.",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END

    latest_reg = get_latest_registration_for_user_event(user.id, event_row["id"])
    if latest_reg and latest_reg["status"] in ACTIVE_REGISTRATION_STATUSES:
        status_map = {
            "waiting_payment": "Ð¾Ð¶Ð¸Ð´Ð°ÐµÑ Ð¾Ð¿Ð»Ð°ÑÑ",
            "waiting_moderation": "Ð½Ð° Ð¼Ð¾Ð´ÐµÑÐ°ÑÐ¸Ð¸",
            "approved": "Ð¿Ð¾Ð´ÑÐ²ÐµÑÐ¶Ð´ÐµÐ½Ð°",
        }
        await update.effective_message.reply_text(
            f"Ð£ Ð²Ð°Ñ ÑÐ¶Ðµ ÐµÑÑÑ Ð·Ð°ÑÐ²ÐºÐ° Ð½Ð° ÑÑÐ¾ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ. Ð¢ÐµÐºÑÑÐ¸Ð¹ ÑÑÐ°ÑÑÑ: {status_map.get(latest_reg['status'], latest_reg['status'])}.",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END

    profile = get_user_profile(user.id)
    if profile and profile["profile_completed"]:
        ok, reason = check_slot_available(event_row, profile["gender"])
        if not ok:
            await update.effective_message.reply_text(reason, reply_markup=main_menu_keyboard())
            return ConversationHandler.END
        await send_event_and_profile_confirmation(update.effective_message, profile, event_row)
        return ConversationHandler.END

    context.user_data["profile_source"] = "participate"
    context.user_data["profile_form"] = {}
    await send_event_message(update.effective_message, event_row)
    await update.effective_message.reply_text(
        "ÐÐ°Ðº Ð²Ð°Ñ Ð·Ð¾Ð²ÑÑ?",
        reply_markup=ReplyKeyboardRemove(),
    )
    return PROFILE_NAME


async def send_event_and_profile_confirmation(message, profile, event_row) -> None:
    await send_event_message(message, event_row)
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("ÐÐµÑÐµÐ¹ÑÐ¸ Ðº Ð¾Ð¿Ð»Ð°ÑÐµ", callback_data=f"pay:{event_row['id']}")],
            [InlineKeyboardButton("ÐÐ·Ð¼ÐµÐ½Ð¸ÑÑ Ð´Ð°Ð½Ð½ÑÐµ", callback_data="edit_profile:participate")],
        ]
    )
    await message.reply_text(
        render_profile_text(profile) + "\n\nÐÑÐ»Ð¸ Ð²ÑÐµ Ð²ÐµÑÐ½Ð¾, Ð¿ÐµÑÐµÑÐ¾Ð´Ð¸ÑÐµ Ðº Ð¾Ð¿Ð»Ð°ÑÐµ.",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    profile = get_user_profile(update.effective_user.id)
    if not profile or not profile["profile_completed"]:
        await update.effective_message.reply_text(
            "ÐÐ½ÐºÐµÑÐ° Ð¿Ð¾ÐºÐ° Ð½Ðµ Ð·Ð°Ð¿Ð¾Ð»Ð½ÐµÐ½Ð°. ÐÐ°Ð¶Ð¼Ð¸ÑÐµ Â«Ð¥Ð¾ÑÑ ÑÑÐ°ÑÑÐ²Ð¾Ð²Ð°ÑÑÂ», Ð¸ Ð±Ð¾Ñ ÑÐ¾Ð±ÐµÑÐµÑ Ð´Ð°Ð½Ð½ÑÐµ Ð¾Ð´Ð¸Ð½ ÑÐ°Ð·.",
            reply_markup=main_menu_keyboard(),
        )
        return
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("ÐÐ·Ð¼ÐµÐ½Ð¸ÑÑ Ð´Ð°Ð½Ð½ÑÐµ", callback_data="edit_profile:profile")]]
    )
    await update.effective_message.reply_text(
        render_profile_text(profile),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def edit_profile_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    source = query.data.split(":", 1)[1] if ":" in query.data else "profile"
    context.user_data["profile_source"] = source
    context.user_data["profile_form"] = {}
    await query.message.reply_text("ÐÐ°Ðº Ð²Ð°Ñ Ð·Ð¾Ð²ÑÑ?", reply_markup=ReplyKeyboardRemove())
    return PROFILE_NAME


async def profile_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = (update.message.text or "").strip()
    if len(name) < 2:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð¸Ð¼Ñ Ð½Ðµ ÐºÐ¾ÑÐ¾ÑÐµ 2 ÑÐ¸Ð¼Ð²Ð¾Ð»Ð¾Ð².")
        return PROFILE_NAME
    context.user_data.setdefault("profile_form", {})["full_name"] = name
    await update.message.reply_text("Ð¡ÐºÐ¾Ð»ÑÐºÐ¾ Ð²Ð°Ð¼ Ð»ÐµÑ?")
    return PROFILE_AGE


async def profile_age(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.message.reply_text("ÐÐ¾Ð·ÑÐ°ÑÑ Ð½ÑÐ¶Ð½Ð¾ Ð²Ð²ÐµÑÑÐ¸ ÑÐ¸ÑÐ»Ð¾Ð¼.")
        return PROFILE_AGE
    age = int(text)
    if age < 18 or age > 99:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð²Ð¾Ð·ÑÐ°ÑÑ Ð¾Ñ 18 Ð´Ð¾ 99.")
        return PROFILE_AGE
    context.user_data.setdefault("profile_form", {})["age"] = age
    keyboard = ReplyKeyboardMarkup([["ÐÑÐ¶ÑÐºÐ¾Ð¹", "ÐÐµÐ½ÑÐºÐ¸Ð¹"]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Ð£ÐºÐ°Ð¶Ð¸ÑÐµ Ð¿Ð¾Ð».", reply_markup=keyboard)
    return PROFILE_GENDER


async def profile_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text not in GENDER_MAP:
        await update.message.reply_text("ÐÑÐ±ÐµÑÐ¸ÑÐµ Ð¿Ð¾Ð» ÐºÐ½Ð¾Ð¿ÐºÐ¾Ð¹ Ð½Ð¸Ð¶Ðµ.")
        return PROFILE_GENDER
    context.user_data.setdefault("profile_form", {})["gender"] = GENDER_MAP[text]
    keyboard = ReplyKeyboardMarkup([["ÐÐ°ÑÐ¸ÑÐ¿Ð¾Ð»Ñ"], ["ÐÑÑÐ³Ð¾Ð¹ Ð³Ð¾ÑÐ¾Ð´"]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Ð£ÐºÐ°Ð¶Ð¸ÑÐµ Ð³Ð¾ÑÐ¾Ð´.", reply_markup=keyboard)
    return PROFILE_CITY


async def profile_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = (update.message.text or "").strip()
    if city == "ÐÑÑÐ³Ð¾Ð¹ Ð³Ð¾ÑÐ¾Ð´":
        await update.message.reply_text("ÐÐ°Ð¿Ð¸ÑÐ¸ÑÐµ Ð²Ð°Ñ Ð³Ð¾ÑÐ¾Ð´ ÑÐµÐºÑÑÐ¾Ð¼.", reply_markup=ReplyKeyboardRemove())
        return PROFILE_CITY
    if len(city) < 2:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ ÐºÐ¾ÑÑÐµÐºÑÐ½ÑÐ¹ Ð³Ð¾ÑÐ¾Ð´.")
        return PROFILE_CITY
    context.user_data.setdefault("profile_form", {})["city"] = city
    await update.message.reply_text(
        "ÐÐ²ÐµÐ´Ð¸ÑÐµ ÑÐµÐ»ÐµÑÐ¾Ð½ Ð² ÑÐ¾ÑÐ¼Ð°ÑÐµ +79991112233 Ð¸Ð»Ð¸ 89991112233.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return PROFILE_PHONE


async def profile_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone((update.message.text or "").strip())
    if not phone:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ ÐºÐ¾ÑÑÐµÐºÑÐ½ÑÐ¹ Ð½Ð¾Ð¼ÐµÑ ÑÐµÐ»ÐµÑÐ¾Ð½Ð°.")
        return PROFILE_PHONE

    form = context.user_data.get("profile_form", {})
    form["phone"] = phone
    upsert_user_profile(
        telegram_id=update.effective_user.id,
        username=update.effective_user.username,
        full_name=form["full_name"],
        age=form["age"],
        gender=form["gender"],
        city=form["city"],
        phone=form["phone"],
    )

    source = context.user_data.get("profile_source", "profile")
    context.user_data.pop("profile_form", None)
    context.user_data.pop("profile_source", None)
    profile = get_user_profile(update.effective_user.id)

    if source == "participate":
        event_row = get_active_event()
        if not event_row:
            await update.message.reply_text(
                "ÐÐ½ÐºÐµÑÐ° ÑÐ¾ÑÑÐ°Ð½ÐµÐ½Ð°. Ð¡ÐµÐ¹ÑÐ°Ñ Ð°ÐºÑÐ¸Ð²Ð½Ð¾Ðµ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾.",
                reply_markup=main_menu_keyboard(),
            )
            return ConversationHandler.END
        ok, reason = check_slot_available(event_row, profile["gender"])
        if not ok:
            await update.message.reply_text(
                f"ÐÐ½ÐºÐµÑÐ° ÑÐ¾ÑÑÐ°Ð½ÐµÐ½Ð°. {reason}",
                reply_markup=main_menu_keyboard(),
            )
            return ConversationHandler.END
        await send_event_and_profile_confirmation(update.message, profile, event_row)
    else:
        await update.message.reply_text(
            "ÐÐ½ÐºÐµÑÐ° ÑÐ¾ÑÑÐ°Ð½ÐµÐ½Ð°.",
            reply_markup=main_menu_keyboard(),
        )
        await update.message.reply_text(render_profile_text(profile), parse_mode=ParseMode.HTML)
    return ConversationHandler.END


async def cancel_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("profile_form", None)
    context.user_data.pop("profile_source", None)
    await update.effective_message.reply_text("ÐÑÐ¼ÐµÐ½ÐµÐ½Ð¾.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def partnership_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["partner_form"] = {}
    await update.effective_message.reply_text(
        "ÐÐ°Ð¿Ð¸ÑÐ¸ÑÐµ Ð²Ð°ÑÐµ Ð¿ÑÐµÐ´Ð»Ð¾Ð¶ÐµÐ½Ð¸Ðµ Ð¿Ð¾ Ð¿Ð°ÑÑÐ½ÐµÑÑÑÐ²Ñ.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return PARTNER_PROPOSAL


async def partner_proposal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if len(text) < 5:
        await update.message.reply_text("ÐÐ¿Ð¸ÑÐ¸ÑÐµ Ð¿ÑÐµÐ´Ð»Ð¾Ð¶ÐµÐ½Ð¸Ðµ ÑÑÑÑ Ð¿Ð¾Ð´ÑÐ¾Ð±Ð½ÐµÐµ.")
        return PARTNER_PROPOSAL
    context.user_data.setdefault("partner_form", {})["proposal_text"] = text
    await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ ÐºÐ¾Ð½ÑÐ°ÐºÑÐ½ÑÐ¹ Ð½Ð¾Ð¼ÐµÑ ÑÐµÐ»ÐµÑÐ¾Ð½Ð°.")
    return PARTNER_PHONE


async def partner_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone((update.message.text or "").strip())
    if not phone:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ ÐºÐ¾ÑÑÐµÐºÑÐ½ÑÐ¹ Ð½Ð¾Ð¼ÐµÑ ÑÐµÐ»ÐµÑÐ¾Ð½Ð°.")
        return PARTNER_PHONE
    proposal = context.user_data.get("partner_form", {}).get("proposal_text", "")
    inquiry_id = create_partner_inquiry(
        telegram_id=update.effective_user.id,
        username=update.effective_user.username,
        telegram_name=update.effective_user.full_name,
        proposal_text=proposal,
        contact_phone=phone,
    )
    context.user_data.pop("partner_form", None)
    await update.message.reply_text(
        "Ð¡Ð¿Ð°ÑÐ¸Ð±Ð¾. ÐÑÐµÐ´Ð»Ð¾Ð¶ÐµÐ½Ð¸Ðµ Ð¾ÑÐ¿ÑÐ°Ð²Ð»ÐµÐ½Ð¾.",
        reply_markup=main_menu_keyboard(),
    )
    target_chat = PARTNERSHIP_CHAT_ID or MODERATION_CHAT_ID
    if target_chat:
        text = (
            f"<b>ÐÐ¾Ð²Ð°Ñ Ð·Ð°ÑÐ²ÐºÐ°: Ð¿Ð°ÑÑÐ½ÐµÑÑÑÐ²Ð¾</b>\n"
            f"ID: {inquiry_id}\n"
            f"ÐÐ¼Ñ Ð² Telegram: {html.escape(update.effective_user.full_name)}\n"
            f"Username: @{html.escape(update.effective_user.username) if update.effective_user.username else '-'}\n"
            f"Ð¢ÐµÐ»ÐµÑÐ¾Ð½: {html.escape(phone)}\n\n"
            f"<b>ÐÑÐµÐ´Ð»Ð¾Ð¶ÐµÐ½Ð¸Ðµ</b>\n{html.escape(proposal)}"
        )
        await context.bot.send_message(target_chat, text, parse_mode=ParseMode.HTML)
    return ConversationHandler.END


async def cancel_partner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("partner_form", None)
    await update.effective_message.reply_text("ÐÑÐ¼ÐµÐ½ÐµÐ½Ð¾.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def pay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    user_row = get_user_profile(query.from_user.id)

    if not event_row or event_row["status"] != "active":
        await query.message.reply_text("Ð¡ÐµÐ¹ÑÐ°Ñ ÑÑÐ¾ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð½ÐµÐ´Ð¾ÑÑÑÐ¿Ð½Ð¾ Ð´Ð»Ñ Ð·Ð°Ð¿Ð¸ÑÐ¸.")
        return
    if not user_row or not user_row["profile_completed"]:
        await query.message.reply_text("Ð¡Ð½Ð°ÑÐ°Ð»Ð° Ð·Ð°Ð¿Ð¾Ð»Ð½Ð¸ÑÐµ Ð°Ð½ÐºÐµÑÑ.")
        return

    latest_reg = get_latest_registration_for_user_event(query.from_user.id, event_id)
    if latest_reg and latest_reg["status"] in ACTIVE_REGISTRATION_STATUSES:
        await query.message.reply_text("Ð£ Ð²Ð°Ñ ÑÐ¶Ðµ ÐµÑÑÑ Ð°ÐºÑÐ¸Ð²Ð½Ð°Ñ Ð·Ð°ÑÐ²ÐºÐ° Ð½Ð° ÑÑÐ¾ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ.")
        return

    ok, reason = check_slot_available(event_row, user_row["gender"])
    if not ok:
        await query.message.reply_text(reason)
        return

    registration_id = create_registration_from_profile(event_row, user_row)
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Ð¯ Ð¾Ð¿Ð»Ð°ÑÐ¸Ð»", callback_data=f"paid:{registration_id}")],
            [InlineKeyboardButton("ÐÑÐ¼ÐµÐ½Ð¸ÑÑ Ð·Ð°ÑÐ²ÐºÑ", callback_data=f"cancel_reg:{registration_id}")],
        ]
    )
    text = (
        f"ÐÐ°ÑÐ²ÐºÐ° ÑÐ¾Ð·Ð´Ð°Ð½Ð° Ð½Ð° Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ <b>{html.escape(event_row['title'])}</b>.\n\n"
        f"{html.escape(PAYMENT_TEXT)}\n\n"
        "ÐÐ¾ÑÐ»Ðµ Ð¾Ð¿Ð»Ð°ÑÑ Ð½Ð°Ð¶Ð¼Ð¸ÑÐµ ÐºÐ½Ð¾Ð¿ÐºÑ Â«Ð¯ Ð¾Ð¿Ð»Ð°ÑÐ¸Ð»Â»."
    )
    await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def paid_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    registration_id = int(query.data.split(":", 1)[1])
    reg = get_registration(registration_id)
    if not reg:
        await query.message.reply_text("ÐÐ°ÑÐ²ÐºÐ° Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð°.")
        return
    if reg["telegram_id"] != query.from_user.id:
        await query.message.reply_text("Ð­ÑÐ¾ Ð½Ðµ Ð²Ð°ÑÐ° Ð·Ð°ÑÐ²ÐºÐ°.")
        return
    if reg["status"] != "waiting_payment":
        await query.message.reply_text("Ð­ÑÐ° Ð·Ð°ÑÐ²ÐºÐ° ÑÐ¶Ðµ Ð¾Ð±ÑÐ°Ð±Ð¾ÑÐ°Ð½Ð° Ð¸Ð»Ð¸ Ð¾ÑÐ¿ÑÐ°Ð²Ð»ÐµÐ½Ð° Ð½Ð° Ð¼Ð¾Ð´ÐµÑÐ°ÑÐ¸Ñ.")
        return

    update_registration_status(registration_id, "waiting_moderation", "paid")
    await query.message.reply_text("ÐÐ¿Ð»Ð°ÑÐ° Ð¾ÑÐ¼ÐµÑÐµÐ½Ð°. ÐÐ°ÑÐ²ÐºÐ° Ð¾ÑÐ¿ÑÐ°Ð²Ð»ÐµÐ½Ð° Ð½Ð° Ð¼Ð¾Ð´ÐµÑÐ°ÑÐ¸Ñ. Ð¡ÐºÐ¾ÑÐ¾ Ð¿Ð¾Ð´ÑÐ²ÐµÑÐ´Ð¸Ð¼ Ð±ÑÐ¾Ð½Ñ.")

    if MODERATION_CHAT_ID:
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("ÐÐ¾Ð´ÑÐ²ÐµÑÐ´Ð¸ÑÑ", callback_data=f"approve:{registration_id}"),
                InlineKeyboardButton("ÐÑÐºÐ»Ð¾Ð½Ð¸ÑÑ", callback_data=f"reject:{registration_id}"),
            ]]
        )
        mod_text = (
            f"<b>ÐÐ¾Ð²Ð°Ñ Ð·Ð°ÑÐ²ÐºÐ° Ð½Ð° Ð¼Ð¾Ð´ÐµÑÐ°ÑÐ¸Ñ</b>\n"
            f"ID Ð·Ð°ÑÐ²ÐºÐ¸: {registration_id}\n"
            f"ÐÐ²ÐµÐ½Ñ: {html.escape(reg['title'])}\n"
            f"ÐÐ°ÑÐ°: {reg['event_date']} {html.escape(reg['event_time'])}\n"
            f"ÐÐ¼Ñ: {html.escape(reg['name_snapshot'])}\n"
            f"ÐÐ¾Ð·ÑÐ°ÑÑ: {reg['age_snapshot']}\n"
            f"ÐÐ¾Ð»: {GENDER_FULL_LABELS.get(reg['gender_snapshot'], reg['gender_snapshot'])}\n"
            f"ÐÐ¾ÑÐ¾Ð´: {html.escape(reg['city_snapshot'])}\n"
            f"Ð¢ÐµÐ»ÐµÑÐ¾Ð½: {html.escape(reg['phone_snapshot'])}"
        )
        await context.bot.send_message(
            chat_id=MODERATION_CHAT_ID,
            text=mod_text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )


async def cancel_registration_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    registration_id = int(query.data.split(":", 1)[1])
    reg = get_registration(registration_id)
    if not reg:
        await query.message.reply_text("ÐÐ°ÑÐ²ÐºÐ° Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð°.")
        return
    if reg["telegram_id"] != query.from_user.id:
        await query.message.reply_text("Ð­ÑÐ¾ Ð½Ðµ Ð²Ð°ÑÐ° Ð·Ð°ÑÐ²ÐºÐ°.")
        return
    if reg["status"] not in ("waiting_payment", "waiting_moderation"):
        await query.message.reply_text("Ð­ÑÑ Ð·Ð°ÑÐ²ÐºÑ ÑÐ¶Ðµ Ð½ÐµÐ»ÑÐ·Ñ Ð¾ÑÐ¼ÐµÐ½Ð¸ÑÑ.")
        return

    update_registration_status(registration_id, "cancelled", "cancelled")
    await query.message.reply_text("ÐÐ°ÑÐ²ÐºÐ° Ð¾ÑÐ¼ÐµÐ½ÐµÐ½Ð°. Ð¡Ð»Ð¾Ñ Ð¾ÑÐ²Ð¾Ð±Ð¾Ð¶Ð´ÐµÐ½.")


async def moderation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð° Ðº Ð¼Ð¾Ð´ÐµÑÐ°ÑÐ¸Ð¸.")
        return

    action, raw_id = query.data.split(":", 1)
    registration_id = int(raw_id)
    reg = get_registration(registration_id)
    if not reg:
        await query.message.reply_text("ÐÐ°ÑÐ²ÐºÐ° Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð°.")
        return
    if reg["status"] != "waiting_moderation":
        await query.message.reply_text(f"Ð­ÑÐ° Ð·Ð°ÑÐ²ÐºÐ° ÑÐ¶Ðµ Ð¸Ð¼ÐµÐµÑ ÑÑÐ°ÑÑÑ: {reg['status']}")
        return

    if action == "approve":
        update_registration_status(registration_id, "approved", "paid")
        await context.bot.send_message(
            chat_id=reg["telegram_id"],
            text=(
                f"ÐÐ°ÑÐ° Ð±ÑÐ¾Ð½Ñ Ð¿Ð¾Ð´ÑÐ²ÐµÑÐ¶Ð´ÐµÐ½Ð° â\n\n"
                f"ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ: {reg['title']}\n"
                f"ÐÐ°ÑÐ°: {reg['event_date']} {reg['event_time']}\n"
                f"ÐÐµÑÑÐ¾: {reg['location']}"
            ),
        )
        await query.edit_message_text(
            query.message.text_html + "\n\nâ ÐÐ¾Ð´ÑÐ²ÐµÑÐ¶Ð´ÐµÐ½Ð¾",
            parse_mode=ParseMode.HTML,
        )
    elif action == "reject":
        update_registration_status(registration_id, "rejected", "rejected")
        await context.bot.send_message(
            chat_id=reg["telegram_id"],
            text="Ð ÑÐ¾Ð¶Ð°Ð»ÐµÐ½Ð¸Ñ, Ð·Ð°ÑÐ²ÐºÐ° Ð¾ÑÐºÐ»Ð¾Ð½ÐµÐ½Ð°. Ð¡Ð»Ð¾Ñ Ð¾ÑÐ²Ð¾Ð±Ð¾Ð¶Ð´ÐµÐ½.",
        )
        await query.edit_message_text(
            query.message.text_html + "\n\nâ ÐÑÐºÐ»Ð¾Ð½ÐµÐ½Ð¾",
            parse_mode=ParseMode.HTML,
        )


async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð° Ðº Ð°Ð´Ð¼Ð¸Ð½-Ð¿Ð°Ð½ÐµÐ»Ð¸.")
        return
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("ÐÐ¾Ð±Ð°Ð²Ð¸ÑÑ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ", callback_data="admin_add_event")],
            [InlineKeyboardButton("Ð¡Ð¿Ð¸ÑÐ¾Ðº Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ð¹", callback_data="admin_list_events")],
        ]
    )
    await update.effective_message.reply_text("ÐÐ´Ð¼Ð¸Ð½-Ð¿Ð°Ð½ÐµÐ»Ñ", reply_markup=keyboard)


async def admin_add_event_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return ConversationHandler.END
    context.user_data["new_event"] = {}
    await query.message.reply_text("ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ?")
    return EVENT_TITLE


async def admin_event_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = (update.message.text or "").strip()
    if len(title) < 2:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾ÑÐ¼Ð°Ð»ÑÐ½Ð¾Ðµ Ð½Ð°Ð·Ð²Ð°Ð½Ð¸Ðµ.")
        return EVENT_TITLE
    context.user_data.setdefault("new_event", {})["title"] = title
    await update.message.reply_text("ÐÐ°ÑÐ° Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ? Ð¤Ð¾ÑÐ¼Ð°Ñ: YYYY-MM-DD")
    return EVENT_DATE


async def admin_event_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("ÐÐµÐ²ÐµÑÐ½ÑÐ¹ ÑÐ¾ÑÐ¼Ð°Ñ Ð´Ð°ÑÑ. ÐÑÐ¿Ð¾Ð»ÑÐ·ÑÐ¹ÑÐµ YYYY-MM-DD")
        return EVENT_DATE
    context.user_data.setdefault("new_event", {})["event_date"] = text
    await update.message.reply_text("ÐÑÐµÐ¼Ñ? ÐÐ°Ð¿ÑÐ¸Ð¼ÐµÑ: 19:00")
    return EVENT_TIME


async def admin_event_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if len(text) < 3:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð²ÑÐµÐ¼Ñ, Ð½Ð°Ð¿ÑÐ¸Ð¼ÐµÑ 19:00")
        return EVENT_TIME
    context.user_data.setdefault("new_event", {})["event_time"] = text
    await update.message.reply_text("ÐÐµÑÑÐ¾ Ð¿ÑÐ¾Ð²ÐµÐ´ÐµÐ½Ð¸Ñ?")
    return EVENT_LOCATION


async def admin_event_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if len(text) < 2:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð¼ÐµÑÑÐ¾ Ð¿ÑÐ¾Ð²ÐµÐ´ÐµÐ½Ð¸Ñ.")
        return EVENT_LOCATION
    context.user_data.setdefault("new_event", {})["location"] = text
    await update.message.reply_text("Ð¡ÑÐ¾Ð¸Ð¼Ð¾ÑÑÑ ÑÑÐ°ÑÑÐ¸Ñ? Ð¢Ð¾Ð»ÑÐºÐ¾ ÑÐ¸ÑÐ»Ð¾, Ð½Ð°Ð¿ÑÐ¸Ð¼ÐµÑ 1000")
    return EVENT_PRICE


async def admin_event_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip().replace(",", ".")
    try:
        price = Decimal(text)
        if price < 0:
            raise InvalidOperation
    except Exception:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ ÐºÐ¾ÑÑÐµÐºÑÐ½ÑÑ ÑÑÐ¾Ð¸Ð¼Ð¾ÑÑÑ. ÐÐ°Ð¿ÑÐ¸Ð¼ÐµÑ: 1000")
        return EVENT_PRICE
    context.user_data.setdefault("new_event", {})["price"] = price
    await update.message.reply_text("ÐÐ¿Ð¸ÑÐ°Ð½Ð¸Ðµ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ? ÐÐ¾Ð¶Ð½Ð¾ ÐºÐ¾ÑÐ¾ÑÐºÐ¾.")
    return EVENT_DESCRIPTION


async def admin_event_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("new_event", {})["description"] = (update.message.text or "").strip()
    await update.message.reply_text("ÐÐ±ÑÐ¸Ð¹ Ð»Ð¸Ð¼Ð¸Ñ ÑÑÐ°ÑÑÐ½Ð¸ÐºÐ¾Ð²? ÐÐ°Ð¿ÑÐ¸Ð¼ÐµÑ: 20")
    return EVENT_LIMIT


async def admin_event_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text.isdigit() or int(text) <= 0:
        await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð»Ð¸Ð¼Ð¸Ñ Ð¿Ð¾Ð»Ð¾Ð¶Ð¸ÑÐµÐ»ÑÐ½ÑÐ¼ ÑÐ¸ÑÐ»Ð¾Ð¼.")
        return EVENT_LIMIT
    context.user_data.setdefault("new_event", {})["total_limit"] = int(text)
    keyboard = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("50/50 Ð²ÐºÐ»ÑÑÐ¸ÑÑ", callback_data="balance:on"),
            InlineKeyboardButton("ÐÐµÐ· 50/50", callback_data="balance:off"),
        ]]
    )
    await update.message.reply_text("ÐÑÐ¶Ð½Ð¾ Ð»Ð¸ Ð²ÐºÐ»ÑÑÐ¸ÑÑ Ð±Ð°Ð»Ð°Ð½Ñ 50/50 Ð¿Ð¾ Ð¿Ð¾Ð»Ñ?", reply_markup=keyboard)
    return EVENT_BALANCE


async def admin_event_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    enabled = query.data.split(":", 1)[1] == "on"
    data = context.user_data.get("new_event", {})
    total_limit = int(data.get("total_limit", 0))
    if enabled and total_limit % 2 != 0:
        await query.message.reply_text(
            "ÐÐ»Ñ ÑÐµÐ¶Ð¸Ð¼Ð° 50/50 Ð¾Ð±ÑÐ¸Ð¹ Ð»Ð¸Ð¼Ð¸Ñ Ð´Ð¾Ð»Ð¶ÐµÐ½ Ð±ÑÑÑ ÑÐµÑÐ½ÑÐ¼. Ð¡Ð¾Ð·Ð´Ð°Ð¹ÑÐµ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð·Ð°Ð½Ð¾Ð²Ð¾ Ñ ÑÐµÑÐ½ÑÐ¼ Ð»Ð¸Ð¼Ð¸ÑÐ¾Ð¼."
        )
        context.user_data.pop("new_event", None)
        return ConversationHandler.END
    data["gender_balance_enabled"] = enabled
    await query.message.reply_text(
        "Ð¢ÐµÐ¿ÐµÑÑ Ð¾ÑÐ¿ÑÐ°Ð²ÑÑÐµ ÑÐ¾ÑÐ¾ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ Ð¸Ð»Ð¸ Ð½Ð°Ð¿Ð¸ÑÐ¸ÑÐµ Ð¡ÐÐÐ, ÐµÑÐ»Ð¸ ÑÐ¾ÑÐ¾ Ð¿Ð¾ÐºÐ° Ð½Ðµ Ð½ÑÐ¶Ð½Ð¾."
    )
    return EVENT_PHOTO


async def admin_event_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("new_event", {})
    photo_file_id = None
    if update.message.photo:
        photo_file_id = update.message.photo[-1].file_id
    else:
        text = (update.message.text or "").strip().lower()
        if text not in {"ÑÐºÐ¸Ð¿", "skip", "Ð½ÐµÑ"}:
            await update.message.reply_text("ÐÑÐ¿ÑÐ°Ð²ÑÑÐµ ÑÐ¾ÑÐ¾ Ð¸Ð»Ð¸ Ð½Ð°Ð¿Ð¸ÑÐ¸ÑÐµ Ð¡ÐÐÐ.")
            return EVENT_PHOTO
    event_id = create_event(
        title=data["title"],
        event_date=data["event_date"],
        event_time=data["event_time"],
        location=data["location"],
        price=data["price"],
        description=data.get("description", ""),
        total_limit=int(data["total_limit"]),
        gender_balance_enabled=bool(data.get("gender_balance_enabled", False)),
        photo_file_id=photo_file_id,
    )
    context.user_data.pop("new_event", None)
    event_row = get_event(event_id)
    await update.message.reply_text("ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ ÑÐ¾Ð·Ð´Ð°Ð½Ð¾.")
    await send_event_message(
        update.message,
        event_row,
        include_stats=True,
        reply_markup=event_admin_keyboard(event_id),
    )
    return ConversationHandler.END


async def admin_cancel_event_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("new_event", None)
    await update.effective_message.reply_text("Ð¡Ð¾Ð·Ð´Ð°Ð½Ð¸Ðµ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ Ð¾ÑÐ¼ÐµÐ½ÐµÐ½Ð¾.")
    return ConversationHandler.END


async def admin_list_events(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query:
        await query.answer()
        user_id = query.from_user.id
        target = query.message
    else:
        user_id = update.effective_user.id
        target = update.effective_message

    if not is_admin(user_id):
        await target.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return

    events = list_events()
    if not events:
        await target.reply_text("ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ð¹ Ð¿Ð¾ÐºÐ° Ð½ÐµÑ.")
        return

    for event_row in events:
        await send_event_message(
            target,
            event_row,
            include_stats=True,
            reply_markup=event_admin_keyboard(event_row["id"]),
        )


async def admin_event_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return
    action, raw_id = query.data.split(":", 1)
    event_id = int(raw_id)
    if action == "activate":
        set_event_status(event_id, "active")
        await query.message.reply_text(f"ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ #{event_id} ÑÐ´ÐµÐ»Ð°Ð½Ð¾ Ð°ÐºÑÐ¸Ð²Ð½ÑÐ¼.")
    elif action == "close":
        set_event_status(event_id, "closed")
        await query.message.reply_text(f"ÐÐ°Ð±Ð¾Ñ Ð½Ð° Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ #{event_id} Ð·Ð°ÐºÑÑÑ.")


async def edit_event_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return
    event_id = int(query.data.split(":", 1)[1])
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ", callback_data=f"edit_field:title:{event_id}")],
            [InlineKeyboardButton("ÐÐ°ÑÐ°", callback_data=f"edit_field:event_date:{event_id}"), InlineKeyboardButton("ÐÑÐµÐ¼Ñ", callback_data=f"edit_field:event_time:{event_id}")],
            [InlineKeyboardButton("ÐÐµÑÑÐ¾", callback_data=f"edit_field:location:{event_id}"), InlineKeyboardButton("Ð¦ÐµÐ½Ð°", callback_data=f"edit_field:price:{event_id}")],
            [InlineKeyboardButton("ÐÐ¿Ð¸ÑÐ°Ð½Ð¸Ðµ", callback_data=f"edit_field:description:{event_id}"), InlineKeyboardButton("ÐÐ¸Ð¼Ð¸Ñ", callback_data=f"edit_field:total_limit:{event_id}")],
            [InlineKeyboardButton("Ð¤Ð¾ÑÐ¾", callback_data=f"edit_field:photo_file_id:{event_id}")],
            [InlineKeyboardButton("ÐÐµÑÐµÐºÐ»ÑÑÐ¸ÑÑ 50/50", callback_data=f"toggle_balance:{event_id}")],
        ]
    )
    await query.message.reply_text(f"Ð§ÑÐ¾ Ð¸Ð·Ð¼ÐµÐ½Ð¸ÑÑ Ð² Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ð¸ #{event_id}?", reply_markup=keyboard)


async def edit_event_field_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return ConversationHandler.END
    _, field, raw_id = query.data.split(":", 2)
    event_id = int(raw_id)
    context.user_data["edit_event"] = {"event_id": event_id, "field": field}
    prompts = {
        "title": "ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾Ð²Ð¾Ðµ Ð½Ð°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ.",
        "event_date": "ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾Ð²ÑÑ Ð´Ð°ÑÑ Ð² ÑÐ¾ÑÐ¼Ð°ÑÐµ YYYY-MM-DD.",
        "event_time": "ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾Ð²Ð¾Ðµ Ð²ÑÐµÐ¼Ñ. ÐÐ°Ð¿ÑÐ¸Ð¼ÐµÑ: 20:00.",
        "location": "ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾Ð²Ð¾Ðµ Ð¼ÐµÑÑÐ¾ Ð¿ÑÐ¾Ð²ÐµÐ´ÐµÐ½Ð¸Ñ.",
        "price": "ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾Ð²ÑÑ ÑÑÐ¾Ð¸Ð¼Ð¾ÑÑÑ ÑÐ¸ÑÐ»Ð¾Ð¼.",
        "description": "ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾Ð²Ð¾Ðµ Ð¾Ð¿Ð¸ÑÐ°Ð½Ð¸Ðµ.",
        "total_limit": "ÐÐ²ÐµÐ´Ð¸ÑÐµ Ð½Ð¾Ð²ÑÐ¹ Ð¾Ð±ÑÐ¸Ð¹ Ð»Ð¸Ð¼Ð¸Ñ ÑÑÐ°ÑÑÐ½Ð¸ÐºÐ¾Ð².",
        "photo_file_id": "ÐÑÐ¿ÑÐ°Ð²ÑÑÐµ Ð½Ð¾Ð²Ð¾Ðµ ÑÐ¾ÑÐ¾ Ð¼ÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ñ Ð¸Ð»Ð¸ Ð½Ð°Ð¿Ð¸ÑÐ¸ÑÐµ Ð£ÐÐÐÐÐ¢Ð¬, ÑÑÐ¾Ð±Ñ ÑÐ±ÑÐ°ÑÑ ÑÐ¾ÑÐ¾.",
    }
    await query.message.reply_text(prompts[field], reply_markup=ReplyKeyboardRemove())
    return EDIT_EVENT_VALUE


async def edit_event_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("edit_event") or {}
    event_id = data.get("event_id")
    field = data.get("field")
    if not event_id or not field:
        await update.effective_message.reply_text("ÐÐµ ÑÐ´Ð°Ð»Ð¾ÑÑ Ð¾Ð¿ÑÐµÐ´ÐµÐ»Ð¸ÑÑ Ð¿Ð¾Ð»Ðµ Ð´Ð»Ñ ÑÐµÐ´Ð°ÐºÑÐ¸ÑÐ¾Ð²Ð°Ð½Ð¸Ñ.")
        return ConversationHandler.END

    value = None
    if field == "photo_file_id":
        if update.message.photo:
            value = update.message.photo[-1].file_id
        else:
            text = (update.message.text or "").strip().lower()
            if text not in {"ÑÐ´Ð°Ð»Ð¸ÑÑ", "delete", "remove"}:
                await update.message.reply_text("ÐÑÐ¿ÑÐ°Ð²ÑÑÐµ ÑÐ¾ÑÐ¾ Ð¸Ð»Ð¸ Ð½Ð°Ð¿Ð¸ÑÐ¸ÑÐµ Ð£ÐÐÐÐÐ¢Ð¬.")
                return EDIT_EVENT_VALUE
            value = None
    else:
        text = (update.message.text or "").strip()
        if field == "event_date":
            try:
                datetime.strptime(text, "%Y-%m-%d")
            except ValueError:
                await update.message.reply_text("ÐÐµÐ²ÐµÑÐ½ÑÐ¹ ÑÐ¾ÑÐ¼Ð°Ñ Ð´Ð°ÑÑ. ÐÑÐ¿Ð¾Ð»ÑÐ·ÑÐ¹ÑÐµ YYYY-MM-DD.")
                return EDIT_EVENT_VALUE
            value = text
        elif field == "price":
            try:
                value = Decimal(text.replace(",", "."))
                if value < 0:
                    raise InvalidOperation
            except Exception:
                await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ ÐºÐ¾ÑÑÐµÐºÑÐ½ÑÑ ÑÑÐ¾Ð¸Ð¼Ð¾ÑÑÑ.")
                return EDIT_EVENT_VALUE
        elif field == "total_limit":
            if not text.isdigit() or int(text) <= 0:
                await update.message.reply_text("ÐÐ²ÐµÐ´Ð¸ÑÐµ ÐºÐ¾ÑÑÐµÐºÑÐ½ÑÐ¹ Ð»Ð¸Ð¼Ð¸Ñ.")
                return EDIT_EVENT_VALUE
            event_row = get_event(event_id)
            if event_row and event_row["gender_balance_enabled"] and int(text) % 2 != 0:
                await update.message.reply_text("ÐÑÐ¸ Ð²ÐºÐ»ÑÑÐµÐ½Ð½Ð¾Ð¼ 50/50 Ð»Ð¸Ð¼Ð¸Ñ Ð´Ð¾Ð»Ð¶ÐµÐ½ Ð±ÑÑÑ ÑÐµÑÐ½ÑÐ¼.")
                return EDIT_EVENT_VALUE
            value = int(text)
        else:
            if len(text) < 1:
                await update.message.reply_text("ÐÐ½Ð°ÑÐµÐ½Ð¸Ðµ Ð½Ðµ Ð¼Ð¾Ð¶ÐµÑ Ð±ÑÑÑ Ð¿ÑÑÑÑÐ¼.")
                return EDIT_EVENT_VALUE
            value = text

    update_event_field(event_id, field, value)
    context.user_data.pop("edit_event", None)
    event_row = get_event(event_id)
    await update.message.reply_text("ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð¾Ð±Ð½Ð¾Ð²Ð»ÐµÐ½Ð¾.")
    await send_event_message(
        update.message,
        event_row,
        include_stats=True,
        reply_markup=event_admin_keyboard(event_id),
    )
    return ConversationHandler.END


async def cancel_edit_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("edit_event", None)
    await update.effective_message.reply_text("Ð ÐµÐ´Ð°ÐºÑÐ¸ÑÐ¾Ð²Ð°Ð½Ð¸Ðµ Ð¾ÑÐ¼ÐµÐ½ÐµÐ½Ð¾.")
    return ConversationHandler.END


async def toggle_balance_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return
    event_id = int(query.data.split(":", 1)[1])
    ok, message = toggle_event_gender_balance(event_id)
    await query.message.reply_text(message)
    if ok:
        event_row = get_event(event_id)
        await send_event_message(
            query.message,
            event_row,
            include_stats=True,
            reply_markup=event_admin_keyboard(event_id),
        )


async def participants_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾.")
        return
    stats = get_event_stats(event_id)
    await query.message.reply_text(
        f"ÐÐ¾Ð´ÑÐ²ÐµÑÐ¶Ð´ÐµÐ½Ð¾ Ð¸ Ð¾Ð¿Ð»Ð°ÑÐµÐ½Ð¾: {stats['confirmed_paid_count']}\nÐÑÐ±ÐµÑÐ¸ÑÐµ, ÐºÐ°ÐºÐ¾Ð¹ ÑÐ¿Ð¸ÑÐ¾Ðº Ð²ÑÐ³ÑÑÐ·Ð¸ÑÑ.",
        reply_markup=participants_export_keyboard(event_id),
    )


async def export_participants_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("Ð£ Ð²Ð°Ñ Ð½ÐµÑ Ð´Ð¾ÑÑÑÐ¿Ð°.")
        return
    _, mode, raw_id = query.data.split(":", 2)
    event_id = int(raw_id)
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("ÐÐµÑÐ¾Ð¿ÑÐ¸ÑÑÐ¸Ðµ Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾.")
        return
    gender = None if mode == "all" else mode
    participants = list_confirmed_participants(event_id, gender=gender)
    if not participants:
        await query.message.reply_text("ÐÐ¾Ð´ÑÐ²ÐµÑÐ¶Ð´ÐµÐ½Ð½ÑÑ Ð¸ Ð¾Ð¿Ð»Ð°ÑÐµÐ½Ð½ÑÑ ÑÑÐ°ÑÑÐ½Ð¸ÐºÐ¾Ð² Ð¿Ð¾ÐºÐ° Ð½ÐµÑ.")
        return

    heading = {
        "all": "ÐÑÐµ ÑÑÐ°ÑÑÐ½Ð¸ÐºÐ¸",
        "male": "ÐÑÐ¶ÑÐ¸Ð½Ñ",
        "female": "ÐÐµÐ½ÑÐ¸Ð½Ñ",
    }[mode]
    lines = [f"{heading} â {event_row['title']}", ""]
    for idx, row in enumerate(participants, start=1):
        lines.append(f"{idx}. {row['name_snapshot']} ({GENDER_LABELS.get(row['gender_snapshot'], row['gender_snapshot'])}) â {row['phone_snapshot']}")
    text = "\n".join(lines)
    buf = io.BytesIO(text.encode("utf-8"))
    buf.name = f"participants_event_{event_id}_{mode}.txt"
    await query.message.reply_document(document=buf, caption=f"ÐÑÐ³ÑÑÐ·ÐºÐ°: {heading}")
    await query.message.reply_text(text[:4000])


async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    if text in {"Ð¥Ð¾ÑÑ ÑÑÐ°ÑÑÐ²Ð¾Ð²Ð°ÑÑ", "ÐÐ°ÑÑÐ½ÐµÑÑÑÐ²Ð¾"}:
        return
    await update.message.reply_text("ÐÑÐ¿Ð¾Ð»ÑÐ·ÑÐ¹ÑÐµ ÐºÐ½Ð¾Ð¿ÐºÐ¸ Ð¼ÐµÐ½Ñ Ð½Ð¸Ð¶Ðµ.", reply_markup=main_menu_keyboard())


def build_application() -> Application:
    application = Application.builder().token(BOT_TOKEN).updater(None).build()

    profile_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(r"^(Ð¥Ð¾ÑÑ ÑÑÐ°ÑÑÐ²Ð¾Ð²Ð°ÑÑ|Ð£ÑÐ°ÑÑÐ²Ð¾Ð²Ð°ÑÑ)$"), participate_entry),
            CallbackQueryHandler(edit_profile_entry, pattern=r"^edit_profile:"),
        ],
        states={
            PROFILE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_name)],
            PROFILE_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_age)],
            PROFILE_GENDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_gender)],
            PROFILE_CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_city)],
            PROFILE_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_phone)],
        },
        fallbacks=[CommandHandler("cancel", cancel_profile)],
        per_chat=True,
        per_user=True,
    )

    partner_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^ÐÐ°ÑÑÐ½ÐµÑÑÑÐ²Ð¾$"), partnership_entry)],
        states={
            PARTNER_PROPOSAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_proposal)],
            PARTNER_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_phone)],
        },
        fallbacks=[CommandHandler("cancel", cancel_partner)],
        per_chat=True,
        per_user=True,
    )

    event_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_add_event_entry, pattern=r"^admin_add_event$")],
        states={
            EVENT_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_title)],
            EVENT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_date)],
            EVENT_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_time)],
            EVENT_LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_location)],
            EVENT_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_price)],
            EVENT_DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_description)],
            EVENT_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_limit)],
            EVENT_BALANCE: [CallbackQueryHandler(admin_event_balance, pattern=r"^balance:")],
            EVENT_PHOTO: [
                MessageHandler(filters.PHOTO, admin_event_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_photo),
            ],
        },
        fallbacks=[CommandHandler("cancel", admin_cancel_event_creation)],
        per_chat=True,
        per_user=True,
    )

    edit_event_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_event_field_entry, pattern=r"^edit_field:")],
        states={
            EDIT_EVENT_VALUE: [
                MessageHandler(filters.PHOTO, edit_event_value),
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_event_value),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_edit_event)],
        per_chat=True,
        per_user=True,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_menu))
    application.add_handler(CommandHandler("profile", show_profile))
    application.add_handler(profile_conv)
    application.add_handler(partner_conv)
    application.add_handler(event_conv)
    application.add_handler(edit_event_conv)
    application.add_handler(CallbackQueryHandler(pay_callback, pattern=r"^pay:"))
    application.add_handler(CallbackQueryHandler(paid_callback, pattern=r"^paid:"))
    application.add_handler(CallbackQueryHandler(cancel_registration_callback, pattern=r"^cancel_reg:"))
    application.add_handler(CallbackQueryHandler(moderation_callback, pattern=r"^(approve|reject):"))
    application.add_handler(CallbackQueryHandler(admin_list_events, pattern=r"^admin_list_events$"))
    application.add_handler(CallbackQueryHandler(admin_event_status_callback, pattern=r"^(activate|close):"))
    application.add_handler(CallbackQueryHandler(edit_event_menu, pattern=r"^edit_event:"))
    application.add_handler(CallbackQueryHandler(toggle_balance_callback, pattern=r"^toggle_balance:"))
    application.add_handler(CallbackQueryHandler(participants_menu_callback, pattern=r"^participants_menu:"))
    application.add_handler(CallbackQueryHandler(export_participants_callback, pattern=r"^export:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))

    return application


telegram_app = build_application()
web_app = FastAPI()


@web_app.get("/")
async def root():
    return {"ok": True, "service": "event-bot", "timezone": TIMEZONE_LABEL}


@web_app.get("/health")
async def health():
    return {"ok": True}


@web_app.post(f"/webhook/{WEBHOOK_SECRET}")
async def telegram_webhook(request: Request):
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret token")
    data = await request.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}


@web_app.on_event("startup")
async def on_startup():
    init_db()
    await telegram_app.initialize()
    await telegram_app.start()
    webhook_url = f"{BASE_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET}"
    await telegram_app.bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET,
        allowed_updates=Update.ALL_TYPES,
    )
    logger.info("Webhook set to %s", webhook_url)


@web_app.on_event("shutdown")
async def on_shutdown():
    try:
        await telegram_app.bot.delete_webhook()
    except Exception as exc:
        logger.warning("Could not delete webhook: %s", exc)
    await telegram_app.stop()
    await telegram_app.shutdown()


app = web_app
