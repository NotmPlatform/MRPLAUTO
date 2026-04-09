# -*- coding: utf-8 -*-
import csv
import html
import io
import logging
import os
import re
import secrets
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional
from zoneinfo import ZoneInfo

import psycopg
from fastapi import FastAPI, HTTPException, Request
from psycopg.errors import UniqueViolation
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
ADMIN_USER_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_USER_IDS", "").split(",")
    if x.strip().isdigit()
}
PAYMENT_TEXT = os.getenv(
    "PAYMENT_TEXT",
    "Оплатите участие по вашим реквизитам. После оплаты нажмите кнопку «Я оплатил».",
)
SBP_PHONE = os.getenv("SBP_PHONE", "+79262326715")
SBP_RECEIVER = os.getenv("SBP_RECEIVER", "Баранцев А.М.")
TIMEZONE_LABEL = os.getenv("TIMEZONE_LABEL", "Europe/Moscow")
PAYMENT_TIMEOUT_MINUTES = int(os.getenv("PAYMENT_TIMEOUT_MINUTES", "30"))
PAYMENT_REMINDER_BEFORE_MINUTES = int(os.getenv("PAYMENT_REMINDER_BEFORE_MINUTES", "15"))
PAYMENT_CODE_LENGTH = int(os.getenv("PAYMENT_CODE_LENGTH", "6"))
EVENT_REMINDER_BEFORE_HOURS = int(os.getenv("EVENT_REMINDER_BEFORE_HOURS", "24"))
BACKGROUND_CHECK_INTERVAL_SECONDS = int(os.getenv("BACKGROUND_CHECK_INTERVAL_SECONDS", "300"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not BASE_URL:
    raise RuntimeError("BASE_URL is required")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

LOCAL_TZ = ZoneInfo(TIMEZONE_LABEL)

(
    PROFILE_NAME,
    PROFILE_AGE,
    PROFILE_GENDER,
    PROFILE_CITY,
    PROFILE_PHONE,
    PROFILE_CONSENT,
    PROFILE_HAS_CAR,
    PARTNER_CONTACT_NAME,
    PARTNER_PROJECT_NAME,
    PARTNER_DESCRIPTION,
    PARTNER_CONTACT,
) = range(11)
(
    EVENT_TITLE,
    EVENT_DATE,
    EVENT_TIME,
    EVENT_LOCATION,
    EVENT_PRICE,
    EVENT_DESCRIPTION,
    EVENT_LIMIT,
    EVENT_MIN_AGE,
    EVENT_MAX_AGE,
    EVENT_BALANCE,
    EVENT_CAR_QUESTION,
    EVENT_POSTER,
    EDIT_EVENT_VALUE,
    EDIT_EVENT_POSTER,
) = range(100, 114)

ACTIVE_REGISTRATION_STATUSES = ("waiting_payment", "waiting_moderation", "approved")
BLOCKING_REGISTRATION_STATUSES = ("waiting_payment", "waiting_moderation", "approved", "waiting_list")
GENDER_MAP = {
    "Мужской": "male",
    "Женский": "female",
}
GENDER_LABELS = {
    "male": "Мужской",
    "female": "Женский",
}
STATUS_LABELS = {
    "waiting_payment": "ожидает оплаты",
    "waiting_moderation": "на модерации",
    "approved": "подтверждена",
    "rejected": "отклонена",
    "cancelled": "отменена",
    "waiting_list": "лист ожидания",
    "expired": "истек таймер оплаты",
}
EVENT_STATUS_LABELS = {
    "draft": "Черновик",
    "upcoming": "Скоро",
    "active": "Активно",
    "closed": "Набор закрыт",
}
PARTICIPATE_BUTTONS = {"Участвовать", "Хочу участвовать"}
PARTNER_BUTTON = "Партнерство"
SKIP_POSTER_TEXT = "Пропустить афишу"
REMOVE_POSTER_TEXT = "Удалить афишу"
CANCEL_EDIT_TEXT = "Отмена"
YES_NO_KEYBOARD = [["Да", "Нет"]]


class DuplicateRegistrationError(Exception):
    pass


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def cleanup_duplicate_blocking_registrations(cur) -> None:
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id, telegram_id
                    ORDER BY
                        CASE status
                            WHEN 'approved' THEN 1
                            WHEN 'waiting_moderation' THEN 2
                            WHEN 'waiting_payment' THEN 3
                            WHEN 'waiting_list' THEN 4
                            ELSE 99
                        END,
                        created_at DESC,
                        id DESC
                ) AS rn
            FROM registrations
            WHERE status IN ('waiting_payment', 'waiting_moderation', 'approved', 'waiting_list')
        )
        UPDATE registrations r
        SET status = 'cancelled',
            payment_status = CASE
                WHEN r.status = 'waiting_list' THEN 'not_required'
                ELSE 'cancelled'
            END,
            reservation_expires_at = NULL
        FROM ranked x
        WHERE r.id = x.id
          AND x.rn > 1;
        """
    )


def generate_payment_code() -> str:
    min_value = 10 ** max(PAYMENT_CODE_LENGTH - 1, 0)
    max_value = (10 ** PAYMENT_CODE_LENGTH) - 1
    return str(secrets.randbelow(max_value - min_value + 1) + min_value)


def generate_unique_payment_code(cur) -> str:
    for _ in range(100):
        code = generate_payment_code()
        cur.execute("SELECT 1 FROM registrations WHERE payment_code = %s LIMIT 1", (code,))
        if cur.fetchone() is None:
            return code
    raise RuntimeError("Could not generate unique payment code")


def backfill_missing_payment_codes(cur) -> None:
    cur.execute("SELECT id FROM registrations WHERE payment_code IS NULL ORDER BY id ASC")
    rows = cur.fetchall()
    for row in rows:
        code = generate_unique_payment_code(cur)
        cur.execute(
            "UPDATE registrations SET payment_code = %s WHERE id = %s",
            (code, row["id"]),
        )


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
                has_car BOOLEAN,
                consent_personal_data BOOLEAN NOT NULL DEFAULT FALSE,
                consent_at TIMESTAMPTZ,
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
                min_age INTEGER NOT NULL DEFAULT 18,
                max_age INTEGER NOT NULL DEFAULT 99,
                gender_balance_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                male_limit INTEGER,
                female_limit INTEGER,
                ask_has_car BOOLEAN NOT NULL DEFAULT FALSE,
                poster_file_id TEXT,
                poster_file_unique_id TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
                has_car_snapshot BOOLEAN,
                consent_snapshot BOOLEAN NOT NULL DEFAULT FALSE,
                payment_code TEXT,
                status TEXT NOT NULL DEFAULT 'waiting_payment',
                payment_status TEXT NOT NULL DEFAULT 'not_paid',
                reservation_expires_at TIMESTAMPTZ,
                payment_reminder_sent_at TIMESTAMPTZ,
                before_event_reminder_sent_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                moderated_at TIMESTAMPTZ
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS partner_requests (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                username TEXT,
                contact_name TEXT NOT NULL,
                project_name TEXT NOT NULL,
                description TEXT,
                contact_value TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # Миграции для уже существующей БД
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS has_car BOOLEAN;")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_personal_data BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS consent_at TIMESTAMPTZ;")

        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS min_age INTEGER NOT NULL DEFAULT 18;")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS max_age INTEGER NOT NULL DEFAULT 99;")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS gender_balance_enabled BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS male_limit INTEGER;")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS female_limit INTEGER;")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS ask_has_car BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'draft';")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS poster_file_id TEXT;")
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS poster_file_unique_id TEXT;")

        cur.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS has_car_snapshot BOOLEAN;")
        cur.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS consent_snapshot BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS payment_code TEXT;")
        cur.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS reservation_expires_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS payment_reminder_sent_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS before_event_reminder_sent_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE registrations ADD COLUMN IF NOT EXISTS moderated_at TIMESTAMPTZ;")

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_reg_event_status ON registrations(event_id, status);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_reg_tg_event ON registrations(telegram_id, event_id);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_reg_reservation_expires ON registrations(reservation_expires_at);"
        )
        cleanup_duplicate_blocking_registrations(cur)
        backfill_missing_payment_codes(cur)

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_status_date ON events(status, event_date);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_ask_has_car_status ON events(ask_has_car, status);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_has_car ON users(has_car);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_partner_requests_status_created ON partner_requests(status, created_at DESC);"
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_reg_one_active_per_user_event
            ON registrations(event_id, telegram_id)
            WHERE status IN ('waiting_payment', 'waiting_moderation', 'approved', 'waiting_list');
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_reg_payment_code
            ON registrations(payment_code)
            WHERE payment_code IS NOT NULL;
            """
        )
        conn.commit()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def normalize_phone(raw_phone: str) -> Optional[str]:
    phone = (raw_phone or "").strip().replace(" ", "")
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 10 or len(digits) > 15:
        return None
    if phone.startswith("8") and len(digits) == 11:
        return "+7" + digits[1:]
    if phone.startswith("+"):
        return "+" + digits
    return "+" + digits


def parse_event_datetime(event_date_value, event_time_value: str) -> Optional[datetime]:
    if event_date_value is None:
        return None
    date_str = str(event_date_value)
    time_raw = (event_time_value or "").strip()
    match = re.search(r"(\d{1,2}):(\d{2})", time_raw)
    if not match:
        logger.warning("Could not parse event time: %s", time_raw)
        return None
    hours = int(match.group(1))
    minutes = int(match.group(2))
    try:
        return datetime.strptime(f"{date_str} {hours:02d}:{minutes:02d}", "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
    except ValueError:
        logger.warning("Invalid event datetime: %s %s", date_str, time_raw)
        return None


def format_price(value) -> str:
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        if value == value.to_integral():
            return str(int(value))
        return str(value)
    return str(value)


def human_event_status(status: str) -> str:
    return EVENT_STATUS_LABELS.get(status, status)


def human_registration_status(status: str) -> str:
    return STATUS_LABELS.get(status, status)


def human_has_car(value) -> str:
    if value is True:
        return "Да"
    if value is False:
        return "Нет"
    return "Не указано"


def format_rub_amount(value) -> str:
    if value is None:
        return "0"
    try:
        amount = Decimal(str(value))
        if amount == amount.to_integral():
            return f"{int(amount):,}".replace(",", " ")
        return f"{amount:,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return format_price(value)


def build_payment_details_text(reg) -> str:
    payment_code = html.escape(str(reg.get("payment_code") or ""))
    return (
        f"Сумма к оплате: <b>{html.escape(format_rub_amount(reg.get('price')))} ₽</b>\n\n"
        f"Оплата по СБП\n"
        f"Номер телефона:\n<code>{html.escape(SBP_PHONE)}</code>\n\n"
        f"Получатель:\n{html.escape(SBP_RECEIVER)}\n\n"
        f"Комментарий к переводу:\n<code>{payment_code}</code>\n\n"
        "Важно: укажите комментарий точно как в примере."
    )



def build_payment_action_keyboard(registration_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Я оплатил", callback_data=f"paid:{registration_id}")],
            [InlineKeyboardButton("Отменить заявку", callback_data=f"cancel_reg:{registration_id}")],
        ]
    )


async def send_existing_payment_prompt(message, reg) -> None:
    expires_at = reg.get("reservation_expires_at")
    if expires_at and expires_at <= now_local():
        update_registration_status(reg["id"], "expired", "expired")
        await message.reply_text(
            "Время на оплату по вашей заявке уже истекло. Нажмите «Участвовать», чтобы создать новую заявку.",
            reply_markup=main_menu_keyboard(),
        )
        return

    expires_text = expires_at.astimezone(LOCAL_TZ).strftime("%H:%M") if expires_at else "—"
    text = (
        "<b>Заявка почти завершена ✅</b>\n\n"
        f"Мероприятие: <b>{html.escape(reg['title'])}</b>\n\n"
        f"⏳ Бронь места действует до <b>{expires_text}</b>.\n\n"
        f"{build_payment_details_text(reg)}\n\n"
        "После перевода нажмите кнопку «Я оплатил».\n"
        "Если передумали — нажмите «Отменить заявку»."
    )
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=build_payment_action_keyboard(reg["id"]))


def should_ask_car_question(event_row, user_row) -> bool:
    return bool(event_row and event_row.get("ask_has_car")) and user_row is not None and user_row.get("has_car") is None


def upsert_user_profile(
    telegram_id: int,
    username: Optional[str],
    full_name: str,
    age: int,
    gender: str,
    city: str,
    phone: str,
    consent_personal_data: bool,
    has_car: Optional[bool] = None,
) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (
                telegram_id, username, full_name, age, gender, city, phone, has_car,
                consent_personal_data, consent_at, profile_completed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CASE WHEN %s THEN NOW() ELSE NULL END, TRUE)
            ON CONFLICT (telegram_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                full_name = EXCLUDED.full_name,
                age = EXCLUDED.age,
                gender = EXCLUDED.gender,
                city = EXCLUDED.city,
                phone = EXCLUDED.phone,
                has_car = COALESCE(EXCLUDED.has_car, users.has_car),
                consent_personal_data = EXCLUDED.consent_personal_data,
                consent_at = CASE
                    WHEN EXCLUDED.consent_personal_data THEN NOW()
                    ELSE users.consent_at
                END,
                profile_completed = TRUE,
                updated_at = NOW();
            """,
            (
                telegram_id,
                username,
                full_name,
                age,
                gender,
                city,
                phone,
                has_car,
                consent_personal_data,
                consent_personal_data,
            ),
        )
        conn.commit()


def get_user_profile(telegram_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
        return cur.fetchone()


def update_user_has_car(telegram_id: int, has_car: bool) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET has_car = %s, updated_at = NOW() WHERE telegram_id = %s",
            (has_car, telegram_id),
        )
        conn.commit()


def create_event(
    title: str,
    event_date: str,
    event_time: str,
    location: str,
    price: Decimal,
    description: str,
    total_limit: int,
    min_age: int,
    max_age: int,
    gender_balance_enabled: bool,
    ask_has_car: bool = False,
    poster_file_id: Optional[str] = None,
    poster_file_unique_id: Optional[str] = None,
) -> int:
    male_limit = female_limit = None
    if gender_balance_enabled:
        male_limit = total_limit // 2
        female_limit = total_limit // 2
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (
                title, event_date, event_time, location, price, description, total_limit,
                min_age, max_age, gender_balance_enabled, male_limit, female_limit,
                ask_has_car, poster_file_id, poster_file_unique_id, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'upcoming')
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
                min_age,
                max_age,
                gender_balance_enabled,
                male_limit,
                female_limit,
                ask_has_car,
                poster_file_id,
                poster_file_unique_id,
            ),
        )
        event_id = cur.fetchone()["id"]
        conn.commit()
        return event_id


def list_events(limit: int = 20):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM events
            ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END, event_date ASC, created_at ASC
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


def update_event_fields(event_id: int, **fields) -> None:
    if not fields:
        return
    allowed = {
        'title', 'event_date', 'event_time', 'location', 'price', 'description',
        'total_limit', 'min_age', 'max_age', 'male_limit', 'female_limit',
        'ask_has_car', 'poster_file_id', 'poster_file_unique_id', 'status'
    }
    invalid = [key for key in fields if key not in allowed]
    if invalid:
        raise ValueError(f'Unsupported fields: {invalid}')
    assignments = ', '.join(f"{key} = %s" for key in fields.keys())
    values = list(fields.values()) + [event_id]
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE events SET {assignments} WHERE id = %s", values)
        conn.commit()


def create_partner_request(
    telegram_id: int,
    username: Optional[str],
    contact_name: str,
    project_name: str,
    description: str,
    contact_value: str,
) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO partner_requests (
                telegram_id, username, contact_name, project_name, description, contact_value
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (telegram_id, username, contact_name, project_name, description, contact_value),
        )
        row = cur.fetchone()
        conn.commit()
        return row['id']


def set_event_status(event_id: int, new_status: str) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        if new_status == "active":
            cur.execute("UPDATE events SET status = 'upcoming' WHERE status = 'active'")
        cur.execute("UPDATE events SET status = %s WHERE id = %s", (new_status, event_id))
        conn.commit()


def delete_event(event_id: int) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM events WHERE id = %s", (event_id,))
        conn.commit()


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


def get_blocking_registration_for_user_event(telegram_id: int, event_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM registrations
            WHERE telegram_id = %s
              AND event_id = %s
              AND status IN ('waiting_payment', 'waiting_moderation', 'approved', 'waiting_list')
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (telegram_id, event_id),
        )
        return cur.fetchone()



def get_latest_waiting_payment_registration_for_user(telegram_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location, e.price, e.status AS event_status
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            WHERE r.telegram_id = %s
              AND r.status = 'waiting_payment'
            ORDER BY r.created_at DESC, r.id DESC
            LIMIT 1
            """,
            (telegram_id,),
        )
        return cur.fetchone()


def count_registrations_by_status(event_id: int) -> dict[str, int]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM registrations
            WHERE event_id = %s
            GROUP BY status
            """,
            (event_id,),
        )
        rows = cur.fetchall()
    data = {row["status"]: int(row["cnt"]) for row in rows}
    for key in [
        "waiting_payment",
        "waiting_moderation",
        "approved",
        "rejected",
        "cancelled",
        "waiting_list",
        "expired",
    ]:
        data.setdefault(key, 0)
    return data


def count_active_registrations(event_id: int) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM registrations
            WHERE event_id = %s
              AND status IN ('waiting_payment', 'waiting_moderation', 'approved')
            """,
            (event_id,),
        )
        row = cur.fetchone()
        return int(row["cnt"]) if row else 0


def count_active_registrations_by_gender(event_id: int, gender: str) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM registrations
            WHERE event_id = %s
              AND gender_snapshot = %s
              AND status IN ('waiting_payment', 'waiting_moderation', 'approved')
            """,
            (event_id, gender),
        )
        row = cur.fetchone()
        return int(row["cnt"]) if row else 0


def check_age_allowed(event_row, age: int) -> tuple[bool, str]:
    min_age = int(event_row.get("min_age") or 18)
    max_age = int(event_row.get("max_age") or 99)
    if age < min_age or age > max_age:
        return False, f"Возраст для этого мероприятия: от {min_age} до {max_age} лет."
    return True, ""


def check_slot_available(event_row, gender: str) -> tuple[bool, str]:
    total_used = count_active_registrations(event_row["id"])
    if total_used >= event_row["total_limit"]:
        return False, "Свободных мест на это мероприятие больше нет."

    if event_row["gender_balance_enabled"]:
        used_for_gender = count_active_registrations_by_gender(event_row["id"], gender)
        limit_for_gender = event_row["male_limit"] if gender == "male" else event_row["female_limit"]
        if limit_for_gender is not None and used_for_gender >= limit_for_gender:
            label = GENDER_LABELS.get(gender, "этой категории")
            return False, f"Места для категории «{label}» уже закончились."

    return True, ""


def can_join_waiting_list(event_row, user_row) -> tuple[bool, str]:
    age_ok, age_reason = check_age_allowed(event_row, user_row["age"])
    if not age_ok:
        return False, age_reason
    slot_ok, slot_reason = check_slot_available(event_row, user_row["gender"])
    if slot_ok:
        return False, "Сейчас место доступно сразу — лист ожидания не нужен."
    return True, slot_reason


def create_registration_from_profile(event_row, user_row, status: str = "waiting_payment") -> int:
    expires_at = None
    if status == "waiting_payment":
        expires_at = now_local() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

    with get_conn() as conn, conn.cursor() as cur:
        for _ in range(20):
            payment_code = generate_unique_payment_code(cur)
            try:
                cur.execute(
                    """
                    INSERT INTO registrations (
                        event_id, telegram_id, name_snapshot, age_snapshot, gender_snapshot,
                        city_snapshot, phone_snapshot, has_car_snapshot, consent_snapshot, payment_code,
                        status, payment_status, reservation_expires_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        user_row.get("has_car"),
                        bool(user_row.get("consent_personal_data")),
                        payment_code,
                        status,
                        "not_paid" if status == "waiting_payment" else "not_required",
                        expires_at,
                    ),
                )
                row = cur.fetchone()
                conn.commit()
                return row["id"]
            except UniqueViolation as exc:
                conn.rollback()
                constraint_name = getattr(getattr(exc, "diag", None), "constraint_name", "") or ""
                if constraint_name == "uniq_reg_one_active_per_user_event":
                    raise DuplicateRegistrationError
                if constraint_name == "uniq_reg_payment_code":
                    continue
                raise
        raise RuntimeError("Could not create registration with unique payment code")


def get_registration(registration_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location, e.price, e.status AS event_status,
                   e.total_limit, e.gender_balance_enabled, e.min_age, e.max_age, e.ask_has_car,
                   u.has_car AS user_has_car
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            LEFT JOIN users u ON u.telegram_id = r.telegram_id
            WHERE r.id = %s
            """,
            (registration_id,),
        )
        return cur.fetchone()


def get_waiting_list_for_event(event_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location, e.price
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            WHERE r.event_id = %s AND r.status = 'waiting_list'
            ORDER BY r.created_at ASC, r.id ASC
            """,
            (event_id,),
        )
        return cur.fetchall()


def get_approved_registrations_for_event(event_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location, e.ask_has_car,
                   u.has_car AS user_has_car
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            LEFT JOIN users u ON u.telegram_id = r.telegram_id
            WHERE r.event_id = %s AND r.status = 'approved'
            ORDER BY r.created_at ASC, r.id ASC
            """,
            (event_id,),
        )
        return cur.fetchall()


def update_registration_status(registration_id: int, status: str, payment_status: Optional[str] = None) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        if payment_status is None:
            cur.execute(
                """
                UPDATE registrations
                SET status = %s,
                    reservation_expires_at = CASE
                        WHEN %s IN ('waiting_payment') THEN reservation_expires_at
                        ELSE NULL
                    END,
                    moderated_at = CASE WHEN %s IN ('approved', 'rejected') THEN NOW() ELSE moderated_at END
                WHERE id = %s
                """,
                (status, status, status, registration_id),
            )
        else:
            cur.execute(
                """
                UPDATE registrations
                SET status = %s,
                    payment_status = %s,
                    reservation_expires_at = CASE
                        WHEN %s IN ('waiting_payment') THEN reservation_expires_at
                        ELSE NULL
                    END,
                    moderated_at = CASE WHEN %s IN ('approved', 'rejected') THEN NOW() ELSE moderated_at END
                WHERE id = %s
                """,
                (status, payment_status, status, status, registration_id),
            )
        conn.commit()


def set_registration_waiting_payment(registration_id: int) -> datetime:
    expires_at = now_local() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE registrations
            SET status = 'waiting_payment',
                payment_status = 'not_paid',
                reservation_expires_at = %s,
                payment_reminder_sent_at = NULL
            WHERE id = %s
            """,
            (expires_at, registration_id),
        )
        conn.commit()
    return expires_at


def mark_payment_reminder_sent(registration_id: int) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE registrations SET payment_reminder_sent_at = NOW() WHERE id = %s",
            (registration_id,),
        )
        conn.commit()


def mark_before_event_reminder_sent(registration_id: int) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE registrations SET before_event_reminder_sent_at = NOW() WHERE id = %s",
            (registration_id,),
        )
        conn.commit()


def get_due_payment_reminders():
    threshold = now_local() + timedelta(minutes=PAYMENT_REMINDER_BEFORE_MINUTES)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location, e.price
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            WHERE r.status = 'waiting_payment'
              AND r.payment_reminder_sent_at IS NULL
              AND r.reservation_expires_at IS NOT NULL
              AND r.reservation_expires_at <= %s
            ORDER BY r.reservation_expires_at ASC
            """,
            (threshold,),
        )
        return cur.fetchall()


def get_expired_registrations():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location, e.price
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            WHERE r.status = 'waiting_payment'
              AND r.reservation_expires_at IS NOT NULL
              AND r.reservation_expires_at <= NOW()
            ORDER BY r.reservation_expires_at ASC
            """
        )
        return cur.fetchall()


def get_due_event_reminders():
    window_end = now_local() + timedelta(hours=EVENT_REMINDER_BEFORE_HOURS)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.*, e.title, e.event_date, e.event_time, e.location, e.price, e.status AS event_status
            FROM registrations r
            JOIN events e ON e.id = r.event_id
            WHERE r.status = 'approved'
              AND r.before_event_reminder_sent_at IS NULL
            ORDER BY e.event_date ASC, r.id ASC
            """
        )
        rows = cur.fetchall()

    due = []
    now_dt = now_local()
    for row in rows:
        event_dt = parse_event_datetime(row["event_date"], row["event_time"])
        if not event_dt:
            continue
        if now_dt <= event_dt <= window_end:
            due.append(row)
    return due


def render_event_text(event_row) -> str:
    balance = "Включен" if event_row["gender_balance_enabled"] else "Выключен"
    extra = ""
    if event_row["gender_balance_enabled"]:
        extra = f"\nБаланс М/Ж: {event_row['male_limit']}/{event_row['female_limit']}"
    description = html.escape(event_row.get("description") or "")
    return (
        f"<b>{html.escape(event_row['title'])}</b>\n"
        f"ID: {event_row['id']}\n"
        f"Дата: {event_row['event_date']}\n"
        f"Время: {html.escape(event_row['event_time'])}\n"
        f"Место: {html.escape(event_row['location'])}\n"
        f"Цена: {format_price(event_row['price'])}\n"
        f"Статус: {html.escape(human_event_status(event_row['status']))}\n"
        f"Лимит: {event_row['total_limit']}\n"
        f"Возраст: {event_row.get('min_age', 18)}–{event_row.get('max_age', 99)}\n"
        f"Вопрос про авто: {'Да' if event_row.get('ask_has_car') else 'Нет'}\n"
        f"50/50: {balance}{extra}"
        + (f"\nОписание: {description}" if description else "")
    )


def render_public_event_text(event_row) -> str:
    description = html.escape(event_row.get("description") or "")
    places_left = max(0, int(event_row['total_limit']) - count_active_registrations(event_row['id']))
    return (
        f"<b>{html.escape(event_row['title'])}</b>\n"
        f"Дата: {event_row['event_date']}\n"
        f"Время: {html.escape(event_row['event_time'])}\n"
        f"Место: {html.escape(event_row['location'])}\n"
        f"Цена: {format_price(event_row['price'])}\n"
        f"Мест осталось: {places_left}\n"
        f"Возраст: {event_row.get('min_age', 18)}–{event_row.get('max_age', 99)}"
        + (f"\nОписание: {description}" if description else "")
    )


def render_profile_text(user_row) -> str:
    consent_label = "Да" if user_row.get("consent_personal_data") else "Нет"
    lines = [
        "<b>Ваша анкета</b>",
        f"Имя: {html.escape(user_row['full_name'])}",
        f"Возраст: {user_row['age']}",
        f"Пол: {GENDER_LABELS.get(user_row['gender'], user_row['gender'])}",
        f"Город: {html.escape(user_row['city'])}",
        f"Телефон: {html.escape(user_row['phone'])}",
    ]
    if user_row.get("has_car") is not None:
        lines.append(f"Автомобиль: {human_has_car(user_row.get('has_car'))}")
    lines.append(f"Согласие на обработку данных: {consent_label}")
    return "\n".join(lines)


def build_event_stats_text(event_row) -> str:
    counts = count_registrations_by_status(event_row["id"])
    approved = counts["approved"]
    total_limit = max(int(event_row["total_limit"]), 1)
    all_non_waitlist = approved + counts["waiting_payment"] + counts["waiting_moderation"] + counts["rejected"] + counts["cancelled"] + counts["expired"]
    confirm_share = round((approved / total_limit) * 100, 1)
    approval_rate = round((approved / all_non_waitlist) * 100, 1) if all_non_waitlist else 0
    return (
        f"<b>Статистика по мероприятию #{event_row['id']}</b>\n"
        f"Название: {html.escape(event_row['title'])}\n"
        f"Вопрос про авто: {'Да' if event_row.get('ask_has_car') else 'Нет'}\n"
        f"Подтверждено: {approved}/{event_row['total_limit']} ({confirm_share}%)\n"
        f"Approval rate: {approval_rate}%\n"
        f"Ожидают оплату: {counts['waiting_payment']}\n"
        f"На модерации: {counts['waiting_moderation']}\n"
        f"Лист ожидания: {counts['waiting_list']}\n"
        f"Отклонено: {counts['rejected']}\n"
        f"Отменено: {counts['cancelled']}\n"
        f"Истек таймер оплаты: {counts['expired']}"
    )


def render_partner_request_text(partner_id: int, data: dict) -> str:
    return (
        f"<b>Новая заявка на партнерство</b>\n"
        f"ID: {partner_id}\n"
        f"Имя: {html.escape(data['contact_name'])}\n"
        f"Проект: {html.escape(data['project_name'])}\n"
        f"Описание: {html.escape(data['description'])}\n"
        f"Контакт: {html.escape(data['contact_value'])}\n"
        f"Telegram ID: {data['telegram_id']}"
        + (f"\nUsername: @{html.escape(data['username'])}" if data.get('username') else "")
    )


def build_edit_event_keyboard(event_row) -> InlineKeyboardMarkup:
    event_id = event_row["id"]
    poster_label = "Редактировать афишу" if event_row.get("poster_file_id") else "Добавить афишу"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(poster_label, callback_data=f"edit_event_field:poster:{event_id}")],
            [
                InlineKeyboardButton("Название", callback_data=f"edit_event_field:title:{event_id}"),
                InlineKeyboardButton("Дата", callback_data=f"edit_event_field:event_date:{event_id}"),
            ],
            [
                InlineKeyboardButton("Время", callback_data=f"edit_event_field:event_time:{event_id}"),
                InlineKeyboardButton("Место", callback_data=f"edit_event_field:location:{event_id}"),
            ],
            [
                InlineKeyboardButton("Цена", callback_data=f"edit_event_field:price:{event_id}"),
                InlineKeyboardButton("Описание", callback_data=f"edit_event_field:description:{event_id}"),
            ],
            [
                InlineKeyboardButton("Лимит", callback_data=f"edit_event_field:total_limit:{event_id}"),
                InlineKeyboardButton("Мин. возраст", callback_data=f"edit_event_field:min_age:{event_id}"),
            ],
            [
                InlineKeyboardButton("Макс. возраст", callback_data=f"edit_event_field:max_age:{event_id}"),
                InlineKeyboardButton("Вопрос про авто", callback_data=f"edit_event_field:ask_has_car:{event_id}"),
            ],
            [InlineKeyboardButton("Удалить мероприятие", callback_data=f"delete_event_prompt:{event_id}")],
        ]
    )


async def send_event_card(message, event_row, *, public: bool, intro: Optional[str] = None, extra_text: Optional[str] = None, reply_markup=None) -> None:
    text = render_public_event_text(event_row) if public else render_event_text(event_row)
    parts = [part for part in [intro, text, extra_text] if part]
    full_payload = "\n\n".join(parts)

    if event_row.get("poster_file_id"):
        if len(full_payload) <= 1024:
            caption = full_payload
        else:
            caption_parts = [part for part in [intro, text] if part]
            caption = "\n\n".join(caption_parts)
            if len(caption) > 1024:
                caption = caption[:1020] + "…"
        await message.reply_photo(
            photo=event_row["poster_file_id"],
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
        )
        if extra_text and len(full_payload) > 1024:
            await message.reply_text(extra_text, parse_mode=ParseMode.HTML)
    else:
        await message.reply_text(full_payload, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Участвовать"], ["Партнерство"], ["Мои данные"]],
        resize_keyboard=True,
    )


async def ask_user_has_car(message) -> None:
    keyboard = ReplyKeyboardMarkup(YES_NO_KEYBOARD, resize_keyboard=True, one_time_keyboard=True)
    await message.reply_text(
        "Для этого мероприятия нужно уточнить: у вас есть автомобиль?",
        reply_markup=keyboard,
    )


def build_pay_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Перейти к оплате", callback_data=f"pay:{event_id}")],
            [InlineKeyboardButton("Изменить данные", callback_data="edit_profile:participate")],
        ]
    )


def build_waiting_list_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Встать в лист ожидания", callback_data=f"join_waitlist:{event_id}")],
            [InlineKeyboardButton("Изменить данные", callback_data="edit_profile:participate")],
        ]
    )


def build_active_event_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Редактировать", callback_data=f"edit_event_menu:{event_id}")],
            [
                InlineKeyboardButton("Сделать активным", callback_data=f"activate:{event_id}"),
                InlineKeyboardButton("Закрыть набор", callback_data=f"close:{event_id}"),
            ],
            [
                InlineKeyboardButton("Статистика", callback_data=f"stats:{event_id}"),
                InlineKeyboardButton("Экспорт confirmed", callback_data=f"export:{event_id}"),
            ],
            [InlineKeyboardButton("Напомнить confirmed", callback_data=f"notify_confirmed:{event_id}")],
            [InlineKeyboardButton("Удалить мероприятие", callback_data=f"delete_event_prompt:{event_id}")],
        ]
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pending_payment = get_latest_waiting_payment_registration_for_user(update.effective_user.id)
    if pending_payment:
        await send_existing_payment_prompt(update.effective_message, pending_payment)
        return

    text = (
        "Привет. Здесь можно быстро записаться на актуальное мероприятие или оставить заявку на партнёрство.\n\n"
        "Используйте кнопки ниже: «Участвовать», «Партнерство» или «Мои данные»."
    )
    await update.effective_message.reply_text(text, reply_markup=main_menu_keyboard())


async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    profile = get_user_profile(update.effective_user.id)
    if not profile or not profile["profile_completed"]:
        await update.effective_message.reply_text(
            "Анкета пока не заполнена. Нажмите «Участвовать», и бот соберет данные один раз.",
            reply_markup=main_menu_keyboard(),
        )
        return

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Изменить данные", callback_data="edit_profile:profile")]]
    )
    await update.effective_message.reply_text(
        render_profile_text(profile),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def participate_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    event_row = get_active_event()
    if not event_row:
        await update.effective_message.reply_text(
            "Сейчас нет активного мероприятия для записи.",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END

    latest_reg = get_blocking_registration_for_user_event(user.id, event_row["id"])
    if latest_reg:
        if latest_reg["status"] == "waiting_payment":
            reg = get_registration(latest_reg["id"])
            if reg:
                await send_existing_payment_prompt(update.effective_message, reg)
            else:
                await update.effective_message.reply_text(
                    "У вас уже есть незавершённая заявка. Либо оплатите её, либо отмените.",
                    reply_markup=main_menu_keyboard(),
                )
            return ConversationHandler.END

        await update.effective_message.reply_text(
            f"У вас уже есть заявка на это мероприятие. Текущий статус: {human_registration_status(latest_reg['status'])}.",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END

    profile = get_user_profile(user.id)
    context.user_data["profile_event_id"] = event_row["id"]
    if profile and profile["profile_completed"]:
        if should_ask_car_question(event_row, profile):
            await ask_user_has_car(update.effective_message)
            return PROFILE_HAS_CAR
        await send_event_and_profile_confirmation(update.effective_message, profile, event_row)
        return ConversationHandler.END

    context.user_data["profile_source"] = "participate"
    context.user_data["profile_form"] = {}
    await send_event_card(
        update.effective_message,
        event_row,
        public=True,
        intro="Сейчас открыта запись на:",
        extra_text="Как вас зовут? (Напишите ваше имя)",
        reply_markup=ReplyKeyboardRemove(),
    )
    return PROFILE_NAME


async def send_event_and_profile_confirmation(message, profile, event_row) -> None:
    age_ok, age_reason = check_age_allowed(event_row, profile["age"])
    if not age_ok:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Изменить данные", callback_data="edit_profile:participate")]]
        )
        await send_event_card(
            message,
            event_row,
            public=True,
            intro="Сейчас открыта запись на:",
            extra_text=f"{render_profile_text(profile)}\n\n⚠️ {html.escape(age_reason)}",
            reply_markup=keyboard,
        )
        return

    slot_ok, slot_reason = check_slot_available(event_row, profile["gender"])
    if slot_ok:
        await send_event_card(
            message,
            event_row,
            public=True,
            intro="Сейчас открыта запись на:",
            extra_text=(
                f"{render_profile_text(profile)}\n\n"
                f"Если все верно, переходите к оплате. После создания заявки место бронируется на {PAYMENT_TIMEOUT_MINUTES} минут."
            ),
            reply_markup=build_pay_keyboard(event_row["id"]),
        )
        return

    await send_event_card(
        message,
        event_row,
        public=True,
        intro="Сейчас открыта запись на:",
        extra_text=(
            f"{render_profile_text(profile)}\n\n"
            f"⚠️ {html.escape(slot_reason)}\n"
            "Можно встать в лист ожидания. Если место освободится, бот напишет автоматически."
        ),
        reply_markup=build_waiting_list_keyboard(event_row["id"]),
    )


async def edit_profile_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    source = query.data.split(":", 1)[1] if ":" in query.data else "profile"
    context.user_data["profile_source"] = source
    if source == "participate":
        event_row = get_active_event()
        if event_row:
            context.user_data["profile_event_id"] = event_row["id"]
    context.user_data["profile_form"] = {}
    await query.message.reply_text("Как вас зовут? (Напишите ваше имя)", reply_markup=ReplyKeyboardRemove())
    return PROFILE_NAME


async def profile_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = (update.message.text or "").strip()
    if len(name) < 2:
        await update.message.reply_text("Введите имя не короче 2 символов.")
        return PROFILE_NAME
    context.user_data.setdefault("profile_form", {})["full_name"] = name
    await update.message.reply_text("Сколько вам лет?")
    return PROFILE_AGE


async def profile_age(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.message.reply_text("Возраст нужно ввести числом.")
        return PROFILE_AGE
    age = int(text)
    if age < 18 or age > 99:
        await update.message.reply_text("Введите возраст от 18 до 99.")
        return PROFILE_AGE
    context.user_data.setdefault("profile_form", {})["age"] = age
    keyboard = ReplyKeyboardMarkup([["Мужской", "Женский"]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Укажите пол.", reply_markup=keyboard)
    return PROFILE_GENDER


async def profile_gender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text not in GENDER_MAP:
        await update.message.reply_text("Выберите пол кнопкой ниже.")
        return PROFILE_GENDER
    context.user_data.setdefault("profile_form", {})["gender"] = GENDER_MAP[text]
    keyboard = ReplyKeyboardMarkup([["Мариуполь"], ["Другой город"]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Укажите город.", reply_markup=keyboard)
    return PROFILE_CITY


async def profile_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = (update.message.text or "").strip()
    if city == "Другой город":
        await update.message.reply_text("Напишите ваш город текстом.", reply_markup=ReplyKeyboardRemove())
        return PROFILE_CITY
    if len(city) < 2:
        await update.message.reply_text("Введите корректный город.")
        return PROFILE_CITY
    context.user_data.setdefault("profile_form", {})["city"] = city
    await update.message.reply_text(
        "Введите телефон в формате +79991112233 или 89991112233.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return PROFILE_PHONE


async def profile_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone(update.message.text or "")
    if not phone:
        await update.message.reply_text("Введите корректный номер телефона.")
        return PROFILE_PHONE

    form = context.user_data.get("profile_form", {})
    form["phone"] = phone
    keyboard = ReplyKeyboardMarkup([["Согласен", "Не согласен"]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text(
        "Подтверждаете согласие на обработку персональных данных для записи на мероприятие?",
        reply_markup=keyboard,
    )
    return PROFILE_CONSENT


async def profile_consent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text not in {"Согласен", "Не согласен"}:
        await update.message.reply_text("Пожалуйста, выберите один из вариантов кнопкой ниже.")
        return PROFILE_CONSENT
    if text == "Не согласен":
        await update.message.reply_text(
            "Без согласия на обработку персональных данных запись недоступна.",
            reply_markup=main_menu_keyboard(),
        )
        context.user_data.pop("profile_form", None)
        context.user_data.pop("profile_source", None)
        return ConversationHandler.END

    form = context.user_data.get("profile_form", {})
    upsert_user_profile(
        telegram_id=update.effective_user.id,
        username=update.effective_user.username,
        full_name=form["full_name"],
        age=form["age"],
        gender=form["gender"],
        city=form["city"],
        phone=form["phone"],
        consent_personal_data=True,
    )

    source = context.user_data.get("profile_source", "profile")
    profile_event_id = context.user_data.get("profile_event_id")
    context.user_data.pop("profile_form", None)
    context.user_data.pop("profile_source", None)
    profile = get_user_profile(update.effective_user.id)

    if source == "participate":
        event_row = get_event(profile_event_id) if profile_event_id else get_active_event()
        if not event_row or event_row["status"] != "active":
            await update.message.reply_text(
                "Анкета сохранена. Сейчас активное мероприятие не найдено.",
                reply_markup=main_menu_keyboard(),
            )
            context.user_data.pop("profile_event_id", None)
            return ConversationHandler.END
        if should_ask_car_question(event_row, profile):
            await ask_user_has_car(update.message)
            return PROFILE_HAS_CAR
        context.user_data.pop("profile_event_id", None)
        await send_event_and_profile_confirmation(update.message, profile, event_row)
    else:
        context.user_data.pop("profile_event_id", None)
        await update.message.reply_text(
            "Анкета сохранена.\n\n" + render_profile_text(profile),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
    return ConversationHandler.END


async def profile_has_car(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_value = (update.message.text or "").strip()
    if text_value not in {"Да", "Нет"}:
        await update.message.reply_text("Выберите вариант кнопкой ниже: «Да» или «Нет».")
        return PROFILE_HAS_CAR

    has_car = text_value == "Да"
    update_user_has_car(update.effective_user.id, has_car)
    profile = get_user_profile(update.effective_user.id)
    profile_event_id = context.user_data.pop("profile_event_id", None)
    context.user_data.pop("profile_form", None)
    context.user_data.pop("profile_source", None)

    event_row = get_event(profile_event_id) if profile_event_id else get_active_event()
    if event_row and event_row.get("status") == "active":
        await send_event_and_profile_confirmation(update.message, profile, event_row)
        return ConversationHandler.END

    await update.message.reply_text(
        "Данные сохранены.\n\n" + render_profile_text(profile),
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


async def cancel_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("profile_form", None)
    context.user_data.pop("profile_source", None)
    context.user_data.pop("profile_event_id", None)
    await update.effective_message.reply_text("Отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def partner_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["partner_form"] = {}
    await update.effective_message.reply_text(
        "Партнерство. Как вас зовут или как к вам обращаться?",
        reply_markup=ReplyKeyboardRemove(),
    )
    return PARTNER_CONTACT_NAME


async def partner_contact_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = (update.message.text or "").strip()
    if len(value) < 2:
        await update.message.reply_text("Введите имя или название контакта не короче 2 символов.")
        return PARTNER_CONTACT_NAME
    context.user_data.setdefault("partner_form", {})["contact_name"] = value
    await update.message.reply_text("Как называется ваш проект / компания?")
    return PARTNER_PROJECT_NAME


async def partner_project_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = (update.message.text or "").strip()
    if len(value) < 2:
        await update.message.reply_text("Введите название проекта.")
        return PARTNER_PROJECT_NAME
    context.user_data.setdefault("partner_form", {})["project_name"] = value
    await update.message.reply_text("Коротко опишите предложение или формат партнерства.")
    return PARTNER_DESCRIPTION


async def partner_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = (update.message.text or "").strip()
    if len(value) < 5:
        await update.message.reply_text("Опишите предложение чуть подробнее.")
        return PARTNER_DESCRIPTION
    context.user_data.setdefault("partner_form", {})["description"] = value
    await update.message.reply_text("Оставьте телефон или @telegram для связи.")
    return PARTNER_CONTACT


async def partner_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or "").strip()
    if not raw:
        await update.message.reply_text("Введите телефон или @telegram.")
        return PARTNER_CONTACT
    contact_value = raw
    if raw.startswith("@"):
        if len(raw) < 5:
            await update.message.reply_text("Введите корректный @telegram.")
            return PARTNER_CONTACT
    else:
        normalized = normalize_phone(raw)
        if not normalized:
            await update.message.reply_text("Введите корректный телефон или @telegram.")
            return PARTNER_CONTACT
        contact_value = normalized

    form = context.user_data.get("partner_form", {})
    form["contact_value"] = contact_value
    partner_id = create_partner_request(
        telegram_id=update.effective_user.id,
        username=update.effective_user.username,
        contact_name=form["contact_name"],
        project_name=form["project_name"],
        description=form["description"],
        contact_value=form["contact_value"],
    )

    if MODERATION_CHAT_ID:
        await context.bot.send_message(
            chat_id=MODERATION_CHAT_ID,
            text=render_partner_request_text(
                partner_id,
                {
                    "telegram_id": update.effective_user.id,
                    "username": update.effective_user.username,
                    "contact_name": form["contact_name"],
                    "project_name": form["project_name"],
                    "description": form["description"],
                    "contact_value": form["contact_value"],
                },
            ),
            parse_mode=ParseMode.HTML,
        )

    context.user_data.pop("partner_form", None)
    await update.message.reply_text(
        "Спасибо. Заявка на партнерство отправлена. Мы свяжемся с вами.",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


async def cancel_partner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("partner_form", None)
    await update.effective_message.reply_text("Отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def pay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    user_row = get_user_profile(query.from_user.id)

    if not event_row or event_row["status"] != "active":
        await query.message.reply_text("Сейчас это мероприятие недоступно для записи.")
        return
    if not user_row or not user_row["profile_completed"]:
        await query.message.reply_text("Сначала заполните анкету.")
        return
    if not user_row.get("consent_personal_data"):
        await query.message.reply_text("Нужно подтвердить согласие на обработку персональных данных.")
        return
    if should_ask_car_question(event_row, user_row):
        await query.message.reply_text(
            "Для этого мероприятия сначала нужно указать, есть ли у вас автомобиль. Нажмите «Участвовать» ещё раз."
        )
        return

    latest_reg = get_blocking_registration_for_user_event(query.from_user.id, event_id)
    if latest_reg:
        await query.message.reply_text(
            f"У вас уже есть заявка на это мероприятие. Статус: {human_registration_status(latest_reg['status'])}."
        )
        return

    age_ok, age_reason = check_age_allowed(event_row, user_row["age"])
    if not age_ok:
        await query.message.reply_text(age_reason)
        return

    slot_ok, slot_reason = check_slot_available(event_row, user_row["gender"])
    if not slot_ok:
        await query.message.reply_text(
            slot_reason,
            reply_markup=build_waiting_list_keyboard(event_id),
        )
        return

    try:
        registration_id = create_registration_from_profile(event_row, user_row, status="waiting_payment")
    except DuplicateRegistrationError:
        await query.message.reply_text("У вас уже есть активная заявка или запись в листе ожидания на это мероприятие.")
        return

    reg = get_registration(registration_id)
    expires_at = reg["reservation_expires_at"]
    expires_text = expires_at.astimezone(LOCAL_TZ).strftime("%H:%M") if expires_at else "—"
    keyboard = build_payment_action_keyboard(registration_id)
    text = (
        "<b>Заявка создана ✅</b>\n\n"
        f"Мероприятие: <b>{html.escape(event_row['title'])}</b>\n\n"
        f"⏳ Бронь места действует до <b>{expires_text}</b>.\n\n"
        f"{build_payment_details_text(reg)}\n\n"
        "После перевода нажмите кнопку «Я оплатил».\n"
        "Если передумали — нажмите «Отменить заявку»."
    )
    await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def join_waiting_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    user_row = get_user_profile(query.from_user.id)

    if not event_row or event_row["status"] != "active":
        await query.message.reply_text("Сейчас лист ожидания для этого мероприятия недоступен.")
        return
    if not user_row or not user_row["profile_completed"]:
        await query.message.reply_text("Сначала заполните анкету.")
        return

    existing = get_blocking_registration_for_user_event(query.from_user.id, event_id)
    if existing:
        await query.message.reply_text(
            f"У вас уже есть запись на это мероприятие. Статус: {human_registration_status(existing['status'])}."
        )
        return

    can_join, reason = can_join_waiting_list(event_row, user_row)
    if not can_join:
        await query.message.reply_text(reason)
        return

    try:
        registration_id = create_registration_from_profile(event_row, user_row, status="waiting_list")
    except DuplicateRegistrationError:
        await query.message.reply_text("Вы уже есть в активной заявке или листе ожидания.")
        return

    reg = get_registration(registration_id)
    position = len(get_waiting_list_for_event(event_id))
    await query.message.reply_text(
        f"Вы добавлены в лист ожидания на <b>{html.escape(reg['title'])}</b>.\n"
        f"Текущая позиция: {position}.\n\n"
        "Если место освободится, бот автоматически пришлет сообщение и даст время на оплату.",
        parse_mode=ParseMode.HTML,
    )


async def paid_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    registration_id = int(query.data.split(":", 1)[1])
    reg = get_registration(registration_id)
    if not reg:
        await query.message.reply_text("Заявка не найдена.")
        return
    if reg["telegram_id"] != query.from_user.id:
        await query.message.reply_text("Это не ваша заявка.")
        return
    if reg["status"] != "waiting_payment":
        await query.message.reply_text("Эта заявка уже обработана или отправлена на модерацию.")
        return

    update_registration_status(registration_id, "waiting_moderation", "claimed_paid")
    await query.message.reply_text("Спасибо. Заявка отправлена на модерацию. Скоро подтвердим бронь.")

    if MODERATION_CHAT_ID:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Подтвердить", callback_data=f"approve:{registration_id}"),
                    InlineKeyboardButton("Отклонить", callback_data=f"reject:{registration_id}"),
                ]
            ]
        )
        mod_text = (
            f"<b>Новая заявка на модерацию</b>\n"
            f"ID заявки: {registration_id}\n"
            f"Код оплаты: {html.escape(str(reg.get('payment_code') or '—'))}\n"
            f"Ивент: {html.escape(reg['title'])}\n"
            f"Дата: {reg['event_date']} {html.escape(reg['event_time'])}\n"
            f"Имя: {html.escape(reg['name_snapshot'])}\n"
            f"Возраст: {reg['age_snapshot']}\n"
            f"Пол: {GENDER_LABELS.get(reg['gender_snapshot'], reg['gender_snapshot'])}\n"
            f"Город: {html.escape(reg['city_snapshot'])}\n"
            f"Телефон: {html.escape(reg['phone_snapshot'])}\n"
            + (f"Автомобиль: {human_has_car(reg.get('has_car_snapshot'))}\n" if reg.get('ask_has_car') else "")
            + f"Согласие ПД: {'Да' if reg['consent_snapshot'] else 'Нет'}"
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
        await query.message.reply_text("Заявка не найдена.")
        return
    if reg["telegram_id"] != query.from_user.id:
        await query.message.reply_text("Это не ваша заявка.")
        return
    if reg["status"] not in ("waiting_payment", "waiting_moderation", "waiting_list"):
        await query.message.reply_text("Эту заявку уже нельзя отменить.")
        return

    new_payment_status = "cancelled" if reg["status"] != "waiting_list" else "not_required"
    update_registration_status(registration_id, "cancelled", new_payment_status)
    await query.message.reply_text("Заявка отменена. Слот освобожден.")
    await promote_waiting_list_for_event(context, reg["event_id"])


async def moderation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа к модерации.")
        return

    action, raw_id = query.data.split(":", 1)
    registration_id = int(raw_id)
    reg = get_registration(registration_id)
    if not reg:
        await query.message.reply_text("Заявка не найдена.")
        return
    if reg["status"] != "waiting_moderation":
        await query.message.reply_text(f"Эта заявка уже имеет статус: {reg['status']}")
        return

    if action == "approve":
        update_registration_status(registration_id, "approved", "paid")
        await context.bot.send_message(
            chat_id=reg["telegram_id"],
            text=(
                f"Ваша бронь подтверждена ✅\n\n"
                f"Мероприятие: {reg['title']}\n"
                f"Дата: {reg['event_date']} {reg['event_time']}\n"
                f"Место: {reg['location']}"
            ),
        )
        await query.edit_message_text(
            query.message.text_html + "\n\n✅ Подтверждено",
            parse_mode=ParseMode.HTML,
        )
    elif action == "reject":
        update_registration_status(registration_id, "rejected", "rejected")
        await context.bot.send_message(
            chat_id=reg["telegram_id"],
            text="К сожалению, заявка отклонена. Слот освобожден.",
        )
        await query.edit_message_text(
            query.message.text_html + "\n\n❌ Отклонено",
            parse_mode=ParseMode.HTML,
        )
        await promote_waiting_list_for_event(context, reg["event_id"])


async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("У вас нет доступа к админ-панели.")
        return
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Добавить мероприятие", callback_data="admin_add_event")],
            [InlineKeyboardButton("Список мероприятий", callback_data="admin_list_events")],
            [InlineKeyboardButton("Активное мероприятие", callback_data="admin_active_event")],
            [InlineKeyboardButton("Редактировать текущее мероприятие", callback_data="admin_edit_current_event")],
        ]
    )
    await update.effective_message.reply_text("Админ-панель", reply_markup=keyboard)


async def admin_show_active_event(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_row = get_active_event()
    if not event_row:
        await query.message.reply_text("Сейчас нет активного мероприятия.")
        return
    await send_event_card(
        query.message,
        event_row,
        public=False,
        intro="Активное мероприятие:",
        extra_text=build_event_stats_text(event_row),
        reply_markup=build_active_event_keyboard(event_row["id"]),
    )


async def admin_edit_current_event(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_row = get_active_event()
    if not event_row:
        await query.message.reply_text("Сейчас нет активного мероприятия для редактирования.")
        return
    await send_event_card(
        query.message,
        event_row,
        public=False,
        intro="Редактирование текущего мероприятия:",
        reply_markup=build_edit_event_keyboard(event_row),
    )


async def admin_delete_event_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("Мероприятие не найдено.")
        return
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Да, удалить", callback_data=f"delete_event_confirm:{event_id}"),
                InlineKeyboardButton("Отмена", callback_data=f"delete_event_cancel:{event_id}"),
            ]
        ]
    )
    await query.message.reply_text(
        f"Удалить мероприятие <b>{html.escape(event_row['title'])}</b>?\n\nЭто удалит само мероприятие, заявки и связанные данные по нему.",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def admin_delete_event_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("Мероприятие уже удалено.")
        return
    title = event_row["title"]
    delete_event(event_id)
    await query.message.reply_text(f"Мероприятие «{title}» удалено.")


async def admin_delete_event_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Удаление отменено")
    await query.message.reply_text("Удаление мероприятия отменено.")


async def admin_add_event_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return ConversationHandler.END
    context.user_data["new_event"] = {}
    await query.message.reply_text("Название мероприятия?")
    return EVENT_TITLE


async def admin_event_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = (update.message.text or "").strip()
    if len(title) < 3:
        await update.message.reply_text("Введите нормальное название мероприятия.")
        return EVENT_TITLE
    context.user_data.setdefault("new_event", {})["title"] = title
    await update.message.reply_text("Дата мероприятия? Формат: YYYY-MM-DD")
    return EVENT_DATE


async def admin_event_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("Неверный формат даты. Используйте YYYY-MM-DD")
        return EVENT_DATE
    context.user_data.setdefault("new_event", {})["event_date"] = text
    await update.message.reply_text("Время? Например: 19:00")
    return EVENT_TIME


async def admin_event_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = (update.message.text or "").strip()
    if not re.search(r"\d{1,2}:\d{2}", value):
        await update.message.reply_text("Введите время в формате 19:00")
        return EVENT_TIME
    context.user_data.setdefault("new_event", {})["event_time"] = value
    await update.message.reply_text("Место проведения?")
    return EVENT_LOCATION


async def admin_event_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = (update.message.text or "").strip()
    if len(value) < 2:
        await update.message.reply_text("Введите место проведения.")
        return EVENT_LOCATION
    context.user_data.setdefault("new_event", {})["location"] = value
    await update.message.reply_text("Стоимость участия? Только число, например 1000")
    return EVENT_PRICE


async def admin_event_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip().replace(",", ".")
    try:
        price = Decimal(text)
        if price < 0:
            raise InvalidOperation
    except Exception:
        await update.message.reply_text("Введите корректную стоимость. Например: 1000")
        return EVENT_PRICE
    context.user_data.setdefault("new_event", {})["price"] = price
    await update.message.reply_text("Описание мероприятия? Можно коротко. Если не нужно — отправьте минус.")
    return EVENT_DESCRIPTION


async def admin_event_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_value = (update.message.text or "").strip()
    context.user_data.setdefault("new_event", {})["description"] = "" if text_value == "-" else text_value
    await update.message.reply_text("Общий лимит участников? Например: 20")
    return EVENT_LIMIT


async def admin_event_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_value = (update.message.text or "").strip()
    if not text_value.isdigit() or int(text_value) <= 0:
        await update.message.reply_text("Введите лимит положительным числом.")
        return EVENT_LIMIT
    limit_value = int(text_value)
    context.user_data.setdefault("new_event", {})["total_limit"] = limit_value
    await update.message.reply_text("Минимальный возраст? Например: 18")
    return EVENT_MIN_AGE


async def admin_event_min_age(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_value = (update.message.text or "").strip()
    if not text_value.isdigit():
        await update.message.reply_text("Введите возраст числом.")
        return EVENT_MIN_AGE
    value = int(text_value)
    if value < 18 or value > 99:
        await update.message.reply_text("Минимальный возраст должен быть от 18 до 99.")
        return EVENT_MIN_AGE
    context.user_data.setdefault("new_event", {})["min_age"] = value
    await update.message.reply_text("Максимальный возраст? Например: 35")
    return EVENT_MAX_AGE


async def admin_event_max_age(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_value = (update.message.text or "").strip()
    if not text_value.isdigit():
        await update.message.reply_text("Введите возраст числом.")
        return EVENT_MAX_AGE
    value = int(text_value)
    data = context.user_data.setdefault("new_event", {})
    min_age = int(data.get("min_age", 18))
    if value < min_age or value > 99:
        await update.message.reply_text(f"Максимальный возраст должен быть не меньше {min_age} и не больше 99.")
        return EVENT_MAX_AGE
    data["max_age"] = value
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("50/50 включить", callback_data="balance:on"),
                InlineKeyboardButton("Без 50/50", callback_data="balance:off"),
            ]
        ]
    )
    await update.message.reply_text("Нужно ли включить баланс 50/50 по полу?", reply_markup=keyboard)
    return EVENT_BALANCE


async def admin_event_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    enabled = query.data.split(":", 1)[1] == "on"
    data = context.user_data.get("new_event", {})
    total_limit = data.get("total_limit", 0)
    if enabled and total_limit % 2 != 0:
        await query.message.reply_text(
            "Для режима 50/50 общий лимит должен быть четным. Создайте мероприятие заново с четным лимитом."
        )
        context.user_data.pop("new_event", None)
        return ConversationHandler.END

    data["gender_balance_enabled"] = enabled
    keyboard = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Да, спрашивать", callback_data="carq:on"),
            InlineKeyboardButton("Нет", callback_data="carq:off"),
        ]]
    )
    await query.message.reply_text(
        "Нужно ли для этого мероприятия задавать участнику вопрос про наличие автомобиля?",
        reply_markup=keyboard,
    )
    return EVENT_CAR_QUESTION


async def admin_event_car_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    enabled = query.data.split(":", 1)[1] == "on"
    data = context.user_data.get("new_event", {})
    data["ask_has_car"] = enabled
    keyboard = ReplyKeyboardMarkup([[SKIP_POSTER_TEXT]], resize_keyboard=True, one_time_keyboard=True)
    await query.message.reply_text(
        "Теперь отправьте афишу одним фото. Если пока без афиши — нажмите «Пропустить афишу».",
        reply_markup=keyboard,
    )
    return EVENT_POSTER


async def admin_event_poster(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("new_event", {})
    poster_file_id = None
    poster_file_unique_id = None

    if update.message.photo:
        poster = update.message.photo[-1]
        poster_file_id = poster.file_id
        poster_file_unique_id = poster.file_unique_id
    else:
        text_value = (update.message.text or "").strip()
        if text_value != SKIP_POSTER_TEXT:
            await update.message.reply_text(
                "Отправьте одно фото афиши или нажмите «Пропустить афишу».",
                reply_markup=ReplyKeyboardMarkup([[SKIP_POSTER_TEXT]], resize_keyboard=True, one_time_keyboard=True),
            )
            return EVENT_POSTER

    event_id = create_event(
        title=data["title"],
        event_date=data["event_date"],
        event_time=data["event_time"],
        location=data["location"],
        price=data["price"],
        description=data.get("description", ""),
        total_limit=data["total_limit"],
        min_age=data.get("min_age", 18),
        max_age=data.get("max_age", 99),
        gender_balance_enabled=data.get("gender_balance_enabled", False),
        ask_has_car=data.get("ask_has_car", False),
        poster_file_id=poster_file_id,
        poster_file_unique_id=poster_file_unique_id,
    )
    context.user_data.pop("new_event", None)
    event_row = get_event(event_id)
    await send_event_card(
        update.message,
        event_row,
        public=False,
        intro="Мероприятие создано:",
        reply_markup=build_active_event_keyboard(event_id),
    )
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
        await target.reply_text("У вас нет доступа.")
        return

    events = list_events()
    if not events:
        await target.reply_text("Мероприятий пока нет.")
        return

    for event_row in events:
        await send_event_card(
            target,
            event_row,
            public=False,
            extra_text=build_event_stats_text(event_row),
            reply_markup=build_active_event_keyboard(event_row["id"]),
        )


async def admin_edit_event_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("Мероприятие не найдено.")
        return
    await send_event_card(
        query.message,
        event_row,
        public=False,
        intro="Выберите, что хотите изменить:",
        reply_markup=build_edit_event_keyboard(event_row),
    )


async def admin_edit_field_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return ConversationHandler.END

    _, field, raw_event_id = query.data.split(":", 2)
    event_id = int(raw_event_id)
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("Мероприятие не найдено.")
        return ConversationHandler.END

    context.user_data["edit_event"] = {"id": event_id, "field": field}

    prompts = {
        "title": "Введите новое название.",
        "event_date": "Введите новую дату в формате YYYY-MM-DD.",
        "event_time": "Введите новое время в формате 19:00.",
        "location": "Введите новое место проведения.",
        "price": "Введите новую стоимость числом.",
        "description": "Введите новое описание. Если хотите очистить — отправьте минус.",
        "total_limit": "Введите новый общий лимит участников.",
        "min_age": "Введите новый минимальный возраст.",
        "max_age": "Введите новый максимальный возраст.",
        "ask_has_car": "Нужно ли задавать вопрос про автомобиль? Ответьте: Да или Нет.",
    }

    if field == "poster":
        keyboard = ReplyKeyboardMarkup(
            [[SKIP_POSTER_TEXT], [REMOVE_POSTER_TEXT, CANCEL_EDIT_TEXT]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        poster_prompt = "Отправьте новую афишу одним фото. Или выберите действие ниже." if event_row.get("poster_file_id") else "Отправьте афишу одним фото. Или выберите действие ниже."
        await query.message.reply_text(
            poster_prompt,
            reply_markup=keyboard,
        )
        return EDIT_EVENT_POSTER

    if field == "ask_has_car":
        keyboard = ReplyKeyboardMarkup(YES_NO_KEYBOARD + [[CANCEL_EDIT_TEXT]], resize_keyboard=True, one_time_keyboard=True)
        await query.message.reply_text(prompts[field], reply_markup=keyboard)
        return EDIT_EVENT_VALUE

    await query.message.reply_text(prompts[field], reply_markup=ReplyKeyboardRemove())
    return EDIT_EVENT_VALUE


async def admin_edit_event_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    edit_data = context.user_data.get("edit_event") or {}
    event_id = edit_data.get("id")
    field = edit_data.get("field")
    event_row = get_event(event_id)
    if not event_row:
        await update.message.reply_text("Мероприятие не найдено.", reply_markup=main_menu_keyboard())
        context.user_data.pop("edit_event", None)
        return ConversationHandler.END

    raw_value = (update.message.text or "").strip()
    updates = {}

    try:
        if field == "title":
            if len(raw_value) < 3:
                await update.message.reply_text("Введите нормальное название мероприятия.")
                return EDIT_EVENT_VALUE
            updates["title"] = raw_value
        elif field == "event_date":
            datetime.strptime(raw_value, "%Y-%m-%d")
            updates["event_date"] = raw_value
        elif field == "event_time":
            if not re.search(r"\d{1,2}:\d{2}", raw_value):
                await update.message.reply_text("Введите время в формате 19:00")
                return EDIT_EVENT_VALUE
            updates["event_time"] = raw_value
        elif field == "location":
            if len(raw_value) < 2:
                await update.message.reply_text("Введите место проведения.")
                return EDIT_EVENT_VALUE
            updates["location"] = raw_value
        elif field == "price":
            price = Decimal(raw_value.replace(",", "."))
            if price < 0:
                raise InvalidOperation
            updates["price"] = price
        elif field == "description":
            updates["description"] = "" if raw_value == "-" else raw_value
        elif field == "total_limit":
            if not raw_value.isdigit() or int(raw_value) <= 0:
                await update.message.reply_text("Введите лимит положительным числом.")
                return EDIT_EVENT_VALUE
            total_limit = int(raw_value)
            active_now = count_active_registrations(event_id)
            if total_limit < active_now:
                await update.message.reply_text(
                    f"Новый лимит не может быть меньше уже занятых мест: {active_now}."
                )
                return EDIT_EVENT_VALUE
            if event_row["gender_balance_enabled"] and total_limit % 2 != 0:
                await update.message.reply_text("Для режима 50/50 лимит должен быть четным.")
                return EDIT_EVENT_VALUE
            updates["total_limit"] = total_limit
            if event_row["gender_balance_enabled"]:
                updates["male_limit"] = total_limit // 2
                updates["female_limit"] = total_limit // 2
        elif field == "min_age":
            if not raw_value.isdigit():
                await update.message.reply_text("Введите возраст числом.")
                return EDIT_EVENT_VALUE
            min_age = int(raw_value)
            max_age = int(event_row.get("max_age") or 99)
            if min_age < 18 or min_age > max_age:
                await update.message.reply_text(f"Минимальный возраст должен быть от 18 до {max_age}.")
                return EDIT_EVENT_VALUE
            updates["min_age"] = min_age
        elif field == "max_age":
            if not raw_value.isdigit():
                await update.message.reply_text("Введите возраст числом.")
                return EDIT_EVENT_VALUE
            max_age = int(raw_value)
            min_age = int(event_row.get("min_age") or 18)
            if max_age < min_age or max_age > 99:
                await update.message.reply_text(f"Максимальный возраст должен быть от {min_age} до 99.")
                return EDIT_EVENT_VALUE
            updates["max_age"] = max_age
        elif field == "ask_has_car":
            if raw_value not in {"Да", "Нет"}:
                await update.message.reply_text("Ответьте: Да или Нет.")
                return EDIT_EVENT_VALUE
            updates["ask_has_car"] = raw_value == "Да"
        else:
            await update.message.reply_text("Это поле пока не поддерживается.")
            context.user_data.pop("edit_event", None)
            return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("Проверьте формат значения и попробуйте снова.")
        return EDIT_EVENT_VALUE
    except InvalidOperation:
        await update.message.reply_text("Введите корректное число.")
        return EDIT_EVENT_VALUE

    update_event_fields(event_id, **updates)
    context.user_data.pop("edit_event", None)
    refreshed = get_event(event_id)
    await send_event_card(
        update.message,
        refreshed,
        public=False,
        intro="Изменения сохранены:",
        reply_markup=build_active_event_keyboard(event_id),
    )
    return ConversationHandler.END


async def admin_edit_event_poster(update: Update, context: ContextTypes.DEFAULT_TYPE):
    edit_data = context.user_data.get("edit_event") or {}
    event_id = edit_data.get("id")
    event_row = get_event(event_id)
    if not event_row:
        await update.message.reply_text("Мероприятие не найдено.", reply_markup=main_menu_keyboard())
        context.user_data.pop("edit_event", None)
        return ConversationHandler.END

    if update.message.photo:
        poster = update.message.photo[-1]
        update_event_fields(
            event_id,
            poster_file_id=poster.file_id,
            poster_file_unique_id=poster.file_unique_id,
        )
    else:
        text_value = (update.message.text or "").strip()
        if text_value == REMOVE_POSTER_TEXT:
            update_event_fields(event_id, poster_file_id=None, poster_file_unique_id=None)
        elif text_value in {SKIP_POSTER_TEXT, CANCEL_EDIT_TEXT}:
            context.user_data.pop("edit_event", None)
            await update.message.reply_text("Редактирование афиши отменено.", reply_markup=main_menu_keyboard())
            return ConversationHandler.END
        else:
            await update.message.reply_text(
                "Отправьте одно фото, либо выберите «Удалить афишу» или «Отмена»."
            )
            return EDIT_EVENT_POSTER

    context.user_data.pop("edit_event", None)
    refreshed = get_event(event_id)
    await send_event_card(
        update.message,
        refreshed,
        public=False,
        intro="Афиша обновлена:",
        reply_markup=build_active_event_keyboard(event_id),
    )
    return ConversationHandler.END


async def admin_cancel_event_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("new_event", None)
    context.user_data.pop("edit_event", None)
    await update.effective_message.reply_text("Действие отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def admin_event_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    action, raw_id = query.data.split(":", 1)
    event_id = int(raw_id)
    if action == "activate":
        set_event_status(event_id, "active")
        await query.message.reply_text(f"Мероприятие #{event_id} сделано активным.")
    elif action == "close":
        set_event_status(event_id, "closed")
        await query.message.reply_text(f"Набор на мероприятие #{event_id} закрыт.")


async def admin_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("Мероприятие не найдено.")
        return
    await query.message.reply_text(build_event_stats_text(event_row), parse_mode=ParseMode.HTML)


def build_confirmed_export_bytes(event_row) -> bytes:
    approved_rows = get_approved_registrations_for_event(event_row["id"])
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow(["event_id", event_row["id"]])
    writer.writerow(["title", event_row["title"]])
    writer.writerow(["date", str(event_row["event_date"])])
    writer.writerow(["time", event_row["event_time"]])
    writer.writerow([])
    writer.writerow([
        "registration_id",
        "payment_code",
        "name",
        "age",
        "gender",
        "city",
        "phone",
        "has_car",
        "status",
        "payment_status",
        "created_at",
    ])
    for row in approved_rows:
        writer.writerow([
            row["id"],
            row.get("payment_code"),
            row["name_snapshot"],
            row["age_snapshot"],
            GENDER_LABELS.get(row["gender_snapshot"], row["gender_snapshot"]),
            row["city_snapshot"],
            row["phone_snapshot"],
            human_has_car(row.get("has_car_snapshot") if row.get("has_car_snapshot") is not None else row.get("user_has_car")),
            row["status"],
            row["payment_status"],
            row["created_at"],
        ])
    return output.getvalue().encode("utf-8-sig")


async def admin_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_id = int(query.data.split(":", 1)[1])
    event_row = get_event(event_id)
    if not event_row:
        await query.message.reply_text("Мероприятие не найдено.")
        return
    counts = count_registrations_by_status(event_id)
    approved = counts["approved"]
    share = round((approved / max(int(event_row["total_limit"]), 1)) * 100, 1)
    csv_bytes = build_confirmed_export_bytes(event_row)
    filename = f"confirmed_event_{event_id}.csv"
    file_obj = io.BytesIO(csv_bytes)
    file_obj.name = filename
    await context.bot.send_document(
        chat_id=query.message.chat_id,
        document=file_obj,
        caption=(
            f"Экспорт по мероприятию #{event_id}.\n"
            f"Подтверждено: {approved}/{event_row['total_limit']} ({share}%)."
        ),
    )


async def admin_notify_confirmed_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа.")
        return
    event_id = int(query.data.split(":", 1)[1])
    approved_rows = get_approved_registrations_for_event(event_id)
    if not approved_rows:
        await query.message.reply_text("Нет подтвержденных участников.")
        return
    sent = 0
    for row in approved_rows:
        try:
            await context.bot.send_message(
                chat_id=row["telegram_id"],
                text=(
                    f"Напоминание от организатора 📌\n\n"
                    f"Мероприятие: {row['title']}\n"
                    f"Дата: {row['event_date']} {row['event_time']}\n"
                    f"Место: {row['location']}"
                ),
            )
            sent += 1
        except Exception as exc:
            logger.warning("Could not notify approved participant %s: %s", row["telegram_id"], exc)
    await query.message.reply_text(f"Напоминание отправлено: {sent} участникам.")


async def promote_waiting_list_for_event(context: ContextTypes.DEFAULT_TYPE, event_id: int) -> None:
    event_row = get_event(event_id)
    if not event_row or event_row["status"] != "active":
        return

    waiters = get_waiting_list_for_event(event_id)
    for row in waiters:
        event_row = get_event(event_id)
        if not event_row:
            return
        age_ok, _ = check_age_allowed(event_row, row["age_snapshot"])
        if not age_ok:
            continue
        slot_ok, _ = check_slot_available(event_row, row["gender_snapshot"])
        if not slot_ok:
            continue

        expires_at = set_registration_waiting_payment(row["id"])
        expires_text = expires_at.astimezone(LOCAL_TZ).strftime("%H:%M")
        keyboard = build_payment_action_keyboard(row["id"])
        reg = get_registration(row["id"])
        try:
            await context.bot.send_message(
                chat_id=row["telegram_id"],
                text=(
                    "<b>Место освободилось ✅</b>\n\n"
                    f"Мероприятие: <b>{html.escape(row['title'])}</b>\n\n"
                    "Вы были в листе ожидания и автоматически переведены к оплате.\n\n"
                    f"⏳ Бронь места действует до <b>{expires_text}</b>.\n\n"
                    f"{build_payment_details_text(reg)}\n\n"
                    "После перевода нажмите кнопку «Я оплатил».\n"
                    "Если передумали — нажмите «Отменить заявку»."
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        except Exception as exc:
            logger.warning("Could not notify waiting list user %s: %s", row["telegram_id"], exc)


async def background_maintenance(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        expired_rows = get_expired_registrations()
        for row in expired_rows:
            update_registration_status(row["id"], "expired", "expired")
            try:
                await context.bot.send_message(
                    chat_id=row["telegram_id"],
                    text=(
                        f"⏰ Время на оплату мероприятия «{row['title']}» истекло. "
                        "Заявка снята, слот освобожден."
                    ),
                )
            except Exception as exc:
                logger.warning("Could not notify expired reservation %s: %s", row["telegram_id"], exc)
            await promote_waiting_list_for_event(context, row["event_id"])

        payment_reminders = get_due_payment_reminders()
        for row in payment_reminders:
            expires_at = row["reservation_expires_at"]
            expires_text = expires_at.astimezone(LOCAL_TZ).strftime("%H:%M") if expires_at else "—"
            try:
                await context.bot.send_message(
                    chat_id=row["telegram_id"],
                    text=(
                        f"Напоминание ⏳\n\n"
                        f"По мероприятию «{row['title']}» у вас еще не подтверждена оплата.\n"
                        f"Бронь действует до {expires_text}."
                    ),
                )
                mark_payment_reminder_sent(row["id"])
            except Exception as exc:
                logger.warning("Could not send payment reminder to %s: %s", row["telegram_id"], exc)

        due_event_rows = get_due_event_reminders()
        for row in due_event_rows:
            try:
                await context.bot.send_message(
                    chat_id=row["telegram_id"],
                    text=(
                        f"Напоминание о мероприятии 📅\n\n"
                        f"{row['title']}\n"
                        f"Дата: {row['event_date']} {row['event_time']}\n"
                        f"Место: {row['location']}"
                    ),
                )
                mark_before_event_reminder_sent(row["id"])
            except Exception as exc:
                logger.warning("Could not send event reminder to %s: %s", row["telegram_id"], exc)
    except Exception as exc:
        logger.exception("Background maintenance failed: %s", exc)


async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    if text in PARTICIPATE_BUTTONS | {PARTNER_BUTTON, "Мои данные", "Согласен", "Не согласен", "Да", "Нет", "Мужской", "Женский", "Мариуполь", "Другой город", SKIP_POSTER_TEXT, REMOVE_POSTER_TEXT, CANCEL_EDIT_TEXT}:
        return
    await update.message.reply_text("Используйте кнопки ниже: «Участвовать», «Партнерство» или «Мои данные».", reply_markup=main_menu_keyboard())


def build_application() -> Application:
    application = Application.builder().token(BOT_TOKEN).updater(None).build()

    profile_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(r"^(Участвовать|Хочу участвовать)$"), participate_entry),
            CallbackQueryHandler(edit_profile_entry, pattern=r"^edit_profile:"),
        ],
        states={
            PROFILE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_name)],
            PROFILE_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_age)],
            PROFILE_GENDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_gender)],
            PROFILE_CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_city)],
            PROFILE_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_phone)],
            PROFILE_CONSENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_consent)],
            PROFILE_HAS_CAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_has_car)],
        },
        fallbacks=[CommandHandler("cancel", cancel_profile)],
        per_chat=True,
        per_user=True,
    )

    partner_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^Партнерство$"), partner_entry)],
        states={
            PARTNER_CONTACT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_contact_name)],
            PARTNER_PROJECT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_project_name)],
            PARTNER_DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_description)],
            PARTNER_CONTACT: [MessageHandler(filters.TEXT & ~filters.COMMAND, partner_contact)],
        },
        fallbacks=[CommandHandler("cancel", cancel_partner)],
        per_chat=True,
        per_user=True,
    )

    event_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(admin_add_event_entry, pattern=r"^admin_add_event$")
        ],
        states={
            EVENT_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_title)],
            EVENT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_date)],
            EVENT_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_time)],
            EVENT_LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_location)],
            EVENT_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_price)],
            EVENT_DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_description)],
            EVENT_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_limit)],
            EVENT_MIN_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_min_age)],
            EVENT_MAX_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_event_max_age)],
            EVENT_BALANCE: [CallbackQueryHandler(admin_event_balance, pattern=r"^balance:")],
            EVENT_CAR_QUESTION: [CallbackQueryHandler(admin_event_car_question, pattern=r"^carq:")],
            EVENT_POSTER: [MessageHandler((filters.PHOTO | filters.TEXT) & ~filters.COMMAND, admin_event_poster)],
        },
        fallbacks=[CommandHandler("cancel", admin_cancel_event_creation)],
        per_chat=True,
        per_user=True,
    )

    edit_event_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_edit_field_entry, pattern=r"^edit_event_field:")],
        states={
            EDIT_EVENT_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_event_value)],
            EDIT_EVENT_POSTER: [MessageHandler((filters.PHOTO | filters.TEXT) & ~filters.COMMAND, admin_edit_event_poster)],
        },
        fallbacks=[CommandHandler("cancel", admin_cancel_event_creation)],
        per_chat=True,
        per_user=True,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_menu))
    application.add_handler(MessageHandler(filters.Regex(r"^Мои данные$"), show_profile))
    application.add_handler(profile_conv)
    application.add_handler(partner_conv)
    application.add_handler(event_conv)
    application.add_handler(edit_event_conv)
    application.add_handler(CallbackQueryHandler(pay_callback, pattern=r"^pay:"))
    application.add_handler(CallbackQueryHandler(join_waiting_list_callback, pattern=r"^join_waitlist:"))
    application.add_handler(CallbackQueryHandler(paid_callback, pattern=r"^paid:"))
    application.add_handler(CallbackQueryHandler(cancel_registration_callback, pattern=r"^cancel_reg:"))
    application.add_handler(CallbackQueryHandler(moderation_callback, pattern=r"^(approve|reject):"))
    application.add_handler(CallbackQueryHandler(admin_list_events, pattern=r"^admin_list_events$"))
    application.add_handler(CallbackQueryHandler(admin_show_active_event, pattern=r"^admin_active_event$"))
    application.add_handler(CallbackQueryHandler(admin_edit_current_event, pattern=r"^admin_edit_current_event$"))
    application.add_handler(CallbackQueryHandler(admin_edit_event_menu, pattern=r"^edit_event_menu:"))
    application.add_handler(CallbackQueryHandler(admin_event_status_callback, pattern=r"^(activate|close):"))
    application.add_handler(CallbackQueryHandler(admin_delete_event_prompt, pattern=r"^delete_event_prompt:"))
    application.add_handler(CallbackQueryHandler(admin_delete_event_confirm, pattern=r"^delete_event_confirm:"))
    application.add_handler(CallbackQueryHandler(admin_delete_event_cancel, pattern=r"^delete_event_cancel:"))
    application.add_handler(CallbackQueryHandler(admin_stats_callback, pattern=r"^stats:"))
    application.add_handler(CallbackQueryHandler(admin_export_callback, pattern=r"^export:"))
    application.add_handler(CallbackQueryHandler(admin_notify_confirmed_callback, pattern=r"^notify_confirmed:"))
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

    if telegram_app.job_queue is not None:
        telegram_app.job_queue.run_repeating(
            background_maintenance,
            interval=BACKGROUND_CHECK_INTERVAL_SECONDS,
            first=15,
            name="background_maintenance",
        )
    else:
        logger.warning("JobQueue is unavailable. Auto-reminders and timers will not work until JobQueue is installed.")

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
