import html
import logging
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import psycopg
from fastapi import FastAPI, HTTPException, Request
from psycopg.rows import dict_row
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
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
TIMEZONE_LABEL = os.getenv("TIMEZONE_LABEL", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not BASE_URL:
    raise RuntimeError("BASE_URL is required")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

PROFILE_NAME, PROFILE_AGE, PROFILE_GENDER, PROFILE_CITY, PROFILE_PHONE = range(5)
EVENT_TITLE, EVENT_DATE, EVENT_TIME, EVENT_LOCATION, EVENT_PRICE, EVENT_DESCRIPTION, EVENT_LIMIT, EVENT_BALANCE = range(100, 108)

ACTIVE_REGISTRATION_STATUSES = ("waiting_payment", "waiting_moderation", "approved")
GENDER_MAP = {
    "Мужской": "male",
    "Женский": "female",
}
GENDER_LABELS = {
    "male": "Мужской",
    "female": "Женский",
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
                status TEXT NOT NULL DEFAULT 'waiting_payment',
                payment_status TEXT NOT NULL DEFAULT 'not_paid',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                moderated_at TIMESTAMPTZ
            );
            """
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


def upsert_user_profile(telegram_id: int, username: Optional[str], full_name: str, age: int, gender: str, city: str, phone: str) -> None:
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


def create_event(title: str, event_date: str, event_time: str, location: str, price: Decimal, description: str, total_limit: int, gender_balance_enabled: bool) -> int:
    male_limit = female_limit = None
    if gender_balance_enabled:
        male_limit = total_limit // 2
        female_limit = total_limit // 2
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (title, event_date, event_time, location, price, description, total_limit,
                                gender_balance_enabled, male_limit, female_limit, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'upcoming')
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
            ORDER BY event_date ASC, created_at ASC
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
            cur.execute("UPDATE events SET status = 'upcoming' WHERE status = 'active'")
        cur.execute("UPDATE events SET status = %s WHERE id = %s", (new_status, event_id))
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


def check_slot_available(event_row, gender: str) -> tuple[bool, str]:
    total_used = count_active_registrations(event_row["id"])
    if total_used >= event_row["total_limit"]:
        return False, "Свободных мест на это мероприятие больше нет."

    if event_row["gender_balance_enabled"]:
        used_for_gender = count_active_registrations_by_gender(event_row["id"], gender)
        limit_for_gender = event_row["male_limit"] if gender == "male" else event_row["female_limit"]
        if used_for_gender >= limit_for_gender:
            label = GENDER_LABELS.get(gender, "этого пола")
            return False, f"Места для категории «{label}» уже закончились."

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
        return row["id"]


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
                "UPDATE registrations SET status = %s, moderated_at = CASE WHEN %s IN ('approved', 'rejected') THEN NOW() ELSE moderated_at END WHERE id = %s",
                (status, status, registration_id),
            )
        else:
            cur.execute(
                "UPDATE registrations SET status = %s, payment_status = %s, moderated_at = CASE WHEN %s IN ('approved', 'rejected') THEN NOW() ELSE moderated_at END WHERE id = %s",
                (status, payment_status, status, registration_id),
            )
        conn.commit()


def format_price(value) -> str:
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        if value == value.to_integral():
            return str(int(value))
        return str(value)
    return str(value)


def render_event_text(event_row) -> str:
    balance = "Включен" if event_row["gender_balance_enabled"] else "Выключен"
    extra = ""
    if event_row["gender_balance_enabled"]:
        extra = f"\nБаланс М/Ж: {event_row['male_limit']}/{event_row['female_limit']}"
    description = html.escape(event_row["description"] or "")
    return (
        f"<b>{html.escape(event_row['title'])}</b>\n"
        f"Дата: {event_row['event_date']}\n"
        f"Время: {html.escape(event_row['event_time'])}\n"
        f"Место: {html.escape(event_row['location'])}\n"
        f"Цена: {format_price(event_row['price'])}\n"
        f"Статус: {html.escape(event_row['status'])}\n"
        f"Лимит: {event_row['total_limit']}\n"
        f"50/50: {balance}{extra}"
        + (f"\nОписание: {description}" if description else "")
    )


def render_profile_text(user_row) -> str:
    return (
        "<b>Ваша анкета</b>\n"
        f"Имя: {html.escape(user_row['full_name'])}\n"
        f"Возраст: {user_row['age']}\n"
        f"Пол: {GENDER_LABELS.get(user_row['gender'], user_row['gender'])}\n"
        f"Город: {html.escape(user_row['city'])}\n"
        f"Телефон: {html.escape(user_row['phone'])}"
    )


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Участвовать"], ["Мои данные"]],
        resize_keyboard=True,
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Привет. Здесь можно быстро записаться на актуальное мероприятие.\n\n"
        "Нажмите «Участвовать», чтобы подать заявку, или «Мои данные», чтобы посмотреть анкету."
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

    latest_reg = get_latest_registration_for_user_event(user.id, event_row["id"])
    if latest_reg and latest_reg["status"] in ACTIVE_REGISTRATION_STATUSES:
        status_map = {
            "waiting_payment": "ожидает оплаты",
            "waiting_moderation": "на модерации",
            "approved": "подтверждена",
        }
        await update.effective_message.reply_text(
            f"У вас уже есть заявка на это мероприятие. Текущий статус: {status_map.get(latest_reg['status'], latest_reg['status'])}.",
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
    await update.effective_message.reply_text(
        f"Сейчас открыта запись на:\n\n{render_event_text(event_row)}\n\nКак вас зовут?",
        parse_mode=ParseMode.HTML,
        reply_markup=ReplyKeyboardRemove(),
    )
    return PROFILE_NAME


async def send_event_and_profile_confirmation(message, profile, event_row) -> None:
    text = (
        f"Сейчас открыта запись на:\n\n{render_event_text(event_row)}\n\n"
        f"{render_profile_text(profile)}\n\n"
        "Если все верно, переходите к оплате."
    )
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Перейти к оплате", callback_data=f"pay:{event_row['id']}")],
            [InlineKeyboardButton("Изменить данные", callback_data="edit_profile:participate")],
        ]
    )
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def edit_profile_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    source = query.data.split(":", 1)[1] if ":" in query.data else "profile"
    context.user_data["profile_source"] = source
    context.user_data["profile_form"] = {}
    await query.message.reply_text("Как вас зовут?", reply_markup=ReplyKeyboardRemove())
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
    phone = (update.message.text or "").strip().replace(" ", "")
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 10 or len(digits) > 15:
        await update.message.reply_text("Введите корректный номер телефона.")
        return PROFILE_PHONE
    if phone.startswith("8") and len(digits) == 11:
        phone = "+7" + digits[1:]
    elif not phone.startswith("+"):
        phone = "+" + digits

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
                "Анкета сохранена. Сейчас активное мероприятие не найдено.",
                reply_markup=main_menu_keyboard(),
            )
            return ConversationHandler.END
        ok, reason = check_slot_available(event_row, profile["gender"])
        if not ok:
            await update.message.reply_text(
                f"Анкета сохранена. {reason}",
                reply_markup=main_menu_keyboard(),
            )
            return ConversationHandler.END
        await send_event_and_profile_confirmation(update.message, profile, event_row)
    else:
        await update.message.reply_text(
            "Анкета сохранена.\n\n" + html.unescape(render_profile_text(profile)),
            reply_markup=main_menu_keyboard(),
        )
    return ConversationHandler.END


async def cancel_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("profile_form", None)
    context.user_data.pop("profile_source", None)
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

    latest_reg = get_latest_registration_for_user_event(query.from_user.id, event_id)
    if latest_reg and latest_reg["status"] in ACTIVE_REGISTRATION_STATUSES:
        await query.message.reply_text("У вас уже есть активная заявка на это мероприятие.")
        return

    ok, reason = check_slot_available(event_row, user_row["gender"])
    if not ok:
        await query.message.reply_text(reason)
        return

    registration_id = create_registration_from_profile(event_row, user_row)
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Я оплатил", callback_data=f"paid:{registration_id}")],
            [InlineKeyboardButton("Отменить заявку", callback_data=f"cancel_reg:{registration_id}")],
        ]
    )
    text = (
        f"Заявка создана на мероприятие <b>{html.escape(event_row['title'])}</b>.\n\n"
        f"{html.escape(PAYMENT_TEXT)}\n\n"
        "После оплаты нажмите кнопку «Я оплатил»."
    )
    await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


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
            f"Ивент: {html.escape(reg['title'])}\n"
            f"Дата: {reg['event_date']} {html.escape(reg['event_time'])}\n"
            f"Имя: {html.escape(reg['name_snapshot'])}\n"
            f"Возраст: {reg['age_snapshot']}\n"
            f"Пол: {GENDER_LABELS.get(reg['gender_snapshot'], reg['gender_snapshot'])}\n"
            f"Город: {html.escape(reg['city_snapshot'])}\n"
            f"Телефон: {html.escape(reg['phone_snapshot'])}"
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
    if reg["status"] not in ("waiting_payment", "waiting_moderation"):
        await query.message.reply_text("Эту заявку уже нельзя отменить.")
        return

    update_registration_status(registration_id, "cancelled", "cancelled")
    await query.message.reply_text("Заявка отменена. Слот освобожден.")


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


async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("У вас нет доступа к админ-панели.")
        return
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Добавить мероприятие", callback_data="admin_add_event")],
            [InlineKeyboardButton("Список мероприятий", callback_data="admin_list_events")],
        ]
    )
    await update.effective_message.reply_text("Админ-панель", reply_markup=keyboard)


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
    context.user_data.setdefault("new_event", {})["title"] = (update.message.text or "").strip()
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
    context.user_data.setdefault("new_event", {})["event_time"] = (update.message.text or "").strip()
    await update.message.reply_text("Место проведения?")
    return EVENT_LOCATION


async def admin_event_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("new_event", {})["location"] = (update.message.text or "").strip()
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
    await update.message.reply_text("Описание мероприятия? Можно коротко.")
    return EVENT_DESCRIPTION


async def admin_event_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("new_event", {})["description"] = (update.message.text or "").strip()
    await update.message.reply_text("Общий лимит участников? Например: 20")
    return EVENT_LIMIT


async def admin_event_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text.isdigit() or int(text) <= 0:
        await update.message.reply_text("Введите лимит положительным числом.")
        return EVENT_LIMIT
    limit_value = int(text)
    context.user_data.setdefault("new_event", {})["total_limit"] = limit_value
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

    event_id = create_event(
        title=data["title"],
        event_date=data["event_date"],
        event_time=data["event_time"],
        location=data["location"],
        price=data["price"],
        description=data.get("description", ""),
        total_limit=total_limit,
        gender_balance_enabled=enabled,
    )
    context.user_data.pop("new_event", None)

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Сделать активным", callback_data=f"activate:{event_id}")]]
    )
    event_row = get_event(event_id)
    await query.message.reply_text(
        "Мероприятие создано.\n\n" + render_event_text(event_row),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    return ConversationHandler.END


async def admin_cancel_event_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("new_event", None)
    await update.effective_message.reply_text("Создание мероприятия отменено.")
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
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Сделать активным", callback_data=f"activate:{event_row['id']}")],
                [InlineKeyboardButton("Закрыть набор", callback_data=f"close:{event_row['id']}")],
            ]
        )
        await target.reply_text(render_event_text(event_row), parse_mode=ParseMode.HTML, reply_markup=keyboard)


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


async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    if text == "Участвовать":
        return
    if text == "Мои данные":
        return
    await update.message.reply_text("Используйте кнопки меню ниже.", reply_markup=main_menu_keyboard())


def build_application() -> Application:
    application = Application.builder().token(BOT_TOKEN).updater(None).build()

    profile_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(r"^Участвовать$"), participate_entry),
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
            EVENT_BALANCE: [CallbackQueryHandler(admin_event_balance, pattern=r"^balance:")],
        },
        fallbacks=[CommandHandler("cancel", admin_cancel_event_creation)],
        per_chat=True,
        per_user=True,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_menu))
    application.add_handler(MessageHandler(filters.Regex(r"^Мои данные$"), show_profile))
    application.add_handler(profile_conv)
    application.add_handler(event_conv)
    application.add_handler(CallbackQueryHandler(pay_callback, pattern=r"^pay:"))
    application.add_handler(CallbackQueryHandler(paid_callback, pattern=r"^paid:"))
    application.add_handler(CallbackQueryHandler(cancel_registration_callback, pattern=r"^cancel_reg:"))
    application.add_handler(CallbackQueryHandler(moderation_callback, pattern=r"^(approve|reject):"))
    application.add_handler(CallbackQueryHandler(admin_list_events, pattern=r"^admin_list_events$"))
    application.add_handler(CallbackQueryHandler(admin_event_status_callback, pattern=r"^(activate|close):"))
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
