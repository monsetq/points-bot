import asyncio
import logging
import os
import asyncpg
import time
import secrets
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.markdown import hbold, hlink

TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "1875573844"))

MIN_POINTS_TO_TRANSFER = 50
TRANSFER_RATE = 3

TRANSFER_CONFIRM_TTL = 300
pending_transfers = {}

ITEMS_PER_PAGE = 30
logging.basicConfig(level=logging.INFO)

BALANCE_MIN = 0
BALANCE_MAX = 100

bot = Bot(token=TOKEN, parse_mode="HTML")
dp = Dispatcher()

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT,
            chat_id BIGINT,
            points INT DEFAULT 0,
            name TEXT,
            username TEXT,
            PRIMARY KEY (user_id, chat_id)
        )
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id BIGINT PRIMARY KEY,
            join_points INT NOT NULL DEFAULT 50
        )
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            chat_id BIGINT,
            user_id BIGINT,
            level INT NOT NULL DEFAULT 1
        )
        """)

        await conn.execute("ALTER TABLE admins ADD COLUMN IF NOT EXISTS chat_id BIGINT")
        await conn.execute("ALTER TABLE admins ADD COLUMN IF NOT EXISTS user_id BIGINT")
        await conn.execute("ALTER TABLE admins ADD COLUMN IF NOT EXISTS level INT NOT NULL DEFAULT 1")

        await conn.execute("""
        UPDATE users u
        SET points = cs.join_points
        FROM chat_settings cs
        WHERE u.chat_id = cs.chat_id AND u.points = 0
        """)

        await conn.execute("""
        UPDATE users
        SET points = 50
        WHERE points = 0
        """)


async def get_join_points(chat_id: int) -> int:
    async with pool.acquire() as conn:
        jp = await conn.fetchval("SELECT join_points FROM chat_settings WHERE chat_id = $1", chat_id)
        if jp is None:
            await conn.execute(
                "INSERT INTO chat_settings (chat_id, join_points) VALUES ($1, 50) ON CONFLICT (chat_id) DO NOTHING",
                chat_id
            )
            return 50
        return int(jp)


async def update_user_data(user_id: int, chat_id: int, name: str, username: str | None = None):
    if username:
        username = username.replace("@", "").lower()

    join_points = await get_join_points(chat_id)

    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, chat_id, points, name, username)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user_id, chat_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            username = COALESCE(EXCLUDED.username, users.username)
        """, user_id, chat_id, join_points, name, username)


async def user_exists_in_chat(user_id: int, chat_id: int) -> bool:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT 1 FROM users WHERE user_id = $1 AND chat_id = $2",
            user_id, chat_id
        ) is not None


async def get_admin_level(user_id: int, chat_id: int) -> int:
    if user_id == OWNER_ID:
        return 999
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT level FROM admins WHERE user_id = $1 AND chat_id = $2 ORDER BY level DESC LIMIT 1",
            user_id, chat_id
        )
    return int(row["level"]) if row else 0


async def has_level(user_id: int, chat_id: int, min_level: int) -> bool:
    return (await get_admin_level(user_id, chat_id)) >= min_level


async def set_admin_level(chat_id: int, user_id: int, level: int, mode: str = "force"):
    async with pool.acquire() as conn:
        if mode == "max":
            res = await conn.execute(
                "UPDATE admins SET level = GREATEST(level, $3) WHERE chat_id = $1 AND user_id = $2",
                chat_id, user_id, level
            )
        else:
            res = await conn.execute(
                "UPDATE admins SET level = $3 WHERE chat_id = $1 AND user_id = $2",
                chat_id, user_id, level
            )

        if res.endswith("UPDATE 0"):
            await conn.execute(
                "INSERT INTO admins (chat_id, user_id, level) VALUES ($1, $2, $3)",
                chat_id, user_id, level
            )


async def remove_admin_level(chat_id: int, user_id: int):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE chat_id = $1 AND user_id = $2", chat_id, user_id)


async def resolve_target(message: types.Message, args: list):
    """
    Возвращает: (tid, name, username, err)
    err:
      None - ок
      "no_target" - не указали
      "not_found" - нигде не нашли username в БД
      "not_in_chat" - нашли user_id глобально, но его нет в этом чате (или бот не смог проверить)
    """
    if message.reply_to_message and message.reply_to_message.from_user:
        u = message.reply_to_message.from_user
        return u.id, u.first_name, u.username, None

    uname = None
    for a in args:
        if a.startswith("@"):
            uname = a.replace("@", "").lower()
            break

    if not uname:
        return None, None, None, "no_target"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id, name, username FROM users WHERE chat_id = $1 AND username = $2",
            message.chat.id, uname
        )
    if row:
        return row["user_id"], row["name"], row["username"], None

    async with pool.acquire() as conn:
        row2 = await conn.fetchrow(
            "SELECT user_id, name, username FROM users WHERE username = $1 ORDER BY chat_id DESC LIMIT 1",
            uname
        )
    if not row2:
        return None, None, None, "not_found"

    tid = int(row2["user_id"])
    tname = row2["name"] or uname
    tuname = row2["username"]

    try:
        member = await bot.get_chat_member(message.chat.id, tid)
        if member.status in ("left", "kicked"):
            return None, None, None, "not_in_chat"
    except Exception:
        return None, None, None, "not_in_chat"

    await update_user_data(tid, message.chat.id, tname, tuname)
    return tid, tname, tuname, None


def silent_link(name, user_id):
    return f'<a href="tg://user?id={user_id}">{name}</a>'


async def log_to_owner(text: str):
    try:
        await bot.send_message(OWNER_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logging.warning(f"Failed to send log to owner: {e}")


def extract_reason_from_args(args: list) -> str:
    if len(args) <= 2:
        return ""

    at_index = None
    for i, a in enumerate(args):
        if a.startswith("@"):
            at_index = i
            break

    if at_index is not None:
        reason_parts = args[at_index + 1:]
    else:
        reason_parts = args[2:]

    return " ".join(reason_parts).strip()


def extract_mass_reason(args: list) -> str:
    last_at = -1
    for i, a in enumerate(args):
        if a.startswith("@"):
            last_at = i
    if last_at == -1:
        return ""
    return " ".join(args[last_at + 1:]).strip()


def get_top_keyboard(current_page: int, total_pages: int, user_id: int):
    builder = InlineKeyboardBuilder()
    if current_page > 0:
        builder.button(text="⬅️", callback_data=f"top:{user_id}:{current_page - 1}")
    if current_page < total_pages - 1:
        builder.button(text="➡️", callback_data=f"top:{user_id}:{current_page + 1}")
    builder.adjust(2)
    return builder.as_markup()


def transfer_confirm_kb(token: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data=f"tconf:{token}")
    builder.button(text="❌ Отмена", callback_data=f"tcancel:{token}")
    builder.adjust(2)
    return builder.as_markup()


async def send_top_page(message: types.Message, page: int, owner_id: int, edit: bool = False):
    offset = page * ITEMS_PER_PAGE
    async with pool.acquire() as conn:
        total_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE chat_id = $1", message.chat.id)
        total_pages = max(1, (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

        top = await conn.fetch(
            "SELECT user_id, name, points, username FROM users "
            "WHERE chat_id = $1 ORDER BY points DESC LIMIT $2 OFFSET $3",
            message.chat.id, ITEMS_PER_PAGE, offset
        )

    if not top:
        return await message.answer("💠 Список лидеров пока пуст.")

    res = [f"🏆 <b>ТОП ЛИДЕРОВ</b> <i>({page + 1}/{total_pages})</i>\n"]
    for i, row in enumerate(top, 1 + offset):
        uid, name, pts, username = row["user_id"], row["name"], row["points"], row["username"]
        if username:
            user_link = hlink(name, f"https://t.me/{username}")
        else:
            user_link = name
        res.append(f"{i}. {user_link} — {hbold(pts)}")

    text = "\n".join(res)
    kb = get_top_keyboard(page, total_pages, owner_id)

    if edit:
        await message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    else:
        await message.answer(text, reply_markup=kb, disable_web_page_preview=True)


def help_block(title: str, lines: list[str]) -> str:
    body = "\n".join([f"• {x}" for x in lines])
    return f"<b>{title}</b>\n{body}\n"


def build_help(role: str, lvl: int, join_points: int) -> str:
    header = (
        "<b>💠 Меню бота баллов</b>\n"
        f"🧩 Стартовые баллы: <b>{join_points}</b>\n"
        f"🔒 Лимит баланса: <b>{BALANCE_MIN}</b>–<b>{BALANCE_MAX}</b>\n"
        f"🔁 Курс перевода: <b>{TRANSFER_RATE}:1</b>\n\n"
    )

    common = help_block("👤 Участнику", [
        "<code>/моиб</code> — баланс",
        "<code>/топб</code> — лидеры",
        "<code>/передать</code> — перевод баллов",
    ])

    if role == "member":
        return header + common

    admin1 = help_block("🛡 Админу 1 уровня", [
        "<code>/инфо</code> — баланс участника",
    ])

    if role == "admin1":
        return header + common + admin1

    admin2 = help_block("🛡 Админу 2 уровня", [
        "<code>/балл</code> — начислить/снять баллы",
        "<code>/баллм</code> — массово начислить/снять",
        "<code>/стартбаллы</code> — стартовые баллы чата",
        "<code>/админ</code> — выдать админа 1 уровня",
        "<code>/повысить</code> — выдать админа 2 уровня",
        "<code>/разжаловать</code> — снять админку",
        "<code>/бадмины</code> — список админов",
    ])

    if role == "owner":
        owner = help_block("👑 Владельцу", [
            "Полный доступ в любом чате",
        ])
        return header + owner + common + admin1 + admin2

    return header + common + admin1 + admin2


@dp.message(Command("start", "help", "bhelp", "бпомощь"))
async def cmd_help(message: types.Message):
    await update_user_data(
        message.from_user.id,
        message.chat.id,
        message.from_user.first_name,
        message.from_user.username
    )

    lvl = await get_admin_level(message.from_user.id, message.chat.id)
    jp = await get_join_points(message.chat.id)

    if message.from_user.id == OWNER_ID:
        text = build_help("owner", lvl, jp)
    elif lvl >= 2:
        text = build_help("admin2", lvl, jp)
    elif lvl >= 1:
        text = build_help("admin1", lvl, jp)
    else:
        text = build_help("member", lvl, jp)

    await message.answer(text, disable_web_page_preview=True)



@dp.message(Command("стартбаллы", "joinpoints"))
async def set_join_points_cmd(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 2):
        return

    args = message.text.split()
    if len(args) < 2:
        jp = await get_join_points(message.chat.id)
        return await message.reply(
            f"Текущие стартовые баллы: <b>{jp}</b>\nУстановить: <code>/стартбаллы 50</code>"
        )

    try:
        jp = int(args[1])
    except ValueError:
        return await message.reply("Введите число. Пример: <code>/стартбаллы 50</code>")

    jp = max(BALANCE_MIN, min(BALANCE_MAX, jp))

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_settings (chat_id, join_points)
            VALUES ($1, $2)
            ON CONFLICT (chat_id)
            DO UPDATE SET join_points = $2
        """, message.chat.id, jp)

    await message.reply(f"✅ Стартовые баллы установлены на <b>{jp}</b>.")


@dp.message(Command("моиб", "myb"))
async def my_points(message: types.Message):
    await update_user_data(message.from_user.id, message.chat.id, message.from_user.first_name, message.from_user.username)
    async with pool.acquire() as conn:
        points = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            message.from_user.id, message.chat.id
        )
    if points is None:
        points = await get_join_points(message.chat.id)
    await message.reply(f"💠 {message.from_user.first_name}, у тебя <b>{points}</b> баллов.")


@dp.message(Command("инфо", "stats"))
async def check_stats(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 1):
        return

    tid, tname, tuname, err = await resolve_target(message, message.text.split())
    if err == "no_target":
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: <code>/инфо @user</code>")
    if err == "not_found":
        return await message.reply("❌ Пользователь не найден. Пусть он напишет сообщение в любой чат с ботом.")
    if err == "not_in_chat":
        return await message.reply("❌ Этот @username не найден среди участников этого чата.")

    if not await user_exists_in_chat(tid, message.chat.id):
        return await message.reply("❌ Пользователь не найден в базе этого чата.\nПусть он напишет сообщение.")

    async with pool.acquire() as conn:
        points = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            tid, message.chat.id
        )
    if points is None:
        points = await get_join_points(message.chat.id)

    user_link = silent_link(tname, tid)
    await message.answer(
        f"<b>📊 Информация</b>\n"
        f"👤 Пользователь: {user_link}\n"
        f"💠 Баланс: <b>{points}</b> баллов",
        disable_web_page_preview=True
    )


@dp.message(Command("топб", "topb"))
async def show_top_command(message: types.Message):
    await send_top_page(message, 0, owner_id=message.from_user.id)


@dp.callback_query(F.data.startswith("top:"))
async def process_top_pagination(callback: types.CallbackQuery):
    data = callback.data.split(":")
    owner_id = int(data[1])
    page = int(data[2])

    if callback.from_user.id != owner_id:
        return await callback.answer()

    await send_top_page(callback.message, page, owner_id=owner_id, edit=True)
    await callback.answer()


@dp.message(Command("передать", "pay"))
async def transfer_points(message: types.Message):
    await update_user_data(
        message.from_user.id,
        message.chat.id,
        message.from_user.first_name,
        message.from_user.username
    )

    args = message.text.split()
    if len(args) < 2:
        return await message.reply("Используй: <code>/передать 30 @username</code> или ответом: <code>/передать 30</code>")

    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("Ошибка! Пример: <code>/передать 30 @username</code>")

    if amount <= 0:
        return await message.reply("Введите положительное число.")

    tid, tname, tuname, err = await resolve_target(message, args)
    if err == "no_target":
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.")
    if err == "not_found":
        return await message.reply("❌ Пользователь не найден. Пусть он напишет сообщение в любой чат с ботом.")
    if err == "not_in_chat":
        return await message.reply("❌ Этот @username не найден среди участников этого чата.")

    if not await user_exists_in_chat(tid, message.chat.id):
        return await message.reply("❌ Пользователь не найден в базе этого чата.\nПусть он напишет сообщение.")

    if tid == message.from_user.id:
        return await message.reply("Нельзя переводить баллы самому себе.")

    received_raw = amount // TRANSFER_RATE
    if received_raw <= 0:
        return await message.reply(f"Минимальный перевод: <b>{TRANSFER_RATE}</b> (тогда получатель получит <b>1</b> балл).")

    async with pool.acquire() as conn:
        sender_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            message.from_user.id, message.chat.id
        )
        if sender_pts is None:
            sender_pts = await get_join_points(message.chat.id)

        target_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            tid, message.chat.id
        )
        if target_pts is None:
            target_pts = await get_join_points(message.chat.id)

    if target_pts + received_raw > BALANCE_MAX:
        can = max(0, BALANCE_MAX - target_pts)
        return await message.reply(
            f"❌ Перевод невозможен: у получателя будет больше <b>{BALANCE_MAX}</b> баллов.\n"
            f"Сейчас у получателя: <b>{target_pts}</b>.\n"
            f"Он может принять максимум: <b>{can}</b>.\n"
            f"Ты хотел перевести (получит): <b>{received_raw}</b>."
        )

    actual_received = received_raw
    actual_spent = actual_received * TRANSFER_RATE

    if sender_pts - actual_spent < MIN_POINTS_TO_TRANSFER:
        return await message.reply(
            f"❌ Нельзя перевести: после перевода у тебя должно остаться "
            f"<b>не меньше {MIN_POINTS_TO_TRANSFER}</b> баллов.\n"
            f"Сейчас: <b>{sender_pts}</b>, спишется: <b>{actual_spent}</b>, останется: <b>{sender_pts - actual_spent}</b>."
        )

    if sender_pts < actual_spent:
        return await message.reply("❌ Недостаточно баллов для перевода.")

    sender_l = silent_link(message.from_user.first_name, message.from_user.id)
    target_l = silent_link(tname, tid)

    token = secrets.token_urlsafe(8).replace("-", "").replace("_", "")
    pending_transfers[token] = {
        "created": time.time(),
        "chat_id": message.chat.id,
        "sender_id": message.from_user.id,
        "sender_name": message.from_user.first_name,
        "target_id": tid,
        "target_name": tname,
        "spent": actual_spent,
        "received": actual_received
    }

    text = (
        f"💠 <b>Подтверждение перевода</b>\n\n"
        f"👤 Отправитель: {sender_l}\n"
        f"🎯 Получатель: {target_l}\n\n"
        f"📉 Спишется у отправителя: <b>{actual_spent}</b>\n"
        f"📈 Получит получатель: <b>{actual_received}</b>\n"
        f"🔁 Курс: <b>{TRANSFER_RATE}:1</b>\n\n"
        f"Подтвердить перевод?"
    )

    await message.answer(text, reply_markup=transfer_confirm_kb(token), disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("tconf:"))
async def transfer_confirm(callback: types.CallbackQuery):
    token = callback.data.split(":", 1)[1]
    req = pending_transfers.get(token)

    if not req:
        return await callback.answer("Заявка не найдена или уже обработана.", show_alert=True)

    if time.time() - req["created"] > TRANSFER_CONFIRM_TTL:
        pending_transfers.pop(token, None)
        await callback.message.edit_text("⌛ Заявка на перевод истекла.")
        return await callback.answer()

    if callback.from_user.id != req["sender_id"]:
        return await callback.answer("Подтверждать может только отправитель.", show_alert=True)

    async with pool.acquire() as conn:
        sender_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            req["sender_id"], req["chat_id"]
        )
        if sender_pts is None:
            sender_pts = await get_join_points(req["chat_id"])

        target_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            req["target_id"], req["chat_id"]
        )
        if target_pts is None:
            target_pts = await get_join_points(req["chat_id"])

        actual_received = req["received"]
        actual_spent = req["spent"]

        if target_pts + actual_received > BALANCE_MAX:
            pending_transfers.pop(token, None)
            await callback.message.edit_text(
                f"❌ Перевод невозможен: у получателя будет больше {BALANCE_MAX} баллов."
            )
            return await callback.answer()

        if sender_pts < actual_spent:
            pending_transfers.pop(token, None)
            await callback.message.edit_text("❌ Перевод невозможен: недостаточно баллов у отправителя.")
            return await callback.answer()

        if sender_pts - actual_spent < MIN_POINTS_TO_TRANSFER:
            pending_transfers.pop(token, None)
            await callback.message.edit_text(
                f"❌ Перевод невозможен: после перевода у отправителя должно остаться минимум {MIN_POINTS_TO_TRANSFER} баллов."
            )
            return await callback.answer()

        new_sender = sender_pts - actual_spent
        new_target = target_pts + actual_received

        await conn.execute(
            "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
            new_sender, req["sender_id"], req["chat_id"]
        )
        await conn.execute(
            "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
            new_target, req["target_id"], req["chat_id"]
        )

    pending_transfers.pop(token, None)

    try:
        chat_title = callback.message.chat.title or str(req["chat_id"])
    except Exception:
        chat_title = str(req["chat_id"])

    sender_l = silent_link(req["sender_name"], req["sender_id"])
    target_l = silent_link(req["target_name"], req["target_id"])

    await log_to_owner(
        "🧾 <b>Лог перевода баллов</b>\n"
        f"🏷 Чат: <b>{chat_title}</b> (<code>{req['chat_id']}</code>)\n"
        f"👤 Отправитель: {sender_l} (<code>{req['sender_id']}</code>)\n"
        f"🎯 Получатель: {target_l} (<code>{req['target_id']}</code>)\n"
        f"📈 Получено: <b>{actual_received}</b>\n"
        f"📉 Списано: <b>{actual_spent}</b> (курс {TRANSFER_RATE}:1)\n"
        f"💠 Балансы после:\n"
        f"   • отправитель: <b>{new_sender}</b>\n"
        f"   • получатель: <b>{new_target}</b>"
    )

    await callback.message.edit_text(
        f"✅ Перевод выполнен!\n"
        f"💠 {sender_l} передал {target_l} <b>{actual_received}</b> балл(ов).\n"
        f"📉 Списано: <b>{actual_spent}</b> (курс {TRANSFER_RATE}:1)",
        disable_web_page_preview=True
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("tcancel:"))
async def transfer_cancel(callback: types.CallbackQuery):
    token = callback.data.split(":", 1)[1]
    req = pending_transfers.get(token)

    if not req:
        return await callback.answer("Заявка не найдена или уже обработана.", show_alert=True)

    if callback.from_user.id != req["sender_id"]:
        return await callback.answer("Отменить может только отправитель.", show_alert=True)

    pending_transfers.pop(token, None)
    await callback.message.edit_text("❌ Перевод отменён.")
    await callback.answer()


@dp.message(Command("балл", "ball"))
async def change_points(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 2):
        return

    args = message.text.split()
    if len(args) < 2:
        return await message.reply(
            "Используй: <code>/балл +10 @username причина</code> или ответом: <code>/балл +10 причина</code>"
        )

    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("Ошибка! Пример: <code>/балл -2 @user флуд</code>")

    tid, tname, tuname, err = await resolve_target(message, args)
    if err == "no_target":
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.")
    if err == "not_found":
        return await message.reply("❌ Пользователь не найден. Пусть он напишет сообщение в любой чат с ботом.")
    if err == "not_in_chat":
        return await message.reply("❌ Этот @username не найден среди участников этого чата.")

    if not await user_exists_in_chat(tid, message.chat.id):
        return await message.reply("❌ Пользователь не найден в базе этого чата.\nПусть он напишет сообщение.")

    reason = extract_reason_from_args(args)
    reason_line_chat = f"\n📝 Причина: <i>{reason}</i>" if reason else ""
    reason_line_log = f"\n📝 Причина: <b>{reason}</b>" if reason else "\n📝 Причина: <i>не указана</i>"

    async with pool.acquire() as conn:
        current_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            tid, message.chat.id
        )
        if current_pts is None:
            current_pts = await get_join_points(message.chat.id)

        if amount > 0 and current_pts + amount > BALANCE_MAX:
            return await message.reply(
                f"❌ Нельзя начислить столько баллов: будет превышен лимит <b>{BALANCE_MAX}</b>.\n"
                f"Сейчас: <b>{current_pts}</b>, пытаешься начислить: <b>{amount}</b>, получилось бы: <b>{current_pts + amount}</b>."
            )

        if amount < 0 and current_pts + amount < BALANCE_MIN:
            return await message.reply(
                f"❌ Нельзя снять столько баллов: баланс не может быть меньше <b>{BALANCE_MIN}</b>.\n"
                f"Сейчас: <b>{current_pts}</b>, пытаешься снять: <b>{abs(amount)}</b>, получилось бы: <b>{current_pts + amount}</b>."
            )

        new_pts = current_pts + amount
        await conn.execute(
            "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
            new_pts, tid, message.chat.id
        )

    admin_l = silent_link(message.from_user.first_name, message.from_user.id)
    target_l = silent_link(tname, tid)

    if amount >= 0:
        await message.answer(
            f"⬆️ Администратор {admin_l} начислил {target_l} <b>{abs(amount)}</b> баллов.{reason_line_chat}",
            disable_web_page_preview=True
        )
    else:
        await message.answer(
            f"⬇️ Администратор {admin_l} снял у {target_l} <b>{abs(amount)}</b> баллов.{reason_line_chat}",
            disable_web_page_preview=True
        )

    chat_title = message.chat.title or str(message.chat.id)
    action = "начислил" if amount >= 0 else "снял"
    sign = "+" if amount >= 0 else "-"

    await log_to_owner(
        "🧾 <b>Лог баллов</b>\n"
        f"🏷 Чат: <b>{chat_title}</b> (<code>{message.chat.id}</code>)\n"
        f"👮 Админ: {admin_l} (<code>{message.from_user.id}</code>)\n"
        f"👤 Участник: {target_l} (<code>{tid}</code>)\n"
        f"📌 Действие: <b>{action}</b> {sign}<b>{abs(amount)}</b>\n"
        f"💠 Новый баланс: <b>{new_pts}</b>"
        f"{reason_line_log}"
    )


@dp.message(Command("баллм", "ballm"))
async def change_points_mass(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 2):
        return

    args = message.text.split()
    if len(args) < 4:
        return await message.reply(
            "Используй: <code>/баллм -5 @user1 @user2 причина</code>\n"
            "Можно указать много @username."
        )

    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("Ошибка! Пример: <code>/баллм -5 @user1 @user2 флуд</code>")

    mentions = [a for a in args[2:] if a.startswith("@")]
    if not mentions:
        return await message.reply("⚠️ Укажи хотя бы один @username.")

    reason = extract_mass_reason(args)
    reason_line_chat = f"\n📝 Причина: <i>{reason}</i>" if reason else ""
    reason_line_log = f"\n📝 Причина: <b>{reason}</b>" if reason else "\n📝 Причина: <i>не указана</i>"

    admin_l = silent_link(message.from_user.first_name, message.from_user.id)
    chat_title = message.chat.title or str(message.chat.id)

    ok_lines = []
    fail_lines = []

    async with pool.acquire() as conn:
        for raw in mentions:
            uname = raw.replace("@", "").lower()

            row = await conn.fetchrow(
                "SELECT user_id, name, points, username FROM users WHERE chat_id = $1 AND username = $2",
                message.chat.id, uname
            )
            if not row:
                fail_lines.append(f"• @{uname}: не найден в этом чате")
                continue

            tid = int(row["user_id"])
            tname = row["name"] or uname
            current_pts = row["points"]
            if current_pts is None:
                current_pts = await get_join_points(message.chat.id)

            if amount > 0 and current_pts + amount > BALANCE_MAX:
                fail_lines.append(
                    f"• {tname}: нельзя +{amount} (сейчас {current_pts}, было бы {current_pts + amount} > {BALANCE_MAX})"
                )
                continue

            if amount < 0 and current_pts + amount < BALANCE_MIN:
                fail_lines.append(
                    f"• {tname}: нельзя {amount} (сейчас {current_pts}, было бы {current_pts + amount} < {BALANCE_MIN})"
                )
                continue

            new_pts = current_pts + amount

            await conn.execute(
                "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
                new_pts, tid, message.chat.id
            )

            ok_lines.append(f"• {silent_link(tname, tid)}: {current_pts} → <b>{new_pts}</b>")

    if not ok_lines and fail_lines:
        return await message.answer("❌ Никому не удалось изменить баллы.\n\n" + "\n".join(fail_lines))

    sign = "+" if amount >= 0 else "-"
    action_word = "начислил" if amount >= 0 else "снял"

    text = (
        f"<b>🧾 Массовое изменение баллов</b>\n"
        f"👮 Админ: {admin_l}\n"
        f"📌 Действие: <b>{action_word}</b> {sign}<b>{abs(amount)}</b>\n\n"
        f"<b>✅ Успешно:</b>\n" + "\n".join(ok_lines) +
        (f"\n\n<b>⚠️ Ошибки:</b>\n" + "\n".join(fail_lines) if fail_lines else "") +
        reason_line_chat
    )

    await message.answer(text, disable_web_page_preview=True)

    await log_to_owner(
        "🧾 <b>Лог массовых баллов</b>\n"
        f"🏷 Чат: <b>{chat_title}</b> (<code>{message.chat.id}</code>)\n"
        f"👮 Админ: {admin_l} (<code>{message.from_user.id}</code>)\n"
        f"📌 Действие: <b>{action_word}</b> {sign}<b>{abs(amount)}</b>\n"
        f"✅ Успешно: <b>{len(ok_lines)}</b>\n"
        f"⚠️ Ошибки: <b>{len(fail_lines)}</b>"
        f"{reason_line_log}"
    )


@dp.message(Command("повысить", "promote"))
async def promote_owner(message: types.Message):
    if message.from_user.id != OWNER_ID and not await has_level(message.from_user.id, message.chat.id, 2):
        return

    args = message.text.split()
    level = 1
    if len(args) >= 3:
        try:
            level = int(args[2])
        except ValueError:
            level = 1
    level = max(1, min(2, level))

    tid, name, tuname, err = await resolve_target(message, args)
    if err == "no_target":
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: <code>/повысить @user 2</code>")
    if err == "not_found":
        return await message.reply("❌ Пользователь не найден. Пусть он напишет сообщение в любой чат с ботом.")
    if err == "not_in_chat":
        return await message.reply("❌ Этот @username не найден среди участников этого чата.")

    if tid == OWNER_ID:
        return await message.reply("❌ Нельзя менять права владельца.")

    if not await user_exists_in_chat(tid, message.chat.id):
        return await message.reply("❌ Пользователь не найден в базе этого чата.\nПусть он напишет сообщение.")

    await set_admin_level(message.chat.id, tid, level, mode="force")
    await message.answer(f"✅ {silent_link(name, tid)} теперь <b>админ {level}</b> уровня.", disable_web_page_preview=True)


@dp.message(Command("админ", "admin"))
async def make_admin_lvl1(message: types.Message):
    issuer_id = message.from_user.id
    issuer_is_owner = (issuer_id == OWNER_ID)
    issuer_is_lvl2 = await has_level(issuer_id, message.chat.id, 2)
    if not issuer_is_owner and not issuer_is_lvl2:
        return

    args = message.text.split()
    tid, name, tuname, err = await resolve_target(message, args)
    if err == "no_target":
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: <code>/админ @user</code>")
    if err == "not_found":
        return await message.reply("❌ Пользователь не найден. Пусть он напишет сообщение в любой чат с ботом.")
    if err == "not_in_chat":
        return await message.reply("❌ Этот @username не найден среди участников этого чата.")

    if tid == OWNER_ID:
        return await message.reply("❌ Нельзя менять права владельца.")

    if not await user_exists_in_chat(tid, message.chat.id):
        return await message.reply("❌ Пользователь не найден в базе этого чата.\nПусть он напишет сообщение.")

    current = await get_admin_level(tid, message.chat.id)
    if current >= 2:
        return await message.answer(f"ℹ️ {silent_link(name, tid)} уже <b>админ 2</b> уровня.", disable_web_page_preview=True)

    await set_admin_level(message.chat.id, tid, 1, mode="max")
    await message.answer(f"✅ {silent_link(name, tid)} теперь <b>админ 1</b> уровня.", disable_web_page_preview=True)


@dp.message(Command("разжаловать", "unadmin"))
async def remove_admin(message: types.Message):
    issuer_id = message.from_user.id
    issuer_is_owner = (issuer_id == OWNER_ID)
    issuer_is_lvl2 = await has_level(issuer_id, message.chat.id, 2)
    if not issuer_is_owner and not issuer_is_lvl2:
        return

    args = message.text.split()
    tid, name, tuname, err = await resolve_target(message, args)
    if err == "no_target":
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: <code>/разжаловать @user</code>")
    if err == "not_found":
        return await message.reply("❌ Пользователь не найден. Пусть он напишет сообщение в любой чат с ботом.")
    if err == "not_in_chat":
        return await message.reply("❌ Этот @username не найден среди участников этого чата.")

    if tid == OWNER_ID:
        return await message.reply("❌ Нельзя разжаловать владельца.")

    if not await user_exists_in_chat(tid, message.chat.id):
        return await message.reply("❌ Пользователь не найден в базе этого чата.")

    current = await get_admin_level(tid, message.chat.id)
    if current <= 0:
        return await message.answer("ℹ️ Этот пользователь не админ.", disable_web_page_preview=True)

    if not issuer_is_owner and current >= 2:
        return await message.reply("❌ Ты можешь снимать только <b>админа 1</b> уровня.")

    await remove_admin_level(message.chat.id, tid)
    await message.answer(f"❌ {silent_link(name, tid)} больше <b>не админ</b>.", disable_web_page_preview=True)


@dp.message(Command("бадмины", "badmins"))
async def list_admins(message: types.Message):
    if message.from_user.id != OWNER_ID and not await has_level(message.from_user.id, message.chat.id, 2):
        return

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                a.user_id,
                MAX(a.level) AS level,
                u.name,
                u.username
            FROM admins a
            LEFT JOIN users u
                ON u.user_id = a.user_id AND u.chat_id = a.chat_id
            WHERE a.chat_id = $1
            GROUP BY a.user_id, u.name, u.username
            ORDER BY MAX(a.level) DESC, a.user_id ASC
        """, message.chat.id)

    if not rows:
        return await message.answer("Список админов пуст.")

    lines = ["<b>🛡 Список админов</b>\n"]
    for i, r in enumerate(rows, 1):
        name = r["name"] or "Без имени"
        username = r["username"]
        level = int(r["level"]) if r["level"] is not None else 1

        if username:
            admin_display = hlink(name, f"https://t.me/{username}")
        else:
            admin_display = name

        lines.append(f"{i}. {admin_display} — <b>{level}</b> уровень")

    await message.answer("\n".join(lines), disable_web_page_preview=True)


@dp.message()
async def auto_update(message: types.Message):
    if message.from_user and message.chat.type in ["group", "supergroup"]:
        await update_user_data(
            message.from_user.id,
            message.chat.id,
            message.from_user.first_name,
            message.from_user.username
        )


async def main():
    print(">>> Бот запущен!")
    await init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())