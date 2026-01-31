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

bot = Bot(token=TOKEN, parse_mode="HTML")
dp = Dispatcher()

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None


# ---------------------- DB ----------------------
async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT,
            chat_id BIGINT,
            points INT DEFAULT 50,
            name TEXT,
            username TEXT,
            PRIMARY KEY (user_id, chat_id)
        )
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id BIGINT PRIMARY KEY,
            level INT NOT NULL DEFAULT 1
        )
        """)
        await conn.execute("ALTER TABLE admins ADD COLUMN IF NOT EXISTS level INT NOT NULL DEFAULT 1")

        await conn.execute("UPDATE users SET points = 50 WHERE points = 0")


async def update_user_data(user_id, chat_id, name, username=None):
    if username:
        username = username.replace("@", "").lower()

    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, chat_id, points, name, username)
        VALUES ($1, $2, 50, $3, $4)
        ON CONFLICT (user_id, chat_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            username = COALESCE(EXCLUDED.username, users.username)
        """, user_id, chat_id, name, username)


# ---------------------- УРОВНИ АДМИНКИ ----------------------
async def get_admin_level(user_id: int) -> int:
    if user_id == OWNER_ID:
        return 999
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT level FROM admins WHERE user_id = $1", user_id)
    return row["level"] if row else 0


async def has_level(user_id: int, min_level: int) -> bool:
    return (await get_admin_level(user_id)) >= min_level


async def get_target_id(message: types.Message, args: list):
    if message.reply_to_message:
        return message.reply_to_message.from_user.id, message.reply_to_message.from_user.first_name

    for arg in args:
        if arg.startswith("@"):
            uname = arg.replace("@", "").lower()
            async with pool.acquire() as conn:
                res = await conn.fetchrow(
                    "SELECT user_id, name FROM users WHERE username = $1 AND chat_id = $2",
                    uname, message.chat.id
                )
            if res:
                return res["user_id"], res["name"]
            return None, "not_found"

    return None, None


# ---------------------- ТОП ----------------------
def silent_link(name, user_id):
    return f'<a href="tg://user?id={user_id}">{name}</a>'


async def log_to_owner(text: str):
    try:
        await bot.send_message(OWNER_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logging.warning(f"Failed to send log to owner: {e}")


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
        total_pages = (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE

        top = await conn.fetch(
            "SELECT user_id, name, points, username FROM users "
            "WHERE chat_id = $1 ORDER BY points DESC LIMIT $2 OFFSET $3",
            message.chat.id, ITEMS_PER_PAGE, offset
        )

    if not top:
        return await message.answer("💠 Список лидеров пока пуст.")

    res = [f"💠 {hbold('ТОП ЛИДЕРОВ')} ({page + 1}/{total_pages})\n"]
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


# ---------------------- Команды ----------------------
@dp.message(Command("start", "bhelp", "бпомощь"))
async def cmd_help(message: types.Message):
    user_id = message.from_user.id
    await update_user_data(user_id, message.chat.id, message.from_user.first_name, message.from_user.username)

    if user_id == OWNER_ID:
        text = (
            "<b>👑 ПАНЕЛЬ ВЛАДЕЛЬЦА</b>\n\n"
            "👤 <b>Общие:</b>\n"
            "• /моиб — узнать свое количество баллов\n"
            "• /топб — топ лидеров\n"
            "• /передать [число] @user — передать баллы другому участнику\n\n"
            "🛡 <b>Администрирование:</b>\n"
            "• /балл [+/- число] @user — начислить/снять\n"
            "• /инфо @user — чекнуть баланс\n\n"
            "⚙️ <b>Управление доступом:</b>\n"
            "• /повысить @user [1/2] — выдать админку\n"
            "• /админ @user — выдать админку 1 уровня\n"
            "• /разжаловать @user — снять админа\n"
            "• /бадмины — список админов\n"
        )
    elif await has_level(user_id, 1):
        lvl = await get_admin_level(user_id)

        if lvl >= 2:
            text = (
                f"<b>🛡 ПАНЕЛЬ АДМИНИСТРАТОРА</b> (уровень <b>{lvl}</b>)\n\n"
                "👤 <b>Общие:</b>\n"
                "• /моиб — узнать свое количество баллов\n"
                "• /топб — открыть таблицу лидеров\n"
                "• /передать [число] @user — передать баллы другому участнику\n\n"
                "🕹 <b>Управление:</b>\n"
                "• /инфо @user — посмотреть баллы юзера\n"
                "• /балл [+/- число] @user — выдать/забрать баллы\n\n"
                "🛡 <b>Админка:</b>\n"
                "• /админ @user — выдать админку\n"
                "• /разжаловать @user — снять админа\n"
                "• /бадмины — список админов\n"
            )
        else:
            text = (
                f"<b>🛡 ПАНЕЛЬ АДМИНИСТРАТОРА</b> (уровень <b>{lvl}</b>)\n\n"
                "👤 <b>Общие:</b>\n"
                "• /моиб — узнать свое количество баллов\n"
                "• /топб — открыть таблицу лидеров\n"
                "• /передать [число] @user — передать баллы другому участнику\n\n"
                "🕹 <b>Доступ:</b>\n"
                "• /инфо @user — посмотреть баллы юзера\n"
            )
    else:
        text = (
            "<b>👤 МЕНЮ УЧАСТНИКА</b>\n\n"
            "• /моиб — узнать свое количество баллов\n"
            "• /топб — топ участников\n"
            "• /передать [число] @user — передать баллы другому участнику\n\n"
            "<i>Чтобы попасть в топ, проявляйте активность в чате!</i>"
        )
    await message.answer(text)


@dp.message(Command("моиб", "myb"))
async def my_points(message: types.Message):
    await update_user_data(message.from_user.id, message.chat.id, message.from_user.first_name, message.from_user.username)
    async with pool.acquire() as conn:
        points = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            message.from_user.id, message.chat.id
        )
    points = points if points is not None else 50
    await message.reply(f"💠 {message.from_user.first_name}, у тебя <b>{points}</b> баллов.")


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

    tid, tname = await get_target_id(message, args)

    if not tid:
        if tname == "not_found":
            return await message.reply("❌ Пользователь не найден в этом чате.")
        return await message.reply("⚠️ Укажите @username или ответьте на сообщение.")

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
        sender_pts = sender_pts if sender_pts is not None else 50

        await update_user_data(tid, message.chat.id, tname)
        target_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            tid, message.chat.id
        )
        target_pts = target_pts if target_pts is not None else 50

    max_can_receive = max(0, 100 - target_pts)
    actual_received = min(received_raw, max_can_receive)

    if actual_received <= 0:
        return await message.reply("❌ У получателя уже максимум баллов (100). Перевод невозможен.")

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
        sender_pts = sender_pts if sender_pts is not None else 50

        target_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            req["target_id"], req["chat_id"]
        )
        target_pts = target_pts if target_pts is not None else 50

        max_can_receive = max(0, 100 - target_pts)
        actual_received = min(req["received"], max_can_receive)
        actual_spent = actual_received * TRANSFER_RATE

        if actual_received <= 0:
            pending_transfers.pop(token, None)
            await callback.message.edit_text("❌ Перевод невозможен: у получателя уже 100 баллов.")
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

        new_sender = max(0, min(100, sender_pts - actual_spent))
        new_target = max(0, min(100, target_pts + actual_received))

        await conn.execute(
            "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
            new_sender, req["sender_id"], req["chat_id"]
        )
        await conn.execute(
            "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
            new_target, req["target_id"], req["chat_id"]
        )

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

    pending_transfers.pop(token, None)

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
    if not await has_level(message.from_user.id, 2):
        return

    args = message.text.split()
    if len(args) < 2:
        return await message.reply("Используй: <code>/балл +10 @username</code>")

    try:
        amount = int(args[1])
        tid, tname = await get_target_id(message, args)

        if tid:
            await update_user_data(tid, message.chat.id, tname)

            async with pool.acquire() as conn:
                current_pts = await conn.fetchval(
                    "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
                    tid, message.chat.id
                )
                current_pts = current_pts if current_pts is not None else 50

                new_pts = max(0, min(100, current_pts + amount))
                actual_change = new_pts - current_pts

                await conn.execute(
                    "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
                    new_pts, tid, message.chat.id
                )

            admin_l = silent_link(message.from_user.first_name, message.from_user.id)
            target_l = silent_link(tname, tid)

            if actual_change >= 0:
                await message.answer(f"⬆️ Администратор {admin_l} начислил {target_l} <b>{abs(actual_change)}</b> баллов.")
            else:
                await message.answer(f"⬇️ Администратор {admin_l} снял у {target_l} <b>{abs(actual_change)}</b> баллов.")

            chat_title = message.chat.title or str(message.chat.id)
            action = "начислил" if actual_change >= 0 else "снял"
            sign = "+" if actual_change >= 0 else "-"

            await log_to_owner(
                "🧾 <b>Лог баллов</b>\n"
                f"🏷 Чат: <b>{chat_title}</b> (<code>{message.chat.id}</code>)\n"
                f"👮 Админ: {admin_l} (<code>{message.from_user.id}</code>)\n"
                f"👤 Участник: {target_l} (<code>{tid}</code>)\n"
                f"📌 Действие: <b>{action}</b> {sign}<b>{abs(actual_change)}</b>\n"
                f"💠 Новый баланс: <b>{new_pts}</b>"
            )

        elif tname == "not_found":
            await message.reply("❌ Пользователь не найден в этом чате.")
        else:
            await message.reply("⚠️ Укажите @username или ответьте на сообщение.")

    except ValueError:
        await message.reply("Ошибка! Введите число.")


@dp.message(Command("инфо", "stats"))
async def check_stats(message: types.Message):
    if not await has_level(message.from_user.id, 1):
        return

    tid, tname = await get_target_id(message, message.text.split())

    if tid:
        async with pool.acquire() as conn:
            points = await conn.fetchval(
                "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
                tid, message.chat.id
            )
        points = points if points is not None else 50
        user_link = silent_link(tname, tid)

        await message.answer(
            f"<b>📊 Информация о пользователе</b>\n"
            f"👤 Имя: {user_link}\n"
            f"💠 Баланс: <b>{points}</b> баллов"
        )
    elif tname == "not_found":
        await message.reply("<b>❌ Ошибка:</b> Пользователь не найден.")
    else:
        await message.reply("<b>⚠️ Внимание:</b> Укажите @username или ответьте на сообщение.")


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


# ----------- Управление админами -----------
@dp.message(Command("повысить", "promote"))
async def promote_owner(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return

    args = message.text.split()
    level = 1
    if len(args) >= 3:
        try:
            level = int(args[2])
        except ValueError:
            level = 1

    if level < 1:
        level = 1
    if level > 2:
        level = 2

    tid, name = await get_target_id(message, args)
    if not tid:
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: <code>/повысить @user 2</code>")

    if tid == OWNER_ID:
        return await message.reply("❌ Нельзя менять права владельца.")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO admins (user_id, level)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET level = $2
            """,
            tid, level
        )

    await message.answer(f"✅ {silent_link(name, tid)} теперь <b>админ {level}</b> уровня.")


@dp.message(Command("админ", "admin"))
async def make_admin_lvl1(message: types.Message):
    issuer_id = message.from_user.id
    if issuer_id != OWNER_ID and not await has_level(issuer_id, 2):
        return

    args = message.text.split()
    tid, name = await get_target_id(message, args)
    if not tid:
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: <code>/админ @user</code>")

    if tid == OWNER_ID:
        return await message.reply("❌ Нельзя менять права владельца.")

    async with pool.acquire() as conn:
        current = await conn.fetchval("SELECT level FROM admins WHERE user_id = $1", tid)
        if current == 2:
            return await message.answer(f"ℹ️ {silent_link(name, tid)} уже <b>админ 2</b> уровня.")

        await conn.execute(
            """
            INSERT INTO admins (user_id, level)
            VALUES ($1, 1)
            ON CONFLICT (user_id)
            DO UPDATE SET level = GREATEST(admins.level, 1)
            """,
            tid
        )

    await message.answer(f"✅ {silent_link(name, tid)} теперь <b>админ 1</b> уровня.")


@dp.message(Command("разжаловать", "unadmin"))
async def remove_admin(message: types.Message):
    issuer_id = message.from_user.id
    issuer_is_owner = (issuer_id == OWNER_ID)
    issuer_is_lvl2 = await has_level(issuer_id, 2)

    if not issuer_is_owner and not issuer_is_lvl2:
        return

    args = message.text.split()
    tid, name = await get_target_id(message, args)
    if not tid:
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: <code>/разжаловать @user</code>")

    if tid == OWNER_ID:
        return await message.reply("❌ Нельзя разжаловать владельца.")

    async with pool.acquire() as conn:
        current = await conn.fetchval("SELECT level FROM admins WHERE user_id = $1", tid)

        if not current:
            return await message.answer("ℹ️ Этот пользователь не админ.")

        if not issuer_is_owner and current >= 2:
            return await message.reply("❌ Ты можешь снимать только <b>админа 1</b> уровня.")

        await conn.execute("DELETE FROM admins WHERE user_id = $1", tid)

    await message.answer(f"❌ {silent_link(name, tid)} больше <b>не админ</b>.")


# ---------------------- /бадмины ----------------------
@dp.message(Command("бадмины", "badmins"))
async def list_admins(message: types.Message):
    if message.from_user.id != OWNER_ID and not await has_level(message.from_user.id, 2):
        return

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                a.user_id,
                a.level,
                u.name,
                u.username
            FROM admins a
            LEFT JOIN (
                SELECT DISTINCT ON (user_id)
                    user_id, name, username
                FROM users
                ORDER BY user_id, chat_id DESC
            ) u ON u.user_id = a.user_id
            ORDER BY a.level DESC, a.user_id ASC
        """)

    if not rows:
        return await message.answer("Список админов пуст.")

    lines = ["<b>🛡 Список админов</b>\n"]
    for i, r in enumerate(rows, 1):
        name = r["name"] or "Без имени"
        username = r["username"]
        level = r["level"]

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


# ---------------------- Main ----------------------
async def main():
    print(">>> Бот запущен!")
    await init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())