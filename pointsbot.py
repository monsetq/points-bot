import asyncio
import logging
import os
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.markdown import hbold, hlink

TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "1875573844"))

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
            user_id BIGINT PRIMARY KEY
        )
        """)
        await conn.execute("UPDATE users SET points = 50 WHERE points = 0")


async def update_user_data(user_id, chat_id, name, username=None):
    if username:
        username = username.replace("@", "").lower()
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, chat_id, points, name, username)
        VALUES ($1, $2, 50, $3, $4)
        ON CONFLICT (user_id, chat_id)
        DO UPDATE SET name = $3, username = $4
        """, user_id, chat_id, name, username)


async def is_admin(user_id):
    if user_id == OWNER_ID:
        return True
    async with pool.acquire() as conn:
        res = await conn.fetchrow("SELECT user_id FROM admins WHERE user_id = $1", user_id)
        return res is not None


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
            else:
                return None, "not_found"
    return None, None


# ---------------------- ТОП ----------------------
def silent_link(name, user_id):
    return f'<a href="tg://user?id={user_id}">{name}</a>'


def get_top_keyboard(current_page: int, total_pages: int, user_id: int):
    builder = InlineKeyboardBuilder()
    if current_page > 0:
        builder.button(text="⬅️", callback_data=f"top:{user_id}:{current_page - 1}")
    if current_page < total_pages - 1:
        builder.button(text="➡️", callback_data=f"top:{user_id}:{current_page + 1}")
    builder.adjust(2)
    return builder.as_markup()


async def send_top_page(message: types.Message, page: int, owner_id: int, edit: bool = False):
    offset = page * ITEMS_PER_PAGE
    async with pool.acquire() as conn:
        total_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE chat_id = $1", message.chat.id)
        total_pages = (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE

        top = await conn.fetch(
            "SELECT user_id, name, points, username FROM users WHERE chat_id = $1 ORDER BY points DESC LIMIT $2 OFFSET $3",
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
            "• /моиб — ваш счет в этом чате\n\n"
            "🛡 <b>Администрирование:</b>\n"
            "• /балл [+/- число] @user — начислить/снять\n"
            "• /инфо @user — чекнуть баланс\n"
            "• /топб — топ лидеров\n\n"
            "⚙️ <b>Управление доступом:</b>\n"
            "• /админ @user — назначить админа\n"
            "• /разжаловать @user — снять админа"
        )
    elif await is_admin(user_id):
        text = (
            "<b>🛡 ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n\n"
            "👤 <b>Общие:</b>\n"
            "• /моиб — ваш счет\n\n"
            "🕹 <b>Управление:</b>\n"
            "• /балл [+/- число] @user — выдать/забрать баллы\n"
            "• /инфо @user — посмотреть баллы юзера\n"
            "• /топб — топ лидеров"
        )
    else:
        text = (
            "<b>👤 МЕНЮ УЧАСТНИКА</b>\n\n"
            "• /моиб — узнать свой счет в этой группе\n"
            "• /топб — топ лидеров\n"
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


@dp.message(Command("балл", "ball"))
async def change_points(message: types.Message):
    if not await is_admin(message.from_user.id):
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
        elif tname == "not_found":
            await message.reply("❌ Пользователь не найден в этом чате.")
    except ValueError:
        await message.reply("Ошибка! Введите число.")


@dp.message(Command("инфо", "stats"))
async def check_stats(message: types.Message):
    if not await is_admin(message.from_user.id):
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


@dp.message(Command("админ", "admin"))
async def make_admin(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    tid, name = await get_target_id(message, message.text.split())
    if tid:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING", tid)
        await message.answer(f"✅ {silent_link(name, tid)} теперь <b>админ</b>.")


@dp.message(Command("разжаловать", "unadmin"))
async def remove_admin(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    tid, name = await get_target_id(message, message.text.split())
    if tid:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM admins WHERE user_id = $1", tid)
        await message.answer(f"❌ {silent_link(name, tid)} больше <b>не админ</b>.")


@dp.message()
async def auto_update(message: types.Message):
    if message.from_user and message.chat.type in ["group", "supergroup"]:
        await update_user_data(message.from_user.id, message.chat.id, message.from_user.first_name, message.from_user.username)


# ---------------------- Main ----------------------
async def main():
    print(">>> Бот запущен!")
    await init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())