import os
import asyncio
import sqlite3
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.markdown import hbold, hlink
from aiogram import Bot

TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = 1875573844
ITEMS_PER_PAGE = 30

logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=TOKEN,
    parse_mode="HTML"  # задаём напрямую
)
dp = Dispatcher()

conn = sqlite3.connect("users_points.db")
cur = conn.cursor()

# ------------------ INIT DB ------------------
def init_db():
    cur.execute("""CREATE TABLE IF NOT EXISTS users 
                   (user_id INTEGER, 
                    chat_id INTEGER,
                    points INTEGER DEFAULT 50, 
                    name TEXT, 
                    username TEXT,
                    PRIMARY KEY (user_id, chat_id))""")
    cur.execute("CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)")
    cur.execute("UPDATE users SET points = 50 WHERE points = 0")
    conn.commit()

init_db()

# ------------------ SILENT LINK ------------------
def silent_link(name, user_id, username=None):
    if username:
        return f'<a href="https://t.me/{username}">{name}</a>'
    else:
        return hbold(name)

# ------------------ UPDATE USER DATA ------------------
def update_user_data(user_id, chat_id, name, username=None):
    username = username.replace("@", "").lower() if username else None
    cur.execute("""INSERT OR IGNORE INTO users (user_id, chat_id, points, name, username) 
                   VALUES (?, ?, 50, ?, ?)""", (user_id, chat_id, name, username))
    if username:
        cur.execute("UPDATE users SET name = ?, username = ? WHERE user_id = ? AND chat_id = ?", 
                    (name, username, user_id, chat_id))
    else:
        cur.execute("UPDATE users SET name = ? WHERE user_id = ? AND chat_id = ?", 
                    (name, user_id, chat_id))
    conn.commit()

# ------------------ CHECK ADMIN ------------------
def is_admin(user_id):
    if user_id == OWNER_ID: return True
    cur.execute("SELECT user_id FROM admins WHERE user_id = ?", (user_id,))
    return cur.fetchone() is not None

# ------------------ GET TARGET ------------------
async def get_target_id(message: types.Message, args: list):
    if message.reply_to_message:
        return message.reply_to_message.from_user.id, message.reply_to_message.from_user.first_name
    for arg in args:
        if arg.startswith("@"):
            uname = arg.replace("@", "").lower()
            cur.execute("SELECT user_id, name FROM users WHERE username = ? AND chat_id = ?", (uname, message.chat.id))
            res = cur.fetchone()
            if res: return res
            else: return None, "not_found"
    return None, None

# ------------------ TOP KEYBOARD ------------------
def get_top_keyboard(current_page: int, total_pages: int, user_id: int):
    builder = InlineKeyboardBuilder()
    if current_page > 0:
        builder.button(text="⬅️", callback_data=f"top:{user_id}:{current_page - 1}")
    if current_page < total_pages - 1:
        builder.button(text="➡️", callback_data=f"top:{user_id}:{current_page + 1}")
    builder.adjust(2)
    return builder.as_markup()

# ------------------ SEND TOP ------------------
async def send_top_page(message: types.Message, page: int, owner_id: int, edit: bool = False):
    offset = page * ITEMS_PER_PAGE

    cur.execute("SELECT COUNT(*) FROM users WHERE chat_id = ?", (message.chat.id,))
    total_count = cur.fetchone()[0]
    total_pages = (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE

    cur.execute(
        "SELECT user_id, name, username, points FROM users WHERE chat_id = ? ORDER BY points DESC LIMIT ? OFFSET ?",
        (message.chat.id, ITEMS_PER_PAGE, offset)
    )
    top = cur.fetchall()

    if not top:
        return await message.answer("💠 Список лидеров пока пуст.")

    res = [f"💠 {hbold('ТОП ЛИДЕРОВ')} ({page + 1}/{total_pages})\n"]
    for i, (uid, name, username, pts) in enumerate(top, 1 + offset):
        if username:
            user_link = hlink(name, f"https://t.me/{username}")
        else:
            user_link = hbold(name)
        res.append(f"{i}. {user_link} — {hbold(pts)}")

    text = "\n".join(res)
    kb = get_top_keyboard(page, total_pages, owner_id)

    if edit:
        await message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    else:
        await message.answer(text, reply_markup=kb, disable_web_page_preview=True)

# ------------------ COMMANDS ------------------
@dp.message(Command("start", "bhelp", "бпомощь"))
async def cmd_help(message: types.Message):
    user_id = message.from_user.id
    update_user_data(user_id, message.chat.id, message.from_user.first_name, message.from_user.username)

    if user_id == OWNER_ID:
        text = (
            "<b>👑 ПАНЕЛЬ ВЛАДЕЛЬЦА</b>\n\n"
            "👤 <b>Общие:</b>\n• /баланс — ваш счет в этом чате\n\n"
            "🛡 <b>Администрирование:</b>\n• /балл [+/- число] @user — начислить/снять\n"
            "• /инфо @user — чекнуть баланс\n• /бтоп — топ лидеров\n\n"
            "⚙️ <b>Управление доступом:</b>\n• /админ @user — назначить админа\n• /разжаловать @user — снять админа"
        )
    elif is_admin(user_id):
        text = (
            "<b>🛡 ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n\n"
            "👤 <b>Общие:</b>\n• /баланс — ваш счет\n\n"
            "🕹 <b>Управление:</b>\n• /балл [+/- число] @user — выдать/забрать баллы\n"
            "• /инфо @user — посмотреть баллы юзера\n• /бтоп — открыть таблицу лидеров"
        )
    else:
        text = (
            "<b>👤 МЕНЮ УЧАСТНИКА</b>\n\n"
            "• /баланс — узнать свой счет в этой группе\n"
            "<i>Чтобы попасть в топ, проявляйте активность в чате!</i>"
        )
    await message.answer(text, disable_web_page_preview=True)

@dp.message(Command("баланс", "balance"))
async def my_points(message: types.Message):
    update_user_data(message.from_user.id, message.chat.id, message.from_user.first_name, message.from_user.username)
    cur.execute("SELECT points FROM users WHERE user_id = ? AND chat_id = ?", (message.from_user.id, message.chat.id))
    res = cur.fetchone()
    pts = res[0] if res else 50
    await message.reply(f"💠 {message.from_user.first_name}, у тебя <b>{pts}</b> баллов.", disable_web_page_preview=True)

@dp.message(Command("балл", "ball"))
async def change_points(message: types.Message):
    if not is_admin(message.from_user.id): return
    args = message.text.split()
    if len(args) < 2: return await message.reply("Используй: <code>/балл +10 @username</code>")

    try:
        amount = int(args[1])
        tid, tname = await get_target_id(message, args)

        if tid:
            update_user_data(tid, message.chat.id, tname)
            cur.execute("SELECT points, username FROM users WHERE user_id = ? AND chat_id = ?", (tid, message.chat.id))
            res = cur.fetchone()
            current_pts = res[0]
            target_username = res[1]

            new_pts = max(0, min(100, current_pts + amount))
            actual_change = new_pts - current_pts

            cur.execute("UPDATE users SET points = ? WHERE user_id = ? AND chat_id = ?", (new_pts, tid, message.chat.id))
            conn.commit()

            admin_l = silent_link(message.from_user.first_name, message.from_user.id, message.from_user.username)
            target_l = silent_link(tname, tid, target_username)

            if actual_change >= 0:
                await message.answer(
                    f"⬆️ Администратор {admin_l} начислил {target_l} <b>{abs(actual_change)}</b> баллов.",
                    disable_web_page_preview=True
                )
            else:
                await message.answer(
                    f"⬇️ Администратор {admin_l} снял у {target_l} <b>{abs(actual_change)}</b> баллов.",
                    disable_web_page_preview=True
                )
        elif tname == "not_found":
            await message.reply("❌ Пользователь не найден в этом чате.", disable_web_page_preview=True)
    except ValueError:
        await message.reply("Ошибка! Введите число.", disable_web_page_preview=True)

@dp.message(Command("инфо", "stats"))
async def check_stats(message: types.Message):
    if not is_admin(message.from_user.id): return

    tid, tname = await get_target_id(message, message.text.split())

    if tid:
        cur.execute("SELECT points, username FROM users WHERE user_id = ? AND chat_id = ?", (tid, message.chat.id))
        res = cur.fetchone()
        points = res[0]
        target_username = res[1]
        user_link = silent_link(tname, tid, target_username)

        await message.answer(
            f"<b>📊 Информация о пользователе</b>\n👤 Имя: {user_link}\n💠 Баланс: <b>{points}</b> баллов",
            disable_web_page_preview=True
        )
    elif tname == "not_found":
        await message.reply("<b>❌ Ошибка:</b> Пользователь не найден.", disable_web_page_preview=True)
    else:
        await message.reply("<b>⚠️ Внимание:</b> Укажите @username или ответьте на сообщение.", disable_web_page_preview=True)

@dp.message(Command("бтоп", "btop"))
async def show_top_command(message: types.Message):
    if not is_admin(message.from_user.id):
        return
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
        return  # только владелец может назначать админов

    args = message.text.split()
    tid = None
    tname = None
    tusername = None

    # 1. Если это reply на сообщение
    if message.reply_to_message:
        user = message.reply_to_message.from_user
        tid = user.id
        tname = user.first_name
        tusername = user.username

    # 2. Если указан @username
    elif len(args) > 1:
        uname = args[1].replace("@", "").lower()
        cur.execute("SELECT user_id, name, username FROM users WHERE username = ? AND chat_id = ?", 
                    (uname, message.chat.id))
        res = cur.fetchone()
        if res:
            tid, tname, tusername = res
        else:
            # Попытка найти через Telegram API
            try:
                user_obj = await bot.get_chat_member(message.chat.id, args[1].replace("@", ""))
                tid = user_obj.user.id
                tname = user_obj.user.first_name
                tusername = user_obj.user.username
            except:
                return await message.reply("❌ Пользователь не найден.")

    # Если удалось определить пользователя
    if tid:
        cur.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (tid,))
        conn.commit()
        await message.answer(
            f"✅ {silent_link(tname, tid, tusername)} теперь <b>админ</b>.",
            disable_web_page_preview=True
        )

@dp.message(Command("разжаловать", "unadmin"))
async def remove_admin(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return  # только владелец может снимать админов

    args = message.text.split()
    tid = None
    tname = None
    tusername = None

    if message.reply_to_message:
        user = message.reply_to_message.from_user
        tid = user.id
        tname = user.first_name
        tusername = user.username

    elif len(args) > 1:
        uname = args[1].replace("@", "").lower()
        cur.execute("SELECT user_id, name, username FROM users WHERE username = ? AND chat_id = ?", 
                    (uname, message.chat.id))
        res = cur.fetchone()
        if res:
            tid, tname, tusername = res
        else:
            try:
                user_obj = await bot.get_chat_member(message.chat.id, args[1].replace("@", ""))
                tid = user_obj.user.id
                tname = user_obj.user.first_name
                tusername = user_obj.user.username
            except:
                return await message.reply("❌ Пользователь не найден.")

    if tid:
        cur.execute("DELETE FROM admins WHERE user_id = ?", (tid,))
        conn.commit()
        await message.answer(
            f"❌ {silent_link(tname, tid, tusername)} больше <b>не админ</b>.",
            disable_web_page_preview=True
        )

# ------------------ AUTO UPDATE ------------------
@dp.message()
async def auto_update(message: types.Message):
    if message.from_user and message.chat.type in ["group", "supergroup"]:
        update_user_data(message.from_user.id, message.chat.id, message.from_user.first_name, message.from_user.username)

# ------------------ PERIODIC UPDATE ------------------
async def update_all_members(chat_id):
    try:
        async for member in bot.get_chat_administrators(chat_id):
            user = member.user
            update_user_data(user.id, chat_id, user.first_name, user.username)
        async for member in bot.get_chat_members(chat_id):
            user = member.user
            update_user_data(user.id, chat_id, user.first_name, user.username)
    except Exception as e:
        print(f"Ошибка при обновлении участников: {e}")

async def periodic_update():
    while True:
        cur.execute("SELECT DISTINCT chat_id FROM users")
        chats = [row[0] for row in cur.fetchall()]
        for chat_id in chats:
            await update_all_members(chat_id)
        await asyncio.sleep(300)  # каждые 5 минут

# ------------------ MAIN ------------------
async def main():
    asyncio.create_task(periodic_update())
    print(">>> Бот запущен локально!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())