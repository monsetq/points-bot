import asyncio
import logging
import os
import asyncpg
import time
import secrets
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "1875573844"))

MENTION_IN_TOP_USER_ID = 6599896838

MIN_POINTS_TO_TRANSFER = 50
TRANSFER_RATE = 3

TRANSFER_CONFIRM_TTL = 300
pending_transfers = {}

ITEMS_PER_PAGE = 30
logging.basicConfig(level=logging.INFO)

BALANCE_MIN = 0
BALANCE_MAX = 100

bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=None)
)
dp = Dispatcher()

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None


PLACEHOLDER = "⬜" 

@dataclass
class RichText:
    parts: List[str]
    entities: List[types.MessageEntity]

    def __init__(self):
        self.parts = []
        self.entities = []

    @property
    def text(self) -> str:
        return "".join(self.parts)

    def add(self, s: str) -> "RichText":
        self.parts.append(str(s))
        return self

    def bold(self, s: str) -> "RichText":
        s = str(s)
        off = len(self.text)
        self.parts.append(s)
        self.entities.append(types.MessageEntity(type="bold", offset=off, length=len(s)))
        return self

    def italic(self, s: str) -> "RichText":
        s = str(s)
        off = len(self.text)
        self.parts.append(s)
        self.entities.append(types.MessageEntity(type="italic", offset=off, length=len(s)))
        return self

    def code(self, s: str) -> "RichText":
        s = str(s)
        off = len(self.text)
        self.parts.append(s)
        self.entities.append(types.MessageEntity(type="code", offset=off, length=len(s)))
        return self

    def link(self, label: str, url: str) -> "RichText":
        label = str(label)
        off = len(self.text)
        self.parts.append(label)
        self.entities.append(types.MessageEntity(type="text_link", offset=off, length=len(label), url=str(url)))
        return self


async def send_rich(message_or_cbmsg, rich: RichText, reply_markup=None, edit: bool = False):
    """
    Универсальная отправка/редактирование: всегда entities.
    Перед отправкой автоматически заменяет настроенные эмодзи на custom_emoji.
    """
    final_text, final_entities = await apply_custom_emojis(
        chat_id=message_or_cbmsg.chat.id,
        text=rich.text,
        entities=rich.entities
    )

    if edit:
        await message_or_cbmsg.edit_text(
    final_text,
    entities=final_entities,
    reply_markup=reply_markup,
    disable_web_page_preview=True,
    parse_mode=None
        )
    else:
        await message_or_cbmsg.answer(
    final_text,
    entities=final_entities,
    reply_markup=reply_markup,
    disable_web_page_preview=True,
    parse_mode=None
        ) 


_EMOJI_CACHE: Dict[int, Tuple[float, Dict[str, Tuple[str, bool]]]] = {}
_EMOJI_CACHE_TTL = 10.0 

async def get_emoji_map(chat_id: int) -> Dict[str, Tuple[str, bool]]:
    """
    returns dict: emoji_text -> (custom_emoji_id, enabled)
    """
    now = time.time()
    cached = _EMOJI_CACHE.get(chat_id)
    if cached and (now - cached[0]) < _EMOJI_CACHE_TTL:
        return cached[1]

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT emoji_text, custom_emoji_id, enabled FROM chat_emojis WHERE chat_id = $1",
            chat_id
        )

    m: Dict[str, Tuple[str, bool]] = {}
    for r in rows:
        et = str(r["emoji_text"])
        cid = r["custom_emoji_id"]
        en = bool(r["enabled"])
        if cid:
            m[et] = (str(cid), en)

    _EMOJI_CACHE[chat_id] = (now, m)
    return m


def _shift_entities(entities: List[types.MessageEntity], start: int, delta: int):
    """
    Сдвигает offset всех entities, которые начинаются ПОСЛЕ start.
    delta может быть отрицательным.
    """
    if delta == 0:
        return
    for e in entities:
        if e.offset > start:
            e.offset += delta


def _overlaps(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return not (a_end <= b_start or b_end <= a_start)


async def apply_custom_emojis(chat_id: int, text: str, entities: List[types.MessageEntity]) -> Tuple[str, List[types.MessageEntity]]:
    """
    Автоматически заменяет ВСЕ настроенные emoji_text на custom_emoji entities.
    Важно: работает с уже существующими entities (bold/link/etc), корректно двигает offset.
    """
    emoji_map = await get_emoji_map(chat_id)
    if not emoji_map:
        return text, entities

    matches = []
    for emoji_text, (custom_id, enabled) in emoji_map.items():
        if not enabled or not custom_id:
            continue
        if not emoji_text:
            continue
        start = 0
        while True:
            idx = text.find(emoji_text, start)
            if idx == -1:
                break
            matches.append((idx, idx + len(emoji_text), emoji_text, custom_id))
            start = idx + len(emoji_text)

    if not matches:
        return text, entities

    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))

    selected = []
    for m in matches:
        s, e, emj, cid = m
        ok = True
        for ss, ee, _, _ in selected:
            if _overlaps(s, e, ss, ee):
                ok = False
                break
        if ok:
            selected.append(m)

    selected.sort(key=lambda x: x[0], reverse=True)

    ents = [types.MessageEntity(**e.model_dump()) for e in entities]

    for s, e, emoji_text, custom_id in selected:
        before = text[:s]
        after = text[e:]
        text = before + PLACEHOLDER + after

        delta = 1 - len(emoji_text)

        _shift_entities(ents, s, delta)

        ents.append(types.MessageEntity(
            type="custom_emoji",
            offset=s,
            length=1,
            custom_emoji_id=str(custom_id)
        ))

    ents.sort(key=lambda x: x.offset)
    return text, ents


async def set_chat_emoji(chat_id: int, emoji_text: str, custom_emoji_id: str, enabled: bool = True):
    emoji_text = (emoji_text or "").strip()
    custom_emoji_id = (custom_emoji_id or "").strip()
    if not emoji_text:
        return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_emojis (chat_id, emoji_text, custom_emoji_id, enabled)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (chat_id, emoji_text)
            DO UPDATE SET custom_emoji_id = EXCLUDED.custom_emoji_id,
                          enabled = EXCLUDED.enabled
        """, chat_id, emoji_text, custom_emoji_id, bool(enabled))

    if chat_id == 0:
        _EMOJI_CACHE.clear()
    else:
        _EMOJI_CACHE.pop(chat_id, None)


async def toggle_chat_emoji(chat_id: int, emoji_text: str, enabled: bool):
    emoji_text = (emoji_text or "").strip()
    if not emoji_text:
        return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_emojis (chat_id, emoji_text, custom_emoji_id, enabled)
            VALUES ($1, $2, NULL, $3)
            ON CONFLICT (chat_id, emoji_text)
            DO UPDATE SET enabled = EXCLUDED.enabled
        """, chat_id, emoji_text, bool(enabled))

    if chat_id == 0:
        _EMOJI_CACHE.clear()
    else:
        _EMOJI_CACHE.pop(chat_id, None)


async def delete_chat_emoji(chat_id: int, emoji_text: str):
    emoji_text = (emoji_text or "").strip()
    if not emoji_text:
        return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM chat_emojis WHERE chat_id = $1 AND emoji_text = $2", chat_id, emoji_text)

    if chat_id == 0:
        _EMOJI_CACHE.clear()
    else:
        _EMOJI_CACHE.pop(chat_id, None)


POINT_ROLES = [
    (0, 49, "😈 Плохиш"),
    (50, 69, "👌 Нормис"),
    (70, 100, "🔥 Крутыш"),
]


def get_point_role(points: int) -> str:
    for mn, mx, title in POINT_ROLES:
        if mn <= points <= mx:
            return title
    if points < POINT_ROLES[0][0]:
        return POINT_ROLES[0][2]
    return POINT_ROLES[-1][2]


def calc_punishment_adjust(points: int) -> tuple[int, int]:
    if points >= 70:
        over = points - 70
        mute_reduce = min(30, (over // 4) * 5)
        warn_reduce = min(3, (over // 7) * 1)
        return -mute_reduce, -warn_reduce

    if points < 50:
        lack = 50 - points
        mute_add = lack * 5
        warn_add = (lack // 2) * 1
        return mute_add, warn_add

    return 0, 0


def fmt_minutes(delta: int) -> str:
    if delta == 0:
        return "без изменений"
    sign = "+" if delta > 0 else "−"
    return f"{sign}{abs(delta)} мин"


def fmt_days(delta: int) -> str:
    if delta == 0:
        return "без изменений"
    sign = "+" if delta > 0 else "−"
    return f"{sign}{abs(delta)} дн"


RATING_INFO_TEXT = (
    "💠 Социальный рейтинг\n\n"
    "• Влияет на наказания и статус в чате\n"
    f"• Старт | 50 (макс. {BALANCE_MAX})\n\n"
    "📈 Высокий рейтинг\n"
    "• наказания мягче\n"
    "• доступны бонусы и фишки\n\n"
    "📉 Низкий рейтинг\n"
    "• наказания строже\n"
    "• нельзя стать администратором\n\n"
    "➕ Как получить\n"
    "• мероприятия\n"
    "• высокая активность\n"
    "• переводы от участников\n\n"
    "➖ За что снимают\n"
    "• нарушения правил\n\n"
    "♻️ Отработка\n"
    "• помощь на мероприятии\n"
    "• высокая активность за сутки\n"
    "(доступна первые 48 часов)\n\n"
    "💱 Баллы = валюта\n"
    "• снятие мута | 10\n"
    "• снятие варна | 15\n"
    "• разбан | 40\n"
    "(тратить баллы нельзя, если их меньше 40)\n\n"
    f"🔁 Переводы | курс {TRANSFER_RATE}:1\n"
    "🧹 Обнуление | раз в 2 месяца\n"
)


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("CREATE SEQUENCE IF NOT EXISTS users_join_seq")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT,
            chat_id BIGINT,
            join_seq BIGINT NOT NULL DEFAULT nextval('users_join_seq'),
            points INT DEFAULT 0,
            name TEXT,
            username TEXT,
            PRIMARY KEY (user_id, chat_id)
        )
        """)
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS join_seq BIGINT")
        await conn.execute("ALTER TABLE users ALTER COLUMN join_seq SET DEFAULT nextval('users_join_seq')")
        await conn.execute("""
        WITH maxs AS (
            SELECT COALESCE(MAX(join_seq), 0) AS m FROM users
        ),
        numbered AS (
            SELECT u.user_id, u.chat_id,
                   (SELECT m FROM maxs) + row_number() OVER (ORDER BY u.chat_id, u.user_id) AS newseq
            FROM users u
            WHERE u.join_seq IS NULL
        )
        UPDATE users u
        SET join_seq = n.newseq
        FROM numbered n
        WHERE u.user_id = n.user_id AND u.chat_id = n.chat_id
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id BIGINT PRIMARY KEY,
            join_points INT NOT NULL DEFAULT 50
        )
        """)
        await conn.execute("ALTER TABLE chat_settings ADD COLUMN IF NOT EXISTS rating_text TEXT")

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS admins_v2 (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            level INT NOT NULL DEFAULT 1,
            PRIMARY KEY (chat_id, user_id)
        )
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_emojis (
            chat_id BIGINT NOT NULL,
            emoji_text TEXT NOT NULL,
            custom_emoji_id TEXT,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            PRIMARY KEY (chat_id, emoji_text)
        )
        """)

        try:
            await conn.execute("""
            INSERT INTO admins_v2 (chat_id, user_id, level)
            SELECT COALESCE(chat_id, 0) AS chat_id, user_id, level
            FROM admins
            WHERE user_id IS NOT NULL
            ON CONFLICT (chat_id, user_id)
            DO UPDATE SET level = GREATEST(admins_v2.level, EXCLUDED.level)
            """)
        except Exception:
            try:
                await conn.execute("""
                INSERT INTO admins_v2 (chat_id, user_id, level)
                SELECT 0 AS chat_id, user_id, level
                FROM admins
                WHERE user_id IS NOT NULL
                ON CONFLICT (chat_id, user_id)
                DO UPDATE SET level = GREATEST(admins_v2.level, EXCLUDED.level)
                """)
            except Exception:
                pass

        try:
            await conn.execute("DROP TABLE IF EXISTS admins")
        except Exception:
            pass

        try:
            await conn.execute("ALTER TABLE admins_v2 RENAME TO admins")
        except Exception:
            pass

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


async def get_rating_text(chat_id: int) -> str:
    async with pool.acquire() as conn:
        txt = await conn.fetchval("SELECT rating_text FROM chat_settings WHERE chat_id = $1", chat_id)
        if txt is None:
            await conn.execute(
                "INSERT INTO chat_settings (chat_id, join_points, rating_text) VALUES ($1, 50, NULL) "
                "ON CONFLICT (chat_id) DO NOTHING",
                chat_id
            )
            return RATING_INFO_TEXT
        txt = str(txt).strip()
        return txt if txt else RATING_INFO_TEXT


async def set_rating_text(chat_id: int, new_text: str):
    new_text = (new_text or "").strip()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO chat_settings (chat_id, join_points, rating_text) VALUES ($1, 50, $2) "
            "ON CONFLICT (chat_id) DO UPDATE SET rating_text = EXCLUDED.rating_text",
            chat_id,
            new_text
        )


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
            await conn.execute("""
                INSERT INTO admins (chat_id, user_id, level)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id, user_id)
                DO UPDATE SET level = GREATEST(admins.level, EXCLUDED.level)
            """, chat_id, user_id, level)
        else:
            await conn.execute("""
                INSERT INTO admins (chat_id, user_id, level)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id, user_id)
                DO UPDATE SET level = EXCLUDED.level
            """, chat_id, user_id, level)


async def remove_admin_level(chat_id: int, user_id: int):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE chat_id = $1 AND user_id = $2", chat_id, user_id)


async def resolve_target(message: types.Message, args: list):
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


def get_role_and_lvl(user_id: int, lvl: int) -> str:
    if user_id == OWNER_ID:
        return "owner"
    if lvl >= 2:
        return "admin2"
    if lvl >= 1:
        return "admin1"
    return "member"


def main_menu_kb(owner_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="📖 Команды", callback_data=f"menu:{owner_id}:help")
    b.button(text="💠 О рейтинге", callback_data=f"menu:{owner_id}:rating")
    b.button(text="🏆 Топ", callback_data=f"menu:{owner_id}:top:0")
    b.button(text="📊 Моя статистика", callback_data=f"menu:{owner_id}:stats")
    b.adjust(2, 2)
    return b.as_markup()


def get_top_keyboard(current_page: int, total_pages: int, user_id: int):
    builder = InlineKeyboardBuilder()
    if current_page > 0:
        builder.button(text="⬅️", callback_data=f"top:{user_id}:{current_page - 1}")
    builder.button(text="🏠 Меню", callback_data=f"menu:{user_id}:main")
    if current_page < total_pages - 1:
        builder.button(text="➡️", callback_data=f"top:{user_id}:{current_page + 1}")
    builder.adjust(3)
    return builder.as_markup()


def transfer_confirm_kb(token: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data=f"tconf:{token}")
    builder.button(text="❌ Отмена", callback_data=f"tcancel:{token}")
    builder.adjust(2)
    return builder.as_markup()


async def build_my_stats(user_id: int, chat_id: int) -> RichText:
    async with pool.acquire() as conn:
        points = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            user_id, chat_id
        )
        if points is None:
            points = await get_join_points(chat_id)

        total = await conn.fetchval("SELECT COUNT(*) FROM users WHERE chat_id = $1", chat_id)
        higher = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE chat_id = $1 AND points > $2",
            chat_id, points
        )

    place = (int(higher) + 1) if higher is not None else 1
    total = int(total) if total is not None else 0

    status = get_point_role(int(points))
    mute_delta, warn_delta = calc_punishment_adjust(int(points))

    b = RichText()
    b.add("📊 ").bold("Моя статистика").add("\n")
    b.add("💠 Баланс | ").bold(points).add("\n")
    b.add("😎 Статус | ").bold(status).add("\n")
    b.add("🏅 Место | ").bold(place).add(" из ").bold(total).add("\n\n")
    b.bold("⏱ Коррекция наказания").add("\n")
    b.add("🔇 Мут | ").bold(fmt_minutes(mute_delta)).add("\n")
    b.add("⚠️ Варн | ").bold(fmt_days(warn_delta)).add("\n")
    return b


def build_help(role: str) -> RichText:
    b = RichText()
    b.add("📖 ").bold("Команды бота").add("\n\n")

    b.bold("👤 Участнику").add("\n")
    b.add("• /моиб | баланс\n")
    b.add("• /топб | топ баллов\n")
    b.add("• /передать | перевод баллов\n")

    if role == "member":
        return b

    b.add("\n").bold("🌐 Админу 1 уровня").add("\n")
    b.add("• /инфо | информация по участнику\n")

    if role == "admin1":
        return b

    b.add("\n").bold("🌐 Админу 2 уровня").add("\n")
    b.add("• /балл | начислить / снять баллы\n")
    b.add("• /баллм | массовое изменение\n")
    b.add("• /стартбаллы | стартовые баллы чата\n")
    b.add("• /админ | выдать админа 1 уровня\n")
    b.add("• /повысить | выдать админа 2 уровня\n")
    b.add("• /разжаловать | снять админку\n")
    b.add("• /бадмины | список админов\n")
    b.add("• +рейтинг | изменить «О рейтинге»\n")
    b.add("• +эмодзи | настройка premium эмодзи\n")

    if role == "owner":
        b.add("\n").bold("👑 Владельцу").add("\n")
        b.add("• Полный доступ в любом чате\n")

    return b


async def send_top_page(message: types.Message, page: int, owner_id: int, edit: bool = False):
    offset = page * ITEMS_PER_PAGE
    async with pool.acquire() as conn:
        total_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE chat_id = $1", message.chat.id)
        total_pages = max(1, (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

        top = await conn.fetch(
            "SELECT user_id, name, points, username FROM users "
            "WHERE chat_id = $1 ORDER BY points DESC, join_seq ASC LIMIT $2 OFFSET $3",
            message.chat.id, ITEMS_PER_PAGE, offset
        )

    if not top:
        b = RichText().add("💠 Список лидеров пока пуст.")
        return await send_rich(message, b, edit=False)

    b = RichText()
    b.add("💠 ").bold("ТОП ЛИДЕРОВ").add(f" ({page + 1}/{total_pages})\n\n")

    for i, row in enumerate(top, 1 + offset):
        uid = int(row["user_id"])
        name = str(row["name"])
        pts = int(row["points"])
        username = row["username"]

        b.add(f"{i}. ")

        if uid == MENTION_IN_TOP_USER_ID:
            b.link(name, f"tg://user?id={uid}")
        else:
            if username:
                b.link(name, f"https://t.me/{username}")
            else:
                b.add(name)

        b.add(" | ").bold(pts).add("\n")

    kb = get_top_keyboard(page, total_pages, owner_id)
    await send_rich(message, b, reply_markup=kb, edit=edit)



@dp.message(Command("start", "bhelp", "бпомощь", "менюб", "menub"))
async def cmd_menu(message: types.Message):
    await update_user_data(
        message.from_user.id,
        message.chat.id,
        message.from_user.first_name,
        message.from_user.username
    )
    b = RichText()
    b.add("💠 ").bold("Меню бота баллов").add("\n")
    b.add("Выбери раздел кнопками ниже.")
    await send_rich(message, b, reply_markup=main_menu_kb(message.from_user.id))


@dp.message(F.text.startswith("+эмодзи"))
async def premium_emoji_cmd(message: types.Message):
    parts = (message.text or "").split()

    is_global = len(parts) >= 2 and parts[1].lower() in ("глоб", "global", "g")

    if is_global:
        if not await has_level(message.from_user.id, message.chat.id, 2) and message.from_user.id != OWNER_ID:
            return
        target_chat_id = 0
        arg_shift = 1
        scope_name = "🌍 Глобальные"

    else:
        if not await has_level(message.from_user.id, message.chat.id, 2) and message.from_user.id != OWNER_ID:
            return
        target_chat_id = message.chat.id
        arg_shift = 0
        scope_name = "🏠 Эмодзи этого чата"


    if len(parts) == 1 or (is_global and len(parts) == 2):
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT emoji_text, custom_emoji_id, enabled FROM chat_emojis WHERE chat_id = $1 ORDER BY emoji_text ASC",
                target_chat_id
            )

        b = RichText()
        b.add("🧩 ").bold(f"{scope_name} — настройки").add("\n\n")

        b.bold("Команды:").add("\n")
        if is_global:
            b.add("• +эмодзи глоб сет «эмодзи» «custom_emoji_id»\n")
            b.add("• +эмодзи глоб вкл «эмодзи»\n")
            b.add("• +эмодзи глоб выкл «эмодзи»\n")
            b.add("• +эмодзи глоб дел «эмодзи»\n\n")
            b.add("Пример: +эмодзи глоб сет 💠 5409123456789012345\n\n")
        else:
            b.add("• +эмодзи сет «эмодзи» «custom_emoji_id»\n")
            b.add("• +эмодзи вкл «эмодзи»\n")
            b.add("• +эмодзи выкл «эмодзи»\n")
            b.add("• +эмодзи дел «эмодзи»\n\n")
            b.add("Пример: +эмодзи сет 💠 5409123456789012345\n\n")

        b.bold("Текущие значения:").add("\n")
        if not rows:
            b.add("— пусто —")
        else:
            for r in rows:
                emj = str(r["emoji_text"])
                cid = r["custom_emoji_id"] or "—"
                en = "✅" if r["enabled"] else "❌"
                b.add(f"• {emj} | {en} | ").code(cid).add("\n")

        return await send_rich(message, b)

    if len(parts) < 3 + arg_shift:
        return await message.reply("❌ Не понял. Напиши просто: +эмодзи (или +эмодзи глоб)")

    action = parts[1 + arg_shift].lower()
    emoji_text = parts[2 + arg_shift]

    if action in ("сет", "set"):
        if len(parts) < 4 + arg_shift:
            if is_global:
                return await message.reply("Используй: +эмодзи глоб сет «эмодзи» «custom_emoji_id»")
            return await message.reply("Используй: +эмодзи сет «эмодзи» «custom_emoji_id»")

        cid = parts[3 + arg_shift].strip()
        await set_chat_emoji(target_chat_id, emoji_text, cid, enabled=True)
        prefix = "🌍 Глобально" if is_global else "🏠 В чате"
        return await message.reply(f"✅ {prefix}: {emoji_text} → {cid}")

    if action in ("вкл", "on"):
        await toggle_chat_emoji(target_chat_id, emoji_text, True)
        prefix = "🌍 Глобально" if is_global else "🏠 В чате"
        return await message.reply(f"✅ {prefix} включено: {emoji_text}")

    if action in ("выкл", "off"):
        await toggle_chat_emoji(target_chat_id, emoji_text, False)
        prefix = "🌍 Глобально" if is_global else "🏠 В чате"
        return await message.reply(f"✅ {prefix} выключено: {emoji_text}")

    if action in ("дел", "del", "удалить", "remove"):
        await delete_chat_emoji(target_chat_id, emoji_text)
        prefix = "🌍 Глобально" if is_global else "🏠 В чате"
        return await message.reply(f"✅ {prefix} удалено: {emoji_text}")

    return await message.reply("❌ Не понял команду. Напиши: +эмодзи (или +эмодзи глоб)")


@dp.message(F.text.startswith("+рейтинг"))
async def edit_rating_cmd(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 2) and message.from_user.id != OWNER_ID:
        return await message.reply("❌ Недостаточно прав. Нужно: админ 2 уровня.")

    new_text = ""
    if message.reply_to_message and message.reply_to_message.text:
        new_text = message.reply_to_message.text.strip()
    else:
        raw = (message.text or "").strip()
        new_text = raw[len("+рейтинг"):].strip()

    if not new_text:
        current = await get_rating_text(message.chat.id)
        b = RichText()
        b.add("💠 ").bold("О рейтинге (текущая версия)").add("\n\n")
        b.add(current).add("\n\n")
        b.add("Чтобы изменить — отправь:\n")
        b.add("• +рейтинг текст\n")
        b.add("или ответь на сообщение с текстом командой +рейтинг")
        return await send_rich(message, b)

    await set_rating_text(message.chat.id, new_text)
    b = RichText().add("✅ ").bold("Текст «О рейтинге» обновлён.")
    await send_rich(message, b)


@dp.callback_query(F.data.startswith("menu:"))
async def menu_handler(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    owner_id = int(parts[1])

    if callback.from_user.id != owner_id:
        return await callback.answer()

    action = parts[2]

    lvl = await get_admin_level(callback.from_user.id, callback.message.chat.id)
    role = get_role_and_lvl(callback.from_user.id, lvl)

    if action == "main":
        b = RichText()
        b.add("💠 ").bold("Меню бота баллов").add("\n")
        b.add("Выбери раздел кнопками ниже.")
        await send_rich(callback.message, b, reply_markup=main_menu_kb(owner_id), edit=True)
        return await callback.answer()

    if action == "help":
        b = build_help(role)
        await send_rich(callback.message, b, reply_markup=main_menu_kb(owner_id), edit=True)
        return await callback.answer()

    if action == "rating":
        txt = await get_rating_text(callback.message.chat.id)
        b = RichText().add(txt)
        await send_rich(callback.message, b, reply_markup=main_menu_kb(owner_id), edit=True)
        return await callback.answer()

    if action == "stats":
        b = await build_my_stats(callback.from_user.id, callback.message.chat.id)
        await send_rich(callback.message, b, reply_markup=main_menu_kb(owner_id), edit=True)
        return await callback.answer()

    if action == "top":
        page = int(parts[3]) if len(parts) > 3 else 0
        await send_top_page(callback.message, page, owner_id=owner_id, edit=True)
        return await callback.answer()

    await callback.answer()


@dp.message(Command("стартбаллы", "joinpoints"))
async def set_join_points_cmd(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 2):
        return

    args = message.text.split()
    if len(args) < 2:
        jp = await get_join_points(message.chat.id)
        b = RichText()
        b.add("Текущие стартовые баллы | ").bold(jp).add("\n")
        b.add("Установить | /стартбаллы 50")
        return await send_rich(message, b)

    try:
        jp = int(args[1])
    except ValueError:
        return await message.reply("Введите число. Используй: /стартбаллы 50")

    jp = max(BALANCE_MIN, min(BALANCE_MAX, jp))

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_settings (chat_id, join_points)
            VALUES ($1, $2)
            ON CONFLICT (chat_id)
            DO UPDATE SET join_points = $2
        """, message.chat.id, jp)

    b = RichText().add("✅ Стартовые баллы установлены на ").bold(jp).add(".")
    await send_rich(message, b)


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

    status = get_point_role(int(points))
    mute_delta, warn_delta = calc_punishment_adjust(int(points))

    b = RichText()
    b.add("💠 ").add(message.from_user.first_name).add("\n")
    b.add("Баланс | ").bold(points).add("\n")
    b.add("Статус | ").bold(status).add("\n\n")
    b.add("🔇 Мут | ").bold(fmt_minutes(mute_delta)).add("\n")
    b.add("⚠️ Варн | ").bold(fmt_days(warn_delta))
    await send_rich(message, b)


@dp.message(Command("инфо", "stats"))
async def check_stats(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 1):
        return

    tid, tname, tuname, err = await resolve_target(message, message.text.split())
    if err == "no_target":
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.")
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

    status = get_point_role(int(points))
    mute_delta, warn_delta = calc_punishment_adjust(int(points))

    b = RichText()
    b.add("📊 ").bold("Информация").add("\n")
    b.add("👤 Пользователь | ").link(tname, f"tg://user?id={tid}").add("\n")
    b.add("💠 Баланс | ").bold(points).add("\n")
    b.add("😎 Статус | ").bold(status).add("\n\n")
    b.bold("⏱ Коррекция наказания по баллам").add("\n")
    b.add("🔇 Мут | ").bold(fmt_minutes(mute_delta)).add("\n")
    b.add("⚠️ Варн | ").bold(fmt_days(warn_delta))
    await send_rich(message, b)


@dp.message(Command("топб", "topb"))
async def show_top_command(message: types.Message):
    args = message.text.split()
    page = 0
    if len(args) >= 2:
        try:
            page = int(args[1]) - 1
        except ValueError:
            page = 0
    if page < 0:
        page = 0
    await send_top_page(message, page, owner_id=message.from_user.id)


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
        return await message.reply("Используй: /передать 30 @username (или ответом: /передать 30)")

    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("Ошибка! Используй: /передать 30 @username")

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
        return await message.reply(f"Минимальный перевод | {TRANSFER_RATE} (получит 1 балл).")

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
            f"❌ Перевод невозможен: будет больше {BALANCE_MAX}.\n"
            f"Сейчас: {target_pts}\n"
            f"Максимум принять: {can}\n"
            f"Ты хотел (получит): {received_raw}"
        )

    actual_received = received_raw
    actual_spent = actual_received * TRANSFER_RATE

    if sender_pts - actual_spent < MIN_POINTS_TO_TRANSFER:
        return await message.reply(
            f"❌ После перевода должно остаться минимум {MIN_POINTS_TO_TRANSFER}.\n"
            f"Сейчас: {sender_pts}\n"
            f"Спишется: {actual_spent}\n"
            f"Останется: {sender_pts - actual_spent}"
        )

    if sender_pts < actual_spent:
        return await message.reply("❌ Недостаточно баллов для перевода.")

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

    b = RichText()
    b.add("💠 ").bold("Подтверждение перевода").add("\n\n")
    b.add("👤 Отправитель | ").link(message.from_user.first_name, f"tg://user?id={message.from_user.id}").add("\n")
    b.add("🎯 Получатель | ").link(tname, f"tg://user?id={tid}").add("\n\n")
    b.add("📉 Спишется | ").bold(actual_spent).add("\n")
    b.add("📈 Получит | ").bold(actual_received).add("\n")
    b.add("🔁 Курс | ").bold(f"{TRANSFER_RATE}:1").add("\n")

    await send_rich(message, b, reply_markup=transfer_confirm_kb(token))


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
            await callback.message.edit_text(f"❌ Перевод невозможен: больше {BALANCE_MAX}.")
            return await callback.answer()

        if sender_pts < actual_spent:
            pending_transfers.pop(token, None)
            await callback.message.edit_text("❌ Перевод невозможен: недостаточно баллов у отправителя.")
            return await callback.answer()

        if sender_pts - actual_spent < MIN_POINTS_TO_TRANSFER:
            pending_transfers.pop(token, None)
            await callback.message.edit_text(f"❌ Перевод невозможен: после перевода минимум {MIN_POINTS_TO_TRANSFER}.")
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

    b = RichText()
    b.add("✅ ").bold("Перевод выполнен!").add("\n")
    b.add("💠 ").link(req["sender_name"], f"tg://user?id={req['sender_id']}").add(" передал ")
    b.link(req["target_name"], f"tg://user?id={req['target_id']}").add(" ")
    b.bold(actual_received).add(" балл(ов)\n")
    b.add("📉 Списано | ").bold(actual_spent).add(f" (курс {TRANSFER_RATE}:1)")

    await send_rich(callback.message, b, edit=True)
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
        return await message.reply("Используй: /балл +10 @username причина (или ответом: /балл +10 причина)")

    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("Ошибка! Пример: /балл -2 @user флуд")

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

    async with pool.acquire() as conn:
        current_pts = await conn.fetchval(
            "SELECT points FROM users WHERE user_id = $1 AND chat_id = $2",
            tid, message.chat.id
        )
        if current_pts is None:
            current_pts = await get_join_points(message.chat.id)

        if amount > 0 and current_pts + amount > BALANCE_MAX:
            return await message.reply(
                f"❌ Нельзя начислить столько: будет превышен лимит {BALANCE_MAX}.\n"
                f"Сейчас: {current_pts}, начисляешь: {amount}, было бы: {current_pts + amount}."
            )

        if amount < 0 and current_pts + amount < BALANCE_MIN:
            return await message.reply(
                f"❌ Нельзя снять столько: баланс не может быть меньше {BALANCE_MIN}.\n"
                f"Сейчас: {current_pts}, снимаешь: {abs(amount)}, было бы: {current_pts + amount}."
            )

        new_pts = current_pts + amount
        await conn.execute(
            "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
            new_pts, tid, message.chat.id
        )

    b = RichText()
    if amount >= 0:
        b.add("⬆️ Администратор ").link(message.from_user.first_name, f"tg://user?id={message.from_user.id}")
        b.add(" начислил ").link(tname, f"tg://user?id={tid}").add(" ")
        b.bold(abs(amount)).add(" баллов.")
    else:
        b.add("⬇️ Администратор ").link(message.from_user.first_name, f"tg://user?id={message.from_user.id}")
        b.add(" снял у ").link(tname, f"tg://user?id={tid}").add(" ")
        b.bold(abs(amount)).add(" баллов.")

    if reason:
        b.add("\n📝 Причина: ").italic(reason)

    await send_rich(message, b)

    chat_title = message.chat.title or str(message.chat.id)
    action = "начислил" if amount >= 0 else "снял"
    sign = "+" if amount >= 0 else "-"

    await log_to_owner(
        "🧾 Лог баллов\n"
        f"🏷 Чат: {chat_title} ({message.chat.id})\n"
        f"👮 Админ: {message.from_user.first_name} ({message.from_user.id})\n"
        f"👤 Участник: {tname} ({tid})\n"
        f"📌 Действие: {action} {sign}{abs(amount)}\n"
        f"💠 Новый баланс: {new_pts}\n"
        f"📝 Причина: {reason if reason else 'не указана'}"
    )


@dp.message(Command("баллм", "ballm"))
async def change_points_mass(message: types.Message):
    if not await has_level(message.from_user.id, message.chat.id, 2):
        return

    args = message.text.split()
    if len(args) < 4:
        return await message.reply("Используй: /баллм -5 @user1 @user2 причина (можно много @username)")

    try:
        amount = int(args[1])
    except ValueError:
        return await message.reply("Ошибка! Пример: /баллм -5 @user1 @user2 флуд")

    mentions = [a for a in args[2:] if a.startswith("@")]
    if not mentions:
        return await message.reply("⚠️ Укажи хотя бы один @username.")

    reason = extract_mass_reason(args)

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
                fail_lines.append(f"• {tname}: нельзя +{amount} (сейчас {current_pts}, было бы > {BALANCE_MAX})")
                continue

            if amount < 0 and current_pts + amount < BALANCE_MIN:
                fail_lines.append(f"• {tname}: нельзя {amount} (сейчас {current_pts}, было бы < {BALANCE_MIN})")
                continue

            new_pts = current_pts + amount
            await conn.execute(
                "UPDATE users SET points = $1 WHERE user_id = $2 AND chat_id = $3",
                new_pts, tid, message.chat.id
            )

            ok_lines.append((tname, tid, current_pts, new_pts))

    if not ok_lines and fail_lines:
        return await message.answer("❌ Никому не удалось изменить баллы.\n\n" + "\n".join(fail_lines))

    sign = "+" if amount >= 0 else "-"
    action_word = "начислил" if amount >= 0 else "снял"

    b = RichText()
    b.add("🧾 ").bold("Массовое изменение баллов").add("\n")
    b.add("👮 Админ: ").link(message.from_user.first_name, f"tg://user?id={message.from_user.id}").add("\n")
    b.add("📌 Действие: ").bold(f"{action_word} {sign}{abs(amount)}").add("\n\n")
    b.bold("✅ Успешно:").add("\n")
    for tname, tid, oldp, newp in ok_lines:
        b.add("• ").link(tname, f"tg://user?id={tid}").add(f": {oldp} → ").bold(newp).add("\n")

    if fail_lines:
        b.add("\n").bold("⚠️ Ошибки:").add("\n")
        for line in fail_lines:
            b.add(line).add("\n")

    if reason:
        b.add("\n📝 Причина: ").italic(reason)

    await send_rich(message, b)


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
        return await message.reply("⚠️ Укажи @username или ответь на сообщение.\nПример: /повысить @user 2")
    if err == "not_found":
        return await message.reply("❌ Пользователь не найден. Пусть он напишет сообщение в любой чат с ботом.")
    if err == "not_in_chat":
        return await message.reply("❌ Этот @username не найден среди участников этого чата.")

    if tid == OWNER_ID:
        return await message.reply("❌ Нельзя менять права владельца.")

    if not await user_exists_in_chat(tid, message.chat.id):
        return await message.reply("❌ Пользователь не найден в базе этого чата.\nПусть он напишет сообщение.")

    await set_admin_level(message.chat.id, tid, level, mode="force")

    b = RichText().add("✅ ").link(name, f"tg://user?id={tid}").add(" теперь ").bold(f"админ {level}").add(" уровня.")
    await send_rich(message, b)


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
        return await message.reply("⚠️ Укажи @username или ответь.\nПример: /админ @user")
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
        b = RichText().add("ℹ️ ").link(name, f"tg://user?id={tid}").add(" уже ").bold("админ 2").add(" уровня.")
        return await send_rich(message, b)

    await set_admin_level(message.chat.id, tid, 1, mode="max")
    b = RichText().add("✅ ").link(name, f"tg://user?id={tid}").add(" теперь ").bold("админ 1").add(" уровня.")
    await send_rich(message, b)


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
        return await message.reply("⚠️ Укажи @username или ответь.\nПример: /разжаловать @user")
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
        return await message.reply("❌ Ты можешь снимать только админа 1 уровня.")

    await remove_admin_level(message.chat.id, tid)
    b = RichText().add("❌ ").link(name, f"tg://user?id={tid}").add(" больше ").bold("не админ").add(".")
    await send_rich(message, b)


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
        return await message.answer("Список админов пуст.", disable_web_page_preview=True)

    b = RichText()
    b.add("🛡 ").bold("Список админов").add("\n\n")
    for i, r in enumerate(rows, 1):
        name = r["name"] or "Без имени"
        username = r["username"]
        level = int(r["level"]) if r["level"] is not None else 1

        b.add(f"{i}. ")
        if username:
            b.link(name, f"https://t.me/{username}")
        else:
            b.link(name, f"tg://user?id={int(r['user_id'])}")
        b.add(" — ").bold(f"{level}").add(" уровень\n")

    await send_rich(message, b)


@dp.message(F.entities)
async def catch_custom_emoji_id(message: types.Message):
    for ent in message.entities:
        if ent.type == "custom_emoji":
            await message.reply(
                f"🆔 custom_emoji_id:\n<code>{ent.custom_emoji_id}</code>",
                parse_mode="HTML"
            )
            return


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