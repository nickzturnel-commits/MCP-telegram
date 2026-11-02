"""Telegram MCP server для Render (Telethon + FastMCP, streamable HTTP).

Переменные окружения на Render:
  TELEGRAM_API_ID        число
  TELEGRAM_API_HASH      строка
  TELEGRAM_SESSION_B64   содержимое session_b64.txt
  MCP_SECRET             длинный случайный секрет (>=24 символов), попадает в путь URL
  TG_READ_ONLY=1         (необязательно) выключает все инструменты записи
"""
import asyncio
import base64
import functools
import io
import json
import os
import pathlib
import time
from datetime import datetime, timezone

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from telethon import TelegramClient, functions, types, utils

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_B64 = os.environ["TELEGRAM_SESSION_B64"]
SECRET = os.environ["MCP_SECRET"].strip("/")
READ_ONLY = os.environ.get("TG_READ_ONLY", "0") == "1"
PORT = int(os.environ.get("PORT", "8000"))

if len(SECRET) < 24:
    raise SystemExit("MCP_SECRET слишком короткий: нужно минимум 24 символа")

# Разворачиваем сессию из base64 во временный файл
SESSION_PATH = "/tmp/tg_render"
pathlib.Path(SESSION_PATH + ".session").write_bytes(base64.b64decode(SESSION_B64))

mcp = FastMCP(
    "telegram",
    host="0.0.0.0",
    port=PORT,
    streamable_http_path=f"/{SECRET}/mcp",
    stateless_http=True,
    log_level="WARNING",  # чтобы секретный путь не светился в логах
)

# ---------------------------------------------------------------- клиент
_client: TelegramClient | None = None
_lock = asyncio.Lock()
_warm = False


async def tg() -> TelegramClient:
    global _client, _warm
    async with _lock:
        if _client is None:
            _client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
        if not _client.is_connected():
            await _client.connect()
        if not await _client.is_user_authorized():
            raise RuntimeError("Сессия не авторизована. Создай её заново через login.py")
        if not _warm:
            await _client.get_dialogs(limit=200)  # кеш сущностей, чтобы работали числовые id
            _warm = True
    return _client


# ---------------------------------------------------------------- хелперы
def peer(chat):
    s = str(chat).strip()
    if s.lower() in ("me", "self"):
        return "me"
    try:
        return int(s)
    except ValueError:
        return s


def j(x) -> str:
    return json.dumps(x, ensure_ascii=False, default=str)


def media_kind(m):
    if not m.media:
        return None
    for k in ("photo", "video", "voice", "audio", "sticker", "gif", "poll", "contact", "geo", "document"):
        if getattr(m, k, None):
            return k
    return type(m.media).__name__


def msg(m) -> dict:
    d = {
        "id": m.id,
        "date": m.date.isoformat() if m.date else None,
        "from_id": m.sender_id,
        "from": utils.get_display_name(m.sender) if m.sender else None,
        "out": m.out,
        "text": m.message or "",
    }
    if m.reply_to:
        d["reply_to"] = m.reply_to.reply_to_msg_id
    if m.fwd_from:
        d["forwarded"] = True
    k = media_kind(m)
    if k:
        d["media"] = k
        if m.file:
            d["file_name"] = m.file.name
            d["size"] = m.file.size
    if m.views is not None:
        d["views"] = m.views
    if m.reactions:
        d["reactions"] = [
            {"emoji": getattr(r.reaction, "emoticon", str(r.reaction)), "count": r.count}
            for r in m.reactions.results
        ]
    if m.edit_date:
        d["edited"] = True
    if m.pinned:
        d["pinned"] = True
    return d


def parse_dt(s: str | None):
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def write_tool(fn):
    """Инструмент записи: блокируется при TG_READ_ONLY=1."""

    @functools.wraps(fn)
    async def wrapper(*a, **k):
        if READ_ONLY:
            return "Режим только чтения (TG_READ_ONLY=1): действие запрещено."
        return await fn(*a, **k)

    return mcp.tool()(wrapper)


FILTERS = {
    "photo": types.InputMessagesFilterPhotos,
    "video": types.InputMessagesFilterVideo,
    "document": types.InputMessagesFilterDocument,
    "url": types.InputMessagesFilterUrl,
    "voice": types.InputMessagesFilterVoice,
    "music": types.InputMessagesFilterMusic,
    "gif": types.InputMessagesFilterGif,
    "round": types.InputMessagesFilterRoundVideo,
    "pinned": types.InputMessagesFilterPinned,
}


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request):
    return PlainTextResponse("ok")


# ================================================================ ЧТЕНИЕ
@mcp.tool()
async def get_me() -> str:
    """Информация о моём аккаунте."""
    c = await tg()
    me = await c.get_me()
    return j({"id": me.id, "name": utils.get_display_name(me), "username": me.username, "phone": me.phone})


@mcp.tool()
async def list_chats(limit: int = 50, archived: bool = False, only_unread: bool = False) -> str:
    """Список диалогов (id, название, тип, непрочитанные). archived=True — архив."""
    c = await tg()
    out = []
    async for d in c.iter_dialogs(limit=None if only_unread else limit, folder=1 if archived else 0):
        if only_unread and not d.unread_count:
            continue
        kind = "channel" if d.is_channel and not d.is_group else "group" if d.is_group else "user"
        out.append({"id": d.id, "name": d.name, "type": kind, "unread": d.unread_count,
                    "pinned": d.pinned, "last": (d.message.message or "")[:80] if d.message else ""})
        if len(out) >= limit:
            break
    return j(out)


@mcp.tool()
async def get_chat_info(chat: str) -> str:
    """Подробности о чате/канале/пользователе: описание, участники, username, телефон."""
    c = await tg()
    ent = await c.get_entity(peer(chat))
    info = {"id": utils.get_peer_id(ent), "type": type(ent).__name__,
            "name": utils.get_display_name(ent), "username": getattr(ent, "username", None)}
    try:
        if isinstance(ent, types.Channel):
            full = (await c(functions.channels.GetFullChannelRequest(ent))).full_chat
            info.update(about=full.about, participants=full.participants_count)
        elif isinstance(ent, types.Chat):
            full = (await c(functions.messages.GetFullChatRequest(ent.id))).full_chat
            info.update(about=full.about)
        elif isinstance(ent, types.User):
            full = (await c(functions.users.GetFullUserRequest(ent))).full_user
            info.update(about=full.about, phone=ent.phone, bot=ent.bot)
    except Exception as e:  # noqa: BLE001
        info["note"] = f"не удалось получить полную информацию: {e}"
    return j(info)


@mcp.tool()
async def read_messages(chat: str, limit: int = 30, offset_id: int = 0, min_id: int = 0,
                        before_iso: str | None = None) -> str:
    """Последние сообщения чата (в хронологическом порядке).
    offset_id — брать сообщения старше этого id; min_id — только новее; before_iso — старше даты (ISO)."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), limit=limit, offset_id=offset_id,
                                             min_id=min_id, offset_date=parse_dt(before_iso))]
    return j([msg(m) for m in reversed(msgs)])


@mcp.tool()
async def get_message(chat: str, message_id: int) -> str:
    """Одно сообщение по id."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    return j(msg(m)) if m else "Сообщение не найдено."


@mcp.tool()
async def get_thread_replies(chat: str, message_id: int, limit: int = 50) -> str:
    """Ответы/комментарии к сообщению (ветка, комментарии под постом канала)."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), reply_to=message_id, limit=limit)]
    return j([msg(m) for m in reversed(msgs)])


@mcp.tool()
async def search_messages(query: str = "", chat: str | None = None, limit: int = 20,
                          from_user: str | None = None, media_filter: str | None = None) -> str:
    """Поиск по сообщениям. chat=None — глобальный поиск по всем чатам.
    media_filter: photo, video, document, url, voice, music, gif, round, pinned."""
    c = await tg()
    kw = {}
    if media_filter:
        kw["filter"] = FILTERS[media_filter]
    if from_user:
        kw["from_user"] = peer(from_user)
    msgs = [m async for m in c.iter_messages(peer(chat) if chat else None, search=query, limit=limit, **kw)]
    res = []
    for m in msgs:
        d = msg(m)
        d["chat_id"] = m.chat_id
        res.append(d)
    return j(res)


@mcp.tool()
async def get_participants(chat: str, limit: int = 100, query: str = "") -> str:
    """Участники группы/канала (query — фильтр по имени)."""
    c = await tg()
    out = []
    async for u in c.iter_participants(peer(chat), limit=limit, search=query):
        out.append({"id": u.id, "name": utils.get_display_name(u), "username": u.username, "bot": u.bot})
    return j(out)


@mcp.tool()
async def list_contacts() -> str:
    """Список контактов."""
    c = await tg()
    r = await c(functions.contacts.GetContactsRequest(hash=0))
    return j([{"id": u.id, "name": utils.get_display_name(u), "username": u.username, "phone": u.phone}
              for u in r.users])


@mcp.tool()
async def download_media(chat: str, message_id: int, max_mb: int = 5) -> str:
    """Скачать вложение сообщения (возвращает base64, ограничено max_mb)."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    if not m or not m.media:
        return "В сообщении нет вложения."
    if m.file and m.file.size and m.file.size > max_mb * 1024 * 1024:
        return f"Файл слишком большой ({m.file.size} байт), лимит {max_mb} МБ."
    data = await c.download_media(m, file=bytes)
    return j({"file_name": m.file.name if m.file else None, "mime": m.file.mime_type if m.file else None,
              "size": len(data), "base64": base64.b64encode(data).decode()})


@mcp.tool()
async def get_active_sessions() -> str:
    """Активные сессии аккаунта (для проверки безопасности)."""
    c = await tg()
    r = await c(functions.account.GetAuthorizationsRequest())
    return j([{"device": a.device_model, "app": a.app_name, "ip": a.ip, "country": a.country,
               "current": a.current, "last_active": a.date_active} for a in r.authorizations])


# ================================================================ ЗАПИСЬ
@write_tool
async def send_message(chat: str, text: str, reply_to: int | None = None, parse_mode: str | None = None,
                       silent: bool = False, schedule_iso: str | None = None,
                       link_preview: bool = True) -> str:
    """Отправить сообщение. parse_mode: md | html | None. schedule_iso — отложенная отправка (ISO, UTC по умолчанию)."""
    c = await tg()
    m = await c.send_message(peer(chat), text, reply_to=reply_to, parse_mode=parse_mode, silent=silent,
                             schedule=parse_dt(schedule_iso), link_preview=link_preview)
    return f"Отправлено, id={m.id}"


@write_tool
async def edit_message(chat: str, message_id: int, text: str, parse_mode: str | None = None) -> str:
    """Изменить текст своего сообщения."""
    c = await tg()
    await c.edit_message(peer(chat), message_id, text, parse_mode=parse_mode)
    return "Изменено."


@write_tool
async def delete_messages(chat: str, message_ids: list[int], for_everyone: bool = True) -> str:
    """Удалить сообщения (for_everyone=True — у всех, если разрешено)."""
    c = await tg()
    await c.delete_messages(peer(chat), message_ids, revoke=for_everyone)
    return f"Удалено сообщений: {len(message_ids)}"


@write_tool
async def forward_messages(from_chat: str, to_chat: str, message_ids: list[int]) -> str:
    """Переслать сообщения."""
    c = await tg()
    await c.forward_messages(peer(to_chat), message_ids, from_peer=peer(from_chat))
    return "Переслано."


@write_tool
async def send_file(chat: str, file_b64: str | None = None, file_url: str | None = None,
                    filename: str | None = None, caption: str = "", as_document: bool = False,
                    voice_note: bool = False, reply_to: int | None = None) -> str:
    """Отправить файл/фото/голосовое: из base64 (file_b64 + filename) или по ссылке (file_url)."""
    c = await tg()
    if file_b64:
        f = io.BytesIO(base64.b64decode(file_b64))
        f.name = filename or "file"
    elif file_url:
        f = file_url
    else:
        return "Нужен file_b64 или file_url."
    m = await c.send_file(peer(chat), f, caption=caption, force_document=as_document,
                          voice_note=voice_note, reply_to=reply_to)
    return f"Файл отправлен, id={m.id}"


@write_tool
async def pin_message(chat: str, message_id: int, notify: bool = False) -> str:
    """Закрепить сообщение."""
    c = await tg()
    await c.pin_message(peer(chat), message_id, notify=notify)
    return "Закреплено."


@write_tool
async def unpin_message(chat: str, message_id: int | None = None) -> str:
    """Открепить сообщение (без message_id — открепить все)."""
    c = await tg()
    await c.unpin_message(peer(chat), message_id)
    return "Откреплено."


@write_tool
async def react(chat: str, message_id: int, emoji: str = "👍") -> str:
    """Поставить реакцию (пустой emoji — убрать реакцию)."""
    c = await tg()
    inp = await c.get_input_entity(peer(chat))
    await c(functions.messages.SendReactionRequest(
        peer=inp, msg_id=message_id, reaction=[types.ReactionEmoji(emoticon=emoji)] if emoji else []))
    return "Готово."


@write_tool
async def mark_read(chat: str) -> str:
    """Пометить чат прочитанным."""
    c = await tg()
    await c.send_read_acknowledge(peer(chat))
    return "Прочитано."


@write_tool
async def join_chat(target: str) -> str:
    """Вступить в чат/канал: @username, ссылка t.me/..., или инвайт-ссылка (t.me/+xxxx, joinchat/xxxx)."""
    c = await tg()
    link = target.strip()
    if "joinchat/" in link or "t.me/+" in link or link.startswith("+"):
        h = link.split("joinchat/")[-1] if "joinchat/" in link else link.split("+")[-1]
        await c(functions.messages.ImportChatInviteRequest(h.split("?")[0].strip("/")))
    else:
        await c(functions.channels.JoinChannelRequest(await c.get_entity(link)))
    return "Вступил."


@write_tool
async def delete_dialog(chat: str) -> str:
    """Покинуть группу/канал или удалить личный диалог (история у себя)."""
    c = await tg()
    await c.delete_dialog(peer(chat))
    return "Готово."


@write_tool
async def create_group(title: str, users: list[str]) -> str:
    """Создать обычную группу с указанными пользователями (@username или id)."""
    c = await tg()
    inputs = [await c.get_input_entity(peer(u)) for u in users]
    await c(functions.messages.CreateChatRequest(users=inputs, title=title))
    return "Группа создана."


@write_tool
async def create_channel(title: str, about: str = "", megagroup: bool = False) -> str:
    """Создать канал (megagroup=True — супергруппа)."""
    c = await tg()
    await c(functions.channels.CreateChannelRequest(title=title, about=about, megagroup=megagroup,
                                                    broadcast=not megagroup))
    return "Создано."


@write_tool
async def rename_chat(chat: str, title: str) -> str:
    """Изменить название группы/канала."""
    c = await tg()
    ent = await c.get_entity(peer(chat))
    if isinstance(ent, types.Channel):
        await c(functions.channels.EditTitleRequest(ent, title))
    else:
        await c(functions.messages.EditChatTitleRequest(ent.id, title))
    return "Переименовано."


@write_tool
async def set_chat_about(chat: str, about: str) -> str:
    """Изменить описание группы/канала."""
    c = await tg()
    await c(functions.messages.EditChatAboutRequest(peer=await c.get_input_entity(peer(chat)), about=about))
    return "Описание изменено."


@write_tool
async def invite_users(chat: str, users: list[str]) -> str:
    """Добавить пользователей в группу/канал."""
    c = await tg()
    ent = await c.get_entity(peer(chat))
    inputs = [await c.get_input_entity(peer(u)) for u in users]
    if isinstance(ent, types.Channel):
        await c(functions.channels.InviteToChannelRequest(ent, inputs))
    else:
        for u in inputs:
            await c(functions.messages.AddChatUserRequest(ent.id, u, fwd_limit=50))
    return "Добавлены."


@write_tool
async def kick_user(chat: str, user: str) -> str:
    """Исключить пользователя (может вернуться)."""
    c = await tg()
    await c.kick_participant(peer(chat), peer(user))
    return "Исключён."


@write_tool
async def ban_user(chat: str, user: str, ban: bool = True) -> str:
    """Забанить (ban=True) или разбанить (ban=False) пользователя в группе/канале."""
    c = await tg()
    await c.edit_permissions(peer(chat), peer(user), view_messages=not ban)
    return "Забанен." if ban else "Разбанен."


@write_tool
async def set_admin(chat: str, user: str, is_admin: bool = True, title: str | None = None) -> str:
    """Назначить/снять администратора."""
    c = await tg()
    await c.edit_admin(peer(chat), peer(user), is_admin=is_admin, title=title)
    return "Готово."


@write_tool
async def add_contact(phone: str, first_name: str, last_name: str = "") -> str:
    """Добавить контакт по номеру телефона."""
    c = await tg()
    await c(functions.contacts.ImportContactsRequest(
        [types.InputPhoneContact(client_id=0, phone=phone, first_name=first_name, last_name=last_name)]))
    return "Контакт добавлен."


@write_tool
async def delete_contact(user: str) -> str:
    """Удалить контакт."""
    c = await tg()
    await c(functions.contacts.DeleteContactsRequest(id=[await c.get_input_entity(peer(user))]))
    return "Контакт удалён."


@write_tool
async def block_user(user: str, block: bool = True) -> str:
    """Заблокировать (block=True) или разблокировать пользователя."""
    c = await tg()
    inp = await c.get_input_entity(peer(user))
    req = functions.contacts.BlockRequest if block else functions.contacts.UnblockRequest
    await c(req(id=inp))
    return "Заблокирован." if block else "Разблокирован."


@write_tool
async def archive_chat(chat: str, archive: bool = True) -> str:
    """Архивировать / вернуть из архива."""
    c = await tg()
    await c.edit_folder(peer(chat), 1 if archive else 0)
    return "Готово."


@write_tool
async def mute_chat(chat: str, hours: int = -1) -> str:
    """Выключить уведомления: hours=-1 навсегда, N — на N часов, 0 — включить обратно."""
    c = await tg()
    inp = await c.get_input_entity(peer(chat))
    until = 2**31 - 1 if hours < 0 else (0 if hours == 0 else int(time.time() + hours * 3600))
    await c(functions.account.UpdateNotifySettingsRequest(
        peer=types.InputNotifyPeer(inp), settings=types.InputPeerNotifySettings(mute_until=until)))
    return "Готово."


@write_tool
async def update_profile(first_name: str | None = None, last_name: str | None = None,
                         about: str | None = None) -> str:
    """Изменить имя, фамилию, «о себе» в профиле."""
    c = await tg()
    await c(functions.account.UpdateProfileRequest(first_name=first_name, last_name=last_name, about=about))
    return "Профиль обновлён."


@write_tool
async def set_username(username: str) -> str:
    """Сменить свой @username (пустая строка — убрать)."""
    c = await tg()
    await c(functions.account.UpdateUsernameRequest(username=username.lstrip("@")))
    return "Username обновлён."


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
