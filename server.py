"""Telegram MCP server для Render (Telethon + FastMCP, streamable HTTP).

Переменные окружения на Render:
  TELEGRAM_API_ID        число
  TELEGRAM_API_HASH      строка
  TELEGRAM_SESSION_B64   содержимое session_b64.txt
  MCP_SECRET             длинный случайный секрет (>=24 символов), попадает в путь URL
  TG_READ_ONLY=1         (необязательно) выключает все инструменты записи
  TG_STORE_DIR           (необязательно) папка файлового хранилища, по умолчанию /tmp/tg_store
  TG_STORE_MAX_MB        (необязательно) лимит хранилища, по умолчанию 1500
  TG_STORE_TTL_HOURS     (необязательно) через сколько часов файлы удаляются, по умолчанию 24
  PUBLIC_URL             (необязательно) публичный адрес сервиса; Render сам задаёт RENDER_EXTERNAL_URL

HTTP-эндпоинты (все под секретом):
  GET  /<secret>/upload              страница загрузки файлов (можно указать чат, тогда файл сразу уйдёт в Telegram)
  POST /<secret>/upload              multipart: file (можно несколько), chat, caption, as_document, voice_note
  PUT  /<secret>/put/<имя файла>     «сырая» загрузка (curl -T file ...), параметры ?chat=&caption=&as_document=1
  GET  /<secret>/files/<id>/<имя>    скачать файл из хранилища
  GET  /health
"""
import asyncio
import base64
import functools
import glob
import io
import ipaddress
import json
import mimetypes
import os
import pathlib
import re
import secrets
import shutil
import socket
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, unquote, urlparse

import httpx
from mcp.server.fastmcp import FastMCP, Image
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from telethon import TelegramClient, errors, functions, types, utils

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_B64 = os.environ["TELEGRAM_SESSION_B64"]
SECRET = os.environ["MCP_SECRET"].strip("/")
READ_ONLY = os.environ.get("TG_READ_ONLY", "0") == "1"
PORT = int(os.environ.get("PORT", "8000"))
PUBLIC_URL = (os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")

STORE = pathlib.Path(os.environ.get("TG_STORE_DIR", "/tmp/tg_store"))
STORE_MAX = int(os.environ.get("TG_STORE_MAX_MB", "1500")) * 1024 * 1024
STORE_TTL = int(float(os.environ.get("TG_STORE_TTL_HOURS", "24")) * 3600)
PART = STORE / "_partial"
PART.mkdir(parents=True, exist_ok=True)

if len(SECRET) < 24:
    raise SystemExit("MCP_SECRET слишком короткий: нужно минимум 24 символа")
if "{" in SECRET or "}" in SECRET:
    raise SystemExit("MCP_SECRET не должен содержать фигурные скобки")

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


def _txt(x):
    """В новых слоях Telegram многие тексты — TextWithEntities."""
    return getattr(x, "text", x)


def media_kind(m):
    if not m.media:
        return None
    for k in ("photo", "video", "voice", "audio", "sticker", "gif", "poll", "contact", "geo", "document"):
        if getattr(m, k, None):
            return k
    return type(m.media).__name__


def _btns(m):
    """Кнопки сообщения: [[{i, j, text, kind, url}, ...], ...]"""
    try:
        rows = m.buttons
    except Exception:  # noqa: BLE001
        return None
    if not rows:
        return None
    return [[{"i": i, "j": k, "text": b.text, "kind": type(b.button).__name__.replace("KeyboardButton", "") or "Text",
              "url": getattr(b.button, "url", None)} for k, b in enumerate(row)] for i, row in enumerate(rows)]


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
    if m.grouped_id:
        d["album"] = m.grouped_id
    if m.views is not None:
        d["views"] = m.views
    if m.forwards:
        d["forwards"] = m.forwards
    if m.reactions:
        d["reactions"] = [
            {"emoji": getattr(r.reaction, "emoticon", str(r.reaction)), "count": r.count}
            for r in m.reactions.results
        ]
    if m.edit_date:
        d["edited"] = True
    if m.pinned:
        d["pinned"] = True
    b = _btns(m)
    if b:
        d["buttons"] = b
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
    "photo_video": types.InputMessagesFilterPhotoVideo,
    "document": types.InputMessagesFilterDocument,
    "url": types.InputMessagesFilterUrl,
    "voice": types.InputMessagesFilterVoice,
    "music": types.InputMessagesFilterMusic,
    "gif": types.InputMessagesFilterGif,
    "round": types.InputMessagesFilterRoundVideo,
    "pinned": types.InputMessagesFilterPinned,
    "mentions": types.InputMessagesFilterMyMentions,
    "geo": types.InputMessagesFilterGeo,
    "contact": types.InputMessagesFilterContacts,
}


def dialog_kind(d) -> str:
    if d.is_user:
        return "bot" if getattr(d.entity, "bot", False) else "user"
    if d.is_channel and not d.is_group:
        return "channel"
    return "group"


def _muted(d) -> bool:
    try:
        mu = d.dialog.notify_settings.mute_until
    except AttributeError:
        return False
    if not mu:
        return False
    if isinstance(mu, datetime):
        return mu > datetime.now(timezone.utc)
    return int(mu) > time.time()


def pk(p):
    try:
        return utils.get_peer_id(p)
    except Exception:  # noqa: BLE001
        return repr(p)


async def retry_flood(fn, *a, **k):
    for _ in range(3):
        try:
            return await fn(*a, **k)
        except errors.FloodWaitError as e:
            if e.seconds > 90:
                raise
            await asyncio.sleep(e.seconds + 1)
    return await fn(*a, **k)


async def _call_variants(c, builders):
    """Пробует несколько вариантов запроса (под разные слои API Telegram)."""
    err = None
    for b in builders:
        try:
            return await c(b())
        except errors.RPCError:
            raise
        except Exception as e:  # noqa: BLE001
            err = e
    raise err


# ---------------------------------------------------------------- фоновые задачи
JOBS: dict[str, dict] = {}


def spawn(kind: str, factory, **meta) -> str:
    """Запускает долгую операцию в фоне. factory(job) -> результат."""
    jid = secrets.token_hex(4)
    job = {"id": jid, "kind": kind, "status": "running", "started": datetime.now(timezone.utc).isoformat(),
           "progress": "", "result": None, "error": None, **meta}
    JOBS[jid] = job

    async def runner():
        try:
            job["result"] = await factory(job)
            job["status"] = "done"
        except asyncio.CancelledError:
            job["status"] = "cancelled"
            raise
        except Exception as e:  # noqa: BLE001
            job["status"] = "error"
            job["error"] = f"{type(e).__name__}: {e}"
        finally:
            job["finished"] = datetime.now(timezone.utc).isoformat()

    job["_task"] = asyncio.create_task(runner())
    for old in list(JOBS)[:-40]:  # храним последние 40
        JOBS.pop(old, None)
    return jid


def _job_view(job):
    return {k: v for k, v in job.items() if not k.startswith("_")}


# ---------------------------------------------------------------- файловое хранилище
def _safe_name(name: str | None) -> str:
    name = os.path.basename((name or "file").replace("\\", "/")).replace("\x00", "")
    name = re.sub(r"[^\w.\- ()\[\]]+", "_", name, flags=re.UNICODE).strip(" .")
    return (name or "file")[:120]


def _new_path(name: str | None):
    fid = secrets.token_hex(5)
    return fid, STORE / f"{fid}__{_safe_name(name)}"


def _find(fid: str):
    if not re.fullmatch(r"[0-9a-f]{10}", fid or ""):
        return None
    hits = list(STORE.glob(f"{glob.escape(fid)}__*"))
    return hits[0] if hits else None


def _store_files():
    return [p for p in STORE.iterdir() if p.is_file() and "__" in p.name]


def _store_used() -> int:
    total = 0
    for d in (STORE, PART):
        total += sum(p.stat().st_size for p in d.iterdir() if p.is_file())
    return total


def _gc(need: int = 0):
    """Удаляет протухшие файлы; при нехватке места — самые старые."""
    now = time.time()
    for d in (STORE, PART):
        for p in list(d.iterdir()):
            if p == PART:
                continue
            try:
                if now - p.stat().st_mtime > STORE_TTL:
                    shutil.rmtree(p, ignore_errors=True) if p.is_dir() else p.unlink(missing_ok=True)
            except OSError:
                pass
    if need:
        files = sorted(_store_files(), key=lambda p: p.stat().st_mtime)
        while files and _store_used() + need > STORE_MAX:
            files.pop(0).unlink(missing_ok=True)


def _url(p: pathlib.Path) -> str:
    fid, name = p.name.split("__", 1)
    return f"{PUBLIC_URL}/{SECRET}/files/{fid}/{quote(name)}"


def _file_info(p: pathlib.Path) -> dict:
    fid, name = p.name.split("__", 1)
    st = p.stat()
    return {"id": fid, "name": name, "size": st.st_size,
            "age_min": int((time.time() - st.st_mtime) / 60), "url": _url(p)}


async def _receive(chunks, name: str | None) -> pathlib.Path:
    """Сохраняет поток байт в хранилище (без загрузки целиком в память)."""
    _gc()
    fid, p = _new_path(name)
    size = 0
    try:
        with open(p, "wb") as out:
            async for chunk in chunks:
                size += len(chunk)
                if size > STORE_MAX:
                    raise ValueError(f"файл больше лимита хранилища ({STORE_MAX // 1048576} МБ)")
                out.write(chunk)
    except Exception:
        p.unlink(missing_ok=True)
        raise
    _gc(need=0)
    return p


async def _iter_upload(uf):
    while True:
        b = await uf.read(1024 * 1024)
        if not b:
            break
        yield b


def _check_url(url: str):
    u = urlparse(url)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise ValueError("нужен http(s) URL")
    for ai in socket.getaddrinfo(u.hostname, None):
        ip = ipaddress.ip_address(ai[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            raise ValueError("адрес во внутренней сети запрещён")


async def _fetch_to_store(url: str, name: str | None = None) -> pathlib.Path:
    async with httpx.AsyncClient(timeout=httpx.Timeout(60, read=180)) as hc:
        for _ in range(6):
            await asyncio.to_thread(_check_url, url)
            async with hc.stream("GET", url, headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.is_redirect and r.next_request is not None:
                    url = str(r.next_request.url)
                    continue
                r.raise_for_status()
                fname = name
                if not fname:
                    cd = r.headers.get("content-disposition", "")
                    mt = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", cd)
                    fname = unquote(mt.group(1)) if mt else unquote(urlparse(url).path.rsplit("/", 1)[-1])
                if not fname:
                    fname = "download"
                if "." not in fname:
                    ext = mimetypes.guess_extension((r.headers.get("content-type") or "").split(";")[0].strip())
                    fname += ext or ""
                return await _receive(r.aiter_bytes(1024 * 1024), fname)
    raise ValueError("слишком много редиректов")


async def _send_stored(c, chat, p: pathlib.Path, caption="", as_document=False, voice_note=False,
                       reply_to=None, progress=None):
    name = p.name.split("__", 1)[1]
    fh = await c.upload_file(str(p), file_name=name, progress_callback=progress)
    return await c.send_file(peer(chat), fh, caption=caption, force_document=as_document,
                             voice_note=voice_note, reply_to=reply_to, supports_streaming=not as_document)


async def _tg_to_store(c, m, progress=None) -> pathlib.Path:
    f = m.file
    name = (f.name if f and f.name else None) or f"{m.id}{(f.ext if f else '') or ''}"
    _gc(need=(f.size or 0) if f else 0)
    _, p = _new_path(name)
    got = await c.download_media(m, file=str(p), progress_callback=progress)
    return pathlib.Path(got)


async def _auto_send(chat, p, caption, as_doc, voice, item):
    if READ_ONLY:
        item["send_error"] = "режим только чтения"
        return
    try:
        c = await tg()
        m = await _send_stored(c, chat, p, caption, as_doc, voice)
        item["sent_message_id"] = m.id
    except Exception as e:  # noqa: BLE001
        item["send_error"] = f"{type(e).__name__}: {e}"


def _truthy(v) -> bool:
    return str(v or "").lower() in ("1", "true", "on", "yes")


UPLOAD_HTML = """<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Загрузка файлов</title>
<body style="font-family:system-ui;max-width:560px;margin:24px auto;padding:0 16px">
<h3>Загрузка на сервер</h3>
<form method="post" action="__ACTION__" enctype="multipart/form-data">
<p><input type="file" name="file" multiple required></p>
<p>Отправить сразу в чат (необязательно):<br><input name="chat" placeholder="@username, id или me" style="width:100%"></p>
<p>Подпись:<br><input name="caption" style="width:100%"></p>
<p><label><input type="checkbox" name="as_document" value="1"> как документ (без сжатия)</label></p>
<p><button type="submit">Загрузить</button></p>
</form></body>"""


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request):
    return PlainTextResponse("ok")


@mcp.custom_route(f"/{SECRET}/upload", methods=["GET", "POST"])
async def upload_endpoint(request: Request):
    if request.method == "GET":
        return HTMLResponse(UPLOAD_HTML.replace("__ACTION__", f"/{SECRET}/upload"))
    form = await request.form()
    chat = (form.get("chat") or "").strip() or None
    caption = form.get("caption") or ""
    as_doc, voice = _truthy(form.get("as_document")), _truthy(form.get("voice_note"))
    out = []
    for uf in form.getlist("file"):
        if not getattr(uf, "filename", None):
            continue
        try:
            p = await _receive(_iter_upload(uf), uf.filename)
        except Exception as e:  # noqa: BLE001
            out.append({"name": uf.filename, "error": str(e)})
            continue
        item = _file_info(p)
        if chat:
            await _auto_send(chat, p, caption, as_doc, voice, item)
        out.append(item)
    return JSONResponse({"files": out})


@mcp.custom_route(f"/{SECRET}/put/{{name:path}}", methods=["PUT", "POST"])
async def put_endpoint(request: Request):
    q = request.query_params
    try:
        p = await _receive(request.stream(), request.path_params["name"])
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"error": str(e)}, status_code=400)
    item = _file_info(p)
    if q.get("chat"):
        await _auto_send(q["chat"], p, q.get("caption", ""), _truthy(q.get("as_document")),
                         _truthy(q.get("voice_note")), item)
    return JSONResponse(item)


@mcp.custom_route(f"/{SECRET}/files/{{fid}}/{{name:path}}", methods=["GET"])
async def file_download(request: Request):
    p = _find(request.path_params["fid"])
    if not p:
        return PlainTextResponse("not found", status_code=404)
    return FileResponse(p, filename=p.name.split("__", 1)[1])


# ================================================================ ЧТЕНИЕ
@mcp.tool()
async def get_me() -> str:
    """Информация о моём аккаунте."""
    c = await tg()
    me = await c.get_me()
    return j({"id": me.id, "name": utils.get_display_name(me), "username": me.username, "phone": me.phone})


@mcp.tool()
async def list_chats(limit: int = 50, archived: bool = False, only_unread: bool = False,
                     kind: str | None = None, hide_muted: bool = False) -> str:
    """Список диалогов (id, название, тип, непрочитанные). archived=True — архив.
    kind: user, bot, group, channel (можно несколько через запятую). hide_muted — скрыть чаты с выключенным звуком."""
    c = await tg()
    kinds = {k.strip() for k in kind.split(",")} if kind else None
    out = []
    async for d in c.iter_dialogs(limit=None if (only_unread or kinds or hide_muted) else limit,
                                  folder=1 if archived else 0):
        if only_unread and not d.unread_count:
            continue
        k = dialog_kind(d)
        if kinds and k not in kinds:
            continue
        if hide_muted and _muted(d):
            continue
        out.append({"id": d.id, "name": d.name, "type": k, "unread": d.unread_count,
                    "pinned": d.pinned, "muted": _muted(d),
                    "last": (d.message.message or "")[:80] if d.message else ""})
        if len(out) >= limit:
            break
    return j(out)


@mcp.tool()
async def unread_overview(top: int = 15, include_muted: bool = False) -> str:
    """Сводка по непрочитанному: сколько чатов и сообщений по типам, плюс топ чатов. Без звука по умолчанию не считаются."""
    c = await tg()
    by_kind = {"user": [0, 0], "bot": [0, 0], "group": [0, 0], "channel": [0, 0]}
    rows = []
    async for d in c.iter_dialogs(limit=None):
        if not d.unread_count or (not include_muted and _muted(d)):
            continue
        k = dialog_kind(d)
        by_kind[k][0] += 1
        by_kind[k][1] += d.unread_count
        rows.append({"id": d.id, "name": d.name, "type": k, "unread": d.unread_count,
                     "last": (d.message.message or "")[:80] if d.message else ""})
    rows.sort(key=lambda r: -r["unread"])
    return j({"by_type": {k: {"chats": v[0], "messages": v[1]} for k, v in by_kind.items()}, "top": rows[:top]})


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
                        before_iso: str | None = None, topic_id: int | None = None) -> str:
    """Последние сообщения чата (в хронологическом порядке).
    offset_id — брать сообщения старше этого id; min_id — только новее; before_iso — старше даты (ISO).
    topic_id — читать конкретную тему форума."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), limit=limit, offset_id=offset_id, min_id=min_id,
                                             offset_date=parse_dt(before_iso), reply_to=topic_id)]
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
    media_filter: photo, video, photo_video, document, url, voice, music, gif, round, pinned, mentions, geo, contact."""
    c = await tg()
    kw = {}
    if media_filter:
        if media_filter not in FILTERS:
            return f"Неизвестный фильтр. Доступно: {', '.join(FILTERS)}"
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
async def search_public(query: str, limit: int = 10) -> str:
    """Поиск публичных каналов, групп, ботов и людей по названию или @username (как глобальный поиск в Telegram)."""
    c = await tg()
    r = await c(functions.contacts.SearchRequest(q=query, limit=limit))
    out = []
    for e in list(r.users) + list(r.chats):
        out.append({"id": utils.get_peer_id(e), "name": utils.get_display_name(e),
                    "username": getattr(e, "username", None), "type": type(e).__name__,
                    "bot": getattr(e, "bot", None)})
    return j(out)


@mcp.tool()
async def get_participants(chat: str, limit: int = 100, query: str = "") -> str:
    """Участники группы/канала (query — фильтр по имени)."""
    c = await tg()
    out = []
    async for u in c.iter_participants(peer(chat), limit=limit, search=query):
        out.append({"id": u.id, "name": utils.get_display_name(u), "username": u.username, "bot": u.bot})
    return j(out)


@mcp.tool()
async def get_common_chats(user: str) -> str:
    """Общие чаты с пользователем."""
    c = await tg()
    r = await c(functions.messages.GetCommonChatsRequest(
        user_id=await c.get_input_entity(peer(user)), max_id=0, limit=100))
    return j([{"id": utils.get_peer_id(x), "name": utils.get_display_name(x)} for x in r.chats])


@mcp.tool()
async def list_contacts() -> str:
    """Список контактов."""
    c = await tg()
    r = await c(functions.contacts.GetContactsRequest(hash=0))
    return j([{"id": u.id, "name": utils.get_display_name(u), "username": u.username, "phone": u.phone}
              for u in r.users])


@mcp.tool()
async def message_link(chat: str, message_id: int) -> str:
    """Ссылка t.me на сообщение (только каналы и супергруппы)."""
    c = await tg()
    ent = await c.get_entity(peer(chat))
    if getattr(ent, "username", None):
        return f"https://t.me/{ent.username}/{message_id}"
    if isinstance(ent, types.Channel):
        return f"https://t.me/c/{ent.id}/{message_id}"
    return "Ссылки на сообщения есть только у каналов и супергрупп."


@mcp.tool()
async def download_media(chat: str, message_id: int, max_mb: int = 20) -> str:
    """Скачать вложение сообщения (возвращает base64, ограничено max_mb).
    Для больших файлов используй file_from_chat и file_read_chunk или ссылку."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    if not m or not m.media:
        return "В сообщении нет вложения."
    if m.file and m.file.size and m.file.size > max_mb * 1024 * 1024:
        return f"Файл слишком большой ({m.file.size} байт), лимит {max_mb} МБ. Используй file_from_chat."
    data = await c.download_media(m, file=bytes)
    return j({"file_name": m.file.name if m.file else None, "mime": m.file.mime_type if m.file else None,
              "size": len(data), "base64": base64.b64encode(data).decode()})


@mcp.tool(structured_output=False)
async def view_media(chat: str, message_id: int, max_mb: int = 8):
    """Посмотреть фото/картинку (или превью видео) из сообщения: возвращает изображение."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    if not m or not m.media:
        return "В сообщении нет вложения."
    if m.file and m.file.size and m.file.size > max_mb * 1024 * 1024 and not m.photo:
        data = await c.download_media(m, file=bytes, thumb=-1)
    elif m.photo or (m.file and (m.file.mime_type or "").startswith("image/")):
        data = await c.download_media(m, file=bytes)
    else:
        data = await c.download_media(m, file=bytes, thumb=-1)
    if not data:
        return "Не удалось получить изображение (видео, стикеры-анимации и голосовые не поддерживаются)."
    mime = (m.file.mime_type if m.file else None) or "image/jpeg"
    fmt = mime.split("/")[-1] if mime.startswith("image/") else "jpeg"
    return Image(data=data, format=fmt if m.photo is None else "jpeg")


@mcp.tool()
async def transcribe_voice(chat: str, message_id: int) -> str:
    """Расшифровать голосовое/кружок в текст (нужен Telegram Premium или остаток бесплатных попыток)."""
    c = await tg()
    inp = await c.get_input_entity(peer(chat))
    r = None
    for _ in range(6):
        r = await c(functions.messages.TranscribeAudioRequest(peer=inp, msg_id=message_id))
        if not getattr(r, "pending", False):
            break
        await asyncio.sleep(2)
    return r.text if r and r.text else "Расшифровка ещё не готова, повтори позже."


@mcp.tool()
async def get_active_sessions() -> str:
    """Активные сессии аккаунта (для проверки безопасности). hash нужен для terminate_session."""
    c = await tg()
    r = await c(functions.account.GetAuthorizationsRequest())
    return j([{"hash": a.hash, "device": a.device_model, "app": a.app_name, "ip": a.ip, "country": a.country,
               "current": a.current, "last_active": a.date_active} for a in r.authorizations])


# ---------------------------------------------------------------- черновики, отложенные, упоминания, реакции
@mcp.tool()
async def list_scheduled(chat: str) -> str:
    """Отложенные (запланированные) сообщения в чате."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), scheduled=True)]
    return j([msg(m) for m in msgs])


@mcp.tool()
async def list_drafts() -> str:
    """Черновики во всех чатах."""
    c = await tg()
    out = []
    async for d in c.iter_drafts():
        out.append({"chat_id": pk(d.input_entity), "chat": utils.get_display_name(d.entity) if d.entity else None,
                    "text": d.text})
    return j(out)


@mcp.tool()
async def list_mentions(chat: str, limit: int = 20) -> str:
    """Сообщения, где упомянули меня (@), в чате."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), limit=limit, filter=types.InputMessagesFilterMyMentions)]
    return j([msg(m) for m in msgs])


@mcp.tool()
async def unread_reactions(chat: str, limit: int = 20) -> str:
    """Сообщения в чате, на которые мне поставили реакции, но я ещё не видел."""
    c = await tg()
    ent = peer(chat)
    inp = await c.get_input_entity(ent)
    r = await c(functions.messages.GetUnreadReactionsRequest(
        peer=inp, offset_id=0, add_offset=0, limit=limit, max_id=0, min_id=0))
    ids = [m.id for m in r.messages]
    if not ids:
        return "[]"
    msgs = await c.get_messages(ent, ids=ids)
    return j([msg(m) for m in msgs if m])


# ---------------------------------------------------------------- опросы
@mcp.tool()
async def poll_results(chat: str, message_id: int) -> str:
    """Результаты опроса: варианты, число голосов, за что проголосовал я."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    mp = m.poll if m else None
    if not mp:
        return "Это не опрос."
    counts = {bytes(r.option): r for r in (mp.results.results or [])} if mp.results and mp.results.results else {}
    answers = []
    for i, a in enumerate(mp.poll.answers):
        r = counts.get(bytes(a.option))
        answers.append({"index": i, "text": _txt(a.text), "voters": r.voters if r else 0,
                        "chosen_by_me": bool(r and r.chosen)})
    return j({"question": _txt(mp.poll.question), "closed": mp.poll.closed,
              "total_voters": mp.results.total_voters if mp.results else None, "answers": answers})


# ---------------------------------------------------------------- форумы (темы)
@mcp.tool()
async def list_topics(chat: str, limit: int = 50, query: str = "") -> str:
    """Темы в группе-форуме."""
    c = await tg()
    ent = await c.get_input_entity(peer(chat))
    r = await c(functions.channels.GetForumTopicsRequest(
        channel=ent, offset_date=None, offset_id=0, offset_topic=0, limit=limit, q=query or None))
    return j([{"id": t.id, "title": getattr(t, "title", None), "unread": getattr(t, "unread_count", None),
               "closed": getattr(t, "closed", None), "pinned": getattr(t, "pinned", None)} for t in r.topics])


# ---------------------------------------------------------------- ссылки-приглашения и заявки
@mcp.tool()
async def list_invite_links(chat: str, revoked: bool = False) -> str:
    """Мои ссылки-приглашения в чат/канал (нужны права админа)."""
    c = await tg()
    r = await c(functions.messages.GetExportedChatInvitesRequest(
        peer=await c.get_input_entity(peer(chat)), admin_id=await c.get_input_entity("me"),
        limit=100, revoked=revoked or None))
    return j([{"link": i.link, "title": i.title, "joined": i.usage, "limit": i.usage_limit, "revoked": i.revoked,
               "expire": i.expire_date, "needs_approval": i.request_needed} for i in r.invites])


@mcp.tool()
async def list_join_requests(chat: str, limit: int = 50) -> str:
    """Заявки на вступление в чат/канал."""
    c = await tg()
    r = await c(functions.messages.GetChatInviteImportersRequest(
        peer=await c.get_input_entity(peer(chat)), limit=limit, offset_date=None,
        offset_user=types.InputUserEmpty(), requested=True))
    users = {u.id: u for u in r.users}
    return j([{"user_id": i.user_id, "name": utils.get_display_name(users[i.user_id]) if i.user_id in users else None,
               "username": getattr(users.get(i.user_id), "username", None), "date": i.date, "about": i.about}
              for i in r.importers])


# ---------------------------------------------------------------- статистика и журнал
@mcp.tool()
async def channel_posts_report(chat: str, limit: int = 50, sort: str = "views") -> str:
    """Отчёт по постам канала: просмотры, пересылки, реакции, комментарии. sort: views, forwards, reactions, date.
    Работает для любого канала без прав админа."""
    c = await tg()
    rows = []
    async for m in c.iter_messages(peer(chat), limit=limit):
        rows.append({"id": m.id, "date": m.date.isoformat() if m.date else None, "views": m.views or 0,
                     "forwards": m.forwards or 0, "replies": m.replies.replies if m.replies else 0,
                     "reactions": sum(r.count for r in m.reactions.results) if m.reactions else 0,
                     "text": (m.message or "")[:60]})
    if not rows:
        return "Постов нет."
    key = sort if sort in ("views", "forwards", "reactions") else None
    out = sorted(rows, key=lambda r: -r[key]) if key else rows
    return j({"posts": len(rows), "avg_views": sum(r["views"] for r in rows) // len(rows),
              "total_forwards": sum(r["forwards"] for r in rows),
              "total_reactions": sum(r["reactions"] for r in rows), "rows": out})


@mcp.tool()
async def channel_stats(chat: str) -> str:
    """Официальная статистика канала (подписчики, охват). Нужны права админа и канал от ~500 подписчиков."""
    c = await tg()
    ent = await c.get_input_entity(peer(chat))
    req = functions.stats.GetBroadcastStatsRequest(channel=ent)
    try:
        r = await c(req)
    except errors.RPCError as e:
        if type(e).__name__ != "StatsMigrateError":
            raise
        sender = await c._borrow_exported_sender(e.dc)  # noqa: SLF001
        try:
            r = await c._call(sender, req)  # noqa: SLF001
        finally:
            await c._return_exported_sender(sender)  # noqa: SLF001

    def sv(x):
        return {"current": x.current, "previous": x.previous} if x is not None and hasattr(x, "current") else None

    out = {n: sv(getattr(r, n, None)) for n in ("followers", "views_per_post", "shares_per_post",
                                                 "reactions_per_post", "views_per_story", "shares_per_story")}
    en = getattr(r, "enabled_notifications", None)
    if en is not None:
        out["notifications_on_percent"] = round(en.part * 100 / en.total, 1) if en.total else None
    return j(out)


@mcp.tool()
async def get_admin_log(chat: str, limit: int = 50, kind: str | None = None, search: str | None = None) -> str:
    """Журнал действий админов в группе/канале. kind: join, leave, invite, ban, unban, restrict, promote, demote, delete, edit, pinned, info, settings."""
    c = await tg()
    allowed = {"join", "leave", "invite", "ban", "unban", "restrict", "unrestrict", "promote", "demote",
               "delete", "edit", "pinned", "info", "settings"}
    kw = {}
    if kind:
        if kind not in allowed:
            return f"Неизвестный kind. Доступно: {', '.join(sorted(allowed))}"
        kw[kind] = True
    out = []
    async for e in c.iter_admin_log(peer(chat), limit=limit, search=search or None, **kw):
        out.append({"id": e.id, "date": e.date.isoformat(), "user_id": e.user_id,
                    "user": utils.get_display_name(e.user) if e.user else None,
                    "action": type(e.action).__name__.replace("ChannelAdminLogEventAction", ""),
                    "old": str(e.old)[:160] if e.old is not None else None,
                    "new": str(e.new)[:160] if e.new is not None else None})
    return j(out)


@mcp.tool()
async def get_privacy(key: str) -> str:
    """Настройка приватности. key: phone_number, last_seen, profile_photo, forwards, calls, invites, about, birthday."""
    c = await tg()
    k = _privacy_key(key)
    if isinstance(k, str):
        return k
    r = await c(functions.account.GetPrivacyRequest(key=k()))
    return j([type(x).__name__.replace("PrivacyValue", "") for x in r.rules])


def _privacy_key(key: str):
    keys = {"phone_number": "PhoneNumber", "last_seen": "StatusTimestamp", "profile_photo": "ProfilePhoto",
            "forwards": "Forwards", "calls": "PhoneCall", "invites": "ChatInvite", "about": "About",
            "birthday": "Birthday"}
    if key not in keys:
        return f"Неизвестный key. Доступно: {', '.join(keys)}"
    cls = getattr(types, "InputPrivacyKey" + keys[key], None)
    return cls or "Этот пункт не поддерживается установленной версией Telethon."


# ---------------------------------------------------------------- папки Telegram
FLAG_NAMES = ("contacts", "non_contacts", "groups", "broadcasts", "bots",
              "exclude_muted", "exclude_read", "exclude_archived")


async def _folders(c):
    r = await c(functions.messages.GetDialogFiltersRequest())
    return list(getattr(r, "filters", r))


def _ftitle(f) -> str:
    return _txt(getattr(f, "title", "")) or ""


def _find_folder(fs, key):
    k = str(key).strip().lower()
    for f in fs:
        if isinstance(f, types.DialogFilterDefault):
            continue
        if str(f.id) == k or _ftitle(f).lower() == k:
            return f
    return None


def _flags_of(f) -> dict:
    d = {n: True for n in FLAG_NAMES if getattr(f, n, None)}
    if getattr(f, "emoticon", None):
        d["emoticon"] = f.emoticon
    if getattr(f, "color", None) is not None:
        d["color"] = f.color
    return d


async def _save_filter(c, fid, title, include, exclude, pinned, flags):
    def mk(t):
        return types.DialogFilter(id=fid, title=t, pinned_peers=list(pinned), include_peers=list(include),
                                  exclude_peers=list(exclude), **flags)

    builders = [lambda: functions.messages.UpdateDialogFilterRequest(
        id=fid, filter=mk(types.TextWithEntities(text=title, entities=[])))]
    builders.append(lambda: functions.messages.UpdateDialogFilterRequest(id=fid, filter=mk(title)))
    await _call_variants(c, builders)


def _folder_summary(f) -> dict:
    if isinstance(f, types.DialogFilterDefault):
        return {"id": 0, "title": "Все чаты (по умолчанию)"}
    d = {"id": f.id, "title": _ftitle(f), "emoji": getattr(f, "emoticon", None),
         "chats": len(f.include_peers), "pinned": len(f.pinned_peers)}
    if isinstance(f, types.DialogFilterChatlist):
        d["shared"] = True
    else:
        d["exclude"] = len(f.exclude_peers)
        d["auto"] = [n for n in FLAG_NAMES if getattr(f, n, None)]
    return d


@mcp.tool()
async def folder_list() -> str:
    """Список папок Telegram (id, название, число чатов, автоправила)."""
    c = await tg()
    return j([_folder_summary(f) for f in await _folders(c)])


@mcp.tool()
async def folder_get(folder: str) -> str:
    """Содержимое папки: какие чаты в ней лежат. folder — id или название."""
    c = await tg()
    f = _find_folder(await _folders(c), folder)
    if not f:
        return "Папка не найдена."
    chats = []
    for p in list(f.include_peers):
        try:
            e = await c.get_entity(p)
            chats.append({"id": utils.get_peer_id(e), "name": utils.get_display_name(e)})
        except Exception:  # noqa: BLE001
            chats.append({"id": pk(p), "name": None})
    return j({**_folder_summary(f), "include": chats})


@mcp.tool()
async def folder_get_chats_unread(folder: str) -> str:
    """Непрочитанные чаты только из этой папки."""
    c = await tg()
    f = _find_folder(await _folders(c), folder)
    if not f:
        return "Папка не найдена."
    ids = {pk(p) for p in f.include_peers}
    out = []
    async for d in c.iter_dialogs(limit=None):
        if d.id in ids and d.unread_count:
            out.append({"id": d.id, "name": d.name, "type": dialog_kind(d), "unread": d.unread_count,
                        "last": (d.message.message or "")[:80] if d.message else ""})
    return j(out)


@write_tool
async def folder_create(title: str, chats: list[str] | None = None, emoji: str | None = None,
                        include_contacts: bool = False, include_non_contacts: bool = False,
                        include_groups: bool = False, include_channels: bool = False, include_bots: bool = False,
                        exclude_muted: bool = False, exclude_read: bool = False,
                        exclude_archived: bool = False) -> str:
    """Создать папку. chats — список чатов (@username или id). include_* — автоматически класть все чаты такого типа."""
    c = await tg()
    fs = await _folders(c)
    used = {f.id for f in fs if hasattr(f, "id")}
    fid = next((i for i in range(2, 256) if i not in used), None)
    if fid is None:
        return "Нет свободных id папок."
    include = [await c.get_input_entity(peer(x)) for x in (chats or [])]
    flags = {n: True for n, on in (("contacts", include_contacts), ("non_contacts", include_non_contacts),
                                   ("groups", include_groups), ("broadcasts", include_channels),
                                   ("bots", include_bots), ("exclude_muted", exclude_muted),
                                   ("exclude_read", exclude_read), ("exclude_archived", exclude_archived)) if on}
    if emoji:
        flags["emoticon"] = emoji
    await _save_filter(c, fid, title, include, [], [], flags)
    return f"Папка создана, id={fid}."


@write_tool
async def folder_edit(folder: str, title: str | None = None, add_chats: list[str] | None = None,
                      remove_chats: list[str] | None = None, emoji: str | None = None,
                      flags: dict[str, bool] | None = None) -> str:
    """Изменить папку (id или название): переименовать, добавить/убрать чаты, сменить эмодзи.
    flags — автоправила: contacts, non_contacts, groups, broadcasts, bots, exclude_muted, exclude_read, exclude_archived."""
    c = await tg()
    f = _find_folder(await _folders(c), folder)
    if not f:
        return "Папка не найдена."
    if isinstance(f, types.DialogFilterChatlist):
        return "Общие папки (по ссылке) можно менять только в приложении."
    include = list(f.include_peers)
    pinned = list(f.pinned_peers)
    for ch in add_chats or []:
        ip = await c.get_input_entity(peer(ch))
        if pk(ip) not in {pk(x) for x in include}:
            include.append(ip)
    for ch in remove_chats or []:
        key = pk(await c.get_input_entity(peer(ch)))
        include = [x for x in include if pk(x) != key]
        pinned = [x for x in pinned if pk(x) != key]
    fl = _flags_of(f)
    for n, v in (flags or {}).items():
        if n not in FLAG_NAMES:
            return f"Неизвестный флаг {n}. Доступно: {', '.join(FLAG_NAMES)}"
        fl.pop(n, None)
        if v:
            fl[n] = True
    if emoji is not None:
        fl.pop("emoticon", None)
        if emoji:
            fl["emoticon"] = emoji
    await _save_filter(c, f.id, title or _ftitle(f), include, list(f.exclude_peers), pinned, fl)
    return "Папка обновлена."


@write_tool
async def folder_delete(folder: str) -> str:
    """Удалить папку (сами чаты не удаляются)."""
    c = await tg()
    f = _find_folder(await _folders(c), folder)
    if not f:
        return "Папка не найдена."
    await c(functions.messages.UpdateDialogFilterRequest(id=f.id, filter=None))
    return "Папка удалена."


@write_tool
async def folder_reorder(order: list[str]) -> str:
    """Задать порядок папок: список id или названий. Неупомянутые идут следом в прежнем порядке."""
    c = await tg()
    fs = await _folders(c)
    existing = [0 if isinstance(f, types.DialogFilterDefault) else f.id for f in fs]
    chosen = []
    for k in order:
        if str(k).strip() == "0":
            chosen.append(0)
            continue
        f = _find_folder(fs, k)
        if not f:
            return f"Папка «{k}» не найдена."
        chosen.append(f.id)
    final = chosen + [x for x in existing if x not in chosen]
    await c(functions.messages.UpdateDialogFiltersOrderRequest(order=final))
    return "Порядок обновлён."


async def match_dialogs(c, kind=None, name_contains=None, min_unread=0, inactive_days=None, muted=None,
                        archived=False, limit=200):
    kinds = {k.strip() for k in kind.split(",")} if kind else None
    now = datetime.now(timezone.utc)
    out = []
    async for d in c.iter_dialogs(limit=None, folder=1 if archived else 0):
        if kinds and dialog_kind(d) not in kinds:
            continue
        if name_contains and name_contains.lower() not in (d.name or "").lower():
            continue
        if min_unread and d.unread_count < min_unread:
            continue
        if muted is not None and _muted(d) != muted:
            continue
        if inactive_days is not None and d.date and (now - d.date).days < inactive_days:
            continue
        out.append(d)
        if len(out) >= limit:
            break
    return out


def _preview(ds):
    return [{"id": d.id, "name": d.name, "type": dialog_kind(d), "unread": d.unread_count} for d in ds]


@write_tool
async def folder_fill(folder: str, kind: str | None = None, name_contains: str | None = None,
                      min_unread: int = 0, inactive_days: int | None = None, muted: bool | None = None,
                      archived: bool = False, limit: int = 100, create: bool = True, dry_run: bool = True) -> str:
    """Положить в папку все чаты по условию (например kind=bot — всех ботов). Если папки нет и create=True, она создаётся.
    kind: user, bot, group, channel (через запятую). dry_run=True только показывает, что будет сделано."""
    c = await tg()
    if not any([kind, name_contains, min_unread, inactive_days is not None, muted is not None]):
        return "Укажи хотя бы одно условие (kind, name_contains, min_unread, inactive_days, muted)."
    ds = await match_dialogs(c, kind, name_contains, min_unread, inactive_days, muted, archived, limit)
    if dry_run:
        return j({"dry_run": True, "matched": len(ds), "chats": _preview(ds),
                  "hint": "Повтори с dry_run=false, чтобы применить."})
    if not ds:
        return "Ничего не подошло."
    fs = await _folders(c)
    f = _find_folder(fs, folder)
    new_peers = [d.input_entity for d in ds]
    if not f:
        if not create:
            return "Папка не найдена."
        used = {x.id for x in fs if hasattr(x, "id")}
        fid = next((i for i in range(2, 256) if i not in used), None)
        if fid is None:
            return "Нет свободных id папок."
        await _save_filter(c, fid, folder, new_peers, [], [], {})
        return f"Папка «{folder}» создана (id={fid}), чатов: {len(new_peers)}."
    if isinstance(f, types.DialogFilterChatlist):
        return "Общие папки (по ссылке) можно менять только в приложении."
    have = {pk(x) for x in f.include_peers}
    include = list(f.include_peers) + [p for p in new_peers if pk(p) not in have]
    await _save_filter(c, f.id, _ftitle(f), include, list(f.exclude_peers), list(f.pinned_peers), _flags_of(f))
    return f"В папку «{_ftitle(f)}» добавлено чатов: {len(include) - len(have)}."


# ================================================================ МАССОВЫЕ ДЕЙСТВИЯ С ЧАТАМИ
async def _apply_action(c, d, action):
    inp = d.input_entity
    if action in ("mute", "unmute"):
        until = 2**31 - 1 if action == "mute" else 0
        await retry_flood(c, functions.account.UpdateNotifySettingsRequest(
            peer=types.InputNotifyPeer(inp), settings=types.InputPeerNotifySettings(mute_until=until)))
    elif action in ("archive", "unarchive"):
        await retry_flood(c.edit_folder, d.entity, 1 if action == "archive" else 0)
    elif action == "mark_read":
        await retry_flood(c.send_read_acknowledge, d.entity)
    elif action == "leave":
        await retry_flood(c.delete_dialog, d.entity)


@write_tool
async def bulk_chats(action: str, kind: str | None = None, name_contains: str | None = None,
                     min_unread: int = 0, inactive_days: int | None = None, muted: bool | None = None,
                     archived: bool = False, limit: int = 100, dry_run: bool = True) -> str:
    """Массовое действие над чатами по условию.
    action: mute, unmute, archive, unarchive, mark_read, leave (выйти из группы/канала или удалить диалог).
    Условия: kind (user, bot, group, channel), name_contains, min_unread, inactive_days (нет сообщений N дней), muted.
    dry_run=True (по умолчанию) только показывает список. Для leave обязательно нужно хотя бы одно условие."""
    c = await tg()
    if action not in ("mute", "unmute", "archive", "unarchive", "mark_read", "leave"):
        return "action: mute, unmute, archive, unarchive, mark_read или leave."
    if action == "leave" and not any([kind, name_contains, inactive_days is not None, min_unread]):
        return "Для leave нужно хотя бы одно условие (kind, name_contains, inactive_days, min_unread)."
    ds = await match_dialogs(c, kind, name_contains, min_unread, inactive_days, muted, archived, limit)
    if dry_run:
        return j({"dry_run": True, "action": action, "matched": len(ds), "chats": _preview(ds),
                  "hint": "Повтори с dry_run=false, чтобы применить."})

    async def run(job=None):
        done = 0
        for d in ds:
            try:
                await _apply_action(c, d, action)
                done += 1
            except Exception as e:  # noqa: BLE001
                if job is not None:
                    job.setdefault("errors", []).append(f"{d.name}: {e}")
            if job is not None:
                job["progress"] = f"{done}/{len(ds)}"
            await asyncio.sleep(0.4)
        return {"done": done, "total": len(ds)}

    if len(ds) > 25:
        return f"Запущено в фоне, job_id={spawn('bulk_chats', run)}. Смотри job_status."
    r = await run()
    return f"Готово: {r['done']} из {r['total']}."


# ================================================================ ФОНОВЫЕ ЗАДАЧИ (статус)
@mcp.tool()
async def job_status(job_id: str) -> str:
    """Статус фоновой задачи (экспорт, массовая загрузка, большая отправка...)."""
    job = JOBS.get(job_id)
    return j(_job_view(job)) if job else "Задача не найдена."


@mcp.tool()
async def job_list() -> str:
    """Список недавних фоновых задач."""
    return j([{k: v for k, v in _job_view(x).items() if k in ("id", "kind", "status", "progress", "started")}
              for x in JOBS.values()])


@mcp.tool()
async def job_cancel(job_id: str) -> str:
    """Отменить фоновую задачу."""
    job = JOBS.get(job_id)
    if not job:
        return "Задача не найдена."
    job["_task"].cancel()
    return "Отмена запрошена."


# ================================================================ ФАЙЛЫ НА RENDER
@mcp.tool()
async def list_uploads() -> str:
    """Файлы в хранилище на сервере (загруженные через /upload, PUT, или сохранённые из Telegram)."""
    _gc()
    files = sorted(_store_files(), key=lambda p: -p.stat().st_mtime)
    return j({"used_mb": round(_store_used() / 1048576, 1), "limit_mb": STORE_MAX // 1048576,
              "ttl_hours": STORE_TTL // 3600, "files": [_file_info(p) for p in files]})


@mcp.tool()
async def file_link(upload_id: str) -> str:
    """Прямая ссылка на скачивание файла из хранилища (доступна всем, у кого есть ссылка)."""
    p = _find(upload_id)
    return _url(p) if p else "Файл не найден."


@mcp.tool()
async def file_from_chat(chat: str, message_id: int, background: bool | None = None) -> str:
    """Скачать вложение сообщения из Telegram в хранилище на сервере (без лимита base64) и вернуть ссылку.
    Большие файлы (>40 МБ) качаются в фоне, тогда вернётся job_id."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    if not m or not m.media:
        return "В сообщении нет вложения."
    size = (m.file.size or 0) if m.file else 0
    if background is None:
        background = size > 40 * 1048576
    if not background:
        return j(_file_info(await _tg_to_store(c, m)))

    async def run(job):
        def prog(cur, total):
            job["progress"] = f"{cur * 100 // max(total, 1)}%"

        return _file_info(await _tg_to_store(c, m, progress=prog))

    return f"Качаю в фоне, job_id={spawn('file_from_chat', run)}. Смотри job_status."


@mcp.tool()
async def file_from_url(url: str, filename: str | None = None) -> str:
    """Сервер сам скачивает файл по ссылке в хранилище (внутренние адреса запрещены). Дальше можно отправить через send_uploaded."""
    p = await _fetch_to_store(url, filename)
    return j(_file_info(p))


@mcp.tool()
async def file_put_begin(filename: str) -> str:
    """Начать загрузку файла по частям (для больших файлов, когда не хватает одного вызова). Вернёт upload_id."""
    _gc()
    fid = secrets.token_hex(5)
    (PART / f"{fid}.part").write_bytes(b"")
    (PART / f"{fid}.name").write_text(_safe_name(filename), encoding="utf-8")
    return fid


@mcp.tool()
async def file_put_chunk(upload_id: str, data_b64: str, offset: int | None = None) -> str:
    """Дописать часть файла (base64). offset — ожидаемый размер до записи: защита от дублей при повторе."""
    if not re.fullmatch(r"[0-9a-f]{10}", upload_id):
        return "Неверный upload_id."
    part = PART / f"{upload_id}.part"
    if not part.exists():
        return "Загрузка не найдена (сначала file_put_begin)."
    cur = part.stat().st_size
    if offset is not None and offset != cur:
        return j({"error": "offset не совпадает", "size": cur})
    data = base64.b64decode(data_b64)
    if cur + len(data) > STORE_MAX:
        return "Превышен лимит хранилища."
    with open(part, "ab") as f:
        f.write(data)
    return j({"size": cur + len(data)})


@mcp.tool()
async def file_put_finish(upload_id: str, send_to: str | None = None, caption: str = "",
                          as_document: bool = False, voice_note: bool = False, reply_to: int | None = None) -> str:
    """Завершить загрузку по частям. Если указан send_to — файл сразу отправится в этот чат Telegram."""
    if not re.fullmatch(r"[0-9a-f]{10}", upload_id):
        return "Неверный upload_id."
    part, namef = PART / f"{upload_id}.part", PART / f"{upload_id}.name"
    if not part.exists():
        return "Загрузка не найдена."
    name = namef.read_text(encoding="utf-8") if namef.exists() else "file"
    _, p = _new_path(name)
    shutil.move(str(part), str(p))
    namef.unlink(missing_ok=True)
    item = _file_info(p)
    if send_to:
        if READ_ONLY:
            item["send_error"] = "режим только чтения"
        else:
            c = await tg()
            if p.stat().st_size > 30 * 1048576:
                async def run(job):
                    def prog(cur, total):
                        job["progress"] = f"{cur * 100 // max(total, 1)}%"

                    m = await _send_stored(c, send_to, p, caption, as_document, voice_note, reply_to, prog)
                    return {"message_id": m.id}

                item["send_job"] = spawn("send_file", run)
            else:
                item["sent_message_id"] = (await _send_stored(c, send_to, p, caption, as_document,
                                                               voice_note, reply_to)).id
    return j(item)


@mcp.tool()
async def file_read_chunk(upload_id: str, offset: int = 0, length: int = 524288) -> str:
    """Прочитать кусок файла из хранилища (base64). Читай по кускам, пока eof=true."""
    p = _find(upload_id)
    if not p:
        return "Файл не найден."
    length = max(1, min(length, 3 * 1048576))
    total = p.stat().st_size
    with open(p, "rb") as f:
        f.seek(offset)
        data = f.read(length)
    return j({"offset": offset, "length": len(data), "total": total, "eof": offset + len(data) >= total,
              "base64": base64.b64encode(data).decode()})


@mcp.tool()
async def file_read_text(upload_id: str, offset: int = 0, max_chars: int = 20000, encoding: str = "utf-8") -> str:
    """Прочитать текстовый файл из хранилища как текст (логи, json, csv, код)."""
    p = _find(upload_id)
    if not p:
        return "Файл не найден."
    with open(p, "rb") as f:
        f.seek(offset)
        raw = f.read(max_chars * 4)
    text = raw.decode(encoding, errors="replace")[:max_chars]
    return j({"offset": offset, "next_offset": offset + len(text.encode(encoding, errors="replace")),
              "total": p.stat().st_size, "text": text})


@mcp.tool()
async def delete_upload(upload_id: str) -> str:
    """Удалить загруженный файл с сервера."""
    p = _find(upload_id)
    if not p:
        return "Файл не найден."
    p.unlink(missing_ok=True)
    return "Удалено."


@mcp.tool()
async def purge_uploads(older_than_hours: float = 0) -> str:
    """Удалить файлы из хранилища старше N часов (0 — все)."""
    now, n = time.time(), 0
    for p in _store_files():
        if now - p.stat().st_mtime >= older_than_hours * 3600:
            p.unlink(missing_ok=True)
            n += 1
    return f"Удалено файлов: {n}"


@write_tool
async def send_uploaded(chat: str, upload_id: str, as_document: bool = False, caption: str = "",
                        delete_after: bool = True, reply_to: int | None = None, voice_note: bool = False,
                        background: bool | None = None) -> str:
    """Отправить в Telegram файл, который лежит в хранилище на сервере (по его id). Большие файлы уходят в фоне."""
    p = _find(upload_id)
    if not p:
        return "Файл не найден."
    c = await tg()
    if background is None:
        background = p.stat().st_size > 30 * 1048576
    if not background:
        m = await _send_stored(c, chat, p, caption, as_document, voice_note, reply_to)
        if delete_after:
            p.unlink(missing_ok=True)
        return f"Файл отправлен, id={m.id}"

    async def run(job):
        def prog(cur, total):
            job["progress"] = f"{cur * 100 // max(total, 1)}%"

        m = await _send_stored(c, chat, p, caption, as_document, voice_note, reply_to, prog)
        if delete_after:
            p.unlink(missing_ok=True)
        return {"message_id": m.id}

    return f"Отправляю в фоне, job_id={spawn('send_uploaded', run)}. Смотри job_status."


@write_tool
async def send_album(chat: str, caption: str = "", file_urls: list[str] | None = None,
                     upload_ids: list[str] | None = None, reply_to: int | None = None) -> str:
    """Отправить несколько фото/файлов одним альбомом (из хранилища и/или по ссылкам)."""
    c = await tg()
    files = list(file_urls or [])
    for uid in upload_ids or []:
        p = _find(uid)
        if not p:
            return f"Файл {uid} не найден."
        files.append(await c.upload_file(str(p), file_name=p.name.split("__", 1)[1]))
    if len(files) < 2:
        return "Для альбома нужно минимум 2 файла."
    r = await c.send_file(peer(chat), files, caption=caption, reply_to=reply_to)
    return f"Альбом отправлен, сообщений: {len(r)}"


@mcp.tool()
async def export_chat(chat: str, fmt: str = "txt", limit: int | None = None, since_iso: str | None = None,
                      until_iso: str | None = None) -> str:
    """Выгрузить историю чата в файл (в фоне) и получить ссылку. fmt: txt или jsonl.
    limit — сколько последних сообщений (пусто = вся история), since_iso/until_iso — период."""
    if fmt not in ("txt", "jsonl"):
        return "fmt: txt или jsonl."
    c = await tg()
    ent = peer(chat)
    since, until = parse_dt(since_iso), parse_dt(until_iso)
    e = await c.get_entity(ent)
    label = _safe_name(utils.get_display_name(e) or str(chat))

    async def run(job):
        _gc(need=50 * 1048576)
        _, p = _new_path(f"{label}.{fmt}")
        n = 0

        async def gen():
            if since or limit is None:
                async for m in c.iter_messages(ent, reverse=True, offset_date=since, limit=limit):
                    yield m
            else:
                buf = [m async for m in c.iter_messages(ent, limit=limit)]
                for m in reversed(buf):
                    yield m

        with open(p, "w", encoding="utf-8") as f:
            async for m in gen():
                if until and m.date and m.date > until:
                    break
                if fmt == "jsonl":
                    f.write(j(msg(m)) + "\n")
                else:
                    who = utils.get_display_name(m.sender) if m.sender else (m.sender_id or "?")
                    tag = f" [{media_kind(m)}]" if m.media else ""
                    f.write(f"[{m.date:%Y-%m-%d %H:%M}] {who}: {m.message or ''}{tag}\n")
                n += 1
                if n % 200 == 0:
                    job["progress"] = f"{n} сообщений"
        return {**_file_info(p), "messages": n}

    return f"Выгрузка запущена, job_id={spawn('export_chat', run)}. Смотри job_status, в result будет ссылка."


@mcp.tool()
async def chat_media_to_store(chat: str, media_filter: str = "photo", limit: int = 100, max_mb: int = 100,
                              make_zip: bool = True) -> str:
    """Скачать вложения чата пачкой в хранилище (в фоне). media_filter: photo, video, photo_video, document, voice, music, gif, round.
    make_zip=True — одним zip-архивом. max_mb — пропускать файлы больше."""
    if media_filter not in FILTERS:
        return f"Неизвестный фильтр. Доступно: {', '.join(FILTERS)}"
    c = await tg()
    ent = peer(chat)

    async def run(job):
        tmp = pathlib.Path(tempfile.mkdtemp(dir=PART))
        try:
            total, n, skipped = 0, 0, 0
            async for m in c.iter_messages(ent, limit=limit, filter=FILTERS[media_filter]):
                if not m.media:
                    continue
                size = (m.file.size or 0) if m.file else 0
                if max_mb and size > max_mb * 1048576:
                    skipped += 1
                    continue
                if total + size > STORE_MAX:
                    break
                base = (m.file.name if m.file and m.file.name else f"media{(m.file.ext if m.file else '') or ''}")
                await c.download_media(m, file=str(tmp / f"{m.id}_{_safe_name(base)}"))
                total += size
                n += 1
                job["progress"] = f"{n} файлов, {total // 1048576} МБ"
            files = [f for f in tmp.iterdir() if f.is_file()]
            _gc(need=sum(f.stat().st_size for f in files))
            if make_zip:
                _, zp = _new_path(f"{_safe_name(str(chat))}_{media_filter}.zip")

                def pack():
                    with zipfile.ZipFile(zp, "w", zipfile.ZIP_STORED) as z:
                        for f in files:
                            z.write(f, f.name)

                await asyncio.to_thread(pack)
                return {"files": n, "skipped_too_big": skipped, "archive": _file_info(zp)}
            infos = []
            for f in files:
                _, dst = _new_path(f.name)
                shutil.move(str(f), str(dst))
                infos.append(_file_info(dst))
            return {"files": n, "skipped_too_big": skipped, "items": infos[:100]}
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    return f"Скачиваю в фоне, job_id={spawn('chat_media', run)}. Смотри job_status."


# ================================================================ ЗАПИСЬ
@write_tool
async def send_message(chat: str, text: str, reply_to: int | None = None, parse_mode: str | None = None,
                       silent: bool = False, schedule_iso: str | None = None,
                       link_preview: bool = True) -> str:
    """Отправить сообщение. parse_mode: md | html | None. schedule_iso — отложенная отправка (ISO, UTC по умолчанию).
    Чтобы написать в тему форума, передай id темы в reply_to."""
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
async def forward_messages(from_chat: str, to_chat: str, message_ids: list[int], hide_sender: bool = False) -> str:
    """Переслать сообщения. hide_sender=True — без подписи «переслано от» (как копия)."""
    c = await tg()
    await c.forward_messages(peer(to_chat), message_ids, from_peer=peer(from_chat), drop_author=hide_sender or None)
    return "Переслано."


@write_tool
async def copy_messages(from_chat: str, message_ids: list[int], to_chat: str) -> str:
    """Скопировать сообщения в другой чат БЕЗ пометки «переслано» (текст + вложения)."""
    c = await tg()
    await c.forward_messages(peer(to_chat), message_ids, from_peer=peer(from_chat), drop_author=True)
    return "Скопировано."


@write_tool
async def send_file(chat: str, file_b64: str | None = None, file_url: str | None = None,
                    filename: str | None = None, caption: str = "", as_document: bool = False,
                    voice_note: bool = False, reply_to: int | None = None) -> str:
    """Отправить файл/фото/голосовое: из base64 (file_b64 + filename) или по ссылке (file_url).
    Для больших файлов лучше file_put_begin/chunk/finish с send_to или send_uploaded."""
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
async def send_contact(chat: str, first_name: str, phone: str, last_name: str = "") -> str:
    """Отправить карточку контакта."""
    c = await tg()
    m = await c.send_file(peer(chat), types.InputMediaContact(
        phone_number=phone, first_name=first_name, last_name=last_name, vcard=""))
    return f"Отправлено, id={m.id}"


@write_tool
async def send_location(chat: str, latitude: float, longitude: float) -> str:
    """Отправить геопозицию."""
    c = await tg()
    m = await c.send_file(peer(chat), types.InputMediaGeoPoint(types.InputGeoPoint(latitude, longitude)))
    return f"Отправлено, id={m.id}"


@write_tool
async def send_poll(chat: str, question: str, answers: list[str], anonymous: bool = True,
                    multiple: bool = False) -> str:
    """Создать опрос (2–10 вариантов)."""
    c = await tg()

    def build(wrap):
        poll = types.Poll(id=0, question=wrap(question), answers=[
            types.PollAnswer(text=wrap(a), option=bytes([i])) for i, a in enumerate(answers)],
            multiple_choice=multiple or None, public_voters=None if anonymous else True)
        return types.InputMediaPoll(poll=poll)

    last = None
    for wrap in (lambda t: types.TextWithEntities(text=t, entities=[]), lambda t: t):
        try:
            m = await c.send_message(peer(chat), file=build(wrap))
            return f"Опрос отправлен, id={m.id}"
        except errors.RPCError:
            raise
        except Exception as e:  # noqa: BLE001
            last = e
    raise last


@write_tool
async def poll_vote(chat: str, message_id: int, options: list[int]) -> str:
    """Проголосовать в опросе: индексы вариантов (с 0). Пустой список — отозвать голос."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    if not m or not m.poll:
        return "Это не опрос."
    opts = [m.poll.poll.answers[i].option for i in options]
    await c(functions.messages.SendVoteRequest(peer=await c.get_input_entity(peer(chat)),
                                               msg_id=message_id, options=opts))
    return "Голос учтён."


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
async def mark_unread(chat: str, unread: bool = True) -> str:
    """Пометить чат непрочитанным (синяя точка) или снять отметку."""
    c = await tg()
    await c(functions.messages.MarkDialogUnreadRequest(
        peer=types.InputDialogPeer(await c.get_input_entity(peer(chat))), unread=unread or None))
    return "Готово."


@write_tool
async def mark_reactions_read(chat: str) -> str:
    """Пометить реакции в чате просмотренными."""
    c = await tg()
    await c(functions.messages.ReadReactionsRequest(peer=await c.get_input_entity(peer(chat))))
    return "Готово."


@write_tool
async def pin_chat(chat: str, pinned: bool = True) -> str:
    """Закрепить чат вверху списка / открепить."""
    c = await tg()
    await c(functions.messages.ToggleDialogPinRequest(
        peer=types.InputDialogPeer(await c.get_input_entity(peer(chat))), pinned=pinned or None))
    return "Готово."


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
async def clear_history(chat: str, for_everyone: bool = False, confirm: bool = False) -> str:
    """Очистить историю чата, не удаляя сам чат. Нужно confirm=true. for_everyone — у всех (если разрешено)."""
    if not confirm:
        return "Это необратимо. Повтори с confirm=true."
    c = await tg()
    await c(functions.messages.DeleteHistoryRequest(
        peer=await c.get_input_entity(peer(chat)), max_id=0, just_clear=True, revoke=for_everyone or None))
    return "История очищена."


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
async def set_chat_photo(chat: str, upload_id: str) -> str:
    """Сменить аватар группы/канала на картинку из хранилища (по id)."""
    p = _find(upload_id)
    if not p:
        return "Файл не найден."
    c = await tg()
    ent = await c.get_entity(peer(chat))
    photo = types.InputChatUploadedPhoto(file=await c.upload_file(str(p)))
    if isinstance(ent, types.Channel):
        await c(functions.channels.EditPhotoRequest(channel=ent, photo=photo))
    else:
        await c(functions.messages.EditChatPhotoRequest(chat_id=ent.id, photo=photo))
    return "Аватар обновлён."


@write_tool
async def set_profile_photo(upload_id: str) -> str:
    """Сменить аватар своего профиля на картинку из хранилища (по id)."""
    p = _find(upload_id)
    if not p:
        return "Файл не найден."
    c = await tg()
    await c(functions.photos.UploadProfilePhotoRequest(file=await c.upload_file(str(p))))
    return "Аватар профиля обновлён."


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
async def restrict_user(chat: str, user: str, hours: int = 0, restrict: bool = True) -> str:
    """Ограничить пользователя (нельзя писать и слать медиа) на N часов (0 — навсегда) или снять ограничения (restrict=False)."""
    c = await tg()
    until = datetime.now(timezone.utc) + timedelta(hours=hours) if (restrict and hours > 0) else None
    allow = not restrict
    await c.edit_permissions(peer(chat), peer(user), until_date=until, send_messages=allow, send_media=allow,
                             send_stickers=allow, send_gifs=allow, send_games=allow, send_inline=allow,
                             embed_link_previews=allow, send_polls=allow)
    return "Ограничен." if restrict else "Ограничения сняты."


@write_tool
async def delete_user_history(chat: str, user: str, confirm: bool = False) -> str:
    """Удалить все сообщения пользователя в супергруппе/канале (антиспам). Нужны права админа и confirm=true."""
    if not confirm:
        return "Это удалит все его сообщения в чате. Повтори с confirm=true."
    c = await tg()
    ent = await c.get_entity(peer(chat))
    if not isinstance(ent, types.Channel):
        return "Работает только в супергруппах и каналах."
    await c(functions.channels.DeleteParticipantHistoryRequest(
        channel=ent, participant=await c.get_input_entity(peer(user))))
    return "Сообщения удалены."


@write_tool
async def set_slow_mode(chat: str, seconds: int = 0) -> str:
    """Медленный режим в супергруппе: 0 (выкл), 10, 30, 60, 300, 900, 3600 секунд."""
    c = await tg()
    await c(functions.channels.ToggleSlowModeRequest(channel=await c.get_input_entity(peer(chat)), seconds=seconds))
    return "Готово."


@write_tool
async def set_admin(chat: str, user: str, is_admin: bool = True, title: str | None = None) -> str:
    """Назначить/снять администратора."""
    c = await tg()
    await c.edit_admin(peer(chat), peer(user), is_admin=is_admin, title=title)
    return "Готово."


@write_tool
async def set_autodelete(chat: str, seconds: int = 0) -> str:
    """Автоудаление сообщений в чате через N секунд (86400 — сутки, 604800 — неделя, 0 — выключить)."""
    c = await tg()
    await c(functions.messages.SetHistoryTTLRequest(peer=await c.get_input_entity(peer(chat)), period=seconds))
    return "Готово."


@write_tool
async def create_invite_link(chat: str, title: str | None = None, expire_iso: str | None = None,
                             usage_limit: int | None = None, request_needed: bool = False) -> str:
    """Создать ссылку-приглашение (с лимитом переходов, сроком, подтверждением заявок)."""
    c = await tg()
    r = await c(functions.messages.ExportChatInviteRequest(
        peer=await c.get_input_entity(peer(chat)), title=title, expire_date=parse_dt(expire_iso),
        usage_limit=usage_limit, request_needed=request_needed or None))
    return r.link


@write_tool
async def revoke_invite_link(chat: str, link: str) -> str:
    """Отозвать ссылку-приглашение."""
    c = await tg()
    await c(functions.messages.EditExportedChatInviteRequest(
        peer=await c.get_input_entity(peer(chat)), link=link, revoked=True))
    return "Ссылка отозвана."


@write_tool
async def handle_join_requests(chat: str, approve: bool = True, user: str | None = None) -> str:
    """Принять/отклонить заявки на вступление: одну (user) или все сразу."""
    c = await tg()
    inp = await c.get_input_entity(peer(chat))
    if user:
        await c(functions.messages.HideChatJoinRequestRequest(
            peer=inp, user_id=await c.get_input_entity(peer(user)), approved=approve))
    else:
        await c(functions.messages.HideAllChatJoinRequestsRequest(peer=inp, approved=approve))
    return "Готово."


@write_tool
async def create_topic(chat: str, title: str) -> str:
    """Создать тему в группе-форуме."""
    c = await tg()
    await c(functions.channels.CreateForumTopicRequest(channel=await c.get_input_entity(peer(chat)), title=title))
    return "Тема создана."


@write_tool
async def edit_topic(chat: str, topic_id: int, title: str | None = None, closed: bool | None = None,
                     hidden: bool | None = None) -> str:
    """Изменить тему форума: название, закрыть/открыть, скрыть."""
    c = await tg()
    await c(functions.channels.EditForumTopicRequest(
        channel=await c.get_input_entity(peer(chat)), topic_id=topic_id, title=title, closed=closed, hidden=hidden))
    return "Готово."


@write_tool
async def delete_topic(chat: str, topic_id: int, confirm: bool = False) -> str:
    """Удалить тему форума вместе с историей. Нужно confirm=true."""
    if not confirm:
        return "Это удалит тему с историей. Повтори с confirm=true."
    c = await tg()
    await c(functions.channels.DeleteTopicHistoryRequest(
        channel=await c.get_input_entity(peer(chat)), top_msg_id=topic_id))
    return "Тема удалена."


# ---------------------------------------------------------------- боты и кнопки
@write_tool
async def click_button(chat: str, message_id: int, text: str | None = None, row: int | None = None,
                       col: int | None = None, wait_seconds: float = 3) -> str:
    """Нажать inline-кнопку под сообщением: по тексту (text) или по позиции (row, col; см. buttons в read_messages).
    Возвращает ответ бота: всплывающее окно, ссылку, обновлённое сообщение и новые сообщения."""
    c = await tg()
    ent = peer(chat)
    m = await c.get_messages(ent, ids=message_id)
    if not m:
        return "Сообщение не найдено."
    if text is not None:
        r = await m.click(text=text)
    elif row is not None and col is not None:
        r = await m.click(row, col)
    else:
        return "Нужно text или row и col."
    out = {"result": type(r).__name__ if r is not None else None}
    if isinstance(r, str):
        out["url"] = r
    elif r is not None:
        for k in ("message", "alert", "url"):
            v = getattr(r, k, None)
            if v:
                out[k] = v
    await asyncio.sleep(min(max(wait_seconds, 0), 20))
    upd = await c.get_messages(ent, ids=message_id)
    if upd:
        out["updated_message"] = msg(upd)
    out["new_messages"] = [msg(x) for x in reversed(
        [x async for x in c.iter_messages(ent, min_id=message_id, limit=5) if not x.out])]
    return j(out)


@write_tool
async def ask_bot(bot: str, text: str, wait_seconds: float = 8, max_messages: int = 5) -> str:
    """Написать боту (например /start или /balance) и дождаться его ответа. Вернёт ответы вместе с кнопками."""
    c = await tg()
    ent = peer(bot)
    sent = await c.send_message(ent, text)
    deadline = time.time() + min(max(wait_seconds, 1), 50)
    got, changed = [], time.time()
    while time.time() < deadline:
        await asyncio.sleep(1)
        msgs = [x async for x in c.iter_messages(ent, min_id=sent.id, limit=max_messages + 5) if not x.out]
        sig = [(x.id, x.edit_date) for x in msgs]
        if sig != [(x.id, x.edit_date) for x in got]:
            got, changed = msgs, time.time()
        elif got and time.time() - changed >= 2:
            break
    return j({"sent_id": sent.id, "replies": [msg(x) for x in reversed(got)][:max_messages]})


@write_tool
async def wait_for_message(chat: str, after_id: int, timeout: float = 30) -> str:
    """Ждать нового входящего сообщения в чате (после after_id), до timeout секунд (макс. 55)."""
    c = await tg()
    ent = peer(chat)
    deadline = time.time() + min(max(timeout, 1), 55)
    while time.time() < deadline:
        msgs = [x async for x in c.iter_messages(ent, min_id=after_id, limit=10) if not x.out]
        if msgs:
            return j([msg(x) for x in reversed(msgs)])
        await asyncio.sleep(1.5)
    return "[]"


@write_tool
async def inline_query(bot: str, query: str, chat: str | None = None, send_index: int | None = None) -> str:
    """Inline-запрос к боту (как @bot запрос). Без send_index вернёт варианты; с send_index и chat отправит выбранный вариант в чат."""
    c = await tg()
    res = await c.inline_query(peer(bot), query)
    if send_index is None:
        return j([{"index": i, "type": r.type, "title": r.title, "description": r.description}
                  for i, r in enumerate(list(res)[:30])])
    if not chat:
        return "Для отправки нужен chat."
    await res[send_index].click(peer(chat))
    return "Отправлено."


# ---------------------------------------------------------------- черновики и отложенные (запись)
@write_tool
async def set_draft(chat: str, text: str = "") -> str:
    """Сохранить черновик в чате (пустой текст — удалить черновик)."""
    c = await tg()
    await c(functions.messages.SaveDraftRequest(peer=await c.get_input_entity(peer(chat)), message=text))
    return "Черновик сохранён." if text else "Черновик удалён."


@write_tool
async def delete_scheduled(chat: str, message_ids: list[int]) -> str:
    """Удалить отложенные сообщения."""
    c = await tg()
    await c(functions.messages.DeleteScheduledMessagesRequest(
        peer=await c.get_input_entity(peer(chat)), id=message_ids))
    return "Удалено."


@write_tool
async def send_scheduled_now(chat: str, message_ids: list[int]) -> str:
    """Отправить отложенные сообщения прямо сейчас."""
    c = await tg()
    await c(functions.messages.SendScheduledMessagesRequest(
        peer=await c.get_input_entity(peer(chat)), id=message_ids))
    return "Отправлено."


# ---------------------------------------------------------------- контакты, профиль, безопасность
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


@write_tool
async def set_privacy(key: str, who: str) -> str:
    """Изменить приватность. key: phone_number, last_seen, profile_photo, forwards, calls, invites, about, birthday.
    who: everybody, contacts, nobody."""
    c = await tg()
    k = _privacy_key(key)
    if isinstance(k, str):
        return k
    rule = {"everybody": types.InputPrivacyValueAllowAll, "contacts": types.InputPrivacyValueAllowContacts,
            "nobody": types.InputPrivacyValueDisallowAll}.get(who)
    if not rule:
        return "who: everybody, contacts или nobody."
    await c(functions.account.SetPrivacyRequest(key=k(), rules=[rule()]))
    return "Приватность обновлена."


@write_tool
async def terminate_session(session_hash: int, confirm: bool = False) -> str:
    """Завершить чужую сессию аккаунта по hash из get_active_sessions. Текущую (сервер) завершить нельзя."""
    if not confirm:
        return "Повтори с confirm=true."
    c = await tg()
    r = await c(functions.account.GetAuthorizationsRequest())
    for a in r.authorizations:
        if a.hash == session_hash and a.current:
            return "Это сессия самого сервера, её завершать нельзя."
    await c(functions.account.ResetAuthorizationRequest(hash=session_hash))
    return "Сессия завершена."


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
