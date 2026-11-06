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
  RENDER_API_KEY         (необязательно) API-ключ Render для чтения логов/событий
  RENDER_OWNER_ID        (необязательно) ID workspace Render (обязателен для логов)
  RENDER_SERVICE_ID      (необязательно) ID сервиса Render, для которого читать логи

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
import logging
import inspect
from collections import deque
from typing import Any
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
from urllib.parse import quote, unquote, urlparse, urljoin

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

# Render API (необязательно; задаётся в Environment на Render).
RENDER_API_KEY = os.environ.get("RENDER_API_KEY", "").strip()
RENDER_OWNER_ID = os.environ.get("RENDER_OWNER_ID", "").strip()
RENDER_SERVICE_ID = os.environ.get("RENDER_SERVICE_ID", "").strip()
RENDER_API_BASE = "https://api.render.com/v1"

# Журнал действий MCP: доступен агенту отдельным инструментом и попадает в Render stdout.
TOOL_LOG_PATH = STORE / "tool_execution.jsonl"
TOOL_LOGS: deque[dict] = deque(maxlen=1500)
TOOL_REGISTRY: dict[str, Any] = {}
WRITE_TOOL_NAMES: set[str] = set()
_LOG_SEQUENCE = 0
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
_TOOL_LOGGER = logging.getLogger("telegram_mcp.tools")

def _load_tool_logs() -> None:
    if not TOOL_LOG_PATH.exists():
        return
    try:
        with TOOL_LOG_PATH.open("r", encoding="utf-8") as fh:
            rows = fh.readlines()[-TOOL_LOGS.maxlen:]
        for row in rows:
            try:
                TOOL_LOGS.append(json.loads(row))
            except json.JSONDecodeError:
                continue
        if TOOL_LOGS:
            global _LOG_SEQUENCE
            _LOG_SEQUENCE = max(int(x.get("sequence", 0)) for x in TOOL_LOGS)
    except OSError:
        _TOOL_LOGGER.exception("Не удалось загрузить журнал инструментов")


def _safe_log_value(key: str, value: Any) -> Any:
    k = key.lower()
    if any(x in k for x in ("secret", "token", "password", "api_hash", "session_hash", "hash", "phone", "file_b64", "data_b64", "base64")):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(a): _safe_log_value(str(a), b) for a, b in list(value.items())[:40]}
    if isinstance(value, (list, tuple)):
        return [_safe_log_value(key, x) for x in list(value)[:40]]
    if isinstance(value, str):
        if k in ("text", "caption", "about", "message"):
            return {"chars": len(value), "content": "[OMITTED]"}
        return value[:300] + ("…" if len(value) > 300 else "")
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)[:200]


def _append_tool_log(tool_name: str, args: dict, status: str, duration_ms: int, result: Any = None, error: str | None = None) -> dict:
    global _LOG_SEQUENCE
    _LOG_SEQUENCE += 1
    record = {
        "sequence": _LOG_SEQUENCE,
        "call_id": secrets.token_hex(4),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool": tool_name,
        "status": status,
        "duration_ms": duration_ms,
        "arguments": {str(k): _safe_log_value(str(k), v) for k, v in list(args.items())[:40]},
    }
    if error:
        record["error"] = error[:1200]
    if result is not None:
        if isinstance(result, str):
            try:
                parsed = json.loads(result)
                record["result"] = _safe_log_value("result", parsed)
            except Exception:
                record["result"] = result[:700] + ("…" if len(result) > 700 else "")
        else:
            record["result"] = _safe_log_value("result", result)
    TOOL_LOGS.append(record)
    try:
        if TOOL_LOG_PATH.exists() and TOOL_LOG_PATH.stat().st_size > 5 * 1024 * 1024:
            rotated = TOOL_LOG_PATH.with_suffix(".jsonl.1")
            rotated.unlink(missing_ok=True)
            TOOL_LOG_PATH.replace(rotated)
        with TOOL_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(j(record) + "\n")
    except OSError:
        _TOOL_LOGGER.exception("Не удалось записать журнал инструмента")
    _TOOL_LOGGER.info("MCP_TOOL %s", j({k: record[k] for k in ("sequence", "call_id", "tool", "status", "duration_ms", "error") if k in record}))
    return record


def _register_logged_tool(fn, **tool_options):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        started = time.monotonic()
        try:
            try:
                logged_args = dict(inspect.signature(fn).bind_partial(*args, **kwargs).arguments)
            except (TypeError, ValueError):
                logged_args = dict(kwargs)
            result = await fn(*args, **kwargs)
            status = "success"
            parsed_result = _workflow_parse_result(result) if "_workflow_parse_result" in globals() else result
            if isinstance(parsed_result, dict) and parsed_result.get("success") is False:
                status = "needs_confirmation" if parsed_result.get("confirmation_required") else "error"
            elif isinstance(parsed_result, dict) and int(parsed_result.get("failed", 0) or 0) > 0:
                status = "partial_error"
            _append_tool_log(fn.__name__, logged_args, status,
                             int((time.monotonic() - started) * 1000), result=result)
            return result
        except Exception as exc:
            try:
                logged_args = dict(inspect.signature(fn).bind_partial(*args, **kwargs).arguments)
            except (TypeError, ValueError):
                logged_args = dict(kwargs)
            error_record = _append_tool_log(fn.__name__, logged_args, "error",
                                            int((time.monotonic() - started) * 1000),
                                            error=f"{type(exc).__name__}: {exc}")
            _TOOL_LOGGER.exception("MCP_TOOL_EXCEPTION call_id=%s tool=%s",
                                   error_record.get("call_id"), fn.__name__)
            raise
    TOOL_REGISTRY[fn.__name__] = wrapper
    return mcp.tool(**tool_options)(wrapper)


def logged_tool(**tool_options):
    """Регистрирует MCP-инструмент и автоматически пишет журнал каждого вызова."""
    def decorator(fn):
        return _register_logged_tool(fn, **tool_options)
    return decorator

_load_tool_logs()

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
    """Инструмент записи: блокируется при TG_READ_ONLY=1 и автоматически логируется."""
    WRITE_TOOL_NAMES.add(fn.__name__)

    @functools.wraps(fn)
    async def guarded(*args, **kwargs):
        if READ_ONLY:
            return "Режим только чтения (TG_READ_ONLY=1): действие запрещено."
        return await fn(*args, **kwargs)

    return _register_logged_tool(guarded)


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
        total += sum(p.stat().st_size for p in d.iterdir() if p.is_file() and p != TOOL_LOG_PATH
                     and not p.name.startswith("tool_execution.jsonl."))
    return total


def _gc(need: int = 0):
    """Удаляет протухшие файлы; при нехватке места — самые старые."""
    now = time.time()
    for d in (STORE, PART):
        for p in list(d.iterdir()):
            if p == PART or p == TOOL_LOG_PATH or p.name.startswith("tool_execution.jsonl."):
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
                _gc(need=len(chunk))
                if _store_used() + len(chunk) > STORE_MAX:
                    raise ValueError("Недостаточно места в файловом хранилище даже после очистки старых файлов.")
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
    # Redirects обрабатываются вручную: каждый новый host проверяется на SSRF до запроса.
    async with httpx.AsyncClient(timeout=httpx.Timeout(60, read=180), follow_redirects=False) as hc:
        for _ in range(6):
            await asyncio.to_thread(_check_url, url)
            async with hc.stream("GET", url, headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.is_redirect:
                    location = r.headers.get("location")
                    if not location:
                        raise ValueError("сервер вернул redirect без Location")
                    url = urljoin(url, location)
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
    expected_size = (f.size or 0) if f else 0
    if expected_size > STORE_MAX:
        raise ValueError(f"Файл превышает лимит хранилища ({STORE_MAX // 1048576} МБ).")
    _gc(need=expected_size)
    if _store_used() + expected_size > STORE_MAX:
        raise ValueError("Недостаточно места в файловом хранилище.")
    _, p = _new_path(name)
    got = await c.download_media(m, file=str(p), progress_callback=progress)
    if not got:
        p.unlink(missing_ok=True)
        raise RuntimeError("Telegram не вернул скачанный файл.")
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
@logged_tool()
async def get_me() -> str:
    """Информация о моём аккаунте."""
    c = await tg()
    me = await c.get_me()
    return j({"id": me.id, "name": utils.get_display_name(me), "username": me.username, "phone": me.phone})


@logged_tool()
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


@logged_tool()
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


@logged_tool()
async def get_chat_info(chat: str) -> str:
    """Подробности о чате/канале/пользователе: описание, участники, username, телефон."""
    c = await tg()
    ent = await c.get_entity(peer(chat))
    info = {"id": utils.get_peer_id(ent), "type": type(ent).__name__,
            "name": utils.get_display_name(ent), "username": getattr(ent, "username", None)}
    try:
        if isinstance(ent, types.Channel):
            full = (await c(functions.channels.GetFullChannelRequest(utils.get_input_channel(ent)))).full_chat
            info.update(about=full.about, participants=full.participants_count)
        elif isinstance(ent, types.Chat):
            full = (await c(functions.messages.GetFullChatRequest(ent.id))).full_chat
            info.update(about=full.about)
        elif isinstance(ent, types.User):
            full = (await c(functions.users.GetFullUserRequest(utils.get_input_user(ent)))).full_user
            info.update(about=full.about, phone=ent.phone, bot=ent.bot)
    except Exception as e:  # noqa: BLE001
        info["note"] = f"не удалось получить полную информацию: {e}"
    return j(info)


@logged_tool()
async def read_messages(chat: str, limit: int = 30, offset_id: int = 0, min_id: int = 0,
                        before_iso: str | None = None, topic_id: int | None = None) -> str:
    """Последние сообщения чата (в хронологическом порядке).
    offset_id — брать сообщения старше этого id; min_id — только новее; before_iso — старше даты (ISO).
    topic_id — читать конкретную тему форума."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), limit=limit, offset_id=offset_id, min_id=min_id,
                                             offset_date=parse_dt(before_iso), reply_to=topic_id)]
    return j([msg(m) for m in reversed(msgs)])


@logged_tool()
async def get_message(chat: str, message_id: int) -> str:
    """Одно сообщение по id."""
    c = await tg()
    m = await c.get_messages(peer(chat), ids=message_id)
    return j(msg(m)) if m else "Сообщение не найдено."


@logged_tool()
async def get_thread_replies(chat: str, message_id: int, limit: int = 50) -> str:
    """Ответы/комментарии к сообщению (ветка, комментарии под постом канала)."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), reply_to=message_id, limit=limit)]
    return j([msg(m) for m in reversed(msgs)])


@logged_tool()
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


@logged_tool()
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


@logged_tool()
async def get_participants(chat: str, limit: int = 100, query: str = "") -> str:
    """Участники группы/канала (query — фильтр по имени)."""
    c = await tg()
    out = []
    async for u in c.iter_participants(peer(chat), limit=limit, search=query):
        out.append({"id": u.id, "name": utils.get_display_name(u), "username": u.username, "bot": u.bot})
    return j(out)


@logged_tool()
async def get_common_chats(user: str) -> str:
    """Общие чаты с пользователем."""
    c = await tg()
    r = await c(functions.messages.GetCommonChatsRequest(
        user_id=utils.get_input_user(await c.get_input_entity(peer(user))), max_id=0, limit=100))
    return j([{"id": utils.get_peer_id(x), "name": utils.get_display_name(x)} for x in r.chats])


@logged_tool()
async def list_contacts() -> str:
    """Список контактов."""
    c = await tg()
    r = await c(functions.contacts.GetContactsRequest(hash=0))
    return j([{"id": u.id, "name": utils.get_display_name(u), "username": u.username, "phone": u.phone}
              for u in r.users])


@logged_tool()
async def message_link(chat: str, message_id: int) -> str:
    """Ссылка t.me на сообщение (только каналы и супергруппы)."""
    c = await tg()
    ent = await c.get_entity(peer(chat))
    if getattr(ent, "username", None):
        return f"https://t.me/{ent.username}/{message_id}"
    if isinstance(ent, types.Channel):
        return f"https://t.me/c/{ent.id}/{message_id}"
    return "Ссылки на сообщения есть только у каналов и супергрупп."


@logged_tool()
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
    if not data:
        return "Не удалось скачать вложение."
    return j({"file_name": m.file.name if m.file else None, "mime": m.file.mime_type if m.file else None,
              "size": len(data), "base64": base64.b64encode(data).decode()})


@logged_tool(structured_output=False)
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


@logged_tool()
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


@logged_tool()
async def get_active_sessions() -> str:
    """Активные сессии аккаунта (для проверки безопасности). hash нужен для terminate_session."""
    c = await tg()
    r = await c(functions.account.GetAuthorizationsRequest())
    # Telegram session hash — signed 64-bit integer. Строка сохраняет точность в JS/JSON-клиентах MCP.
    return j([{"hash": str(a.hash), "device": a.device_model, "app": a.app_name, "ip": a.ip, "country": a.country,
               "current": a.current, "last_active": a.date_active} for a in r.authorizations])


# ---------------------------------------------------------------- черновики, отложенные, упоминания, реакции
@logged_tool()
async def list_scheduled(chat: str) -> str:
    """Отложенные (запланированные) сообщения в чате."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), scheduled=True)]
    return j([msg(m) for m in msgs])


@logged_tool()
async def list_drafts() -> str:
    """Черновики во всех чатах."""
    c = await tg()
    out = []
    async for d in c.iter_drafts():
        out.append({"chat_id": pk(d.input_entity), "chat": utils.get_display_name(d.entity) if d.entity else None,
                    "text": d.text})
    return j(out)


@logged_tool()
async def list_mentions(chat: str, limit: int = 20) -> str:
    """Сообщения, где упомянули меня (@), в чате."""
    c = await tg()
    msgs = [m async for m in c.iter_messages(peer(chat), limit=limit, filter=types.InputMessagesFilterMyMentions)]
    return j([msg(m) for m in msgs])


@logged_tool()
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
@logged_tool()
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
@logged_tool()
async def list_topics(chat: str, limit: int = 50, query: str = "") -> str:
    """Темы в группе-форуме."""
    c = await tg()
    ent = utils.get_input_channel(await c.get_input_entity(peer(chat)))
    r = await c(functions.channels.GetForumTopicsRequest(
        channel=ent, offset_date=None, offset_id=0, offset_topic=0, limit=limit, q=query or None))
    return j([{"id": t.id, "title": getattr(t, "title", None), "unread": getattr(t, "unread_count", None),
               "closed": getattr(t, "closed", None), "pinned": getattr(t, "pinned", None)} for t in r.topics])


# ---------------------------------------------------------------- ссылки-приглашения и заявки
@logged_tool()
async def list_invite_links(chat: str, revoked: bool = False) -> str:
    """Мои ссылки-приглашения в чат/канал (нужны права админа)."""
    c = await tg()
    r = await c(functions.messages.GetExportedChatInvitesRequest(
        peer=await c.get_input_entity(peer(chat)), admin_id=utils.get_input_user(await c.get_input_entity("me")),
        limit=100, revoked=revoked or None))
    return j([{"link": i.link, "title": i.title, "joined": i.usage, "limit": i.usage_limit, "revoked": i.revoked,
               "expire": i.expire_date, "needs_approval": i.request_needed} for i in r.invites])


@logged_tool()
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
@logged_tool()
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


@logged_tool()
async def channel_stats(chat: str) -> str:
    """Официальная статистика канала (подписчики, охват). Нужны права админа и канал от ~500 подписчиков."""
    c = await tg()
    ent = utils.get_input_channel(await c.get_input_entity(peer(chat)))
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


@logged_tool()
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


@logged_tool()
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


@logged_tool()
async def folder_list() -> str:
    """Список папок Telegram (id, название, число чатов, автоправила)."""
    c = await tg()
    return j([_folder_summary(f) for f in await _folders(c)])


@logged_tool()
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


@logged_tool()
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
                        archived=False, limit=200, pinned=None):
    kinds = {k.strip().lower() for k in kind.split(",") if k.strip()} if kind else None
    now = datetime.now(timezone.utc)
    limit = max(1, min(int(limit or 1), 1000))
    out = []
    async for d in c.iter_dialogs(limit=None, folder=1 if archived else 0):
        if kinds and dialog_kind(d) not in kinds:
            continue
        if name_contains and name_contains.casefold() not in (d.name or "").casefold():
            continue
        if min_unread and d.unread_count < min_unread:
            continue
        if muted is not None and _muted(d) != muted:
            continue
        if pinned is not None and bool(d.pinned) != pinned:
            continue
        if inactive_days is not None:
            # Отсутствующая дата последнего сообщения не должна ошибочно считаться неактивностью.
            if not d.date or (now - d.date).days < max(0, inactive_days):
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
                      archived: bool = False, limit: int = 100, create: bool = True,
                      dry_run: bool = True, confirm: bool = False) -> str:
    """Положить в папку все чаты по условию (например kind=bot — всех ботов). Если папки нет и create=True, она создаётся.
    kind: user, bot, group, channel (через запятую). Сначала dry_run=True, для применения dry_run=False и confirm=True."""
    c = await tg()
    if inactive_days is not None and inactive_days < 1:
        return "inactive_days должен быть положительным числом дней."
    if min_unread < 0:
        return "min_unread не может быть отрицательным."
    if kind and any(k.strip().lower() not in {"user", "bot", "group", "channel"} for k in kind.split(",") if k.strip()):
        return "kind: user, bot, group или channel (можно через запятую)."
    if not any([kind, name_contains, min_unread, inactive_days is not None, muted is not None]):
        return "Укажи хотя бы одно условие (kind, name_contains, min_unread, inactive_days, muted)."
    ds = await match_dialogs(c, kind, name_contains, min_unread, inactive_days, muted, archived, limit)
    if dry_run:
        return j({"dry_run": True, "matched": len(ds), "chats": _preview(ds),
                  "hint": "Повтори с dry_run=false и confirm=true, чтобы применить."})
    if not confirm:
        return "Изменение папки не выполнено: требуется confirm=true после проверки предварительного списка."
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
    elif action in ("pin", "unpin"):
        await retry_flood(c, functions.messages.ToggleDialogPinRequest(
            peer=types.InputDialogPeer(inp), pinned=(action == "pin")))
    elif action == "leave":
        await retry_flood(c.delete_dialog, d.entity)


@write_tool
async def bulk_chats(action: str, kind: str | None = None, name_contains: str | None = None,
                     min_unread: int = 0, inactive_days: int | None = None, muted: bool | None = None,
                     archived: bool = False, limit: int = 100, dry_run: bool = True,
                     pinned: bool | None = None, confirm: bool = False) -> str:
    """Массовые действия над чатами. action: mute, unmute, archive, unarchive, mark_read, pin, unpin, leave.
    Фильтры: kind (user, bot, group, channel), name_contains, min_unread, inactive_days, muted, pinned, archived.
    Всегда начинай с dry_run=True. Для применения dry_run=False и confirm=True. leave удаляет личный диалог или покидает группу/канал."""
    allowed = ("mute", "unmute", "archive", "unarchive", "mark_read", "pin", "unpin", "leave")
    if action not in allowed:
        return f"Недопустимое действие. Доступно: {', '.join(allowed)}."
    if inactive_days is not None and inactive_days < 1:
        return "inactive_days должен быть положительным числом дней."
    if min_unread < 0:
        return "min_unread не может быть отрицательным."
    if kind and any(k.strip().lower() not in {"user", "bot", "group", "channel"} for k in kind.split(",") if k.strip()):
        return "kind: user, bot, group или channel (можно через запятую)."
    if not any([kind, name_contains, min_unread, inactive_days is not None, muted is not None, pinned is not None]):
        return "Нужно указать хотя бы один фильтр, чтобы случайно не обработать все чаты."
    if not dry_run and not confirm:
        return "Применение не выполнено: установи confirm=true после проверки предварительного списка."
    c = await tg()
    ds = await match_dialogs(c, kind, name_contains, min_unread, inactive_days, muted, archived, limit, pinned)
    if dry_run:
        return j({"dry_run": True, "action": action, "matched": len(ds), "chats": _preview(ds),
                  "next": "Проверь список и повтори с dry_run=false, confirm=true."})
    if not ds:
        return j({"success": True, "done": 0, "total": 0, "errors": []})

    async def run(job=None):
        done, errors_out = 0, []
        for index, d in enumerate(ds, 1):
            try:
                await _apply_action(c, d, action)
                done += 1
            except Exception as exc:  # noqa: BLE001
                item = {"chat_id": d.id, "chat": d.name, "error": f"{type(exc).__name__}: {exc}"}
                errors_out.append(item)
                if job is not None:
                    job.setdefault("errors", []).append(item)
            if job is not None:
                job["progress"] = f"{index}/{len(ds)}"
            await asyncio.sleep(0.5)
        return {"success": not errors_out, "done": done, "total": len(ds), "failed": len(errors_out), "errors": errors_out}

    if len(ds) > 25:
        return j({"started": True, "job_id": spawn("bulk_chats", run), "total": len(ds), "action": action})
    return j(await run())

# ================================================================ ФОНОВЫЕ ЗАДАЧИ (статус)
@logged_tool()
async def job_status(job_id: str) -> str:
    """Статус фоновой задачи (экспорт, массовая загрузка, большая отправка...)."""
    job = JOBS.get(job_id)
    return j(_job_view(job)) if job else "Задача не найдена."


@logged_tool()
async def job_list() -> str:
    """Список недавних фоновых задач."""
    return j([{k: v for k, v in _job_view(x).items() if k in ("id", "kind", "status", "progress", "started")}
              for x in JOBS.values()])


@logged_tool()
async def job_cancel(job_id: str) -> str:
    """Отменить фоновую задачу."""
    job = JOBS.get(job_id)
    if not job:
        return "Задача не найдена."
    job["_task"].cancel()
    return "Отмена запрошена."


# ================================================================ ФАЙЛЫ НА RENDER
@logged_tool()
async def list_uploads() -> str:
    """Файлы в хранилище на сервере (загруженные через /upload, PUT, или сохранённые из Telegram)."""
    _gc()
    files = sorted(_store_files(), key=lambda p: -p.stat().st_mtime)
    return j({"used_mb": round(_store_used() / 1048576, 1), "limit_mb": STORE_MAX // 1048576,
              "ttl_hours": STORE_TTL // 3600, "files": [_file_info(p) for p in files]})


@logged_tool()
async def file_link(upload_id: str) -> str:
    """Прямая ссылка на скачивание файла из хранилища (доступна всем, у кого есть ссылка)."""
    p = _find(upload_id)
    return _url(p) if p else "Файл не найден."


@logged_tool()
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


@logged_tool()
async def file_from_url(url: str, filename: str | None = None) -> str:
    """Сервер сам скачивает файл по ссылке в хранилище (внутренние адреса запрещены). Дальше можно отправить через send_uploaded."""
    p = await _fetch_to_store(url, filename)
    return j(_file_info(p))


@logged_tool()
async def file_put_begin(filename: str) -> str:
    """Начать загрузку файла по частям (для больших файлов, когда не хватает одного вызова). Вернёт upload_id."""
    _gc()
    fid = secrets.token_hex(5)
    (PART / f"{fid}.part").write_bytes(b"")
    (PART / f"{fid}.name").write_text(_safe_name(filename), encoding="utf-8")
    return fid


@logged_tool()
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


@logged_tool()
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


@logged_tool()
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


@logged_tool()
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


@logged_tool()
async def delete_upload(upload_id: str) -> str:
    """Удалить загруженный файл с сервера."""
    p = _find(upload_id)
    if not p:
        return "Файл не найден."
    p.unlink(missing_ok=True)
    return "Удалено."


@logged_tool()
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


@logged_tool()
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


@logged_tool()
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
    inputs = [utils.get_input_user(await c.get_input_entity(peer(u))) for u in users]
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
    inputs = [utils.get_input_user(await c.get_input_entity(peer(u))) for u in users]
    if isinstance(ent, types.Channel):
        channel = utils.get_input_channel(await c.get_input_entity(peer(chat)))
        await c(functions.channels.InviteToChannelRequest(channel, inputs))
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
        channel=utils.get_input_channel(ent), participant=await c.get_input_entity(peer(user))))
    return "Сообщения удалены."


@write_tool
async def set_slow_mode(chat: str, seconds: int = 0) -> str:
    """Медленный режим в супергруппе: 0 (выкл), 10, 30, 60, 300, 900, 3600 секунд."""
    c = await tg()
    await c(functions.channels.ToggleSlowModeRequest(
        channel=utils.get_input_channel(await c.get_input_entity(peer(chat))), seconds=seconds))
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
            peer=inp, user_id=utils.get_input_user(await c.get_input_entity(peer(user))), approved=approve))
    else:
        await c(functions.messages.HideAllChatJoinRequestsRequest(peer=inp, approved=approve))
    return "Готово."


@write_tool
async def create_topic(chat: str, title: str) -> str:
    """Создать тему в группе-форуме."""
    c = await tg()
    await c(functions.channels.CreateForumTopicRequest(
        channel=utils.get_input_channel(await c.get_input_entity(peer(chat))), title=title))
    return "Тема создана."


@write_tool
async def edit_topic(chat: str, topic_id: int, title: str | None = None, closed: bool | None = None,
                     hidden: bool | None = None) -> str:
    """Изменить тему форума: название, закрыть/открыть, скрыть."""
    c = await tg()
    await c(functions.channels.EditForumTopicRequest(
        channel=utils.get_input_channel(await c.get_input_entity(peer(chat))),
        topic_id=topic_id, title=title, closed=closed, hidden=hidden))
    return "Готово."


@write_tool
async def delete_topic(chat: str, topic_id: int, confirm: bool = False) -> str:
    """Удалить тему форума вместе с историей. Нужно confirm=true."""
    if not confirm:
        return "Это удалит тему с историей. Повтори с confirm=true."
    c = await tg()
    await c(functions.channels.DeleteTopicHistoryRequest(
        channel=utils.get_input_channel(await c.get_input_entity(peer(chat))), top_msg_id=topic_id))
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
    await c(functions.contacts.DeleteContactsRequest(
        id=[utils.get_input_user(await c.get_input_entity(peer(user)))]))
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
async def terminate_session(session_hash: str, confirm: bool = False) -> str:
    """Завершить чужую Telegram-сессию. Передавай hash СТРОКОЙ из get_active_sessions (не числом — иначе теряется точность). После вызова проверяет, что сессия исчезла."""
    try:
        target_hash = int(str(session_hash).strip())
    except (TypeError, ValueError):
        return "Некорректный hash. Возьми строковое поле hash из get_active_sessions."
    if not -(2 ** 63) <= target_hash < (2 ** 63):
        return "Hash выходит за диапазон Telegram int64. Получи актуальный hash через get_active_sessions."
    c = await tg()
    current = await c(functions.account.GetAuthorizationsRequest())
    target = next((a for a in current.authorizations if int(a.hash) == target_hash), None)
    if target is None:
        return j({"success": False, "verified": False,
                  "error": "Переданный hash не найден. Возможно, клиент потерял точность числа или hash устарел. Повтори get_active_sessions и передай hash строго строкой.",
                  "active_sessions": [{"device": a.device_model, "app": a.app_name, "ip": a.ip, "country": a.country}
                                     for a in current.authorizations if not a.current]})
    if target.current:
        return j({"success": False, "verified": False, "error": "Нельзя завершить текущую сессию MCP-сервера."})
    if not confirm:
        return j({"success": False, "confirmation_required": True, "target": {
            "hash": str(target.hash), "device": target.device_model, "app": target.app_name,
            "ip": target.ip, "country": target.country, "last_active": target.date_active}})
    result = await c(functions.account.ResetAuthorizationRequest(hash=target_hash))
    if result is False:
        return j({"success": False, "verified": False, "error": "Telegram API вернул False при завершении сеанса."})
    # Telegram иногда обновляет список авторизаций не мгновенно — проверяем повторно.
    still_present = True
    for attempt in range(4):
        await asyncio.sleep(0.6 if attempt else 0.2)
        check = await c(functions.account.GetAuthorizationsRequest())
        still_present = any(int(a.hash) == target_hash for a in check.authorizations)
        if not still_present:
            break
    return j({"success": not still_present, "verified": not still_present, "hash": str(target_hash),
              "telegram_result": bool(result),
              "message": "Сеанс завершён и отсутствие подтверждено." if not still_present else
                         "Telegram принял запрос, но сеанс всё ещё отображается. Не считаю операцию успешной; проверь позже."})


# ================================================================ МАССОВЫЕ ОПЕРАЦИИ И ИНСТРУМЕНТЫ ДЛЯ АГЕНТОВ
async def _resolve_participant_targets(c, chat, users=None, query="", name_contains=None,
                                       username_contains=None, is_bot=None, limit=500):
    """Собирает участников по явным ID и/или фильтрам; не меняет данные."""
    ent = await c.get_entity(peer(chat))
    selected = {}
    for user_ref in users or []:
        user_ent = await c.get_entity(peer(user_ref))
        selected[user_ent.id] = user_ent
    if query or name_contains or username_contains or is_bot is not None:
        max_items = max(1, min(int(limit or 1), 5000))
        async for user in c.iter_participants(ent, limit=max_items, search=query or ""):
            full_name = utils.get_display_name(user) or ""
            username = getattr(user, "username", None) or ""
            if name_contains and name_contains.casefold() not in full_name.casefold():
                continue
            if username_contains and username_contains.casefold() not in username.casefold():
                continue
            if is_bot is not None and bool(getattr(user, "bot", False)) != is_bot:
                continue
            selected[user.id] = user
    return ent, list(selected.values())[:max(1, min(int(limit or 1), 5000))]


async def _apply_participant_action(c, chat_ent, user, action, hours=0, admin_title=None):
    if action == "invite":
        if isinstance(chat_ent, types.Channel):
            channel = utils.get_input_channel(await c.get_input_entity(chat_ent))
            input_user = utils.get_input_user(await c.get_input_entity(user))
            await retry_flood(c, functions.channels.InviteToChannelRequest(channel, [input_user]))
        else:
            input_user = utils.get_input_user(await c.get_input_entity(user))
            await retry_flood(c, functions.messages.AddChatUserRequest(chat_ent.id, input_user, fwd_limit=50))
    elif action == "kick":
        await retry_flood(c.kick_participant, chat_ent, user)
    elif action in ("ban", "unban"):
        await retry_flood(c.edit_permissions, chat_ent, user, view_messages=(action == "unban"))
    elif action in ("restrict", "unrestrict"):
        restricted = action == "restrict"
        until = datetime.now(timezone.utc) + timedelta(hours=hours) if restricted and hours > 0 else None
        allow = not restricted
        await retry_flood(c.edit_permissions, chat_ent, user, until_date=until,
                          send_messages=allow, send_media=allow, send_stickers=allow,
                          send_gifs=allow, send_games=allow, send_inline=allow,
                          embed_link_previews=allow, send_polls=allow)
    elif action in ("promote", "demote"):
        await retry_flood(c.edit_admin, chat_ent, user, is_admin=(action == "promote"), title=admin_title)
    elif action == "delete_history":
        if not isinstance(chat_ent, types.Channel):
            raise ValueError("Удаление истории участника поддерживается только в супергруппах/каналах.")
        await retry_flood(c, functions.channels.DeleteParticipantHistoryRequest(
            channel=utils.get_input_channel(chat_ent), participant=await c.get_input_entity(user)))


@write_tool
async def bulk_manage_participants(chat: str, action: str, users: list[str] | None = None,
                                   query: str = "", name_contains: str | None = None,
                                   username_contains: str | None = None, is_bot: bool | None = None,
                                   limit: int = 200, hours: int = 0, admin_title: str | None = None,
                                   dry_run: bool = True, confirm: bool = False) -> str:
    """Массовое управление участниками чата: invite, kick, ban, unban, restrict, unrestrict, promote, demote, delete_history.
    Цели задаются явным списком users и/или фильтрами query/name_contains/username_contains/is_bot. Сначала dry_run=True; для применения dry_run=False, confirm=True."""
    allowed = {"invite", "kick", "ban", "unban", "restrict", "unrestrict", "promote", "demote", "delete_history"}
    if action not in allowed:
        return j({"success": False, "error": f"action должен быть одним из {sorted(allowed)}"})
    if not any([users, query, name_contains, username_contains, is_bot is not None]):
        return "Укажи users или хотя бы один фильтр, чтобы не затронуть всех участников случайно."
    if users and any([query, name_contains, username_contains, is_bot is not None]):
        return "Используй либо явный список users, либо фильтры поиска, но не смешивай их в одном запуске."
    if hours < 0:
        return "hours не может быть отрицательным; 0 означает бессрочное ограничение."
    if action == "invite" and not users:
        return "Для invite нужен явный список users: поиск участников текущего чата не находит внешних пользователей."
    if users and len(users) > max(1, min(int(limit or 1), 5000)):
        return f"В списке {len(users)} пользователей, но limit={limit}. Увеличь limit до размера списка (максимум 5000), чтобы никого не пропустить."
    if action in {"kick", "ban", "delete_history", "promote", "demote"} and not dry_run and not confirm:
        return "Для этой операции требуется confirm=true после проверки предварительного списка."
    if not dry_run and not confirm:
        return "Для выполнения требуется confirm=true после проверки предварительного списка."
    c = await tg()
    chat_ent, targets = await _resolve_participant_targets(c, chat, users, query, name_contains,
                                                            username_contains, is_bot, limit)
    preview = [{"id": u.id, "name": utils.get_display_name(u), "username": getattr(u, "username", None),
                "bot": bool(getattr(u, "bot", False))} for u in targets]
    if dry_run:
        return j({"dry_run": True, "chat": utils.get_display_name(chat_ent), "action": action,
                  "matched": len(targets), "users": preview,
                  "next": "Проверь список и повтори с dry_run=false, confirm=true."})
    if not targets:
        return j({"success": True, "done": 0, "total": 0, "errors": []})

    async def run(job=None):
        done, errors_out = 0, []
        for i, user in enumerate(targets, 1):
            try:
                await _apply_participant_action(c, chat_ent, user, action, hours, admin_title)
                done += 1
            except Exception as exc:  # noqa: BLE001
                row = {"user_id": user.id, "username": getattr(user, "username", None),
                       "name": utils.get_display_name(user), "error": f"{type(exc).__name__}: {exc}"}
                errors_out.append(row)
                if job is not None:
                    job.setdefault("errors", []).append(row)
            if job is not None:
                job["progress"] = f"{i}/{len(targets)}"
            await asyncio.sleep(0.6)
        return {"success": not errors_out, "action": action, "done": done, "total": len(targets),
                "failed": len(errors_out), "errors": errors_out}

    if len(targets) > 20:
        return j({"started": True, "job_id": spawn("bulk_manage_participants", run), "total": len(targets), "action": action})
    return j(await run())


@write_tool
async def bulk_delete_messages(chat: str, message_ids: list[int] | None = None, query: str = "",
                               from_user: str | None = None, media_filter: str | None = None,
                               before_iso: str | None = None, after_iso: str | None = None,
                               limit: int = 100, for_everyone: bool = True,
                               dry_run: bool = True, confirm: bool = False) -> str:
    """Найти и пакетно удалить сообщения в одном чате по ID, тексту, отправителю, типу медиа и датам. По умолчанию только preview; для удаления dry_run=false и confirm=true."""
    if not any([message_ids, query, from_user, media_filter, before_iso, after_iso]):
        return "Укажи ID сообщений или хотя бы один фильтр (query/from_user/media_filter/дата)."
    if media_filter and media_filter not in FILTERS:
        return f"Неизвестный media_filter. Доступно: {', '.join(FILTERS)}"
    if message_ids and any([query, from_user, media_filter, before_iso, after_iso]):
        return "Передавай либо явный список message_ids, либо фильтры поиска — не смешивай два режима."
    if message_ids and len(set(int(x) for x in message_ids)) > 1000:
        return "За один запуск можно передать максимум 1000 message_ids."
    c = await tg()
    target = peer(chat)
    msgs = []
    if message_ids:
        requested_ids = list(dict.fromkeys(int(x) for x in message_ids))[:1000]
        for offset in range(0, len(requested_ids), 100):
            batch = await c.get_messages(target, ids=requested_ids[offset:offset + 100])
            if batch:
                msgs.extend(batch if isinstance(batch, list) else [batch])
    else:
        kwargs = {"limit": max(1, min(int(limit or 1), 1000))}
        if query:
            kwargs["search"] = query
        if from_user:
            kwargs["from_user"] = peer(from_user)
        if media_filter:
            kwargs["filter"] = FILTERS[media_filter]
        async for item in c.iter_messages(target, **kwargs):
            if before_iso and item.date and item.date > parse_dt(before_iso):
                continue
            if after_iso and item.date and item.date < parse_dt(after_iso):
                continue
            msgs.append(item)
    previews = [{"id": m.id, "date": m.date.isoformat() if m.date else None,
                 "from_id": m.sender_id, "text": (m.message or "")[:100], "media": media_kind(m)} for m in msgs]
    if dry_run:
        return j({"dry_run": True, "chat": str(chat), "matched": len(msgs), "messages": previews,
                  "for_everyone": for_everyone, "next": "Проверь сообщения и повтори с dry_run=false, confirm=true."})
    if not confirm:
        return "Удаление не выполнено: требуется confirm=true."
    ids = [m.id for m in msgs]
    deleted, errors_out = [], []
    for i in range(0, len(ids), 100):
        batch = ids[i:i + 100]
        try:
            await retry_flood(c.delete_messages, target, batch, revoke=for_everyone)
            deleted.extend(batch)
        except Exception as exc:  # noqa: BLE001
            errors_out.append({"message_ids": batch, "error": f"{type(exc).__name__}: {exc}"})
    return j({"success": not errors_out, "deleted": len(deleted), "failed_batches": len(errors_out),
              "message_ids": deleted, "errors": errors_out})


@write_tool
async def bulk_send_message(chats: list[str], text: str, parse_mode: str | None = None,
                            silent: bool = False, delay_seconds: float = 2.0,
                            dry_run: bool = True, confirm: bool = False) -> str:
    """Отправить одно сообщение в явно указанный список чатов. Не ищет получателей автоматически; максимум 50 адресатов, задержка не меньше 1.5 сек. Сначала dry_run=True."""
    targets = list(dict.fromkeys(str(x).strip() for x in chats or [] if str(x).strip()))
    if not targets:
        return "Список chats пуст."
    if len(targets) > 50:
        return "За один запуск можно указать максимум 50 чатов. Разбей список на несколько подтверждённых операций."
    delay_seconds = max(1.5, min(float(delay_seconds), 30.0))
    if dry_run:
        return j({"dry_run": True, "recipients": targets, "count": len(targets),
                  "text_chars": len(text), "delay_seconds": delay_seconds,
                  "next": "Проверь получателей и повтори с dry_run=false, confirm=true."})
    if not confirm:
        return "Рассылка не выполнена: требуется confirm=true."
    c = await tg()

    async def run(job=None):
        sent, errors_out = [], []
        for i, target in enumerate(targets, 1):
            try:
                m = await retry_flood(c.send_message, peer(target), text, parse_mode=parse_mode,
                                      silent=silent, link_preview=True)
                sent.append({"chat": target, "message_id": m.id})
            except Exception as exc:  # noqa: BLE001
                errors_out.append({"chat": target, "error": f"{type(exc).__name__}: {exc}"})
            if job is not None:
                job["progress"] = f"{i}/{len(targets)}"
            if i < len(targets):
                await asyncio.sleep(delay_seconds)
        return {"success": not errors_out, "sent": len(sent), "failed": len(errors_out),
                "results": sent, "errors": errors_out}

    if len(targets) > 10:
        return j({"started": True, "job_id": spawn("bulk_send_message", run), "total": len(targets)})
    return j(await run())


@write_tool
async def bulk_forward_messages(from_chat: str, to_chats: list[str], message_ids: list[int],
                                hide_sender: bool = False, delay_seconds: float = 2.0,
                                dry_run: bool = True, confirm: bool = False) -> str:
    """Переслать выбранные сообщения в явно указанные чаты. Preview обязателен перед выполнением."""
    destinations = list(dict.fromkeys(str(x).strip() for x in to_chats or [] if str(x).strip()))
    ids = list(dict.fromkeys(int(x) for x in message_ids or []))
    if not destinations or not ids:
        return "Нужны непустые to_chats и message_ids."
    if len(destinations) > 50 or len(ids) > 100:
        return "Ограничение: максимум 50 чатов и 100 сообщений за один запуск."
    delay_seconds = max(1.5, min(float(delay_seconds), 30.0))
    if dry_run:
        return j({"dry_run": True, "from_chat": from_chat, "to_chats": destinations,
                  "message_ids": ids, "hide_sender": hide_sender, "next": "Для выполнения dry_run=false, confirm=true."})
    if not confirm:
        return "Пересылка не выполнена: требуется confirm=true."
    c = await tg()

    async def run(job=None):
        done, errors_out = [], []
        for i, dest in enumerate(destinations, 1):
            try:
                r = await retry_flood(c.forward_messages, peer(dest), ids, from_peer=peer(from_chat),
                                      drop_author=hide_sender or None)
                done.append({"chat": dest, "messages": len(r) if isinstance(r, list) else 1})
            except Exception as exc:
                errors_out.append({"chat": dest, "error": f"{type(exc).__name__}: {exc}"})
            if job is not None:
                job["progress"] = f"{i}/{len(destinations)}"
            if i < len(destinations):
                await asyncio.sleep(delay_seconds)
        return {"success": not errors_out, "sent_to": len(done), "failed": len(errors_out),
                "results": done, "errors": errors_out}

    if len(destinations) > 10:
        return j({"started": True, "job_id": spawn("bulk_forward_messages", run), "total": len(destinations)})
    return j(await run())


@write_tool
async def bulk_manage_messages(chat: str, action: str, message_ids: list[int],
                               reaction: str = "👍", for_everyone: bool = True,
                               dry_run: bool = True, confirm: bool = False) -> str:
    """Пакетно управлять явным списком сообщений одного чата: delete, pin, unpin, react, unreact. Для изменения сначала dry_run=True, затем dry_run=false и confirm=true."""
    allowed = {"delete", "pin", "unpin", "react", "unreact"}
    if action not in allowed:
        return f"action должен быть одним из {sorted(allowed)}"
    ids = list(dict.fromkeys(int(x) for x in message_ids or []))
    if len(ids) > 500:
        return "За один запуск можно передать максимум 500 message_ids."
    if not ids:
        return "message_ids пуст."
    c = await tg()
    found = await c.get_messages(peer(chat), ids=ids)
    found = [m for m in (found if isinstance(found, list) else [found]) if m]
    valid_ids = [m.id for m in found]
    preview = [{"id": m.id, "text": (m.message or "")[:100], "media": media_kind(m),
                "pinned": bool(m.pinned)} for m in found]
    if dry_run:
        return j({"dry_run": True, "action": action, "chat": str(chat), "matched": len(found),
                  "messages": preview, "next": "Проверь список и повтори с dry_run=false, confirm=true."})
    if not confirm:
        return "Операция не выполнена: требуется confirm=true."
    done, errors_out = [], []
    if action == "delete":
        for i in range(0, len(valid_ids), 100):
            try:
                await retry_flood(c.delete_messages, peer(chat), valid_ids[i:i+100], revoke=for_everyone)
                done.extend(valid_ids[i:i+100])
            except Exception as exc:
                errors_out.append({"message_ids": valid_ids[i:i+100], "error": f"{type(exc).__name__}: {exc}"})
    else:
        inp = await c.get_input_entity(peer(chat))
        for i, mid in enumerate(valid_ids, 1):
            try:
                if action == "pin":
                    await retry_flood(c.pin_message, peer(chat), mid, notify=False)
                elif action == "unpin":
                    await retry_flood(c.unpin_message, peer(chat), mid)
                else:
                    em = [] if action == "unreact" else [types.ReactionEmoji(emoticon=reaction)]
                    await retry_flood(c, functions.messages.SendReactionRequest(peer=inp, msg_id=mid, reaction=em))
                done.append(mid)
            except Exception as exc:
                errors_out.append({"message_id": mid, "error": f"{type(exc).__name__}: {exc}"})
            if i < len(valid_ids):
                await asyncio.sleep(0.3)
    return j({"success": not errors_out, "action": action, "done": len(done),
              "failed": len(errors_out), "message_ids": done, "errors": errors_out})


@write_tool
async def terminate_sessions(session_hashes: list[str], confirm: bool = False) -> str:
    """Пакетно завершить явно перечисленные Telegram-сессии по строковым hash из get_active_sessions. Текущая сессия сервера защищена; результат проверяется повторным запросом к Telegram."""
    requested = list(dict.fromkeys(str(x).strip() for x in session_hashes or [] if str(x).strip()))
    if not requested:
        return "Укажи session_hashes — список строковых hash из get_active_sessions."
    c = await tg()
    current = await c(functions.account.GetAuthorizationsRequest())
    by_hash = {str(a.hash): a for a in current.authorizations}
    targets, skipped = [], []
    for raw in requested:
        try:
            parsed_hash = int(raw)
            if not -(2 ** 63) <= parsed_hash < (2 ** 63):
                raise ValueError("int64 range")
            key = str(parsed_hash)
        except ValueError:
            skipped.append({"hash": raw, "reason": "invalid_hash"})
            continue
        auth = by_hash.get(key)
        if not auth:
            skipped.append({"hash": key, "reason": "not_found_or_already_closed"})
        elif auth.current:
            skipped.append({"hash": key, "reason": "current_server_session_protected"})
        else:
            targets.append(auth)
    preview = [{"hash": str(a.hash), "device": a.device_model, "app": a.app_name,
                "ip": a.ip, "country": a.country} for a in targets]
    if not confirm:
        return j({"success": False, "confirmation_required": True, "targets": preview, "skipped": skipped})
    closed, failed = [], []
    for auth in targets:
        h = int(auth.hash)
        try:
            result = await retry_flood(c, functions.account.ResetAuthorizationRequest(hash=h))
            if result is False:
                raise RuntimeError("Telegram API вернул False")
            closed.append(str(h))
        except Exception as exc:
            failed.append({"hash": str(h), "error": f"{type(exc).__name__}: {exc}"})
    still = set(closed)
    for attempt in range(4):
        await asyncio.sleep(0.6 if attempt else 0.2)
        verify = await retry_flood(c, functions.account.GetAuthorizationsRequest())
        active_now = {str(a.hash) for a in verify.authorizations}
        still = {h for h in still if h in active_now}
        if not still:
            break
    verified_closed = [h for h in closed if h not in still]
    not_verified = [h for h in closed if h in still]
    return j({"success": not failed and not not_verified and not skipped, "requested": len(requested),
              "verified_closed": verified_closed, "not_verified": not_verified,
              "failed": failed, "skipped": skipped})


@write_tool
async def bulk_manage_contacts(action: str, users: list[str],
                               dry_run: bool = True, confirm: bool = False) -> str:
    """Пакетно удалить контакты или заблокировать/разблокировать пользователей: delete, block, unblock. Принимает только явный список users; сначала dry_run=True."""
    if action not in {"delete", "block", "unblock"}:
        return "action: delete, block или unblock."
    targets = list(dict.fromkeys(str(x).strip() for x in users or [] if str(x).strip()))
    if not targets:
        return "Список users пуст."
    if len(targets) > 200:
        return "Максимум 200 пользователей за один запуск."
    if dry_run:
        return j({"dry_run": True, "action": action, "users": targets, "count": len(targets),
                  "next": "Проверь список и повтори с dry_run=false, confirm=true."})
    if not confirm:
        return "Операция не выполнена: требуется confirm=true."
    c = await tg()
    done, errors_out = [], []
    if action == "delete":
        for i in range(0, len(targets), 50):
            batch = targets[i:i + 50]
            try:
                inputs = [utils.get_input_user(await c.get_input_entity(peer(x))) for x in batch]
                await retry_flood(c, functions.contacts.DeleteContactsRequest(id=inputs))
                done.extend(batch)
            except Exception as exc:  # noqa: BLE001
                errors_out.append({"users": batch, "error": f"{type(exc).__name__}: {exc}"})
    else:
        req = functions.contacts.BlockRequest if action == "block" else functions.contacts.UnblockRequest
        for i, user in enumerate(targets, 1):
            try:
                inp = await c.get_input_entity(peer(user))
                await retry_flood(c, req(id=inp))
                done.append(user)
            except Exception as exc:
                errors_out.append({"user": user, "error": f"{type(exc).__name__}: {exc}"})
            if i < len(targets):
                await asyncio.sleep(0.3)
    return j({"success": not errors_out, "action": action, "done": len(done),
              "failed": len(errors_out), "users": done, "errors": errors_out})


# ---------------------------------------------------------------- Интеграция Render Logs API
async def _render_request(path: str, params=None):
    if not RENDER_API_KEY:
        raise RuntimeError("Не задан RENDER_API_KEY в переменных окружения Render.")
    headers = {"Authorization": f"Bearer {RENDER_API_KEY}", "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30, connect=10)) as client:
        for attempt in range(3):
            response = await client.get(f"{RENDER_API_BASE}{path}", headers=headers, params=params)
            if response.status_code == 429 and attempt < 2:
                retry_after = response.headers.get("Retry-After", "2")
                try:
                    wait_seconds = max(1, min(int(retry_after), 15))
                except ValueError:
                    wait_seconds = 2
                await asyncio.sleep(wait_seconds)
                continue
            if response.status_code >= 400:
                detail = response.text[:1000]
                raise RuntimeError(f"Render API HTTP {response.status_code}: {detail}")
            return response.json()
    raise RuntimeError("Render API: превышено ограничение запросов после повторных попыток.")


async def _fetch_render_logs(minutes=60, max_logs=100, level=None, text_filter=None,
                             log_type=None, resource_id=None):
    owner = RENDER_OWNER_ID
    resource = (resource_id or RENDER_SERVICE_ID).strip()
    if not owner:
        raise RuntimeError("Не задан RENDER_OWNER_ID в переменных окружения Render.")
    if not resource:
        raise RuntimeError("Не задан RENDER_SERVICE_ID и не передан resource_id.")
    minutes = max(1, min(int(minutes or 60), 24 * 60))
    max_logs = max(1, min(int(max_logs or 100), 500))
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    params_base = [("ownerId", owner), ("startTime", start.isoformat()), ("endTime", end.isoformat()),
                   ("direction", "backward"), ("resource", resource)]
    if level:
        for item in (x.strip() for x in level.split(",")):
            if item:
                params_base.append(("level", item))
    if text_filter:
        params_base.append(("text", text_filter))
    if log_type:
        for item in (x.strip() for x in log_type.split(",")):
            if item:
                params_base.append(("type", item))
    collected, has_more, pages = [], False, 0
    next_start, next_end = start.isoformat(), end.isoformat()
    while len(collected) < max_logs and pages < 10:
        params = [(k, v) for k, v in params_base if k not in ("startTime", "endTime")]
        params.extend([("startTime", next_start), ("endTime", next_end), ("limit", min(100, max_logs - len(collected)))])
        payload = await _render_request("/logs", params=params)
        if isinstance(payload, list):
            rows = payload
            has_more = False
        else:
            rows = payload.get("logs", []) or []
            has_more = bool(payload.get("hasMore", False))
        collected.extend(rows)
        pages += 1
        if not has_more or not rows or not isinstance(payload, dict):
            break
        next_start = payload.get("nextStartTime") or next_start
        next_end = payload.get("nextEndTime") or next_end
        if not payload.get("nextStartTime") and not payload.get("nextEndTime"):
            break
    return {"resource_id": resource, "owner_id": owner, "window_minutes": minutes,
            "start_time": start.isoformat(), "end_time": end.isoformat(),
            "count": len(collected[:max_logs]), "has_more": has_more, "pages": pages,
            "logs": collected[:max_logs]}


@logged_tool()
async def render_get_logs(minutes: int = 60, max_logs: int = 100, level: str | None = None,
                          text_filter: str | None = None, log_type: str = "app",
                          resource_id: str | None = None) -> str:
    """Получить реальные логи Render за последние N минут (по умолчанию час). Фильтры level=error,warning; text_filter; log_type=app,request,build. Нужны RENDER_API_KEY, RENDER_OWNER_ID, RENDER_SERVICE_ID."""
    return j(await _fetch_render_logs(minutes, max_logs, level, text_filter, log_type, resource_id))


@logged_tool()
async def render_get_log_values(label: str = "level", minutes: int = 60,
                                resource_id: str | None = None) -> str:
    """Получить доступные значения меток логов Render (level, type, statusCode, method, host, instance) за выбранный интервал. Полезно перед фильтрацией render_get_logs."""
    allowed = {"level", "type", "statusCode", "method", "host", "instance"}
    if label not in allowed:
        return f"label должен быть одним из {sorted(allowed)}"
    owner = RENDER_OWNER_ID
    resource = (resource_id or RENDER_SERVICE_ID).strip()
    if not owner or not resource:
        raise RuntimeError("Для Render log values нужны RENDER_OWNER_ID и RENDER_SERVICE_ID (или resource_id).")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=max(1, min(int(minutes), 24 * 60)))
    payload = await _render_request("/logs/values", params=[
        ("ownerId", owner), ("label", label), ("startTime", start.isoformat()),
        ("endTime", end.isoformat()), ("direction", "backward"), ("resource", resource),
    ])
    return j({"label": label, "values": payload, "resource_id": resource,
              "start_time": start.isoformat(), "end_time": end.isoformat()})


@logged_tool()
async def render_list_services(limit: int = 100) -> str:
    """Получить сервисы Render и их ID, чтобы настроить RENDER_SERVICE_ID. Нужен RENDER_API_KEY."""
    payload = await _render_request("/services", params={"limit": max(1, min(int(limit), 100))})
    rows = payload if isinstance(payload, list) else payload.get("services", payload.get("items", []))
    out = []
    for item in rows:
        service = item.get("service", item) if isinstance(item, dict) else {}
        if not isinstance(service, dict):
            continue
        out.append({"id": service.get("id"), "name": service.get("name"),
                    "type": service.get("type"), "owner_id": service.get("ownerId") or service.get("owner_id"),
                    "suspended": service.get("suspended"), "created_at": service.get("createdAt")})
    return j(out)


@logged_tool()
async def render_get_service_events(minutes: int = 60, limit: int = 100,
                                    service_id: str | None = None) -> str:
    """Получить события деплоя/перезапусков указанного Render-сервиса за последние N минут."""
    sid = (service_id or RENDER_SERVICE_ID).strip()
    if not sid:
        return "Не задан RENDER_SERVICE_ID и не передан service_id."
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=max(1, min(int(minutes), 24 * 60)))
    payload = await _render_request(f"/services/{quote(sid, safe='')}/events", params={
        "startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "endTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"), "limit": max(1, min(int(limit), 100))})
    return j({"service_id": sid, "start_time": start.isoformat(), "end_time": end.isoformat(), "events": payload})


@logged_tool()
async def render_diagnostics(minutes: int = 60, max_logs: int = 150,
                             log_type: str = "app,request,build") -> str:
    """Диагностика Render: получает контекстные логи сервиса за окно времени (по умолчанию последний час), события не фильтрует только по уровню, чтобы не скрыть первопричину; добавляет ошибки MCP-инструментов."""
    render_data = await _fetch_render_logs(minutes, max_logs, log_type=log_type)
    local_errors = [x for x in TOOL_LOGS if x.get("status") in ("error", "partial_error")][-50:]
    return j({"render": render_data, "mcp_tool_errors": local_errors,
              "hint": "Логи Render включают контекст до и после ошибки. Сопоставь timestamp, tool, call_id и текст ошибки; для сужения вызови render_get_logs с level или text_filter."})


# ---------------------------------------------------------------- Журнал вызовов и каталог инструментов
@logged_tool()
async def get_tool_logs(limit: int = 50, tool_name: str | None = None,
                        status: str | None = None, since_minutes: int | None = None) -> str:
    """Журнал выполнений MCP-инструментов: время, имя, параметры (секреты скрыты), статус, длительность, результат/ошибка."""
    rows = list(TOOL_LOGS)
    if tool_name:
        rows = [x for x in rows if x.get("tool") == tool_name]
    if status:
        rows = [x for x in rows if x.get("status") == status]
    if since_minutes is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(0, since_minutes))
        rows = [x for x in rows if parse_dt(x.get("timestamp")) and parse_dt(x.get("timestamp")) >= cutoff]
    rows = rows[-max(1, min(int(limit or 50), 300)):]
    return j({"count": len(rows), "logs": rows})


@logged_tool()
async def agent_guide() -> str:
    """Краткая инструкция для LLM-агента по безопасному и эффективному использованию Telegram MCP: цепочки вызовов, dry-run, фоновые задачи, логи и диагностика."""
    return j({
        "workflow": {
            "tool": "run_tool_workflow",
            "format": "steps_json — JSON-массив шагов {id,tool,args,for_each,item_var}",
            "references": ["${step_id}", "${step_id.result.field}", "${step_id.result[0].field}"],
            "foreach": "for_each принимает ссылку на массив, item_var (по умолчанию item) задаёт имя элемента; максимум 100 элементов на шаг; максимум 20 шагов.",
            "execution": "Сначала dry_run=true, затем после согласования dry_run=false и confirm=true. stop_on_error=true останавливает цепочку при первой ошибке."
        },
        "bulk_operations": {
            "preview_first": True,
            "tools": ["bulk_chats", "bulk_manage_participants", "bulk_manage_messages",
                      "bulk_delete_messages", "bulk_send_message", "bulk_forward_messages",
                      "bulk_manage_contacts", "terminate_sessions", "folder_fill"],
            "rule": "Проверь matched/count и список целей в preview; для изменения передавай confirm=true."
        },
        "background_jobs": {"tools": ["job_list", "job_status", "job_cancel"],
                             "rule": "Если инструмент вернул job_id, не считай операцию завершённой — опрашивай job_status до done/error/cancelled."},
        "logging": {"tools": ["get_tool_logs", "render_get_logs", "render_get_log_values", "render_get_service_events", "render_diagnostics"],
                    "rule": "После ошибки сначала смотри get_tool_logs; затем render_diagnostics (последние 60 минут по умолчанию); при необходимости render_get_logs без фильтра level для полного контекста."},
        "sessions": {"rule": "Всегда передавай session hash как строку из get_active_sessions. Успех terminate_session/terminate_sessions подтверждается повторной проверкой через Telegram API."},
        "limits": {"workflow_steps": 20, "foreach_items": 100, "bulk_chat_actions": 1000,
                   "bulk_participants": 5000, "bulk_message_ids": 500, "bulk_send_recipients": 50},
        "runtime": {"read_only": READ_ONLY, "render_api_configured": bool(RENDER_API_KEY and RENDER_OWNER_ID and RENDER_SERVICE_ID)}
    })


@logged_tool()
async def agent_runtime_info() -> str:
    """Состояние MCP для агента: подключение Telegram, режим доступа, каталог инструментов, фоновые задачи и готовность Render API без раскрытия секретов."""
    status = {"telegram_connected": False, "telegram_authorized": False, "telegram_error": None}
    try:
        c = await tg()
        status["telegram_connected"] = bool(c.is_connected())
        status["telegram_authorized"] = bool(await c.is_user_authorized())
    except Exception as exc:  # noqa: BLE001
        status["telegram_error"] = f"{type(exc).__name__}: {exc}"
    return j({"status": status, "read_only": READ_ONLY, "registered_tools": len(TOOL_REGISTRY),
              "write_tools": len(WRITE_TOOL_NAMES), "recent_jobs": [
                  {k: v for k, v in _job_view(job).items() if k in ("id", "kind", "status", "progress", "started")}
                  for job in list(JOBS.values())[-10:]],
              "tool_log_records_in_memory": len(TOOL_LOGS),
              "render_api_configured": bool(RENDER_API_KEY and RENDER_OWNER_ID and RENDER_SERVICE_ID),
              "render_owner_id_set": bool(RENDER_OWNER_ID), "render_service_id_set": bool(RENDER_SERVICE_ID)})


@logged_tool()
async def tool_catalog(include_descriptions: bool = True) -> str:
    """Динамический каталог доступных MCP-инструментов: имя, аргументы, read/write, описание. Удобно для планирования агентом цепочек действий."""
    out = []
    for name, fn in sorted(TOOL_REGISTRY.items()):
        try:
            signature = str(inspect.signature(fn))
        except (TypeError, ValueError):
            signature = "()"
        row = {"name": name, "signature": signature,
                "mode": "write" if name in WRITE_TOOL_NAMES else "read_or_utility"}
        if include_descriptions:
            row["description"] = inspect.getdoc(fn) or ""
        out.append(row)
    return j({"count": len(out), "tools": out,
              "workflow_tool": "run_tool_workflow", "log_tool": "get_tool_logs",
              "render_log_tool": "render_get_logs", "render_log_values_tool": "render_get_log_values"})


def _workflow_parse_result(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _workflow_execution_log(tool_name: str, after_sequence: int):
    return next((row for row in reversed(TOOL_LOGS)
                 if row.get("sequence", 0) > after_sequence and row.get("tool") == tool_name), None)


def _workflow_lookup(path: str, context: dict):
    parts = path.split(".")
    root = parts.pop(0)
    if root not in context:
        raise ValueError(f"Ссылка на неизвестный шаг workflow: {root}")
    value = context[root]
    for part_index, part in enumerate(parts):
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?:\[(\d+)\])?", part)
        if not match:
            raise ValueError(f"Некорректный путь ссылки: {path}")
        key, idx = match.groups()
        if part_index == 0 and key == "result":
            # result — псевдоним самого результата шага только в начале пути.
            if idx is not None:
                value = value[int(idx)]
            continue
        if isinstance(value, dict):
            value = value[key]
        else:
            raise ValueError(f"Нельзя получить поле {key} из {type(value).__name__}")
        if idx is not None:
            value = value[int(idx)]
    return value


def _workflow_resolve(value, context):
    token = re.compile(r"\$\{([A-Za-z0-9_-]+(?:\.[A-Za-z_][A-Za-z0-9_]*(?:\[\d+\])?)*)\}")
    if isinstance(value, dict):
        return {k: _workflow_resolve(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [_workflow_resolve(x, context) for x in value]
    if isinstance(value, str):
        full = token.fullmatch(value)
        if full:
            return _workflow_lookup(full.group(1), context)
        return token.sub(lambda m: str(_workflow_lookup(m.group(1), context)), value)
    return value


@logged_tool()
async def run_tool_workflow(steps_json: str, dry_run: bool = True,
                            confirm: bool = False, stop_on_error: bool = True) -> str:
    """Связать несколько MCP-инструментов в последовательный workflow. steps_json — JSON-массив объектов {id,tool,args,for_each,item_var}. Результат шага доступен как ${step_id} или ${step_id.result.field}; for_each выполняет инструмент для каждого элемента массива (максимум 100). Сначала dry_run=True; для выполнения dry_run=false и confirm=true. Максимум 20 шагов."""
    try:
        steps = json.loads(steps_json)
    except json.JSONDecodeError as exc:
        return j({"success": False, "error": f"Некорректный JSON: {exc}"})
    if not isinstance(steps, list) or not steps or len(steps) > 20:
        return "steps_json должен быть непустым JSON-массивом максимум из 20 шагов."
    normalized, ids = [], set()
    for index, raw in enumerate(steps, 1):
        if not isinstance(raw, dict) or not isinstance(raw.get("tool"), str):
            return f"Шаг {index}: ожидается объект с полем tool."
        step_id = str(raw.get("id") or f"step_{index}")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", step_id) or step_id in ids:
            return f"Шаг {index}: некорректный или повторяющийся id {step_id!r}."
        ids.add(step_id)
        tool_name = raw["tool"]
        if tool_name not in TOOL_REGISTRY or tool_name in {"run_tool_workflow", "tool_catalog"}:
            return f"Шаг {index}: инструмент {tool_name!r} отсутствует или запрещён для вложенного вызова."
        args = raw.get("args", {})
        if not isinstance(args, dict):
            return f"Шаг {index}: args должен быть JSON-объектом."
        item_var = str(raw.get("item_var") or "item")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,30}", item_var):
            return f"Шаг {index}: item_var должен быть допустимым именем переменной."
        normalized.append({"id": step_id, "tool": tool_name, "args": args,
                           "for_each": raw.get("for_each"), "item_var": item_var})
    side_effect_names = WRITE_TOOL_NAMES | {
        "file_put_begin", "file_put_chunk", "file_put_finish", "delete_upload", "purge_uploads",
        "file_from_chat", "file_from_url", "chat_media_to_store", "export_chat", "job_cancel"
    }
    writes = [x["tool"] for x in normalized if x["tool"] in side_effect_names]
    plan = [{"id": x["id"], "tool": x["tool"], "mode": "write" if x["tool"] in side_effect_names else "read_or_utility",
             "arguments": {k: _safe_log_value(k, v) for k, v in x["args"].items()},
             "for_each": x["for_each"], "item_var": x["item_var"] if x["for_each"] is not None else None}
            for x in normalized]
    if dry_run:
        return j({"dry_run": True, "steps": plan, "write_steps": writes,
                  "next": "Проверь план; для выполнения повтори с dry_run=false, confirm=true."})
    if not confirm:
        return "Workflow не запущен: требуется confirm=true после проверки плана."
    if READ_ONLY and writes:
        return j({"success": False, "error": "TG_READ_ONLY=1 блокирует изменение Telegram.", "write_steps": writes})

    context, results = {}, []
    for step in normalized:
        before_sequence = _LOG_SEQUENCE
        try:
            fn = TOOL_REGISTRY[step["tool"]]
            if step["for_each"] is None:
                args = _workflow_resolve(step["args"], context)
                before_sequence = _LOG_SEQUENCE
                result = await fn(**args)
                parsed = _workflow_parse_result(result)
                context[step["id"]] = parsed
                row = {"id": step["id"], "tool": step["tool"], "status": "success", "result": parsed}
                execution_log = _workflow_execution_log(step["tool"], before_sequence)
                if execution_log:
                    row["execution_log"] = execution_log
                results.append(row)
            else:
                items = _workflow_resolve(step["for_each"], context)
                if not isinstance(items, list):
                    raise ValueError("for_each должен разрешиться в массив.")
                if len(items) > 100:
                    raise ValueError("for_each ограничен 100 элементами за шаг.")
                item_results = []
                for item_index, item in enumerate(items):
                    local_context = dict(context)
                    local_context[step["item_var"]] = item
                    local_context["index"] = item_index
                    args = _workflow_resolve(step["args"], local_context)
                    before_sequence = _LOG_SEQUENCE
                    try:
                        result = await fn(**args)
                        parsed = _workflow_parse_result(result)
                        item_row = {"index": item_index, "status": "success", "result": parsed}
                        execution_log = _workflow_execution_log(step["tool"], before_sequence)
                        if execution_log:
                            item_row["execution_log"] = execution_log
                        item_results.append(item_row)
                    except Exception as exc:  # noqa: BLE001
                        item_row = {"index": item_index, "status": "error",
                                    "error": f"{type(exc).__name__}: {exc}"}
                        execution_log = _workflow_execution_log(step["tool"], before_sequence)
                        if execution_log:
                            item_row["execution_log"] = execution_log
                        item_results.append(item_row)
                        if stop_on_error:
                            break
                context[step["id"]] = item_results
                row = {"id": step["id"], "tool": step["tool"], "status":
                       "success" if all(x["status"] == "success" for x in item_results) and len(item_results) == len(items) else "error",
                       "processed": len(item_results), "total_items": len(items), "results": item_results}
                results.append(row)
                if row["status"] == "error" and stop_on_error:
                    break
        except Exception as exc:  # noqa: BLE001
            row = {"id": step["id"], "tool": step["tool"],
                   "status": "error", "error": f"{type(exc).__name__}: {exc}"}
            execution_log = _workflow_execution_log(step["tool"], before_sequence)
            if execution_log:
                row["execution_log"] = execution_log
            results.append(row)
            context[step["id"]] = {"error": row["error"]}
            if stop_on_error:
                break
    return j({"success": all(x["status"] == "success" for x in results) and len(results) == len(normalized),
              "completed_steps": sum(x["status"] == "success" for x in results),
              "total_steps": len(normalized), "results": results,
              "logs_available_via": "get_tool_logs"})


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
