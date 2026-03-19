import logging
from collections import defaultdict
from typing import Dict

from aiohttp import web

from .transfer import get_file_info, yield_chunks
from ..config import Config

log = logging.getLogger(__name__)
routes = web.RouteTableDef()

# Per-IP concurrent-request counter (mirrors tgfilestream's ongoing_requests)
ongoing_requests: Dict[str, int] = defaultdict(int)
REQUEST_LIMIT = 5


def _get_client_ip(req: web.Request) -> str:
    forwarded = req.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    peername = req.transport.get_extra_info("peername")
    return peername[0] if peername else "unknown"


@routes.head(r"/file/{chat_id}/{message_id}")
async def handle_head(req: web.Request) -> web.Response:
    return await _handle_request(req, head=True)


@routes.get(r"/file/{chat_id}/{message_id}")
async def handle_get(req: web.Request) -> web.StreamResponse:
    return await _handle_request(req, head=False)


async def _handle_request(req: web.Request, head: bool):
    client = req.app["client"]

    # ── optional IAM auth ──────────────────────────────────────────────────
    if Config.IAM_HEADER:
        if req.headers.get("IAM") != Config.IAM_HEADER:
            return web.Response(status=401, text="401: Unauthorized")

    # ── parse path params ──────────────────────────────────────────────────
    try:
        chat_id = int(req.match_info["chat_id"])
        message_id = int(req.match_info["message_id"])
    except ValueError:
        return web.Response(status=400, text="400: Bad Request")

    # ── fetch message ──────────────────────────────────────────────────────
    try:
        message = await client.get_messages(chat_id, message_id)
    except Exception as exc:
        log.error("Failed to get message %s/%s: %s", chat_id, message_id, exc)
        return web.Response(status=404, text="404: Not Found")

    if not message or not message.media:
        return web.Response(status=404, text="404: Not Found")

    file_name, mime_type, file_size = await get_file_info(message)
    if not file_size:
        return web.Response(status=404, text="404: Not Found")

    # ── parse Range header ─────────────────────────────────────────────────
    try:
        offset = req.http_range.start or 0
        limit = file_size if req.http_range.stop is None else req.http_range.stop
        if limit > file_size or offset < 0 or limit <= offset:
            raise ValueError("range out of bounds")
    except ValueError:
        return web.Response(
            status=416,
            text="416: Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    status = 206 if (limit - offset) != file_size else 200
    headers = {
        "Content-Type": mime_type,
        "Content-Range": f"bytes {offset}-{limit - 1}/{file_size}",
        "Content-Length": str(limit - offset),
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Accept-Ranges": "bytes",
    }

    # ── HEAD: return headers only ─────────────────────────────────────────
    if head:
        return web.Response(status=status, headers=headers)

    # ── GET: stream the body ───────────────────────────────────────────────
    ip = _get_client_ip(req)
    if ongoing_requests[ip] >= REQUEST_LIMIT:
        return web.Response(status=429, text="429: Too Many Requests")

    ongoing_requests[ip] += 1
    log.info(
        "Serving %s/%s to %s (bytes %s-%s)", chat_id, message_id, ip, offset, limit
    )

    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(req)
    try:
        async for chunk in yield_chunks(client, message, offset, limit):
            await response.write(chunk)
    except Exception as exc:
        log.error("Stream error for %s/%s: %s", chat_id, message_id, exc)
    finally:
        ongoing_requests[ip] -= 1
        await response.write_eof()

    return response
