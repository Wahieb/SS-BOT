import math
import logging
from typing import AsyncGenerator

log = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MB – matches Pyrogram's internal stream_media chunk size


async def get_file_info(message):
    """Return (file_name, mime_type, file_size) for a Pyrogram message, or (None, None, None)."""
    media = (
        message.video
        or message.document
        or message.audio
        or message.voice
        or message.video_note
        or message.animation
    )
    if not media:
        return None, None, None

    file_name = getattr(media, "file_name", None) or f"file_{message.id}"
    mime_type = getattr(media, "mime_type", None) or "application/octet-stream"
    file_size = getattr(media, "file_size", None)
    return file_name, mime_type, file_size


async def yield_chunks(
    client, message, byte_offset: int, byte_limit: int
) -> AsyncGenerator[bytes, None]:
    """Stream *byte_offset* to *byte_limit* (exclusive) from a Pyrogram message.

    Adapts Pyrogram's chunk-based ``stream_media(offset, limit)`` to byte-accurate
    range requests, mirroring the role of tgfilestream's ``ParallelTransferrer``.

    ``stream_media`` ``offset`` / ``limit`` are in CHUNK_SIZE units.
    """
    start_chunk = math.floor(byte_offset / CHUNK_SIZE)
    end_chunk = (byte_limit + CHUNK_SIZE - 1) // CHUNK_SIZE  # exclusive, integer arithmetic
    num_chunks = end_chunk - start_chunk

    first_cut = byte_offset % CHUNK_SIZE  # bytes to skip at the start of the first chunk
    last_cut = byte_limit % CHUNK_SIZE    # bytes to keep from the last chunk (0 → full chunk)
    if last_cut == 0:
        last_cut = CHUNK_SIZE

    chunk_index = 0
    async for chunk in client.stream_media(message, offset=start_chunk, limit=num_chunks):
        abs_chunk = start_chunk + chunk_index
        is_first = abs_chunk == start_chunk
        is_last = abs_chunk == (end_chunk - 1)

        if is_first and is_last:
            yield chunk[first_cut: first_cut + (byte_limit - byte_offset)]
        elif is_first:
            yield chunk[first_cut:]
        elif is_last:
            yield chunk[:last_cut]
        else:
            yield chunk

        chunk_index += 1
