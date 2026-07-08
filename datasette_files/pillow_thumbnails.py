import asyncio
import json
import sys
from typing import Optional

from .base import (
    DEFAULT_THUMBNAIL_MAX_PIXELS,
    DEFAULT_THUMBNAIL_MEMORY_LIMIT_BYTES,
    ThumbnailGenerationError,
    ThumbnailGenerator,
    ThumbnailResult,
)

SUPPORTED_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
    "image/tiff",
}

_WORKER_COMMAND = [sys.executable, "-m", "datasette_files.pillow_worker"]


def _parse_worker_response(stdout: bytes, returncode) -> ThumbnailResult:
    header, separator, thumbnail = stdout.partition(b"\n")
    if returncode or not separator:
        raise ThumbnailGenerationError("generation_failed")
    try:
        response = json.loads(header)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ThumbnailGenerationError("generation_failed")
    if not response.get("ok"):
        raise ThumbnailGenerationError(
            response.get("reason", "generation_failed"),
            skipped=bool(response.get("skipped")),
        )
    return ThumbnailResult(
        thumb_bytes=thumbnail,
        content_type=response["content_type"],
        width=response["width"],
        height=response["height"],
    )


class PillowThumbnailGenerator(ThumbnailGenerator):
    name = "pillow"
    version = "2"

    def __init__(
        self,
        *,
        max_pixels: int = DEFAULT_THUMBNAIL_MAX_PIXELS,
        memory_limit_bytes: int = DEFAULT_THUMBNAIL_MEMORY_LIMIT_BYTES,
    ):
        self.max_pixels = max_pixels
        self.memory_limit_bytes = memory_limit_bytes

    async def can_generate(self, content_type: str, filename: str) -> bool:
        return content_type in SUPPORTED_CONTENT_TYPES

    async def generate(
        self,
        file_bytes: bytes,
        content_type: str,
        filename: str,
        max_width: int = 200,
        max_height: int = 200,
    ) -> Optional[ThumbnailResult]:
        header = json.dumps(
            {
                "max_width": max_width,
                "max_height": max_height,
                "max_pixels": self.max_pixels,
                "memory_limit_bytes": self.memory_limit_bytes,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        process = await asyncio.create_subprocess_exec(
            *_WORKER_COMMAND,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await process.communicate(header + b"\n" + file_bytes)
        except asyncio.CancelledError:
            # The coordinator's timeout (or a dropped request) cancelled this
            # coroutine. The worker must not outlive it: its memory belongs to
            # the concurrency slot that is about to be released.
            process.kill()
            await process.wait()
            raise
        return _parse_worker_response(stdout, process.returncode)
