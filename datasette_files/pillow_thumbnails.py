import asyncio
import json
import subprocess
import sys
from typing import Optional

from .base import ThumbnailGenerationError, ThumbnailGenerator, ThumbnailResult

SUPPORTED_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
    "image/tiff",
}


def _run_worker(
    file_bytes: bytes,
    max_width: int,
    max_height: int,
    max_pixels: int,
    memory_limit_bytes: int,
    timeout_seconds: float,
) -> tuple[ThumbnailResult, int]:
    metadata = json.dumps(
        {
            "max_width": max_width,
            "max_height": max_height,
            "max_pixels": max_pixels,
            "memory_limit_bytes": memory_limit_bytes,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    process = subprocess.Popen(
        [sys.executable, "-m", "datasette_files.pillow_worker"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, _ = process.communicate(
            metadata + b"\n" + file_bytes, timeout=timeout_seconds
        )
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise ThumbnailGenerationError("timeout")

    header, separator, thumbnail = stdout.partition(b"\n")
    if process.returncode or not separator:
        raise ThumbnailGenerationError("generation_failed")
    try:
        response = json.loads(header)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ThumbnailGenerationError("generation_failed")
    if not response.get("ok"):
        raise ThumbnailGenerationError(response.get("reason", "generation_failed"))
    return (
        ThumbnailResult(
            thumb_bytes=thumbnail,
            content_type=response["content_type"],
            width=response["width"],
            height=response["height"],
        ),
        response["pid"],
    )


class PillowThumbnailGenerator(ThumbnailGenerator):
    name = "pillow"
    version = "2"

    def __init__(
        self,
        *,
        max_pixels: int = 12_000_000,
        memory_limit_bytes: int = 128 * 1024 * 1024,
        timeout_seconds: float = 10.0,
    ):
        self.max_pixels = max_pixels
        self.memory_limit_bytes = memory_limit_bytes
        self.timeout_seconds = timeout_seconds
        self.last_worker_pid = None

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
        result, worker_pid = await asyncio.to_thread(
            _run_worker,
            file_bytes,
            max_width,
            max_height,
            self.max_pixels,
            self.memory_limit_bytes,
            self.timeout_seconds,
        )
        self.last_worker_pid = worker_pid
        return result
