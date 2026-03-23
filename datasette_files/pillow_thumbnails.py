import io
from typing import Optional

from .base import ThumbnailGenerator, ThumbnailResult

SUPPORTED_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
    "image/tiff",
}


class PillowThumbnailGenerator(ThumbnailGenerator):
    name = "pillow"

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
        try:
            from PIL import Image
        except ImportError:
            return None
        try:
            from PIL import ImageOps

            img = Image.open(io.BytesIO(file_bytes))
            img = ImageOps.exif_transpose(img)
            img.thumbnail((max_width, max_height), Image.LANCZOS)
            if img.mode in ("RGBA", "LA", "PA"):
                out_format = "PNG"
                out_content_type = "image/png"
            else:
                img = img.convert("RGB")
                out_format = "JPEG"
                out_content_type = "image/jpeg"
            buf = io.BytesIO()
            img.save(buf, format=out_format, quality=85)
            return ThumbnailResult(
                thumb_bytes=buf.getvalue(),
                content_type=out_content_type,
                width=img.size[0],
                height=img.size[1],
            )
        except Exception:
            return None
