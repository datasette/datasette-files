"""Resource-constrained Pillow thumbnail worker.

This module is an internal subprocess entry point. It accepts one JSON header line
followed by image bytes on stdin, then writes a JSON header and thumbnail bytes.
"""

import io
import json
import sys


def _set_memory_limit(limit: int) -> None:
    # RLIMIT_AS is reliable on Linux, where small Datasette deployments commonly
    # run. macOS accounts shared mappings in ways that make this limit unusable.
    if not sys.platform.startswith("linux") or not limit:
        return
    import resource

    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def _respond(payload: dict, body: bytes = b"") -> None:
    sys.stdout.buffer.write(
        json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n" + body
    )


def main() -> None:
    try:
        options = json.loads(sys.stdin.buffer.readline())
        _set_memory_limit(int(options["memory_limit_bytes"]))
        file_bytes = sys.stdin.buffer.read()

        from PIL import Image, ImageOps

        image = Image.open(io.BytesIO(file_bytes))
        if image.width * image.height > int(options["max_pixels"]):
            _respond({"ok": False, "reason": "too_many_pixels", "skipped": True})
            return
        image = ImageOps.exif_transpose(image)
        image.thumbnail(
            (int(options["max_width"]), int(options["max_height"])), Image.LANCZOS
        )
        if image.mode in ("RGBA", "LA", "PA"):
            output_format = "PNG"
            output_content_type = "image/png"
        else:
            image = image.convert("RGB")
            output_format = "JPEG"
            output_content_type = "image/jpeg"
        output = io.BytesIO()
        image.save(output, format=output_format, quality=85)
        _respond(
            {
                "ok": True,
                "content_type": output_content_type,
                "width": image.width,
                "height": image.height,
            },
            output.getvalue(),
        )
    except MemoryError:
        _respond({"ok": False, "reason": "memory_limit"})
    except Exception:
        _respond({"ok": False, "reason": "generation_failed"})


if __name__ == "__main__":
    main()
