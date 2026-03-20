# Missing Security Headers on File Downloads

**Severity**: Medium
**Category**: Content That Browsers Execute
**File**: `datasette_files/__init__.py` (lines 835–850)
**CVSSv3 estimate**: 5.4 (Medium)

## Summary

File download responses are missing the `X-Content-Type-Options: nosniff` header and serve all files with `Content-Disposition: inline`. This allows browsers to MIME-sniff uploaded content into executable types and renders uploaded files directly in the browser, both of which expand the attack surface for stored XSS and content injection.

## Root Cause

`_StreamingFileResponse.asgi_send()` sets only two headers:

```python
# datasette_files/__init__.py:836-839
headers = {
    "content-type": self.content_type,
    "content-disposition": f'inline; filename="{self.filename}"',
}
```

Missing:
- `X-Content-Type-Options: nosniff` — without this, browsers may ignore the `Content-Type` header and MIME-sniff the content. A file served as `text/plain` could be sniffed as `text/html` if it contains HTML-like content.
- `Content-Security-Policy` — no CSP restricts script execution in served files.
- Safe `Content-Disposition` — all files use `inline` regardless of type, so the browser renders them instead of downloading.

Additionally, the filename is not escaped in the Content-Disposition header (see below).

## Exploit

### Reproduce with the test suite

```bash
uv run pytest tests/test_security_exploits.py::test_missing_nosniff_header -v
uv run pytest tests/test_security_exploits.py::test_content_disposition_filename_injection -v
```

### Missing nosniff

```python
download = await ds.client.get(result["file"]["download_url"])
assert "x-content-type-options" not in download.headers  # PASSES — header is absent
```

Without `nosniff`, a browser receiving a file with `Content-Type: text/plain` that starts with `<html>` may decide to render it as HTML, executing any embedded scripts.

### Content-Disposition filename injection

```python
result = await _upload_file(ds, filename='evil".html', ...)
download = await ds.client.get(result["file"]["download_url"])
cd = download.headers.get("content-disposition", "")
# Produces: inline; filename="evil".html"
# The unescaped quote breaks the header syntax
```

Different browsers parse malformed Content-Disposition headers differently. Some may ignore the filename entirely, others may use a truncated version, and in edge cases the response could be interpreted as having no Content-Disposition at all (falling back to inline rendering based on Content-Type).

## Suggested Fix

```python
import re

def _safe_filename_for_header(filename: str) -> str:
    """Escape or strip characters that break Content-Disposition headers."""
    # Remove characters that can break the header
    filename = filename.replace('"', "'").replace("\\", "_")
    # Fallback if empty
    return filename or "download"

class _StreamingFileResponse:
    async def asgi_send(self, send):
        safe_name = _safe_filename_for_header(self.filename)

        # Only allow inline for known-safe image types
        if self.content_type in ("image/jpeg", "image/png", "image/gif", "image/webp"):
            disposition = f'inline; filename="{safe_name}"'
        else:
            disposition = f'attachment; filename="{safe_name}"'

        headers = {
            "content-type": self.content_type,
            "content-disposition": disposition,
            "x-content-type-options": "nosniff",
        }
        # ... rest of method
```
