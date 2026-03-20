# Stored XSS via Inline File Serving (SVG/HTML)

**Severity**: Critical
**Category**: Content That Browsers Execute
**File**: `datasette_files/__init__.py` (lines 825–853)
**CVSSv3 estimate**: 8.1 (High)

## Summary

Uploaded files are served inline from the same origin as the Datasette application. An attacker who uploads an SVG or HTML file containing JavaScript can achieve stored cross-site scripting (XSS) against any user who views the file. The embedded script runs with full access to the victim's session, cookies, localStorage, and can make authenticated API requests.

## Root Cause

`_StreamingFileResponse` sets `Content-Disposition: inline` for all file types and does not set `X-Content-Type-Options: nosniff`:

```python
# datasette_files/__init__.py:838
headers = {
    "content-type": self.content_type,
    "content-disposition": f'inline; filename="{self.filename}"',
}
```

There is no allowlist of safe content types for inline rendering. SVGs are valid XML that the SVG specification allows to contain `<script>` elements. HTML files execute JavaScript natively.

## Exploit

### Reproduce with the test suite

```bash
uv run pytest tests/test_security_exploits.py::test_xss_svg_upload_served_inline -v
uv run pytest tests/test_security_exploits.py::test_xss_html_upload_served_inline -v
```

### Step-by-step

1. An attacker with upload permission creates an SVG file:

```xml
<svg xmlns="http://www.w3.org/2000/svg">
  <script>
    fetch('/api/account/email', {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({email: 'attacker@evil.com'})
    })
  </script>
</svg>
```

2. The attacker uploads this file via the prepare/upload/complete API.

3. Any user who navigates to `/-/files/{file_id}/download` in their browser will execute the embedded JavaScript in the context of the Datasette application origin.

4. The script can:
   - Read `document.cookie` and exfiltrate session tokens
   - Make authenticated API calls (delete files, modify data, etc.)
   - Read DOM content from other pages via fetch

### What the test proves

```python
download = await ds.client.get(result["file"]["download_url"])
assert download.headers["content-type"] == "image/svg+xml"       # SVG served as SVG
assert "inline" in download.headers.get("content-disposition", "") # Rendered inline
assert "x-content-type-options" not in download.headers            # No nosniff
assert b"<script>" in download.content                             # JS in body
```

## Suggested Fix

```python
# Safe content types that can be rendered inline
_INLINE_SAFE_TYPES = frozenset({
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "text/plain", "application/pdf",
})

class _StreamingFileResponse:
    async def asgi_send(self, send):
        # Use 'attachment' for anything not proven safe
        if self.content_type in _INLINE_SAFE_TYPES:
            disposition = f'inline; filename="{self.filename}"'
        else:
            disposition = f'attachment; filename="{self.filename}"'

        headers = {
            "content-type": self.content_type,
            "content-disposition": disposition,
            "x-content-type-options": "nosniff",
        }
        # ...
```

For maximum safety, serve uploaded files from a separate origin (e.g., `uploads.yourcdn.com`) that shares no cookies with the main application.

## Additional Issue: Filename Injection in Content-Disposition

The filename is interpolated directly into the header without escaping quotes:

```python
f'inline; filename="{self.filename}"'
```

A filename like `evil".html` produces the malformed header `inline; filename="evil".html"`, which browsers may interpret unpredictably. Use RFC 5987 encoding or strip/escape quotes.

**Test**: `test_content_disposition_filename_injection`
