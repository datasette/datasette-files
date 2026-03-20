# Filename Sanitization Gaps

**Severity**: Low
**Category**: Filenames and Path Traversal
**File**: `datasette_files/__init__.py` (lines 270–276)
**CVSSv3 estimate**: 3.7 (Low)

## Summary

The `_sanitize_filename()` function replaces path separators (`/`, `\`) and strips null bytes, but does not handle `..` sequences, does not enforce a length limit, and does not strip special characters that can cause issues on certain filesystems. While the current upload flow mitigates most risk by placing files under a ULID-based directory, these gaps represent incomplete input validation.

## Root Cause

```python
# datasette_files/__init__.py:270-276
def _sanitize_filename(filename):
    """Remove path separators and other dangerous characters from a filename."""
    # Strip directory components
    filename = filename.replace("/", "_").replace("\\", "_")
    # Remove null bytes
    filename = filename.replace("\x00", "")
    return filename or "unnamed"
```

Missing:
1. **No `..` handling**: `..hidden` or `....` remain in the output. While slashes are replaced, the resulting `.._.._etc_passwd` still contains `..` sequences that could be misinterpreted by downstream systems.
2. **No length limit**: A 500+ character filename is accepted and stored on disk. Many filesystems limit filenames to 255 bytes, which could cause errors or truncation.
3. **No special character filtering**: Characters like `:` (NTFS alternate data streams), `<>|*?` (Windows reserved), or leading dots (hidden files on Unix) are preserved.

## Exploit

### Reproduce with the test suite

```bash
uv run pytest tests/test_security_exploits.py::test_filename_sanitization_preserves_dotdot -v
uv run pytest tests/test_security_exploits.py::test_filename_no_length_limit -v
uv run pytest tests/test_security_exploits.py::test_upload_accepts_any_filename -v
```

### Dot-dot preservation

```python
from datasette_files import _sanitize_filename

result = _sanitize_filename("../../../etc/passwd")
# Returns: ".._.._.._etc_passwd"
# The ".." sequences remain in the filename
```

### No length limit

```python
long_name = "A" * 500 + ".txt"
sanitized = _sanitize_filename(long_name)
assert len(sanitized) == 504  # No truncation
```

### Full upload with traversal filename

```python
result = await _upload_file(
    ds,
    filename="../../etc/passwd",
    content=b"root:x:0:0:",
    content_type="text/plain",
)
# Accepted — filename becomes ".._.._etc_passwd" on disk
assert result["file"]["filename"] == ".._.._etc_passwd"
```

## Impact

The current risk is low because:
- The upload flow places files under `{ulid}/` subdirectories, so the sanitized filename alone cannot escape the storage root.
- Path traversal via the storage layer is a separate, more severe issue (see report 02).

However, incomplete sanitization is a defense-in-depth failure. If the ULID prefix is ever removed or if filenames are used in other contexts (shell commands, log files, exports), the weak sanitization could become exploitable.

## Suggested Fix

```python
import re
import unicodedata

def _sanitize_filename(filename):
    """Remove path separators, dangerous sequences, and enforce limits."""
    # Strip directory components
    filename = filename.replace("/", "_").replace("\\", "_")
    # Remove null bytes
    filename = filename.replace("\x00", "")
    # Collapse dot-dot sequences
    filename = filename.replace("..", "_")
    # Remove characters problematic on various filesystems
    filename = re.sub(r'[<>:"|?*]', "_", filename)
    # Strip leading dots (hidden files on Unix)
    filename = filename.lstrip(".")
    # Normalize unicode
    filename = unicodedata.normalize("NFC", filename)
    # Enforce length limit (255 bytes is the common filesystem max)
    name, _, ext = filename.rpartition(".")
    if ext and name:
        max_name = 255 - len(ext.encode("utf-8")) - 1
        filename = name.encode("utf-8")[:max_name].decode("utf-8", errors="ignore") + "." + ext
    else:
        filename = filename.encode("utf-8")[:255].decode("utf-8", errors="ignore")
    return filename or "unnamed"
```
