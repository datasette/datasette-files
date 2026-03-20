# Path Traversal in FilesystemStorage

**Severity**: High
**Category**: Filenames and Path Traversal
**File**: `datasette_files/filesystem.py` (all methods)
**CVSSv3 estimate**: 7.5 (High)

## Summary

`FilesystemStorage` constructs file paths by joining the configured root directory with a user-influenced `path` argument using `self.root / path`. There is no validation that the resolved path remains within the root directory. An attacker who can influence the `path` parameter (directly or through database manipulation, plugins, or future API changes) can read, write, list, and delete arbitrary files on the server.

## Root Cause

Every method in `FilesystemStorage` uses this pattern without a containment check:

```python
# datasette_files/filesystem.py:27–28
async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
    target = self.root / path   # No validation!
    if not target.exists():
        return None
```

The same pattern appears in `read_file` (line 38), `stream_file` (line 43), `receive_upload` (line 80), `delete_file` (line 98), and `list_files` (line 61).

Python's `pathlib.Path` happily resolves `..` components, so `self.root / "../secret.txt"` points to a file in the root's parent directory.

### Current mitigations

In the normal upload flow, the `path` is server-generated (`{ulid}/{sanitized_filename}` at `__init__.py:466`), which prevents traversal via the upload API. However:

1. The storage methods accept arbitrary strings and are part of the public `Storage` API that plugins can call.
2. If a `path` value in the `datasette_files` database is ever corrupted or manipulated, all subsequent reads/deletes follow it.
3. The `list_files(prefix=...)` parameter is not sanitized either.

Defense in depth requires the storage layer itself to be safe regardless of caller.

## Exploit

### Reproduce with the test suite

```bash
uv run pytest tests/test_security_exploits.py::test_path_traversal_read -v
uv run pytest tests/test_security_exploits.py::test_path_traversal_write -v
uv run pytest tests/test_security_exploits.py::test_path_traversal_delete -v
uv run pytest tests/test_security_exploits.py::test_path_traversal_list -v
```

### Reading files outside the root

```python
storage = _sources["test-uploads"]

# Creates /tmp/.../uploads as root, writes secret to /tmp/.../secret.txt
content = await storage.read_file("../secret.txt")
# Returns b"TOP SECRET DATA"
```

### Writing files outside the root

```python
async def _chunks():
    yield b"MALICIOUS CONTENT"

await storage.receive_upload("../evil_file.txt", _chunks(), "text/plain")
# File is written to storage.root.parent / "evil_file.txt"
```

### Deleting files outside the root

```python
await storage.delete_file("../important.cfg")
# Deletes storage.root.parent / "important.cfg"
```

### Listing files outside the root

```python
files, _ = await storage.list_files(prefix="..")
# Returns files from the parent directory and all subdirectories
```

## Suggested Fix

Add a path containment check to a shared helper and call it from every method:

```python
# datasette_files/filesystem.py

import os

class FilesystemStorage(Storage):
    def _safe_path(self, path: str) -> Path:
        """Resolve the path and verify it stays within the root directory."""
        target = (self.root / path).resolve()
        root_resolved = self.root.resolve()
        if not str(target).startswith(str(root_resolved) + os.sep) and target != root_resolved:
            raise ValueError(f"Path traversal detected: {path}")
        return target

    async def read_file(self, path: str) -> bytes:
        target = self._safe_path(path)
        if not target.exists():
            raise FileNotFoundError(f"File not found: {path}")
        return target.read_bytes()

    async def receive_upload(self, path: str, stream, content_type: str) -> FileMetadata:
        target = self._safe_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        # ... rest of method

    async def delete_file(self, path: str) -> None:
        target = self._safe_path(path)
        # ... rest of method

    async def list_files(self, prefix: str = "", ...) -> ...:
        search_root = self._safe_path(prefix) if prefix else self.root
        # ... rest of method
```

Apply the same check to `get_file_metadata`, `stream_file`, and any other method that constructs filesystem paths.
