# datasette-files Plugin System Design

## Context

Datasette needs first-class file support where different plugins provide different storage backends (S3, Google Drive, filesystem, Internet Archive, etc.). The `datasette-files` plugin serves as the core framework: it defines the storage plugin interface, manages the file registry in the internal database, provides UI integration (render_cell, upload components), and exposes REST APIs. Backend-specific plugins (`datasette-files-s3`, etc.) implement the actual storage operations. A **filesystem storage is built into datasette-files itself** so the plugin is immediately useful.

Nothing has shipped yet, so this is a clean-slate design.

### MVP Goal

**"Attach a new file to a row in a table."** The initial implementation targets a working end-to-end flow: configure a filesystem source, mark a column as a file column (via table actions UI), upload a file through the UI, and have it display in the table. This means building: the Storage ABC, filesystem storage, source configuration, internal DB schema, upload/serve endpoints, render_cell hook, and the upload UI integration.

### Key Dependency: Datasette's New `request.form()` API

Datasette 1.0a24 (Jan 2026) added `request.form(files=True)` — a streaming multipart parser with `UploadedFile` objects, automatic disk spilling for large files, configurable size limits, and async context manager cleanup. **This is what the filesystem upload flow will use** — the upload endpoint receives the file via `request.form(files=True)`, gets an `UploadedFile` with `.read()`, `.filename`, `.content_type`, `.size`, and passes the content to `storage.receive_upload()`.

Key file: `/tmp/datasette/datasette/utils/multipart.py`

## Terminology

Four nouns, used consistently everywhere:

| Term | Meaning | Example |
|------|---------|---------|
| **Source** | A configured connection to a file storage backend. The unit of administration and permission scoping. | "product-images" (an S3 bucket), "local-uploads" (a directory) |
| **Storage** | The *type* of backend, implemented as a plugin class. Developer-facing; users see "source". | `S3Storage`, `FilesystemStorage` |
| **File** | A managed object within a source. Has a unique ID, lives in exactly one source. | `df-01j5a3b4c5d6e7f8g9h0jkmnpq` |
| **File column** | A table column configured to hold file ID references. The bridge between relational data and file storage. | A `photo` TEXT column containing `df-...` strings |

Deliberately *not* nouns: "upload" (verb/action), "attachment" (too vague), "blob" (reserved for SQLite BLOB columns).

## Data Model

### Internal Database Tables (in `_internal`)

```sql
CREATE TABLE IF NOT EXISTS datasette_files_sources (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    storage_type TEXT NOT NULL,
    label TEXT,
    config TEXT DEFAULT '{}',         -- JSON configuration for this source
    last_sync_token TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS datasette_files (
    id TEXT PRIMARY KEY,              -- "df-{ULID}"
    source_id INTEGER NOT NULL REFERENCES datasette_files_sources(id),
    path TEXT NOT NULL,               -- path within the source
    filename TEXT NOT NULL,           -- human-readable filename
    content_type TEXT,                -- MIME type
    content_hash TEXT,                -- hash of file content (e.g. sha256), NULL if unknown
    size INTEGER,                     -- bytes
    width INTEGER,                    -- image pixels (NULL if n/a)
    height INTEGER,                   -- image pixels (NULL if n/a)
    uploaded_by TEXT,                 -- actor ID (NULL for synced files)
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    metadata TEXT DEFAULT '{}',       -- JSON
    UNIQUE(source_id, path)
);
```

No pending table for the MVP. The S3 two-phase upload flow will be designed when we implement datasette-files-s3 (likely using signed tokens rather than database state).

### File ID Format

`df-{ULID}` in lowercase, e.g. `df-01j5a3b4c5d6e7f8g9h0jkmnpq`. The `df-` prefix makes IDs instantly recognizable in columns and allows the `render_cell` hook to detect them with a simple `startswith` check.

- **Single-file column**: stores one `df-...` string.
- **Multi-file column**: stores a JSON array: `["df-01j5...", "df-01j6..."]`.

### Content Hash

The `content_hash` column stores a hash of the file content (e.g. `sha256:abcdef...`) for storages that support it. This enables:
- **Deduplication**: Detect when the same file is uploaded twice.
- **Change detection**: During sync, detect files that have changed on the backend.
- **Integrity verification**: Confirm a download matches what was uploaded.

The hash is optional — some storages (like S3 with ETags) provide this naturally, others may not.

### File Column Metadata

Configured via Datasette's existing column metadata system (`metadata_columns` table / `datasette.yaml`). **Critically, this must also be modifiable at runtime** — users designate columns as file columns via a table action in the UI, which writes to `metadata_columns`.

```yaml
databases:
  products:
    tables:
      inventory:
        columns:
          main_photo:
            file_column: "true"
            file_source: product-images     # which source for uploads
          gallery:
            file_column: "true"
            file_source: product-images
            file_multiple: "true"
            file_accept: "image/*"
          spec_sheet:
            file_column: "true"
            file_source: archive
            file_accept: "application/pdf"
            file_max_size: "20971520"
```

Keys: `file_column`, `file_source`, `file_multiple`, `file_accept`, `file_max_size`.

The table action for designating file columns uses `datasette.set_column_metadata()` to write these keys at runtime.

## Source Plugin Interface

### Core Classes (`datasette_files/base.py`)

```python
@dataclass
class FileMetadata:
    path: str
    filename: str
    content_type: Optional[str] = None
    content_hash: Optional[str] = None   # e.g. "sha256:abcdef..."
    size: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    created_at: Optional[str] = None
    metadata: dict = field(default_factory=dict)

@dataclass
class UploadInstructions:
    upload_url: str                       # URL the client should POST/PUT the file to
    upload_method: str = "POST"           # "POST" or "PUT"
    upload_headers: dict = field(default_factory=dict)
    upload_fields: dict = field(default_factory=dict)  # extra form fields for multipart

@dataclass
class StorageCapabilities:
    can_upload: bool = False
    can_delete: bool = False
    can_list: bool = False
    can_generate_signed_urls: bool = False
    can_generate_thumbnails: bool = False
    requires_proxy_download: bool = False
    max_file_size: Optional[int] = None
```

Note: `UploadInstructions` no longer has a `proxy` boolean. The `upload_url` always points to wherever the client should send the file. For filesystem storage, this points to a datasette-files upload handler route. For S3, it points to a presigned S3 URL. Storage plugins that need custom upload handling register their own routes via `register_routes()`.

### Storage ABC

```python
class Storage(ABC):
    @property
    @abstractmethod
    def storage_type(self) -> str: ...           # e.g. "s3", "filesystem"

    @property
    @abstractmethod
    def capabilities(self) -> StorageCapabilities: ...

    @abstractmethod
    async def configure(self, config: dict, get_secret) -> None: ...
        # get_secret: async (name) -> Optional[str], wraps datasette-secrets

    @abstractmethod
    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]: ...

    @abstractmethod
    async def read_file(self, path: str) -> bytes: ...

    # Optional (override based on capabilities):
    async def list_files(self, prefix="", cursor=None, limit=100)
        -> tuple[list[FileMetadata], Optional[str]]: ...
    async def download_url(self, path, expires_in=300) -> str: ...
    async def stream_file(self, path) -> AsyncIterator[bytes]: ...
    async def prepare_upload(self, filename, content_type, size)
        -> UploadInstructions: ...
    async def receive_upload(self, path, content, content_type) -> FileMetadata: ...
    async def delete_file(self, path) -> None: ...
    async def thumbnail_url(self, path, width, height) -> Optional[str]: ...
```

### Registration Hook

Plugins register **classes** (not instances) — instantiation and configuration is handled by datasette-files core:

```python
# hookspecs.py
@hookspec
def register_files_storage_types(datasette):
    "Return a list of Storage subclasses"

# In datasette-files-s3:
@hookimpl
def register_files_storage_types():
    return [S3Storage]
```

### Source Instantiation (at startup)

1. Collect all storage types via `register_files_storage_types` hook.
2. Read source definitions from both `datasette.plugin_config("datasette-files")["sources"]` AND the `datasette_files_sources` table (for runtime-created sources).
3. For each source: instantiate the storage class, call `await storage.configure(config, get_secret_func)`, upsert a `datasette_files_sources` row (with config stored in the `config` JSON column), cache the instance.

### Integration with datasette-secrets

`datasette-files` implements `register_secrets` to declare all secrets referenced across configured sources. At configure-time, each source's `get_secret` callable wraps `datasette_secrets.get_secret()`.

## Configuration

Sources can be declared in `datasette.yaml` **and/or** created at runtime via the UI or API:

```yaml
plugins:
  datasette-files:
    sources:
      product-images:
        storage: s3
        config:
          bucket: my-photos-bucket
          prefix: "uploads/"
          region: us-west-2
        secrets:
          - AWS_ACCESS_KEY_ID
          - AWS_SECRET_ACCESS_KEY
      local-docs:
        storage: filesystem
        config:
          root: /data/documents
```

Sources from `datasette.yaml` are synced to the `datasette_files_sources` table at startup. Additional sources can be created via a UI (requires `files-manage-sources` permission), which writes directly to the `datasette_files_sources` table with the `config` JSON column.

### Sync Model for Read-Only Sources

For read-only sources (Internet Archive, third-party S3 buckets, etc.), files enter the `datasette_files` registry via:

1. **Background sync on startup**: On startup, `datasette-files` calls `list_files()` on each source that has `can_list=True` and populates/updates the registry. This means files are available in the registry before anyone references them.
2. **Manual sync trigger**: `/-/files/sync/{source_slug}` endpoint (requires `files-manage-sources` permission) and a `datasette files sync {source_slug}` CLI command that admins can trigger to refresh the registry on demand.

## Permission Model

### FileSourceResource

A new top-level resource type following the existing pattern in `datasette/resources.py`:

```python
class FileSourceResource(Resource):
    name = "file-source"
    parent_class = None

    def __init__(self, source_slug: str):
        super().__init__(parent=source_slug, child=None)
```

### Actions

| Action | Resource | Default | Description |
|--------|----------|---------|-------------|
| `files-browse` | FileSourceResource | **deny** | Browse, search, view, and download files in a source |
| `files-upload` | FileSourceResource | deny | Upload files to a source |
| `files-delete` | FileSourceResource | deny | Delete files from a source |
| `files-manage-sources` | None (global) | deny | Administrative access |

All file access actions default to **deny**. Permissions must be explicitly granted via `datasette.yaml`.

### Permission Configuration

Permissions are configured in the `permissions:` block of `datasette.yaml`.

**Global (all sources):**

```yaml
permissions:
  files-browse: true                   # Allow everyone
  files-browse:
    id: alice                          # Allow only alice
```

**Per-source:**

```yaml
permissions:
  files-browse:
    public-files:
      allow: true                      # Everyone can browse public-files
    private-files:
      allow:
        id: alice                      # Only alice can browse private-files
```

The plugin implements `permission_resources_sql` to translate this config into Datasette's SQL permission system. Global rules cascade to all sources; per-source rules apply at the parent level.

### Search

The search endpoint (`/-/files/search`) uses Datasette's `allowed_resources_sql()` to get the list of sources the current actor can browse, then filters FTS and listing queries to only those sources. This means:

- Anonymous users see nothing unless explicitly granted `files-browse`
- Search results are filtered server-side — no client-side permission checks
- FTS5 indexes filename and content_type for fast text search
- The search endpoint supports both HTML and JSON (`.json` suffix)

### Composition with Table Permissions

Viewing a file through a table cell requires both `view-table` on the table AND `files-browse` on the file's source. If a user can see the table but lacks `files-browse`, the batch.json endpoint filters out inaccessible files.

### File Ownership

`uploaded_by` records the actor ID. For now this is informational. A future iteration could add ownership-based deletion rules without changing the schema.

## File Serving

### URL Scheme

```
/-/files/search                                  # Search/browse files (HTML)
/-/files/search.json?q=&source=                  # Search/browse files (JSON)
/-/files/{file_id}                               # HTML info page about the file
/-/files/{file_id}/download                      # Download file (302 redirect or stream content)
/-/files/{file_id}/thumbnail?w=200&h=200         # Thumbnail
/-/files/{file_id}.json                          # JSON metadata
/-/files/batch.json?id=df-abc&id=df-def          # Bulk file metadata
/-/files/browse/{source_slug}                    # Browse files in a source
/-/files/upload/{source_slug}                    # Upload endpoint
/-/files/sync/{source_slug}                      # Trigger sync for a source
/-/files/sources.json                            # List sources
```

### File Info Page (`/-/files/{file_id}`)

An HTML page showing file metadata: filename, content type, size, source, upload date, who uploaded it, content hash. Image files get a preview. Download link points to `/-/files/{file_id}/download`.

### Download Endpoint (`/-/files/{file_id}/download`)

1. **Signed-URL backends** (S3, GCS): Returns a **302 redirect** to a short-lived signed URL. `Cache-Control: no-cache` on the redirect forces permission re-checks.
2. **Proxy backends** (filesystem, blob): Datasette streams the file content directly. `Cache-Control: private, max-age=3600` with ETag based on file ID (files are immutable).

### Thumbnails

Layered approach:
1. **Backend-native**: If storage implements `thumbnail_url()`, use it (e.g. Imgix, Cloudflare Images).
2. **Datasette-generated**: Read file via `read_file()`, resize with Pillow (optional dependency), cache in `datasette_files_thumbnails` table.
3. **No thumbnail**: Non-image files get no thumbnail (or a generic icon via CSS).

## Upload Flow

For the filesystem MVP, upload is a single atomic request:

```
POST /-/files/upload/{source_slug}
Content-Type: multipart/form-data

(file data)
```

The endpoint:
1. Checks `files-upload` permission on the source.
2. Parses the upload via `request.form(files=True)`.
3. Generates a `df-{ULID}` file ID and storage path (`{ulid}/{sanitized_filename}`).
4. Calls `storage.receive_upload(path, content, content_type)` which returns `FileMetadata`.
5. Inserts the permanent record into `datasette_files`.
6. Returns: `{"file_id": "df-...", "filename": "...", "content_type": "...", "size": ..., "url": "/-/files/df-..."}`

For non-proxy backends (S3), the storage plugin registers its own upload routes via `register_routes()` and returns `UploadInstructions` with `upload_url` pointing to those routes or directly to a presigned URL. The upload web component uses `upload_url` to know where to send the file — it doesn't care whether that's a Datasette endpoint or an S3 URL.

### Path Generation

Core generates storage paths: `{ulid}/{sanitized_filename}`. Ensures uniqueness even with duplicate filenames.

## UI Integration

### render_cell Hook

Detects `df-` prefixed strings in table cells. For configured file columns, renders rich output:
- **Images**: Thumbnail with link to file info page
- **Non-images**: File icon + filename + size with link to file info page
- **Multi-file**: Grid of thumbnails/icons with "+N more" overflow

**Performance**: Batch-prefetch all file IDs on the current page in a single query to avoid N+1.

### Upload Web Component

`<file-upload>` — a source-agnostic web component (Shadow DOM) that handles:
- Drag-and-drop + file picker
- Sends file to `upload_url` (whether that's a Datasette endpoint or an S3 presigned URL)
- Progress bars, concurrent upload limiting
- Emits `file-uploaded` custom events with file ID

Used both standalone on the browse page and as a modal within table row editing.

### File Column Configuration (Table Action)

A table action ("Configure file columns") allows users to designate columns as file columns at runtime. Opens a form where you can:
- Select which columns are file columns
- Choose which source each column uploads to
- Set single/multiple, accepted types, max size

Writes to `metadata_columns` via `datasette.set_column_metadata()`.

### File Column Input Widget

`<file-column-input>` — wraps the upload component for use in table row editing:
- Shows current file(s) as previews
- "Attach file" button opens upload modal
- Multi-file columns: list of files with remove buttons
- Stores `df-...` IDs in a hidden input

### File Browser Page

`/-/files/browse/{source_slug}` — grid/list view of files in a source with pagination, upload button (permission-gated). Added to nav via `menu_links` hook.

## API Summary

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/-/files/search?q=&source=` | Search files (HTML, permission-filtered) |
| GET | `/-/files/search.json?q=&source=` | Search files (JSON, permission-filtered) |
| GET | `/-/files/sources.json` | List all sources with capabilities |
| GET | `/-/files/batch.json?id=df-abc&id=df-def` | Bulk file metadata (permission-filtered) |
| GET | `/-/files/browse/{source}.json?cursor=&limit=` | List files in source |
| GET | `/-/files/{file_id}` | HTML file info page (requires files-browse) |
| GET | `/-/files/{file_id}.json` | File metadata as JSON (requires files-browse) |
| GET | `/-/files/{file_id}/download` | Download file (requires files-browse) |
| GET | `/-/files/{file_id}/thumbnail?w=&h=` | Thumbnail |
| POST | `/-/files/upload/{source}` | Upload file (filesystem: multipart, others: returns UploadInstructions) |
| POST | `/-/files/sync/{source}` | Trigger source sync |
| DELETE | `/-/files/{file_id}` | Delete file |

## Design Principles

- **Files are immutable.** No update-in-place. Upload a new file, get a new ID. Simplifies caching and avoids versioning complexity.
- **Sources can come from config or runtime.** `datasette.yaml` for infrastructure setup, but also creatable via UI for flexibility. Both backed by the `datasette_files_sources` table.
- **Plugins register classes, not instances.** datasette-files core handles instantiation and lifecycle.
- **Upload URL is the abstraction.** Storage plugins provide `upload_url` — the client doesn't know or care whether it points to Datasette or S3. No proxy boolean needed.
- **Permission boundaries at the source level.** Fine-grained enough for most use cases without over-complicating things. File ownership is recorded for future use.
- **Column config is runtime-mutable.** Users designate file columns via table actions, not just YAML.

## Built-in Filesystem Storage

`datasette-files` ships with a `FilesystemStorage` so it's immediately useful:

```python
class FilesystemStorage(Storage):
    storage_type = "filesystem"
    capabilities = StorageCapabilities(
        can_upload=True,
        can_delete=True,
        can_list=True,
        can_generate_signed_urls=False,
        requires_proxy_download=True,
    )

    async def configure(self, config, get_secret):
        self.root = Path(config["root"])
        self.max_file_size = config.get("max_file_size")
        self.root.mkdir(parents=True, exist_ok=True)

    async def read_file(self, path):
        return (self.root / path).read_bytes()

    async def receive_upload(self, path, content, content_type):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        content_hash = "sha256:" + hashlib.sha256(content).hexdigest()
        return FileMetadata(
            path=path,
            filename=Path(path).name,
            content_type=content_type,
            content_hash=content_hash,
            size=len(content),
        )

    # etc.
```

Registered internally (not via the hook — built-in types are always available):

```python
BUILT_IN_STORAGE_TYPES = {"filesystem": FilesystemStorage}
```

## Key Files to Modify

- `datasette_files/base.py` — Rewrite: Storage ABC, FileMetadata, UploadInstructions, StorageCapabilities
- `datasette_files/filesystem.py` — New: built-in FilesystemStorage implementation
- `datasette_files/__init__.py` — Rewrite: routes, startup, render_cell, permissions, source management
- `datasette_files/hookspecs.py` — Change hook from `register_files_storages` to `register_files_storage_types`
- `datasette_files/templates/` — File info page, upload component, browse page
- `datasette_files/static/` — `file-upload.js` web component
- `tests/test_files.py` — Comprehensive tests
- `pyproject.toml` — Remove boto3 dependency, add python-ulid, keep datasette>=1.0a24

## MVP Implementation Order

1. `base.py` — Storage ABC, dataclasses (FileMetadata, UploadInstructions, StorageCapabilities)
2. `hookspecs.py` — `register_files_storage_types` hook spec
3. `filesystem.py` — FilesystemStorage implementation
4. `__init__.py` — Startup (schema creation, source instantiation), upload/serve/download routes, render_cell hook, permission registration, table action for file column config
5. `templates/` + `static/` — File info page, upload web component, render_cell CSS
6. `tests/` — End-to-end test: configure filesystem source, upload file, verify it renders in a table column

## Verification

1. Configure a filesystem source in test datasette.yaml
2. Mark a column as `file_column: "true"` via table action (or config)
3. Upload a file via `POST /-/files/upload/uploads` (multipart)
4. Insert a row with the returned `df-{ULID}` in the file column
5. View the table — verify render_cell shows the file as a link
6. Visit `/-/files/{id}` — verify HTML info page renders
7. Visit `/-/files/{id}/download` — verify file content is served
8. Test permissions: unauthenticated user denied upload, allowed view
9. Test with mock storage for unit tests, filesystem for integration tests
