from datetime import datetime, timezone
import hashlib
import json
import re
import string
import time
from dataclasses import dataclass, field
from html import escape
from typing import Optional
from datasette import hookimpl, Response, NotFound, Forbidden
from datasette.column_types import ColumnType, SQLiteType
from datasette.permissions import Action, PermissionSQL, Resource
from datasette.plugins import pm
from datasette.utils import await_me_maybe
from markupsafe import Markup
from ulid import ULID
from . import hookspecs
from .base import StorageCapabilities
from .filesystem import FilesystemStorage

_FILE_ID_RE = re.compile(r"^df-[a-z0-9]{26}$")


@dataclass
class UploadToken:
    source_slug: str
    filename: str
    content_type: str
    size: Optional[int]
    path: str
    file_id: str
    created_at: float
    used: bool = False
    content_received: bool = False
    actor: Optional[dict] = None
    file_meta: Optional["FileMetadata"] = None
    actual_size: Optional[int] = None


# Upload token store: {token_string: UploadToken}
_upload_tokens: dict[str, UploadToken] = {}
_UPLOAD_TOKEN_TTL = 3600  # 1 hour
_DEFAULT_MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
_MAX_FILENAME_BYTES = 255

pm.add_hookspecs(hookspecs)

# Built-in storage types (always available, no plugin needed)
BUILT_IN_STORAGE_TYPES = {"filesystem": FilesystemStorage}

# Registry of configured source instances: {slug: storage_instance}
_sources = {}
# Registry of source metadata: {slug: {slug, storage_type, source_id, ...}}
_source_meta = {}

# Registry of thumbnail generators (populated in startup)
_thumbnail_generators: list = []

# --- SVG file-type icon generation ---

_FILE_ICON_STYLES = {
    "CSV": {
        "badge": "#2E7D32",
        "bg": "#EEF7EE",
        "stroke": "#7AB87E",
        "fold": "#C4E0C5",
    },
    "PDF": {
        "badge": "#E05050",
        "bg": "#FDF0EE",
        "stroke": "#D4837A",
        "fold": "#F5C4C0",
    },
    "JSON": {
        "badge": "#F59E0B",
        "bg": "#FFFBEB",
        "stroke": "#D4A34A",
        "fold": "#FDE68A",
    },
    "GEOJSON": {
        "badge": "#F59E0B",
        "bg": "#FFFBEB",
        "stroke": "#D4A34A",
        "fold": "#FDE68A",
    },
    "XLS": {
        "badge": "#1D6F42",
        "bg": "#EDF5F0",
        "stroke": "#6DA88A",
        "fold": "#B7DAC5",
    },
    "XLSX": {
        "badge": "#1D6F42",
        "bg": "#EDF5F0",
        "stroke": "#6DA88A",
        "fold": "#B7DAC5",
    },
    "DOC": {
        "badge": "#2B579A",
        "bg": "#EEF1F7",
        "stroke": "#7B8FB8",
        "fold": "#BDC9E0",
    },
    "DOCX": {
        "badge": "#2B579A",
        "bg": "#EEF1F7",
        "stroke": "#7B8FB8",
        "fold": "#BDC9E0",
    },
    "ZIP": {
        "badge": "#7C3AED",
        "bg": "#F3EEFF",
        "stroke": "#A78BDB",
        "fold": "#D4BFFA",
    },
    "GZ": {"badge": "#7C3AED", "bg": "#F3EEFF", "stroke": "#A78BDB", "fold": "#D4BFFA"},
    "TAR": {
        "badge": "#7C3AED",
        "bg": "#F3EEFF",
        "stroke": "#A78BDB",
        "fold": "#D4BFFA",
    },
    "BZ2": {
        "badge": "#7C3AED",
        "bg": "#F3EEFF",
        "stroke": "#A78BDB",
        "fold": "#D4BFFA",
    },
    "7Z": {"badge": "#7C3AED", "bg": "#F3EEFF", "stroke": "#A78BDB", "fold": "#D4BFFA"},
    "RAR": {
        "badge": "#7C3AED",
        "bg": "#F3EEFF",
        "stroke": "#A78BDB",
        "fold": "#D4BFFA",
    },
    "MP4": {
        "badge": "#DC2626",
        "bg": "#FEF2F2",
        "stroke": "#D48A8A",
        "fold": "#FECACA",
    },
    "MOV": {
        "badge": "#DC2626",
        "bg": "#FEF2F2",
        "stroke": "#D48A8A",
        "fold": "#FECACA",
    },
    "AVI": {
        "badge": "#DC2626",
        "bg": "#FEF2F2",
        "stroke": "#D48A8A",
        "fold": "#FECACA",
    },
    "MKV": {
        "badge": "#DC2626",
        "bg": "#FEF2F2",
        "stroke": "#D48A8A",
        "fold": "#FECACA",
    },
    "WEBM": {
        "badge": "#DC2626",
        "bg": "#FEF2F2",
        "stroke": "#D48A8A",
        "fold": "#FECACA",
    },
    "MP3": {
        "badge": "#9333EA",
        "bg": "#FAF5FF",
        "stroke": "#B48AD8",
        "fold": "#DDD6FE",
    },
    "WAV": {
        "badge": "#9333EA",
        "bg": "#FAF5FF",
        "stroke": "#B48AD8",
        "fold": "#DDD6FE",
    },
    "OGG": {
        "badge": "#9333EA",
        "bg": "#FAF5FF",
        "stroke": "#B48AD8",
        "fold": "#DDD6FE",
    },
    "FLAC": {
        "badge": "#9333EA",
        "bg": "#FAF5FF",
        "stroke": "#B48AD8",
        "fold": "#DDD6FE",
    },
    "M4A": {
        "badge": "#9333EA",
        "bg": "#FAF5FF",
        "stroke": "#B48AD8",
        "fold": "#DDD6FE",
    },
}
_TEXT_EXTS = {"TXT", "MD", "RST"}
_TEXT_STYLE = {
    "badge": "#6B7280",
    "bg": "#F3F4F6",
    "stroke": "#9CA3AF",
    "fold": "#D1D5DB",
}
_DEFAULT_STYLE = {
    "badge": "#6B7280",
    "bg": "#F9FAFB",
    "stroke": "#9CA3AF",
    "fold": "#E5E7EB",
}

_SVG_ICON_TEMPLATE = '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="200" viewBox="-2 -2 410 310"><rect x="4" y="4" width="400" height="300" rx="12" fill="#00000008"/><path d="M0,12 Q0,0 12,0 L340,0 L400,60 L400,288 Q400,300 388,300 L12,300 Q0,300 0,288 Z" fill="{bg}" stroke="{stroke}" stroke-width="2"/><path d="M340,0 L340,48 Q340,60 352,60 L400,60" fill="{fold}" stroke="{stroke}" stroke-width="2"/><rect x="100" y="110" width="200" height="80" rx="10" fill="{badge}"/><text x="200" y="150" text-anchor="middle" font-family="system-ui,sans-serif" font-size="36" font-weight="500" fill="#FFFFFF" dominant-baseline="central">{label}</text></svg>'


def _generate_file_icon_svg(filename: str, content_type: str) -> str:
    ext = filename.rsplit(".", 1)[-1].upper() if "." in filename else "?"
    if content_type == "text/csv":
        ext = "CSV"
    elif content_type == "application/pdf":
        ext = "PDF"
    elif content_type == "application/json":
        ext = "JSON"

    style = _FILE_ICON_STYLES.get(ext)
    if not style:
        if ext in _TEXT_EXTS or (content_type and content_type.startswith("text/")):
            style = _TEXT_STYLE
        else:
            style = _DEFAULT_STYLE
    return _SVG_ICON_TEMPLATE.format(label=escape(ext), **style)


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS datasette_files_sources (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    storage_type TEXT NOT NULL,
    label TEXT,
    config TEXT DEFAULT '{}',
    last_sync_token TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS datasette_files (
    id TEXT PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES datasette_files_sources(id),
    path TEXT NOT NULL,
    filename TEXT NOT NULL,
    content_type TEXT,
    content_hash TEXT,
    size INTEGER,
    width INTEGER,
    height INTEGER,
    uploaded_by TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    metadata TEXT DEFAULT '{}',
    search_text TEXT DEFAULT '',
    UNIQUE(source_id, path)
);

CREATE TABLE IF NOT EXISTS _datasette_files_imports (
    id INTEGER PRIMARY KEY,
    file_id TEXT NOT NULL,
    import_type TEXT NOT NULL DEFAULT 'csv',
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    row_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    bytes_read INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    actor_id TEXT
);

CREATE TABLE IF NOT EXISTS datasette_files_thumbnails (
    file_id TEXT PRIMARY KEY,
    thumbnail BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'image/png',
    width INTEGER,
    height INTEGER,
    generator TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

FTS_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS datasette_files_fts USING fts5(
    id UNINDEXED,
    filename,
    content_type,
    search_text,
    content='datasette_files',
    content_rowid='rowid'
);
"""

FTS_TRIGGERS_SQL = """
CREATE TRIGGER IF NOT EXISTS datasette_files_ai AFTER INSERT ON datasette_files BEGIN
    INSERT INTO datasette_files_fts(rowid, id, filename, content_type, search_text)
    VALUES (new.rowid, new.id, new.filename, new.content_type, new.search_text);
END;

CREATE TRIGGER IF NOT EXISTS datasette_files_ad AFTER DELETE ON datasette_files BEGIN
    INSERT INTO datasette_files_fts(datasette_files_fts, rowid, id, filename, content_type, search_text)
    VALUES ('delete', old.rowid, old.id, old.filename, old.content_type, old.search_text);
END;

CREATE TRIGGER IF NOT EXISTS datasette_files_au AFTER UPDATE ON datasette_files BEGIN
    INSERT INTO datasette_files_fts(datasette_files_fts, rowid, id, filename, content_type, search_text)
    VALUES ('delete', old.rowid, old.id, old.filename, old.content_type, old.search_text);
    INSERT INTO datasette_files_fts(rowid, id, filename, content_type, search_text)
    VALUES (new.rowid, new.id, new.filename, new.content_type, new.search_text);
END;
"""


class FileColumnType(ColumnType):
    name = "file"
    description = "Link to a file"
    sqlite_types = (SQLiteType.TEXT,)

    async def render_cell(self, value, column, table, database, datasette, request):
        return _render_file_cell(datasette, value, column, table)


# --- Resource and Action definitions ---


class FileSourceResource(Resource):
    """A file source in datasette-files."""

    name = "file-source"
    parent_class = None  # Top-level resource

    def __init__(self, source_slug: str):
        super().__init__(parent=source_slug, child=None)

    @classmethod
    async def resources_sql(cls, datasette, actor) -> str:
        return "SELECT slug AS parent, NULL AS child FROM datasette_files_sources"


class FileResource(Resource):
    """An individual file within a source."""

    name = "file"
    parent_class = FileSourceResource

    def __init__(self, source_slug: str, file_id: str):
        super().__init__(parent=source_slug, child=file_id)

    @classmethod
    async def resources_sql(cls, datasette, actor=None) -> str:
        return """
            SELECT s.slug AS parent, f.id AS child
            FROM datasette_files f
            JOIN datasette_files_sources s ON f.source_id = s.id
        """


@hookimpl
def register_actions():
    return [
        Action(
            name="files-browse",
            abbr="fb",
            description="Browse and search files in a source",
            resource_class=FileSourceResource,
        ),
        Action(
            name="files-upload",
            abbr="fu",
            description="Upload files to a source",
            resource_class=FileSourceResource,
        ),
        Action(
            name="files-edit",
            abbr="fe",
            description="Edit file metadata",
            resource_class=FileResource,
        ),
        Action(
            name="files-delete",
            abbr="fd",
            description="Delete files",
            resource_class=FileResource,
        ),
    ]


@hookimpl
def register_column_types(datasette):
    return [FileColumnType]


@hookimpl(specname="permission_resources_sql")
def files_permission_resources_sql(datasette, actor, action):
    """Provide permission rules for files-browse, files-upload, files-delete.

    By default no access is granted. Permissions must be explicitly configured
    via datasette.yaml permissions block.

    Supports two config patterns:

    Global (all sources):
        permissions:
          files-browse: true

    Per-source:
        permissions:
          files-browse:
            my-source:
              allow: true
            secret-source:
              allow:
                id: alice
    """
    if action not in ("files-browse", "files-upload", "files-edit", "files-delete"):
        return None

    config = datasette.config or {}
    permissions_config = config.get("permissions") or {}
    action_config = permissions_config.get(action)
    if action_config is None:
        return None

    from datasette.utils import actor_matches_allow

    rules = []
    params = {}

    # Check if this is a simple allow block (global) or a per-source dict
    if isinstance(action_config, bool) or (
        isinstance(action_config, dict)
        and ("id" in action_config or "unauthenticated" in action_config)
    ):
        # Global allow block: applies to all sources
        allowed = actor_matches_allow(actor, action_config)
        i = len(rules)
        rules.append(
            f"SELECT NULL AS parent, NULL AS child, :dfp_allow_{i} AS allow, :dfp_reason_{i} AS reason"
        )
        params[f"dfp_allow_{i}"] = 1 if allowed else 0
        params[f"dfp_reason_{i}"] = (
            f"datasette-files config {'allow' if allowed else 'deny'} for {action}"
        )
    elif isinstance(action_config, dict):
        # Per-source permissions: {source_slug: allow_block, ...}
        for source_slug, source_allow_block in action_config.items():
            # source_allow_block can be a simple allow block or {"allow": ...}
            if isinstance(source_allow_block, dict) and "allow" in source_allow_block:
                allow_block = source_allow_block["allow"]
            else:
                allow_block = source_allow_block
            allowed = actor_matches_allow(actor, allow_block)
            i = len(rules)
            rules.append(
                f"SELECT :dfp_parent_{i} AS parent, NULL AS child, :dfp_allow_{i} AS allow, :dfp_reason_{i} AS reason"
            )
            params[f"dfp_parent_{i}"] = source_slug
            params[f"dfp_allow_{i}"] = 1 if allowed else 0
            params[f"dfp_reason_{i}"] = (
                f"datasette-files config {'allow' if allowed else 'deny'} for {action} on {source_slug}"
            )

    if not rules:
        return None

    return PermissionSQL(
        sql="\nUNION ALL\n".join(rules),
        params=params,
    )


@hookimpl(specname="permission_resources_sql")
def files_owner_permissions_sql(datasette, actor, action):
    """Grant file owners edit/delete permission on their own files.

    Enabled via plugin config:
        plugins:
          datasette-files:
            owners_can_edit: true
            owners_can_delete: true
    """
    if action not in ("files-edit", "files-delete"):
        return None
    if not actor or not actor.get("id"):
        return None

    plugin_config = datasette.plugin_config("datasette-files") or {}
    if action == "files-edit" and not plugin_config.get("owners_can_edit"):
        return None
    if action == "files-delete" and not plugin_config.get("owners_can_delete"):
        return None

    return PermissionSQL(
        sql="""
            SELECT s.slug AS parent, f.id AS child,
                   1 AS allow,
                   'file owner' AS reason
            FROM datasette_files f
            JOIN datasette_files_sources s ON f.source_id = s.id
            WHERE f.uploaded_by = :dfow_actor_id
        """,
        params={"dfow_actor_id": actor["id"]},
    )


# --- Helpers ---


def _sanitize_filename(filename):
    """Remove path separators and other dangerous characters from a filename."""
    # Strip directory components
    filename = filename.replace("/", "_").replace("\\", "_")
    # Remove null bytes
    filename = filename.replace("\x00", "")
    filename = filename or "unnamed"

    if len(filename.encode("utf-8")) <= _MAX_FILENAME_BYTES:
        return filename

    stem, dot, ext = filename.rpartition(".")
    if dot and stem:
        ext = ext.encode("utf-8")[: _MAX_FILENAME_BYTES - 1].decode(
            "utf-8", errors="ignore"
        )
        stem_budget = _MAX_FILENAME_BYTES - len(ext.encode("utf-8")) - 1
        if stem_budget > 0:
            stem = stem.encode("utf-8")[:stem_budget].decode("utf-8", errors="ignore")
            filename = f"{stem}.{ext}"
        else:
            filename = filename.encode("utf-8")[:_MAX_FILENAME_BYTES].decode(
                "utf-8", errors="ignore"
            )
    else:
        filename = filename.encode("utf-8")[:_MAX_FILENAME_BYTES].decode(
            "utf-8", errors="ignore"
        )

    return filename or "unnamed"


async def _check_browse_permission(datasette, request, source_slug):
    """Check files-browse permission for a source. Raises Forbidden if denied."""
    allowed = await datasette.allowed(
        action="files-browse",
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not allowed:
        raise Forbidden("Permission denied: files-browse on source " + source_slug)


# --- Startup ---


@hookimpl
def startup(datasette):
    async def inner():
        db = datasette.get_internal_database()
        await db.execute_write_script(CREATE_SQL)

        # Migrate: add search_text column if missing (pre-existing databases)
        columns = (await db.execute("PRAGMA table_info(datasette_files)")).rows
        col_names = {row["name"] for row in columns}
        if "search_text" not in col_names:
            await db.execute_write(
                "ALTER TABLE datasette_files ADD COLUMN search_text TEXT DEFAULT ''"
            )

        # Drop and recreate FTS table + triggers to ensure schema matches
        # (safe because FTS is a secondary index rebuilt from content table)
        await db.execute_write_script("""
            DROP TRIGGER IF EXISTS datasette_files_ai;
            DROP TRIGGER IF EXISTS datasette_files_ad;
            DROP TRIGGER IF EXISTS datasette_files_au;
            DROP TABLE IF EXISTS datasette_files_fts;
            """)
        await db.execute_write_script(FTS_SQL)
        await db.execute_write_script(FTS_TRIGGERS_SQL)

        # Backfill FTS from content table
        await db.execute_write("""
            INSERT INTO datasette_files_fts(rowid, id, filename, content_type, search_text)
            SELECT rowid, id, filename, content_type, search_text FROM datasette_files
            """)

        # Collect storage types from plugins
        storage_types = dict(BUILT_IN_STORAGE_TYPES)
        for hook in pm.hook.register_files_storage_types(datasette=datasette):
            result = await await_me_maybe(hook)
            if result:
                for cls in result:
                    storage_types[cls.storage_type] = cls

        # Read source definitions from plugin config
        config = datasette.plugin_config("datasette-files") or {}
        sources_config = config.get("sources") or {}

        for slug, source_def in sources_config.items():
            storage_type_name = source_def.get("storage")
            if storage_type_name not in storage_types:
                raise ValueError(
                    f"Unknown storage type '{storage_type_name}' for source '{slug}'. "
                    f"Available: {list(storage_types.keys())}"
                )

            storage_cls = storage_types[storage_type_name]
            storage = storage_cls()

            source_config = source_def.get("config", {})
            await storage.configure(source_config, get_secret=None)

            # Upsert source row
            await db.execute_write(
                """
                INSERT INTO datasette_files_sources (slug, storage_type, config)
                VALUES (:slug, :storage_type, :config)
                ON CONFLICT(slug) DO UPDATE SET
                    storage_type = :storage_type,
                    config = :config
                """,
                {
                    "slug": slug,
                    "storage_type": storage_type_name,
                    "config": json.dumps(source_config),
                },
            )

            # Get the source ID
            row = (
                await db.execute(
                    "SELECT id FROM datasette_files_sources WHERE slug = ?", [slug]
                )
            ).first()

            _sources[slug] = storage
            _source_meta[slug] = {
                "slug": slug,
                "storage_type": storage_type_name,
                "source_id": row["id"],
                "capabilities": storage.capabilities,
            }

        # Collect thumbnail generators
        _thumbnail_generators.clear()
        for hook in pm.hook.register_thumbnail_generators(datasette=datasette):
            result = await await_me_maybe(hook)
            if result:
                for gen in result:
                    _thumbnail_generators.append(gen)

    return inner


# --- Route handlers ---


async def upload_page(request, datasette):
    """GET /-/files/upload/{source_slug} - dedicated upload page."""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    if request.method != "GET":
        return Response.text("Method not allowed", status=405, headers={"Allow": "GET"})

    can_upload = await datasette.allowed(
        action="files-upload",
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not can_upload:
        raise Forbidden("Permission denied: files-upload on source " + source_slug)

    return Response.html(
        await datasette.render_template(
            "files_upload.html",
            {"source_slug": source_slug},
            request=request,
        )
    )


def _clean_expired_tokens():
    """Remove expired upload tokens."""
    now = time.time()
    expired = [
        t for t, v in _upload_tokens.items() if now - v.created_at > _UPLOAD_TOKEN_TTL
    ]
    for t in expired:
        del _upload_tokens[t]


_ALNUM = set(string.ascii_letters + string.digits)


def _ext_from_content_type(content_type: str) -> str:
    """Return a file extension (without dot) for common content types, or ''."""
    return {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
        "image/svg+xml": "svg",
        "image/bmp": "bmp",
        "image/tiff": "tiff",
        "text/plain": "txt",
        "text/html": "html",
        "text/css": "css",
        "text/csv": "csv",
        "text/tab-separated-values": "tsv",
        "application/pdf": "pdf",
        "application/json": "json",
        "application/zip": "zip",
        "application/gzip": "gz",
        "application/xml": "xml",
        "text/xml": "xml",
    }.get(content_type, "dat")


def _safe_download_filename(filename: str, content_type: str = "") -> str:
    """Sanitize filename for Content-Disposition: only a-zA-Z0-9 plus .ext."""
    if "." in filename:
        name, ext = filename.rsplit(".", 1)
        ext = "".join(c for c in ext if c in _ALNUM)
    else:
        name = filename
        ext = ""
    name = "".join(c for c in name if c in _ALNUM)
    if not name:
        name = "download"
    if not ext and content_type:
        ext = _ext_from_content_type(content_type)
    return f"{name}.{ext}" if ext else name


def _error(message, status=400):
    return Response.json({"ok": False, "errors": [message]}, status=status)


def _etag_for_bytes(content: bytes) -> str:
    return f'"{hashlib.md5(content).hexdigest()}"'


def _response_with_etag(request, body: bytes, content_type: str):
    etag = _etag_for_bytes(body)
    headers = {"ETag": etag}
    if request.headers.get("if-none-match") == etag:
        return Response(body=b"", status=304, headers=headers)
    return Response(body=body, content_type=content_type, headers=headers)


async def _check_upload_permission_json(datasette, request, source_slug):
    """Check files-upload permission, return error Response or None."""
    allowed = await datasette.allowed(
        action="files-upload",
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not allowed:
        return _error("Permission denied", status=403)
    return None


async def upload_prepare(request, datasette):
    """POST /-/files/upload/{source_slug}/-/prepare - get upload instructions."""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    error = await _check_upload_permission_json(datasette, request, source_slug)
    if error:
        return error

    try:
        body = json.loads(await request.post_body())
    except (json.JSONDecodeError, ValueError):
        return _error("Invalid JSON")

    filename = body.get("filename")
    if not filename:
        return _error("filename is required")

    content_type = body.get("content_type", "application/octet-stream")
    size = body.get("size")

    filename = _sanitize_filename(filename)

    # Generate file ID and storage path
    file_id = "df-" + str(ULID()).lower()
    ulid_part = file_id[3:]
    path = f"{ulid_part}/{filename}"

    # Generate upload token
    _clean_expired_tokens()
    token = "tok_" + str(ULID()).lower()
    _upload_tokens[token] = UploadToken(
        source_slug=source_slug,
        filename=filename,
        content_type=content_type,
        size=size,
        path=path,
        file_id=file_id,
        created_at=time.time(),
        actor=request.actor,
    )

    # Build upload URL - for filesystem, it points to our upload endpoint
    upload_url = datasette.urls.path(f"/-/files/upload/{source_slug}/-/upload")

    return Response.json(
        {
            "ok": True,
            "upload_token": token,
            "upload_url": upload_url,
            "upload_method": "POST",
            "upload_headers": {},
            "upload_fields": {
                "upload_token": token,
            },
        }
    )


async def upload_content(request, datasette):
    """POST /-/files/upload/{source_slug}/-/upload - receive file bytes"""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]

    # Parse multipart form — use storage's max_file_size if configured,
    # otherwise use the default limit.
    max_size = storage.capabilities.max_file_size or _DEFAULT_MAX_FILE_SIZE
    form = await request.form(
        files=True,
        max_file_size=max_size,
        max_request_size=max_size + 1024 * 1024,  # file + form overhead
    )
    try:
        token_value = form.get("upload_token")
        if not token_value or (hasattr(token_value, "read") and not token_value):
            # Try string value
            pass
        if hasattr(token_value, "read"):
            token_value = None

        if not token_value:
            return _error("upload_token is required")

        token_data = _upload_tokens.get(token_value)
        if not token_data:
            return _error("Invalid or expired upload token")

        if token_data.source_slug != source_slug:
            return _error("Token does not match this source")

        if token_data.content_received:
            return _error("Content already uploaded for this token")

        uploaded = form.get("file")
        if uploaded is None or not hasattr(uploaded, "read"):
            return _error("No file provided")

        content_type = token_data.content_type
        path = token_data.path

        # Stream file chunks to the storage backend
        async def _upload_chunks(uploaded_file, chunk_size=65536):
            while True:
                chunk = await uploaded_file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        file_meta = await storage.receive_upload(
            path, _upload_chunks(uploaded), content_type
        )

        # Save metadata on the token for the complete step
        token_data.content_received = True
        token_data.file_meta = file_meta
        token_data.actual_size = file_meta.size

        return Response.json({"ok": True})
    finally:
        await form.aclose()


async def upload_complete(request, datasette):
    """POST /-/files/upload/{source_slug}/-/complete - finalize upload and register file."""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    error = await _check_upload_permission_json(datasette, request, source_slug)
    if error:
        return error

    meta = _source_meta[source_slug]

    try:
        body = json.loads(await request.post_body())
    except (json.JSONDecodeError, ValueError):
        return _error("Invalid JSON")

    token_value = body.get("upload_token")
    if not token_value:
        return _error("upload_token is required")

    token_data = _upload_tokens.get(token_value)
    if not token_data:
        return _error("Invalid or expired upload token")

    if token_data.source_slug != source_slug:
        return _error("Token does not match this source")

    if not token_data.content_received:
        return _error("File content has not been uploaded yet")

    if token_data.used:
        return _error("This upload token has already been used")

    # Mark token as used
    token_data.used = True

    file_id = token_data.file_id
    file_meta = token_data.file_meta
    filename = token_data.filename
    content_type = token_data.content_type
    path = token_data.path
    actor = token_data.actor

    # Record in internal database
    db = datasette.get_internal_database()
    await db.execute_write(
        """
        INSERT INTO datasette_files
            (id, source_id, path, filename, content_type, content_hash, size, uploaded_by)
        VALUES
            (:id, :source_id, :path, :filename, :content_type, :content_hash, :size, :uploaded_by)
        """,
        {
            "id": file_id,
            "source_id": meta["source_id"],
            "path": path,
            "filename": filename,
            "content_type": file_meta.content_type or content_type,
            "content_hash": file_meta.content_hash,
            "size": file_meta.size or token_data.actual_size,
            "uploaded_by": (actor or {}).get("id"),
        },
    )

    # Clean up token
    del _upload_tokens[token_value]

    # Fetch the created record for the response
    row = await _get_file_record(datasette, file_id)

    # Eagerly generate thumbnails when a registered generator can handle the file.
    try:
        await _get_or_generate_thumbnail(datasette, file_id, row)
    except Exception:
        pass  # Non-fatal: thumbnail will be generated lazily on first request

    return Response.json(
        {
            "ok": True,
            "file": {
                "id": file_id,
                "filename": row["filename"],
                "content_type": row["content_type"],
                "content_hash": row["content_hash"],
                "size": row["size"],
                "width": row["width"],
                "height": row["height"],
                "source_slug": row["source_slug"],
                "uploaded_by": row["uploaded_by"],
                "created_at": row["created_at"],
                "url": datasette.urls.path(f"/-/files/{file_id}"),
                "download_url": datasette.urls.path(f"/-/files/{file_id}/download"),
                "thumbnail_url": datasette.urls.path(f"/-/files/{file_id}/thumbnail"),
            },
        },
        status=201,
    )


async def file_delete(request, datasette):
    """POST /-/files/{file_id}/-/delete - delete a file."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    source_slug = row["source_slug"]

    # Check delete permission
    allowed = await datasette.allowed(
        action="files-delete",
        resource=FileResource(source_slug, file_id),
        actor=request.actor,
    )
    if not allowed:
        raise Forbidden("Permission denied: files-delete on source " + source_slug)

    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]

    if not storage.capabilities.can_delete:
        return _error("This storage backend does not support deletion")

    # Delete from storage backend
    await storage.delete_file(row["path"])

    # Delete from internal database
    db = datasette.get_internal_database()
    await db.execute_write(
        "DELETE FROM datasette_files_thumbnails WHERE file_id = ?", [file_id]
    )
    await db.execute_write("DELETE FROM datasette_files WHERE id = ?", [file_id])

    return Response.json({"ok": True})


async def file_update(request, datasette):
    """POST /-/files/{file_id}/-/update - update file metadata."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    source_slug = row["source_slug"]

    # Check edit permission
    allowed = await datasette.allowed(
        action="files-edit",
        resource=FileResource(source_slug, file_id),
        actor=request.actor,
    )
    if not allowed:
        raise Forbidden("Permission denied: files-edit on source " + source_slug)

    try:
        body = json.loads(await request.post_body())
    except (json.JSONDecodeError, ValueError):
        return _error("Invalid JSON")

    update = body.get("update")
    if not update or not isinstance(update, dict):
        return _error("update object is required")

    # Only allow editing search_text for now
    allowed_fields = {"search_text"}
    invalid_fields = set(update.keys()) - allowed_fields
    if invalid_fields:
        return _error(f"Cannot update fields: {', '.join(sorted(invalid_fields))}")

    if not update:
        return _error("No valid fields to update")

    db = datasette.get_internal_database()
    if "search_text" in update:
        await db.execute_write(
            "UPDATE datasette_files SET search_text = :search_text WHERE id = :id",
            {"search_text": update["search_text"], "id": file_id},
        )

    # Re-fetch updated record
    row = await _get_file_record(datasette, file_id)

    return Response.json(
        {
            "ok": True,
            "file": dict(row),
        }
    )


async def _get_file_record(datasette, file_id):
    """Look up a file record from the internal database."""
    db = datasette.get_internal_database()
    row = (
        await db.execute(
            """
            SELECT f.*, s.slug as source_slug
            FROM datasette_files f
            JOIN datasette_files_sources s ON f.source_id = s.id
            WHERE f.id = ?
            """,
            [file_id],
        )
    ).first()
    return row


def _parse_created_at(value):
    """Parse stored timestamps into UTC-aware datetimes.

    Accepts SQLite's default ``YYYY-MM-DD HH:MM:SS`` format plus common
    ISO 8601 variants such as ``T`` separators, fractional seconds, and
    explicit offsets.
    """
    if isinstance(value, datetime):
        dt = value
    else:
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class File:
    """Represents a file managed by datasette-files.

    Returned by :func:`get_file`. Provides metadata attributes and methods
    for reading file content.
    """

    def __init__(self, row, storage):
        self.id = row["id"]
        self.filename = row["filename"]
        self.content_type = row["content_type"]
        self.size = row["size"]
        self.source_slug = row["source_slug"]
        self.uploaded_by = row["uploaded_by"]
        self.created_at = _parse_created_at(row["created_at"])
        self.metadata = json.loads(row["metadata"] or "{}")
        self._storage = storage
        self._path = row["path"]

    async def read(self, max_bytes=None):
        """Read file content as bytes.

        Args:
            max_bytes: If set, read at most this many bytes from the start
                of the file. Useful to avoid loading very large files into
                memory.

        Returns:
            The file content as bytes.
        """
        if max_bytes is not None:
            return await self._storage.read_bytes(self._path, max_bytes)
        return await self._storage.read_file(self._path)

    def open(self):
        """Open the file for streaming reads.

        Returns an async context manager that yields an async iterator
        of bytes chunks::

            async with file.open() as stream:
                async for chunk in stream:
                    process(chunk)
        """
        return _FileStream(self._storage, self._path)


class _FileStream:
    def __init__(self, storage, path):
        self._storage = storage
        self._path = path
        self._iterator = None

    async def __aenter__(self):
        self._iterator = self._storage.stream_file(self._path)
        return self

    async def __aexit__(self, *exc):
        if self._iterator and hasattr(self._iterator, "aclose"):
            await self._iterator.aclose()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._iterator is None:
            raise StopAsyncIteration
        return await self._iterator.__anext__()


async def get_file(datasette, file_id):
    """Look up a file by its ID and return a :class:`File` object.

    Args:
        datasette: The Datasette instance.
        file_id: The file ID (e.g. ``"df-01j5a3b4c5d6e7f8g9h0jkmnpq"``).

    Returns:
        A :class:`File` object, or ``None`` if the file was not found.
    """
    row = await _get_file_record(datasette, file_id)
    if row is None:
        return None
    source_slug = row["source_slug"]
    if source_slug not in _sources:
        return None
    return File(row, _sources[source_slug])


async def file_info(request, datasette):
    """GET /-/files/{file_id} - HTML info page about a file."""
    if request.method != "GET":
        return Response.text("Method not allowed", status=405, headers={"Allow": "GET"})

    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    await _check_browse_permission(datasette, request, row["source_slug"])

    file_dict = dict(row)

    source_slug = row["source_slug"]
    preview_bytes = b""
    if source_slug in _sources:
        try:
            preview_bytes = await _sources[source_slug].read_bytes(row["path"])
        except Exception:
            pass

    # Collect file actions from plugins
    async def get_file_actions():
        links = []
        for hook in pm.hook.file_actions(
            datasette=datasette,
            actor=request.actor,
            file=file_dict,
            preview_bytes=preview_bytes,
        ):
            extra_links = await await_me_maybe(hook)
            if extra_links:
                links.extend(extra_links)
        return links

    return Response.html(
        await datasette.render_template(
            "file_info.html",
            {
                "file": file_dict,
                "file_actions": get_file_actions,
            },
            request=request,
        )
    )


async def file_json(request, datasette):
    """GET /-/files/{file_id}.json - file metadata as JSON."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    await _check_browse_permission(datasette, request, row["source_slug"])

    return Response.json(dict(row))


class _StreamingFileResponse:
    """ASGI response that streams file content in chunks via storage.stream_file()."""

    def __init__(self, storage, path, content_type, filename, size=None):
        self.storage = storage
        self.path = path
        self.content_type = content_type
        self.filename = filename
        self.size = size

    async def asgi_send(self, send):
        disposition = f'attachment; filename="{_safe_download_filename(self.filename, self.content_type)}"'
        headers = {
            "content-type": self.content_type,
            "content-disposition": disposition,
            "x-content-type-options": "nosniff",
        }
        if self.size is not None:
            headers["content-length"] = str(self.size)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [k.encode("latin1"), v.encode("latin1")] for k, v in headers.items()
                ],
            }
        )
        async for chunk in self.storage.stream_file(self.path):
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})


async def file_thumbnail(request, datasette):
    """GET /-/files/{file_id}/thumbnail - return thumbnail or file-type icon."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    await _check_browse_permission(datasette, request, row["source_slug"])

    content_type = row["content_type"] or ""

    result = await _get_or_generate_thumbnail(datasette, file_id, row)
    if result:
        return _response_with_etag(request, result.thumb_bytes, result.content_type)

    # For non-image files (or if generation failed), return SVG icon
    svg = _generate_file_icon_svg(row["filename"], content_type).encode("utf-8")
    return _response_with_etag(request, svg, "image/svg+xml")


async def _get_or_generate_thumbnail(datasette, file_id, row):
    """Return ThumbnailResult or None. Checks cache, generates on miss."""
    from .base import ThumbnailResult

    db = datasette.get_internal_database()

    cached = (
        await db.execute(
            "SELECT thumbnail, content_type, width, height FROM datasette_files_thumbnails WHERE file_id = ?",
            [file_id],
        )
    ).first()
    if cached:
        return ThumbnailResult(
            thumb_bytes=cached["thumbnail"],
            content_type=cached["content_type"],
            width=cached["width"],
            height=cached["height"],
        )

    content_type = row["content_type"] or ""
    filename = row["filename"]
    source_slug = row["source_slug"]

    if source_slug not in _sources:
        return None

    storage = _sources[source_slug]

    matching_generators = []
    for generator in _thumbnail_generators:
        try:
            if await generator.can_generate(content_type, filename):
                matching_generators.append(generator)
        except Exception:
            continue

    if not matching_generators:
        return None

    try:
        file_bytes = await storage.read_file(row["path"])
    except Exception:
        return None

    for generator in matching_generators:
        try:
            result = await generator.generate(
                file_bytes, content_type, filename, max_width=200, max_height=200
            )
            if result:
                await db.execute_write(
                    """INSERT OR REPLACE INTO datasette_files_thumbnails
                       (file_id, thumbnail, content_type, width, height, generator)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    [
                        file_id,
                        result.thumb_bytes,
                        result.content_type,
                        result.width,
                        result.height,
                        generator.name,
                    ],
                )
                return result
        except Exception:
            continue

    return None


async def file_download(request, datasette):
    """GET /-/files/{file_id}/download - download the file."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    await _check_browse_permission(datasette, request, row["source_slug"])

    source_slug = row["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]

    # If the storage can generate signed URLs, redirect
    if storage.capabilities.can_generate_signed_urls:
        url = await storage.download_url(row["path"])
        return Response.redirect(url, status=302)

    # Stream the file content without loading it all into memory
    content_type = row["content_type"] or "application/octet-stream"
    return _StreamingFileResponse(
        storage=storage,
        path=row["path"],
        content_type=content_type,
        filename=row["filename"],
        size=row["size"],
    )


async def batch_json(request, datasette):
    """GET /-/files/batch.json?id=df-abc&id=df-def - bulk file metadata."""
    ids = request.args.getlist("id")
    # Filter to valid file IDs only
    ids = [i for i in ids if _FILE_ID_RE.match(i)]
    if not ids:
        return Response.json({"files": {}})

    placeholders = ",".join("?" * len(ids))
    db = datasette.get_internal_database()
    rows = (
        await db.execute(
            f"""
            SELECT f.id, f.filename, f.content_type, f.size, f.width, f.height,
                   s.slug as source_slug
            FROM datasette_files f
            JOIN datasette_files_sources s ON f.source_id = s.id
            WHERE f.id IN ({placeholders})
            """,
            ids,
        )
    ).rows

    files = {}
    for row in rows:
        row = dict(row)
        file_id = row["id"]
        # Check browse permission for this file's source
        allowed = await datasette.allowed(
            action="files-browse",
            resource=FileSourceResource(row["source_slug"]),
            actor=request.actor,
        )
        if not allowed:
            continue
        files[file_id] = {
            "id": file_id,
            "filename": row["filename"],
            "content_type": row["content_type"],
            "size": row["size"],
            "width": row["width"],
            "height": row["height"],
            "download_url": datasette.urls.path(f"/-/files/{file_id}/download"),
            "thumbnail_url": datasette.urls.path(f"/-/files/{file_id}/thumbnail"),
            "info_url": datasette.urls.path(f"/-/files/{file_id}"),
        }

    return Response.json({"files": files})


async def sources_json(request, datasette):
    """GET /-/files/sources.json - list all configured sources."""
    sources = []
    for slug, meta in _source_meta.items():
        caps = meta["capabilities"]
        sources.append(
            {
                "slug": slug,
                "storage_type": meta["storage_type"],
                "capabilities": {
                    "can_upload": caps.can_upload,
                    "can_delete": caps.can_delete,
                    "can_list": caps.can_list,
                    "can_generate_signed_urls": caps.can_generate_signed_urls,
                    "requires_proxy_download": caps.requires_proxy_download,
                },
            }
        )
    return Response.json({"sources": sources})


PAGE_SIZE = 20


async def _list_files(db, allowed_slugs, source_filter=None, offset=0, limit=PAGE_SIZE):
    """List recent files filtered to allowed sources, with pagination."""
    if not allowed_slugs:
        return [], 0
    placeholders = ",".join(f":_slug_{i}" for i in range(len(allowed_slugs)))
    where_source = "AND s.slug = :source_filter" if source_filter else ""
    params = {}
    for i, slug in enumerate(allowed_slugs):
        params[f"_slug_{i}"] = slug
    if source_filter:
        params["source_filter"] = source_filter

    count_sql = """
        SELECT COUNT(*) as count
        FROM datasette_files f
        JOIN datasette_files_sources s ON f.source_id = s.id
        WHERE s.slug IN ({placeholders})
        {where_source}
    """.format(placeholders=placeholders, where_source=where_source)
    total = (await db.execute(count_sql, params)).first()["count"]

    list_sql = """
        SELECT f.id, f.filename, f.content_type, f.size, f.width, f.height,
               f.created_at, f.uploaded_by, s.slug as source_slug
        FROM datasette_files f
        JOIN datasette_files_sources s ON f.source_id = s.id
        WHERE s.slug IN ({placeholders})
        {where_source}
        ORDER BY f.created_at DESC
        LIMIT :_limit OFFSET :_offset
    """.format(placeholders=placeholders, where_source=where_source)
    params["_limit"] = limit
    params["_offset"] = offset
    files = [dict(row) for row in (await db.execute(list_sql, params)).rows]
    return files, total


async def search_files(request, datasette):
    """GET /-/files/search - search files across sources the actor can browse."""
    q = request.args.get("q", "").strip()
    source_filter = request.args.get("source", "").strip()

    db = datasette.get_internal_database()

    # Get allowed source slugs via the permissions SQL CTE.
    # We execute this as a separate query because the CTE uses WITH clauses
    # that cannot be nested inside another query alongside FTS MATCH.
    resources_sql = await datasette.allowed_resources_sql(
        action="files-browse",
        actor=request.actor,
    )
    allowed_rows = (await db.execute(resources_sql.sql, resources_sql.params)).rows
    allowed_slugs = [row["parent"] for row in allowed_rows]

    if not allowed_slugs:
        files = []
    elif q:
        # Build prefix query: append * to each term for prefix matching, OR between terms
        terms = ['"{}"*'.format(term.replace('"', '""')) for term in q.split() if term]
        fts_q = " OR ".join(terms) if len(terms) > 1 else terms[0] if terms else q
        # FTS search filtered to allowed sources
        placeholders = ",".join(f":_slug_{i}" for i in range(len(allowed_slugs)))
        search_sql = """
            SELECT f.id, f.filename, f.content_type, f.size, f.width, f.height,
                   f.created_at, f.uploaded_by, s.slug as source_slug
            FROM datasette_files_fts fts
            JOIN datasette_files f ON fts.id = f.id
            JOIN datasette_files_sources s ON f.source_id = s.id
            WHERE datasette_files_fts MATCH :q
            AND s.slug IN ({placeholders})
            {source_where}
            ORDER BY fts.rank
            LIMIT 50
        """.format(
            placeholders=placeholders,
            source_where="AND s.slug = :source_filter" if source_filter else "",
        )
        params = {"q": fts_q}
        for i, slug in enumerate(allowed_slugs):
            params[f"_slug_{i}"] = slug
        if source_filter:
            params["source_filter"] = source_filter
        files = [dict(row) for row in (await db.execute(search_sql, params)).rows]
    else:
        files, _ = await _list_files(
            db, allowed_slugs, source_filter=source_filter or None, limit=50
        )

    # The allowed_slugs already represent the browsable sources
    browsable_sources = allowed_slugs

    is_json = request.path.endswith(".json")
    if is_json:
        return Response.json(
            {
                "q": q,
                "source": source_filter,
                "files": files,
                "sources": browsable_sources,
            }
        )

    return Response.html(
        await datasette.render_template(
            "files_search.html",
            {
                "q": q,
                "source_filter": source_filter,
                "files": files,
                "sources": browsable_sources,
                "show_source": True,
            },
            request=request,
        )
    )


async def source_files(request, datasette):
    """GET /-/files/source/{source_slug} - list files in a source with pagination."""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    can_browse = await datasette.allowed(
        action="files-browse",
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not can_browse:
        raise Forbidden("Permission denied: files-browse on source " + source_slug)

    can_upload = await datasette.allowed(
        action="files-upload",
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )

    db = datasette.get_internal_database()
    try:
        page = int(request.args.get("page", "1"))
    except ValueError:
        page = 1
    if page < 1:
        page = 1
    offset = (page - 1) * PAGE_SIZE

    files, total = await _list_files(db, [source_slug], offset=offset, limit=PAGE_SIZE)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    return Response.html(
        await datasette.render_template(
            "files_source.html",
            {
                "source_slug": source_slug,
                "files": files,
                "can_upload": can_upload,
                "page": page,
                "total_pages": total_pages,
                "total": total,
            },
            request=request,
        )
    )


async def files_index(request, datasette):
    """GET /-/files - index page listing sources with file counts."""
    db = datasette.get_internal_database()

    resources_sql = await datasette.allowed_resources_sql(
        action="files-browse",
        actor=request.actor,
    )
    allowed_rows = (await db.execute(resources_sql.sql, resources_sql.params)).rows
    allowed_slugs = [row["parent"] for row in allowed_rows]

    sources = []
    if allowed_slugs:
        placeholders = ",".join(f":_slug_{i}" for i in range(len(allowed_slugs)))
        sql = """
            SELECT s.slug, COUNT(f.id) as file_count
            FROM datasette_files_sources s
            LEFT JOIN datasette_files f ON f.source_id = s.id
            WHERE s.slug IN ({placeholders})
            GROUP BY s.slug
            ORDER BY s.slug
        """.format(placeholders=placeholders)
        params = {}
        for i, slug in enumerate(allowed_slugs):
            params[f"_slug_{i}"] = slug
        sources = [dict(row) for row in (await db.execute(sql, params)).rows]

    return Response.html(
        await datasette.render_template(
            "files_index.html",
            {"sources": sources},
            request=request,
        )
    )


@hookimpl
def register_routes():
    return [
        (r"^/-/files/search\.json$", search_files),
        (r"^/-/files/search$", search_files),
        (r"^/-/files/batch\.json$", batch_json),
        (r"^/-/files/sources\.json$", sources_json),
        # New unified upload API (prepare/upload/complete)
        (r"^/-/files/upload/(?P<source_slug>[^/]+)/-/prepare$", upload_prepare),
        (r"^/-/files/upload/(?P<source_slug>[^/]+)/-/upload$", upload_content),
        (r"^/-/files/upload/(?P<source_slug>[^/]+)/-/complete$", upload_complete),
        (r"^/-/files/upload/(?P<source_slug>[^/]+)$", upload_page),
        (
            r"^/-/files/import/(?P<file_id>df-[a-z0-9]+)/(?P<import_id>\d+)\.json$",
            import_progress_view,
        ),
        (
            r"^/-/files/import/(?P<file_id>df-[a-z0-9]+)/(?P<import_id>\d+)$",
            import_progress_view,
        ),
        (r"^/-/files/import/(?P<file_id>df-[a-z0-9]+)$", import_file_view),
        # File operations (delete, update)
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)/-/delete$", file_delete),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)/-/update$", file_update),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)\.json$", file_json),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)/thumbnail$", file_thumbnail),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)/download$", file_download),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)$", file_info),
        (r"^/-/files/source/(?P<source_slug>[^/]+)$", source_files),
        (r"^/-/files$", files_index),
    ]


def _render_file_cell(datasette, value, column, table):
    if not isinstance(value, str) or not _FILE_ID_RE.match(value):
        return None
    col_attr = (
        ' data-column="{}"'.format(Markup.escape(column)) if table is not None else ""
    )
    url = datasette.urls.path(f"/-/files/{value}")
    return Markup(
        '<datasette-file file-id="{v}"{col}>'
        '<a href="{url}">{v}</a>'
        "</datasette-file>".format(v=value, col=col_attr, url=url)
    )


@hookimpl
def extra_js_urls(template, database, table, columns, view_name, request, datasette):
    if view_name in ("table", "row", "database"):
        return [
            {
                "url": "/-/static-plugins/datasette_files/datasette-file-cell.js",
                "module": True,
            }
        ]
    return []


@hookimpl
async def extra_body_script(
    template, database, table, columns, view_name, request, datasette
):
    if view_name not in ("table", "row") or table is None:
        return ""
    from datasette.resources import TableResource

    can_update = await datasette.allowed(
        action="update-row",
        resource=TableResource(database=database, table=table),
        actor=request.actor,
    )
    column_types = await datasette.get_column_types(database, table)
    file_columns = [
        column_name
        for column_name, column_type in column_types.items()
        if column_type.name == FileColumnType.name
    ]
    return "window.__datasette_files = {};".format(
        json.dumps(
            {
                "canUpdate": can_update,
                "database": database,
                "fileColumns": file_columns,
                "table": table,
            }
        )
    )


import asyncio
import csv
import io
import sqlite_utils


def _parse_csv_preview(content_bytes, max_rows=10):
    """Parse CSV content and return (columns, rows, dialect).

    Uses csv.Sniffer to detect delimiter. First row is used as headers.
    """
    text = content_bytes.decode("utf-8", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(text[:8192])
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(text), dialect)
    columns = next(reader, None)
    if columns is None:
        return [], [], dialect
    rows = []
    for row in reader:
        rows.append(row)
        if len(rows) >= max_rows:
            break
    return columns, rows, dialect


async def _run_csv_import(
    datasette, import_id, storage, file_path, target_db, table_name
):
    """Run CSV import as a background task, updating progress in _datasette_files_imports."""
    from sqlite_utils.utils import TypeTracker as _TypeTracker

    internal_db = datasette.get_internal_database()

    try:
        # Mark as running
        await internal_db.execute_write(
            "UPDATE _datasette_files_imports SET status = 'running' WHERE id = :id",
            {"id": import_id},
        )

        # Read file content
        content = await storage.read_file(file_path)
        total_size = len(content)
        text = content.decode("utf-8", errors="replace")

        # Sniff delimiter
        try:
            dialect = csv.Sniffer().sniff(text[:8192])
        except csv.Error:
            dialect = csv.excel

        reader = csv.reader(io.StringIO(text), dialect)
        columns = next(reader, None)
        if columns is None:
            await internal_db.execute_write(
                "UPDATE _datasette_files_imports SET status = 'error', error = 'No columns found', finished_at = datetime('now') WHERE id = :id",
                {"id": import_id},
            )
            return

        # Collect all rows through TypeTracker
        tracker = _TypeTracker()
        batch = []
        batch_size = 500
        row_count = 0
        bytes_read = 0

        for row_values in reader:
            if not row_values:
                continue
            row_dict = dict(zip(columns, row_values))
            batch.append(row_dict)
            # Estimate bytes read from row content
            bytes_read += sum(len(v) for v in row_values) + len(row_values)
            row_count += 1

            if len(batch) >= batch_size:
                wrapped = list(tracker.wrap(batch))
                await target_db.execute_write_fn(
                    lambda conn, rows=wrapped: sqlite_utils.Database(conn)[
                        table_name
                    ].insert_all(rows)
                )
                await internal_db.execute_write(
                    "UPDATE _datasette_files_imports SET row_count = :row_count, bytes_read = :bytes_read WHERE id = :id",
                    {
                        "id": import_id,
                        "row_count": row_count,
                        "bytes_read": min(bytes_read, total_size),
                    },
                )
                batch = []

        # Insert remaining rows
        if batch:
            wrapped = list(tracker.wrap(batch))
            await target_db.execute_write_fn(
                lambda conn, rows=wrapped: sqlite_utils.Database(conn)[
                    table_name
                ].insert_all(rows)
            )

        # Apply detected types via transform
        detected_types = tracker.types
        if detected_types:
            await target_db.execute_write_fn(
                lambda conn: sqlite_utils.Database(conn)[table_name].transform(
                    types=detected_types
                )
            )

        # Mark as finished
        await internal_db.execute_write(
            """UPDATE _datasette_files_imports
               SET status = 'finished', row_count = :row_count,
                   bytes_read = :total_size, finished_at = datetime('now')
               WHERE id = :id""",
            {"id": import_id, "row_count": row_count, "total_size": total_size},
        )

    except Exception as e:
        await internal_db.execute_write(
            """UPDATE _datasette_files_imports
               SET status = 'error', error = :error, finished_at = datetime('now')
               WHERE id = :id""",
            {"id": import_id, "error": str(e)},
        )


async def import_file_view(request, datasette):
    """GET/POST /-/files/import/{file_id} - import a file as a database table."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    await _check_browse_permission(datasette, request, row["source_slug"])

    source_slug = row["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]
    file_dict = dict(row)

    if request.method == "GET":
        # Read file content for preview
        content = await storage.read_file(row["path"])
        # Use up to 64KB for preview/sniffing
        preview_bytes = content[: 64 * 1024]
        columns, preview_rows, dialect = _parse_csv_preview(preview_bytes)

        # Check if there are more rows than shown
        total_lines = content.count(b"\n")
        has_more_rows = total_lines > len(preview_rows) + 1

        # Default table name from filename without extension
        filename = row["filename"]
        default_table_name = filename.rsplit(".", 1)[0] if "." in filename else filename

        # Get writable databases
        databases = [
            db_name
            for db_name in datasette.databases
            if db_name != "_internal" and not datasette.get_database(db_name).is_memory
        ]
        if not databases:
            # Fallback to memory databases if no file-based DBs
            databases = [
                db_name for db_name in datasette.databases if db_name != "_internal"
            ]

        return Response.html(
            await datasette.render_template(
                "files_import.html",
                {
                    "file": file_dict,
                    "columns": columns,
                    "preview_rows": preview_rows,
                    "has_more_rows": has_more_rows,
                    "default_table_name": default_table_name,
                    "databases": databases,
                },
                request=request,
            )
        )

    # POST: start the import
    form = await request.post_vars()
    table_name = form.get("table_name", "").strip()
    database_name = form.get("database_name", "").strip()

    if not table_name:
        return Response.json({"error": "table_name is required"}, status=400)
    if not database_name:
        return Response.json({"error": "database_name is required"}, status=400)

    # Verify target database exists
    try:
        target_db = datasette.get_database(database_name)
    except KeyError:
        return Response.json(
            {"error": f"Database not found: {database_name}"}, status=400
        )

    # Check create-table permission
    from datasette.resources import DatabaseResource

    if not await datasette.allowed(
        action="create-table",
        resource=DatabaseResource(database=database_name),
        actor=request.actor,
    ):
        return Response.json({"error": "Permission denied: create-table"}, status=403)

    # Check insert-row permission
    if not await datasette.allowed(
        action="insert-row",
        resource=DatabaseResource(database=database_name),
        actor=request.actor,
    ):
        return Response.json({"error": "Permission denied: insert-row"}, status=403)

    # Check table does not already exist
    table_names = await target_db.table_names()
    if table_name in table_names:
        return Response.json(
            {"error": f"Table already exists: {table_name}"}, status=400
        )

    # Create import job record
    internal_db = datasette.get_internal_database()
    file_size = row["size"] or 0
    actor_id = (request.actor or {}).get("id")

    result = await internal_db.execute_write(
        """
        INSERT INTO _datasette_files_imports
            (file_id, import_type, database_name, table_name, status, total_size, actor_id)
        VALUES
            (:file_id, 'csv', :database_name, :table_name, 'pending', :total_size, :actor_id)
        """,
        {
            "file_id": file_id,
            "database_name": database_name,
            "table_name": table_name,
            "total_size": file_size,
            "actor_id": actor_id,
        },
    )
    import_id = result.lastrowid

    # Launch async import task
    asyncio.create_task(
        _run_csv_import(
            datasette, import_id, storage, row["path"], target_db, table_name
        )
    )

    return Response.redirect(
        datasette.urls.path(f"/-/files/import/{file_id}/{import_id}")
    )


async def import_progress_view(request, datasette):
    """GET /-/files/import/{file_id}/{import_id}[.json] - import progress page or JSON."""
    file_id = request.url_vars["file_id"]
    import_id = request.url_vars["import_id"]

    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    await _check_browse_permission(datasette, request, row["source_slug"])

    internal_db = datasette.get_internal_database()
    job_row = (
        await internal_db.execute(
            "SELECT * FROM _datasette_files_imports WHERE id = :id AND file_id = :file_id",
            {"id": import_id, "file_id": file_id},
        )
    ).first()

    if job_row is None:
        raise NotFound(f"Import job not found: {import_id}")

    job = dict(job_row)

    is_json = request.path.endswith(".json")
    if is_json:
        table_url = f"/{job['database_name']}/{job['table_name']}"
        return Response.json(
            {
                "id": job["id"],
                "file_id": job["file_id"],
                "import_type": job["import_type"],
                "database_name": job["database_name"],
                "table_name": job["table_name"],
                "status": job["status"],
                "row_count": job["row_count"],
                "total_size": job["total_size"],
                "bytes_read": job["bytes_read"],
                "error": job["error"],
                "started_at": job["started_at"],
                "finished_at": job["finished_at"],
                "table_url": table_url,
            }
        )

    return Response.html(
        await datasette.render_template(
            "files_import_progress.html",
            {
                "file": dict(row),
                "job": job,
            },
            request=request,
        )
    )


@hookimpl
def file_actions(datasette, actor, file, preview_bytes):
    """Suggest CSV/TSV import for files that look like CSV or TSV."""
    filename = file.get("filename", "")
    content_type = file.get("content_type", "")
    if content_type in ("text/csv", "text/tab-separated-values") or filename.endswith(
        (".csv", ".tsv")
    ):
        return [
            {
                "href": datasette.urls.path(f"/-/files/import/{file['id']}"),
                "label": "Import as table",
                "description": "Import this CSV file as a database table",
            },
        ]
    return []


@hookimpl
async def homepage_actions(datasette, actor, request):
    resources_sql = await datasette.allowed_resources_sql(
        action="files-browse",
        actor=actor,
    )
    db = datasette.get_internal_database()
    allowed_rows = (await db.execute(resources_sql.sql, resources_sql.params)).rows
    if allowed_rows:
        return [
            {
                "href": datasette.urls.path("/-/files"),
                "label": "Manage files",
                "description": "Browse and manage uploaded files",
            }
        ]


@hookimpl
def register_thumbnail_generators(datasette):
    from .pillow_thumbnails import PillowThumbnailGenerator

    return [PillowThumbnailGenerator()]


@hookimpl
def skip_csrf(datasette, scope):
    if scope["type"] != "http":
        return False
    path = scope["path"]
    if path.startswith("/-/files/upload/"):
        return True
    # Match /-/files/{file_id}/-/delete and /-/files/{file_id}/-/update
    if _FILE_ID_RE.match(path.split("/")[-2] if path.count("/") >= 4 else ""):
        if path.endswith("/-/delete") or path.endswith("/-/update"):
            return True
