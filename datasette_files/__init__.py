import json
import re
import time
from datasette import hookimpl, Response, NotFound, Forbidden
from datasette.permissions import Action, PermissionSQL, Resource
from datasette.plugins import pm
from datasette.utils import await_me_maybe
from markupsafe import Markup
from ulid import ULID
from . import hookspecs
from .base import StorageCapabilities
from .filesystem import FilesystemStorage

_FILE_ID_RE = re.compile(r"^df-[a-z0-9]{26}$")

# Upload token store: {token: {source_slug, filename, content_type, size, path, file_id, created_at, used}}
_upload_tokens = {}
_UPLOAD_TOKEN_TTL = 3600  # 1 hour

pm.add_hookspecs(hookspecs)

# Built-in storage types (always available, no plugin needed)
BUILT_IN_STORAGE_TYPES = {"filesystem": FilesystemStorage}

# Registry of configured source instances: {slug: storage_instance}
_sources = {}
# Registry of source metadata: {slug: {slug, storage_type, source_id, ...}}
_source_meta = {}

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


# --- Resource and Action definitions ---


class FileSourceResource(Resource):
    """A file source in datasette-files."""

    name = "file-source"
    parent_class = None  # Top-level resource

    def __init__(self, source_slug: str):
        super().__init__(parent=source_slug, child=None)

    @classmethod
    async def resources_sql(cls, datasette) -> str:
        return "SELECT slug AS parent, NULL AS child FROM datasette_files_sources"


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
            description="Edit file metadata in a source",
            resource_class=FileSourceResource,
        ),
        Action(
            name="files-delete",
            abbr="fd",
            description="Delete files from a source",
            resource_class=FileSourceResource,
        ),
    ]


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


# --- Helpers ---


def _sanitize_filename(filename):
    """Remove path separators and other dangerous characters from a filename."""
    # Strip directory components
    filename = filename.replace("/", "_").replace("\\", "_")
    # Remove null bytes
    filename = filename.replace("\x00", "")
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

    return inner


# --- Route handlers ---


async def upload_file(request, datasette):
    """GET/POST /-/files/upload/{source_slug} - upload form and handler."""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]
    meta = _source_meta[source_slug]

    # Check upload permission for both GET (form page) and POST (actual upload)
    can_upload = await datasette.allowed(
        action="files-upload",
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not can_upload:
        raise Forbidden("Permission denied: files-upload on source " + source_slug)

    if request.method == "GET":
        return Response.html(
            await datasette.render_template(
                "files_upload.html",
                {"source_slug": source_slug},
                request=request,
            )
        )

    # Parse the multipart upload
    form = await request.form(files=True)
    uploaded = form.get("file")
    if uploaded is None or not hasattr(uploaded, "read"):
        return Response.json({"error": "No file provided"}, status=400)

    content = await uploaded.read()
    filename = _sanitize_filename(uploaded.filename or "unnamed")
    content_type = uploaded.content_type or "application/octet-stream"

    # Generate file ID and path
    file_id = "df-" + str(ULID()).lower()
    ulid_part = file_id[3:]
    path = f"{ulid_part}/{filename}"

    # Store the file
    file_meta = await storage.receive_upload(path, content, content_type)

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
            "size": file_meta.size or len(content),
            "uploaded_by": (request.actor or {}).get("id"),
        },
    )

    await form.aclose()

    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return Response.redirect(f"/-/files/{file_id}")

    return Response.json(
        {
            "file_id": file_id,
            "filename": filename,
            "content_type": content_type,
            "size": file_meta.size or len(content),
            "url": f"/-/files/{file_id}",
        }
    )


def _clean_expired_tokens():
    """Remove expired upload tokens."""
    now = time.time()
    expired = [t for t, v in _upload_tokens.items() if now - v["created_at"] > _UPLOAD_TOKEN_TTL]
    for t in expired:
        del _upload_tokens[t]


async def _check_upload_permission_json(datasette, request, source_slug):
    """Check files-upload permission, return error Response or None."""
    allowed = await datasette.allowed(
        action="files-upload",
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not allowed:
        return Response.json(
            {"ok": False, "errors": ["Permission denied"]}, status=403
        )
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
        return Response.json({"ok": False, "errors": ["Invalid JSON"]}, status=400)

    filename = body.get("filename")
    if not filename:
        return Response.json(
            {"ok": False, "errors": ["filename is required"]}, status=400
        )

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
    _upload_tokens[token] = {
        "source_slug": source_slug,
        "filename": filename,
        "content_type": content_type,
        "size": size,
        "path": path,
        "file_id": file_id,
        "created_at": time.time(),
        "used": False,
        "content_received": False,
        "actor": request.actor,
    }

    # Build upload URL - for filesystem, it points to our content endpoint
    upload_url = f"/-/files/upload/{source_slug}/-/content"

    return Response.json({
        "ok": True,
        "upload_token": token,
        "upload_url": upload_url,
        "upload_method": "POST",
        "upload_headers": {},
        "upload_fields": {
            "upload_token": token,
        },
    })


async def upload_content(request, datasette):
    """POST /-/files/upload/{source_slug}/-/content - receive file bytes (filesystem proxy)."""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]

    # Parse multipart form
    form = await request.form(files=True)

    token_value = form.get("upload_token")
    if not token_value or (hasattr(token_value, "read") and not token_value):
        # Try string value
        pass
    if hasattr(token_value, "read"):
        token_value = None

    if not token_value:
        await form.aclose()
        return Response.json(
            {"ok": False, "errors": ["upload_token is required"]}, status=400
        )

    token_data = _upload_tokens.get(token_value)
    if not token_data:
        await form.aclose()
        return Response.json(
            {"ok": False, "errors": ["Invalid or expired upload token"]}, status=400
        )

    if token_data["source_slug"] != source_slug:
        await form.aclose()
        return Response.json(
            {"ok": False, "errors": ["Token does not match this source"]}, status=400
        )

    if token_data["content_received"]:
        await form.aclose()
        return Response.json(
            {"ok": False, "errors": ["Content already uploaded for this token"]},
            status=400,
        )

    uploaded = form.get("file")
    if uploaded is None or not hasattr(uploaded, "read"):
        await form.aclose()
        return Response.json(
            {"ok": False, "errors": ["No file provided"]}, status=400
        )

    content = await uploaded.read()
    content_type = token_data["content_type"]
    path = token_data["path"]

    # Store the file
    file_meta = await storage.receive_upload(path, content, content_type)

    # Save metadata on the token for the complete step
    token_data["content_received"] = True
    token_data["file_meta"] = file_meta
    token_data["actual_size"] = len(content)

    await form.aclose()

    return Response.json({"ok": True})


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
        return Response.json({"ok": False, "errors": ["Invalid JSON"]}, status=400)

    token_value = body.get("upload_token")
    if not token_value:
        return Response.json(
            {"ok": False, "errors": ["upload_token is required"]}, status=400
        )

    token_data = _upload_tokens.get(token_value)
    if not token_data:
        return Response.json(
            {"ok": False, "errors": ["Invalid or expired upload token"]}, status=400
        )

    if token_data["source_slug"] != source_slug:
        return Response.json(
            {"ok": False, "errors": ["Token does not match this source"]}, status=400
        )

    if not token_data["content_received"]:
        return Response.json(
            {"ok": False, "errors": ["File content has not been uploaded yet"]},
            status=400,
        )

    if token_data["used"]:
        return Response.json(
            {"ok": False, "errors": ["This upload token has already been used"]},
            status=400,
        )

    # Mark token as used
    token_data["used"] = True

    file_id = token_data["file_id"]
    file_meta = token_data["file_meta"]
    filename = token_data["filename"]
    content_type = token_data["content_type"]
    path = token_data["path"]
    actor = token_data["actor"]

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
            "size": file_meta.size or token_data.get("actual_size"),
            "uploaded_by": (actor or {}).get("id"),
        },
    )

    # Clean up token
    del _upload_tokens[token_value]

    # Fetch the created record for the response
    row = await _get_file_record(datasette, file_id)

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
                "url": f"/-/files/{file_id}",
                "download_url": f"/-/files/{file_id}/download",
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
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not allowed:
        raise Forbidden("Permission denied: files-delete on source " + source_slug)

    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]

    if not storage.capabilities.can_delete:
        return Response.json(
            {"ok": False, "errors": ["This storage backend does not support deletion"]},
            status=400,
        )

    # Delete from storage backend
    await storage.delete_file(row["path"])

    # Delete from internal database
    db = datasette.get_internal_database()
    await db.execute_write(
        "DELETE FROM datasette_files WHERE id = ?", [file_id]
    )

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
        resource=FileSourceResource(source_slug),
        actor=request.actor,
    )
    if not allowed:
        raise Forbidden("Permission denied: files-edit on source " + source_slug)

    try:
        body = json.loads(await request.post_body())
    except (json.JSONDecodeError, ValueError):
        return Response.json({"ok": False, "errors": ["Invalid JSON"]}, status=400)

    update = body.get("update")
    if not update or not isinstance(update, dict):
        return Response.json(
            {"ok": False, "errors": ["update object is required"]}, status=400
        )

    # Only allow editing search_text for now
    allowed_fields = {"search_text"}
    invalid_fields = set(update.keys()) - allowed_fields
    if invalid_fields:
        return Response.json(
            {
                "ok": False,
                "errors": [f"Cannot update fields: {', '.join(sorted(invalid_fields))}"],
            },
            status=400,
        )

    if not update:
        return Response.json(
            {"ok": False, "errors": ["No valid fields to update"]}, status=400
        )

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


async def file_info(request, datasette):
    """GET/POST /-/files/{file_id} - HTML info page about a file.

    POST updates the search_text field (requires files-edit permission).
    """
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    await _check_browse_permission(datasette, request, row["source_slug"])

    saved = False

    if request.method == "POST":
        # Check files-edit permission
        can_edit = await datasette.allowed(
            action="files-edit",
            resource=FileSourceResource(row["source_slug"]),
            actor=request.actor,
        )
        if not can_edit:
            raise Forbidden(
                "Permission denied: files-edit on source " + row["source_slug"]
            )

        form = await request.post_vars()
        search_text = form.get("search_text", "")

        db = datasette.get_internal_database()
        await db.execute_write(
            "UPDATE datasette_files SET search_text = :search_text WHERE id = :id",
            {"search_text": search_text, "id": file_id},
        )

        # Re-fetch the updated row
        row = await _get_file_record(datasette, file_id)
        saved = True

    # Check if current actor can edit (for showing/hiding the form)
    can_edit = await datasette.allowed(
        action="files-edit",
        resource=FileSourceResource(row["source_slug"]),
        actor=request.actor,
    )

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
                "can_edit": can_edit,
                "saved": saved,
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

    # Otherwise proxy the content
    content = await storage.read_file(row["path"])
    content_type = row["content_type"] or "application/octet-stream"
    return Response(
        body=content,
        content_type=content_type,
        headers={
            "Content-Disposition": f'inline; filename="{row["filename"]}"',
        },
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
            "download_url": f"/-/files/{file_id}/download",
            "info_url": f"/-/files/{file_id}",
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
        # New unified upload API (prepare/content/complete)
        (r"^/-/files/upload/(?P<source_slug>[^/]+)/-/prepare$", upload_prepare),
        (r"^/-/files/upload/(?P<source_slug>[^/]+)/-/content$", upload_content),
        (r"^/-/files/upload/(?P<source_slug>[^/]+)/-/complete$", upload_complete),
        # Legacy upload endpoint (still works)
        (r"^/-/files/upload/(?P<source_slug>[^/]+)$", upload_file),
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
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)/download$", file_download),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)$", file_info),
        (r"^/-/files/source/(?P<source_slug>[^/]+)$", source_files),
        (r"^/-/files$", files_index),
    ]


@hookimpl
def render_cell(value, column, table, database, datasette, request):
    if not isinstance(value, str) or not _FILE_ID_RE.match(value):
        return None
    col_attr = (
        ' data-column="{}"'.format(Markup.escape(column)) if table is not None else ""
    )
    return Markup(
        '<datasette-file file-id="{v}"{col}>'
        '<a href="/-/files/{v}">{v}</a>'
        "</datasette-file>".format(v=value, col=col_attr)
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
    return "window.__datasette_files = {};".format(
        json.dumps(
            {
                "canUpdate": can_update,
                "database": database,
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

    return Response.redirect(f"/-/files/import/{file_id}/{import_id}")


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
    """Suggest CSV import for files that look like CSV."""
    filename = file.get("filename", "")
    content_type = file.get("content_type", "")
    if content_type == "text/csv" or filename.endswith(".csv"):
        return [
            {
                "href": f"/-/files/import/{file['id']}",
                "label": "Import as table",
                "description": "Import this CSV file as a database table",
            },
        ]
    return []


_FILE_INFO_PATH_RE = re.compile(r"^/-/files/df-[a-z0-9]{26}$")


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
    # POST to file info page (search_text edit form)
    if _FILE_INFO_PATH_RE.match(path):
        return True
