import json
import re
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
        await db.execute_write_script(
            """
            DROP TRIGGER IF EXISTS datasette_files_ai;
            DROP TRIGGER IF EXISTS datasette_files_ad;
            DROP TRIGGER IF EXISTS datasette_files_au;
            DROP TABLE IF EXISTS datasette_files_fts;
            """
        )
        await db.execute_write_script(FTS_SQL)
        await db.execute_write_script(FTS_TRIGGERS_SQL)

        # Backfill FTS from content table
        await db.execute_write(
            """
            INSERT INTO datasette_files_fts(rowid, id, filename, content_type, search_text)
            SELECT rowid, id, filename, content_type, search_text FROM datasette_files
            """
        )

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

    if request.method == "GET":
        # Check upload permission for the form page
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

    return Response.html(
        await datasette.render_template(
            "file_info.html",
            {"file": dict(row), "can_edit": can_edit, "saved": saved},
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
        # No query — list recent files filtered to allowed sources
        placeholders = ",".join(f":_slug_{i}" for i in range(len(allowed_slugs)))
        search_sql = """
            SELECT f.id, f.filename, f.content_type, f.size, f.width, f.height,
                   f.created_at, f.uploaded_by, s.slug as source_slug
            FROM datasette_files f
            JOIN datasette_files_sources s ON f.source_id = s.id
            WHERE s.slug IN ({placeholders})
            {source_where}
            ORDER BY f.created_at DESC
            LIMIT 50
        """.format(
            placeholders=placeholders,
            source_where="AND s.slug = :source_filter" if source_filter else "",
        )
        params = {}
        for i, slug in enumerate(allowed_slugs):
            params[f"_slug_{i}"] = slug
        if source_filter:
            params["source_filter"] = source_filter
        files = [dict(row) for row in (await db.execute(search_sql, params)).rows]

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
            },
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
        (r"^/-/files/upload/(?P<source_slug>[^/]+)$", upload_file),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)\.json$", file_json),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)/download$", file_download),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)$", file_info),
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


_FILE_INFO_PATH_RE = re.compile(r"^/-/files/df-[a-z0-9]{26}$")


@hookimpl
def skip_csrf(datasette, scope):
    if scope["type"] != "http":
        return False
    path = scope["path"]
    if path.startswith("/-/files/upload/"):
        return True
    # POST to file info page (search_text edit form)
    if _FILE_INFO_PATH_RE.match(path):
        return True
