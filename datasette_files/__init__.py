import json
from datasette import hookimpl, Response, NotFound
from datasette.plugins import pm
from datasette.utils import await_me_maybe
from ulid import ULID
from . import hookspecs
from .base import StorageCapabilities
from .filesystem import FilesystemStorage

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
    UNIQUE(source_id, path)
);
"""


def _sanitize_filename(filename):
    """Remove path separators and other dangerous characters from a filename."""
    # Strip directory components
    filename = filename.replace("/", "_").replace("\\", "_")
    # Remove null bytes
    filename = filename.replace("\x00", "")
    return filename or "unnamed"


@hookimpl
def startup(datasette):
    async def inner():
        db = datasette.get_internal_database()
        await db.execute_write_script(CREATE_SQL)

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


async def upload_file(request, datasette):
    """POST /-/files/upload/{source_slug} — upload a file."""
    source_slug = request.url_vars["source_slug"]
    if source_slug not in _sources:
        raise NotFound(f"Source not found: {source_slug}")

    storage = _sources[source_slug]
    meta = _source_meta[source_slug]

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
    """GET /-/files/{file_id} — HTML info page about a file."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    return Response.html(
        await datasette.render_template(
            "file_info.html",
            {"file": dict(row)},
            request=request,
        )
    )


async def file_json(request, datasette):
    """GET /-/files/{file_id}.json — file metadata as JSON."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

    return Response.json(dict(row))


async def file_download(request, datasette):
    """GET /-/files/{file_id}/download — download the file."""
    file_id = request.url_vars["file_id"]
    row = await _get_file_record(datasette, file_id)
    if row is None:
        raise NotFound(f"File not found: {file_id}")

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


async def sources_json(request, datasette):
    """GET /-/files/sources.json — list all configured sources."""
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


@hookimpl
def register_routes():
    return [
        (r"^/-/files/sources\.json$", sources_json),
        (r"^/-/files/upload/(?P<source_slug>[^/]+)$", upload_file),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)\.json$", file_json),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)/download$", file_download),
        (r"^/-/files/(?P<file_id>df-[a-z0-9]+)$", file_info),
    ]


@hookimpl
def skip_csrf(datasette, scope):
    if scope["type"] == "http" and scope["path"].startswith("/-/files/upload/"):
        return True
