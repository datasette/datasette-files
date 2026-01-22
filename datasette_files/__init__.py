import os
import time
import datetime
import boto3
import json
import re
import markupsafe
from datasette import hookimpl, Response, Forbidden, NotFound
from datasette.permissions import Action
from datasette.utils import await_me_maybe
from datasette.plugins import pm
from ulid import ULID
from . import hookspecs

pm.add_hookspecs(hookspecs)

# Configuration – set these as appropriate for your environment.
S3_BUCKET = os.getenv("S3_BUCKET", "datasette-files-cors-bucket")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Initialize the boto3 client.
s3_client = boto3.client("s3", region_name=AWS_REGION)

# Pattern to match df-{ulid} file references
FILE_REF_PATTERN = re.compile(r"^df-([0-9a-z]{26})$", re.IGNORECASE)


def extract_filename(path):
    """Extract filename from path like 'uploads/{ulid}/{filename}'."""
    return path.split("/")[-1] if path else ""


def format_file_size(size):
    """Format file size in human-readable form."""
    if size is None:
        return "Unknown"
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def is_image_type(file_type):
    """Check if the file type is an image."""
    if not file_type:
        return False
    return file_type.startswith("image/")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS files_sources (
   id INTEGER PRIMARY KEY,
   name TEXT NOT NULL,
   type TEXT NOT NULL,
   config TEXT,
   secrets TEXT,
   UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS files_files (
   ulid TEXT PRIMARY KEY, -- str(ULID()).lower()
   source_id INTEGER NOT NULL,
   path TEXT NOT NULL,
   size INTEGER,
   mtime INTEGER, -- no ctime because some providers don't offer it
   type TEXT,
   metadata TEXT,
   FOREIGN KEY (source_id) REFERENCES files_sources(id),
   UNIQUE(path)
);

CREATE TABLE IF NOT EXISTS files_pending (
   ulid TEXT PRIMARY KEY, -- str(ULID()).lower()
   filename TEXT NOT NULL,
   path TEXT NOT NULL,
   size INTEGER NOT NULL,
   type TEXT NOT NULL
);


CREATE INDEX IF NOT EXISTS idx_files_path ON files_files(path);
"""


@hookimpl
def register_actions(datasette):
    return [
        Action(
            name="debug-storages",
            description="Debug storages",
        )
    ]


@hookimpl
def startup(datasette):
    async def inner():
        db = datasette.get_internal_database()
        await db.execute_write_script(CREATE_SQL)

    return inner


async def s3_upload(request, datasette):
    """
    Endpoint: POST /-/files/s3/upload

    Expects JSON with:
      - filename
      - size
      - type
    Returns a JSON with upload details (a presigned POST) and an on_complete URL.
    """
    if request.method != "POST":
        return Response.html(
            await datasette.render_template("files_s3_upload.html", request=request)
        )

    try:
        body = await request.post_body()
        data = json.loads(body.decode())
    except ValueError as ex:
        return Response.json({"error": "Invalid JSON: {}".format(str(ex))}, status=400)

    filename = data.get("filename")
    file_size = data.get("size")
    file_type = data.get("type")

    if not filename or not file_type:
        return Response.json({"error": "Missing filename or file type"}, status=400)
    if not isinstance(file_size, int):
        return Response.json({"error": "Invalid file size"}, status=400)

    # Generate a unique ID for the upload.
    upload_id = str(ULID()).lower()
    # Create an S3 key. For example: uploads/<upload_id>/<filename>
    key = f"uploads/{upload_id}/{filename}"

    # Create a presigned POST so the client can upload directly to S3.
    presigned_post = s3_client.generate_presigned_post(
        Bucket=S3_BUCKET,
        Key=key,
        Fields={"Content-Type": file_type},
        Conditions=[{"Content-Type": file_type}],
        ExpiresIn=3600,
    )

    # Log to console for debugging.
    print(
        f"[UPLOAD] Prepared upload for '{filename}' (size: {file_size}, type: {file_type}) with ID: {upload_id}"
    )

    db = datasette.get_internal_database()
    await db.execute_write(
        """
        insert into files_pending (ulid, filename, path, size, type)
        values (?, ?, ?, ?, ?)
    """,
        (upload_id, filename, key, file_size, file_type),
    )

    # Return the details in the expected format.
    return Response.json(
        {
            "upload": {
                "url": presigned_post["url"],
                "method": "POST",
                "headers": presigned_post["fields"],
            },
            "on_complete": {"url": f"/-/files/complete?id={upload_id}"},
        }
    )


async def upload_complete(request, datasette):
    """
    Endpoint: POST /-/files/complete?id=...

    This endpoint is called once the client finishes uploading the file.
    """
    upload_id = request.args.get("id")
    if not upload_id:
        return Response.json({"error": "Missing id parameter"}, status=400)

    db = datasette.get_internal_database()
    details = (
        await db.execute("select * from files_pending where ulid = ?", (upload_id,))
    ).first()

    await db.execute_write(
        """
        insert into files_files (ulid, source_id, path, size, mtime, type, metadata)
        values (?, ?, ?, ?, ?, ?, ?)
    """,
        (
            upload_id,
            1,
            details["path"],
            details["size"],
            int(time.time()),
            details["type"],
            "{}",
        ),
    )
    await db.execute_write(
        """
        delete from files_pending where ulid = ?
    """,
        (upload_id,),
    )

    return Response.json(
        {"status": "success", "id": upload_id, "details": dict(details)}
    )


async def file_detail(request, datasette):
    """
    Endpoint: GET /-/files/{ulid}

    Display file details page with metadata and inline preview for images.
    """
    ulid = request.url_vars["ulid"]

    db = datasette.get_internal_database()
    file_row = (
        await db.execute("select * from files_files where ulid = ?", (ulid,))
    ).first()

    if not file_row:
        raise NotFound("File not found")

    file_info = dict(file_row)
    filename = extract_filename(file_info["path"])
    file_type = file_info.get("type", "")
    is_image = is_image_type(file_type)

    # Generate expiring download URL
    download_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": file_info["path"]},
        ExpiresIn=3600,
    )

    # Format mtime
    mtime = file_info.get("mtime")
    if mtime:
        mtime_formatted = datetime.datetime.fromtimestamp(mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    else:
        mtime_formatted = "Unknown"

    return Response.html(
        await datasette.render_template(
            "files_detail.html",
            {
                "ulid": ulid,
                "filename": filename,
                "size": format_file_size(file_info.get("size")),
                "size_bytes": file_info.get("size"),
                "type": file_type,
                "mtime": mtime_formatted,
                "is_image": is_image,
                "download_url": download_url,
            },
            request=request,
        )
    )


async def file_download(request, datasette):
    """
    Endpoint: GET /-/files/{ulid}/download

    Redirect to an expiring S3 presigned URL for download.
    """
    ulid = request.url_vars["ulid"]

    db = datasette.get_internal_database()
    file_row = (
        await db.execute("select * from files_files where ulid = ?", (ulid,))
    ).first()

    if not file_row:
        raise NotFound("File not found")

    file_info = dict(file_row)
    filename = extract_filename(file_info["path"])

    # Generate expiring download URL with content-disposition for download
    download_url = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": S3_BUCKET,
            "Key": file_info["path"],
            "ResponseContentDisposition": f'attachment; filename="{filename}"',
        },
        ExpiresIn=3600,
    )

    return Response.redirect(download_url)


async def debug_storages(datasette, request):
    if not await datasette.allowed(actor=request.actor, action="debug-storages"):
        raise Forbidden("Needs debug-storages permission")
    storages = await load_storages(datasette)
    return Response.json(
        {"storages": [obj.__dict__ for obj in storages]},
        default=lambda obj: (
            obj.isoformat() if isinstance(obj, datetime.datetime) else obj
        ),
    )


async def list_storage(datasette, request):
    if not await datasette.allowed(actor=request.actor, action="debug-storages"):
        raise Forbidden("Needs debug-storages permission")
    name = request.url_vars["name"]
    storages = await load_storages(datasette)
    matches = [storage for storage in storages if storage.name == name]
    if not matches:
        raise NotFound("Storage not found")
    storage = matches[0]
    return Response.json(
        {
            "files": list([obj async for obj in storage.list_files()]),
        },
        default=special_repr,
    )


def special_repr(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif hasattr(obj, "__dict__"):
        return obj.__dict__
    else:
        return obj


async def load_storages(datasette):
    storages = []
    for hook in pm.hook.register_files_storages(datasette=datasette):
        extra_storages = await await_me_maybe(hook)
        if extra_storages:
            storages.extend(extra_storages)
    return storages


async def get_file_info(datasette, ulid):
    """Look up file info by ULID from internal database."""
    db = datasette.get_internal_database()
    file_row = (
        await db.execute("select * from files_files where ulid = ?", (ulid,))
    ).first()
    if not file_row:
        return None
    return dict(file_row)


def render_file_reference(ulid, file_info):
    """Render HTML for a single file reference."""
    if file_info is None:
        # File not found - render as broken reference
        return f'<span class="df-file df-missing" title="File not found">df-{markupsafe.escape(ulid)}</span>'

    filename = extract_filename(file_info["path"])
    size = format_file_size(file_info.get("size"))
    file_type = file_info.get("type", "")

    # Determine icon based on file type
    if is_image_type(file_type):
        icon = "🖼️"
    elif file_type and file_type.startswith("video/"):
        icon = "🎬"
    elif file_type and file_type.startswith("audio/"):
        icon = "🎵"
    elif file_type == "application/pdf":
        icon = "📄"
    else:
        icon = "📎"

    return (
        f'<a href="/-/files/{markupsafe.escape(ulid)}" class="df-file" '
        f'title="{markupsafe.escape(file_type)}">'
        f'{icon} {markupsafe.escape(filename)} <span class="df-size">({size})</span></a>'
    )


@hookimpl
def render_cell(value, datasette):
    """
    Render cells containing file references.

    Detects:
    - Single file: "df-{ulid}"
    - Multiple files: ["df-{ulid1}", "df-{ulid2}"]
    """
    async def inner():
        if value is None:
            return None

        items = None

        # Check for single string reference or JSON array string
        if isinstance(value, str):
            match = FILE_REF_PATTERN.match(value)
            if match:
                ulid = match.group(1).lower()
                file_info = await get_file_info(datasette, ulid)
                return markupsafe.Markup(render_file_reference(ulid, file_info))

            # Try parsing as JSON array
            if value.startswith("["):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        items = parsed
                except (json.JSONDecodeError, ValueError):
                    pass

            if items is None:
                return None

        elif isinstance(value, list):
            items = value
        else:
            return None

        # Check if all items match the pattern
        file_refs = []
        for item in items:
            if not isinstance(item, str):
                return None
            match = FILE_REF_PATTERN.match(item)
            if not match:
                return None
            file_refs.append(match.group(1).lower())

        if not file_refs:
            return None

        # Render all file references
        html_parts = []
        for ulid in file_refs:
            file_info = await get_file_info(datasette, ulid)
            html_parts.append(render_file_reference(ulid, file_info))

        return markupsafe.Markup('<ul class="df-file-list">' +
            "".join(f"<li>{part}</li>" for part in html_parts) +
            "</ul>")

    return inner


@hookimpl
def register_routes():
    return [
        (r"^/-/files/s3/upload$", s3_upload),
        (r"^/-/files/complete$", upload_complete),
        (r"^/-/files/storages$", debug_storages),
        (r"^/-/files/storages/list/(?P<name>[^/]+)$", list_storage),
        (r"^/-/files/(?P<ulid>[0-9a-z]{26})/download$", file_download),
        (r"^/-/files/(?P<ulid>[0-9a-z]{26})$", file_detail),
    ]


@hookimpl
def skip_csrf(datasette, scope):
    return scope["path"] in (
        datasette.urls.path("/-/files/s3/upload"),
        datasette.urls.path("/-/files/complete"),
    )
