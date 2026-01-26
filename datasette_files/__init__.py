import os
import time
import datetime
import pathlib
import boto3
import json
from datasette import hookimpl, Response, Permission, Forbidden, NotFound
from datasette.utils import await_me_maybe
from datasette.utils.multipart import UploadedFile
from datasette.permissions import Action
from datasette.plugins import pm
from ulid import ULID
from . import hookspecs
from .local import LocalDirectoryStorage
from .base import Storage, File

pm.add_hookspecs(hookspecs)

# Configuration – set these as appropriate for your environment.
S3_BUCKET = os.getenv("S3_BUCKET", "datasette-files-cors-bucket")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Initialize the boto3 client.
s3_client = boto3.client("s3", region_name=AWS_REGION)

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
def register_permissions(datasette):
    return [
        Permission(
            name="debug-storages",
            abbr=None,
            description="Debug storages",
            takes_database=False,
            takes_resource=False,
            default=False,
        )
    ]


@hookimpl
def startup(datasette):
    async def inner():
        db = datasette.get_internal_database()
        await db.execute_write_script(CREATE_SQL)

    return inner


@hookimpl
def register_actions():
    return [
        Action(
            name="debug-storages",
            description="Debug storages",
        )
    ]


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


async def debug_storages(datasette, request):
    if not await datasette.allowed(actor=request.actor, action="debug-storages"):
        raise Forbidden("Needs debug-storages permission")
    storages = await load_storages(datasette)
    return Response.json(
        {"storages": [obj.__dict__ for obj in storages]},
        default=special_repr,
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


async def local_upload(request, datasette):
    """
    Endpoint: POST /-/files/local/upload/<storage_name>

    Handles multipart form uploads directly to a local directory storage.
    Uses the new request.form() method with files=True.
    """
    storage_name = request.url_vars.get("storage_name")

    if request.method == "GET":
        return Response.html(
            await datasette.render_template(
                "files_local_upload.html",
                {"storage_name": storage_name},
                request=request
            )
        )

    # POST - handle file upload
    storages = await load_storages(datasette)
    matches = [s for s in storages if s.name == storage_name and isinstance(s, LocalDirectoryStorage)]

    if not matches:
        return Response.json(
            {"error": f"Local storage '{storage_name}' not found"},
            status=404
        )

    storage = matches[0]

    # Parse multipart form data with file support
    form_data = await request.form(files=True)

    # Get the uploaded file
    uploaded_file = form_data.get("file")

    if not uploaded_file or not isinstance(uploaded_file, UploadedFile):
        return Response.json(
            {"error": "No file uploaded. Use 'file' form field."},
            status=400
        )

    # Read the file content
    content = await uploaded_file.read()

    # Save to local storage
    saved_path = await storage.upload_file(
        filename=uploaded_file.filename,
        content=content,
        content_type=uploaded_file.content_type
    )

    # Record in database
    db = datasette.get_internal_database()
    upload_id = str(ULID()).lower()

    # Get the source_id for this storage
    source_row = (
        await db.execute(
            "SELECT id FROM files_sources WHERE name = ?",
            (storage_name,)
        )
    ).first()

    if source_row:
        source_id = source_row["id"]
    else:
        # Create source entry if it doesn't exist
        await db.execute_write(
            """
            INSERT OR IGNORE INTO files_sources (name, type, config)
            VALUES (?, ?, ?)
            """,
            (storage_name, "local", json.dumps({"directory": str(storage.directory)}))
        )
        source_row = (
            await db.execute(
                "SELECT id FROM files_sources WHERE name = ?",
                (storage_name,)
            )
        ).first()
        source_id = source_row["id"]

    await db.execute_write(
        """
        INSERT INTO files_files (ulid, source_id, path, size, mtime, type, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            upload_id,
            source_id,
            saved_path,
            uploaded_file.size,
            int(time.time()),
            uploaded_file.content_type,
            "{}",
        ),
    )

    return Response.json({
        "status": "success",
        "id": upload_id,
        "filename": uploaded_file.filename,
        "path": saved_path,
        "size": uploaded_file.size,
        "content_type": uploaded_file.content_type,
    })


def special_repr(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, pathlib.Path):
        return str(obj)
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


@hookimpl
def register_routes():
    return [
        (r"^/-/files/s3/upload$", s3_upload),
        (r"^/-/files/complete$", upload_complete),
        (r"^/-/files/storages$", debug_storages),
        (r"^/-/files/storages/list/(?P<name>[^/]+)$", list_storage),
        (r"^/-/files/local/upload/(?P<storage_name>[^/]+)$", local_upload),
    ]


@hookimpl
def skip_csrf(datasette, scope):
    path = scope["path"]
    if path in (
        datasette.urls.path("/-/files/s3/upload"),
        datasette.urls.path("/-/files/complete"),
    ):
        return True
    # Allow local upload paths (they have variable storage_name)
    if path.startswith(datasette.urls.path("/-/files/local/upload/")):
        return True
    return False


@hookimpl
def register_files_storages(datasette):
    """
    Register LocalDirectoryStorage instances from plugin configuration.

    Configuration in datasette.yaml:

        plugins:
          datasette-files:
            local-dirs:
              - name: uploads
                directory: /path/to/uploads
                base_url: https://example.com/files  # optional
    """
    config = datasette.plugin_config("datasette-files") or {}
    local_dirs = config.get("local-dirs") or []

    storages = []
    for directory_config in local_dirs:
        name = directory_config.get("name")
        directory = directory_config.get("directory")

        if not name or not directory:
            continue

        base_url = directory_config.get("base_url")

        storages.append(
            LocalDirectoryStorage(
                name=name,
                directory=directory,
                base_url=base_url
            )
        )

    return storages
