import os
import time
import boto3
import json
from datasette import hookimpl, Response
from datasette.plugins import pm
from ulid import ULID
from . import hookspecs

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
   mtime INTEGER,
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


@hookimpl
def register_routes():
    return [
        (r"^/-/files/s3/upload$", s3_upload),
        (r"^/-/files/complete$", upload_complete),
    ]


@hookimpl
def skip_csrf(datasette, scope):
    return scope["path"] in (
        datasette.urls.path("/-/files/s3/upload"),
        datasette.urls.path("/-/files/complete"),
    )
