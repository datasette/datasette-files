#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "starlette",
#     "boto3",
#     "uvicorn",
# ]
# ///
import os
import uuid
import boto3

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, FileResponse
from starlette.routing import Route
import uvicorn

# Configuration – set these as appropriate for your environment.
S3_BUCKET = os.getenv("S3_BUCKET", "datasette-files-cors-bucket")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Initialize the boto3 client.
s3_client = boto3.client("s3", region_name=AWS_REGION)


async def get_upload_details(request: Request):
    """
    Endpoint: POST /api/upload/

    Expects JSON with:
      - filename
      - size
      - type
    Returns a JSON with upload details (a presigned POST) and an on_complete URL.
    """
    try:
        data = await request.json()
    except Exception as e:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    filename = data.get("filename")
    file_size = data.get("size")
    file_type = data.get("type")
    if not filename or not file_type:
        return JSONResponse({"error": "Missing filename or file type"}, status_code=400)

    # Generate a unique ID for the upload.
    upload_id = str(uuid.uuid4())
    # Create an S3 key. For example: uploads/<upload_id>/<filename>
    key = f"uploads/{upload_id}/{filename}"

    # Create a presigned POST so the client can upload directly to S3.
    # Note: You may want to add additional Conditions as needed.
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

    # Return the details in the expected format.
    return JSONResponse(
        {
            "upload": {
                "url": presigned_post["url"],
                "method": "POST",
                "headers": presigned_post["fields"],
            },
            "on_complete": {"url": f"/api/upload/done/?id={upload_id}"},
        }
    )


async def upload_done(request: Request):
    """
    Endpoint: POST /api/upload/done/?id=...

    This endpoint is called once the client finishes uploading the file.
    For now, it simply prints a message.
    """
    upload_id = request.query_params.get("id")
    if not upload_id:
        return JSONResponse({"error": "Missing id parameter"}, status_code=400)

    # Log the completion to the console.
    print(f"[UPLOAD COMPLETE] Upload finished for ID: {upload_id}")
    return JSONResponse({"status": "success", "id": upload_id})


routes = [
    Route("/api/upload/", endpoint=get_upload_details, methods=["POST"]),
    Route("/api/upload/done/", endpoint=upload_done, methods=["POST"]),
    # Serve index.html on /
    Route("/", endpoint=FileResponse("index.html")),
]

app = Starlette(debug=True, routes=routes)

if __name__ == "__main__":
    # Run the app with Uvicorn.
    uvicorn.run(app, host="0.0.0.0", port=8090)
