# datasette-files

[![PyPI](https://img.shields.io/pypi/v/datasette-files.svg)](https://pypi.org/project/datasette-files/)
[![Changelog](https://img.shields.io/github/v/release/datasette/datasette-files?include_prereleases&label=changelog)](https://github.com/datasette/datasette-files/releases)
[![Tests](https://github.com/datasette/datasette-files/actions/workflows/test.yml/badge.svg)](https://github.com/datasette/datasette-files/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/datasette/datasette-files/blob/main/LICENSE)

Upload files to Datasette with support for cloud storage backends.

## Features

- Browser-based file upload UI with drag-and-drop support
- Direct client-to-S3 uploads via presigned POST (files don't pass through the server)
- Progress tracking for concurrent uploads
- File metadata stored in Datasette's internal database
- Plugin hook system for custom storage backends

## Installation

Install this plugin in the same environment as Datasette:

```bash
datasette install datasette-files
```

## Configuration

### S3 Storage

Set the following environment variables to configure S3 storage:

```bash
export S3_BUCKET="your-bucket-name"
export AWS_REGION="us-east-1"
```

You'll also need AWS credentials configured (via environment variables, IAM role, or AWS config file).

### S3 Bucket CORS Configuration

Your S3 bucket needs CORS configured to allow direct browser uploads:

```json
[
    {
        "AllowedHeaders": ["*"],
        "AllowedMethods": ["POST"],
        "AllowedOrigins": ["*"],
        "ExposeHeaders": []
    }
]
```

## Usage

### Upload Interface

Navigate to `/-/files/s3/upload` to access the file upload interface. You can:

- Drag and drop files onto the upload area
- Click to select files using a file picker
- Upload multiple files concurrently (up to 4 simultaneous uploads)
- Track progress for each file

### API Endpoints

#### `POST /-/files/s3/upload`

Initiates an S3 upload. Send a JSON body with:

```json
{
    "filename": "example.txt",
    "size": 1234,
    "type": "text/plain"
}
```

Returns presigned POST data for direct S3 upload:

```json
{
    "upload": {
        "url": "https://bucket.s3.amazonaws.com/",
        "method": "POST",
        "headers": { "...presigned fields..." }
    },
    "on_complete": {
        "url": "/-/files/complete?id=<upload_id>"
    }
}
```

#### `POST /-/files/complete?id=<upload_id>`

Call this endpoint after the file has been uploaded to S3 to finalize the upload and record it in the database.

#### `GET /-/files/storages`

Debug endpoint listing registered storage backends. Requires `debug-storages` permission.

#### `GET /-/files/storages/list/<name>`

Debug endpoint listing files in a specific storage backend. Requires `debug-storages` permission.

## Database Schema

The plugin creates three tables in Datasette's internal database:

- `files_sources` - Configured storage backends
- `files_files` - Successfully uploaded files with metadata (ULID, path, size, type, mtime)
- `files_pending` - Files currently being uploaded

## Permissions

The plugin registers one permission:

- `debug-storages` - Required to access the storage debug endpoints

Grant this permission in your Datasette configuration:

```yaml
permissions:
  debug-storages:
    id: root
```

## Development

To set up this plugin locally, first checkout the code. Run the tests with `uv`:

```bash
cd datasette-files
uv run pytest
```

Run a test server:

```bash
uv run datasette . --internal internal.db --root --reload \
  --secret 1 -s permissions.debug-storages.id root
```

If you're using `datasette-secrets` to manage secrets:

```bash
uv run datasette secrets generate-encryption-key > key.txt
uv run datasette . --internal internal.db --root --reload \
  --secret 1 -s permissions.debug-storages.id root \
  -s plugins.datasette-secrets.encryption-key "$(cat key.txt)" \
  -s permissions.manage-secrets.id root
```

## Plugin Hook for Storage Backends

Plugins can register custom storage backends by implementing the `register_files_storages` hook:

```python
from datasette import hookimpl

@hookimpl
def register_files_storages(datasette):
    return [MyCustomStorage()]
```

Storage backends should implement the `Storage` interface defined in `datasette_files.base`.
