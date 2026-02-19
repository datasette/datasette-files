# datasette-files

[![PyPI](https://img.shields.io/pypi/v/datasette-files.svg)](https://pypi.org/project/datasette-files/)
[![Changelog](https://img.shields.io/github/v/release/datasette/datasette-files?include_prereleases&label=changelog)](https://github.com/datasette/datasette-files/releases)
[![Tests](https://github.com/datasette/datasette-files/actions/workflows/test.yml/badge.svg)](https://github.com/datasette/datasette-files/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/datasette/datasette-files/blob/main/LICENSE)

File management for Datasette. Upload, serve, search and manage files through a pluggable storage backend system. Ships with built-in filesystem storage and a plugin hook for adding custom backends (S3, Google Cloud Storage, etc.).

## Installation

Install this plugin in the same environment as Datasette.
```bash
datasette install datasette-files
```

## Usage

datasette-files manages files through **sources** — named connections to storage backends. Each source has a slug, a storage type, and backend-specific configuration.

### Configuring sources

Define sources in your `datasette.yaml` (or `metadata.yaml`) under the `datasette-files` plugin config:

```yaml
plugins:
  datasette-files:
    sources:
      my-files:
        storage: filesystem
        config:
          root: /data/uploads
```

This creates a source called `my-files` backed by a local directory at `/data/uploads`. The directory will be created if it doesn't exist.

You can configure multiple sources:

```yaml
plugins:
  datasette-files:
    sources:
      photos:
        storage: filesystem
        config:
          root: /data/photos
      documents:
        storage: filesystem
        config:
          root: /data/documents
```

### Permissions

All access is **denied by default**. You must explicitly grant permissions in the `permissions:` block of your `datasette.yaml`.

There are four permission actions, each scoped to a source:

| Action | Description |
|--------|-------------|
| `files-browse` | Browse, search, view, and download files |
| `files-upload` | Upload files to a source |
| `files-edit` | Edit file metadata (e.g. search text) |
| `files-delete` | Delete files from a source |

**Grant access to everyone (all sources):**

```yaml
permissions:
  files-browse: true
  files-upload: true
```

**Grant access to a specific user:**

```yaml
permissions:
  files-browse:
    id: alice
  files-upload:
    id: alice
```

**Per-source permissions:**

```yaml
permissions:
  files-browse:
    public-files:
      allow: true
    private-files:
      allow:
        id: alice
  files-upload:
    public-files:
      allow:
        id: alice
```

### Uploading files

Upload a file by sending a `POST` request with multipart form data to `/-/files/upload/{source_slug}`:

```bash
curl -X POST "http://localhost:8001/-/files/upload/my-files" \
  -F "file=@photo.jpg"
```

The response includes the file's unique ID and metadata:

```json
{
  "file_id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
  "filename": "photo.jpg",
  "content_type": "image/jpeg",
  "size": 48210,
  "url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq"
}
```

File IDs use the format `df-{ULID}` — the `df-` prefix makes them instantly recognizable when stored in database columns.

### Viewing files

Each file has an HTML info page at `/-/files/{file_id}` showing its metadata, a preview (for images), and a download link.

Download the file content directly at `/-/files/{file_id}/download`.

Get file metadata as JSON at `/-/files/{file_id}.json`.

### Searching files

Visit `/-/files/search` to search across all files you have permission to browse. The search page supports full-text search over filenames, content types, and custom search text.

The search endpoint is also available as JSON at `/-/files/search.json?q=query&source=source-slug`.

Each file has an editable `search_text` field (requires `files-edit` permission) that is included in the full-text search index. This can be used to add descriptions, tags, or transcriptions to make files more discoverable.

### Batch metadata

Fetch metadata for multiple files in a single request:

```
GET /-/files/batch.json?id=df-abc123&id=df-def456
```

This returns metadata for all requested files that the current user has permission to browse. This endpoint is used internally by the `render_cell` web component to efficiently load file information for table views.

### Listing sources

View all configured sources and their capabilities:

```
GET /-/files/sources.json
```

### Table cell integration

Any database column containing a `df-...` file ID will automatically render as a rich file reference in Datasette's table views. The `render_cell` hook detects file IDs and replaces them with a `<datasette-file>` web component that displays the filename, content type, and a thumbnail for images.

This works for any text column — store a `df-...` ID returned from the upload endpoint in a column and it will render as a file link automatically.

## API reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/-/files/search` | Search files (HTML) |
| `GET` | `/-/files/search.json?q=&source=` | Search files (JSON) |
| `GET` | `/-/files/sources.json` | List configured sources |
| `GET` | `/-/files/batch.json?id=df-...&id=df-...` | Bulk file metadata |
| `POST` | `/-/files/upload/{source_slug}` | Upload a file (multipart) |
| `GET` | `/-/files/{file_id}` | File info page (HTML) |
| `GET` | `/-/files/{file_id}.json` | File metadata (JSON) |
| `GET` | `/-/files/{file_id}/download` | Download file content |

## Plugin hook: `register_files_storage_types`

datasette-files uses a plugin hook to allow other Datasette plugins to provide custom storage backends. This is how you would build plugins like `datasette-files-s3` or `datasette-files-gcs`.

### How it works

The hook is called at startup. Your plugin returns a list of `Storage` subclasses (not instances). datasette-files handles instantiation, configuration, and lifecycle management.

```python
from datasette import hookimpl

@hookimpl
def register_files_storage_types(datasette):
    from my_plugin.storage import S3Storage
    return [S3Storage]
```

When a source in `datasette.yaml` references your storage type, datasette-files will:

1. Instantiate your class (calling `S3Storage()`)
2. Call `await storage.configure(config, get_secret)` with the source's config dict
3. Use your storage instance for all file operations on that source

### The `Storage` base class

Import the base class and supporting dataclasses from `datasette_files.base`:

```python
from datasette_files.base import Storage, StorageCapabilities, FileMetadata
```

#### `StorageCapabilities`

A dataclass declaring what your storage backend supports:

```python
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

- `can_upload`: The backend can receive file uploads via `receive_upload()`
- `can_delete`: The backend can delete files via `delete_file()`
- `can_list`: The backend can list files via `list_files()`
- `can_generate_signed_urls`: The backend can produce expiring download URLs via `download_url()` — if `True`, file downloads will use a 302 redirect to the signed URL instead of proxying content through Datasette
- `can_generate_thumbnails`: The backend can produce thumbnail URLs via `thumbnail_url()`
- `requires_proxy_download`: File content must be proxied through Datasette (e.g. filesystem storage) rather than redirecting to an external URL
- `max_file_size`: Optional maximum file size in bytes

#### `FileMetadata`

Returned by several storage methods to describe a file:

```python
@dataclass
class FileMetadata:
    path: str                              # Path within the storage backend
    filename: str                          # Human-readable filename
    content_type: Optional[str] = None     # MIME type
    content_hash: Optional[str] = None     # e.g. "sha256:abcdef..."
    size: Optional[int] = None             # Size in bytes
    width: Optional[int] = None            # Image width in pixels
    height: Optional[int] = None           # Image height in pixels
    created_at: Optional[str] = None
    metadata: dict = field(default_factory=dict)
```

#### Required methods

Every `Storage` subclass must implement these:

**`storage_type`** (property) — A unique string identifier for this storage type, used in source configuration. This is how datasette-files matches a source's `storage: s3` to your class.

```python
@property
def storage_type(self) -> str:
    return "s3"
```

**`capabilities`** (property) — Return a `StorageCapabilities` instance declaring what this backend supports.

```python
@property
def capabilities(self) -> StorageCapabilities:
    return StorageCapabilities(
        can_upload=True,
        can_delete=True,
        can_generate_signed_urls=True,
    )
```

**`configure(config, get_secret)`** — Called once at startup with the source's `config` dict from `datasette.yaml` and a `get_secret` callable for retrieving secrets from `datasette-secrets`.

```python
async def configure(self, config: dict, get_secret) -> None:
    self.bucket = config["bucket"]
    self.prefix = config.get("prefix", "")
    self.region = config.get("region", "us-east-1")
```

**`get_file_metadata(path)`** — Return a `FileMetadata` for the given path, or `None` if the file doesn't exist.

```python
async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
    # Check if the file exists in your backend and return its metadata
    ...
```

**`read_file(path)`** — Return the full content of a file as bytes. Raise `FileNotFoundError` if missing.

```python
async def read_file(self, path: str) -> bytes:
    # Read and return the file content
    ...
```

#### Optional methods

Override these based on the capabilities you declared:

**`receive_upload(path, content, content_type)`** — Store file content. Return a `FileMetadata` with at least the `content_hash` and `size` populated. Required if `can_upload` is `True`.

```python
async def receive_upload(self, path: str, content: bytes, content_type: str) -> FileMetadata:
    # Store the file and return metadata
    ...
```

**`delete_file(path)`** — Delete a file. Required if `can_delete` is `True`.

**`list_files(prefix, cursor, limit)`** — List files, returning `(files, next_cursor)`. Required if `can_list` is `True`.

**`download_url(path, expires_in)`** — Return a signed/expiring download URL. Required if `can_generate_signed_urls` is `True`.

**`stream_file(path)`** — Yield file content in chunks as an async iterator. The default implementation reads the entire file with `read_file()` and yields it as a single chunk.

**`thumbnail_url(path, width, height)`** — Return a URL for a thumbnail of the file, or `None`.

### Full example: S3 storage plugin

Here's a complete example of what a `datasette-files-s3` plugin would look like:

```python
# datasette_files_s3/__init__.py
from datasette import hookimpl
from datasette_files.base import Storage, StorageCapabilities, FileMetadata
import boto3
import hashlib
from typing import Optional


class S3Storage(Storage):
    storage_type = "s3"
    capabilities = StorageCapabilities(
        can_upload=True,
        can_delete=True,
        can_list=True,
        can_generate_signed_urls=True,
        requires_proxy_download=False,
    )

    async def configure(self, config: dict, get_secret) -> None:
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "")
        self.region = config.get("region", "us-east-1")
        self.client = boto3.client("s3", region_name=self.region)

    def _key(self, path: str) -> str:
        return f"{self.prefix}{path}" if self.prefix else path

    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
        try:
            resp = self.client.head_object(
                Bucket=self.bucket, Key=self._key(path)
            )
            return FileMetadata(
                path=path,
                filename=path.split("/")[-1],
                content_type=resp.get("ContentType"),
                size=resp.get("ContentLength"),
            )
        except self.client.exceptions.ClientError:
            return None

    async def read_file(self, path: str) -> bytes:
        resp = self.client.get_object(
            Bucket=self.bucket, Key=self._key(path)
        )
        return resp["Body"].read()

    async def receive_upload(
        self, path: str, content: bytes, content_type: str
    ) -> FileMetadata:
        self.client.put_object(
            Bucket=self.bucket,
            Key=self._key(path),
            Body=content,
            ContentType=content_type,
        )
        content_hash = "sha256:" + hashlib.sha256(content).hexdigest()
        return FileMetadata(
            path=path,
            filename=path.split("/")[-1],
            content_type=content_type,
            content_hash=content_hash,
            size=len(content),
        )

    async def download_url(self, path: str, expires_in: int = 300) -> str:
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": self._key(path)},
            ExpiresIn=expires_in,
        )

    async def delete_file(self, path: str) -> None:
        self.client.delete_object(
            Bucket=self.bucket, Key=self._key(path)
        )

    async def list_files(
        self, prefix: str = "", cursor: Optional[str] = None, limit: int = 100
    ) -> tuple[list[FileMetadata], Optional[str]]:
        kwargs = {
            "Bucket": self.bucket,
            "Prefix": self._key(prefix),
            "MaxKeys": limit,
        }
        if cursor:
            kwargs["ContinuationToken"] = cursor
        resp = self.client.list_objects_v2(**kwargs)
        files = [
            FileMetadata(
                path=obj["Key"].removeprefix(self.prefix),
                filename=obj["Key"].split("/")[-1],
                size=obj["Size"],
            )
            for obj in resp.get("Contents", [])
        ]
        next_cursor = resp.get("NextContinuationToken")
        return files, next_cursor


@hookimpl
def register_files_storage_types(datasette):
    return [S3Storage]
```

The plugin's `pyproject.toml` would register itself as a Datasette plugin:

```toml
[project.entry-points.datasette]
files_s3 = "datasette_files_s3"
```

Then configure it in `datasette.yaml`:

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
```

### Built-in filesystem storage reference

The built-in `FilesystemStorage` stores files on the local filesystem. It supports upload, delete, and listing but does not support signed URLs — file downloads are proxied through Datasette.

**Configuration options:**

| Key | Required | Description |
|-----|----------|-------------|
| `root` | Yes | Absolute path to the directory where files are stored |
| `max_file_size` | No | Maximum upload size in bytes |

**Capabilities:**

| Capability | Value |
|-----------|-------|
| `can_upload` | `True` |
| `can_delete` | `True` |
| `can_list` | `True` |
| `can_generate_signed_urls` | `False` |
| `requires_proxy_download` | `True` |

## Development

To set up this plugin locally, first checkout the code. Run the tests with `uv`:
```bash
cd datasette-files
uv run pytest
```

Recommendation to run a test server:
```bash
./dev-server.sh
```

