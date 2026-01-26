# datasette-files

[![PyPI](https://img.shields.io/pypi/v/datasette-files.svg)](https://pypi.org/project/datasette-files/)
[![Changelog](https://img.shields.io/github/v/release/datasette/datasette-files?include_prereleases&label=changelog)](https://github.com/datasette/datasette-files/releases)
[![Tests](https://github.com/datasette/datasette-files/actions/workflows/test.yml/badge.svg)](https://github.com/datasette/datasette-files/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/datasette/datasette-files/blob/main/LICENSE)

Upload files to Datasette

## Installation

Install this plugin in the same environment as Datasette.
```bash
datasette install datasette-files
```
## Usage

This plugin provides a storage abstraction for uploading and managing files in Datasette. It supports multiple storage backends through a plugin hook system.

### Built-in Storage Providers

#### LocalDirectoryStorage

The `LocalDirectoryStorage` provider stores uploaded files directly to a local filesystem directory. This is useful for development, self-hosted deployments, or scenarios where you want files stored on disk.

To use it, create a plugin that registers the storage:

```python
# my_plugin.py
from datasette import hookimpl
from datasette_files.local import LocalDirectoryStorage

@hookimpl
def register_files_storages(datasette):
    return [
        LocalDirectoryStorage(
            name="uploads",
            directory="/path/to/uploads",
            base_url="https://example.com/files"  # optional
        )
    ]
```

Once registered, you can upload files via:
- **Web UI**: Visit `/-/files/local/upload/<storage_name>` for a drag-and-drop upload interface
- **API**: POST multipart form data to `/-/files/local/upload/<storage_name>` with a `file` field

Example API usage:
```bash
curl -X POST -F "file=@myfile.txt" http://localhost:8001/-/files/local/upload/uploads
```

Response:
```json
{
  "status": "success",
  "id": "01hxyz...",
  "filename": "myfile.txt",
  "path": "myfile.txt",
  "size": 1234,
  "content_type": "text/plain"
}
```

### Creating Custom Storage Providers

You can create custom storage providers by implementing the `Storage` abstract base class:

```python
from datasette_files.base import Storage, File

class MyCustomStorage(Storage):
    supports_uploads = True
    name = "my-storage"

    async def list_files(self, last_token=None):
        # Yield File objects
        pass

    async def upload_form_fields(self, file_name, file_type):
        # Return dict of form fields for browser uploads
        return {}

    async def upload_complete(self, file_name, file_type):
        # Called after upload completes
        pass

    async def read_file(self, path):
        # Return file contents as bytes
        pass

    async def expiring_download_url(self, path, expires_after=300):
        # Return a URL for downloading the file
        pass
```

### Plugin Hook

Register your storage providers using the `register_files_storages` hook:

```python
from datasette import hookimpl

@hookimpl
def register_files_storages(datasette):
    return [MyCustomStorage(), AnotherStorage()]
```

## Development

To set up this plugin locally, first checkout the code. Run the tests with `uv`:
```bash
cd datasette-files
uv run pytest
```

Recommendation to run a test server:
```bash
uv run datasette . --internal internal.db --root --reload \
  --secret 1 -s permissions.debug-storages.id root
```
And if you're using `datasette-secrets` to manage any secrets for those plugins:
```bash
uv run datasette secrets generate-encryption-key > key.txt
```
Then add this to the `datasette` line:
```bash
uv run datasette . --internal internal.db --root --reload \
  --secret 1 -s permissions.debug-storages.id root \
  -s plugins.datasette-secrets.encryption-key "$(cat key.txt)" \
  -s permissions.manage-secrets.id root 
```
