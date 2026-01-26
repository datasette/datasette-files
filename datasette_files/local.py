"""
Local directory storage provider for datasette-files.

Stores uploaded files directly to a local filesystem directory.
"""

import os
import aiofiles
import aiofiles.os
from pathlib import Path
from typing import AsyncGenerator, Optional
from .base import Storage, File


class LocalDirectoryStorage(Storage):
    """
    Storage provider that saves files to a local directory.

    Files are stored directly in the configured directory path.
    """

    supports_uploads = True

    def __init__(self, name: str, directory: str, base_url: Optional[str] = None):
        """
        Initialize the local directory storage.

        Args:
            name: Name of this storage provider
            directory: Path to the directory where files will be stored
            base_url: Optional base URL for serving files (for expiring_download_url)
        """
        self.name = name
        self.directory = Path(directory)
        self.base_url = base_url

    async def list_files(self, last_token: str = None) -> AsyncGenerator[File, None]:
        """List all files in the storage directory."""
        if not self.directory.exists():
            return

        for entry in self.directory.iterdir():
            if entry.is_file():
                stat = entry.stat()
                yield File(
                    name=entry.name,
                    path=str(entry.relative_to(self.directory)),
                    type=_guess_mime_type(entry.name),
                    mtime=int(stat.st_mtime),
                    ctime=int(stat.st_ctime),
                )

    async def upload_form_fields(self, file_name: str, file_type: str) -> dict:
        """
        Return form fields for upload.

        For local storage, uploads go directly to our upload endpoint,
        so we return an empty dict.
        """
        return {}

    async def upload_complete(self, file_name: str, file_type: str):
        """Called when upload is complete (no-op for local storage)."""
        pass

    async def upload_file(self, filename: str, content: bytes, content_type: Optional[str] = None) -> str:
        """
        Save an uploaded file to the local directory.

        Args:
            filename: Name of the file
            content: File content as bytes
            content_type: Optional MIME type

        Returns:
            The path where the file was stored
        """
        # Ensure the directory exists
        await aiofiles.os.makedirs(str(self.directory), exist_ok=True)

        # Sanitize filename to prevent path traversal
        safe_filename = Path(filename).name

        file_path = self.directory / safe_filename

        # Handle filename conflicts by appending a number
        counter = 1
        original_stem = file_path.stem
        original_suffix = file_path.suffix
        while file_path.exists():
            file_path = self.directory / f"{original_stem}_{counter}{original_suffix}"
            counter += 1

        async with aiofiles.open(str(file_path), "wb") as f:
            await f.write(content)

        return str(file_path.relative_to(self.directory))

    async def read_file(self, path: str) -> bytes:
        """Read and return the content of a file."""
        file_path = self.directory / path

        # Prevent path traversal
        try:
            file_path.resolve().relative_to(self.directory.resolve())
        except ValueError:
            raise FileNotFoundError(f"File not found: {path}")

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        async with aiofiles.open(str(file_path), "rb") as f:
            return await f.read()

    async def expiring_download_url(self, path: str, expires_after: int = 5 * 60) -> str:
        """
        Return a URL for downloading the file.

        For local storage, this returns a direct path if base_url is configured,
        otherwise raises NotImplementedError.
        """
        if self.base_url:
            return f"{self.base_url.rstrip('/')}/{path}"
        raise NotImplementedError(
            "expiring_download_url requires base_url to be configured"
        )


def _guess_mime_type(filename: str) -> Optional[str]:
    """Guess the MIME type based on file extension."""
    from mimetypes import guess_type
    mime_type, _ = guess_type(filename)
    return mime_type
