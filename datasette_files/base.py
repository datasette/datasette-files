from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, AsyncIterator, Tuple


@dataclass
class FileMetadata:
    """Metadata about a file in a storage backend."""

    path: str
    filename: str
    content_type: Optional[str] = None
    content_hash: Optional[str] = None  # e.g. "sha256:abcdef..."
    size: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    created_at: Optional[str] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class UploadInstructions:
    """Instructions returned to the client for uploading a file."""

    upload_url: str
    upload_method: str = "POST"
    upload_headers: dict = field(default_factory=dict)
    upload_fields: dict = field(default_factory=dict)


@dataclass
class StorageCapabilities:
    """Declares what a storage backend supports."""

    can_upload: bool = False
    can_delete: bool = False
    can_list: bool = False
    can_generate_signed_urls: bool = False
    requires_proxy_download: bool = False
    max_file_size: Optional[int] = None


class Storage(ABC):
    """Abstract base for file storage backends."""

    @property
    @abstractmethod
    def storage_type(self) -> str:
        """Unique identifier for this storage type, e.g. 's3', 'filesystem'."""
        ...

    @property
    @abstractmethod
    def capabilities(self) -> StorageCapabilities:
        """Return the capabilities of this storage backend."""
        ...

    @abstractmethod
    async def configure(self, config: dict, get_secret) -> None:
        """Called once at startup with the source's config dict and a secret-fetching callable."""
        ...

    @abstractmethod
    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
        """Return metadata for a single file, or None if it doesn't exist."""
        ...

    @abstractmethod
    async def read_file(self, path: str) -> bytes:
        """Return the full content of a file. Raises FileNotFoundError if missing."""
        ...

    # Optional methods — override based on capabilities

    async def list_files(
        self,
        prefix: str = "",
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> tuple[list[FileMetadata], Optional[str]]:
        """List files. Returns (files, next_cursor)."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support listing")

    async def download_url(self, path: str, expires_in: int = 300) -> str:
        """Return an expiring download URL."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support signed download URLs"
        )

    async def read_bytes(self, path: str, num_bytes: int = 2048) -> bytes:
        """Return up to num_bytes from the start of a file.

        Storage backends can override this to avoid reading the full file
        (e.g. using HTTP Range headers for S3).
        """
        content = await self.read_file(path)
        return content[:num_bytes]

    async def stream_file(self, path: str) -> AsyncIterator[bytes]:
        """Yield file content in chunks."""
        yield await self.read_file(path)

    async def prepare_upload(
        self, filename: str, content_type: str, size: int
    ) -> UploadInstructions:
        """Prepare for a file upload. Returns instructions for the client."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support uploads")

    async def receive_upload(
        self, path: str, stream: AsyncIterator[bytes], content_type: str
    ) -> FileMetadata:
        """Receive and store file content streamed as chunks (for proxy uploads).

        ``stream`` is an async iterator yielding ``bytes`` chunks.  Backends
        should consume the iterator incrementally to avoid buffering the
        entire file in memory.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support proxy uploads"
        )

    async def delete_file(self, path: str) -> None:
        """Delete a file from the backend."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support deletion"
        )


class ThumbnailGenerator(ABC):
    """Abstract base for thumbnail generators."""

    name: str

    @abstractmethod
    async def can_generate(self, content_type: str, filename: str) -> bool:
        """Return True if this generator can produce a thumbnail for the given file."""
        ...

    @abstractmethod
    async def generate(
        self,
        file_bytes: bytes,
        content_type: str,
        filename: str,
        max_width: int = 200,
        max_height: int = 200,
    ) -> Optional[Tuple[bytes, str]]:
        """Generate a thumbnail. Returns (thumbnail_bytes, content_type) or None."""
        ...
