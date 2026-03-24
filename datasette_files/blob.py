import hashlib
from pathlib import PurePosixPath
from typing import AsyncIterator, Optional

from .base import FileMetadata, Storage, StorageCapabilities

CREATE_BLOB_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS datasette_files_blobs (
    source_slug TEXT NOT NULL,
    path TEXT NOT NULL,
    content BLOB NOT NULL,
    content_type TEXT,
    content_hash TEXT,
    size INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source_slug, path)
);
"""


class BlobStorage(Storage):
    """Built-in storage backend that stores file content as blobs in the
    Datasette internal database.  Requires no external configuration — just
    set ``storage: blob`` in your source definition."""

    storage_type = "blob"
    capabilities = StorageCapabilities(
        can_upload=True,
        can_delete=True,
        can_list=True,
        can_generate_signed_urls=False,
        requires_proxy_download=True,
    )

    def __init__(self, datasette, source_slug):
        self.datasette = datasette
        self.source_slug = source_slug

    async def configure(self, config: dict, get_secret) -> None:
        db = self.datasette.get_internal_database()
        await db.execute_write_script(CREATE_BLOB_TABLE_SQL)

    def _db(self):
        return self.datasette.get_internal_database()

    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
        row = (
            await self._db().execute(
                "SELECT path, content_type, content_hash, size FROM datasette_files_blobs "
                "WHERE source_slug = ? AND path = ?",
                [self.source_slug, path],
            )
        ).first()
        if row is None:
            return None
        return FileMetadata(
            path=row["path"],
            filename=PurePosixPath(row["path"]).name,
            content_type=row["content_type"],
            content_hash=row["content_hash"],
            size=row["size"],
        )

    async def read_file(self, path: str) -> bytes:
        row = (
            await self._db().execute(
                "SELECT content FROM datasette_files_blobs WHERE source_slug = ? AND path = ?",
                [self.source_slug, path],
            )
        ).first()
        if row is None:
            raise FileNotFoundError(f"File not found: {path}")
        return row["content"]

    async def read_bytes(self, path: str, num_bytes: int = 2048) -> bytes:
        row = (
            await self._db().execute(
                "SELECT substr(content, 1, ?) AS head FROM datasette_files_blobs "
                "WHERE source_slug = ? AND path = ?",
                [num_bytes, self.source_slug, path],
            )
        ).first()
        if row is None:
            raise FileNotFoundError(f"File not found: {path}")
        return row["head"]

    async def stream_file(self, path: str) -> AsyncIterator[bytes]:
        content = await self.read_file(path)
        yield content

    async def list_files(
        self,
        prefix: str = "",
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> tuple[list[FileMetadata], Optional[str]]:
        if prefix:
            rows = (
                await self._db().execute(
                    "SELECT path, content_type, content_hash, size FROM datasette_files_blobs "
                    "WHERE source_slug = ? AND path LIKE ? ORDER BY path LIMIT ?",
                    [self.source_slug, prefix + "%", limit],
                )
            ).rows
        else:
            rows = (
                await self._db().execute(
                    "SELECT path, content_type, content_hash, size FROM datasette_files_blobs "
                    "WHERE source_slug = ? ORDER BY path LIMIT ?",
                    [self.source_slug, limit],
                )
            ).rows
        files = [
            FileMetadata(
                path=row["path"],
                filename=PurePosixPath(row["path"]).name,
                content_type=row["content_type"],
                content_hash=row["content_hash"],
                size=row["size"],
            )
            for row in rows
        ]
        return files, None

    async def receive_upload(
        self, path: str, stream, content_type: str
    ) -> FileMetadata:
        chunks = []
        sha256 = hashlib.sha256()
        size = 0
        async for chunk in stream:
            chunks.append(chunk)
            sha256.update(chunk)
            size += len(chunk)
        content = b"".join(chunks)
        content_hash = "sha256:" + sha256.hexdigest()

        await self._db().execute_write(
            """
            INSERT OR REPLACE INTO datasette_files_blobs
                (source_slug, path, content, content_type, content_hash, size)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [self.source_slug, path, content, content_type, content_hash, size],
        )
        return FileMetadata(
            path=path,
            filename=PurePosixPath(path).name,
            content_type=content_type,
            content_hash=content_hash,
            size=size,
        )

    async def delete_file(self, path: str) -> None:
        row = (
            await self._db().execute(
                "SELECT 1 FROM datasette_files_blobs WHERE source_slug = ? AND path = ?",
                [self.source_slug, path],
            )
        ).first()
        if row is None:
            raise FileNotFoundError(f"File not found: {path}")
        await self._db().execute_write(
            "DELETE FROM datasette_files_blobs WHERE source_slug = ? AND path = ?",
            [self.source_slug, path],
        )
