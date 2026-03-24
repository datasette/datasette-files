import hashlib
from pathlib import PurePosixPath
from typing import AsyncIterator, Optional

from .base import FileMetadata, Storage, StorageCapabilities

CHUNK_SIZE = 512 * 1024  # 512 KB

CREATE_BLOB_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS datasette_files_blobs (
    source_slug TEXT NOT NULL,
    path TEXT NOT NULL,
    content_type TEXT,
    content_hash TEXT,
    size INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source_slug, path)
);

CREATE TABLE IF NOT EXISTS datasette_files_blob_chunks (
    source_slug TEXT NOT NULL,
    path TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (source_slug, path, chunk_index),
    FOREIGN KEY (source_slug, path) REFERENCES datasette_files_blobs(source_slug, path)
);
"""


class BlobStorage(Storage):
    """Built-in storage backend that stores file content as blobs in the
    Datasette internal database.  Requires no external configuration — just
    set ``storage: blob`` in your source definition.

    File content is split into 512 KB chunks so that large files can be
    streamed without loading the entire content into memory."""

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
        await db.execute_write_script(CREATE_BLOB_TABLES_SQL)

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
        rows = (
            await self._db().execute(
                "SELECT data FROM datasette_files_blob_chunks "
                "WHERE source_slug = ? AND path = ? ORDER BY chunk_index",
                [self.source_slug, path],
            )
        ).rows
        if not rows:
            # Distinguish "no chunks" from "file doesn't exist"
            meta = await self.get_file_metadata(path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {path}")
            return b""
        return b"".join(row["data"] for row in rows)

    async def read_bytes(self, path: str, num_bytes: int = 2048) -> bytes:
        # Only fetch enough chunks to cover the requested bytes
        chunks_needed = (num_bytes + CHUNK_SIZE - 1) // CHUNK_SIZE
        rows = (
            await self._db().execute(
                "SELECT data FROM datasette_files_blob_chunks "
                "WHERE source_slug = ? AND path = ? ORDER BY chunk_index LIMIT ?",
                [self.source_slug, path, chunks_needed],
            )
        ).rows
        if not rows:
            meta = await self.get_file_metadata(path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {path}")
            return b""
        content = b"".join(row["data"] for row in rows)
        return content[:num_bytes]

    async def stream_file(self, path: str) -> AsyncIterator[bytes]:
        rows = (
            await self._db().execute(
                "SELECT data FROM datasette_files_blob_chunks "
                "WHERE source_slug = ? AND path = ? ORDER BY chunk_index",
                [self.source_slug, path],
            )
        ).rows
        if not rows:
            meta = await self.get_file_metadata(path)
            if meta is None:
                raise FileNotFoundError(f"File not found: {path}")
            return
        for row in rows:
            yield row["data"]

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
        # Buffer incoming stream, computing hash and splitting into chunks
        buf = bytearray()
        sha256 = hashlib.sha256()
        size = 0
        chunks = []
        async for data in stream:
            sha256.update(data)
            size += len(data)
            buf.extend(data)
            while len(buf) >= CHUNK_SIZE:
                chunks.append(bytes(buf[:CHUNK_SIZE]))
                del buf[:CHUNK_SIZE]
        if buf:
            chunks.append(bytes(buf))

        content_hash = "sha256:" + sha256.hexdigest()

        db = self._db()

        # Delete any existing chunks for this path (for replace)
        await db.execute_write(
            "DELETE FROM datasette_files_blob_chunks WHERE source_slug = ? AND path = ?",
            [self.source_slug, path],
        )

        # Insert metadata row
        await db.execute_write(
            """
            INSERT OR REPLACE INTO datasette_files_blobs
                (source_slug, path, content_type, content_hash, size)
            VALUES (?, ?, ?, ?, ?)
            """,
            [self.source_slug, path, content_type, content_hash, size],
        )

        # Insert chunks
        for i, chunk in enumerate(chunks):
            await db.execute_write(
                """
                INSERT INTO datasette_files_blob_chunks
                    (source_slug, path, chunk_index, data)
                VALUES (?, ?, ?, ?)
                """,
                [self.source_slug, path, i, chunk],
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
        db = self._db()
        await db.execute_write(
            "DELETE FROM datasette_files_blob_chunks WHERE source_slug = ? AND path = ?",
            [self.source_slug, path],
        )
        await db.execute_write(
            "DELETE FROM datasette_files_blobs WHERE source_slug = ? AND path = ?",
            [self.source_slug, path],
        )
