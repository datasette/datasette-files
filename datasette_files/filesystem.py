import hashlib
import os
from pathlib import Path
from typing import Optional

from .base import FileMetadata, Storage, StorageCapabilities


class FilesystemStorage(Storage):
    """Built-in filesystem storage backend."""

    storage_type = "filesystem"
    capabilities = StorageCapabilities(
        can_upload=True,
        can_delete=True,
        can_list=True,
        can_generate_signed_urls=False,
        requires_proxy_download=True,
    )

    async def configure(self, config: dict, get_secret) -> None:
        self.root = Path(config["root"])
        self.max_file_size = config.get("max_file_size")
        self.root.mkdir(parents=True, exist_ok=True)

    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
        target = self.root / path
        if not target.exists():
            return None
        stat = target.stat()
        return FileMetadata(
            path=path,
            filename=target.name,
            size=stat.st_size,
        )

    async def read_file(self, path: str) -> bytes:
        target = self.root / path
        if not target.exists():
            raise FileNotFoundError(f"File not found: {path}")
        return target.read_bytes()

    async def list_files(
        self,
        prefix: str = "",
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> tuple[list[FileMetadata], Optional[str]]:
        files = []
        search_root = self.root / prefix if prefix else self.root
        for file_path in sorted(search_root.rglob("*")):
            if file_path.is_file():
                rel_path = str(file_path.relative_to(self.root))
                stat = file_path.stat()
                files.append(
                    FileMetadata(
                        path=rel_path,
                        filename=file_path.name,
                        size=stat.st_size,
                    )
                )
                if len(files) >= limit:
                    break
        return files, None

    async def receive_upload(
        self, path: str, content: bytes, content_type: str
    ) -> FileMetadata:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        content_hash = "sha256:" + hashlib.sha256(content).hexdigest()
        return FileMetadata(
            path=path,
            filename=Path(path).name,
            content_type=content_type,
            content_hash=content_hash,
            size=len(content),
        )

    async def delete_file(self, path: str) -> None:
        target = self.root / path
        if not target.exists():
            raise FileNotFoundError(f"File not found: {path}")
        target.unlink()
