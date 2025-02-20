from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, AsyncGenerator


@dataclass
class File:
    name: str
    path: str
    type: Optional[str]
    mtime: Optional[int]
    ctime: Optional[int]


class Storage(ABC):
    supports_uploads = False
    name: str

    @abstractmethod
    async def list_files(self, last_token: str = None) -> AsyncGenerator[File, None]:
        # Yields File() dataclasses
        # The last_token is a string that can be persisted and used for fetch-since-list-time
        # on the sources that support that
        pass

    @abstractmethod
    async def upload_form_fields(self, file_name, file_type) -> dict:
        # used by the browser to upload - for S3 it's presigned stuff
        # other plugins will use register_routes() to provide their own thing
        pass

    @abstractmethod
    async def upload_complete(self, file_name, file_type):
        # Optional, I don't think the S3 one needs this, maybe it does though
        pass

    @abstractmethod
    async def read_file(self, path: str) -> bytes:
        # Return the content of the file with the given name
        pass

    @abstractmethod
    async def expiring_download_url(self, path: str, expires_after=5 * 60) -> str:
        # URL users can download from
        pass
