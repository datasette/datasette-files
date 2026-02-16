from datasette.app import Datasette
import pytest
import json
import tempfile
import os


@pytest.fixture
def upload_dir(tmp_path):
    """Create a temporary directory for filesystem storage uploads."""
    d = tmp_path / "uploads"
    d.mkdir()
    return str(d)


@pytest.fixture
def datasette_with_files(upload_dir):
    """Create a Datasette instance configured with a filesystem file source."""
    return Datasette(
        memory=True,
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-uploads": {
                            "storage": "filesystem",
                            "config": {
                                "root": upload_dir,
                            },
                        }
                    }
                }
            }
        },
    )


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-files" in installed_plugins


# --- Base classes ---


def test_file_metadata_dataclass():
    from datasette_files.base import FileMetadata

    fm = FileMetadata(path="abc/photo.jpg", filename="photo.jpg")
    assert fm.path == "abc/photo.jpg"
    assert fm.filename == "photo.jpg"
    assert fm.content_type is None
    assert fm.content_hash is None
    assert fm.size is None
    assert fm.metadata == {}


def test_upload_instructions_dataclass():
    from datasette_files.base import UploadInstructions

    ui = UploadInstructions(upload_url="/-/files/upload/test")
    assert ui.upload_url == "/-/files/upload/test"
    assert ui.upload_method == "POST"
    assert ui.upload_headers == {}
    assert ui.upload_fields == {}


def test_storage_capabilities_defaults():
    from datasette_files.base import StorageCapabilities

    caps = StorageCapabilities()
    assert caps.can_upload is False
    assert caps.can_delete is False
    assert caps.can_list is False
    assert caps.can_generate_signed_urls is False
    assert caps.requires_proxy_download is False


# --- Filesystem storage ---


@pytest.mark.asyncio
async def test_filesystem_storage_configure(upload_dir):
    from datasette_files.filesystem import FilesystemStorage

    storage = FilesystemStorage()
    await storage.configure({"root": upload_dir}, get_secret=None)
    assert storage.storage_type == "filesystem"
    assert storage.capabilities.can_upload is True
    assert storage.capabilities.can_list is True
    assert storage.capabilities.requires_proxy_download is True


@pytest.mark.asyncio
async def test_filesystem_storage_receive_and_read(upload_dir):
    from datasette_files.filesystem import FilesystemStorage

    storage = FilesystemStorage()
    await storage.configure({"root": upload_dir}, get_secret=None)

    metadata = await storage.receive_upload(
        path="abc123/hello.txt",
        content=b"Hello, world!",
        content_type="text/plain",
    )
    assert metadata.filename == "hello.txt"
    assert metadata.size == 13
    assert metadata.content_type == "text/plain"
    assert metadata.content_hash is not None
    assert metadata.content_hash.startswith("sha256:")

    # Verify we can read it back
    content = await storage.read_file("abc123/hello.txt")
    assert content == b"Hello, world!"


@pytest.mark.asyncio
async def test_filesystem_storage_list_files(upload_dir):
    from datasette_files.filesystem import FilesystemStorage

    storage = FilesystemStorage()
    await storage.configure({"root": upload_dir}, get_secret=None)

    # Upload a file
    await storage.receive_upload("a/file1.txt", b"one", "text/plain")
    await storage.receive_upload("b/file2.txt", b"two", "text/plain")

    files, cursor = await storage.list_files()
    assert len(files) == 2
    filenames = {f.filename for f in files}
    assert "file1.txt" in filenames
    assert "file2.txt" in filenames


@pytest.mark.asyncio
async def test_filesystem_storage_delete(upload_dir):
    from datasette_files.filesystem import FilesystemStorage

    storage = FilesystemStorage()
    await storage.configure({"root": upload_dir}, get_secret=None)

    await storage.receive_upload("del/gone.txt", b"bye", "text/plain")
    content = await storage.read_file("del/gone.txt")
    assert content == b"bye"

    await storage.delete_file("del/gone.txt")
    with pytest.raises(FileNotFoundError):
        await storage.read_file("del/gone.txt")


# --- Internal database schema ---


@pytest.mark.asyncio
async def test_startup_creates_tables(datasette_with_files):
    ds = datasette_with_files
    # Trigger startup
    await ds.invoke_startup()

    db = ds.get_internal_database()

    # Check tables exist
    tables = await db.table_names()
    assert "datasette_files_sources" in tables
    assert "datasette_files" in tables

    # Check source was registered
    rows = (await db.execute("select * from datasette_files_sources")).rows
    assert len(rows) == 1
    assert rows[0]["slug"] == "test-uploads"
    assert rows[0]["storage_type"] == "filesystem"


# --- Upload endpoint ---


@pytest.mark.asyncio
async def test_upload_file(datasette_with_files, upload_dir):
    ds = datasette_with_files

    # Upload a file
    response = await ds.client.post(
        "/-/files/upload/test-uploads",
        content=b"--boundary\r\n"
        b'Content-Disposition: form-data; name="file"; filename="test.txt"\r\n'
        b"Content-Type: text/plain\r\n"
        b"\r\n"
        b"Hello from test!\r\n"
        b"--boundary--\r\n",
        headers={
            "Content-Type": "multipart/form-data; boundary=boundary",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "file_id" in data
    assert data["file_id"].startswith("df-")
    assert data["filename"] == "test.txt"
    assert data["content_type"] == "text/plain"

    # Verify file exists on disk
    file_id = data["file_id"]
    ulid_part = file_id[3:]  # strip "df-"
    expected_path = os.path.join(upload_dir, ulid_part, "test.txt")
    assert os.path.exists(expected_path)
    with open(expected_path) as f:
        assert f.read() == "Hello from test!"


@pytest.mark.asyncio
async def test_upload_requires_valid_source(datasette_with_files):
    ds = datasette_with_files
    response = await ds.client.post(
        "/-/files/upload/nonexistent",
        content=b"--boundary\r\n"
        b'Content-Disposition: form-data; name="file"; filename="test.txt"\r\n'
        b"Content-Type: text/plain\r\n"
        b"\r\n"
        b"data\r\n"
        b"--boundary--\r\n",
        headers={
            "Content-Type": "multipart/form-data; boundary=boundary",
        },
    )
    assert response.status_code == 404


# --- File info and download ---


@pytest.mark.asyncio
async def test_file_info_page(datasette_with_files, upload_dir):
    ds = datasette_with_files

    # Upload a file first
    response = await ds.client.post(
        "/-/files/upload/test-uploads",
        content=b"--boundary\r\n"
        b'Content-Disposition: form-data; name="file"; filename="info.txt"\r\n'
        b"Content-Type: text/plain\r\n"
        b"\r\n"
        b"File info test\r\n"
        b"--boundary--\r\n",
        headers={
            "Content-Type": "multipart/form-data; boundary=boundary",
        },
    )
    file_id = response.json()["file_id"]

    # Get the info page
    info_response = await ds.client.get(f"/-/files/{file_id}")
    assert info_response.status_code == 200
    assert "text/html" in info_response.headers["content-type"]
    assert "info.txt" in info_response.text


@pytest.mark.asyncio
async def test_file_json_metadata(datasette_with_files, upload_dir):
    ds = datasette_with_files

    response = await ds.client.post(
        "/-/files/upload/test-uploads",
        content=b"--boundary\r\n"
        b'Content-Disposition: form-data; name="file"; filename="meta.txt"\r\n'
        b"Content-Type: text/plain\r\n"
        b"\r\n"
        b"Metadata test\r\n"
        b"--boundary--\r\n",
        headers={
            "Content-Type": "multipart/form-data; boundary=boundary",
        },
    )
    file_id = response.json()["file_id"]

    # Get JSON metadata
    meta_response = await ds.client.get(f"/-/files/{file_id}.json")
    assert meta_response.status_code == 200
    meta = meta_response.json()
    assert meta["id"] == file_id
    assert meta["filename"] == "meta.txt"
    assert meta["content_type"] == "text/plain"
    assert "content_hash" in meta


@pytest.mark.asyncio
async def test_file_download(datasette_with_files, upload_dir):
    ds = datasette_with_files

    response = await ds.client.post(
        "/-/files/upload/test-uploads",
        content=b"--boundary\r\n"
        b'Content-Disposition: form-data; name="file"; filename="dl.txt"\r\n'
        b"Content-Type: text/plain\r\n"
        b"\r\n"
        b"Download me!\r\n"
        b"--boundary--\r\n",
        headers={
            "Content-Type": "multipart/form-data; boundary=boundary",
        },
    )
    file_id = response.json()["file_id"]

    # Download the file
    dl_response = await ds.client.get(f"/-/files/{file_id}/download")
    assert dl_response.status_code == 200
    assert dl_response.text == "Download me!"
    assert dl_response.headers["content-type"] == "text/plain"


@pytest.mark.asyncio
async def test_file_not_found(datasette_with_files):
    ds = datasette_with_files
    response = await ds.client.get("/-/files/df-nonexistent")
    assert response.status_code == 404


# --- Sources API ---


@pytest.mark.asyncio
async def test_sources_json(datasette_with_files):
    ds = datasette_with_files
    response = await ds.client.get("/-/files/sources.json")
    assert response.status_code == 200
    data = response.json()
    assert "sources" in data
    assert len(data["sources"]) == 1
    source = data["sources"][0]
    assert source["slug"] == "test-uploads"
    assert source["storage_type"] == "filesystem"
    assert "capabilities" in source
