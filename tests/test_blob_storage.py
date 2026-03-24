"""Tests for the built-in BlobStorage backend (internal database)."""
from datasette.app import Datasette
import json
import pytest

from conftest import _upload_file


async def _bytes_stream(data: bytes):
    yield data


def _make_blob_datasette(permissions=None):
    """Create a Datasette instance configured with a blob storage source."""
    config = {
        "plugins": {
            "datasette-files": {
                "sources": {
                    "blob-src": {
                        "storage": "blob",
                    }
                }
            }
        }
    }
    if permissions:
        config["permissions"] = permissions
    return Datasette(memory=True, config=config)


# --- Unit tests for BlobStorage ---


@pytest.mark.asyncio
async def test_blob_storage_type():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    storage = BlobStorage(datasette=ds, source_slug="test")
    assert storage.storage_type == "blob"


@pytest.mark.asyncio
async def test_blob_storage_capabilities():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    storage = BlobStorage(datasette=ds, source_slug="test")
    assert storage.capabilities.can_upload is True
    assert storage.capabilities.can_delete is True
    assert storage.capabilities.can_list is True
    assert storage.capabilities.can_generate_signed_urls is False
    assert storage.capabilities.requires_proxy_download is True


@pytest.mark.asyncio
async def test_blob_storage_configure_is_noop():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    storage = BlobStorage(datasette=ds, source_slug="test")
    # configure should work with empty config — blob needs no configuration
    await storage.configure({}, get_secret=None)


@pytest.mark.asyncio
async def test_blob_storage_receive_and_read():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    metadata = await storage.receive_upload(
        path="abc123/hello.txt",
        stream=_bytes_stream(b"Hello, world!"),
        content_type="text/plain",
    )
    assert metadata.filename == "hello.txt"
    assert metadata.size == 13
    assert metadata.content_type == "text/plain"
    assert metadata.content_hash is not None
    assert metadata.content_hash.startswith("sha256:")

    # Read it back
    content = await storage.read_file("abc123/hello.txt")
    assert content == b"Hello, world!"


@pytest.mark.asyncio
async def test_blob_storage_read_missing_file():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    with pytest.raises(FileNotFoundError):
        await storage.read_file("nonexistent.txt")


@pytest.mark.asyncio
async def test_blob_storage_get_file_metadata():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    # Missing file returns None
    assert await storage.get_file_metadata("nope.txt") is None

    # Upload and check metadata
    await storage.receive_upload(
        path="meta/test.txt",
        stream=_bytes_stream(b"test content"),
        content_type="text/plain",
    )
    meta = await storage.get_file_metadata("meta/test.txt")
    assert meta is not None
    assert meta.path == "meta/test.txt"
    assert meta.filename == "test.txt"
    assert meta.size == 12


@pytest.mark.asyncio
async def test_blob_storage_list_files():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    await storage.receive_upload("a/file1.txt", _bytes_stream(b"one"), "text/plain")
    await storage.receive_upload("b/file2.txt", _bytes_stream(b"two"), "text/plain")

    files, cursor = await storage.list_files()
    assert len(files) == 2
    filenames = {f.filename for f in files}
    assert "file1.txt" in filenames
    assert "file2.txt" in filenames


@pytest.mark.asyncio
async def test_blob_storage_list_files_with_prefix():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    await storage.receive_upload("photos/a.jpg", _bytes_stream(b"img1"), "image/jpeg")
    await storage.receive_upload("photos/b.jpg", _bytes_stream(b"img2"), "image/jpeg")
    await storage.receive_upload("docs/c.txt", _bytes_stream(b"doc"), "text/plain")

    files, _ = await storage.list_files(prefix="photos/")
    assert len(files) == 2
    filenames = {f.filename for f in files}
    assert filenames == {"a.jpg", "b.jpg"}


@pytest.mark.asyncio
async def test_blob_storage_delete():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    await storage.receive_upload("del/gone.txt", _bytes_stream(b"bye"), "text/plain")
    content = await storage.read_file("del/gone.txt")
    assert content == b"bye"

    await storage.delete_file("del/gone.txt")
    with pytest.raises(FileNotFoundError):
        await storage.read_file("del/gone.txt")


@pytest.mark.asyncio
async def test_blob_storage_delete_missing_file():
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    with pytest.raises(FileNotFoundError):
        await storage.delete_file("nonexistent.txt")


@pytest.mark.asyncio
async def test_blob_storage_source_isolation():
    """Two blob sources sharing the same internal DB should not see each other's files."""
    from datasette_files.blob import BlobStorage

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage_a = BlobStorage(datasette=ds, source_slug="source-a")
    storage_b = BlobStorage(datasette=ds, source_slug="source-b")
    await storage_a.configure({}, get_secret=None)
    await storage_b.configure({}, get_secret=None)

    await storage_a.receive_upload("shared/file.txt", _bytes_stream(b"from A"), "text/plain")
    await storage_b.receive_upload("shared/file.txt", _bytes_stream(b"from B"), "text/plain")

    assert await storage_a.read_file("shared/file.txt") == b"from A"
    assert await storage_b.read_file("shared/file.txt") == b"from B"

    files_a, _ = await storage_a.list_files()
    files_b, _ = await storage_b.list_files()
    assert len(files_a) == 1
    assert len(files_b) == 1

    await storage_a.delete_file("shared/file.txt")
    assert await storage_a.get_file_metadata("shared/file.txt") is None
    # B's file should still exist
    assert await storage_b.get_file_metadata("shared/file.txt") is not None


# --- Chunked storage ---


@pytest.mark.asyncio
async def test_blob_storage_uses_chunk_table():
    """File content should be stored in datasette_files_blob_chunks, not as a
    single blob — this ensures large files don't need to be loaded entirely
    into memory."""
    from datasette_files.blob import BlobStorage, CHUNK_SIZE

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    # Create content larger than one chunk
    content = b"x" * (CHUNK_SIZE + 100)
    await storage.receive_upload("big/file.bin", _bytes_stream(content), "application/octet-stream")

    db = ds.get_internal_database()
    rows = (
        await db.execute(
            "SELECT chunk_index, length(data) as len FROM datasette_files_blob_chunks "
            "WHERE source_slug = ? AND path = ? ORDER BY chunk_index",
            ["test", "big/file.bin"],
        )
    ).rows
    assert len(rows) == 2
    assert rows[0]["len"] == CHUNK_SIZE
    assert rows[1]["len"] == 100


@pytest.mark.asyncio
async def test_blob_storage_stream_file_yields_chunks():
    """stream_file should yield content in chunks without assembling the full body."""
    from datasette_files.blob import BlobStorage, CHUNK_SIZE

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    content = b"A" * CHUNK_SIZE + b"B" * 100
    await storage.receive_upload("stream/test.bin", _bytes_stream(content), "application/octet-stream")

    chunks = []
    async for chunk in storage.stream_file("stream/test.bin"):
        chunks.append(chunk)

    assert len(chunks) == 2
    assert chunks[0] == b"A" * CHUNK_SIZE
    assert chunks[1] == b"B" * 100


@pytest.mark.asyncio
async def test_blob_storage_read_bytes_partial():
    """read_bytes should fetch only the requested amount, not the full file."""
    from datasette_files.blob import BlobStorage, CHUNK_SIZE

    ds = Datasette(memory=True)
    await ds.invoke_startup()
    storage = BlobStorage(datasette=ds, source_slug="test")
    await storage.configure({}, get_secret=None)

    content = b"HEADER" + b"\x00" * (CHUNK_SIZE * 3)
    await storage.receive_upload("partial/file.bin", _bytes_stream(content), "application/octet-stream")

    head = await storage.read_bytes("partial/file.bin", num_bytes=6)
    assert head == b"HEADER"


# --- Integration: blob storage through the full datasette-files API ---


@pytest.mark.asyncio
async def test_blob_source_appears_in_sources_json():
    ds = _make_blob_datasette()
    response = await ds.client.get("/-/files/sources.json")
    assert response.status_code == 200
    slugs = [s["slug"] for s in response.json()["sources"]]
    assert "blob-src" in slugs
    source = [s for s in response.json()["sources"] if s["slug"] == "blob-src"][0]
    assert source["storage_type"] == "blob"


@pytest.mark.asyncio
async def test_blob_upload_and_download():
    ds = _make_blob_datasette(
        permissions={"files-browse": True, "files-upload": True}
    )
    result = await _upload_file(
        ds,
        source="blob-src",
        filename="hello.txt",
        content=b"blob hello!",
        content_type="text/plain",
    )
    assert result["filename"] == "hello.txt"
    assert result["size"] == 11

    # Download
    download = await ds.client.get(result["file"]["download_url"])
    assert download.status_code == 200
    assert download.content == b"blob hello!"


@pytest.mark.asyncio
async def test_blob_upload_and_delete():
    ds = _make_blob_datasette(
        permissions={
            "files-browse": True,
            "files-upload": True,
            "files-delete": True,
        }
    )
    result = await _upload_file(
        ds,
        source="blob-src",
        filename="deleteme.txt",
        content=b"delete me",
        content_type="text/plain",
    )
    file_id = result["file_id"]

    # Delete
    delete_resp = await ds.client.post(
        f"/-/files/{file_id}/-/delete",
        content=json.dumps({}),
        headers={"Content-Type": "application/json"},
    )
    assert delete_resp.status_code == 200

    # Confirm gone
    get_resp = await ds.client.get(f"/-/files/{file_id}.json")
    assert get_resp.status_code == 404
