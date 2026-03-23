"""Tests for path traversal protection in FilesystemStorage."""

import pytest

from datasette_files.filesystem import FilesystemStorage


async def _make_storage(tmp_path):
    root = tmp_path / "uploads"
    root.mkdir()
    storage = FilesystemStorage()
    await storage.configure({"root": str(root)}, get_secret=None)
    return storage


@pytest.mark.asyncio
async def test_path_traversal_read_blocked(tmp_path):
    storage = await _make_storage(tmp_path)
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP SECRET")

    with pytest.raises(ValueError, match="outside"):
        await storage.read_file("../secret.txt")


@pytest.mark.asyncio
async def test_path_traversal_write_blocked(tmp_path):
    storage = await _make_storage(tmp_path)

    async def _chunks():
        yield b"MALICIOUS"

    with pytest.raises(ValueError, match="outside"):
        await storage.receive_upload("../escaped.txt", _chunks(), "text/plain")

    assert not (tmp_path / "escaped.txt").exists()


@pytest.mark.asyncio
async def test_path_traversal_delete_blocked(tmp_path):
    storage = await _make_storage(tmp_path)
    victim = tmp_path / "victim.txt"
    victim.write_text("important data")

    with pytest.raises(ValueError, match="outside"):
        await storage.delete_file("../victim.txt")

    assert victim.exists()


@pytest.mark.asyncio
async def test_path_traversal_metadata_blocked(tmp_path):
    storage = await _make_storage(tmp_path)
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP SECRET")

    with pytest.raises(ValueError, match="outside"):
        await storage.get_file_metadata("../secret.txt")


@pytest.mark.asyncio
async def test_path_traversal_stream_blocked(tmp_path):
    storage = await _make_storage(tmp_path)
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP SECRET")

    with pytest.raises(ValueError, match="outside"):
        async for _ in storage.stream_file("../secret.txt"):
            pass


@pytest.mark.asyncio
async def test_path_traversal_list_blocked(tmp_path):
    storage = await _make_storage(tmp_path)

    with pytest.raises(ValueError, match="outside"):
        await storage.list_files(prefix="..")


@pytest.mark.asyncio
async def test_valid_paths_still_work(tmp_path):
    """Normal paths within root continue to work."""
    storage = await _make_storage(tmp_path)

    async def _chunks():
        yield b"hello"

    meta = await storage.receive_upload("subdir/test.txt", _chunks(), "text/plain")
    assert meta.path == "subdir/test.txt"

    content = await storage.read_file("subdir/test.txt")
    assert content == b"hello"

    file_meta = await storage.get_file_metadata("subdir/test.txt")
    assert file_meta is not None
    assert file_meta.filename == "test.txt"

    files, _ = await storage.list_files(prefix="subdir")
    assert len(files) == 1

    chunks = []
    async for chunk in storage.stream_file("subdir/test.txt"):
        chunks.append(chunk)
    assert b"".join(chunks) == b"hello"

    await storage.delete_file("subdir/test.txt")
    assert await storage.get_file_metadata("subdir/test.txt") is None
