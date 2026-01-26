from datasette.app import Datasette
from datasette_files.local import LocalDirectoryStorage
from datasette_files import hookspecs
from datasette.plugins import pm
from datasette import hookimpl
import pytest
import tempfile
import os


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-files" in installed_plugins


@pytest.mark.asyncio
async def test_local_directory_storage_list_empty():
    """Test listing files in an empty directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalDirectoryStorage(name="test", directory=tmpdir)
        files = [f async for f in storage.list_files()]
        assert files == []


@pytest.mark.asyncio
async def test_local_directory_storage_upload_and_list():
    """Test uploading a file and listing it."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalDirectoryStorage(name="test", directory=tmpdir)

        # Upload a file
        path = await storage.upload_file(
            filename="test.txt",
            content=b"Hello, World!",
            content_type="text/plain"
        )

        assert path == "test.txt"

        # List files
        files = [f async for f in storage.list_files()]
        assert len(files) == 1
        assert files[0].name == "test.txt"
        assert files[0].path == "test.txt"
        assert files[0].type == "text/plain"


@pytest.mark.asyncio
async def test_local_directory_storage_read_file():
    """Test reading an uploaded file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalDirectoryStorage(name="test", directory=tmpdir)

        # Upload a file
        await storage.upload_file(
            filename="test.txt",
            content=b"Hello, World!",
            content_type="text/plain"
        )

        # Read it back
        content = await storage.read_file("test.txt")
        assert content == b"Hello, World!"


@pytest.mark.asyncio
async def test_local_directory_storage_filename_conflict():
    """Test that filename conflicts are handled by appending a number."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalDirectoryStorage(name="test", directory=tmpdir)

        # Upload same filename twice
        path1 = await storage.upload_file(
            filename="test.txt",
            content=b"First file",
            content_type="text/plain"
        )
        path2 = await storage.upload_file(
            filename="test.txt",
            content=b"Second file",
            content_type="text/plain"
        )

        assert path1 == "test.txt"
        assert path2 == "test_1.txt"

        # Verify contents
        content1 = await storage.read_file("test.txt")
        content2 = await storage.read_file("test_1.txt")
        assert content1 == b"First file"
        assert content2 == b"Second file"


@pytest.mark.asyncio
async def test_local_directory_storage_path_traversal_prevention():
    """Test that path traversal attacks are prevented."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalDirectoryStorage(name="test", directory=tmpdir)

        # Upload a file with path traversal in filename (should be sanitized)
        path = await storage.upload_file(
            filename="../../../etc/passwd",
            content=b"malicious content",
            content_type="text/plain"
        )
        # Should only use the filename part
        assert path == "passwd"

        # Try to read with path traversal
        with pytest.raises(FileNotFoundError):
            await storage.read_file("../../../etc/passwd")


@pytest.mark.asyncio
async def test_local_directory_storage_expiring_url_with_base_url():
    """Test that expiring_download_url works with base_url configured."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalDirectoryStorage(
            name="test",
            directory=tmpdir,
            base_url="https://example.com/files"
        )

        url = await storage.expiring_download_url("test.txt")
        assert url == "https://example.com/files/test.txt"


@pytest.mark.asyncio
async def test_local_directory_storage_expiring_url_without_base_url():
    """Test that expiring_download_url raises error without base_url."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalDirectoryStorage(name="test", directory=tmpdir)

        with pytest.raises(NotImplementedError):
            await storage.expiring_download_url("test.txt")


@pytest.mark.asyncio
async def test_local_upload_endpoint():
    """Test the local upload HTTP endpoint."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a plugin that registers our test storage
        class TestStoragePlugin:
            __name__ = "test_storage_plugin"

            @staticmethod
            @hookimpl
            def register_files_storages(datasette):
                return [LocalDirectoryStorage(name="test-storage", directory=tmpdir)]

        pm.register(TestStoragePlugin(), name="test_storage_plugin")

        try:
            datasette = Datasette(memory=True)
            await datasette.invoke_startup()

            # Test GET request returns the upload form
            response = await datasette.client.get("/-/files/local/upload/test-storage")
            assert response.status_code == 200
            assert "Upload to test-storage" in response.text

            # Test POST with multipart form data
            response = await datasette.client.post(
                "/-/files/local/upload/test-storage",
                files={"file": ("test.txt", b"Hello from test!", "text/plain")},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert data["filename"] == "test.txt"
            assert data["size"] == len(b"Hello from test!")
            assert data["content_type"] == "text/plain"

            # Verify file was actually saved
            saved_path = os.path.join(tmpdir, data["path"])
            with open(saved_path, "rb") as f:
                assert f.read() == b"Hello from test!"
        finally:
            pm.unregister(name="test_storage_plugin")


@pytest.mark.asyncio
async def test_local_upload_endpoint_storage_not_found():
    """Test that uploading to a non-existent storage returns 404."""
    datasette = Datasette(memory=True)
    await datasette.invoke_startup()

    response = await datasette.client.post(
        "/-/files/local/upload/nonexistent",
        files={"file": ("test.txt", b"Hello!", "text/plain")},
    )
    assert response.status_code == 404
    assert "not found" in response.json()["error"]


@pytest.mark.asyncio
async def test_local_upload_endpoint_no_file():
    """Test that uploading without a file returns 400."""
    with tempfile.TemporaryDirectory() as tmpdir:
        class TestStoragePlugin:
            __name__ = "test_storage_plugin_nofile"

            @staticmethod
            @hookimpl
            def register_files_storages(datasette):
                return [LocalDirectoryStorage(name="test-storage", directory=tmpdir)]

        pm.register(TestStoragePlugin(), name="test_storage_plugin_nofile")

        try:
            datasette = Datasette(memory=True)
            await datasette.invoke_startup()

            # POST without file field
            response = await datasette.client.post(
                "/-/files/local/upload/test-storage",
                data={"other_field": "value"},
            )
            assert response.status_code == 400
            assert "No file uploaded" in response.json()["error"]
        finally:
            pm.unregister(name="test_storage_plugin_nofile")
