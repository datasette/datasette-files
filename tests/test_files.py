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


def _make_datasette(upload_dir, permissions=None, extra_sources=None):
    """Create a Datasette instance configured with file sources and optional permissions."""
    sources = {
        "test-uploads": {
            "storage": "filesystem",
            "config": {
                "root": upload_dir,
            },
        }
    }
    if extra_sources:
        sources.update(extra_sources)

    config = {
        "plugins": {
            "datasette-files": {
                "sources": sources,
            }
        },
    }
    if permissions:
        config["permissions"] = permissions

    return Datasette(memory=True, config=config)


@pytest.fixture
def datasette_with_files(upload_dir):
    """Datasette with a filesystem source but no browse permissions (default deny)."""
    return _make_datasette(upload_dir)


@pytest.fixture
def datasette_browse_allowed(upload_dir):
    """Datasette with files-browse granted to all actors."""
    return _make_datasette(
        upload_dir,
        permissions={
            "files-browse": True,
        },
    )


async def _upload_file(ds, source="test-uploads", filename="test.txt",
                       content=b"Hello from test!", content_type="text/plain"):
    """Helper to upload a file and return the response JSON."""
    response = await ds.client.post(
        f"/-/files/upload/{source}",
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="' + filename.encode() + b'"\r\n'
            b"Content-Type: " + content_type.encode() + b"\r\n"
            b"\r\n"
            + content + b"\r\n"
            b"--boundary--\r\n"
        ),
        headers={
            "Content-Type": "multipart/form-data; boundary=boundary",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


# --- Plugin installation ---


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
    assert "datasette_files_fts" in tables

    # Check source was registered
    rows = (await db.execute("select * from datasette_files_sources")).rows
    assert len(rows) == 1
    assert rows[0]["slug"] == "test-uploads"
    assert rows[0]["storage_type"] == "filesystem"


@pytest.mark.asyncio
async def test_startup_creates_fts_triggers(datasette_with_files):
    ds = datasette_with_files
    await ds.invoke_startup()

    db = ds.get_internal_database()

    # Check triggers exist
    triggers = (
        await db.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'datasette_files_%'"
        )
    ).rows
    trigger_names = {row["name"] for row in triggers}
    assert "datasette_files_ai" in trigger_names
    assert "datasette_files_ad" in trigger_names
    assert "datasette_files_au" in trigger_names


# --- Actions registration ---


@pytest.mark.asyncio
async def test_actions_registered(datasette_with_files):
    ds = datasette_with_files
    await ds.invoke_startup()

    assert "files-browse" in ds.actions
    assert "files-upload" in ds.actions
    assert "files-delete" in ds.actions

    browse = ds.actions["files-browse"]
    assert browse.abbr == "fb"
    assert browse.resource_class is not None
    assert browse.resource_class.name == "file-source"


# --- Upload endpoint ---


@pytest.mark.asyncio
async def test_upload_file(datasette_with_files, upload_dir):
    ds = datasette_with_files

    data = await _upload_file(ds)
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


# --- Permission enforcement (default deny) ---


@pytest.mark.asyncio
async def test_file_info_denied_without_permission(datasette_with_files):
    """Without files-browse permission, file info returns 403."""
    ds = datasette_with_files
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_file_json_denied_without_permission(datasette_with_files):
    ds = datasette_with_files
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}.json")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_file_download_denied_without_permission(datasette_with_files):
    ds = datasette_with_files
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}/download")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_batch_json_denied_without_permission(datasette_with_files):
    """batch.json excludes files from sources the actor cannot browse."""
    ds = datasette_with_files
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/batch.json?id={file_id}")
    assert response.status_code == 200
    assert response.json()["files"] == {}


# --- File info and download (with permission) ---


@pytest.mark.asyncio
async def test_file_info_page(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="info.txt", content=b"File info test")
    file_id = data["file_id"]

    info_response = await ds.client.get(f"/-/files/{file_id}")
    assert info_response.status_code == 200
    assert "text/html" in info_response.headers["content-type"]
    assert "info.txt" in info_response.text


@pytest.mark.asyncio
async def test_file_json_metadata(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="meta.txt", content=b"Metadata test")
    file_id = data["file_id"]

    meta_response = await ds.client.get(f"/-/files/{file_id}.json")
    assert meta_response.status_code == 200
    meta = meta_response.json()
    assert meta["id"] == file_id
    assert meta["filename"] == "meta.txt"
    assert meta["content_type"] == "text/plain"
    assert "content_hash" in meta


@pytest.mark.asyncio
async def test_file_download(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="dl.txt", content=b"Download me!")
    file_id = data["file_id"]

    dl_response = await ds.client.get(f"/-/files/{file_id}/download")
    assert dl_response.status_code == 200
    assert dl_response.text == "Download me!"
    assert dl_response.headers["content-type"] == "text/plain"


@pytest.mark.asyncio
async def test_file_not_found(datasette_browse_allowed):
    ds = datasette_browse_allowed
    response = await ds.client.get("/-/files/df-nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_batch_json_with_permission(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="batch.txt", content=b"batch test")
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/batch.json?id={file_id}")
    assert response.status_code == 200
    files = response.json()["files"]
    assert file_id in files
    assert files[file_id]["filename"] == "batch.txt"


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


# --- FTS ---


@pytest.mark.asyncio
async def test_fts_populated_on_upload(datasette_browse_allowed, upload_dir):
    """Uploading a file populates the FTS index."""
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="searchable.txt")
    file_id = data["file_id"]

    db = ds.get_internal_database()
    rows = (
        await db.execute(
            "SELECT id, filename FROM datasette_files_fts WHERE datasette_files_fts MATCH ?",
            ["searchable"],
        )
    ).rows
    assert len(rows) == 1
    assert rows[0]["id"] == file_id
    assert rows[0]["filename"] == "searchable.txt"


# --- Search endpoint ---


@pytest.mark.asyncio
async def test_search_json_empty(datasette_browse_allowed):
    """Search with no files returns empty results."""
    ds = datasette_browse_allowed
    response = await ds.client.get("/-/files/search.json")
    assert response.status_code == 200
    data = response.json()
    assert data["files"] == []
    assert data["q"] == ""


@pytest.mark.asyncio
async def test_search_json_with_query(datasette_browse_allowed, upload_dir):
    """Search with a query finds matching files."""
    ds = datasette_browse_allowed
    await _upload_file(ds, filename="report.pdf", content=b"pdf content", content_type="application/pdf")
    await _upload_file(ds, filename="photo.jpg", content=b"jpg content", content_type="image/jpeg")

    # Search for "report"
    response = await ds.client.get("/-/files/search.json?q=report")
    assert response.status_code == 200
    data = response.json()
    assert len(data["files"]) == 1
    assert data["files"][0]["filename"] == "report.pdf"
    assert data["q"] == "report"


@pytest.mark.asyncio
async def test_search_json_no_query_lists_recent(datasette_browse_allowed, upload_dir):
    """Search with no query lists recent files."""
    ds = datasette_browse_allowed
    await _upload_file(ds, filename="first.txt", content=b"1")
    await _upload_file(ds, filename="second.txt", content=b"2")

    response = await ds.client.get("/-/files/search.json")
    assert response.status_code == 200
    data = response.json()
    assert len(data["files"]) == 2
    filenames = {f["filename"] for f in data["files"]}
    assert "first.txt" in filenames
    assert "second.txt" in filenames


@pytest.mark.asyncio
async def test_search_html_page(datasette_browse_allowed, upload_dir):
    """Search HTML endpoint renders a page."""
    ds = datasette_browse_allowed
    await _upload_file(ds, filename="findme.txt", content=b"content")

    response = await ds.client.get("/-/files/search?q=findme")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "findme.txt" in response.text


@pytest.mark.asyncio
async def test_search_source_filter(datasette_browse_allowed, upload_dir):
    """Search can filter by source slug."""
    ds = datasette_browse_allowed
    await _upload_file(ds, filename="filtered.txt", content=b"data")

    # Filter by the correct source
    response = await ds.client.get("/-/files/search.json?source=test-uploads")
    assert response.status_code == 200
    assert len(response.json()["files"]) == 1

    # Filter by nonexistent source
    response = await ds.client.get("/-/files/search.json?source=nonexistent")
    assert response.status_code == 200
    assert len(response.json()["files"]) == 0


# --- Permission-filtered search ---


@pytest.mark.asyncio
async def test_search_denied_without_permission(datasette_with_files, upload_dir):
    """Search returns empty results when actor lacks files-browse permission."""
    ds = datasette_with_files
    await _upload_file(ds, filename="secret.txt", content=b"secret data")

    response = await ds.client.get("/-/files/search.json")
    assert response.status_code == 200
    data = response.json()
    assert data["files"] == []
    assert data["sources"] == []


@pytest.mark.asyncio
async def test_search_with_fts_query_denied(datasette_with_files, upload_dir):
    """FTS search also returns empty when no browse permission."""
    ds = datasette_with_files
    await _upload_file(ds, filename="secret.txt", content=b"secret data")

    response = await ds.client.get("/-/files/search.json?q=secret")
    assert response.status_code == 200
    assert response.json()["files"] == []


@pytest.mark.asyncio
async def test_search_multi_source_permission(tmp_path):
    """Search only returns files from sources the actor can browse."""
    public_dir = str(tmp_path / "public")
    private_dir = str(tmp_path / "private")
    os.makedirs(public_dir)
    os.makedirs(private_dir)

    ds = Datasette(
        memory=True,
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "public-files": {
                            "storage": "filesystem",
                            "config": {"root": public_dir},
                        },
                        "private-files": {
                            "storage": "filesystem",
                            "config": {"root": private_dir},
                        },
                    }
                }
            },
            "permissions": {
                "files-browse": {
                    "public-files": {"allow": True},
                    # private-files: no allow = default deny
                },
            },
        },
    )

    # Upload to both sources
    await _upload_file(ds, source="public-files", filename="public-doc.txt", content=b"public")
    await _upload_file(ds, source="private-files", filename="private-doc.txt", content=b"private")

    # Search should only return public files
    response = await ds.client.get("/-/files/search.json")
    assert response.status_code == 200
    data = response.json()
    filenames = {f["filename"] for f in data["files"]}
    assert "public-doc.txt" in filenames
    assert "private-doc.txt" not in filenames

    # Only public-files should be in browsable sources
    assert "public-files" in data["sources"]
    assert "private-files" not in data["sources"]


@pytest.mark.asyncio
async def test_search_multi_source_fts_filtered(tmp_path):
    """FTS search is also filtered by source permissions."""
    public_dir = str(tmp_path / "public")
    private_dir = str(tmp_path / "private")
    os.makedirs(public_dir)
    os.makedirs(private_dir)

    ds = Datasette(
        memory=True,
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "public-files": {
                            "storage": "filesystem",
                            "config": {"root": public_dir},
                        },
                        "private-files": {
                            "storage": "filesystem",
                            "config": {"root": private_dir},
                        },
                    }
                }
            },
            "permissions": {
                "files-browse": {
                    "public-files": {"allow": True},
                },
            },
        },
    )

    # Both sources have a file named "report"
    await _upload_file(ds, source="public-files", filename="report.txt", content=b"public report")
    await _upload_file(ds, source="private-files", filename="report.txt", content=b"private report")

    # FTS search for "report" should only return the public one
    response = await ds.client.get("/-/files/search.json?q=report")
    assert response.status_code == 200
    data = response.json()
    assert len(data["files"]) == 1
    assert data["files"][0]["source_slug"] == "public-files"


@pytest.mark.asyncio
async def test_search_actor_specific_permission(tmp_path):
    """Search respects actor-specific permissions."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = Datasette(
        memory=True,
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "team-files": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": {
                    "team-files": {
                        "allow": {"id": "alice"},
                    },
                },
            },
        },
    )

    await _upload_file(ds, source="team-files", filename="team-doc.txt", content=b"team data")

    # Anonymous user (no actor) gets empty results
    response = await ds.client.get("/-/files/search.json")
    assert response.status_code == 200
    assert response.json()["files"] == []

    # Alice can browse
    response = await ds.client.get(
        "/-/files/search.json",
        cookies={"ds_actor": ds.sign({"a": {"id": "alice"}}, "actor")},
    )
    assert response.status_code == 200
    assert len(response.json()["files"]) == 1
    assert response.json()["files"][0]["filename"] == "team-doc.txt"

    # Bob cannot
    response = await ds.client.get(
        "/-/files/search.json",
        cookies={"ds_actor": ds.sign({"a": {"id": "bob"}}, "actor")},
    )
    assert response.status_code == 200
    assert response.json()["files"] == []


# --- File info/download permission with specific actors ---


@pytest.mark.asyncio
async def test_file_info_actor_permission(tmp_path):
    """File info respects actor-specific browse permission."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = Datasette(
        memory=True,
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "restricted": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": {
                    "restricted": {
                        "allow": {"id": "alice"},
                    },
                },
            },
        },
    )

    data = await _upload_file(ds, source="restricted", filename="secret.txt", content=b"secret")
    file_id = data["file_id"]

    # Anonymous: 403
    response = await ds.client.get(f"/-/files/{file_id}")
    assert response.status_code == 403

    # Alice: 200
    response = await ds.client.get(
        f"/-/files/{file_id}",
        cookies={"ds_actor": ds.sign({"a": {"id": "alice"}}, "actor")},
    )
    assert response.status_code == 200
    assert "secret.txt" in response.text

    # Bob: 403
    response = await ds.client.get(
        f"/-/files/{file_id}",
        cookies={"ds_actor": ds.sign({"a": {"id": "bob"}}, "actor")},
    )
    assert response.status_code == 403
