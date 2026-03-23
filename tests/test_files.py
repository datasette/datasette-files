from datasette.app import Datasette
import pytest
import json
import tempfile
import os
import sqlite3

from conftest import _upload_file, _make_datasette


async def _bytes_stream(data: bytes):
    yield data


def _create_sqlite_database(path):
    """Create an empty SQLite database file so Datasette can open it on startup."""
    conn = sqlite3.connect(path)
    conn.close()


# --- Plugin installation ---


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-files" in installed_plugins


@pytest.mark.asyncio
async def test_file_column_type_is_registered():
    datasette = Datasette(memory=True)
    await datasette.invoke_startup()
    assert "file" in datasette._column_types


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
        stream=_bytes_stream(b"Hello, world!"),
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
    await storage.receive_upload("a/file1.txt", _bytes_stream(b"one"), "text/plain")
    await storage.receive_upload("b/file2.txt", _bytes_stream(b"two"), "text/plain")

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

    await storage.receive_upload("del/gone.txt", _bytes_stream(b"bye"), "text/plain")
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
async def test_upload_file(datasette_upload_allowed, upload_dir):
    ds = datasette_upload_allowed

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
        "/-/files/upload/nonexistent/-/prepare",
        content=json.dumps(
            {"filename": "test.txt", "content_type": "text/plain", "size": 4}
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 404


# --- Permission enforcement (default deny) ---


@pytest.mark.asyncio
async def test_upload_post_denied_without_permission(datasette_with_files):
    """Prepare should return 403 without files-upload permission."""
    ds = datasette_with_files
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/prepare",
        content=json.dumps(
            {"filename": "test.txt", "content_type": "text/plain", "size": 13}
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_file_info_denied_without_permission(datasette_upload_allowed):
    """Without files-browse permission, file info returns 403."""
    ds = datasette_upload_allowed
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_file_json_denied_without_permission(datasette_upload_allowed):
    ds = datasette_upload_allowed
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}.json")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_file_download_denied_without_permission(datasette_upload_allowed):
    ds = datasette_upload_allowed
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}/download")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_batch_json_denied_without_permission(datasette_upload_allowed):
    """batch.json excludes files from sources the actor cannot browse."""
    ds = datasette_upload_allowed
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
    await _upload_file(
        ds,
        filename="report.pdf",
        content=b"pdf content",
        content_type="application/pdf",
    )
    await _upload_file(
        ds, filename="photo.jpg", content=b"jpg content", content_type="image/jpeg"
    )

    # Search for "report"
    response = await ds.client.get("/-/files/search.json?q=report")
    assert response.status_code == 200
    data = response.json()
    assert len(data["files"]) == 1
    assert data["files"][0]["filename"] == "report.pdf"
    assert data["q"] == "report"


@pytest.mark.asyncio
async def test_search_json_prefix_match(datasette_browse_allowed, upload_dir):
    """Search with a partial prefix finds matching files."""
    ds = datasette_browse_allowed
    await _upload_file(
        ds, filename="projects5.png", content=b"png data", content_type="image/png"
    )
    await _upload_file(
        ds, filename="report.txt", content=b"txt data", content_type="text/plain"
    )

    # "proj" should match "projects5.png" via prefix
    response = await ds.client.get("/-/files/search.json?q=proj")
    assert response.status_code == 200
    data = response.json()
    assert len(data["files"]) == 1
    assert data["files"][0]["filename"] == "projects5.png"

    # "pro 5" (multiple terms) should also match "projects5.png"
    response = await ds.client.get("/-/files/search.json?q=pro+5")
    assert response.status_code == 200
    data = response.json()
    filenames = {f["filename"] for f in data["files"]}
    assert "projects5.png" in filenames


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


# --- Homepage action ---


@pytest.mark.asyncio
async def test_homepage_action_with_browse_permission(datasette_browse_allowed):
    """Homepage shows 'Manage files' action when actor has files-browse permission."""
    ds = datasette_browse_allowed
    response = await ds.client.get("/")
    assert response.status_code == 200
    assert "Manage files" in response.text
    assert "/-/files" in response.text


@pytest.mark.asyncio
async def test_homepage_action_without_permission(datasette_with_files):
    """Homepage does not show 'Manage files' action without files-browse permission."""
    ds = datasette_with_files
    response = await ds.client.get("/")
    assert response.status_code == 200
    assert "Manage files" not in response.text


# --- Files index page ---


@pytest.mark.asyncio
async def test_files_index_page(datasette_browse_allowed, upload_dir):
    """/-/files index page lists sources with file counts."""
    ds = datasette_browse_allowed
    await _upload_file(ds, filename="a.txt", content=b"aaa")
    await _upload_file(ds, filename="b.txt", content=b"bbb")

    response = await ds.client.get("/-/files")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "test-uploads" in response.text
    # Should contain a link to the source page
    assert "/-/files/source/test-uploads" in response.text


@pytest.mark.asyncio
async def test_files_index_no_permission(datasette_with_files):
    """/-/files shows no sources when actor lacks files-browse permission."""
    ds = datasette_with_files
    response = await ds.client.get("/-/files")
    assert response.status_code == 200
    assert "No sources available" in response.text


@pytest.mark.asyncio
async def test_files_index_multi_source(tmp_path):
    """/-/files shows only sources the actor can browse."""
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
                "files-upload": True,
                "files-browse": {
                    "public-files": {"allow": True},
                },
            },
        },
    )

    await _upload_file(ds, source="public-files", filename="pub.txt", content=b"pub")
    await _upload_file(ds, source="private-files", filename="priv.txt", content=b"priv")

    response = await ds.client.get("/-/files")
    assert response.status_code == 200
    assert "public-files" in response.text
    assert "private-files" not in response.text


# --- Source files page ---


@pytest.mark.asyncio
async def test_source_files_page(datasette_browse_allowed, upload_dir):
    """/-/files/source/{slug} lists files in that source."""
    ds = datasette_browse_allowed
    await _upload_file(ds, filename="src1.txt", content=b"one")
    await _upload_file(ds, filename="src2.txt", content=b"two")

    response = await ds.client.get("/-/files/source/test-uploads")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "src1.txt" in response.text
    assert "src2.txt" in response.text
    assert "2 files" in response.text


@pytest.mark.asyncio
async def test_source_files_page_shows_upload_form(tmp_path):
    """/-/files/source/{slug} shows upload form when actor has files-upload permission."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = _make_datasette(
        upload_dir,
        permissions={
            "files-browse": True,
            "files-upload": True,
        },
    )

    response = await ds.client.get("/-/files/source/test-uploads")
    assert response.status_code == 200
    assert "datasette-file-upload" in response.text
    assert 'source="test-uploads"' in response.text


@pytest.mark.asyncio
async def test_source_files_page_hides_upload_without_permission(
    datasette_browse_only,
):
    """/-/files/source/{slug} hides upload form without files-upload permission."""
    ds = datasette_browse_only
    response = await ds.client.get("/-/files/source/test-uploads")
    assert response.status_code == 200
    assert 'type="file"' not in response.text


@pytest.mark.asyncio
async def test_source_files_denied_without_browse(datasette_with_files):
    """/-/files/source/{slug} returns 403 without files-browse permission."""
    ds = datasette_with_files
    response = await ds.client.get("/-/files/source/test-uploads")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_source_files_not_found(datasette_browse_allowed):
    """/-/files/source/{slug} returns 404 for nonexistent source."""
    ds = datasette_browse_allowed
    response = await ds.client.get("/-/files/source/nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_source_files_pagination(datasette_browse_allowed, upload_dir):
    """/-/files/source/{slug} paginates with page parameter."""
    ds = datasette_browse_allowed
    # Upload 25 files (PAGE_SIZE is 20)
    for i in range(25):
        await _upload_file(
            ds, filename=f"file{i:03d}.txt", content=f"content{i}".encode()
        )

    # Page 1 should have 20 files
    response = await ds.client.get("/-/files/source/test-uploads")
    assert response.status_code == 200
    assert "Page 1 of 2" in response.text
    assert "Next" in response.text

    # Page 2 should have 5 files
    response = await ds.client.get("/-/files/source/test-uploads?page=2")
    assert response.status_code == 200
    assert "Page 2 of 2" in response.text
    assert "Previous" in response.text


@pytest.mark.asyncio
async def test_source_files_empty(datasette_browse_allowed):
    """/-/files/source/{slug} shows message when source has no files."""
    ds = datasette_browse_allowed
    response = await ds.client.get("/-/files/source/test-uploads")
    assert response.status_code == 200
    assert "No files in this source" in response.text


# --- Permission-filtered search ---


@pytest.mark.asyncio
async def test_search_denied_without_permission(datasette_upload_allowed, upload_dir):
    """Search returns empty results when actor lacks files-browse permission."""
    ds = datasette_upload_allowed
    await _upload_file(ds, filename="secret.txt", content=b"secret data")

    response = await ds.client.get("/-/files/search.json")
    assert response.status_code == 200
    data = response.json()
    assert data["files"] == []
    assert data["sources"] == []


@pytest.mark.asyncio
async def test_search_with_fts_query_denied(datasette_upload_allowed, upload_dir):
    """FTS search also returns empty when no browse permission."""
    ds = datasette_upload_allowed
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
                "files-upload": True,
                "files-browse": {
                    "public-files": {"allow": True},
                    # private-files: no allow = default deny
                },
            },
        },
    )

    # Upload to both sources
    await _upload_file(
        ds, source="public-files", filename="public-doc.txt", content=b"public"
    )
    await _upload_file(
        ds, source="private-files", filename="private-doc.txt", content=b"private"
    )

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
                "files-upload": True,
                "files-browse": {
                    "public-files": {"allow": True},
                },
            },
        },
    )

    # Both sources have a file named "report"
    await _upload_file(
        ds, source="public-files", filename="report.txt", content=b"public report"
    )
    await _upload_file(
        ds, source="private-files", filename="report.txt", content=b"private report"
    )

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
                "files-upload": True,
                "files-browse": {
                    "team-files": {
                        "allow": {"id": "alice"},
                    },
                },
            },
        },
    )

    await _upload_file(
        ds, source="team-files", filename="team-doc.txt", content=b"team data"
    )

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
                "files-upload": True,
                "files-browse": {
                    "restricted": {
                        "allow": {"id": "alice"},
                    },
                },
            },
        },
    )

    data = await _upload_file(
        ds, source="restricted", filename="secret.txt", content=b"secret"
    )
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


# --- search_text column ---


@pytest.mark.asyncio
async def test_search_text_column_exists(datasette_with_files):
    ds = datasette_with_files
    await ds.invoke_startup()

    db = ds.get_internal_database()
    columns = (await db.execute("PRAGMA table_info(datasette_files)")).rows
    col_names = {row["name"] for row in columns}
    assert "search_text" in col_names


@pytest.mark.asyncio
async def test_search_text_in_fts(datasette_browse_allowed, upload_dir):
    """FTS index includes search_text column."""
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="notes.txt", content=b"some file")
    file_id = data["file_id"]

    # Directly update search_text
    db = ds.get_internal_database()
    await db.execute_write(
        "UPDATE datasette_files SET search_text = ? WHERE id = ?",
        ["quarterly revenue analysis financial", file_id],
    )

    # FTS should now match on the search_text
    response = await ds.client.get("/-/files/search.json?q=revenue")
    assert response.status_code == 200
    files = response.json()["files"]
    assert len(files) == 1
    assert files[0]["id"] == file_id


@pytest.mark.asyncio
async def test_search_text_not_in_filename_match(datasette_browse_allowed, upload_dir):
    """search_text doesn't interfere with filename-only searches."""
    ds = datasette_browse_allowed
    await _upload_file(ds, filename="report.txt", content=b"data")
    await _upload_file(ds, filename="other.txt", content=b"data")

    # Update search_text for other.txt (shouldn't match "report" query)
    db = ds.get_internal_database()
    rows = (
        await db.execute("SELECT id FROM datasette_files WHERE filename = 'other.txt'")
    ).rows
    other_id = rows[0]["id"]
    await db.execute_write(
        "UPDATE datasette_files SET search_text = ? WHERE id = ?",
        ["unrelated content", other_id],
    )

    response = await ds.client.get("/-/files/search.json?q=report")
    assert response.status_code == 200
    files = response.json()["files"]
    assert len(files) == 1
    assert files[0]["filename"] == "report.txt"


# --- files-edit action ---


@pytest.mark.asyncio
async def test_files_edit_action_registered(datasette_with_files):
    ds = datasette_with_files
    await ds.invoke_startup()
    assert "files-edit" in ds.actions


# --- render_cell with data-column ---


@pytest.mark.asyncio
async def test_render_cell_includes_data_column(datasette_browse_allowed, upload_dir):
    """FileColumnType.render_cell includes data-column when table context is present."""
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="cell.txt", content=b"cell test")
    file_id = data["file_id"]

    from datasette_files import FileColumnType

    result = await FileColumnType().render_cell(
        value=file_id,
        column="document",
        table="projects",
        database="demo",
        datasette=ds,
        request=None,
    )
    assert result is not None
    assert f'data-column="document"' in result
    assert f'file-id="{file_id}"' in result


@pytest.mark.asyncio
async def test_render_cell_no_data_column_without_table(
    datasette_browse_allowed, upload_dir
):
    """FileColumnType.render_cell omits data-column when table is None."""
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="cell.txt", content=b"cell test")
    file_id = data["file_id"]

    from datasette_files import FileColumnType

    result = await FileColumnType().render_cell(
        value=file_id,
        column="document",
        table=None,
        database="demo",
        datasette=ds,
        request=None,
    )
    assert result is not None
    assert "data-column" not in result


@pytest.mark.asyncio
async def test_render_cell_no_match_for_non_file_id(datasette_browse_allowed):
    """FileColumnType.render_cell returns None for non-file values."""
    ds = datasette_browse_allowed

    from datasette_files import FileColumnType

    result = await FileColumnType().render_cell(
        value="not-a-file-id",
        column="name",
        table="projects",
        database="demo",
        datasette=ds,
        request=None,
    )
    assert result is None


@pytest.mark.asyncio
async def test_render_cell_escapes_column_name(datasette_browse_allowed, upload_dir):
    """FileColumnType.render_cell escapes column names to prevent XSS."""
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="cell.txt", content=b"cell test")
    file_id = data["file_id"]

    from datasette_files import FileColumnType

    result = await FileColumnType().render_cell(
        value=file_id,
        column='"><script>alert(1)</script>',
        table="projects",
        database="demo",
        datasette=ds,
        request=None,
    )
    assert result is not None
    assert "<script>" not in result
    assert "&lt;script&gt;" in result or "&#" in result


@pytest.mark.asyncio
async def test_table_page_only_renders_file_typed_columns(tmp_path):
    """Only columns assigned the file column type render datasette-file components."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = Datasette(
        [str(tmp_path / "test.db")],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-src": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "files-upload": True,
            },
            "databases": {
                "test": {
                    "tables": {
                        "projects": {
                            "column_types": {
                                "logo": "file",
                            }
                        }
                    }
                }
            },
        },
    )
    db = ds.get_database("test")
    await db.execute_write(
        "CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, logo TEXT, note TEXT)"
    )
    data = await _upload_file(ds, source="test-src", filename="logo.txt")
    file_id = data["file_id"]
    await db.execute_write(
        "INSERT INTO projects (id, logo, note) VALUES (?, ?, ?)",
        [1, file_id, file_id],
    )

    response = await ds.client.get("/test/projects")
    assert response.status_code == 200
    assert response.text.count(f'file-id="{file_id}"') == 1
    assert f">{file_id}</td>" in response.text


# --- extra_body_script ---


@pytest.mark.asyncio
async def test_extra_body_script_on_table_page(tmp_path):
    """extra_body_script emits window.__datasette_files on table pages."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = Datasette(
        [str(tmp_path / "test.db")],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-src": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
            },
        },
    )
    db = ds.get_database("test")
    await db.execute_write(
        "CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, name TEXT, logo TEXT)"
    )

    # Visit the table page as anonymous user
    response = await ds.client.get("/test/projects")
    assert response.status_code == 200
    assert "window.__datasette_files" in response.text
    # Anonymous user cannot update rows by default
    assert '"canUpdate": false' in response.text
    assert '"database": "test"' in response.text
    assert '"fileColumns": []' in response.text
    assert '"table": "projects"' in response.text


@pytest.mark.asyncio
async def test_extra_body_script_includes_file_columns(tmp_path):
    """extra_body_script includes typed file columns for table-page enhancement."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = Datasette(
        [str(tmp_path / "test.db")],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-src": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
            },
            "databases": {
                "test": {
                    "tables": {
                        "projects": {
                            "column_types": {
                                "logo": "file",
                            }
                        }
                    }
                }
            },
        },
    )
    db = ds.get_database("test")
    await db.execute_write(
        "CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, name TEXT, logo TEXT)"
    )

    response = await ds.client.get("/test/projects")
    assert response.status_code == 200
    assert '"fileColumns": ["logo"]' in response.text


@pytest.mark.asyncio
async def test_extra_body_script_with_update_permission(tmp_path):
    """extra_body_script sets canUpdate true when actor has update-row permission."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = Datasette(
        [str(tmp_path / "test.db")],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-src": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "update-row": True,
            },
        },
    )
    db = ds.get_database("test")
    await db.execute_write(
        "CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, name TEXT)"
    )

    response = await ds.client.get("/test/projects")
    assert response.status_code == 200
    assert '"canUpdate": true' in response.text


@pytest.mark.asyncio
async def test_extra_body_script_not_on_non_table_pages(tmp_path):
    """extra_body_script does not emit on non-table pages like database index."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)

    ds = Datasette(
        [str(tmp_path / "test.db")],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-src": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
        },
    )
    db = ds.get_database("test")
    await db.execute_write("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")

    # Database page
    response = await ds.client.get("/test")
    assert response.status_code == 200
    assert "window.__datasette_files" not in response.text


# --- file_actions plugin hook ---


@pytest.mark.asyncio
async def test_file_actions_hook_no_actions_by_default(upload_dir):
    """Without any plugin implementing file_actions, no action menu is shown."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    result = await _upload_file(ds)
    file_id = result["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}")
    assert response.status_code == 200
    assert "File actions" not in response.text
    assert "actions-menu-links" not in response.text


@pytest.mark.asyncio
async def test_file_actions_hook_with_plugin(upload_dir):
    """A plugin implementing file_actions adds actions to the file info page."""
    from datasette import hookimpl
    from datasette.plugins import pm

    class FileActionsTestPlugin:
        __name__ = "FileActionsTestPlugin"

        @hookimpl
        def file_actions(self, datasette, actor, file, preview_bytes):
            return [
                {
                    "href": f"/-/convert-csv/{file['id']}",
                    "label": "Convert to table",
                    "description": "Import this CSV as a database table",
                },
            ]

    pm.register(FileActionsTestPlugin(), name="undo_FileActionsTestPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
        )
        result = await _upload_file(ds)
        file_id = result["file_id"]

        response = await ds.client.get(f"/-/files/{file_id}")
        assert response.status_code == 200
        assert "File actions" in response.text
        assert "actions-menu-links" in response.text
        assert "Convert to table" in response.text
        assert "Import this CSV as a database table" in response.text
        assert f"/-/convert-csv/{file_id}" in response.text
    finally:
        pm.unregister(name="undo_FileActionsTestPlugin")


@pytest.mark.asyncio
async def test_file_actions_hook_async(upload_dir):
    """file_actions hook supports async implementations."""
    from datasette import hookimpl
    from datasette.plugins import pm

    class AsyncFileActionsPlugin:
        __name__ = "AsyncFileActionsPlugin"

        @hookimpl
        def file_actions(self, datasette, actor, file, preview_bytes):
            async def inner():
                return [
                    {
                        "href": f"/-/async-action/{file['id']}",
                        "label": "Async action",
                    },
                ]

            return inner

    pm.register(AsyncFileActionsPlugin(), name="undo_AsyncFileActionsPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
        )
        result = await _upload_file(ds)
        file_id = result["file_id"]

        response = await ds.client.get(f"/-/files/{file_id}")
        assert response.status_code == 200
        assert "Async action" in response.text
        assert f"/-/async-action/{file_id}" in response.text
    finally:
        pm.unregister(name="undo_AsyncFileActionsPlugin")


@pytest.mark.asyncio
async def test_file_actions_suggestion_based_on_preview_bytes(upload_dir):
    """Plugin can inspect preview_bytes to decide whether to suggest an action."""
    from datasette import hookimpl
    from datasette.plugins import pm

    class CsvSuggestionPlugin:
        __name__ = "CsvSuggestionPlugin"

        @hookimpl
        def file_actions(self, datasette, actor, file, preview_bytes):
            # Only suggest for files that look like CSV
            if file.get("content_type") == "text/csv" or file.get(
                "filename", ""
            ).endswith(".csv"):
                try:
                    text = preview_bytes.decode("utf-8", errors="ignore")
                    if "," in text:
                        return [
                            {
                                "href": f"/-/import-csv/{file['id']}",
                                "label": "Import as table",
                                "description": "Import this CSV file as a database table",
                            },
                        ]
                except Exception:
                    pass
            return []

    pm.register(CsvSuggestionPlugin(), name="undo_CsvSuggestionPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
        )

        # Upload a CSV file - should get the action suggested
        csv_result = await _upload_file(
            ds,
            filename="data.csv",
            content=b"name,age\nAlice,30\nBob,25\n",
            content_type="text/csv",
        )
        response = await ds.client.get(f"/-/files/{csv_result['file_id']}")
        assert response.status_code == 200
        assert "Import as table" in response.text
        assert f"/-/import-csv/{csv_result['file_id']}" in response.text

        # Upload a plain text file - should NOT get the action
        txt_result = await _upload_file(
            ds,
            filename="notes.txt",
            content=b"Just some plain text notes.",
            content_type="text/plain",
        )
        response = await ds.client.get(f"/-/files/{txt_result['file_id']}")
        assert response.status_code == 200
        assert "Import as table" not in response.text
    finally:
        pm.unregister(name="undo_CsvSuggestionPlugin")


# --- CSV Import tests ---


@pytest.mark.asyncio
async def test_imports_table_created_at_startup(upload_dir):
    """The _datasette_files_imports table is created in internal DB at startup."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    await ds.invoke_startup()
    db = ds.get_internal_database()
    tables = (
        await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='_datasette_files_imports'"
        )
    ).rows
    assert len(tables) == 1

    # Verify expected columns
    columns = (await db.execute("PRAGMA table_info(_datasette_files_imports)")).rows
    col_names = {row["name"] for row in columns}
    assert "id" in col_names
    assert "file_id" in col_names
    assert "import_type" in col_names
    assert "database_name" in col_names
    assert "table_name" in col_names
    assert "status" in col_names
    assert "row_count" in col_names
    assert "total_size" in col_names
    assert "bytes_read" in col_names
    assert "error" in col_names
    assert "started_at" in col_names
    assert "finished_at" in col_names
    assert "actor_id" in col_names


@pytest.mark.asyncio
async def test_csv_file_actions_suggests_import(upload_dir):
    """The built-in file_actions hook suggests importing CSV files as tables."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    result = await _upload_file(
        ds,
        filename="data.csv",
        content=b"name,age\nAlice,30\nBob,25\n",
        content_type="text/csv",
    )
    file_id = result["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}")
    assert response.status_code == 200
    assert "Import as table" in response.text
    assert f"/-/files/import/{file_id}" in response.text


@pytest.mark.asyncio
async def test_tsv_file_actions_suggests_import(upload_dir):
    """The built-in file_actions hook suggests importing TSV files as tables."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    result = await _upload_file(
        ds,
        filename="data.tsv",
        content=b"name\tage\nAlice\t30\nBob\t25\n",
        content_type="text/tab-separated-values",
    )
    file_id = result["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}")
    assert response.status_code == 200
    assert "Import as table" in response.text
    assert f"/-/files/import/{file_id}" in response.text


@pytest.mark.asyncio
async def test_csv_file_actions_not_for_non_csv(upload_dir):
    """The built-in file_actions hook does NOT suggest import for non-CSV files."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    result = await _upload_file(
        ds,
        filename="photo.png",
        content=b"\x89PNG\r\n\x1a\n",
        content_type="image/png",
    )
    file_id = result["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}")
    assert response.status_code == 200
    assert "Import as table" not in response.text


@pytest.mark.asyncio
async def test_import_preview_get(upload_dir):
    """GET /-/files/import/{file_id} shows a preview of the CSV data and a form."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    result = await _upload_file(
        ds,
        filename="data.csv",
        content=b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n",
        content_type="text/csv",
    )
    file_id = result["file_id"]

    response = await ds.client.get(f"/-/files/import/{file_id}")
    assert response.status_code == 200
    # Should show the filename
    assert "data.csv" in response.text
    # Should show column headers from first row
    assert "name" in response.text
    assert "age" in response.text
    assert "city" in response.text
    # Should show preview data
    assert "Alice" in response.text
    assert "Bob" in response.text
    # Should have a table name input
    assert "table_name" in response.text
    # Should have a submit button
    assert "Import" in response.text


@pytest.mark.asyncio
async def test_import_preview_get_404_for_missing_file(upload_dir):
    """GET /-/files/import/{bad_id} returns 404."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True},
    )
    response = await ds.client.get("/-/files/import/df-00000000000000000000000000")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_import_post_requires_csrf_token(upload_dir):
    """POST /-/files/import/{file_id} without a CSRF token should return 403."""
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    result = await _upload_file(
        ds,
        filename="data.csv",
        content=b"name,age\nAlice,30\n",
        content_type="text/csv",
    )
    file_id = result["file_id"]

    # GET the page first to obtain the CSRF cookie
    get_response = await ds.client.get(f"/-/files/import/{file_id}")
    assert get_response.status_code == 200
    csrf_cookie = get_response.cookies.get("ds_csrftoken")
    assert csrf_cookie, "Expected ds_csrftoken cookie from GET"

    # POST with the cookie but without the csrftoken form field
    response = await ds.client.post(
        f"/-/files/import/{file_id}",
        data={"table_name": "data", "database_name": "data"},
        cookies={"ds_csrftoken": csrf_cookie},
        follow_redirects=False,
    )
    assert response.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "permissions,create_table,expected_status,expected_error",
    [
        (
            {"files-browse": True, "files-upload": True},
            False,
            403,
            "create-table",
        ),
        (
            {"files-browse": True, "files-upload": True, "create-table": True},
            False,
            403,
            "insert-row",
        ),
        (
            {
                "files-browse": True,
                "files-upload": True,
                "create-table": True,
                "insert-row": True,
            },
            True,
            400,
            "already exists",
        ),
    ],
    ids=["no-create-table", "no-insert-row", "table-already-exists"],
)
async def test_import_permission_checks(
    tmp_path, permissions, create_table, expected_status, expected_error
):
    """POST /-/files/import/{file_id} checks create-table, insert-row, and table existence."""
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)
    db_path = str(tmp_path / "data.db")

    if create_table:
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE people (name TEXT, age INTEGER)")
        conn.close()
    else:
        _create_sqlite_database(db_path)

    ds = Datasette(
        [db_path],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-uploads": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": permissions,
        },
    )

    result = await _upload_file(
        ds,
        filename="people.csv",
        content=b"name,age\nAlice,30\n",
        content_type="text/csv",
    )

    response = await ds.client.post(
        f"/-/files/import/{result['file_id']}",
        data={"table_name": "people", "database_name": "data"},
        follow_redirects=False,
    )
    assert response.status_code == expected_status
    assert expected_error in response.json()["error"]


@pytest.mark.asyncio
async def test_import_post_creates_job_and_imports(tmp_path):
    """POST /-/files/import/{file_id} creates import job, imports CSV, and redirects to progress page."""
    import asyncio

    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)
    db_path = str(tmp_path / "data.db")
    _create_sqlite_database(db_path)

    ds = Datasette(
        [db_path],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-uploads": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "files-upload": True,
                "create-table": True,
                "insert-row": True,
            },
        },
    )

    csv_content = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n"
    result = await _upload_file(
        ds,
        filename="people.csv",
        content=csv_content,
        content_type="text/csv",
    )
    file_id = result["file_id"]

    # POST to start the import
    response = await ds.client.post(
        f"/-/files/import/{file_id}",
        data={"table_name": "people", "database_name": "data"},
        follow_redirects=False,
    )
    # Should redirect to progress page
    assert response.status_code in (301, 302, 303)
    location = response.headers["location"]
    assert "/-/files/import/" in location

    # Give the async task time to complete
    await asyncio.sleep(0.5)

    # Verify import job was created in internal DB
    internal_db = ds.get_internal_database()
    jobs = (await internal_db.execute("SELECT * FROM _datasette_files_imports")).rows
    assert len(jobs) == 1
    job = dict(jobs[0])
    assert job["file_id"] == file_id
    assert job["import_type"] == "csv"
    assert job["database_name"] == "data"
    assert job["table_name"] == "people"
    assert job["status"] == "finished"
    assert job["row_count"] == 3

    # Verify the table was actually created with the right data
    data_db = ds.get_database("data")
    rows = (await data_db.execute("SELECT * FROM people ORDER BY name")).rows
    assert len(rows) == 3
    assert dict(rows[0])["name"] == "Alice"
    assert dict(rows[1])["name"] == "Bob"
    assert dict(rows[2])["name"] == "Charlie"

    # Verify TypeTracker ran - age should be integer, not text
    col_info = (await data_db.execute("PRAGMA table_info(people)")).rows
    col_types = {row["name"]: row["type"] for row in col_info}
    assert col_types["age"].lower() == "integer"
    assert col_types["name"].lower() == "text"


@pytest.mark.asyncio
async def test_import_csv_with_empty_rows(tmp_path):
    """CSV import handles files with empty rows (blank lines) without division by zero."""
    import asyncio

    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)
    db_path = str(tmp_path / "data.db")
    _create_sqlite_database(db_path)

    ds = Datasette(
        [db_path],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-uploads": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "files-upload": True,
                "create-table": True,
                "insert-row": True,
            },
        },
    )

    csv_content = b"name,age\nAlice,30\n\nBob,25\n\n\nCharlie,35\n"
    result = await _upload_file(
        ds,
        filename="with_blanks.csv",
        content=csv_content,
        content_type="text/csv",
    )
    file_id = result["file_id"]

    response = await ds.client.post(
        f"/-/files/import/{file_id}",
        data={"table_name": "with_blanks", "database_name": "data"},
        follow_redirects=False,
    )
    assert response.status_code in (301, 302, 303)

    await asyncio.sleep(0.5)

    internal_db = ds.get_internal_database()
    jobs = (await internal_db.execute("SELECT * FROM _datasette_files_imports")).rows
    job = dict(jobs[0])
    assert job["status"] == "finished"
    assert job["row_count"] == 3

    data_db = ds.get_database("data")
    rows = (await data_db.execute("SELECT * FROM with_blanks ORDER BY name")).rows
    assert len(rows) == 3
    names = [dict(r)["name"] for r in rows]
    assert names == ["Alice", "Bob", "Charlie"]


@pytest.mark.asyncio
async def test_import_progress_json(tmp_path):
    """GET /-/files/import/{file_id}/{import_id}.json returns progress data."""
    import asyncio

    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)
    db_path = str(tmp_path / "data.db")
    _create_sqlite_database(db_path)

    ds = Datasette(
        [db_path],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-uploads": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "files-upload": True,
                "create-table": True,
                "insert-row": True,
            },
        },
    )

    csv_content = b"name,age\nAlice,30\nBob,25\n"
    result = await _upload_file(
        ds,
        filename="progress_test.csv",
        content=csv_content,
        content_type="text/csv",
    )
    file_id = result["file_id"]

    # Start import
    response = await ds.client.post(
        f"/-/files/import/{file_id}",
        data={"table_name": "progress_test", "database_name": "data"},
        follow_redirects=False,
    )
    location = response.headers["location"]
    # Extract import_id from redirect URL
    import_id = location.split("/")[-1]

    await asyncio.sleep(0.5)

    # Fetch progress JSON
    response = await ds.client.get(f"{location}.json")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "finished"
    assert data["row_count"] == 2
    assert data["total_size"] == len(csv_content)
    assert data["bytes_read"] == len(csv_content)
    assert "table_url" in data


@pytest.mark.asyncio
async def test_import_progress_page(tmp_path):
    """GET /-/files/import/{file_id}/{import_id} returns an HTML progress page with progress bar."""
    import asyncio

    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir)
    db_path = str(tmp_path / "data.db")
    _create_sqlite_database(db_path)

    ds = Datasette(
        [db_path],
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-uploads": {
                            "storage": "filesystem",
                            "config": {"root": upload_dir},
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "files-upload": True,
                "create-table": True,
                "insert-row": True,
            },
        },
    )

    csv_content = b"name,age\nAlice,30\nBob,25\n"
    result = await _upload_file(
        ds,
        filename="progress_page.csv",
        content=csv_content,
        content_type="text/csv",
    )
    file_id = result["file_id"]

    response = await ds.client.post(
        f"/-/files/import/{file_id}",
        data={"table_name": "progress_page", "database_name": "data"},
        follow_redirects=False,
    )
    location = response.headers["location"]

    await asyncio.sleep(0.5)

    # Fetch progress HTML page
    response = await ds.client.get(location)
    assert response.status_code == 200
    assert "import-progress" in response.text or "progress" in response.text.lower()
    # Should contain the table name
    assert "progress_page" in response.text
