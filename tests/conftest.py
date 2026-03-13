from datasette.app import Datasette
import pytest
import os


@pytest.fixture
def upload_dir(tmp_path):
    """Create a temporary directory for filesystem storage uploads."""
    d = tmp_path / "uploads"
    d.mkdir()
    return str(d)


def _make_datasette(upload_dir, permissions=None, extra_sources=None, databases=None):
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

    kwargs = {"memory": True, "config": config}
    if databases:
        kwargs["memory"] = False
        kwargs["files"] = databases

    return Datasette(**kwargs)


@pytest.fixture
def datasette_with_files(upload_dir):
    """Datasette with a filesystem source but no browse permissions (default deny)."""
    return _make_datasette(upload_dir)


@pytest.fixture
def datasette_upload_allowed(upload_dir):
    """Datasette with files-upload granted but NOT files-browse (default deny)."""
    return _make_datasette(
        upload_dir,
        permissions={
            "files-upload": True,
        },
    )


@pytest.fixture
def datasette_browse_allowed(upload_dir):
    """Datasette with files-browse and files-upload granted to all actors."""
    return _make_datasette(
        upload_dir,
        permissions={
            "files-browse": True,
            "files-upload": True,
        },
    )


@pytest.fixture
def datasette_browse_only(upload_dir):
    """Datasette with files-browse granted but NOT files-upload."""
    return _make_datasette(
        upload_dir,
        permissions={
            "files-browse": True,
        },
    )


@pytest.fixture
def datasette_all_permissions(upload_dir):
    """Datasette with all file permissions granted."""
    return _make_datasette(
        upload_dir,
        permissions={
            "files-browse": True,
            "files-upload": True,
            "files-edit": True,
            "files-delete": True,
        },
    )


async def _upload_file(
    ds,
    source="test-uploads",
    filename="test.txt",
    content=b"Hello from test!",
    content_type="text/plain",
):
    """Helper to upload a file via the legacy multipart endpoint and return the response JSON."""
    response = await ds.client.post(
        f"/-/files/upload/{source}",
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="'
            + filename.encode()
            + b'"\r\n'
            b"Content-Type: " + content_type.encode() + b"\r\n"
            b"\r\n" + content + b"\r\n"
            b"--boundary--\r\n"
        ),
        headers={
            "Content-Type": "multipart/form-data; boundary=boundary",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()
