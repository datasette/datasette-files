"""Tests for the unified prepare/upload/complete upload API and upload UI."""
from datasette.app import Datasette
import pytest
import json
import os

from conftest import _upload_file, _make_datasette


# --- Upload page UI ---


@pytest.mark.asyncio
async def test_upload_page_uses_web_component(datasette_all_permissions):
    ds = datasette_all_permissions
    response = await ds.client.get("/-/files/upload/test-uploads")
    assert response.status_code == 200
    assert "datasette-file-upload" in response.text
    assert 'source="test-uploads"' in response.text
    assert "datasette-file-upload.js" in response.text


@pytest.mark.asyncio
async def test_source_page_shows_upload_component(datasette_all_permissions):
    ds = datasette_all_permissions
    # Upload a file so the source page has content
    await _upload_file(ds)
    response = await ds.client.get("/-/files/source/test-uploads")
    assert response.status_code == 200
    assert "datasette-file-upload" in response.text
    assert 'source="test-uploads"' in response.text


# --- Prepare endpoint ---


@pytest.mark.asyncio
async def test_prepare_returns_upload_instructions(datasette_all_permissions):
    ds = datasette_all_permissions
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/prepare",
        content=json.dumps(
            {
                "filename": "report.pdf",
                "content_type": "application/pdf",
                "size": 12345,
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert "upload_token" in data
    assert "upload_url" in data
    assert data["upload_method"] == "POST"
    assert "upload_fields" in data
    # upload_token should be in the upload_fields for filesystem
    assert data["upload_fields"]["upload_token"] == data["upload_token"]


@pytest.mark.asyncio
async def test_prepare_requires_upload_permission(datasette_browse_only):
    ds = datasette_browse_only
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/prepare",
        content=json.dumps(
            {"filename": "test.txt", "content_type": "text/plain", "size": 100}
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 403
    data = response.json()
    assert data["ok"] is False
    assert "errors" in data


@pytest.mark.asyncio
async def test_prepare_requires_filename(datasette_all_permissions):
    ds = datasette_all_permissions
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/prepare",
        content=json.dumps({"content_type": "text/plain", "size": 100}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    data = response.json()
    assert data["ok"] is False


@pytest.mark.asyncio
async def test_prepare_invalid_source(datasette_all_permissions):
    ds = datasette_all_permissions
    response = await ds.client.post(
        "/-/files/upload/nonexistent/-/prepare",
        content=json.dumps(
            {"filename": "test.txt", "content_type": "text/plain", "size": 100}
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 404


# --- Content endpoint ---


async def _prepare_upload(ds, filename="test.txt", content_type="text/plain", size=100):
    """Helper: call prepare and return the response data."""
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/prepare",
        content=json.dumps(
            {"filename": filename, "content_type": content_type, "size": size}
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    return response.json()


@pytest.mark.asyncio
async def test_content_upload(datasette_all_permissions):
    ds = datasette_all_permissions
    prep = await _prepare_upload(ds)
    token = prep["upload_token"]

    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/content",
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\n" + token.encode() + b"\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="test.txt"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\nHello world\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_content_upload_invalid_token(datasette_all_permissions):
    ds = datasette_all_permissions
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/content",
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\nbad-token\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="test.txt"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\nHello world\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )
    assert response.status_code == 400
    data = response.json()
    assert data["ok"] is False


# --- Complete endpoint ---


@pytest.mark.asyncio
async def test_complete_registers_file(datasette_all_permissions, upload_dir):
    ds = datasette_all_permissions
    prep = await _prepare_upload(ds, filename="hello.txt")
    token = prep["upload_token"]

    # Upload content
    await ds.client.post(
        "/-/files/upload/test-uploads/-/content",
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\n" + token.encode() + b"\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="hello.txt"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\nHello world\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )

    # Complete
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/complete",
        content=json.dumps({"upload_token": token}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["ok"] is True
    assert "file" in data
    file_data = data["file"]
    assert file_data["id"].startswith("df-")
    assert file_data["filename"] == "hello.txt"
    assert file_data["content_type"] == "text/plain"
    assert file_data["size"] == 11  # "Hello world"
    assert file_data["source_slug"] == "test-uploads"
    assert file_data["download_url"].startswith("/-/files/df-")
    assert file_data["content_hash"].startswith("sha256:")


@pytest.mark.asyncio
async def test_complete_without_content_fails(datasette_all_permissions):
    ds = datasette_all_permissions
    prep = await _prepare_upload(ds)
    token = prep["upload_token"]

    # Try to complete without uploading content
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/complete",
        content=json.dumps({"upload_token": token}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    data = response.json()
    assert data["ok"] is False
    assert "not been uploaded" in data["errors"][0]


@pytest.mark.asyncio
async def test_complete_double_use_fails(datasette_all_permissions):
    ds = datasette_all_permissions
    prep = await _prepare_upload(ds, filename="double.txt")
    token = prep["upload_token"]

    # Upload content
    await ds.client.post(
        "/-/files/upload/test-uploads/-/content",
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\n" + token.encode() + b"\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="double.txt"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\ndata\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )

    # First complete
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/complete",
        content=json.dumps({"upload_token": token}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 201

    # Second complete should fail
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/complete",
        content=json.dumps({"upload_token": token}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_full_upload_flow_file_on_disk(datasette_all_permissions, upload_dir):
    """Full prepare -> content -> complete flow, verifying file lands on disk."""
    ds = datasette_all_permissions

    file_content = b"This is a test PDF content"
    prep = await _prepare_upload(
        ds, filename="test.pdf", content_type="application/pdf", size=len(file_content)
    )
    token = prep["upload_token"]

    # Upload content
    await ds.client.post(
        prep["upload_url"],
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\n" + token.encode() + b"\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="test.pdf"\r\n'
            b"Content-Type: application/pdf\r\n"
            b"\r\n" + file_content + b"\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )

    # Complete
    response = await ds.client.post(
        "/-/files/upload/test-uploads/-/complete",
        content=json.dumps({"upload_token": token}),
        headers={"Content-Type": "application/json"},
    )
    data = response.json()
    file_id = data["file"]["id"]

    # Verify file on disk
    ulid_part = file_id[3:]
    expected_path = os.path.join(upload_dir, ulid_part, "test.pdf")
    assert os.path.exists(expected_path)
    with open(expected_path, "rb") as f:
        assert f.read() == file_content

    # Verify we can download it
    download = await ds.client.get(f"/-/files/{file_id}/download")
    assert download.status_code == 200
    assert download.content == file_content
