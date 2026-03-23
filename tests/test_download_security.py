"""Tests for secure file download headers (Content-Disposition, X-Content-Type-Options)."""

import pytest
from conftest import _make_datasette, _upload_file


@pytest.mark.asyncio
async def test_svg_download_served_as_attachment(datasette_browse_allowed, upload_dir):
    """SVG files must be served as attachment, not inline, to prevent XSS."""
    ds = datasette_browse_allowed
    svg = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'
    data = await _upload_file(
        ds, filename="evil.svg", content=svg, content_type="image/svg+xml"
    )
    response = await ds.client.get(f"/-/files/{data['file_id']}/download")
    assert response.status_code == 200
    cd = response.headers.get("content-disposition", "")
    assert "attachment" in cd
    assert "inline" not in cd


@pytest.mark.asyncio
async def test_html_download_served_as_attachment(datasette_browse_allowed, upload_dir):
    """HTML files must be served as attachment to prevent XSS."""
    ds = datasette_browse_allowed
    html = b"<html><script>alert(1)</script></html>"
    data = await _upload_file(
        ds, filename="page.html", content=html, content_type="text/html"
    )
    response = await ds.client.get(f"/-/files/{data['file_id']}/download")
    assert response.status_code == 200
    cd = response.headers.get("content-disposition", "")
    assert "attachment" in cd
    assert "inline" not in cd


@pytest.mark.asyncio
async def test_all_downloads_are_attachment(datasette_browse_allowed, upload_dir):
    """All file types should be served as attachment, never inline."""
    ds = datasette_browse_allowed
    for ct in ("image/jpeg", "text/plain", "application/pdf", "text/html"):
        data = await _upload_file(
            ds, filename=f"file.{ct.split('/')[-1]}", content=b"x", content_type=ct
        )
        response = await ds.client.get(f"/-/files/{data['file_id']}/download")
        assert response.status_code == 200
        cd = response.headers.get("content-disposition", "")
        assert "attachment" in cd, f"Expected attachment for {ct}, got: {cd}"
        assert "inline" not in cd


@pytest.mark.asyncio
async def test_download_has_nosniff_header(datasette_browse_allowed, upload_dir):
    """All downloads must include X-Content-Type-Options: nosniff."""
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="test.txt", content=b"hello")
    response = await ds.client.get(f"/-/files/{data['file_id']}/download")
    assert response.status_code == 200
    assert response.headers.get("x-content-type-options") == "nosniff"


@pytest.mark.asyncio
async def test_filename_stripped_to_alnum_in_content_disposition(
    datasette_browse_allowed, upload_dir
):
    """Only a-zA-Z0-9 characters survive in the Content-Disposition filename."""
    ds = datasette_browse_allowed
    data = await _upload_file(
        ds,
        filename='has"quote & spaces!.txt',
        content=b"hello",
        content_type="text/plain",
    )
    response = await ds.client.get(f"/-/files/{data['file_id']}/download")
    assert response.status_code == 200
    cd = response.headers.get("content-disposition", "")
    assert 'filename="hasquotespaces.txt"' in cd


@pytest.mark.asyncio
async def test_filename_fallback_when_all_stripped(
    datasette_browse_allowed, upload_dir
):
    """If all characters are stripped, use 'download' plus the original extension."""
    ds = datasette_browse_allowed
    data = await _upload_file(
        ds, filename="!!!.pdf", content=b"hello", content_type="application/pdf"
    )
    response = await ds.client.get(f"/-/files/{data['file_id']}/download")
    assert response.status_code == 200
    cd = response.headers.get("content-disposition", "")
    assert 'filename="download.pdf"' in cd


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type,expected_ext",
    [
        ("text/plain", "txt"),
        ("image/jpeg", "jpg"),
        ("image/png", "png"),
        ("application/pdf", "pdf"),
        ("application/json", "json"),
        ("text/csv", "csv"),
        ("application/zip", "zip"),
        ("application/octet-stream", "dat"),
    ],
)
async def test_filename_fallback_uses_content_type_extension(
    datasette_browse_allowed, upload_dir, content_type, expected_ext
):
    """If filename has no extension, derive one from content_type."""
    ds = datasette_browse_allowed
    data = await _upload_file(
        ds, filename="$$$", content=b"hello", content_type=content_type
    )
    response = await ds.client.get(f"/-/files/{data['file_id']}/download")
    assert response.status_code == 200
    cd = response.headers.get("content-disposition", "")
    assert f'filename="download.{expected_ext}"' in cd
