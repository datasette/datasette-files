import io
import pytest
from PIL import Image
from conftest import _make_datasette, _upload_file
from datasette_files.base import ThumbnailResult

# --- Group 1: SVG icon generation ---


def test_generate_file_icon_svg_pdf():
    from datasette_files import _generate_file_icon_svg

    svg = _generate_file_icon_svg("report.pdf", "application/pdf")
    assert svg.startswith("<svg")
    assert "PDF" in svg
    assert "#E05050" in svg  # PDF badge color


def test_generate_file_icon_svg_unknown():
    from datasette_files import _generate_file_icon_svg

    svg = _generate_file_icon_svg("data.xyz", "application/octet-stream")
    assert "XYZ" in svg
    assert "#6B7280" in svg  # default badge color


def test_generate_file_icon_svg_csv_by_content_type():
    from datasette_files import _generate_file_icon_svg

    svg = _generate_file_icon_svg("data.txt", "text/csv")
    assert "CSV" in svg
    assert "#2E7D32" in svg  # CSV badge color


def test_generate_file_icon_svg_escapes_label():
    from datasette_files import _generate_file_icon_svg

    svg = _generate_file_icon_svg(
        'report.</text><script>alert("x")</script>', "application/octet-stream"
    )
    assert "&lt;/TEXT&gt;&lt;SCRIPT&gt;ALERT(&quot;X&quot;)&lt;/SCRIPT&gt;" in svg
    assert "</TEXT><SCRIPT>" not in svg


# --- Group 2: PillowThumbnailGenerator ---


@pytest.mark.asyncio
async def test_pillow_generator_registered_via_hook():
    """PillowThumbnailGenerator should be registered through the plugin hook,
    not hardcoded in startup."""
    from datasette_files import register_thumbnail_generators

    # The function should exist as a hookimpl on the module
    assert hasattr(register_thumbnail_generators, "datasette_impl")
    from datasette_files.pillow_thumbnails import PillowThumbnailGenerator

    result = register_thumbnail_generators(datasette=None)
    assert any(isinstance(g, PillowThumbnailGenerator) for g in result)


@pytest.mark.asyncio
async def test_pillow_can_generate_jpeg():
    from datasette_files.pillow_thumbnails import PillowThumbnailGenerator

    gen = PillowThumbnailGenerator()
    assert await gen.can_generate("image/jpeg", "photo.jpg") is True


@pytest.mark.asyncio
async def test_pillow_cannot_generate_pdf():
    from datasette_files.pillow_thumbnails import PillowThumbnailGenerator

    gen = PillowThumbnailGenerator()
    assert await gen.can_generate("application/pdf", "doc.pdf") is False


@pytest.mark.asyncio
async def test_pillow_generates_thumbnail():
    from datasette_files.pillow_thumbnails import PillowThumbnailGenerator

    img = Image.new("RGB", (400, 300), color="red")
    buf = io.BytesIO()
    img.save(buf, format="JPEG")

    gen = PillowThumbnailGenerator()
    result = await gen.generate(buf.getvalue(), "image/jpeg", "photo.jpg")
    assert result is not None
    assert result.content_type == "image/jpeg"
    assert result.width <= 200
    assert result.height <= 200
    thumb = Image.open(io.BytesIO(result.thumb_bytes))
    assert thumb.width == result.width
    assert thumb.height == result.height


@pytest.mark.asyncio
async def test_pillow_preserves_transparency():
    from datasette_files.pillow_thumbnails import PillowThumbnailGenerator

    img = Image.new("RGBA", (400, 300), color=(255, 0, 0, 128))
    buf = io.BytesIO()
    img.save(buf, format="PNG")

    gen = PillowThumbnailGenerator()
    result = await gen.generate(buf.getvalue(), "image/png", "logo.png")
    assert result is not None
    assert result.content_type == "image/png"


# --- Group 3: Thumbnail endpoint (non-image) ---


@pytest.mark.asyncio
async def test_thumbnail_endpoint_for_text_file(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="readme.txt", content=b"hello")
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/svg+xml"
    assert "cache-control" not in response.headers
    assert "TXT" in response.text


@pytest.mark.asyncio
async def test_thumbnail_endpoint_for_text_file_supports_etag(
    datasette_browse_allowed, upload_dir
):
    ds = datasette_browse_allowed
    data = await _upload_file(ds, filename="readme.txt", content=b"hello")
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert response.status_code == 200
    assert "etag" in response.headers
    etag = response.headers["etag"]

    response_304 = await ds.client.get(
        f"/-/files/{file_id}/thumbnail", headers={"if-none-match": etag}
    )
    assert response_304.status_code == 304
    assert response_304.headers["etag"] == etag
    assert response_304.text == ""


@pytest.mark.asyncio
async def test_thumbnail_endpoint_for_pdf(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    data = await _upload_file(
        ds, filename="doc.pdf", content=b"%PDF-fake", content_type="application/pdf"
    )
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert response.status_code == 200
    assert "image/svg+xml" in response.headers["content-type"]
    assert "PDF" in response.text


@pytest.mark.asyncio
async def test_thumbnail_not_found(datasette_browse_allowed):
    ds = datasette_browse_allowed
    response = await ds.client.get("/-/files/df-00000000000000000000000000/thumbnail")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_thumbnail_permission_denied(datasette_upload_allowed, upload_dir):
    ds = datasette_upload_allowed
    data = await _upload_file(ds)
    file_id = data["file_id"]
    response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert response.status_code == 403


# --- Group 4: Thumbnail endpoint (image) + caching ---


def _make_test_jpeg(width=400, height=300):
    img = Image.new("RGB", (width, height), color="blue")
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def _make_test_png(width=64, height=48):
    img = Image.new("RGBA", (width, height), color=(0, 128, 255, 180))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_thumbnail_endpoint_for_image(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    jpeg_bytes = _make_test_jpeg()
    data = await _upload_file(
        ds, filename="photo.jpg", content=jpeg_bytes, content_type="image/jpeg"
    )
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert response.status_code == 200
    assert response.headers["content-type"] in ("image/jpeg", "image/png")
    assert "cache-control" not in response.headers
    thumb = Image.open(io.BytesIO(response.content))
    assert thumb.width <= 200
    assert thumb.height <= 200


@pytest.mark.asyncio
async def test_thumbnail_endpoint_for_image_supports_etag(
    datasette_browse_allowed, upload_dir
):
    ds = datasette_browse_allowed
    jpeg_bytes = _make_test_jpeg()
    data = await _upload_file(
        ds, filename="photo.jpg", content=jpeg_bytes, content_type="image/jpeg"
    )
    file_id = data["file_id"]

    response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert response.status_code == 200
    assert "etag" in response.headers
    etag = response.headers["etag"]

    response_304 = await ds.client.get(
        f"/-/files/{file_id}/thumbnail", headers={"if-none-match": etag}
    )
    assert response_304.status_code == 304
    assert response_304.headers["etag"] == etag
    assert response_304.text == ""


@pytest.mark.asyncio
async def test_thumbnail_endpoint_uses_registered_non_image_generator(upload_dir):
    from datasette import hookimpl
    from datasette.plugins import pm

    thumbnail_bytes = _make_test_png(32, 24)

    class PdfThumbnailPlugin:
        __name__ = "PdfThumbnailPlugin"

        @hookimpl
        def register_thumbnail_generators(self, datasette):
            class PdfThumbnailGenerator:
                name = "fake-pdf"

                async def can_generate(self, content_type, filename):
                    return content_type == "application/pdf"

                async def generate(
                    self,
                    file_bytes,
                    content_type,
                    filename,
                    max_width=200,
                    max_height=200,
                ):
                    return ThumbnailResult(
                        thumb_bytes=thumbnail_bytes,
                        content_type="image/png",
                        width=32,
                        height=24,
                    )

            return [PdfThumbnailGenerator()]

    pm.register(PdfThumbnailPlugin(), name="undo_PdfThumbnailPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
        )
        data = await _upload_file(
            ds, filename="doc.pdf", content=b"%PDF-fake", content_type="application/pdf"
        )
        file_id = data["file_id"]

        response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
        assert response.status_code == 200
        assert response.headers["content-type"] == "image/png"
        assert response.content == thumbnail_bytes

        db = ds.get_internal_database()
        row = (
            await db.execute(
                "SELECT generator, width, height FROM datasette_files_thumbnails WHERE file_id = ?",
                [file_id],
            )
        ).first()
        assert row is not None
        assert row["generator"] == "fake-pdf"
        assert row["width"] == 32
        assert row["height"] == 24
    finally:
        pm.unregister(name="undo_PdfThumbnailPlugin")


@pytest.mark.asyncio
async def test_thumbnail_is_cached_in_database(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    jpeg_bytes = _make_test_jpeg()
    data = await _upload_file(
        ds, filename="cached.jpg", content=jpeg_bytes, content_type="image/jpeg"
    )
    file_id = data["file_id"]

    resp1 = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert resp1.status_code == 200

    # Verify it's in the database
    db = ds.get_internal_database()
    row = (
        await db.execute(
            "SELECT content_type, generator, width, height FROM datasette_files_thumbnails WHERE file_id = ?",
            [file_id],
        )
    ).first()
    assert row is not None
    assert row["generator"] == "pillow"
    assert row["width"] is not None and row["width"] > 0
    assert row["height"] is not None and row["height"] > 0

    # Second request returns same content (from cache)
    resp2 = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert resp2.content == resp1.content


# --- Group 5: Eager generation on upload ---


@pytest.mark.asyncio
async def test_thumbnail_generated_on_upload(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    jpeg_bytes = _make_test_jpeg()
    data = await _upload_file(
        ds, filename="eager.jpg", content=jpeg_bytes, content_type="image/jpeg"
    )
    file_id = data["file_id"]

    # Thumbnail should already be in the database (eager generation)
    db = ds.get_internal_database()
    row = (
        await db.execute(
            "SELECT file_id FROM datasette_files_thumbnails WHERE file_id = ?",
            [file_id],
        )
    ).first()
    assert row is not None


@pytest.mark.asyncio
async def test_thumbnail_generated_on_upload_for_non_image_generator(upload_dir):
    from datasette import hookimpl
    from datasette.plugins import pm

    thumbnail_bytes = _make_test_png(40, 30)

    class PdfThumbnailPlugin:
        __name__ = "PdfThumbnailPluginForUpload"

        @hookimpl
        def register_thumbnail_generators(self, datasette):
            class PdfThumbnailGenerator:
                name = "fake-pdf"

                async def can_generate(self, content_type, filename):
                    return content_type == "application/pdf"

                async def generate(
                    self,
                    file_bytes,
                    content_type,
                    filename,
                    max_width=200,
                    max_height=200,
                ):
                    return ThumbnailResult(
                        thumb_bytes=thumbnail_bytes,
                        content_type="image/png",
                        width=40,
                        height=30,
                    )

            return [PdfThumbnailGenerator()]

    pm.register(PdfThumbnailPlugin(), name="undo_PdfThumbnailPluginForUpload")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
        )
        data = await _upload_file(
            ds,
            filename="eager.pdf",
            content=b"%PDF-upload",
            content_type="application/pdf",
        )
        file_id = data["file_id"]

        db = ds.get_internal_database()
        row = (
            await db.execute(
                "SELECT generator, content_type, width, height FROM datasette_files_thumbnails WHERE file_id = ?",
                [file_id],
            )
        ).first()
        assert row is not None
        assert row["generator"] == "fake-pdf"
        assert row["content_type"] == "image/png"
        assert row["width"] == 40
        assert row["height"] == 30
    finally:
        pm.unregister(name="undo_PdfThumbnailPluginForUpload")


# --- Group 6: Deletion cleanup ---


@pytest.mark.asyncio
async def test_thumbnail_deleted_with_file(datasette_all_permissions, upload_dir):
    ds = datasette_all_permissions
    jpeg_bytes = _make_test_jpeg(100, 100)
    data = await _upload_file(
        ds, filename="todelete.jpg", content=jpeg_bytes, content_type="image/jpeg"
    )
    file_id = data["file_id"]

    # Generate thumbnail
    await ds.client.get(f"/-/files/{file_id}/thumbnail")

    # Delete the file
    await ds.client.post(f"/-/files/{file_id}/-/delete")

    # Thumbnail should be gone
    db = ds.get_internal_database()
    row = (
        await db.execute(
            "SELECT file_id FROM datasette_files_thumbnails WHERE file_id = ?",
            [file_id],
        )
    ).first()
    assert row is None
