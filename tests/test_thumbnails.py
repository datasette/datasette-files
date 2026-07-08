import asyncio
import io
import os
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
    from datasette_files import _drain_eager_thumbnails

    ds = datasette_browse_allowed
    jpeg_bytes = _make_test_jpeg()
    data = await _upload_file(
        ds, filename="eager.jpg", content=jpeg_bytes, content_type="image/jpeg"
    )
    file_id = data["file_id"]
    await _drain_eager_thumbnails()

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
        from datasette_files import _drain_eager_thumbnails

        await _drain_eager_thumbnails()

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
    from datasette_files import _drain_eager_thumbnails

    ds = datasette_all_permissions
    jpeg_bytes = _make_test_jpeg(100, 100)
    data = await _upload_file(
        ds, filename="todelete.jpg", content=jpeg_bytes, content_type="image/jpeg"
    )
    file_id = data["file_id"]
    await _drain_eager_thumbnails()

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


# --- Group 7: Resource safety policies ---


@pytest.mark.asyncio
async def test_thumbnail_skips_source_larger_than_configured_limit(upload_dir):
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
        plugin_options={
            "thumbnail_eager": False,
            "thumbnail_max_source_bytes": 100,
        },
    )
    data = await _upload_file(
        ds,
        filename="large.jpg",
        content=_make_test_jpeg(),
        content_type="image/jpeg",
    )

    response = await ds.client.get(f"/-/files/{data['file_id']}/thumbnail")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/svg+xml"
    row = (
        await ds.get_internal_database().execute(
            "SELECT status, reason FROM datasette_files_thumbnail_failures WHERE file_id = ?",
            [data["file_id"]],
        )
    ).first()
    assert dict(row) == {"status": "skipped", "reason": "too_large"}


@pytest.mark.asyncio
async def test_filesystem_bounded_read_rejects_actual_oversize_file(tmp_path):
    from datasette_files.base import FileTooLarge
    from datasette_files.filesystem import FilesystemStorage

    storage = FilesystemStorage()
    await storage.configure({"root": str(tmp_path)}, get_secret=None)
    (tmp_path / "large.bin").write_bytes(b"x" * 101)
    with pytest.raises(FileTooLarge):
        await storage.read_file_limited("large.bin", 100)


@pytest.mark.asyncio
async def test_thumbnail_rejects_image_over_pixel_limit(upload_dir):
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
        plugin_options={
            "thumbnail_eager": False,
            "thumbnail_max_pixels": 10_000,
        },
    )
    data = await _upload_file(
        ds,
        filename="many-pixels.jpg",
        content=_make_test_jpeg(200, 200),
        content_type="image/jpeg",
    )

    response = await ds.client.get(f"/-/files/{data['file_id']}/thumbnail")
    assert response.headers["content-type"] == "image/svg+xml"
    row = (
        await ds.get_internal_database().execute(
            "SELECT reason FROM datasette_files_thumbnail_failures WHERE file_id = ?",
            [data["file_id"]],
        )
    ).first()
    assert row["reason"] == "too_many_pixels"


@pytest.mark.asyncio
async def test_thumbnail_eager_generation_can_be_disabled(upload_dir):
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
        plugin_options={"thumbnail_eager": False},
    )
    data = await _upload_file(
        ds,
        filename="lazy.jpg",
        content=_make_test_jpeg(),
        content_type="image/jpeg",
    )
    row = (
        await ds.get_internal_database().execute(
            "SELECT file_id FROM datasette_files_thumbnails WHERE file_id = ?",
            [data["file_id"]],
        )
    ).first()
    assert row is None


@pytest.mark.asyncio
async def test_failed_generation_is_cached_and_generator_version_invalidates(upload_dir):
    from datasette import hookimpl
    from datasette.plugins import pm

    calls = 0

    class FailingPlugin:
        __name__ = "FailingThumbnailPlugin"

        @hookimpl
        def register_thumbnail_generators(self, datasette):
            class FailingGenerator:
                name = "failing"
                version = "1"

                async def can_generate(self, content_type, filename):
                    return content_type == "application/x-failing"

                async def generate(self, *args, **kwargs):
                    nonlocal calls
                    calls += 1
                    return None

            self.generator = FailingGenerator()
            return [self.generator]

    plugin = FailingPlugin()
    pm.register(plugin, name="undo_FailingThumbnailPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
            plugin_options={"thumbnail_eager": False},
        )
        data = await _upload_file(
            ds,
            filename="bad.fail",
            content=b"not renderable",
            content_type="application/x-failing",
        )
        url = f"/-/files/{data['file_id']}/thumbnail"
        await ds.client.get(url)
        await ds.client.get(url)
        assert calls == 1

        plugin.generator.version = "2"
        await ds.client.get(url)
        assert calls == 2
    finally:
        pm.unregister(name="undo_FailingThumbnailPlugin")


@pytest.mark.asyncio
async def test_thumbnail_generation_timeout_is_negatively_cached(upload_dir):
    from datasette import hookimpl
    from datasette.plugins import pm

    calls = 0

    class SlowPlugin:
        __name__ = "SlowThumbnailPlugin"

        @hookimpl
        def register_thumbnail_generators(self, datasette):
            class SlowGenerator:
                name = "slow"

                async def can_generate(self, content_type, filename):
                    return content_type == "application/x-slow"

                async def generate(self, *args, **kwargs):
                    nonlocal calls
                    calls += 1
                    await asyncio.sleep(0.2)

            return [SlowGenerator()]

    pm.register(SlowPlugin(), name="undo_SlowThumbnailPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
            plugin_options={
                "thumbnail_eager": False,
                "thumbnail_timeout_seconds": 0.01,
            },
        )
        data = await _upload_file(
            ds,
            filename="slow.bin",
            content=b"slow",
            content_type="application/x-slow",
        )
        url = f"/-/files/{data['file_id']}/thumbnail"
        response = await ds.client.get(url)
        assert response.headers["content-type"] == "image/svg+xml"
        await ds.client.get(url)
        assert calls == 1
        row = (
            await ds.get_internal_database().execute(
                "SELECT reason FROM datasette_files_thumbnail_failures WHERE file_id = ?",
                [data["file_id"]],
            )
        ).first()
        assert row["reason"] == "timeout"
    finally:
        pm.unregister(name="undo_SlowThumbnailPlugin")


@pytest.mark.asyncio
async def test_thumbnail_generation_is_serialized_by_default(upload_dir):
    from datasette import hookimpl
    from datasette.plugins import pm

    active = 0
    maximum_active = 0

    class ConcurrencyPlugin:
        __name__ = "ConcurrencyThumbnailPlugin"

        @hookimpl
        def register_thumbnail_generators(self, datasette):
            class ConcurrencyGenerator:
                name = "concurrency"

                async def can_generate(self, content_type, filename):
                    return content_type == "application/x-concurrency"

                async def generate(self, *args, **kwargs):
                    nonlocal active, maximum_active
                    active += 1
                    maximum_active = max(maximum_active, active)
                    await asyncio.sleep(0.05)
                    active -= 1
                    return None

            return [ConcurrencyGenerator()]

    pm.register(ConcurrencyPlugin(), name="undo_ConcurrencyThumbnailPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
            plugin_options={"thumbnail_eager": False},
        )
        one = await _upload_file(
            ds, filename="one.bin", content=b"1", content_type="application/x-concurrency"
        )
        two = await _upload_file(
            ds, filename="two.bin", content=b"2", content_type="application/x-concurrency"
        )
        await asyncio.gather(
            ds.client.get(f"/-/files/{one['file_id']}/thumbnail"),
            ds.client.get(f"/-/files/{two['file_id']}/thumbnail"),
        )
        assert maximum_active == 1
    finally:
        pm.unregister(name="undo_ConcurrencyThumbnailPlugin")


@pytest.mark.asyncio
async def test_missing_source_does_not_permanently_break_thumbnails(upload_dir):
    import datasette_files

    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
        plugin_options={"thumbnail_eager": False},
    )
    data = await _upload_file(
        ds,
        filename="transient.jpg",
        content=_make_test_jpeg(),
        content_type="image/jpeg",
    )
    url = f"/-/files/{data['file_id']}/thumbnail"

    # Simulate the source being temporarily unavailable (e.g. removed from config)
    storage = datasette_files._sources.pop("test-uploads")
    try:
        response = await ds.client.get(url)
        assert response.headers["content-type"] == "image/svg+xml"
    finally:
        datasette_files._sources["test-uploads"] = storage

    # Once the source is back, thumbnails should generate again
    response = await ds.client.get(url)
    assert response.headers["content-type"] == "image/jpeg"


@pytest.mark.asyncio
async def test_failed_generation_is_retried_after_cooldown(upload_dir):
    from datasette import hookimpl
    from datasette.plugins import pm

    calls = 0

    class FlakyPlugin:
        __name__ = "FlakyThumbnailPlugin"

        @hookimpl
        def register_thumbnail_generators(self, datasette):
            class FlakyGenerator:
                name = "flaky"

                async def can_generate(self, content_type, filename):
                    return content_type == "application/x-flaky"

                async def generate(self, *args, **kwargs):
                    nonlocal calls
                    calls += 1
                    raise RuntimeError("transient failure")

            return [FlakyGenerator()]

    pm.register(FlakyPlugin(), name="undo_FlakyThumbnailPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
            plugin_options={"thumbnail_eager": False},
        )
        data = await _upload_file(
            ds,
            filename="flaky.bin",
            content=b"flaky",
            content_type="application/x-flaky",
        )
        url = f"/-/files/{data['file_id']}/thumbnail"
        await ds.client.get(url)
        await ds.client.get(url)
        assert calls == 1  # Within the cooldown the failure stays cached

        # Backdate the failure past the retry cooldown
        await ds.get_internal_database().execute_write(
            """UPDATE datasette_files_thumbnail_failures
               SET created_at = datetime('now', '-1 hour') WHERE file_id = ?""",
            [data["file_id"]],
        )
        await ds.client.get(url)
        assert calls == 2
    finally:
        pm.unregister(name="undo_FlakyThumbnailPlugin")


@pytest.mark.asyncio
async def test_cached_thumbnail_outcome_needs_a_single_cache_query(upload_dir):
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
        plugin_options={"thumbnail_eager": False},
    )
    data = await _upload_file(
        ds, filename="doc.txt", content=b"hello", content_type="text/plain"
    )
    url = f"/-/files/{data['file_id']}/thumbnail"
    # First request caches the skipped/unsupported outcome
    await ds.client.get(url)

    db = ds.get_internal_database()
    executed = []
    original_execute = db.execute

    def spying_execute(sql, *args, **kwargs):
        executed.append(sql)
        return original_execute(sql, *args, **kwargs)

    db.execute = spying_execute
    try:
        response = await ds.client.get(url)
    finally:
        db.execute = original_execute

    assert response.headers["content-type"] == "image/svg+xml"
    cache_queries = [sql for sql in executed if "datasette_files_thumbnail" in sql]
    assert len(cache_queries) == 1


@pytest.mark.asyncio
async def test_generator_can_mark_failure_as_policy_skip(upload_dir):
    from datasette import hookimpl
    from datasette.plugins import pm
    from datasette_files.base import ThumbnailGenerationError

    class SkippingPlugin:
        __name__ = "SkippingThumbnailPlugin"

        @hookimpl
        def register_thumbnail_generators(self, datasette):
            class SkippingGenerator:
                name = "skipping"

                async def can_generate(self, content_type, filename):
                    return content_type == "application/x-skip"

                async def generate(self, *args, **kwargs):
                    raise ThumbnailGenerationError("wrong_colorspace", skipped=True)

            return [SkippingGenerator()]

    pm.register(SkippingPlugin(), name="undo_SkippingThumbnailPlugin")
    try:
        ds = _make_datasette(
            upload_dir,
            permissions={"files-browse": True, "files-upload": True},
            plugin_options={"thumbnail_eager": False},
        )
        data = await _upload_file(
            ds,
            filename="skip.bin",
            content=b"skip",
            content_type="application/x-skip",
        )
        await ds.client.get(f"/-/files/{data['file_id']}/thumbnail")
        row = (
            await ds.get_internal_database().execute(
                "SELECT status, reason FROM datasette_files_thumbnail_failures WHERE file_id = ?",
                [data["file_id"]],
            )
        ).first()
        assert dict(row) == {"status": "skipped", "reason": "wrong_colorspace"}
    finally:
        pm.unregister(name="undo_SkippingThumbnailPlugin")


@pytest.mark.asyncio
async def test_existing_thumbnails_survive_upgrade_migration(upload_dir):
    import datasette_files

    ds = _make_datasette(
        upload_dir, permissions={"files-browse": True, "files-upload": True}
    )
    data = await _upload_file(
        ds,
        filename="legacy.jpg",
        content=_make_test_jpeg(),
        content_type="image/jpeg",
    )
    file_id = data["file_id"]
    await datasette_files._drain_eager_thumbnails()
    db = ds.get_internal_database()

    # Simulate a database created before cache keys existed, holding a file
    # the new limits would refuse to regenerate
    await db.execute_write(
        "UPDATE datasette_files_thumbnails SET cache_key = NULL WHERE file_id = ?",
        [file_id],
    )
    await db.execute_write(
        "UPDATE datasette_files SET size = 999999999 WHERE id = ?", [file_id]
    )

    # Simulate a restart against the existing database
    await datasette_files.startup(ds)()

    response = await ds.client.get(f"/-/files/{file_id}/thumbnail")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/jpeg"


@pytest.mark.asyncio
async def test_thumbnail_settings_are_per_instance(upload_dir, tmp_path):
    strict = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
        plugin_options={
            "thumbnail_eager": False,
            "thumbnail_max_source_bytes": 100,
        },
    )
    data = await _upload_file(
        strict,
        filename="big.jpg",
        content=_make_test_jpeg(),
        content_type="image/jpeg",
    )

    # Starting a second instance in the same process must not affect the first
    other = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
    )
    await other.invoke_startup()

    response = await strict.client.get(f"/-/files/{data['file_id']}/thumbnail")
    assert response.headers["content-type"] == "image/svg+xml"
    row = (
        await strict.get_internal_database().execute(
            "SELECT status, reason FROM datasette_files_thumbnail_failures WHERE file_id = ?",
            [data["file_id"]],
        )
    ).first()
    assert dict(row) == {"status": "skipped", "reason": "too_large"}


@pytest.mark.asyncio
async def test_read_file_limited_default_reads_when_size_unknown():
    from datasette_files.base import (
        FileMetadata,
        FileTooLarge,
        Storage,
        StorageCapabilities,
    )

    class UnknownSizeStorage(Storage):
        storage_type = "unknown-size"
        capabilities = StorageCapabilities()
        content = b"tiny"

        async def configure(self, config, get_secret):
            pass

        async def get_file_metadata(self, path):
            return FileMetadata(path=path, filename=path)

        async def read_file(self, path):
            return self.content

    storage = UnknownSizeStorage()
    assert await storage.read_file_limited("small.jpg", 100) == b"tiny"

    oversized = UnknownSizeStorage()
    oversized.content = b"x" * 101
    with pytest.raises(FileTooLarge):
        await oversized.read_file_limited("large.jpg", 100)


@pytest.mark.asyncio
async def test_thumbnail_generated_when_recorded_size_is_unknown(upload_dir):
    ds = _make_datasette(
        upload_dir,
        permissions={"files-browse": True, "files-upload": True},
        plugin_options={"thumbnail_eager": False},
    )
    data = await _upload_file(
        ds,
        filename="nosize.jpg",
        content=_make_test_jpeg(),
        content_type="image/jpeg",
    )
    db = ds.get_internal_database()
    await db.execute_write(
        "UPDATE datasette_files SET size = NULL WHERE id = ?", [data["file_id"]]
    )

    response = await ds.client.get(f"/-/files/{data['file_id']}/thumbnail")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/jpeg"


@pytest.mark.asyncio
async def test_upload_response_not_blocked_by_thumbnail_queue(upload_dir):
    import datasette_files

    ds = _make_datasette(
        upload_dir, permissions={"files-browse": True, "files-upload": True}
    )
    await ds.invoke_startup()
    state = datasette_files._thumbnail_state(ds)

    # Simulate another request occupying the single generation slot
    await state.semaphore.acquire()
    try:
        data = await asyncio.wait_for(
            _upload_file(
                ds,
                filename="queued.jpg",
                content=_make_test_jpeg(),
                content_type="image/jpeg",
            ),
            timeout=2.0,
        )
    finally:
        state.semaphore.release()

    # The deferred eager generation still completes once the slot frees
    await datasette_files._drain_eager_thumbnails()
    row = (
        await ds.get_internal_database().execute(
            "SELECT file_id FROM datasette_files_thumbnails WHERE file_id = ?",
            [data["file_id"]],
        )
    ).first()
    assert row is not None


@pytest.mark.asyncio
async def test_cancelled_generation_kills_worker_process(monkeypatch, tmp_path):
    import sys
    from datasette_files import pillow_thumbnails

    pid_file = tmp_path / "worker.pid"
    fake_worker = (
        "import os, sys, time\n"
        f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "sys.stdin.buffer.read()\n"
        "time.sleep(30)\n"
    )
    monkeypatch.setattr(
        pillow_thumbnails,
        "_WORKER_COMMAND",
        [sys.executable, "-c", fake_worker],
        raising=False,
    )

    generator = pillow_thumbnails.PillowThumbnailGenerator()
    task = asyncio.create_task(generator.generate(b"x", "image/jpeg", "slow.jpg"))
    for _ in range(200):
        if pid_file.exists() and pid_file.read_text():
            break
        await asyncio.sleep(0.05)
    else:
        task.cancel()
        pytest.fail("worker subprocess never started")
    pid = int(pid_file.read_text())

    # This is what the coordinator's timeout does to a slow generator
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The worker must not outlive the cancelled request
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        await asyncio.sleep(0.05)
    else:
        os.kill(pid, 9)
        pytest.fail("worker subprocess survived cancellation")


@pytest.mark.asyncio
async def test_pillow_generation_runs_in_an_isolated_process():
    from datasette_files.pillow_thumbnails import PillowThumbnailGenerator

    generator = PillowThumbnailGenerator()
    result = await generator.generate(
        _make_test_jpeg(), "image/jpeg", "isolated.jpg"
    )
    assert result is not None
    assert generator.last_worker_pid != os.getpid()
