"""Tests for the get_file() public Python API."""

from datetime import datetime, timezone
import pytest

from conftest import _upload_file


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-03-13 23:23:24", datetime(2026, 3, 13, 23, 23, 24, tzinfo=timezone.utc)),
        ("2026-03-13T23:23:24", datetime(2026, 3, 13, 23, 23, 24, tzinfo=timezone.utc)),
        (
            "2026-03-13T23:23:24.123456",
            datetime(2026, 3, 13, 23, 23, 24, 123456, tzinfo=timezone.utc),
        ),
        (
            "2026-03-13T15:23:24-08:00",
            datetime(2026, 3, 13, 23, 23, 24, tzinfo=timezone.utc),
        ),
        (
            "2026-03-13T23:23:24Z",
            datetime(2026, 3, 13, 23, 23, 24, tzinfo=timezone.utc),
        ),
    ],
)
def test_parse_created_at_variants(value, expected):
    from datasette_files import _parse_created_at

    assert _parse_created_at(value) == expected


@pytest.mark.asyncio
async def test_get_file_returns_file_object(datasette_all_permissions):
    ds = datasette_all_permissions
    await ds.invoke_startup()
    uploaded = await _upload_file(
        ds,
        filename="hello.txt",
        content=b"Hello, world!",
        content_type="text/plain",
    )
    file_id = uploaded["file_id"]

    from datasette_files import get_file

    file = await get_file(ds, file_id)
    assert file is not None
    assert file.id == file_id
    assert file.filename == "hello.txt"
    assert file.content_type == "text/plain"
    assert file.size == 13
    assert file.source_slug == "test-uploads"


@pytest.mark.asyncio
async def test_get_file_returns_none_for_missing(datasette_all_permissions):
    ds = datasette_all_permissions
    await ds.invoke_startup()

    from datasette_files import get_file

    file = await get_file(ds, "df-00000000000000000000000000")
    assert file is None


@pytest.mark.asyncio
async def test_get_file_read_all(datasette_all_permissions):
    ds = datasette_all_permissions
    await ds.invoke_startup()
    uploaded = await _upload_file(
        ds,
        filename="data.bin",
        content=b"binary content here",
        content_type="application/octet-stream",
    )

    from datasette_files import get_file

    file = await get_file(ds, uploaded["file_id"])
    content = await file.read()
    assert content == b"binary content here"


@pytest.mark.asyncio
async def test_get_file_read_max_bytes(datasette_all_permissions):
    ds = datasette_all_permissions
    await ds.invoke_startup()
    uploaded = await _upload_file(
        ds, filename="big.txt", content=b"abcdefghijklmnop", content_type="text/plain"
    )

    from datasette_files import get_file

    file = await get_file(ds, uploaded["file_id"])
    content = await file.read(max_bytes=5)
    assert content == b"abcde"


@pytest.mark.asyncio
async def test_get_file_open_streaming(datasette_all_permissions):
    ds = datasette_all_permissions
    await ds.invoke_startup()
    uploaded = await _upload_file(
        ds,
        filename="stream.txt",
        content=b"stream this content",
        content_type="text/plain",
    )

    from datasette_files import get_file

    file = await get_file(ds, uploaded["file_id"])
    chunks = []
    async with file.open() as stream:
        async for chunk in stream:
            chunks.append(chunk)
    assert b"".join(chunks) == b"stream this content"


@pytest.mark.asyncio
async def test_get_file_metadata_fields(datasette_all_permissions):
    ds = datasette_all_permissions
    await ds.invoke_startup()
    uploaded = await _upload_file(
        ds, filename="photo.jpg", content=b"\xff\xd8\xff\xe0", content_type="image/jpeg"
    )

    from datasette_files import get_file

    file = await get_file(ds, uploaded["file_id"])
    assert file.uploaded_by is None  # anonymous upload
    assert isinstance(file.created_at, datetime)
    assert file.created_at.tzinfo is timezone.utc
    assert isinstance(file.metadata, dict)
