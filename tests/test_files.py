from datasette.app import Datasette
import pytest
import pytest_asyncio
import time
from unittest.mock import patch, MagicMock


@pytest_asyncio.fixture
async def datasette_with_file():
    """Create a Datasette instance with a test file in the database."""
    datasette = Datasette(memory=True)
    # Trigger startup to create tables
    await datasette.client.get("/")

    db = datasette.get_internal_database()

    # Insert a test file
    await db.execute_write(
        """
        INSERT INTO files_files (ulid, source_id, path, size, mtime, type, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "01hq3k5p7r2x4y6z8a0b1c2d3e",
            1,
            "uploads/01hq3k5p7r2x4y6z8a0b1c2d3e/test-image.png",
            12345,
            int(time.time()),
            "image/png",
            "{}",
        ),
    )

    # Insert a second test file (non-image)
    await db.execute_write(
        """
        INSERT INTO files_files (ulid, source_id, path, size, mtime, type, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "01hq3k5p7r2x4y6z8a0b1c2d4f",
            1,
            "uploads/01hq3k5p7r2x4y6z8a0b1c2d4f/document.pdf",
            67890,
            int(time.time()),
            "application/pdf",
            "{}",
        ),
    )

    return datasette


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-files" in installed_plugins


@pytest.mark.asyncio
async def test_database_schema_created_on_startup():
    """Test that the plugin creates the required database tables on startup."""
    datasette = Datasette(memory=True)
    # Trigger startup by making a request
    await datasette.client.get("/")

    db = datasette.get_internal_database()

    # Check that all three tables exist
    tables = await db.table_names()
    assert "files_sources" in tables
    assert "files_files" in tables
    assert "files_pending" in tables

    # Verify files_files table has expected columns
    columns = await db.execute("PRAGMA table_info(files_files)")
    column_names = {row[1] for row in columns.rows}
    assert "ulid" in column_names
    assert "source_id" in column_names
    assert "path" in column_names
    assert "size" in column_names
    assert "type" in column_names


@pytest.mark.asyncio
async def test_routes_are_registered():
    """Test that the plugin registers the expected routes."""
    datasette = Datasette(memory=True)

    # Test that upload page returns HTML on GET
    response = await datasette.client.get("/-/files/s3/upload")
    assert response.status_code == 200

    # Test that complete endpoint requires id parameter
    response = await datasette.client.post("/-/files/complete")
    assert response.status_code == 400
    assert response.json()["error"] == "Missing id parameter"

    # Test that a non-existent route returns 404
    response = await datasette.client.get("/-/files/nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_file_detail_page(datasette_with_file):
    """Test the file detail page displays file information."""
    datasette = datasette_with_file

    # Mock the S3 client to avoid needing real credentials
    with patch("datasette_files.s3_client") as mock_s3:
        mock_s3.generate_presigned_url.return_value = "https://example.com/presigned-url"

        # Test existing file
        response = await datasette.client.get("/-/files/01hq3k5p7r2x4y6z8a0b1c2d3e")
        assert response.status_code == 200
        html = response.text
        assert "test-image.png" in html
        assert "image/png" in html
        assert "df-01hq3k5p7r2x4y6z8a0b1c2d3e" in html
        # Should have preview section for images
        assert "Preview" in html

        # Test non-image file (no preview)
        response = await datasette.client.get("/-/files/01hq3k5p7r2x4y6z8a0b1c2d4f")
        assert response.status_code == 200
        html = response.text
        assert "document.pdf" in html
        assert "Preview" not in html

    # Test non-existent file returns 404 (no S3 call needed)
    response = await datasette.client.get("/-/files/01hq3k5p7r2x4y6z8a0b1c2d9z")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_render_cell_single_file(datasette_with_file):
    """Test render_cell hook detects single file references."""
    from datasette_files import render_cell, FILE_REF_PATTERN

    datasette = datasette_with_file

    # Test pattern matching
    assert FILE_REF_PATTERN.match("df-01hq3k5p7r2x4y6z8a0b1c2d3e")
    assert not FILE_REF_PATTERN.match("not-a-file-ref")
    assert not FILE_REF_PATTERN.match("df-tooshort")

    # Test render_cell with valid file reference
    result = render_cell(
        value="df-01hq3k5p7r2x4y6z8a0b1c2d3e",
        datasette=datasette,
    )
    # render_cell returns an async function
    html = await result()
    assert html is not None
    assert "test-image.png" in str(html)
    assert "/-/files/01hq3k5p7r2x4y6z8a0b1c2d3e" in str(html)

    # Test render_cell with non-matching value
    result = render_cell(value="not-a-file-ref", datasette=datasette)
    html = await result()
    assert html is None


@pytest.mark.asyncio
async def test_render_cell_missing_file(datasette_with_file):
    """Test render_cell handles missing files gracefully."""
    from datasette_files import render_cell

    datasette = datasette_with_file

    # Test with non-existent ULID
    result = render_cell(
        value="df-01hq3k5p7r2x4y6z8a0b1c2d9z",
        datasette=datasette,
    )
    html = await result()
    assert html is not None
    assert "df-missing" in str(html)


@pytest.mark.asyncio
async def test_render_cell_json_array(datasette_with_file):
    """Test render_cell handles JSON arrays of file references."""
    from datasette_files import render_cell

    datasette = datasette_with_file

    # Test with JSON array of file references
    result = render_cell(
        value='["df-01hq3k5p7r2x4y6z8a0b1c2d3e", "df-01hq3k5p7r2x4y6z8a0b1c2d4f"]',
        datasette=datasette,
    )
    html = await result()
    assert html is not None
    assert "test-image.png" in str(html)
    assert "document.pdf" in str(html)
    assert "df-file-list" in str(html)

    # Test with non-file-ref array
    result = render_cell(value='["not", "files"]', datasette=datasette)
    html = await result()
    assert html is None
