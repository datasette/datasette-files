from datasette.app import Datasette
import pytest


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
