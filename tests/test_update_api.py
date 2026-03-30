"""Tests for POST /-/files/{file_id}/-/update endpoint."""

import pytest
import json

from conftest import _upload_file


@pytest.mark.asyncio
async def test_update_search_text(datasette_all_permissions):
    ds = datasette_all_permissions
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.post(
        f"/-/files/{file_id}/-/update",
        content=json.dumps({"update": {"search_text": "new description here"}}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["file"]["search_text"] == "new description here"


@pytest.mark.asyncio
async def test_update_requires_edit_permission(datasette_browse_allowed):
    ds = datasette_browse_allowed
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.post(
        f"/-/files/{file_id}/-/update",
        content=json.dumps({"update": {"search_text": "test"}}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_invalid_field(datasette_all_permissions):
    ds = datasette_all_permissions
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.post(
        f"/-/files/{file_id}/-/update",
        content=json.dumps({"update": {"filename": "hacked.exe"}}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    data = response.json()
    assert data["ok"] is False
    assert "filename" in data["errors"][0]


@pytest.mark.asyncio
async def test_update_missing_update_key(datasette_all_permissions):
    ds = datasette_all_permissions
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.post(
        f"/-/files/{file_id}/-/update",
        content=json.dumps({"search_text": "wrong structure"}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    data = response.json()
    assert data["ok"] is False


@pytest.mark.asyncio
async def test_update_nonexistent_file(datasette_all_permissions):
    ds = datasette_all_permissions
    response = await ds.client.post(
        "/-/files/df-00000000000000000000000000/-/update",
        content=json.dumps({"update": {"search_text": "test"}}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_search_text_appears_in_search(datasette_all_permissions):
    """Updated search_text should be findable via search."""
    ds = datasette_all_permissions
    data = await _upload_file(
        ds, filename="invoice.pdf", content_type="application/pdf"
    )
    file_id = data["file_id"]

    # Update search text
    await ds.client.post(
        f"/-/files/{file_id}/-/update",
        content=json.dumps({"update": {"search_text": "acme corporation billing"}}),
        headers={"Content-Type": "application/json"},
    )

    # Search for it
    response = await ds.client.get("/-/files/search.json?q=acme")
    assert response.status_code == 200
    data = response.json()
    file_ids = [f["id"] for f in data["files"]]
    assert file_id in file_ids
