"""Tests for POST /-/files/{file_id}/-/delete endpoint."""

from datasette.app import Datasette
import pytest
import json
import os

from conftest import _upload_file, _make_datasette


@pytest.mark.asyncio
async def test_delete_file(datasette_all_permissions, upload_dir):
    ds = datasette_all_permissions
    data = await _upload_file(ds)
    file_id = data["file_id"]

    # Verify file exists on disk
    ulid_part = file_id[3:]
    disk_path = os.path.join(upload_dir, ulid_part, "test.txt")
    assert os.path.exists(disk_path)

    # Delete it
    response = await ds.client.post(
        f"/-/files/{file_id}/-/delete",
        content=json.dumps({}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True

    # Verify file is gone from disk
    assert not os.path.exists(disk_path)

    # Verify 404 on info
    response = await ds.client.get(f"/-/files/{file_id}.json")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_requires_permission(datasette_browse_allowed, upload_dir):
    ds = datasette_browse_allowed
    data = await _upload_file(ds)
    file_id = data["file_id"]

    # browse_allowed doesn't have files-delete permission
    response = await ds.client.post(
        f"/-/files/{file_id}/-/delete",
        content=json.dumps({}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_delete_nonexistent_file(datasette_all_permissions):
    ds = datasette_all_permissions
    response = await ds.client.post(
        "/-/files/df-00000000000000000000000000/-/delete",
        content=json.dumps({}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 404
