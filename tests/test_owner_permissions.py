"""Tests for owner-based edit/delete permissions.

When `owners_can_edit` or `owners_can_delete` is configured in datasette-files
plugin settings, the user who uploaded a file gains edit/delete permission on
that specific file — even without a blanket files-edit/files-delete grant.
"""

from collections import namedtuple

from datasette.app import Datasette
import json
import pytest

from conftest import _make_datasette, _upload_file


def _make_owner_datasette(upload_dir, owners_can_edit=False, owners_can_delete=False):
    """Create a Datasette with browse+upload for everyone, and owner permissions configured."""
    sources = {
        "test-uploads": {
            "storage": "filesystem",
            "config": {"root": upload_dir},
        }
    }
    plugin_config = {"sources": sources}
    if owners_can_edit:
        plugin_config["owners_can_edit"] = True
    if owners_can_delete:
        plugin_config["owners_can_delete"] = True

    return Datasette(
        memory=True,
        config={
            "plugins": {"datasette-files": plugin_config},
            "permissions": {
                "files-browse": True,
                "files-upload": True,
            },
        },
    )


async def _upload_as(ds, actor_id, filename="test.txt", content=b"hello"):
    """Upload a file as a specific actor and return the file_id."""
    cookies = {"ds_actor": ds.sign({"a": {"id": actor_id}}, "actor")}
    prepare = await ds.client.post(
        "/-/files/upload/test-uploads/-/prepare",
        content=json.dumps(
            {
                "filename": filename,
                "content_type": "text/plain",
                "size": len(content),
            }
        ),
        headers={"Content-Type": "application/json"},
        cookies=cookies,
    )
    assert prepare.status_code == 200, prepare.text
    data = prepare.json()

    upload = await ds.client.post(
        data["upload_url"],
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\n" + data["upload_token"].encode() + b"\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="'
            + filename.encode()
            + b'"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\n" + content + b"\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
        cookies=cookies,
    )
    assert upload.status_code == 200, upload.text

    complete = await ds.client.post(
        "/-/files/upload/test-uploads/-/complete",
        content=json.dumps({"upload_token": data["upload_token"]}),
        headers={"Content-Type": "application/json"},
        cookies=cookies,
    )
    assert complete.status_code == 201, complete.text
    return complete.json()["file"]["id"]


# --- FileResource action registration ---


def test_file_resource_used_for_edit_and_delete():
    """files-edit and files-delete should be registered against FileResource."""
    from datasette_files import FileResource, FileSourceResource, register_actions

    actions = {a.name: a for a in register_actions()}
    assert actions["files-edit"].resource_class is FileResource
    assert actions["files-delete"].resource_class is FileResource
    assert actions["files-browse"].resource_class is FileSourceResource
    assert actions["files-upload"].resource_class is FileSourceResource


# --- Source-level permissions still cascade to files ---


@pytest.mark.asyncio
async def test_source_level_delete_permission_still_works(datasette_all_permissions):
    """A source-level files-delete grant should still allow deleting any file."""
    ds = datasette_all_permissions
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.post(
        f"/-/files/{file_id}/-/delete",
        content=json.dumps({}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    assert response.json()["ok"] is True


@pytest.mark.asyncio
async def test_source_level_edit_permission_still_works(datasette_all_permissions):
    """A source-level files-edit grant should still allow editing any file."""
    ds = datasette_all_permissions
    data = await _upload_file(ds)
    file_id = data["file_id"]

    response = await ds.client.post(
        f"/-/files/{file_id}/-/update",
        content=json.dumps({"update": {"search_text": "updated"}}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    assert response.json()["ok"] is True


# --- Owner permission tests ---

OwnerPermissionCase = namedtuple(
    "OwnerPermissionCase",
    (
        "description",
        "endpoint_path",
        "body",
        "owners_can_edit",
        "owners_can_delete",
        "actor",
        "expect_status",
    ),
)

OWNER_PERMISSION_CASES = (
    OwnerPermissionCase(
        description="Owner can delete their own file",
        endpoint_path="/-/delete",
        body={},
        owners_can_edit=False,
        owners_can_delete=True,
        actor="alice",
        expect_status=200,
    ),
    OwnerPermissionCase(
        description="Owner can edit their own file",
        endpoint_path="/-/update",
        body={"update": {"search_text": "new"}},
        owners_can_edit=True,
        owners_can_delete=False,
        actor="alice",
        expect_status=200,
    ),
    OwnerPermissionCase(
        description="Owner cannot delete without owners_can_delete config",
        endpoint_path="/-/delete",
        body={},
        owners_can_edit=False,
        owners_can_delete=False,
        actor="alice",
        expect_status=403,
    ),
    OwnerPermissionCase(
        description="Owner cannot edit without owners_can_edit config",
        endpoint_path="/-/update",
        body={"update": {"search_text": "new"}},
        owners_can_edit=False,
        owners_can_delete=False,
        actor="alice",
        expect_status=403,
    ),
    OwnerPermissionCase(
        description="owners_can_edit does not grant delete",
        endpoint_path="/-/delete",
        body={},
        owners_can_edit=True,
        owners_can_delete=False,
        actor="alice",
        expect_status=403,
    ),
    OwnerPermissionCase(
        description="owners_can_delete does not grant edit",
        endpoint_path="/-/update",
        body={"update": {"search_text": "new"}},
        owners_can_edit=False,
        owners_can_delete=True,
        actor="alice",
        expect_status=403,
    ),
    OwnerPermissionCase(
        description="Non-owner cannot delete even with owners_can_delete",
        endpoint_path="/-/delete",
        body={},
        owners_can_edit=False,
        owners_can_delete=True,
        actor="bob",
        expect_status=403,
    ),
    OwnerPermissionCase(
        description="Non-owner cannot edit even with owners_can_edit",
        endpoint_path="/-/update",
        body={"update": {"search_text": "new"}},
        owners_can_edit=True,
        owners_can_delete=False,
        actor="bob",
        expect_status=403,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    OwnerPermissionCase._fields,
    OWNER_PERMISSION_CASES,
    ids=[c.description for c in OWNER_PERMISSION_CASES],
)
async def test_owner_permission(
    upload_dir,
    description,
    endpoint_path,
    body,
    owners_can_edit,
    owners_can_delete,
    actor,
    expect_status,
):
    ds = _make_owner_datasette(
        upload_dir,
        owners_can_edit=owners_can_edit,
        owners_can_delete=owners_can_delete,
    )
    # Always upload as alice — actor param controls who attempts the action
    file_id = await _upload_as(ds, "alice")

    cookies = {"ds_actor": ds.sign({"a": {"id": actor}}, "actor")}
    response = await ds.client.post(
        f"/-/files/{file_id}{endpoint_path}",
        content=json.dumps(body),
        headers={"Content-Type": "application/json"},
        cookies=cookies,
    )
    assert response.status_code == expect_status


@pytest.mark.asyncio
async def test_anonymous_upload_no_owner(upload_dir):
    """Files uploaded without an actor have no owner — owner permissions don't apply."""
    ds = _make_owner_datasette(upload_dir, owners_can_delete=True)

    # Upload without actor
    data = await _upload_file(ds)
    file_id = data["file_id"]

    # Even with owners_can_delete, alice can't delete an ownerless file
    cookies = {"ds_actor": ds.sign({"a": {"id": "alice"}}, "actor")}
    response = await ds.client.post(
        f"/-/files/{file_id}/-/delete",
        content=json.dumps({}),
        headers={"Content-Type": "application/json"},
        cookies=cookies,
    )
    assert response.status_code == 403
