# datasette-files JSON API Design

## Overview

This document proposes a complete JSON API for uploading, listing, searching, downloading, and deleting files managed by datasette-files. The API follows Datasette's existing conventions: `.json` suffix for JSON responses, permission-gated access, and consistent error shapes.

Several of these endpoints already exist in the codebase. This design consolidates them with the not-yet-implemented endpoints (delete, source listing with pagination, metadata update) into a single coherent specification.

## Authentication & Permissions

All endpoints respect Datasette's permission system. Requests can authenticate via any method Datasette supports (API tokens, cookies, etc.).

Four permission actions scope access at the **source** level:

| Action | Required for |
|--------|-------------|
| `files-browse` | Viewing, searching, downloading files |
| `files-upload` | Uploading files to a source |
| `files-edit` | Editing file metadata (search_text) |
| `files-delete` | Deleting files from a source |

All default to **deny**. Unauthorized requests receive a `403` with:

```json
{"ok": false, "errors": ["Permission denied"]}
```

## Error Format

All error responses use a consistent shape, matching Datasette's write API convention:

```json
{
  "ok": false,
  "errors": ["Human-readable error message"]
}
```

The `errors` field is always an array (even for single errors), matching Datasette's `/-/create`, `/-/insert`, etc.

HTTP status codes: `400` (bad request), `403` (forbidden), `404` (not found).

---

## Endpoints

### 1. Upload a File

All uploads use the same two-phase API pattern, regardless of storage backend:

1. **Prepare**: Client tells datasette-files what it wants to upload. Server returns upload instructions (a URL, method, and any required fields/headers).
2. **Upload**: Client sends the file to the URL from step 1. For filesystem, this is a datasette-files endpoint. For S3, this is a presigned S3 URL. The client doesn't need to know the difference.
3. **Complete**: Client tells datasette-files the upload is done. Server verifies, registers the file, and returns the file record.

This means every client follows the same three-step flow. The only thing that varies is _where_ step 2 sends the bytes, and the prepare response tells the client exactly where that is.

#### Step 1: Prepare

```
POST /-/files/upload/{source_slug}/prepare
```

**Permission:** `files-upload` on the source.

**Request:**

```json
{
  "filename": "report.pdf",
  "content_type": "application/pdf",
  "size": 245678
}
```

**Response (200):**

For a **filesystem** backend, the upload URL points back to a datasette-files endpoint:

```json
{
  "ok": true,
  "upload_token": "tok_01j5a3b4c5d6e7f8g9h0jkmnpq",
  "upload_url": "/-/files/upload/local-uploads/content",
  "upload_method": "POST",
  "upload_headers": {},
  "upload_fields": {
    "upload_token": "tok_01j5a3b4c5d6e7f8g9h0jkmnpq"
  }
}
```

For an **S3** backend, the upload URL points directly to S3:

```json
{
  "ok": true,
  "upload_token": "tok_01j5a3b4c5d6e7f8g9h0jkmnpq",
  "upload_url": "https://my-bucket.s3.amazonaws.com/",
  "upload_method": "POST",
  "upload_headers": {},
  "upload_fields": {
    "key": "uploads/01j5a3b4c5d6e7f8g9h0jkmnpq/report.pdf",
    "Content-Type": "application/pdf",
    "X-Amz-Credential": "...",
    "X-Amz-Date": "...",
    "Policy": "...",
    "X-Amz-Signature": "..."
  }
}
```

**Errors:**
- `400` — Missing required fields, or file too large for this source.
- `403` — Missing `files-upload` permission.
- `404` — Source not found.

**Notes on the upload token:**
- The `upload_token` is an opaque, short-lived token (e.g. a signed JWT or ULID with server-side state) that ties the prepare, upload, and complete steps together.
- It encodes the expected filename, content_type, size, source, storage path, and expiry.
- Tokens expire after a configurable window (e.g. 1 hour) to prevent stale uploads.

#### Step 2: Upload the file content

The client sends the file to `upload_url` using `upload_method`, including any `upload_headers` and `upload_fields`.

For **filesystem** backends, this is a multipart POST to datasette-files:

```
POST /-/files/upload/local-uploads/content
Content-Type: multipart/form-data

upload_token=tok_01j5a3b4c5d6e7f8g9h0jkmnpq
file=(binary data)
```

The `/content` endpoint validates the token, receives the bytes via `storage.receive_upload()`, and returns a simple acknowledgment:

```json
{"ok": true}
```

For **S3** backends, this is a direct POST/PUT to the presigned S3 URL. The client includes the signed fields as form data alongside the file. S3 returns its own response (HTTP 204 on success).

#### Step 3: Complete

```
POST /-/files/upload/{source_slug}/complete
```

**Permission:** `files-upload` on the source.

**Request:**

```json
{
  "upload_token": "tok_01j5a3b4c5d6e7f8g9h0jkmnpq"
}
```

**Response (201):**

```json
{
  "ok": true,
  "file": {
    "id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
    "filename": "report.pdf",
    "content_type": "application/pdf",
    "content_hash": "sha256:e3b0c44298fc1c149afbf4c8996fb924...",
    "size": 245678,
    "width": null,
    "height": null,
    "source_slug": "local-uploads",
    "uploaded_by": "alice",
    "created_at": "2026-03-13T14:30:00",
    "url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq",
    "download_url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq/download"
  }
}
```

On completion, datasette-files:
1. Validates the upload token (not expired, not already used).
2. Verifies the file exists in the backend (via `get_file_metadata` on the expected path).
3. Assigns the permanent `df-{ULID}` file ID.
4. Inserts the record into `datasette_files`.
5. Returns the full file record.

**Errors:**
- `400` — Invalid, expired, or already-used upload token. Or the file was not found at the expected storage path.
- `403` — Missing `files-upload` permission.

#### Why one flow for all backends?

- **Clients are simpler.** Every client implements the same prepare → upload → complete sequence. No branching logic based on backend type.
- **The web component is simpler.** `<file-upload>` always does the same three steps. The only variable is the URL it sends bytes to, which the server tells it.
- **It's honest about what filesystem upload actually is.** Even for local files, the server does real work during prepare (generating a path, allocating a token) and during complete (registering metadata, assigning the ID). The extra round-trips are cheap for local uploads.
- **Future backends just work.** A new storage plugin only needs to implement `prepare_upload()` to return the right URL and fields. The client code doesn't change.

---

### 2. Get File Metadata

```
GET /-/files/{file_id}.json
```

**Permission:** `files-browse` on the file's source.

**Response (200):**

```json
{
  "ok": true,
  "file": {
    "id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
    "source_slug": "local-uploads",
    "filename": "report.pdf",
    "content_type": "application/pdf",
    "content_hash": "sha256:e3b0c44298fc1c149afbf4c8996fb924...",
    "size": 245678,
    "width": null,
    "height": null,
    "uploaded_by": "alice",
    "created_at": "2026-03-13T14:30:00",
    "metadata": {},
    "search_text": "",
    "download_url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq/download"
  }
}
```

**Errors:**
- `404` — File not found.
- `403` — Missing `files-browse` permission.

---

### 3. Download a File

```
GET /-/files/{file_id}/download
```

**Permission:** `files-browse` on the file's source.

**Response:**
- **Signed-URL backends** (S3, GCS): `302` redirect to a short-lived signed URL.
- **Proxy backends** (filesystem): The file content streamed directly with appropriate `Content-Type` and `Content-Disposition: inline; filename="..."` headers.

**Headers (proxy mode):**
```
Content-Type: application/pdf
Content-Disposition: inline; filename="report.pdf"
```

**Errors:**
- `404` — File not found.
- `403` — Missing `files-browse` permission.

---

### 4. Batch File Metadata

```
GET /-/files/batch.json?id={file_id}&id={file_id}&...
```

**Permission:** `files-browse` checked per-file (inaccessible files are silently omitted).

**Response (200):**

```json
{
  "ok": true,
  "files": {
    "df-01j5a3b4c5d6e7f8g9h0jkmnpq": {
      "id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
      "filename": "report.pdf",
      "content_type": "application/pdf",
      "size": 245678,
      "width": null,
      "height": null,
      "download_url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq/download",
      "info_url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq"
    },
    "df-01j6x7y8z9a0b1c2d3e4f5g6h7": {
      "id": "df-01j6x7y8z9a0b1c2d3e4f5g6h7",
      "filename": "logo.png",
      "content_type": "image/png",
      "size": 12345,
      "width": 800,
      "height": 600,
      "download_url": "/-/files/df-01j6x7y8z9a0b1c2d3e4f5g6h7/download",
      "info_url": "/-/files/df-01j6x7y8z9a0b1c2d3e4f5g6h7"
    }
  }
}
```

**Notes:**
- Files the actor cannot browse are silently excluded from the result (no error, just missing from the `files` dict). This is intentional — the batch endpoint is used by `render_cell` to fetch metadata for all file IDs visible in a table page, and some may belong to sources the user cannot access.
- Invalid file IDs (not matching `df-[a-z0-9]+`) are silently ignored.
- Returns `{"ok": true, "files": {}}` if no valid/accessible IDs are provided.

---

### 5. Search Files

```
GET /-/files/search.json?q={query}&source={source_slug}
```

**Permission:** `files-browse` (results filtered to accessible sources only).

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `q` | No | Full-text search query. Prefix-matched against filename, content_type, and search_text. |
| `source` | No | Filter to a specific source slug. |

**Response (200):**

```json
{
  "ok": true,
  "q": "report",
  "source": null,
  "files": [
    {
      "id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
      "filename": "report.pdf",
      "content_type": "application/pdf",
      "size": 245678,
      "width": null,
      "height": null,
      "created_at": "2026-03-13T14:30:00",
      "uploaded_by": "alice",
      "source_slug": "local-uploads"
    }
  ],
  "sources": ["local-uploads", "product-images"]
}
```

**Notes:**
- `sources` lists all source slugs the actor has `files-browse` permission on, useful for building source filter dropdowns.
- When `q` is empty/omitted, returns recent files (ordered by `created_at` descending).
- Results are capped at 50 rows. A future iteration could add cursor-based pagination.
- FTS5 prefix matching: each search term gets a `*` suffix. Multiple terms are OR'd together.

---

### 6. List Files in a Source

```
GET /-/files/source/{source_slug}.json?page={n}
```

**Permission:** `files-browse` on the source.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `page` | No | Page number (1-based, default 1). |

**Response (200):**

```json
{
  "ok": true,
  "source": "local-uploads",
  "files": [
    {
      "id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
      "filename": "report.pdf",
      "content_type": "application/pdf",
      "size": 245678,
      "width": null,
      "height": null,
      "created_at": "2026-03-13T14:30:00",
      "uploaded_by": "alice",
      "source_slug": "local-uploads"
    }
  ],
  "page": 1,
  "total_pages": 3,
  "total_files": 54
}
```

**Notes:**
- Page size is 20 (matching the existing `PAGE_SIZE` constant).
- The current implementation serves this only as HTML at `/-/files/source/{slug}`. This adds a `.json` variant.
- Files are ordered by `created_at` descending (most recent first).

**Errors:**
- `404` — Source not found.
- `403` — Missing `files-browse` permission.

---

### 7. List Sources

```
GET /-/files/sources.json
```

**Permission:** None required (sources are listed, but capabilities are public metadata). Access to files within sources still requires `files-browse`.

**Response (200):**

```json
{
  "ok": true,
  "sources": [
    {
      "slug": "local-uploads",
      "storage_type": "filesystem",
      "capabilities": {
        "can_upload": true,
        "can_delete": true,
        "can_list": true,
        "can_generate_signed_urls": false,
        "requires_proxy_download": true,
        "max_file_size": null
      }
    },
    {
      "slug": "product-images",
      "storage_type": "s3",
      "capabilities": {
        "can_upload": true,
        "can_delete": true,
        "can_list": true,
        "can_generate_signed_urls": true,
        "requires_proxy_download": false,
        "max_file_size": 104857600
      }
    }
  ]
}
```

---

### 8. Delete a File

```
POST /-/files/{file_id}/-/delete
```

**Permission:** `files-delete` on the file's source.

**Request:**

```json
{}
```

**Response (200):**

```json
{
  "ok": true
}
```

**Behavior:**
1. Look up the file record and verify it exists.
2. Check `files-delete` permission on the file's source.
3. Check that the source's storage has `can_delete` capability.
4. Call `storage.delete_file(path)` to remove from the backend.
5. Delete the row from `datasette_files` (FTS triggers handle index cleanup).
6. Return success.

**Errors:**
- `404` — File not found.
- `403` — Missing `files-delete` permission.
- `400` — Storage backend does not support deletion.

**Notes:**
- This endpoint is listed in DESIGN.md but not yet implemented. The Storage ABC already defines `delete_file()`, and `FilesystemStorage` implements it.
- Files are immutable — there is no update/replace endpoint. To replace a file, delete the old one and upload a new one.
- Callers are responsible for updating any table columns that reference the deleted file ID.

---

### 9. Update File Metadata

```
POST /-/files/{file_id}/-/update
```

**Permission:** `files-edit` on the file's source.

**Request:**

```json
{
  "update": {
    "search_text": "Updated description or extracted text content"
  }
}
```

**Response (200):**

```json
{
  "ok": true,
  "file": {
    "id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
    "filename": "report.pdf",
    "search_text": "Updated description or extracted text content",
    "...": "..."
  }
}
```

**Notes:**
- Only `search_text` is editable for now. The current implementation handles this via a form POST on the HTML file info page. This endpoint provides a JSON equivalent.
- Could be extended in the future to allow editing `metadata` (the JSON field) without changing the API shape.
- The `update` key follows the same pattern as Datasette's `POST /<db>/<table>/<pk>/-/update` endpoint.

**Errors:**
- `404` — File not found.
- `403` — Missing `files-edit` permission.
- `400` — No valid fields in `update`.

---

## Changes from Current Implementation

| Area | Current | Proposed |
|------|---------|----------|
| Upload flow | Single POST with multipart, returns file record immediately | Unified three-step prepare → upload → complete for all backends |
| Response wrapper | Mixed — some return raw data, upload returns flat `{file_id, ...}` | All responses wrapped in `{ok: true, ...}` |
| Upload response | `{file_id, filename, content_type, size, url}` | `{ok, file: {id, filename, content_type, content_hash, size, source_slug, download_url, ...}}` |
| File metadata response | `dict(row)` (raw DB columns) | Curated `{ok, file: {...}}` with `download_url` |
| Batch response | `{files: {...}}` | `{ok: true, files: {...}}` |
| Search response | `{q, source, files, sources}` | `{ok: true, q, source, files, sources}` |
| Source file listing | HTML only | Add `.json` variant with pagination metadata |
| Delete | Not implemented | `POST /-/files/{file_id}/-/delete` |
| Metadata update | HTML form POST only | `POST /-/files/{file_id}/-/update` |
| Error format | Inconsistent | All errors: `{ok: false, errors: [...]}` |

## Design Decisions

### Why `{ok: true/false}` wrapper?

Follows the convention used by Datasette's own write API (`/-/create`, `/-/drop`, etc.). Makes it easy for clients to check success without inspecting HTTP status codes. The `ok` field is always present.

### Why no cursor-based pagination on search?

Search results are capped at 50 rows, which covers the vast majority of use cases. Adding cursor pagination to FTS queries adds complexity (FTS5 rank-ordered results don't have a natural cursor). If needed, this can be added later without breaking the API — add an optional `cursor` parameter and a `next_cursor` field in the response.

### Why silent omission in batch.json?

The batch endpoint exists to power `render_cell` — it fetches metadata for every `df-...` ID visible on a table page. Some files may belong to sources the user can't access. Returning errors for those would complicate the client unnecessarily. Silent omission means the client just doesn't render a rich preview for inaccessible files.

### Why POST-only for writes?

Matches Datasette's write API convention. Datasette uses `POST /<db>/<table>/<pk>/-/delete` and `POST /<db>/<table>/<pk>/-/update` rather than `DELETE` and `PATCH` verbs. This keeps the API consistent and avoids issues with proxies and clients that don't support all HTTP methods.

### Why no file content update?

Files are immutable by design (per DESIGN.md). Immutability simplifies caching (`ETag` = file ID), avoids versioning complexity, and means `content_hash` is stable. To "update" a file, upload a new one and update the reference.
