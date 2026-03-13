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
{"ok": false, "error": "Permission denied"}
```

## Error Format

All error responses use a consistent shape:

```json
{
  "ok": false,
  "error": "Human-readable error message"
}
```

HTTP status codes: `400` (bad request), `403` (forbidden), `404` (not found), `405` (method not allowed).

---

## Endpoints

### 1. Upload a File

There are two upload flows depending on the storage backend:

#### Flow A: Proxy Upload (filesystem and similar backends)

The client sends the file directly to datasette-files, which proxies it to the storage backend. This is the simplest flow and is used by backends that set `requires_proxy_download: true`.

```
POST /-/files/upload/{source_slug}
```

**Permission:** `files-upload` on the source.

**Request:** `multipart/form-data` with a `file` field.

```
Content-Type: multipart/form-data; boundary=...

--boundary
Content-Disposition: form-data; name="file"; filename="report.pdf"
Content-Type: application/pdf

(binary data)
--boundary--
```

**Response (201):**

```json
{
  "ok": true,
  "file": {
    "id": "df-01j5a3b4c5d6e7f8g9h0jkmnpq",
    "filename": "report.pdf",
    "content_type": "application/pdf",
    "content_hash": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
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

**Errors:**
- `400` — No file provided, file too large, or invalid source slug.
- `403` — Missing `files-upload` permission on this source.

**Notes:**
- The current implementation returns a flat object (`{file_id, filename, ...}`). This design wraps it in `{ok, file: {...}}` for consistency with the rest of the API and to include additional fields like `download_url` and `source_slug`.
- Content negotiation: if the `Accept` header includes `text/html`, redirect to the file info page instead.

#### Flow B: Two-Phase Upload (S3 and similar backends)

For backends like S3, it's wasteful to proxy large files through the Datasette server. Instead, the client asks datasette-files for upload instructions (including signed form parameters), uploads directly to the storage backend, then notifies datasette-files that the upload is complete.

**Phase 1: Request upload instructions**

```
POST /-/files/upload/{source_slug}/prepare
```

**Permission:** `files-upload` on the source.

**Request:**

```json
{
  "filename": "large-video.mp4",
  "content_type": "video/mp4",
  "size": 524288000
}
```

**Response (200):**

```json
{
  "ok": true,
  "upload_token": "tok_01j5a3b4c5d6e7f8g9h0jkmnpq",
  "upload_url": "https://my-bucket.s3.amazonaws.com/",
  "upload_method": "POST",
  "upload_headers": {},
  "upload_fields": {
    "key": "uploads/01j5a3b4c5d6e7f8g9h0jkmnpq/large-video.mp4",
    "Content-Type": "video/mp4",
    "X-Amz-Credential": "...",
    "X-Amz-Date": "...",
    "Policy": "...",
    "X-Amz-Signature": "..."
  }
}
```

The client then uploads the file directly to `upload_url` using the specified method, headers, and fields (as a multipart form POST for S3, or a PUT with headers for other backends). The `upload_token` is an opaque, short-lived token that ties phase 1 to phase 2.

**Phase 2: Confirm upload complete**

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
    "filename": "large-video.mp4",
    "content_type": "video/mp4",
    "content_hash": null,
    "size": 524288000,
    "width": null,
    "height": null,
    "source_slug": "product-images",
    "uploaded_by": "alice",
    "created_at": "2026-03-13T14:30:00",
    "url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq",
    "download_url": "/-/files/df-01j5a3b4c5d6e7f8g9h0jkmnpq/download"
  }
}
```

On completion, datasette-files verifies the file exists in the backend (via `get_file_metadata`), registers it in the `datasette_files` table, and returns the full file record.

**Errors:**
- `400` — Invalid or expired upload token, or file not found at the expected path.
- `403` — Missing `files-upload` permission.

#### How clients discover which flow to use

The `/-/files/sources.json` endpoint reports each source's capabilities. Clients check:

- If `requires_proxy_download` is `true` → use **Flow A** (proxy upload via `POST /-/files/upload/{source_slug}`)
- If `can_generate_signed_urls` is `true` → use **Flow B** (prepare/complete)

The `<file-upload>` web component handles this automatically. API clients can inspect `sources.json` to choose the right flow. Both flows return the same `{ok, file}` response shape on success.

#### Why two-phase instead of a single signed URL?

A single endpoint that returns "upload here, then I'll figure it out" has a gap: datasette-files doesn't know when the client-side upload finishes. The explicit `complete` call lets datasette-files:
1. Verify the file actually landed in the backend
2. Record accurate metadata (the backend may compute `content_hash`, `size`, etc.)
3. Assign the permanent `df-{ULID}` file ID atomically
4. Return the file record to the client in the same request

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
DELETE /-/files/{file_id}
```

**Permission:** `files-delete` on the file's source.

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
PATCH /-/files/{file_id}.json
```

**Permission:** `files-edit` on the file's source.

**Request:**

```json
{
  "search_text": "Updated description or extracted text content"
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

**Errors:**
- `404` — File not found.
- `403` — Missing `files-edit` permission.
- `400` — No valid fields provided.

---

## Changes from Current Implementation

| Area | Current | Proposed |
|------|---------|----------|
| Response wrapper | Mixed — some return raw data, upload returns flat `{file_id, ...}` | All responses wrapped in `{ok: true, ...}` |
| Upload response | `{file_id, filename, content_type, size, url}` | `{ok, file: {id, filename, content_type, content_hash, size, source_slug, download_url, ...}}` |
| File metadata response | `dict(row)` (raw DB columns) | Curated `{ok, file: {...}}` with `download_url` |
| Batch response | `{files: {...}}` | `{ok: true, files: {...}}` |
| Search response | `{q, source, files, sources}` | `{ok: true, q, source, files, sources}` |
| Source file listing | HTML only | Add `.json` variant with pagination metadata |
| Delete | Not implemented | `DELETE /-/files/{file_id}` |
| Metadata update | HTML form POST only | `PATCH /-/files/{file_id}.json` |
| Error format | Inconsistent | All errors: `{ok: false, error: "..."}` |

## Design Decisions

### Why `{ok: true/false}` wrapper?

Follows the convention used by Datasette's own write API (`/-/create`, `/-/drop`, etc.). Makes it easy for clients to check success without inspecting HTTP status codes. The `ok` field is always present.

### Why no cursor-based pagination on search?

Search results are capped at 50 rows, which covers the vast majority of use cases. Adding cursor pagination to FTS queries adds complexity (FTS5 rank-ordered results don't have a natural cursor). If needed, this can be added later without breaking the API — add an optional `cursor` parameter and a `next_cursor` field in the response.

### Why silent omission in batch.json?

The batch endpoint exists to power `render_cell` — it fetches metadata for every `df-...` ID visible on a table page. Some files may belong to sources the user can't access. Returning errors for those would complicate the client unnecessarily. Silent omission means the client just doesn't render a rich preview for inaccessible files.

### Why no file content update (PUT)?

Files are immutable by design (per DESIGN.md). Immutability simplifies caching (`ETag` = file ID), avoids versioning complexity, and means `content_hash` is stable. To "update" a file, upload a new one and update the reference.

### Why PATCH for metadata instead of PUT?

`PATCH` signals partial update — only the provided fields are changed. This avoids requiring the client to send the full file metadata object. `PUT` would imply replacing the entire resource, which doesn't make sense for a partial metadata edit.
