# Next Steps for datasette-files

This document outlines the planned development roadmap for the datasette-files plugin.

## Current State Summary

The plugin has a working MVP for S3 file uploads with:
- Browser-based upload UI with drag-and-drop
- Presigned POST for client-direct S3 uploads
- Database tracking of uploaded files
- Plugin hook system foundation

## Priority 1: Cell Rendering & File Display

### 1.1 render_cell() Hook for File References

Implement the `render_cell()` plugin hook to detect and render file references in table cells.

**File Reference Format:**
- Single file: `"df-{ulid}"` (lowercase `df-` prefix)
- Multiple files: `["df-{ulid1}", "df-{ulid2}"]` (JSON array)

**Rendered Output:**
- Display filename (extracted from last path segment), size, and modified time
- Link to file detail page at `/-/files/{ulid}`
- For arrays, render all files in a list

**Tasks:**
- [ ] Implement `render_cell()` hook to detect `df-` prefixed strings
- [ ] Parse JSON arrays to detect multiple file references
- [ ] Look up file metadata from `files_files` table by ULID
- [ ] Extract filename from path (last segment of `uploads/{ulid}/{filename}`)
- [ ] Render file info with link to detail page
- [ ] Handle missing/invalid ULIDs gracefully

### 1.2 File Detail Page (`/-/files/{ulid}`)

Create a detail page for individual files.

**Tasks:**
- [ ] Add route `/-/files/{ulid}` returning file detail page
- [ ] Display file metadata (filename, size, type, upload time)
- [ ] Provide download link (expiring S3 URL)
- [ ] Inline view for images (img tag with expiring URL)
- [ ] Return 404 for unknown ULIDs

### 1.3 File Download Endpoint

**Tasks:**
- [ ] Add `/-/files/{ulid}/download` endpoint
- [ ] Generate expiring S3 presigned GET URL
- [ ] Redirect to the presigned URL (or proxy if needed)

## Priority 2: Storage Plugin Architecture

### 2.1 Implement S3 Storage Plugin Class

Convert the hardcoded S3 functionality into a proper Storage subclass implementing the abstract interface in `base.py`.

**Tasks:**
- [ ] Create `datasette_files/storages/s3.py` implementing the `Storage` abstract class
- [ ] Implement `list_files()` with pagination support using S3 list objects
- [ ] Implement `read_file()` using boto3 S3 get_object
- [ ] Implement `expiring_download_url()` using S3 presigned GET URLs
- [ ] Implement `upload_form_fields()` (move existing presigned POST logic)
- [ ] Register S3 storage via the plugin hook system
- [ ] Update `/__init__.py` to use the storage plugin system instead of hardcoded S3

### 2.2 File Deletion

**Tasks:**
- [ ] Add `DELETE /-/files/{ulid}` endpoint
- [ ] Implement S3 delete_object in storage class
- [ ] Add delete UI buttons with confirmation
- [ ] Handle cascading deletes in database

### 2.3 File Browser UI

**Tasks:**
- [ ] Create `/-/files` page listing all uploaded files
- [ ] Display file metadata (name, size, type, upload date)
- [ ] Add pagination for large file lists
- [ ] Add filtering/search capabilities
- [ ] Include download and delete actions per file

## Priority 3: Testing

### 3.1 Unit Tests

**Tasks:**
- [ ] Test database schema creation
- [ ] Test file metadata persistence
- [ ] Test upload state transitions (pending → queued → uploading → complete)
- [ ] Test Storage abstract class interface compliance

### 3.2 Integration Tests

**Tasks:**
- [ ] Test S3 presigned POST generation (with mocked boto3)
- [ ] Test upload completion endpoint
- [ ] Test file download endpoints
- [ ] Test permission checking
- [ ] Test CSRF protection behavior

### 3.3 End-to-End Tests

**Tasks:**
- [ ] Test full upload flow with mocked S3
- [ ] Test error handling scenarios
- [ ] Test concurrent upload handling

## Priority 4: Multiple Storage Backends

### 4.1 Complete Plugin Hook System

**Tasks:**
- [ ] Document the storage plugin hook interface
- [ ] Create example plugin implementation
- [ ] Test plugin registration and discovery
- [ ] Allow runtime storage configuration via database

### 4.2 Local Filesystem Storage Backend

**Tasks:**
- [ ] Create `datasette_files/storages/local.py`
- [ ] Implement file-based storage for development/testing
- [ ] Configure storage path via settings
- [ ] Handle security considerations (path traversal prevention)

### 4.3 Google Cloud Storage Backend

**Tasks:**
- [ ] Create `datasette_files/storages/gcs.py`
- [ ] Implement GCS signed URL uploads
- [ ] Add google-cloud-storage dependency (optional)

### 4.4 Azure Blob Storage Backend

**Tasks:**
- [ ] Create `datasette_files/storages/azure.py`
- [ ] Implement Azure SAS token uploads
- [ ] Add azure-storage-blob dependency (optional)

## Priority 5: Security & Permissions

### 5.1 Permission System

**Tasks:**
- [ ] Define granular permissions: upload, download, delete, admin
- [ ] Implement per-file permission checking
- [ ] Integrate with Datasette's actor permissions
- [ ] Add permission configuration UI for admins

### 5.2 Input Validation & Security

**Tasks:**
- [ ] Validate file types (configurable allowlist/blocklist)
- [ ] Validate file sizes (configurable max size)
- [ ] Sanitize file names
- [ ] Implement rate limiting for uploads
- [ ] Add audit logging for file operations

### 5.3 S3 Configuration Security

**Tasks:**
- [ ] Remove hardcoded environment variables
- [ ] Use datasette-secrets for credential storage
- [ ] Validate bucket existence and permissions on startup
- [ ] Add health check endpoint for storage connectivity

## Priority 6: Documentation

### 6.1 User Documentation

**Tasks:**
- [ ] Complete README.md with full usage instructions
- [ ] Document configuration options
- [ ] Add screenshots of the upload UI
- [ ] Provide example deployment configurations

### 6.2 Developer Documentation

**Tasks:**
- [ ] Document the Storage plugin interface
- [ ] Create guide for building custom storage backends
- [ ] Add API reference for all endpoints
- [ ] Document database schema

### 6.3 Examples

**Tasks:**
- [ ] Create example project using datasette-files
- [ ] Document integration with other Datasette plugins
- [ ] Provide Docker deployment example

## Priority 7: UX Improvements

### 7.1 Upload UI Enhancements

**Tasks:**
- [ ] Add file type icons
- [ ] Show upload speed/time remaining
- [ ] Add retry button for failed uploads
- [ ] Support folder uploads (recursive)
- [ ] Add paste-to-upload functionality

### 7.2 File Management UI

**Tasks:**
- [ ] Add file preview for images
- [ ] Support bulk selection and operations
- [ ] Add file renaming capability
- [ ] Implement file organization (folders/tags)

### 7.3 Accessibility

**Tasks:**
- [ ] Ensure keyboard navigation for all features
- [ ] Add proper ARIA labels
- [ ] Test with screen readers
- [ ] Support reduced motion preferences

## Technical Debt

### Code Quality

**Tasks:**
- [ ] Add type hints throughout codebase
- [ ] Set up mypy for type checking
- [ ] Add pre-commit hooks (ruff, black)
- [ ] Refactor hardcoded S3 logic into storage plugin

### Error Handling

**Tasks:**
- [ ] Improve error messages for S3 connectivity issues
- [ ] Add graceful degradation when storage is unavailable
- [ ] Implement proper error responses for API endpoints
- [ ] Add user-friendly error display in UI

## Suggested Implementation Order

1. **Phase 1 - Cell Rendering** (Priority 1)
   - Implement `render_cell()` hook for `df-{ulid}` detection
   - Create file detail page at `/-/files/{ulid}`
   - Add download endpoint with expiring URLs
   - Inline image viewing

2. **Phase 2 - Storage Architecture** (Priority 2.1)
   - Refactor S3 into proper Storage plugin class
   - Implement expiring download URLs via storage interface

3. **Phase 3 - File Management** (Priority 2.2 + 2.3)
   - File browser UI at `/-/files`
   - Delete functionality

4. **Phase 4 - Testing** (Priority 3)
   - Unit tests for render_cell detection
   - Integration tests for file endpoints
   - E2E tests with mocked S3

5. **Phase 5 - Production Ready** (Priority 5 + 6.1)
   - Security and permissions
   - User documentation

6. **Phase 6 - Ecosystem** (Priority 4)
   - Additional storage backends (local, GCS, Azure)
   - Plugin system documentation

7. **Phase 7 - Polish** (Priority 7 + Technical Debt)
   - UX improvements
   - Address technical debt
