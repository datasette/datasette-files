# Next Steps for datasette-files

This document outlines the planned development roadmap for the datasette-files plugin.

## Current State Summary

The plugin has a working MVP for S3 file uploads with:
- Browser-based upload UI with drag-and-drop
- Presigned POST for client-direct S3 uploads
- Database tracking of uploaded files
- Plugin hook system foundation

## Priority 1: Core Functionality Completion

### 1.1 Implement S3 Storage Plugin Class

Convert the hardcoded S3 functionality into a proper Storage subclass implementing the abstract interface in `base.py`.

**Tasks:**
- [ ] Create `datasette_files/storages/s3.py` implementing the `Storage` abstract class
- [ ] Implement `list_files()` with pagination support using S3 list objects
- [ ] Implement `read_file()` using boto3 S3 get_object
- [ ] Implement `expiring_download_url()` using S3 presigned GET URLs
- [ ] Implement `upload_form_fields()` (move existing presigned POST logic)
- [ ] Register S3 storage via the plugin hook system
- [ ] Update `/__init__.py` to use the storage plugin system instead of hardcoded S3

### 1.2 File Download Functionality

**Tasks:**
- [ ] Add `GET /-/files/download/<file_id>` endpoint for direct downloads
- [ ] Add `GET /-/files/download-url/<file_id>` endpoint returning expiring download URLs
- [ ] Implement permission checking for file downloads
- [ ] Add download links to the UI

### 1.3 File Deletion

**Tasks:**
- [ ] Add `DELETE /-/files/<file_id>` endpoint
- [ ] Implement S3 delete_object in storage class
- [ ] Add delete UI buttons with confirmation
- [ ] Handle cascading deletes in database

### 1.4 File Browser UI

**Tasks:**
- [ ] Create `/-/files` page listing all uploaded files
- [ ] Display file metadata (name, size, type, upload date)
- [ ] Add pagination for large file lists
- [ ] Add filtering/search capabilities
- [ ] Include download and delete actions per file

## Priority 2: Testing

### 2.1 Unit Tests

**Tasks:**
- [ ] Test database schema creation
- [ ] Test file metadata persistence
- [ ] Test upload state transitions (pending → queued → uploading → complete)
- [ ] Test Storage abstract class interface compliance

### 2.2 Integration Tests

**Tasks:**
- [ ] Test S3 presigned POST generation (with mocked boto3)
- [ ] Test upload completion endpoint
- [ ] Test file download endpoints
- [ ] Test permission checking
- [ ] Test CSRF protection behavior

### 2.3 End-to-End Tests

**Tasks:**
- [ ] Test full upload flow with mocked S3
- [ ] Test error handling scenarios
- [ ] Test concurrent upload handling

## Priority 3: Plugin System & Multiple Backends

### 3.1 Complete Plugin Hook System

**Tasks:**
- [ ] Document the storage plugin hook interface
- [ ] Create example plugin implementation
- [ ] Test plugin registration and discovery
- [ ] Allow runtime storage configuration via database

### 3.2 Local Filesystem Storage Backend

**Tasks:**
- [ ] Create `datasette_files/storages/local.py`
- [ ] Implement file-based storage for development/testing
- [ ] Configure storage path via settings
- [ ] Handle security considerations (path traversal prevention)

### 3.3 Google Cloud Storage Backend

**Tasks:**
- [ ] Create `datasette_files/storages/gcs.py`
- [ ] Implement GCS signed URL uploads
- [ ] Add google-cloud-storage dependency (optional)

### 3.4 Azure Blob Storage Backend

**Tasks:**
- [ ] Create `datasette_files/storages/azure.py`
- [ ] Implement Azure SAS token uploads
- [ ] Add azure-storage-blob dependency (optional)

## Priority 4: Security & Permissions

### 4.1 Permission System

**Tasks:**
- [ ] Define granular permissions: upload, download, delete, admin
- [ ] Implement per-file permission checking
- [ ] Integrate with Datasette's actor permissions
- [ ] Add permission configuration UI for admins

### 4.2 Input Validation & Security

**Tasks:**
- [ ] Validate file types (configurable allowlist/blocklist)
- [ ] Validate file sizes (configurable max size)
- [ ] Sanitize file names
- [ ] Implement rate limiting for uploads
- [ ] Add audit logging for file operations

### 4.3 S3 Configuration Security

**Tasks:**
- [ ] Remove hardcoded environment variables
- [ ] Use datasette-secrets for credential storage
- [ ] Validate bucket existence and permissions on startup
- [ ] Add health check endpoint for storage connectivity

## Priority 5: Documentation

### 5.1 User Documentation

**Tasks:**
- [ ] Complete README.md with full usage instructions
- [ ] Document configuration options
- [ ] Add screenshots of the upload UI
- [ ] Provide example deployment configurations

### 5.2 Developer Documentation

**Tasks:**
- [ ] Document the Storage plugin interface
- [ ] Create guide for building custom storage backends
- [ ] Add API reference for all endpoints
- [ ] Document database schema

### 5.3 Examples

**Tasks:**
- [ ] Create example project using datasette-files
- [ ] Document integration with other Datasette plugins
- [ ] Provide Docker deployment example

## Priority 6: UX Improvements

### 6.1 Upload UI Enhancements

**Tasks:**
- [ ] Add file type icons
- [ ] Show upload speed/time remaining
- [ ] Add retry button for failed uploads
- [ ] Support folder uploads (recursive)
- [ ] Add paste-to-upload functionality

### 6.2 File Management UI

**Tasks:**
- [ ] Add file preview for images
- [ ] Support bulk selection and operations
- [ ] Add file renaming capability
- [ ] Implement file organization (folders/tags)

### 6.3 Accessibility

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

1. **Phase 1 - Foundation** (Priority 1.1 + 2.1)
   - Implement S3 Storage class using plugin interface
   - Add basic unit tests for core functionality

2. **Phase 2 - Complete Upload Flow** (Priority 1.4 + 2.2)
   - Build file browser UI
   - Add integration tests

3. **Phase 3 - Full File Operations** (Priority 1.2 + 1.3)
   - Implement download and delete functionality
   - Complete the file management lifecycle

4. **Phase 4 - Production Ready** (Priority 4 + 5.1)
   - Implement security and permissions
   - Write user documentation

5. **Phase 5 - Ecosystem** (Priority 3)
   - Add additional storage backends
   - Document plugin system for third-party developers

6. **Phase 6 - Polish** (Priority 6 + Technical Debt)
   - UX improvements
   - Address technical debt
