// <datasette-file-upload> web component
// Drag-and-drop multi-file upload area with progress bars.
// Uses the prepare/content/complete API flow.

const _FILE_ICON_STYLES = {
  CSV:  { badge: '#2E7D32', bg: '#EEF7EE', stroke: '#7AB87E', fold: '#C4E0C5' },
  PDF:  { badge: '#E05050', bg: '#FDF0EE', stroke: '#D4837A', fold: '#F5C4C0' },
  JSON: { badge: '#F59E0B', bg: '#FFFBEB', stroke: '#D4A34A', fold: '#FDE68A' },
  XLS:  { badge: '#1D6F42', bg: '#EDF5F0', stroke: '#6DA88A', fold: '#B7DAC5' },
  XLSX: { badge: '#1D6F42', bg: '#EDF5F0', stroke: '#6DA88A', fold: '#B7DAC5' },
  DOC:  { badge: '#2B579A', bg: '#EEF1F7', stroke: '#7B8FB8', fold: '#BDC9E0' },
  DOCX: { badge: '#2B579A', bg: '#EEF1F7', stroke: '#7B8FB8', fold: '#BDC9E0' },
  ZIP:  { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  GZ:   { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  MP4:  { badge: '#DC2626', bg: '#FEF2F2', stroke: '#D48A8A', fold: '#FECACA' },
  MOV:  { badge: '#DC2626', bg: '#FEF2F2', stroke: '#D48A8A', fold: '#FECACA' },
  MP3:  { badge: '#9333EA', bg: '#FAF5FF', stroke: '#B48AD8', fold: '#DDD6FE' },
  WAV:  { badge: '#9333EA', bg: '#FAF5FF', stroke: '#B48AD8', fold: '#DDD6FE' },
};
const _TEXT_EXTS = new Set(['TXT', 'MD', 'RST', 'LOG']);
const _DEFAULT_STYLE = { badge: '#6B7280', bg: '#F9FAFB', stroke: '#9CA3AF', fold: '#E5E7EB' };
const _TEXT_STYLE = { badge: '#6B7280', bg: '#F3F4F6', stroke: '#9CA3AF', fold: '#D1D5DB' };

function _getIconStyle(filename, contentType) {
  const ext = filename.includes('.') ? filename.split('.').pop().toUpperCase() : '?';
  if (contentType === 'text/csv') return { ext: 'CSV', ..._FILE_ICON_STYLES.CSV };
  if (contentType === 'application/pdf') return { ext: 'PDF', ..._FILE_ICON_STYLES.PDF };
  if (contentType === 'application/json') return { ext, ..._FILE_ICON_STYLES.JSON };
  if (_FILE_ICON_STYLES[ext]) return { ext, ..._FILE_ICON_STYLES[ext] };
  if (_TEXT_EXTS.has(ext) || (contentType && contentType.startsWith('text/')))
    return { ext, ..._TEXT_STYLE };
  return { ext, ..._DEFAULT_STYLE };
}

function _makeIconSvg(style, size = 48) {
  const h = Math.round(size * 40 / 60);
  return `<svg width="${size}" height="${h}" viewBox="-2 -2 410 310">
  <rect x="4" y="4" width="400" height="300" rx="12" fill="#00000008"/>
  <path d="M0,12 Q0,0 12,0 L340,0 L400,60 L400,288 Q400,300 388,300 L12,300 Q0,300 0,288 Z" fill="${style.bg}" stroke="${style.stroke}" stroke-width="2"/>
  <path d="M340,0 L340,48 Q340,60 352,60 L400,60" fill="${style.fold}" stroke="${style.stroke}" stroke-width="2"/>
  <rect x="100" y="110" width="200" height="80" rx="10" fill="${style.badge}"/>
  <text x="200" y="150" text-anchor="middle" font-family="system-ui,sans-serif" font-size="36" font-weight="500" fill="#FFF" dominant-baseline="central">${style.ext}</text>
</svg>`;
}

function _formatSize(bytes) {
  if (bytes == null) return '';
  if (bytes < 1024) return bytes + ' B';
  const kb = bytes / 1024;
  if (kb < 1024) return kb.toFixed(1) + ' KB';
  const mb = kb / 1024;
  if (mb < 1024) return mb.toFixed(1) + ' MB';
  return (mb / 1024).toFixed(1) + ' GB';
}

function _getCsrfToken() {
  const match = document.cookie.match(/ds_csrftoken=([^;]+)/);
  return match ? match[1] : '';
}

function _setXhrHeaders(xhr, headers) {
  for (const [key, value] of Object.entries(headers || {})) {
    xhr.setRequestHeader(key, value);
  }
}

class DatasetteFileUpload extends HTMLElement {
  connectedCallback() {
    this._source = this.getAttribute('source');
    this._files = []; // {file, status, progress, error, fileId}
    this._uploading = false;
    this._thumbUrls = new WeakMap(); // file -> cached object URL
    this._render();
  }

  _render() {
    this.innerHTML = `
      <style>
        .dsf-upload-area {
          border: 2px dashed #CBD5E1;
          border-radius: 8px;
          padding: 24px;
          text-align: center;
          cursor: pointer;
          transition: border-color 0.15s, background 0.15s;
          margin-bottom: 12px;
        }
        .dsf-upload-area.dragover {
          border-color: #3B82F6;
          background: #EFF6FF;
        }
        .dsf-upload-area p {
          margin: 0 0 8px;
          color: #64748B;
          font-size: 0.95em;
        }
        .dsf-upload-area .dsf-browse-link {
          color: #3B82F6;
          text-decoration: underline;
          cursor: pointer;
        }
        .dsf-file-list {
          list-style: none;
          padding: 0;
          margin: 0 0 12px;
        }
        .dsf-file-item {
          display: flex;
          align-items: center;
          gap: 10px;
          padding: 8px 0;
          border-bottom: 1px solid #F1F5F9;
        }
        .dsf-file-item:last-child { border-bottom: none; }
        .dsf-file-icon { flex-shrink: 0; }
        .dsf-file-icon img {
          width: 48px;
          height: 32px;
          object-fit: contain;
          border-radius: 3px;
        }
        .dsf-file-details {
          flex: 1;
          min-width: 0;
        }
        .dsf-file-name {
          display: block;
          font-size: 0.9em;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .dsf-file-meta {
          display: block;
          font-size: 0.8em;
          color: #94A3B8;
        }
        .dsf-file-remove {
          background: none;
          border: none;
          color: #94A3B8;
          cursor: pointer;
          font-size: 1.2em;
          padding: 0 4px;
        }
        .dsf-file-remove:hover { color: #EF4444; }
        .dsf-progress-bar {
          width: 100%;
          height: 4px;
          background: #E2E8F0;
          border-radius: 2px;
          margin-top: 4px;
          overflow: hidden;
        }
        .dsf-progress-fill {
          height: 100%;
          background: #3B82F6;
          border-radius: 2px;
          transition: width 0.2s;
        }
        .dsf-progress-fill.done { background: #22C55E; }
        .dsf-progress-fill.error { background: #EF4444; }
        .dsf-file-status {
          font-size: 0.8em;
          margin-top: 2px;
        }
        .dsf-file-status.error { color: #EF4444; }
        .dsf-file-status.done { color: #22C55E; }
        .dsf-upload-btn {
          padding: 8px 20px;
          background: #3B82F6;
          color: white;
          border: none;
          border-radius: 6px;
          font-size: 0.95em;
          cursor: pointer;
        }
        .dsf-upload-btn:hover { background: #2563EB; }
        .dsf-upload-btn:disabled {
          background: #94A3B8;
          cursor: not-allowed;
        }
        .dsf-upload-actions {
          display: flex;
          gap: 8px;
          align-items: center;
        }
      </style>
      <div class="dsf-upload-area">
        <p>Drag and drop files here, or <span class="dsf-browse-link">browse</span></p>
        <input type="file" multiple style="display:none">
      </div>
      <ul class="dsf-file-list"></ul>
      <div class="dsf-upload-actions" style="display:none">
        <button class="dsf-upload-btn">Upload</button>
      </div>
    `;

    const dropArea = this.querySelector('.dsf-upload-area');
    const fileInput = this.querySelector('input[type="file"]');
    const browseLink = this.querySelector('.dsf-browse-link');
    const uploadBtn = this.querySelector('.dsf-upload-btn');

    // Drag and drop
    dropArea.addEventListener('dragover', (e) => {
      e.preventDefault();
      dropArea.classList.add('dragover');
    });
    dropArea.addEventListener('dragleave', () => {
      dropArea.classList.remove('dragover');
    });
    dropArea.addEventListener('drop', (e) => {
      e.preventDefault();
      dropArea.classList.remove('dragover');
      this._addFiles(e.dataTransfer.files);
    });

    // Click to browse
    browseLink.addEventListener('click', () => fileInput.click());
    dropArea.addEventListener('click', (e) => {
      if (e.target === dropArea || e.target.tagName === 'P') fileInput.click();
    });

    fileInput.addEventListener('change', () => {
      this._addFiles(fileInput.files);
      fileInput.value = '';
    });

    uploadBtn.addEventListener('click', () => this._uploadAll());
  }

  _addFiles(fileList) {
    for (const file of fileList) {
      this._files.push({
        file,
        status: 'pending',
        progress: 0,
        error: null,
        fileId: null,
      });
    }
    this._renderFileList();
  }

  _renderFileList() {
    const list = this.querySelector('.dsf-file-list');
    const actions = this.querySelector('.dsf-upload-actions');
    const btn = this.querySelector('.dsf-upload-btn');

    list.innerHTML = '';

    const pending = this._files.filter(f => f.status === 'pending');

    for (let i = 0; i < this._files.length; i++) {
      const entry = this._files[i];
      const li = document.createElement('li');
      li.className = 'dsf-file-item';

      // Icon/thumbnail
      const iconDiv = document.createElement('div');
      iconDiv.className = 'dsf-file-icon';
      const isImage = entry.file.type && entry.file.type.startsWith('image/');
      if (isImage) {
        const img = document.createElement('img');
        if (!this._thumbUrls.has(entry.file)) {
          this._thumbUrls.set(entry.file, URL.createObjectURL(entry.file));
        }
        img.src = this._thumbUrls.get(entry.file);
        img.alt = entry.file.name;
        iconDiv.appendChild(img);
      } else {
        const style = _getIconStyle(entry.file.name, entry.file.type);
        iconDiv.innerHTML = _makeIconSvg(style, 48);
      }
      li.appendChild(iconDiv);

      // Details
      const details = document.createElement('div');
      details.className = 'dsf-file-details';

      const name = document.createElement('span');
      name.className = 'dsf-file-name';
      name.textContent = entry.file.name;
      details.appendChild(name);

      const meta = document.createElement('span');
      meta.className = 'dsf-file-meta';
      meta.textContent = _formatSize(entry.file.size);
      details.appendChild(meta);

      // Progress bar
      if (entry.status === 'uploading' || entry.status === 'done' || entry.status === 'error') {
        const bar = document.createElement('div');
        bar.className = 'dsf-progress-bar';
        const fill = document.createElement('div');
        fill.className = 'dsf-progress-fill';
        if (entry.status === 'done') fill.classList.add('done');
        if (entry.status === 'error') fill.classList.add('error');
        fill.style.width = entry.progress + '%';
        bar.appendChild(fill);
        details.appendChild(bar);
      }

      // Status text
      if (entry.status === 'done') {
        const st = document.createElement('span');
        st.className = 'dsf-file-status done';
        st.textContent = 'Uploaded';
        details.appendChild(st);
      } else if (entry.status === 'error') {
        const st = document.createElement('span');
        st.className = 'dsf-file-status error';
        st.textContent = entry.error || 'Upload failed';
        details.appendChild(st);
      }

      li.appendChild(details);

      // Remove button (only for pending files)
      if (entry.status === 'pending') {
        const removeBtn = document.createElement('button');
        removeBtn.className = 'dsf-file-remove';
        removeBtn.innerHTML = '&times;';
        removeBtn.title = 'Remove';
        removeBtn.addEventListener('click', () => {
          this._files.splice(i, 1);
          this._renderFileList();
        });
        li.appendChild(removeBtn);
      }

      list.appendChild(li);
    }

    if (pending.length > 0 && !this._uploading) {
      actions.style.display = 'flex';
      btn.textContent = pending.length === 1 ? 'Upload' : 'Upload all';
      btn.disabled = false;
    } else if (this._uploading) {
      actions.style.display = 'flex';
      btn.textContent = 'Uploading...';
      btn.disabled = true;
    } else {
      actions.style.display = 'none';
    }
  }

  async _uploadAll() {
    this._uploading = true;
    const csrfToken = _getCsrfToken();
    const pending = this._files.filter(f => f.status === 'pending');
    this._renderFileList();

    for (const entry of pending) {
      entry.status = 'uploading';
      entry.progress = 0;
      this._renderFileList();

      try {
        // Step 1: Prepare
        entry.progress = 10;
        this._renderFileList();

        const prepResp = await fetch(`/-/files/upload/${this._source}/-/prepare`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-csrftoken': csrfToken,
          },
          body: JSON.stringify({
            filename: entry.file.name,
            content_type: entry.file.type || 'application/octet-stream',
            size: entry.file.size,
          }),
        });
        if (!prepResp.ok) {
          const errData = await prepResp.json().catch(() => null);
          throw new Error(errData?.errors?.[0] || `Prepare failed (${prepResp.status})`);
        }
        const prepData = await prepResp.json();
        entry.progress = 20;
        this._renderFileList();

        // Step 2: Upload content using XHR for progress tracking
        await new Promise((resolve, reject) => {
          const xhr = new XMLHttpRequest();
          xhr.open('POST', prepData.upload_url);
          _setXhrHeaders(xhr, prepData.upload_headers);

          xhr.upload.addEventListener('progress', (e) => {
            if (e.lengthComputable) {
              entry.progress = 20 + Math.round((e.loaded / e.total) * 60);
              this._renderFileList();
            }
          });

          xhr.addEventListener('load', () => {
            if (xhr.status >= 200 && xhr.status < 300) {
              resolve();
            } else {
              reject(new Error(`Upload failed (${xhr.status})`));
            }
          });
          xhr.addEventListener('error', () => reject(new Error('Network error')));

          const formData = new FormData();
          // Add upload_fields
          for (const [key, value] of Object.entries(prepData.upload_fields || {})) {
            formData.append(key, value);
          }
          formData.append('file', entry.file);
          xhr.send(formData);
        });

        entry.progress = 85;
        this._renderFileList();

        // Step 3: Complete
        const completeResp = await fetch(`/-/files/upload/${this._source}/-/complete`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-csrftoken': csrfToken,
          },
          body: JSON.stringify({ upload_token: prepData.upload_token }),
        });
        if (!completeResp.ok) {
          const errData = await completeResp.json().catch(() => null);
          throw new Error(errData?.errors?.[0] || `Complete failed (${completeResp.status})`);
        }
        const completeData = await completeResp.json();

        entry.status = 'done';
        entry.progress = 100;
        entry.fileId = completeData.file.id;
        this._renderFileList();

      } catch (err) {
        entry.status = 'error';
        entry.progress = 100;
        entry.error = err.message;
        this._renderFileList();
      }
    }

    this._uploading = false;
    this._renderFileList();

    // If all done, reload after a short delay to show updated file list
    const allDone = this._files.every(f => f.status === 'done');
    if (allDone && this._files.length > 0) {
      setTimeout(() => location.reload(), 800);
    }
  }
}

customElements.define('datasette-file-upload', DatasetteFileUpload);
