// datasette-file-picker.js
// Opens a <dialog> for searching and uploading files, returns selected file ID.

let _stylesInjected = false;

function _injectStyles() {
  if (_stylesInjected) return;
  _stylesInjected = true;
  const style = document.createElement("style");
  style.textContent = `
    .dsf-picker-dialog {
      border: 1px solid #ccc;
      border-radius: 8px;
      padding: 0;
      max-width: 520px;
      width: 90vw;
      max-height: 80vh;
      display: flex;
      flex-direction: column;
      font-family: inherit;
    }
    .dsf-picker-dialog::backdrop {
      background: rgba(0,0,0,0.4);
    }
    .dsf-picker-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 16px;
      border-bottom: 1px solid #eee;
    }
    .dsf-picker-header h3 {
      margin: 0;
      font-size: 1em;
    }
    .dsf-picker-close {
      background: none;
      border: none;
      font-size: 1.3em;
      cursor: pointer;
      color: #666;
      padding: 0 4px;
    }
    .dsf-picker-body {
      padding: 12px 16px;
      overflow-y: auto;
      flex: 1;
      min-height: 200px;
    }
    .dsf-picker-search {
      width: 100%;
      padding: 6px 10px;
      border: 1px solid #ccc;
      border-radius: 4px;
      font-size: 0.95em;
      box-sizing: border-box;
    }
    .dsf-picker-results {
      list-style: none;
      padding: 0;
      margin: 8px 0 0 0;
    }
    .dsf-picker-results li {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 6px 8px;
      border-radius: 4px;
      cursor: pointer;
    }
    .dsf-picker-results li:hover,
    .dsf-picker-results li:focus {
      background: #f0f4ff;
      outline: 2px solid #4a90d9;
      outline-offset: -2px;
    }
    .dsf-picker-results li.dsf-selected {
      background: #e0e8ff;
    }
    .dsf-picker-thumb {
      width: 32px;
      height: 32px;
      object-fit: cover;
      border-radius: 3px;
      flex-shrink: 0;
    }
    .dsf-picker-file-info {
      flex: 1;
      min-width: 0;
    }
    .dsf-picker-filename {
      display: block;
      font-size: 0.95em;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .dsf-picker-meta {
      display: block;
      font-size: 0.8em;
      color: #666;
    }
    .dsf-picker-empty {
      color: #999;
      font-size: 0.9em;
      padding: 12px 0;
      text-align: center;
    }
    .dsf-picker-upload-section {
      border-top: 1px solid #eee;
      padding: 12px 16px;
    }
    .dsf-picker-upload-section summary {
      cursor: pointer;
      font-size: 0.9em;
      color: #333;
      user-select: none;
    }
    .dsf-picker-upload-row {
      display: flex;
      gap: 8px;
      align-items: center;
      margin-top: 8px;
    }
    .dsf-picker-upload-row select {
      padding: 4px 6px;
      border: 1px solid #ccc;
      border-radius: 4px;
      font-size: 0.9em;
    }
    .dsf-picker-upload-row input[type="file"] {
      font-size: 0.9em;
      flex: 1;
      min-width: 0;
    }
    .dsf-picker-upload-btn {
      padding: 4px 12px;
      border: 1px solid #ccc;
      border-radius: 4px;
      background: #f8f8f8;
      cursor: pointer;
      font-size: 0.9em;
    }
    .dsf-picker-upload-btn:hover {
      background: #eee;
    }
    .dsf-picker-remove-btn {
      display: block;
      margin: 8px 0 0 0;
      padding: 6px 12px;
      border: 1px solid #c00;
      border-radius: 4px;
      background: #fff;
      color: #c00;
      cursor: pointer;
      font-size: 0.9em;
    }
    .dsf-picker-remove-btn:hover {
      background: #fef0f0;
    }
    .dsf-picker-error {
      color: #c00;
      font-size: 0.85em;
      margin-top: 6px;
    }
    .dsf-picker-uploading {
      color: #666;
      font-size: 0.85em;
      margin-top: 6px;
    }
  `;
  document.head.appendChild(style);
}

function _getCsrfToken() {
  const match = document.cookie.match(/ds_csrftoken=([^;]+)/);
  return match ? match[1] : "";
}

function _formatSize(bytes) {
  if (bytes == null) return "";
  if (bytes < 1024) return bytes + " B";
  const kb = bytes / 1024;
  if (kb < 1024) return kb.toFixed(1) + " KB";
  const mb = kb / 1024;
  if (mb < 1024) return mb.toFixed(1) + " MB";
  const gb = mb / 1024;
  return gb.toFixed(1) + " GB";
}

/**
 * Open a file picker dialog.
 * @param {Object} options
 * @param {string} options.column - Column name (shown in header)
 * @param {string} [options.currentFileId] - Currently selected file ID
 * @returns {Promise<string|null>} Selected file ID, "" to remove, or null if cancelled
 */
export function openFilePicker({ column, currentFileId }) {
  _injectStyles();

  return new Promise((resolve) => {
    const dialog = document.createElement("dialog");
    dialog.className = "dsf-picker-dialog";

    let _resolved = false;
    function done(fileId) {
      if (_resolved) return;
      _resolved = true;
      dialog.close();
      dialog.remove();
      resolve(fileId);
    }

    // Build dialog HTML
    dialog.innerHTML = `
      <div class="dsf-picker-header">
        <h3>Select file for <em>${_escapeHtml(column)}</em></h3>
        <button class="dsf-picker-close" title="Close">&times;</button>
      </div>
      <div class="dsf-picker-body">
        <input type="search" class="dsf-picker-search" placeholder="Search files..." autofocus>
        <ul class="dsf-picker-results" role="listbox"></ul>
      </div>
      <div class="dsf-picker-upload-section">
        <details>
          <summary>Upload a new file</summary>
          <div class="dsf-picker-upload-row">
            <select class="dsf-picker-source-select"></select>
            <input type="file" class="dsf-picker-file-input">
            <button class="dsf-picker-upload-btn">Upload</button>
          </div>
          <div class="dsf-picker-upload-status"></div>
        </details>
      </div>
    `;

    // Add remove button if there's a current file
    if (currentFileId) {
      const removeBtn = document.createElement("button");
      removeBtn.className = "dsf-picker-remove-btn";
      removeBtn.textContent = "Remove file";
      removeBtn.addEventListener("click", () => done(""));
      const body = dialog.querySelector(".dsf-picker-body");
      body.insertBefore(removeBtn, body.querySelector(".dsf-picker-results"));
    }

    const closeBtn = dialog.querySelector(".dsf-picker-close");
    const searchInput = dialog.querySelector(".dsf-picker-search");
    const resultsList = dialog.querySelector(".dsf-picker-results");
    const sourceSelect = dialog.querySelector(".dsf-picker-source-select");
    const fileInput = dialog.querySelector(".dsf-picker-file-input");
    const uploadBtn = dialog.querySelector(".dsf-picker-upload-btn");
    const uploadStatus = dialog.querySelector(".dsf-picker-upload-status");

    closeBtn.addEventListener("click", () => done(null));
    dialog.addEventListener("cancel", () => done(null));

    // Search with debounce
    let _searchTimer = null;
    searchInput.addEventListener("input", () => {
      clearTimeout(_searchTimer);
      _searchTimer = setTimeout(() => _doSearch(searchInput.value.trim()), 300);
    });

    // Arrow down from search moves focus to first result
    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        const first = resultsList.querySelector("li[tabindex]");
        if (first) first.focus();
      }
    });

    async function _doSearch(q) {
      try {
        const url = q
          ? `/-/files/search.json?q=${encodeURIComponent(q)}`
          : `/-/files/search.json`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`Search failed: ${resp.status}`);
        const data = await resp.json();
        _renderResults(data.files);
      } catch (err) {
        resultsList.innerHTML = `<li class="dsf-picker-empty">Search error: ${_escapeHtml(err.message)}</li>`;
      }
    }

    function _renderResults(files) {
      resultsList.innerHTML = "";
      if (files.length === 0) {
        resultsList.innerHTML = '<li class="dsf-picker-empty">No files found</li>';
        return;
      }
      for (const f of files) {
        const li = document.createElement("li");
        li.tabIndex = 0;
        li.setAttribute("role", "option");
        if (f.id === currentFileId) {
          li.classList.add("dsf-selected");
          li.setAttribute("aria-selected", "true");
        }

        const isImage =
          f.content_type && f.content_type.startsWith("image/");
        if (isImage) {
          const img = document.createElement("img");
          img.className = "dsf-picker-thumb";
          img.src = `/-/files/${f.id}/download`;
          img.alt = f.filename;
          img.loading = "lazy";
          img.onerror = () => img.remove();
          li.appendChild(img);
        }

        const info = document.createElement("span");
        info.className = "dsf-picker-file-info";

        const name = document.createElement("span");
        name.className = "dsf-picker-filename";
        name.textContent = f.filename;
        info.appendChild(name);

        const meta = document.createElement("span");
        meta.className = "dsf-picker-meta";
        const parts = [];
        if (f.size != null) parts.push(_formatSize(f.size));
        if (f.source_slug) parts.push(f.source_slug);
        meta.textContent = parts.join(" \u00b7 ");
        info.appendChild(meta);

        li.appendChild(info);

        li.addEventListener("click", () => done(f.id));
        li.addEventListener("keydown", (e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            done(f.id);
          } else if (e.key === "ArrowDown") {
            e.preventDefault();
            const next = li.nextElementSibling;
            if (next && next.tabIndex === 0) next.focus();
          } else if (e.key === "ArrowUp") {
            e.preventDefault();
            const prev = li.previousElementSibling;
            if (prev && prev.tabIndex === 0) prev.focus();
            else searchInput.focus();
          }
        });
        resultsList.appendChild(li);
      }
    }

    // Load sources for upload
    async function _loadSources() {
      try {
        const resp = await fetch("/-/files/sources.json");
        if (!resp.ok) return;
        const data = await resp.json();
        const uploadable = data.sources.filter((s) => s.capabilities.can_upload);
        if (uploadable.length === 0) {
          dialog.querySelector(".dsf-picker-upload-section").style.display =
            "none";
          return;
        }
        sourceSelect.innerHTML = "";
        for (const s of uploadable) {
          const opt = document.createElement("option");
          opt.value = s.slug;
          opt.textContent = s.slug;
          sourceSelect.appendChild(opt);
        }
        if (uploadable.length === 1) {
          sourceSelect.style.display = "none";
        }
      } catch {
        // Hide upload section on error
        dialog.querySelector(".dsf-picker-upload-section").style.display =
          "none";
      }
    }

    // Upload handler
    uploadBtn.addEventListener("click", async () => {
      const file = fileInput.files[0];
      if (!file) {
        uploadStatus.innerHTML =
          '<div class="dsf-picker-error">Please select a file</div>';
        return;
      }
      const source = sourceSelect.value;
      if (!source) return;

      uploadStatus.innerHTML =
        '<div class="dsf-picker-uploading">Uploading...</div>';
      uploadBtn.disabled = true;

      try {
        const formData = new FormData();
        formData.append("file", file);

        const resp = await fetch(`/-/files/upload/${source}`, {
          method: "POST",
          headers: { "x-csrftoken": _getCsrfToken() },
          body: formData,
        });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(`Upload failed (${resp.status}): ${text}`);
        }
        const data = await resp.json();
        done(data.file_id);
      } catch (err) {
        uploadStatus.innerHTML = `<div class="dsf-picker-error">${_escapeHtml(err.message)}</div>`;
        uploadBtn.disabled = false;
      }
    });

    document.body.appendChild(dialog);
    dialog.showModal();

    // Initial load
    _doSearch("");
    _loadSources();
  });
}

function _escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}
