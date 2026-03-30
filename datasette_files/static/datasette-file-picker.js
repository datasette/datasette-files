// <datasette-file-picker> web component
// Modal dialog for searching and selecting files, with optional upload.
// Dispatches "file-selected" event with detail.fileId.

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

function _escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

const PICKER_STYLES = `
  :host {
    display: contents;
  }
  dialog {
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
  dialog::backdrop {
    background: rgba(0,0,0,0.4);
  }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    border-bottom: 1px solid #eee;
  }
  .header h3 {
    margin: 0;
    font-size: 1em;
  }
  .close-btn {
    background: none;
    border: none;
    font-size: 1.3em;
    cursor: pointer;
    color: #666;
    padding: 0 4px;
  }
  .body {
    padding: 12px 16px;
    overflow-y: auto;
    flex: 1;
    min-height: 200px;
  }
  .search {
    width: 100%;
    padding: 6px 10px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.95em;
    box-sizing: border-box;
  }
  .results {
    list-style: none;
    padding: 0;
    margin: 8px 0 0 0;
  }
  .results li {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 8px;
    border-radius: 4px;
    cursor: pointer;
  }
  .results li:hover,
  .results li:focus {
    background: #f0f4ff;
    outline: 2px solid #4a90d9;
    outline-offset: -2px;
  }
  .results li.selected {
    background: #e0e8ff;
  }
  .thumb {
    width: 32px;
    height: 32px;
    object-fit: cover;
    border-radius: 3px;
    flex-shrink: 0;
  }
  .file-info {
    flex: 1;
    min-width: 0;
  }
  .filename {
    display: block;
    font-size: 0.95em;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .meta {
    display: block;
    font-size: 0.8em;
    color: #666;
  }
  .empty {
    color: #999;
    font-size: 0.9em;
    padding: 12px 0;
    text-align: center;
  }
  .upload-section {
    border-top: 1px solid #eee;
    padding: 12px 16px;
  }
  .upload-section summary {
    cursor: pointer;
    font-size: 0.9em;
    color: #333;
    user-select: none;
  }
  .upload-row {
    display: flex;
    gap: 8px;
    align-items: center;
    margin-top: 8px;
  }
  .upload-row select {
    padding: 4px 6px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.9em;
  }
  .upload-row input[type="file"] {
    font-size: 0.9em;
    flex: 1;
    min-width: 0;
  }
  .upload-btn {
    padding: 4px 12px;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: #f8f8f8;
    cursor: pointer;
    font-size: 0.9em;
  }
  .upload-btn:hover {
    background: #eee;
  }
  .remove-btn {
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
  .remove-btn:hover {
    background: #fef0f0;
  }
  .error {
    color: #c00;
    font-size: 0.85em;
    margin-top: 6px;
  }
  .uploading {
    color: #666;
    font-size: 0.85em;
    margin-top: 6px;
  }
`;

class DatasetteFilePicker extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._resolved = false;
    this._resolve = null;
    this._searchTimer = null;
  }

  connectedCallback() {
    const column = this.getAttribute("column") || "";
    const currentFileId = this.getAttribute("current-file-id") || "";

    this.shadowRoot.innerHTML = `
      <style>${PICKER_STYLES}</style>
      <dialog>
        <div class="header">
          <h3>Select file for <em>${_escapeHtml(column)}</em></h3>
          <button class="close-btn" title="Close">&times;</button>
        </div>
        <div class="body">
          <input type="search" class="search" placeholder="Search files..." autofocus>
          <ul class="results" role="listbox"></ul>
        </div>
        <div class="upload-section">
          <details>
            <summary>Upload a new file</summary>
            <div class="upload-row">
              <select class="source-select"></select>
              <input type="file" class="file-input">
              <button class="upload-btn">Upload</button>
            </div>
            <div class="upload-status"></div>
          </details>
        </div>
      </dialog>
    `;

    this._dialog = this.shadowRoot.querySelector("dialog");
    this._searchInput = this.shadowRoot.querySelector(".search");
    this._resultsList = this.shadowRoot.querySelector(".results");
    this._sourceSelect = this.shadowRoot.querySelector(".source-select");
    this._fileInput = this.shadowRoot.querySelector(".file-input");
    this._uploadBtn = this.shadowRoot.querySelector(".upload-btn");
    this._uploadStatus = this.shadowRoot.querySelector(".upload-status");

    // Add remove button if there's a current file
    if (currentFileId) {
      const removeBtn = document.createElement("button");
      removeBtn.className = "remove-btn";
      removeBtn.textContent = "Remove file";
      removeBtn.addEventListener("click", () => this._done(""));
      const body = this.shadowRoot.querySelector(".body");
      body.insertBefore(removeBtn, this._resultsList);
    }

    // Close button
    this.shadowRoot.querySelector(".close-btn").addEventListener("click", () => this._done(null));
    this._dialog.addEventListener("cancel", () => this._done(null));

    // Search with debounce
    this._searchInput.addEventListener("input", () => {
      clearTimeout(this._searchTimer);
      this._searchTimer = setTimeout(() => this._doSearch(this._searchInput.value.trim()), 300);
    });

    // Arrow down from search moves focus to first result
    this._searchInput.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        const first = this._resultsList.querySelector("li[tabindex]");
        if (first) first.focus();
      }
    });

    // Upload handler
    this._uploadBtn.addEventListener("click", () => this._handleUpload());

    // Show the dialog and do initial load
    this._dialog.showModal();
    this._doSearch("");
    this._loadSources();
  }

  disconnectedCallback() {
    clearTimeout(this._searchTimer);
  }

  _done(fileId) {
    if (this._resolved) return;
    this._resolved = true;
    if (this._dialog.open) this._dialog.close();
    this.dispatchEvent(new CustomEvent("file-selected", {
      detail: { fileId },
      bubbles: true,
    }));
    if (this._resolve) this._resolve(fileId);
    this.remove();
  }

  /** Returns a Promise that resolves when a file is selected (or dialog dismissed). */
  get result() {
    if (!this._resultPromise) {
      this._resultPromise = new Promise((resolve) => {
        this._resolve = resolve;
      });
    }
    return this._resultPromise;
  }

  async _doSearch(q) {
    try {
      const url = q
        ? `/-/files/search.json?q=${encodeURIComponent(q)}`
        : `/-/files/search.json`;
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`Search failed: ${resp.status}`);
      const data = await resp.json();
      this._renderResults(data.files);
    } catch (err) {
      this._resultsList.innerHTML = `<li class="empty">Search error: ${_escapeHtml(err.message)}</li>`;
    }
  }

  _renderResults(files) {
    const currentFileId = this.getAttribute("current-file-id") || "";
    this._resultsList.innerHTML = "";
    if (files.length === 0) {
      this._resultsList.innerHTML = '<li class="empty">No files found</li>';
      return;
    }
    for (const f of files) {
      const li = document.createElement("li");
      li.tabIndex = 0;
      li.setAttribute("role", "option");
      if (f.id === currentFileId) {
        li.classList.add("selected");
        li.setAttribute("aria-selected", "true");
      }

      {
        const img = document.createElement("img");
        img.className = "thumb";
        img.src = `/-/files/${f.id}/thumbnail`;
        img.alt = f.filename;
        img.loading = "lazy";
        img.onerror = () => img.remove();
        li.appendChild(img);
      }

      const info = document.createElement("span");
      info.className = "file-info";

      const name = document.createElement("span");
      name.className = "filename";
      name.textContent = f.filename;
      info.appendChild(name);

      const meta = document.createElement("span");
      meta.className = "meta";
      const parts = [];
      if (f.size != null) parts.push(_formatSize(f.size));
      if (f.source_slug) parts.push(f.source_slug);
      meta.textContent = parts.join(" \u00b7 ");
      info.appendChild(meta);

      li.appendChild(info);

      li.addEventListener("click", () => this._done(f.id));
      li.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          this._done(f.id);
        } else if (e.key === "ArrowDown") {
          e.preventDefault();
          const next = li.nextElementSibling;
          if (next && next.tabIndex === 0) next.focus();
        } else if (e.key === "ArrowUp") {
          e.preventDefault();
          const prev = li.previousElementSibling;
          if (prev && prev.tabIndex === 0) prev.focus();
          else this._searchInput.focus();
        }
      });
      this._resultsList.appendChild(li);
    }
  }

  async _loadSources() {
    try {
      const resp = await fetch("/-/files/sources.json");
      if (!resp.ok) return;
      const data = await resp.json();
      const uploadable = data.sources.filter((s) => s.capabilities.can_upload);
      if (uploadable.length === 0) {
        this.shadowRoot.querySelector(".upload-section").style.display = "none";
        return;
      }
      this._sourceSelect.innerHTML = "";
      for (const s of uploadable) {
        const opt = document.createElement("option");
        opt.value = s.slug;
        opt.textContent = s.slug;
        this._sourceSelect.appendChild(opt);
      }
      if (uploadable.length === 1) {
        this._sourceSelect.style.display = "none";
      }
    } catch {
      this.shadowRoot.querySelector(".upload-section").style.display = "none";
    }
  }

  async _handleUpload() {
    const file = this._fileInput.files[0];
    if (!file) {
      this._uploadStatus.innerHTML = '<div class="error">Please select a file</div>';
      return;
    }
    const source = this._sourceSelect.value;
    if (!source) return;

    this._uploadStatus.innerHTML = '<div class="uploading">Uploading...</div>';
    this._uploadBtn.disabled = true;

    try {
      const csrfToken = _getCsrfToken();

      // Step 1: Prepare
      const prepResp = await fetch(`/-/files/upload/${source}/-/prepare`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-csrftoken": csrfToken,
        },
        body: JSON.stringify({
          filename: file.name,
          content_type: file.type || "application/octet-stream",
          size: file.size,
        }),
      });
      if (!prepResp.ok) {
        const errData = await prepResp.json().catch(() => null);
        throw new Error(errData?.errors?.[0] || `Prepare failed (${prepResp.status})`);
      }
      const prepData = await prepResp.json();

      // Step 2: Upload file bytes
      const formData = new FormData();
      for (const [key, value] of Object.entries(prepData.upload_fields || {})) {
        formData.append(key, value);
      }
      formData.append("file", file);

      const uploadResp = await fetch(prepData.upload_url, {
        method: "POST",
        headers: prepData.upload_headers || {},
        body: formData,
      });
      if (!uploadResp.ok) {
        throw new Error(`Upload failed (${uploadResp.status})`);
      }

      // Step 3: Complete
      const completeResp = await fetch(`/-/files/upload/${source}/-/complete`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-csrftoken": csrfToken,
        },
        body: JSON.stringify({ upload_token: prepData.upload_token }),
      });
      if (!completeResp.ok) {
        const errData = await completeResp.json().catch(() => null);
        throw new Error(errData?.errors?.[0] || `Complete failed (${completeResp.status})`);
      }
      const completeData = await completeResp.json();
      this._done(completeData.file.id);
    } catch (err) {
      this._uploadStatus.innerHTML = `<div class="error">${_escapeHtml(err.message)}</div>`;
      this._uploadBtn.disabled = false;
    }
  }
}

customElements.define("datasette-file-picker", DatasetteFilePicker);

/**
 * Open a file picker dialog.
 * Backwards-compatible wrapper — creates a <datasette-file-picker> element
 * and returns a Promise that resolves to the selected file ID.
 * @param {Object} options
 * @param {string} options.column - Column name (shown in header)
 * @param {string} [options.currentFileId] - Currently selected file ID
 * @returns {Promise<string|null>} Selected file ID, "" to remove, or null if cancelled
 */
export function openFilePicker({ column, currentFileId }) {
  const picker = document.createElement("datasette-file-picker");
  picker.setAttribute("column", column);
  if (currentFileId) {
    picker.setAttribute("current-file-id", currentFileId);
  }
  document.body.appendChild(picker);
  return picker.result;
}
