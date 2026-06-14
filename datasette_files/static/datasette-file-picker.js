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
  :host([mode="inline"]) {
    display: block;
  }
  dialog {
    --ink: #0f0f0f;
    --paper: #eef6ff;
    --muted: #6b6b6b;
    --rule: #d8e6f5;
    --accent: #1a56db;
    --card: #ffffff;
    background: var(--card);
    border: none;
    border-radius: var(--modal-border-radius, 0.75rem);
    box-shadow: var(--modal-shadow, 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04));
    color: var(--ink);
    font-family: system-ui, -apple-system, sans-serif;
    margin: auto;
    max-height: min(720px, calc(100vh - 32px));
    max-width: 95vw;
    overflow: hidden;
    padding: 0;
    width: min(640px, calc(100vw - 32px));
  }
  dialog:not([open]) {
    display: none;
  }
  dialog[open] {
    display: flex;
    flex-direction: column;
  }
  dialog::backdrop {
    background: var(--modal-backdrop-bg, rgba(0, 0, 0, 0.5));
    backdrop-filter: var(--modal-backdrop-blur, blur(4px));
    -webkit-backdrop-filter: var(--modal-backdrop-blur, blur(4px));
  }
  .inline-picker {
    --ink: #0f0f0f;
    --paper: #eef6ff;
    --muted: #6b6b6b;
    --rule: #d8e6f5;
    --accent: #1a56db;
    --card: #ffffff;
    background: var(--card);
    border: 1px solid var(--rule);
    border-radius: 5px;
    box-sizing: border-box;
    color: var(--ink);
    display: flex;
    flex-direction: column;
    font-family: system-ui, -apple-system, sans-serif;
    max-height: min(28rem, calc(100vh - 220px));
    min-height: 0;
    overflow: hidden;
    width: 100%;
  }
  .header {
    align-items: center;
    border-bottom: 1px solid var(--rule);
    display: flex;
    flex-shrink: 0;
    gap: 12px;
    align-items: center;
    min-width: 0;
    padding: 20px 24px 12px;
  }
  .header h3 {
    align-items: center;
    color: var(--ink);
    display: flex;
    flex-wrap: wrap;
    font-size: 1rem;
    font-weight: 600;
    gap: 0.35rem;
    margin: 0;
    min-width: 0;
  }
  .header h3 em {
    background: var(--paper);
    border: 1px solid var(--rule);
    border-radius: 4px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 0.92em;
    font-style: normal;
    font-weight: 500;
    padding: 2px 5px;
  }
  .body {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 16px 24px 20px;
  }
  .search {
    box-sizing: border-box;
    width: 100%;
    min-width: 0;
    border: 1px solid var(--rule);
    border-radius: 5px;
    padding: 8px 10px;
    color: var(--ink);
    background: #fff;
    font: inherit;
  }
  .search:focus {
    border-color: var(--accent);
    outline: 3px solid rgba(26, 86, 219, 0.12);
  }
  .results {
    display: grid;
    gap: 4px;
    list-style: none;
    margin: 12px 0 0;
    padding: 0;
  }
  .results li {
    align-items: center;
    border: 1px solid transparent;
    border-radius: 5px;
    cursor: pointer;
    display: flex;
    gap: 8px;
    min-width: 0;
    padding: 8px 10px;
  }
  .results li:hover,
  .results li:focus {
    background: #f8fafc;
    border-color: var(--rule);
    outline: 3px solid rgba(26, 86, 219, 0.12);
    outline-offset: 1px;
  }
  .results li.selected {
    background: var(--paper);
    border-color: var(--rule);
  }
  .thumb {
    border-radius: 3px;
    flex-shrink: 0;
    height: 40px;
    object-fit: cover;
    width: 52px;
  }
  .file-info {
    flex: 1;
    min-width: 0;
  }
  .filename {
    color: var(--ink);
    display: block;
    font-size: 0.9rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .meta {
    color: var(--muted);
    display: block;
    font-size: 0.78rem;
  }
  .empty {
    color: var(--muted);
    cursor: default;
    font-size: 0.9rem;
    padding: 16px 0;
    text-align: center;
  }
  .empty:hover,
  .empty:focus {
    background: transparent;
    border-color: transparent;
    outline: none;
  }
  .upload-section {
    border-top: 1px solid var(--rule);
    margin-top: 16px;
    padding-top: 14px;
  }
  .upload-section summary {
    color: var(--ink);
    cursor: pointer;
    font-size: 0.85rem;
    font-weight: 500;
    user-select: none;
  }
  .upload-row {
    display: flex;
    gap: 8px;
    align-items: center;
    margin-top: 8px;
  }
  .upload-row select {
    background: #fff;
    border: 1px solid var(--rule);
    border-radius: 5px;
    color: var(--ink);
    font: inherit;
    font-size: 0.85rem;
    padding: 7px 9px;
  }
  .upload-row input[type="file"] {
    flex: 1;
    font-size: 0.85rem;
    min-width: 0;
  }
  .modal-footer {
    align-items: center;
    background: var(--paper);
    border-top: 1px solid var(--rule);
    display: flex;
    flex-shrink: 0;
    gap: 10px;
    justify-content: flex-end;
    padding: 14px 20px;
  }
  .btn,
  .upload-btn,
  .remove-btn {
    border: none;
    border-radius: 5px;
    cursor: pointer;
    font-family: inherit;
    font-size: 0.85rem;
    font-weight: 500;
    padding: 9px 20px;
    touch-action: manipulation;
    transition: background 0.12s;
  }
  .btn-ghost,
  .remove-btn {
    background: transparent;
    border: 1px solid var(--rule);
    color: var(--muted);
  }
  .btn-ghost:hover,
  .remove-btn:hover {
    background: var(--rule);
    color: var(--ink);
  }
  .btn-primary,
  .upload-btn {
    background: var(--accent);
    color: #fff;
  }
  .btn-primary:hover,
  .upload-btn:hover {
    background: #1949b8;
  }
  .remove-btn {
    display: block;
    margin: 10px 0 0;
  }
  .error {
    background: #fff1f1;
    border-left: 4px solid #b91c1c;
    border-radius: 4px;
    color: #7f1d1d;
    font-size: 0.85em;
    margin-top: 6px;
    padding: 8px 10px;
  }
  .uploading {
    color: var(--muted);
    font-size: 0.85em;
    margin-top: 6px;
  }
  .inline-picker .header {
    background: var(--paper);
    padding: 10px 12px;
  }
  .inline-picker .header h3 {
    font-size: 0.88rem;
  }
  .inline-picker .body {
    padding: 10px 12px 12px;
  }
  .inline-picker .results {
    max-height: 14rem;
    overflow-y: auto;
  }
  .inline-picker .modal-footer {
    background: #fff;
    padding: 10px 12px;
  }
  @media (max-width: 640px) {
    .header,
    .body {
      padding-left: 16px;
      padding-right: 16px;
    }
    .upload-row {
      align-items: stretch;
      flex-direction: column;
    }
    .modal-footer {
      padding: 12px 16px;
    }
    .btn {
      width: 100%;
    }
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
    const inline = this.getAttribute("mode") === "inline";
    const titleId = "datasette-file-picker-title-" + Math.random().toString(36).slice(2);

    this.shadowRoot.innerHTML = `
      <style>${PICKER_STYLES}</style>
      ${inline
        ? `<div class="inline-picker" role="group" aria-labelledby="${titleId}">`
        : `<dialog aria-labelledby="${titleId}">`}
        <div class="header">
          <h3 id="${titleId}">Select file for <em>${_escapeHtml(column)}</em></h3>
        </div>
        <div class="body">
          <input type="search" class="search" placeholder="Search files..." autofocus>
          <ul class="results" role="listbox"></ul>
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
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-ghost close-btn">Cancel</button>
        </div>
      ${inline ? `</div>` : `</dialog>`}
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
    if (this._dialog) {
      this._dialog.addEventListener("cancel", () => this._done(null));
    }
    this.shadowRoot.addEventListener("keydown", (e) => {
      if (inline && e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        this._done(null);
      }
    });

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

    if (this._dialog) {
      this._dialog.showModal();
    } else {
      requestAnimationFrame(() => this._searchInput.focus());
    }
    this._doSearch("");
    this._loadSources();
  }

  disconnectedCallback() {
    clearTimeout(this._searchTimer);
    if (!this._resolved && this._resolve) {
      this._resolved = true;
      this._resolve(null);
    }
  }

  _done(fileId) {
    if (this._resolved) return;
    this._resolved = true;
    if (this._dialog && this._dialog.open) this._dialog.close();
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
      this._renderResults(data.files, q);
    } catch (err) {
      this._resultsList.innerHTML = `<li class="empty">Search error: ${_escapeHtml(err.message)}</li>`;
    }
  }

  _renderResults(files, q = "") {
    const currentFileId = this.getAttribute("current-file-id") || "";
    this._resultsList.innerHTML = "";
    if (files.length === 0) {
      if (q) {
        this._resultsList.innerHTML = '<li class="empty">No files found</li>';
      }
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
