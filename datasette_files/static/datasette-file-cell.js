// <datasette-file file-id="df-xxx"> web component
// Batch-fetches metadata for all instances on the page in a single request.
// Adds edit buttons when the user has update-row permission.

import { openFilePicker } from "./datasette-file-picker.js";

const BATCH_DELAY = 50; // ms to wait for more elements before fetching
let _pendingIds = new Set();
let _batchTimer = null;
let _cache = {}; // file-id -> metadata (or Promise)
let _batchPromise = null;

function _scheduleBatch() {
  if (_batchTimer) return;
  _batchTimer = setTimeout(_runBatch, BATCH_DELAY);
}

async function _runBatch() {
  _batchTimer = null;
  const ids = [..._pendingIds];
  _pendingIds.clear();
  if (ids.length === 0) return;

  const params = new URLSearchParams();
  ids.forEach((id) => params.append("id", id));

  const resolvers = {};
  _batchPromise = new Promise((resolve) => {
    resolvers.resolve = resolve;
  });

  try {
    const resp = await fetch(`/-/files/batch.json?${params}`);
    if (!resp.ok) throw new Error(`batch.json returned ${resp.status}`);
    const data = await resp.json();
    for (const [id, meta] of Object.entries(data.files)) {
      _cache[id] = meta;
    }
    // Mark missing IDs so we don't re-fetch
    for (const id of ids) {
      if (!_cache[id]) _cache[id] = null;
    }
  } catch (err) {
    console.error("datasette-file batch fetch failed:", err);
    for (const id of ids) {
      if (!_cache[id]) _cache[id] = null;
    }
  }

  resolvers.resolve();
  _batchPromise = null;

  // Notify all waiting elements
  document.querySelectorAll("datasette-file").forEach((el) => el._onBatchComplete());

  // After first batch, enhance empty cells in file columns
  _enhanceEmptyFileCells();
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

function _getCsrfToken() {
  const match = document.cookie.match(/ds_csrftoken=([^;]+)/);
  return match ? match[1] : "";
}

function _getPkPath(element) {
  const tr = element.closest("tr");
  if (!tr) return null;
  const pkCell = tr.querySelector("td.type-pk a");
  if (!pkCell) return null;
  const href = pkCell.getAttribute("href");
  if (!href) return null;
  // href is like "/demo/projects/1" — PK is everything after /{db}/{table}/
  const parts = href.split("/");
  // parts: ["", "demo", "projects", "1"] — PK is everything from index 3
  return parts.slice(3).join("/");
}

async function _writeUpdate(database, table, pkPath, column, fileId) {
  const resp = await fetch(`/${database}/${table}/${pkPath}/-/update`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-csrftoken": _getCsrfToken(),
    },
    body: JSON.stringify({ update: { [column]: fileId } }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Update failed (${resp.status}): ${text}`);
  }
  return resp.json();
}

class DatasetteFile extends HTMLElement {
  connectedCallback() {
    this._fileId = this.getAttribute("file-id");
    if (!this._fileId) return;

    // Server-rendered fallback link is already inside — leave it until JS hydrates

    if (_cache[this._fileId]) {
      this._render(_cache[this._fileId]);
    } else if (_cache[this._fileId] === null) {
      // Already fetched, not found — leave as raw ID
    } else {
      _pendingIds.add(this._fileId);
      _scheduleBatch();
    }
  }

  _onBatchComplete() {
    if (!this._fileId) return;
    const meta = _cache[this._fileId];
    if (meta) {
      this._render(meta);
    }
  }

  _render(meta) {
    this.innerHTML = "";
    this.title = `${meta.filename} (${_formatSize(meta.size)})`;

    const isImage = meta.content_type && meta.content_type.startsWith("image/");

    if (isImage) {
      const link = document.createElement("a");
      link.href = meta.info_url;
      const img = document.createElement("img");
      img.src = meta.download_url;
      img.alt = meta.filename;
      img.loading = "lazy";
      img.style.cssText = "max-width:150px;max-height:100px;display:block;border-radius:3px;min-width:16px;min-height:16px;";
      img.onerror = () => { img.remove(); };
      link.appendChild(img);
      this.appendChild(link);

      const labelWrap = document.createElement("span");
      labelWrap.style.cssText = "display:block;font-size:0.9em;white-space:nowrap;";
      const label = document.createElement("a");
      label.href = meta.info_url;
      label.textContent = meta.filename;
      labelWrap.appendChild(label);

      if (meta.size != null) {
        const size = document.createElement("span");
        size.textContent = "\u00a0(" + _formatSize(meta.size) + ")";
        size.style.cssText = "color:#666;";
        labelWrap.appendChild(size);
      }
      this.appendChild(labelWrap);
    } else {
      const link = document.createElement("a");
      link.href = meta.info_url;
      link.textContent = meta.filename;
      this.appendChild(link);

      if (meta.size != null) {
        const size = document.createElement("span");
        size.textContent = "\u00a0(" + _formatSize(meta.size) + ")";
        size.style.cssText = "color:#666;font-size:0.9em;";
        this.appendChild(size);
      }
    }

    // Add edit button if we have column context and update permission
    this._maybeAddEditButton();
  }

  _maybeAddEditButton() {
    const ctx = window.__datasette_files;
    if (!ctx || !ctx.canUpdate) return;
    const column = this.getAttribute("data-column");
    if (!column) return;

    const btn = document.createElement("a");
    btn.href = "#";
    btn.textContent = "\u270e";
    btn.title = "Change file";
    btn.style.cssText = "margin-left:4px;text-decoration:none;color:#666;font-size:0.9em;";
    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      await this._handleEdit(column);
    });
    this.appendChild(btn);
  }

  async _handleEdit(column) {
    const ctx = window.__datasette_files;
    if (!ctx) return;

    const fileId = await openFilePicker({
      column,
      currentFileId: this._fileId,
    });
    if (fileId === null || fileId === this._fileId) return;

    const pkPath = _getPkPath(this);
    if (!pkPath) {
      console.error("datasette-file: could not determine PK path");
      return;
    }

    // "" means remove the file reference
    const writeValue = fileId === "" ? null : fileId;

    try {
      await _writeUpdate(ctx.database, ctx.table, pkPath, column, writeValue);
    } catch (err) {
      alert("Failed to update: " + err.message);
      return;
    }

    if (fileId === "") {
      // Removed — reload to show empty cell
      location.reload();
      return;
    }

    // Re-render with new file
    this.setAttribute("file-id", fileId);
    this._fileId = fileId;
    delete _cache[fileId];
    _pendingIds.add(fileId);
    _scheduleBatch();
  }
}

customElements.define("datasette-file", DatasetteFile);

// --- Empty cell enhancement ---

let _emptyEnhanced = false;

function _enhanceEmptyFileCells() {
  if (_emptyEnhanced) return;
  _emptyEnhanced = true;

  const ctx = window.__datasette_files;
  if (!ctx || !ctx.canUpdate) return;

  const table = document.querySelector("table.rows-and-columns");
  if (!table) return;

  // Find column indices that contain file cells
  const fileColIndices = new Set();
  const fileColNames = {};
  table.querySelectorAll("td datasette-file[data-column]").forEach((el) => {
    const td = el.closest("td");
    const tr = td.closest("tr");
    const idx = Array.from(tr.children).indexOf(td);
    fileColIndices.add(idx);
    fileColNames[idx] = el.getAttribute("data-column");
  });

  if (fileColIndices.size === 0) return;

  // For each empty cell in a file column, inject an attach button
  table.querySelectorAll("tbody tr").forEach((row) => {
    fileColIndices.forEach((idx) => {
      const td = row.children[idx];
      if (!td) return;
      if (td.querySelector("datasette-file")) return;
      if (td.textContent.trim() !== "" && td.textContent.trim() !== "\u00a0") return;

      const column = fileColNames[idx];
      if (!column) return;

      const btn = document.createElement("a");
      btn.href = "#";
      btn.textContent = "+";
      btn.title = "Attach file";
      btn.style.cssText = "color:#666;text-decoration:none;font-size:1.2em;";
      btn.addEventListener("click", async (e) => {
        e.preventDefault();
        const fileId = await openFilePicker({ column });
        if (!fileId) return;

        const pkPath = _getPkPath(td);
        if (!pkPath) {
          console.error("datasette-file: could not determine PK path");
          return;
        }

        try {
          await _writeUpdate(ctx.database, ctx.table, pkPath, column, fileId);
          location.reload();
        } catch (err) {
          alert("Failed to update: " + err.message);
        }
      });
      td.appendChild(btn);
    });
  });
}
