// <datasette-file file-id="df-xxx"> web component
// Batch-fetches metadata for all instances on the page in a single request.

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
      const img = document.createElement("img");
      img.src = meta.download_url;
      img.alt = meta.filename;
      img.loading = "lazy";
      img.style.cssText = "max-width:150px;max-height:100px;display:block;border-radius:3px;";
      const link = document.createElement("a");
      link.href = meta.info_url;
      link.appendChild(img);
      this.appendChild(link);
    } else {
      const link = document.createElement("a");
      link.href = meta.info_url;
      link.textContent = meta.filename;
      this.appendChild(link);

      if (meta.size != null) {
        const size = document.createElement("span");
        size.textContent = " (" + _formatSize(meta.size) + ")";
        size.style.cssText = "color:#666;font-size:0.9em;";
        this.appendChild(size);
      }
    }
  }
}

customElements.define("datasette-file", DatasetteFile);
