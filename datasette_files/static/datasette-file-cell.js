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

const _FILE_ICON_STYLES = {
  CSV:  { badge: '#2E7D32', bg: '#EEF7EE', stroke: '#7AB87E', fold: '#C4E0C5' },
  PDF:  { badge: '#E05050', bg: '#FDF0EE', stroke: '#D4837A', fold: '#F5C4C0' },
  JSON: { badge: '#F59E0B', bg: '#FFFBEB', stroke: '#D4A34A', fold: '#FDE68A' },
  GEOJSON: { badge: '#F59E0B', bg: '#FFFBEB', stroke: '#D4A34A', fold: '#FDE68A' },
  XLS:  { badge: '#1D6F42', bg: '#EDF5F0', stroke: '#6DA88A', fold: '#B7DAC5' },
  XLSX: { badge: '#1D6F42', bg: '#EDF5F0', stroke: '#6DA88A', fold: '#B7DAC5' },
  DOC:  { badge: '#2B579A', bg: '#EEF1F7', stroke: '#7B8FB8', fold: '#BDC9E0' },
  DOCX: { badge: '#2B579A', bg: '#EEF1F7', stroke: '#7B8FB8', fold: '#BDC9E0' },
  ZIP:  { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  GZ:   { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  TAR:  { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  BZ2:  { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  '7Z': { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  RAR:  { badge: '#7C3AED', bg: '#F3EEFF', stroke: '#A78BDB', fold: '#D4BFFA' },
  MP4:  { badge: '#DC2626', bg: '#FEF2F2', stroke: '#D48A8A', fold: '#FECACA' },
  MOV:  { badge: '#DC2626', bg: '#FEF2F2', stroke: '#D48A8A', fold: '#FECACA' },
  AVI:  { badge: '#DC2626', bg: '#FEF2F2', stroke: '#D48A8A', fold: '#FECACA' },
  MKV:  { badge: '#DC2626', bg: '#FEF2F2', stroke: '#D48A8A', fold: '#FECACA' },
  WEBM: { badge: '#DC2626', bg: '#FEF2F2', stroke: '#D48A8A', fold: '#FECACA' },
  MP3:  { badge: '#9333EA', bg: '#FAF5FF', stroke: '#B48AD8', fold: '#DDD6FE' },
  WAV:  { badge: '#9333EA', bg: '#FAF5FF', stroke: '#B48AD8', fold: '#DDD6FE' },
  OGG:  { badge: '#9333EA', bg: '#FAF5FF', stroke: '#B48AD8', fold: '#DDD6FE' },
  FLAC: { badge: '#9333EA', bg: '#FAF5FF', stroke: '#B48AD8', fold: '#DDD6FE' },
  M4A:  { badge: '#9333EA', bg: '#FAF5FF', stroke: '#B48AD8', fold: '#DDD6FE' },
};
const _TEXT_EXTS = new Set(['TXT', 'MD', 'RST']);
const _DEFAULT_STYLE = { badge: '#6B7280', bg: '#F9FAFB', stroke: '#9CA3AF', fold: '#E5E7EB' };
const _TEXT_STYLE = { badge: '#6B7280', bg: '#F3F4F6', stroke: '#9CA3AF', fold: '#D1D5DB' };

function _getFileIconStyle(filename, contentType) {
  const ext = filename.includes('.') ? filename.split('.').pop().toUpperCase() : '?';
  if (contentType === 'text/csv') return { ext: 'CSV', ..._FILE_ICON_STYLES.CSV };
  if (contentType === 'application/pdf') return { ext: 'PDF', ..._FILE_ICON_STYLES.PDF };
  if (contentType === 'application/json') return { ext, ..._FILE_ICON_STYLES.JSON };
  if (_FILE_ICON_STYLES[ext]) return { ext, ..._FILE_ICON_STYLES[ext] };
  if (_TEXT_EXTS.has(ext) || (contentType && contentType.startsWith('text/')))
    return { ext, ..._TEXT_STYLE };
  return { ext, ..._DEFAULT_STYLE };
}

function _makeFileIconSvg(style) {
  return `<svg width="60" height="40" viewBox="-2 -2 410 310" style="vertical-align:middle;margin-right:6px;">
  <rect x="4" y="4" width="400" height="300" rx="12" fill="#00000008"/>
  <path d="M0,12 Q0,0 12,0 L340,0 L400,60 L400,288 Q400,300 388,300 L12,300 Q0,300 0,288 Z" fill="${style.bg}" stroke="${style.stroke}" stroke-width="2"/>
  <path d="M340,0 L340,48 Q340,60 352,60 L400,60" fill="${style.fold}" stroke="${style.stroke}" stroke-width="2"/>
  <rect x="100" y="110" width="200" height="80" rx="10" fill="${style.badge}"/>
  <text x="200" y="150" text-anchor="middle" font-family="system-ui,sans-serif" font-size="36" font-weight="500" fill="#FFFFFF" dominant-baseline="central">${style.ext}</text>
</svg>`;
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

    const link = document.createElement("a");
    link.href = meta.info_url;

    {
      const img = document.createElement("img");
      img.src = meta.thumbnail_url;
      img.alt = meta.filename;
      img.loading = "lazy";
      img.style.cssText = "width:60px;height:40px;object-fit:contain;vertical-align:middle;border-radius:2px;margin-right:6px;";
      img.onerror = () => { img.remove(); };
      link.appendChild(img);
    }

    link.appendChild(document.createTextNode(meta.filename));
    this.appendChild(link);

    if (meta.size != null) {
      const size = document.createElement("span");
      size.textContent = "\u00a0(" + _formatSize(meta.size) + ")";
      size.style.cssText = "color:#666;font-size:0.9em;";
      this.appendChild(size);
    }

  }
}

customElements.define("datasette-file", DatasetteFile);
