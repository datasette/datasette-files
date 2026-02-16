# render_cell Table Integration

*2026-02-16T04:49:46Z by Showboat 0.5.0*

This demo shows the `render_cell` table integration for datasette-files. When a table cell contains a `df-*` file ID, the plugin replaces it with a `<datasette-file>` web component that batch-fetches metadata and renders inline thumbnails for images or filename links for other file types.

## Setup

Datasette is running with datasette-files configured with a filesystem storage source. Confirm the plugin is loaded with the new hooks:

```bash
curl -s http://localhost:8151/-/plugins.json | python3 -m json.tool
```

```output
[
    {
        "name": "datasette-files",
        "static": true,
        "templates": true,
        "version": "0.1",
        "hooks": [
            "extra_js_urls",
            "register_routes",
            "render_cell",
            "skip_csrf",
            "startup"
        ]
    }
]
```

The plugin now registers `static: true` (serving the web component JS) and two new hooks: `render_cell` (detects file IDs in table cells) and `extra_js_urls` (injects the client-side script).

## Batch Metadata Endpoint

The web component fetches file metadata from a batch endpoint. This lets it load all file info on a page in a single request:

```bash
curl -s 'http://localhost:8151/-/files/batch.json?id=df-01khjcgjpynkrb2r23y1d2act1&id=df-01khjcgjvd5sypzh9w39p18shq' | python3 -m json.tool
```

```output
{
    "files": {
        "df-01khjcgjpynkrb2r23y1d2act1": {
            "id": "df-01khjcgjpynkrb2r23y1d2act1",
            "filename": "logo.png",
            "content_type": "image/png",
            "size": 180,
            "width": null,
            "height": null,
            "download_url": "/-/files/df-01khjcgjpynkrb2r23y1d2act1/download",
            "info_url": "/-/files/df-01khjcgjpynkrb2r23y1d2act1"
        },
        "df-01khjcgjvd5sypzh9w39p18shq": {
            "id": "df-01khjcgjvd5sypzh9w39p18shq",
            "filename": "spec.pdf",
            "content_type": "application/pdf",
            "size": 28,
            "width": null,
            "height": null,
            "download_url": "/-/files/df-01khjcgjvd5sypzh9w39p18shq/download",
            "info_url": "/-/files/df-01khjcgjvd5sypzh9w39p18shq"
        }
    }
}
```

Both file IDs are returned in a single response with filename, content type, size, and URLs. The web component uses `content_type` to decide whether to render a thumbnail (images) or a filename link (other types).

## Table with File References

A \`projects\` table has been created with \`logo\` and \`spec\` columns containing \`df-*\` file IDs. Viewing it as JSON shows the raw IDs:

```bash
curl -s 'http://localhost:8151/demo-render/projects.json?_shape=array' | python3 -m json.tool
```

```output
[
    {
        "id": 1,
        "name": "Acme Dashboard",
        "logo": "df-01khjcgjpynkrb2r23y1d2act1",
        "spec": "df-01khjcgjvd5sypzh9w39p18shq"
    },
    {
        "id": 2,
        "name": "Widget Factory",
        "logo": "df-01khjcgjs6mb5128x20r250b8k",
        "spec": null
    },
    {
        "id": 3,
        "name": "Data Pipeline",
        "logo": null,
        "spec": "df-01khjcgjvd5sypzh9w39p18shq"
    }
]
```

## Server-Side HTML (render_cell)

The `render_cell` hook detects `df-*` values and wraps them in a `<datasette-file>` custom element with a fallback `<a>` link. Inspecting the raw HTML of a table cell shows:

```bash
curl -s 'http://localhost:8151/demo-render/projects' | python3 -c "
import sys, html
page = sys.stdin.read()
# Find a datasette-file tag
import re
match = re.search(r'<datasette-file[^>]*>.*?</datasette-file>', page)
if match:
    print(match.group(0))
"
```

```output
<datasette-file file-id="df-01khjcgjpynkrb2r23y1d2act1"><a href="/-/files/df-01khjcgjpynkrb2r23y1d2act1">df-01khjcgjpynkrb2r23y1d2act1</a></datasette-file>
```

The fallback `<a>` link is visible even without JavaScript — clicking it navigates to the file info page. When JS loads, the web component replaces this with a rich rendering.

## Browser Rendering

In the browser, the `<datasette-file>` web component hydrates each cell. Image file IDs become inline thumbnails; other file types show as filename links with size:

```bash {image}
/tmp/table-render-cell-final.png
```

![abebe489-2026-02-16](abebe489-2026-02-16.png)

The `logo` column shows inline image thumbnails rendered from the `/-/files/{id}/download` endpoint. The `spec` column shows `spec.pdf (28 B)` as a clickable filename link. Null values render as empty cells. All file metadata was fetched in a single batch request when the page loaded.

## Static JS Module

The web component JavaScript is served as a static plugin asset:

```bash
curl -s -o /dev/null -w 'HTTP %{http_code} (%{size_download} bytes)' http://localhost:8151/-/static-plugins/datasette_files/datasette-file-cell.js
```

```output
HTTP 200 (3523 bytes)```
```

The JS module is automatically injected on table, row, and database views via the `extra_js_urls` hook. It defines the `<datasette-file>` custom element which handles batch loading and rendering.

## How It Works

1. **Server-side** (`render_cell`): Detects `df-{ulid}` values in any table cell and emits `<datasette-file file-id="..."><a href="...">...</a></datasette-file>` — the inner link works without JS
2. **Client-side** (`datasette-file-cell.js`): On page load, collects all `<datasette-file>` elements, waits 50ms for the DOM to settle, then fires a single `GET /-/files/batch.json?id=...&id=...` request
3. **Hydration**: Each element replaces its fallback content with either an inline `<img>` thumbnail (for images) or a filename link with size (for other types)

## Cleanup

```bash
kill $(lsof -ti:8151) 2>/dev/null && echo "Datasette stopped" || echo "Already stopped"
```

```output
Datasette stopped
```
