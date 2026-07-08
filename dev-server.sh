#!/bin/bash
mkdir -p dev-files
uv run datasette . --internal internal.db --root --reload \
  --secret 1 \
  -s plugins.datasette-files.sources.dev-files.storage filesystem \
  -s plugins.datasette-files.sources.dev-files.config.root "$(pwd)/dev-files" \
  "$@"
