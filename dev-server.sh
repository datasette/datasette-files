#!/bin/bash
mkdir -p dev-files
uv run --with datasette-write-ui \
  --with 'datasette-edit-schema>=0.8a5' \
  datasette . --internal internal.db --root --reload \
  --secret 1 \
  -s plugins.datasette-files.sources.dev-files.storage filesystem \
  -s plugins.datasette-files.sources.dev-files.config.root "$(pwd)/dev-files"
