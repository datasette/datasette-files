# datasette-files

[![PyPI](https://img.shields.io/pypi/v/datasette-files.svg)](https://pypi.org/project/datasette-files/)
[![Changelog](https://img.shields.io/github/v/release/datasette/datasette-files?include_prereleases&label=changelog)](https://github.com/datasette/datasette-files/releases)
[![Tests](https://github.com/datasette/datasette-files/actions/workflows/test.yml/badge.svg)](https://github.com/datasette/datasette-files/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/datasette/datasette-files/blob/main/LICENSE)

Upload files to Datasette

## Installation

Install this plugin in the same environment as Datasette.
```bash
datasette install datasette-files
```
## Usage

Usage instructions go here.

## Development

To set up this plugin locally, first checkout the code. Then create a new virtual environment:
```bash
cd datasette-files
python3 -m venv venv
source venv/bin/activate
```
Now install the dependencies and test dependencies:
```bash
pip install -e '.[test]'
```
To run the tests:
```bash
pytest
```
