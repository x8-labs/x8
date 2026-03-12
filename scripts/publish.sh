#!/usr/bin/env bash
set -euo pipefail

rm -rf dist

# Use uv-managed Python and ephemeral tool deps so this works across platforms.
uv run --with build python -m build
uv run --with twine python -m twine upload --repository pypi dist/*


# export TWINE_USERNAME="__token__"
# export TWINE_PASSWORD="pypi-<your-pypi-token>"