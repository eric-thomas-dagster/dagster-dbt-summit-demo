#!/bin/bash
# Delete /pyproject.toml so the template Dockerfile's middle
# `pip install .` step (which runs from / and would try to PyPI-resolve
# every dep transitively) is skipped entirely. The pyproject.toml at
# /opt/dagster/app/pyproject.toml (copied later via `COPY . /opt/dagster/app`)
# is unaffected — dagster components find it at that location for
# `get_defs_module_path_for_project`. All actual deps get installed
# via `pip install -r requirements.txt` at the end of the Dockerfile
# from the wheels we ship.
set -e
if [ -f "/pyproject.toml" ]; then
    echo "[pre-install] Removing /pyproject.toml to skip middle pip-install step."
    rm /pyproject.toml
fi
