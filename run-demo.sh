#!/usr/bin/env bash
# One-shot demo launcher — sets DAGSTER_HOME to the checked-in `.dagster_home`
# so freshness policies + persisted run history are enabled (the default
# ephemeral home doesn't pick up dagster.yaml, and freshness policies stay
# invisible during preview without explicit config).

set -euo pipefail

cd "$(dirname "$0")"

export DAGSTER_HOME="$(pwd)/.dagster_home"

echo "DAGSTER_HOME=$DAGSTER_HOME"
echo "Booting dagster dev with 4 code locations from workspace.yaml..."
exec uv run dagster dev -w workspace.yaml
