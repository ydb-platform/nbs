#!/usr/bin/env bash

EXAMPLE_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec "$EXAMPLE_DIR/run-cell2.sh" 3-start_nbs.sh "$@"
