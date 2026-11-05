#!/usr/bin/env bash
set -e

EXAMPLE_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd -- "$EXAMPLE_DIR"

python3 ./prepare_cells.py
./setup_cell.sh
./run-cell2.sh setup_cell.sh
