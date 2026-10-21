#!/usr/bin/env bash
set -e

EXAMPLE_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
script=${1:?Usage: ./run-cell2.sh SCRIPT [ARGS...]}
shift

if [[ ! -f "$EXAMPLE_DIR/cell2/$script" ]]; then
    echo "Run ./0-setup.sh first" >&2
    exit 1
fi

export PERSISTENT_TMP_DIR=${CELL2_PERSISTENT_TMP_DIR:-$HOME/tmp/nbs-cell2}
export GRPC_PORT=${GRPC_PORT:-19001}
export NBS_CLIENT_PORT=${NBS_CLIENT_PORT:-19766}
export JOURNAL_PORT=${JOURNAL_PORT:-39900}
export QEMU_QMP_PORT=${QEMU_QMP_PORT:-18678}
export QEMU_SSH_PORT=${QEMU_SSH_PORT:-18679}

case "$script" in
    1-start_storage.sh)
        export MON_PORT=${MON_PORT:-18765}
        ;;
    3-start_nbs.sh)
        export DA_IC_PORT=${DA_IC_PORT:-39012}
        export DA_MON_PORT=${DA_MON_PORT:-19100}
        export IC_PORT=${IC_PORT:-39502}
        export MON_PORT=${MON_PORT:-18766}
        export SERVER_PORT=${SERVER_PORT:-19766}
        export DATA_SERVER_PORT=${DATA_SERVER_PORT:-19767}
        export SECURE_SERVER_PORT=${SECURE_SERVER_PORT:-19768}
        ;;
    disk-agent.sh)
        export IC_PORT=${IC_PORT:-39012}
        export MON_PORT=${MON_PORT:-19100}
        ;;
esac

cd -- "$EXAMPLE_DIR/cell2"
exec "./$script" "$@"
