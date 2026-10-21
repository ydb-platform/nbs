#!/usr/bin/env bash

nbs_count=2
start_nbs_enabled=true
start_da_enabled=true
nbsd_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-nbs)
            start_nbs_enabled=false
            ;;
        --skip-da)
            start_da_enabled=false
            ;;
        -h | --help)
            cat <<EOF
Usage: ./3-start_nbs.sh [--one-nbs] [--skip-nbs] [--skip-da] [-- NBS_ARGS...]

Start two NBS nodes and five disk agents in the current cell.
--one-nbs   Start only the first NBS node.
--skip-nbs  Start only disk agents; NBS must already be running.
--skip-da   Start only NBS nodes.
EOF
            exit 0
            ;;
        --one-nbs)
            nbs_count=1
            ;;
        --)
            shift
            nbsd_args+=("$@")
            break
            ;;
        *)
            nbsd_args+=("$1")
            ;;
    esac
    shift
done

if [[ "$start_nbs_enabled" == false && "$start_da_enabled" == false ]]; then
    echo "Nothing to start"
    exit 0
fi

DA_IC_PORT=${DA_IC_PORT:-29012}
DA_MON_PORT=${DA_MON_PORT:-9100}
IC_PORT=${IC_PORT:-29502}
GRPC_PORT=${GRPC_PORT:-9001}
SERVER_PORT=${SERVER_PORT:-9766}
DATA_SERVER_PORT=${DATA_SERVER_PORT:-9767}
SECURE_SERVER_PORT=${SECURE_SERVER_PORT:-9768}
MON_PORT=${MON_PORT:-8766}
source ./prepare_binaries.sh || exit 1

export ASAN_SYMBOLIZER_PATH=${ASAN_SYMBOLIZER_PATH:-$("$REPO_ROOT/ya" tool llvm-symbolizer --print-path)}

start_nbs() {
    local index=$1
    local offset=$(( (index - 1) * 20 ))
    local suffix=""
    [[ "$index" == 1 ]] || suffix=2
    shift

    echo "NBS $index http://localhost:$(( MON_PORT + offset ))/blockstore/service"

    export LD_LIBRARY_PATH=$(dirname -- "$NBSD_BIN")
    exec "$NBSD_BIN" \
        --domain             Root \
        --node-broker        "localhost:$GRPC_PORT" \
        --ic-port            "$(( IC_PORT + offset ))" \
        --mon-port           "$(( MON_PORT + offset ))" \
        --server-port        "$(( SERVER_PORT + offset ))" \
        --data-server-port   "$(( DATA_SERVER_PORT + offset ))" \
        --secure-server-port "$(( SECURE_SERVER_PORT + offset ))" \
        --discovery-file     nbs/nbs-discovery.txt \
        --domains-file       nbs/nbs-domains.txt \
        --ic-file            nbs/nbs-ic.txt \
        --log-file           nbs/nbs-log.txt \
        --sys-file           nbs/nbs-sys.txt \
        --server-file        "nbs/nbs-server$suffix.txt" \
        --storage-file       "nbs/nbs-storage$suffix.txt" \
        --naming-file        nbs/nbs-names.txt \
        --diag-file          nbs/nbs-diag.txt \
        --cells-file         nbs/nbs-cells.txt \
        --features-file      nbs/nbs-features.txt \
        --auth-file          nbs/nbs-auth.txt \
        --dr-proxy-file      nbs/nbs-dr-proxy.txt \
        --rdma-file          nbs/nbs-rdma.txt \
        --service            kikimr \
        --load-configs-from-cms \
        --profile-file       "logs/profile-log$suffix.bin" \
        "$@" > "logs/nbs.$index.log" 2>&1
}

configure_disk_registry() {
    local NBS_CLIENT_PORT=$SERVER_PORT
    local ready=false
    local attempt
    echo "Waiting for NBS on port $SERVER_PORT"
    for ((attempt = 0; attempt < 30; ++attempt)); do
        check_services
        if blockstore-client ping --timeout 1 --verbose error >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 1
    done
    if [[ "$ready" == false ]]; then
        echo "NBS on port $SERVER_PORT did not become ready" >&2
        return 1
    fi

    blockstore-client ExecuteAction --action DiskRegistrySetWritableState \
        --verbose error --timeout 5 --input-bytes '{"State":true}' || return 1
    blockstore-client UpdateDiskRegistryConfig --verbose error --timeout 5 \
        --input "$PWD/nbs/nbs-disk-registry.txt" --proto
}

start_agent() {
    local index=$1
    IC_PORT=$(( DA_IC_PORT + index * 100 ))
    MON_PORT=$(( DA_MON_PORT + index ))
    source ./prepare_disk-agent.sh || exit 1
    start_disk-agent \
        --location-file "$PWD/nbs/nbs-location-$index.txt" \
        --disk-agent-file "$PWD/nbs/nbs-disk-agent-$index.txt"
}

pids=()
check_services() {
    local pid
    for pid in "${pids[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid"
            exit "$?"
        fi
    done
}

stop_services() {
    trap - EXIT INT TERM
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
}
trap stop_services EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$start_nbs_enabled" == true ]]; then
    for ((index = 1; index <= nbs_count; ++index)); do
        start_nbs "$index" "${nbsd_args[@]}" &
        pids+=("$!")
    done
fi

if [[ "$start_da_enabled" == true ]]; then
    configure_disk_registry || exit 1
    for index in 0 1 2 3 4; do
        start_agent "$index" > "logs/remote-da$index.log" 2>&1 &
        pids+=("$!")
        echo "Agent $index http://localhost:$(( DA_MON_PORT + index ))/blockstore/disk_agent"
    done
fi

# Stop all remaining services if any child exits.
check_services
status=0
wait -n "${pids[@]}" || status=$?
exit "$status"
