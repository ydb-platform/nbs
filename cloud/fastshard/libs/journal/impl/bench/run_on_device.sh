#!/usr/bin/env bash
#
# Runs journalled-device-bench on one device of a disk agent and prints the
# latency of 4K requests for the v1 and v2 devices.
#
# Usage:
#   sudo ./run_on_device.sh <deviceUUID> [disk agent config file ...]
#
# The script asks the disk registry for the path, the offset and the size of
# the device (blockstore-client and jq must be installed). If a config file is
# given, it reads the device from this disk agent config instead (text proto
# with "FileDevices { Path: ... DeviceId: ... }").
#
# THE BENCHMARK OVERWRITES THE DEVICE. The disk agent may keep running, but
# nothing may use this device during the run: the disk agent does not know
# about the benchmark, and its own reads and writes would mix with ours.
#
# After the runs the script writes zeroes over everything the benchmark
# touched (the journal parts and the working set), so that the disk agent
# finds an empty journal there.
#
# Environment:
#   BENCH          path to journalled-device-bench (default: next to the script)
#   DEVICE_PATH    path of the device; skips the lookup
#   DEVICE_OFFSET  offset of the device on its path, in bytes (default: from
#                  the lookup, or 0 with DEVICE_PATH)
#   BLOCKSTORE_CLIENT  blockstore-client command with its options
#                  (default: "blockstore-client")
#   DURATION       seconds per run (default: 30)
#   FASTSHARD_CLIENT  path to fastshard-client (default: next to the script
#                  or in PATH)
#   FASTSHARD_ARGS options of fastshard-client; they go after the command
#                  (default: "--port $FASTSHARD_PORT")
#   FASTSHARD_PORT port of the journalled device server of the disk agent
#                  (default: 30010)
#   LOG_META_SIZE  journal metadata size
#   LOG_DATA_SIZE  journal data size
#   DATA_SIZE      data area size
#                  By default the three sizes come from
#                  "fastshard-client listdevices".
#   DEFAULT_SIZES=1  do not call fastshard-client; use 93M, 930M and 1G for
#                  the sizes that are not set
#   WORKING_SET    part of the data area that requests address (default: 256M)
#   OUT_DIR        where to keep the logs (default: ./jdbench-<uuid>-<time>)
#   YES=1          do not ask before overwriting the device
#   FORCE=1        run even if the registry says that the device belongs to
#                  another host
#   NO_CLEANUP=1   do not write zeroes over the device after the runs
#   WRITE_IODEPTHS depths of the write runs (default: "1 16")
#   READ_IODEPTHS  depths of the read runs (default: "1 16 64 256")
#   NO_FIO=1       do not run fio
#   FIO            path to fio (default: fio in PATH)
#
# The script also runs fio with the same parameters right on the block device
# (libaio, O_DIRECT, random 4K requests over the working set, one thread per
# request in flight). These rows show what the device itself can do, without
# any of our code.

set -euo pipefail

die() {
    echo "error: $*" >&2
    exit 1
}

[[ $# -ge 1 ]] || die "usage: $0 <deviceUUID> [disk agent config file ...]"

UUID=$1
shift

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH=${BENCH:-$SCRIPT_DIR/journalled-device-bench}
BLOCKSTORE_CLIENT=${BLOCKSTORE_CLIENT:-blockstore-client}
DURATION=${DURATION:-30}
FASTSHARD_PORT=${FASTSHARD_PORT:-30010}
WORKING_SET=${WORKING_SET:-256M}
OUT_DIR=${OUT_DIR:-./jdbench-$UUID-$(date +%Y%m%d-%H%M%S)}

[[ -x $BENCH ]] || die "$BENCH not found; set BENCH"
[[ $EUID -eq 0 ]] || die "run as root: the benchmark writes to a block device"

################################################################################
# Find the device

# Prints "path offset logMetaSize logDataSize" of the device with the given
# UUID. Empty fields are printed as "-".
find_in_config() {
    awk -v uuid="$UUID" '
        function value(line) {
            sub(/^[^:]*:[ \t]*/, "", line)
            gsub(/["\r]/, "", line)
            sub(/[ \t]+$/, "", line)
            return line
        }
        function reset() {
            path = "-"; id = ""; offset = 0; meta = "-"; data = "-"
        }
        depth == 0 && /^[ \t]*FileDevices[ \t]*\{/ {
            depth = 1
            reset()
            next
        }
        depth > 0 {
            if ($0 ~ /\{/) depth++
            if ($0 ~ /\}/) depth--
            if ($0 ~ /^[ \t]*Path:/)        path = value($0)
            if ($0 ~ /^[ \t]*DeviceId:/)    id = value($0)
            if ($0 ~ /^[ \t]*Offset:/)      offset = value($0)
            if ($0 ~ /^[ \t]*LogMetaSize:/) meta = value($0)
            if ($0 ~ /^[ \t]*LogDataSize:/) data = value($0)
            if (depth == 0 && id == uuid) {
                print path, offset, meta, data
                exit
            }
        }
    ' "$1"
}

# Prints "path offset size agentId" of the device with the given UUID, as the
# disk registry knows it. The sizes are in bytes.
find_in_disk_registry() {
    # shellcheck disable=SC2086
    $BLOCKSTORE_CLIENT executeaction \
        --action backupdiskregistrystate --input-bytes '{}' |
        jq -r --arg uuid "$UUID" '
            first(
                (.MemoryBackup // .LocalDBBackup // .Backup // .).Agents[]?
                | .AgentId as $agent
                | .Devices[]?
                | select(.DeviceUUID == $uuid)
                | [ .DeviceName,
                    (.PhysicalOffset // 0 | tonumber),
                    ((.UnadjustedBlockCount // .BlocksCount | tonumber)
                        * .BlockSize),
                    $agent ]
                | @tsv)'
}

CONFIG=
SOURCE=
OFFSET=0
REGISTRY_SIZE=

if [[ -n ${DEVICE_PATH:-} ]]; then
    SOURCE="DEVICE_PATH"
elif [[ $# -gt 0 ]]; then
    FOUND=

    for file in "$@"; do
        [[ -f $file ]] || die "$file is not a file"
        FOUND=$(find_in_config "$file")
        if [[ -n $FOUND ]]; then
            CONFIG=$file
            break
        fi
    done

    [[ -n $FOUND ]] || die "device $UUID not found in $*"

    read -r DEVICE_PATH OFFSET CFG_META CFG_DATA <<<"$FOUND"

    [[ $DEVICE_PATH != - ]] || die "no Path for device $UUID in $CONFIG"

    [[ $CFG_META == - ]] || LOG_META_SIZE=${LOG_META_SIZE:-$CFG_META}
    [[ $CFG_DATA == - ]] || LOG_DATA_SIZE=${LOG_DATA_SIZE:-$CFG_DATA}

    SOURCE="config $CONFIG"
else
    command -v jq >/dev/null || die "jq not found; install it, pass a disk" \
        "agent config file as the second argument or set DEVICE_PATH"
    command -v "${BLOCKSTORE_CLIENT%% *}" >/dev/null ||
        die "${BLOCKSTORE_CLIENT%% *} not found; set BLOCKSTORE_CLIENT"

    FOUND=$(find_in_disk_registry) ||
        die "the request to the disk registry failed"
    [[ -n $FOUND ]] || die "device $UUID not found in the disk registry"

    IFS=$'\t' read -r DEVICE_PATH OFFSET REGISTRY_SIZE AGENT_ID <<<"$FOUND"

    #
    # The path is only right on the host of the agent that owns the device
    #

    if [[ $AGENT_ID != "$(hostname -f)" && $AGENT_ID != "$(hostname)" &&
        ${FORCE:-} != 1 ]]; then
        die "device $UUID belongs to agent $AGENT_ID, and this host is" \
            "$(hostname -f) (set FORCE=1 if this is the same host)"
    fi

    SOURCE="disk registry, agent $AGENT_ID"
fi

OFFSET=${DEVICE_OFFSET:-$OFFSET}

[[ $((OFFSET % 4096)) -eq 0 ]] || die "the offset $OFFSET of device $UUID" \
    "is not a multiple of the 4K page size"

################################################################################
# Find the layout

# Prints "logMetaSize logDataSize dataSize" of the device with the given UUID,
# as the journalled device server of the disk agent reports them, in bytes.
find_in_list_devices() {
    # The line looks like this:
    # DeviceUUID: d1 BlockSize: 4096 LogMetaSize: 1 LogDataSize: 2 DataSize: 3
    # shellcheck disable=SC2086
    "$FASTSHARD_CLIENT" listdevices $FASTSHARD_ARGS |
        awk -v uuid="$UUID" '
            $1 == "DeviceUUID:" && $2 == uuid && !found {
                for (i = 3; i < NF; i += 2) {
                    if ($i == "LogMetaSize:") meta = $(i + 1)
                    if ($i == "LogDataSize:") data = $(i + 1)
                    if ($i == "DataSize:")    size = $(i + 1)
                }
                print meta, data, size
                found = 1
            }
        '
}

SIZES_SOURCE="environment"

if [[ ${DEFAULT_SIZES:-} == 1 ]]; then
    LOG_META_SIZE=${LOG_META_SIZE:-93M}
    LOG_DATA_SIZE=${LOG_DATA_SIZE:-930M}
    DATA_SIZE=${DATA_SIZE:-1G}
    SIZES_SOURCE="defaults"
elif [[ -z ${LOG_META_SIZE:-} || -z ${LOG_DATA_SIZE:-} || -z ${DATA_SIZE:-} ]]
then
    if [[ -z ${FASTSHARD_CLIENT:-} ]]; then
        if [[ -x $SCRIPT_DIR/fastshard-client ]]; then
            FASTSHARD_CLIENT=$SCRIPT_DIR/fastshard-client
        else
            FASTSHARD_CLIENT=fastshard-client
        fi
    fi

    FASTSHARD_ARGS=${FASTSHARD_ARGS:---port $FASTSHARD_PORT}
    LIST_DEVICES="$FASTSHARD_CLIENT listdevices $FASTSHARD_ARGS"

    command -v "$FASTSHARD_CLIENT" >/dev/null ||
        die "$FASTSHARD_CLIENT not found; set FASTSHARD_CLIENT, or" \
            "set LOG_META_SIZE, LOG_DATA_SIZE and DATA_SIZE, or DEFAULT_SIZES=1"

    SIZES=$(find_in_list_devices) || die "'$LIST_DEVICES' failed; the disk" \
        "agent must run at this point. Check FASTSHARD_PORT," \
        "or set LOG_META_SIZE, LOG_DATA_SIZE and DATA_SIZE, or DEFAULT_SIZES=1"
    [[ -n $SIZES ]] || die "device $UUID is not in the output of" \
        "'$LIST_DEVICES': the disk agent does not serve it" \
        "as a journalled device. Set the sizes or DEFAULT_SIZES=1"

    read -r LD_META LD_DATA LD_SIZE <<<"$SIZES"

    #
    # A device without a journal reports zero journal sizes, and the
    # benchmark needs a journal for v2
    #

    [[ $LD_META -gt 0 && $LD_DATA -gt 0 ]] || die "device $UUID has no" \
        "journal (LogMetaSize $LD_META, LogDataSize $LD_DATA); set" \
        "LOG_META_SIZE and LOG_DATA_SIZE, or DEFAULT_SIZES=1"

    LOG_META_SIZE=${LOG_META_SIZE:-$LD_META}
    LOG_DATA_SIZE=${LOG_DATA_SIZE:-$LD_DATA}
    DATA_SIZE=${DATA_SIZE:-$LD_SIZE}
    SIZES_SOURCE="fastshard-client listdevices"
fi

REAL_PATH=$(readlink -f "$DEVICE_PATH")
[[ -b $REAL_PATH ]] || die "$DEVICE_PATH ($REAL_PATH) is not a block device"

################################################################################
# Check that the device is free and large enough

to_bytes() {
    numfmt --from=iec "${1^^}"
}

TOTAL=$(($(to_bytes "$LOG_META_SIZE") + $(to_bytes "$LOG_DATA_SIZE") +
    $(to_bytes "$DATA_SIZE")))
PATH_SIZE=$(blockdev --getsize64 "$REAL_PATH")

[[ $OFFSET -lt $PATH_SIZE ]] || die "the offset $OFFSET is beyond the end of" \
    "$REAL_PATH ($PATH_SIZE bytes)"

DEVICE_SIZE=$((PATH_SIZE - OFFSET))

#
# Several devices can share one path. Stay inside our device then
#

if [[ -n $REGISTRY_SIZE && $REGISTRY_SIZE -lt $DEVICE_SIZE ]]; then
    DEVICE_SIZE=$REGISTRY_SIZE
fi

[[ $TOTAL -le $DEVICE_SIZE ]] || die "the layout needs $TOTAL bytes, but" \
    "device $UUID on $REAL_PATH has only $DEVICE_SIZE"

find_holders() {
    for fd in /proc/[0-9]*/fd/*; do
        if [[ $(readlink "$fd" 2>/dev/null) == "$REAL_PATH" ]]; then
            pid=${fd#/proc/}
            pid=${pid%%/*}
            echo "$pid ($(cat "/proc/$pid/comm" 2>/dev/null))"
        fi
    done | sort -u
}

HOLDERS=$(find_holders)
if grep -qsE "^$REAL_PATH " /proc/mounts; then
    die "$REAL_PATH is mounted"
fi

echo "Device:    $UUID"
echo "Path:      $DEVICE_PATH -> $REAL_PATH"
echo "Offset:    $OFFSET, device size $DEVICE_SIZE bytes"
echo "Found in:  $SOURCE"
echo "Layout:    log meta $LOG_META_SIZE, log data $LOG_DATA_SIZE," \
    "data $DATA_SIZE, working set $WORKING_SET"
echo "Sizes from: $SIZES_SOURCE"
echo "Duration:  $DURATION s per run"
echo
if [[ -n $HOLDERS ]]; then
    echo "WARNING: these processes keep $REAL_PATH open:"
    echo "$HOLDERS" | sed 's/^/    /'
    echo "This is fine for a running disk agent, but only if NOTHING uses" \
        "device $UUID now."
    echo
fi

echo "The benchmark will WRITE to $REAL_PATH at offset $OFFSET:"
echo "    the journal parts: the first" \
    "$(($(to_bytes "$LOG_META_SIZE") + $(to_bytes "$LOG_DATA_SIZE"))) bytes"
echo "    the working set: the next $(to_bytes "$WORKING_SET") bytes"

if [[ ${YES:-} != 1 ]]; then
    read -r -p "Type 'yes' to continue: " answer
    [[ $answer == yes ]] || die "cancelled"
fi

################################################################################
# Run

mkdir -p "$OUT_DIR"
RESULTS=$OUT_DIR/results.tsv
: >"$RESULTS"

JOURNAL_BYTES=$(($(to_bytes "$LOG_META_SIZE") + $(to_bytes "$LOG_DATA_SIZE")))
WORKING_SET_BYTES=$(to_bytes "$WORKING_SET")

# zero <offset from the start of the device> <length>
zero() {
    dd if=/dev/zero of="$REAL_PATH" bs=1M conv=notrunc status=none \
        oflag=direct,seek_bytes iflag=count_bytes \
        seek=$((OFFSET + $1)) count="$2"
}

# Writes zeroes over everything the benchmark touched: the journal parts and
# the working set at the start of the data area. The disk agent then finds an
# empty journal and an empty device there.
cleanup() {
    trap - EXIT INT TERM

    if [[ ${NO_CLEANUP:-} == 1 ]]; then
        echo "NO_CLEANUP=1: the benchmark data stays on the device"
        return
    fi

    echo "Writing zeroes over the journal and the working set..."
    zero 0 "$JOURNAL_BYTES" || echo "error: can't zero the journal" >&2
    zero "$JOURNAL_BYTES" "$WORKING_SET_BYTES" ||
        echo "error: can't zero the working set" >&2
}

trap cleanup EXIT
trap 'exit 130' INT TERM

# run <device> <workload> <iodepth>
run() {
    local device=$1 workload=$2 iodepth=$3
    local name=$device-$workload-qd$iodepth
    local log=$OUT_DIR/$name.log

    echo ">>> $(date +%H:%M:%S) $name ($RUN_NO of $RUN_COUNT)"
    RUN_NO=$((RUN_NO + 1))

    #
    # Show the output of the benchmark as it comes, and keep it in the log
    #

    if ! "$BENCH" \
        --device "$device" \
        --workload "$workload" \
        --backing file \
        --device-path "$REAL_PATH" \
        --device-offset "$OFFSET" \
        --page-size 4096 \
        --pages 1 \
        --iodepth "$iodepth" \
        --duration "$DURATION" \
        --log-meta-size "$LOG_META_SIZE" \
        --log-data-size "$LOG_DATA_SIZE" \
        --data-size "$DATA_SIZE" \
        --working-set "$WORKING_SET" \
        --results "$OUT_DIR/$name.json" \
        2>&1 | tee "$log" | sed -u 's/^/    /'
    then
        echo "    FAILED, see $log" >&2
        printf '%s\t%s\t%s\tFAILED\n' "$device" "$workload" "$iodepth" \
            >>"$RESULTS"
        return
    fi

    # The line looks like this:
    # WriteLogRecord: 5 ok, 0 retries, 1.0 req/s, 0.1 MB/s, latency us:
    #     mean 86.3 p50 54.8 p90 60.9 p99 120.3 p99.9 5611.6 max 13450.0
    awk -v device="$device" -v workload="$workload" -v iodepth="$iodepth" '
        /^(WriteLogRecord|ReadPages): / {
            for (i = 1; i < NF; i++) {
                if ($(i + 1) == "retries,") retries = $i
                if ($(i + 1) == "req/s,")   iops = $i
                if ($i == "mean")  mean = $(i + 1)
                if ($i == "p50")   p50 = $(i + 1)
                if ($i == "p90")   p90 = $(i + 1)
                if ($i == "p99")   p99 = $(i + 1)
                if ($i == "p99.9") p999 = $(i + 1)
            }
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                device, workload, iodepth, mean, p50, p90, p99, p999,
                iops, retries
        }
    ' "$log" >>"$RESULTS"
}

# run_fio <read|write> <iodepth>
#
# Runs fio on the working set: the same pages that the v1 device and the
# flusher of the v2 device use, so the cleanup covers them.
#
# The depth is made the way the benchmark makes it: <iodepth> threads with
# one request in flight each. One fio thread with a deep queue can not keep
# 16 requests in flight on a fast device: it runs out of CPU first.
run_fio() {
    local workload=$1 iodepth=$2
    local name=fio-$workload-qd$iodepth
    local log=$OUT_DIR/$name.log
    local json=$OUT_DIR/$name.json

    [[ -n $FIO ]] || return 0

    echo ">>> $(date +%H:%M:%S) $name ($RUN_NO of $RUN_COUNT)"
    RUN_NO=$((RUN_NO + 1))

    # shellcheck disable=SC2086
    if ! "$FIO" \
        --name="$name" \
        --filename="$REAL_PATH" \
        --offset=$((OFFSET + JOURNAL_BYTES)) \
        --size="$WORKING_SET_BYTES" \
        --rw="rand$workload" \
        --bs=4096 \
        --direct=1 \
        --ioengine=libaio \
        --iodepth=1 \
        --numjobs="$iodepth" \
        --thread \
        --group_reporting \
        --time_based \
        --runtime="$DURATION" \
        $FIO_LAT_PERCENTILES \
        --percentile_list=50:90:99:99.9 \
        --output-format=json \
        --output="$json" \
        >"$log" 2>&1
    then
        echo "    FAILED, see $log" >&2
        printf 'fio\t%s\t%s\tFAILED\n' "$workload" "$iodepth" >>"$RESULTS"
        return
    fi

    #
    # Take the total latency (from submit to completion). An old fio has no
    # percentiles for it: take the completion latency then
    #

    if ! jq -r --arg workload "$workload" --arg iodepth "$iodepth" '
        def us: . / 1000 * 10 | round / 10;
        .jobs[0][$workload] as $io
        | (if ($io.lat_ns.percentile // {} | length) > 0
            then $io.lat_ns else $io.clat_ns end) as $lat
        | [ "fio", $workload, $iodepth,
            ($lat.mean | us),
            ($lat.percentile["50.000000"] | us),
            ($lat.percentile["90.000000"] | us),
            ($lat.percentile["99.000000"] | us),
            ($lat.percentile["99.900000"] | us),
            ($io.iops * 10 | round / 10),
            "-" ]
        | @tsv' "$json" >>"$RESULTS"
    then
        echo "    FAILED: can't read $json" >&2
        printf 'fio\t%s\t%s\tFAILED\n' "$workload" "$iodepth" >>"$RESULTS"
        return
    fi

    tail -n 1 "$RESULTS" | awk -F'\t' '{
        printf "    fio: %s req/s, latency us: mean %s p50 %s p90 %s p99 %s p99.9 %s\n",
            $9, $4, $5, $6, $7, $8
    }'
}

FIO=${FIO:-fio}

WRITE_IODEPTHS=${WRITE_IODEPTHS:-1 16}
READ_IODEPTHS=${READ_IODEPTHS:-1 16 64 256}

if [[ ${NO_FIO:-} == 1 ]]; then
    FIO=
elif ! command -v "$FIO" >/dev/null || ! command -v jq >/dev/null; then
    echo "WARNING: fio or jq not found, the fio runs are skipped" >&2
    FIO=
fi

#
# A write depth is 2 runs and a read depth is 3, plus one fio run for each
#

# shellcheck disable=SC2086
WRITE_DEPTH_COUNT=$(set -- $WRITE_IODEPTHS; echo $#)
# shellcheck disable=SC2086
READ_DEPTH_COUNT=$(set -- $READ_IODEPTHS; echo $#)

RUN_COUNT=$((WRITE_DEPTH_COUNT * 2 + READ_DEPTH_COUNT * 3))
if [[ -n $FIO ]]; then
    RUN_COUNT=$((RUN_COUNT + WRITE_DEPTH_COUNT + READ_DEPTH_COUNT))
fi

#
# Ask for the percentiles of the total latency if this fio knows the option
#

FIO_LAT_PERCENTILES=
if [[ -n $FIO ]] && "$FIO" --cmdhelp=lat_percentiles >/dev/null 2>&1; then
    FIO_LAT_PERCENTILES=--lat_percentiles=1
fi

RUN_NO=1

for iodepth in $WRITE_IODEPTHS; do
    run_fio write "$iodepth"
    run v1 write "$iodepth"
    run v2 write "$iodepth"
done

#
# Read after the v1 run: its prefill has written the whole working set
#

for iodepth in $READ_IODEPTHS; do
    run v1 read-data "$iodepth"
    run_fio read "$iodepth"
    run v2 read-data "$iodepth"
    run v2 read-journal "$iodepth"
done

################################################################################
# Report

echo
echo "4K requests, latency in microseconds"
echo

{
    printf 'device\tworkload\tiodepth\tmean\tp50\tp90\tp99\tp99.9\treq/s\tretries\n'
    cat "$RESULTS"
} | column -t -s $'\t'

echo
echo "Logs and JSON results: $OUT_DIR"
