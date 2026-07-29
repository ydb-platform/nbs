#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 REPORT_DIR YA_OUT [EVLOG]" >&2
    exit 2
fi

report_dir=$1
ya_out=$2
evlog=${3:-}

if [ ! -d "$report_dir" ]; then
    echo "Trace report directory does not exist: $report_dir" >&2
    exit 2
fi
if [ ! -d "$ya_out" ]; then
    echo "Ya output directory does not exist: $ya_out" >&2
    exit 2
fi

report_dir=$(realpath -- "$report_dir")
ya_out=$(realpath -- "$ya_out")
file_list="$report_dir/trace-inputs.files"
manifest="$report_dir/trace-inputs.manifest.json"
output="$report_dir/trace-inputs.tar.gz"
temporary=

cleanup() {
    if [ -n "$temporary" ]; then
        rm -f -- "$temporary"
    fi
    for helper in "$file_list" "$manifest"; do
        if [ ! -d "$helper" ] || [ -L "$helper" ]; then
            rm -f -- "$helper"
        fi
    done
}
trap cleanup EXIT

rm -f -- "$output"

file_list_exists=0
manifest_exists=0
if [ -e "$file_list" ] || [ -L "$file_list" ]; then
    file_list_exists=1
fi
if [ -e "$manifest" ] || [ -L "$manifest" ]; then
    manifest_exists=1
fi

if [ "$file_list_exists" -eq 0 ] && [ "$manifest_exists" -eq 0 ]; then
    exit 0
fi
if [ "$file_list_exists" -ne "$manifest_exists" ]; then
    echo "Trace input file list and manifest must either both exist or both be absent" >&2
    exit 2
fi
if [ ! -f "$file_list" ] || [ -L "$file_list" ]; then
    echo "Trace input file list is unsafe: $file_list" >&2
    exit 2
fi
if [ ! -f "$manifest" ] || [ -L "$manifest" ]; then
    echo "Trace input manifest is unsafe: $manifest" >&2
    exit 2
fi

temporary=$(mktemp "$report_dir/.trace-inputs.XXXXXX.tar.gz.tmp")

tar_args=(
    --create
    "--file=$temporary"
    --gzip
    # Archive exactly the Python-selected files even if one becomes a directory.
    --no-recursion
    --null
    # Treat every NUL-delimited path literally: do not unquote it or interpret
    # a leading '-' as a tar option.
    --verbatim-files-from
    --owner=0
    --group=0
    --numeric-owner
    '--mode=u=rw,go=r'
    # Only Python-listed paths start with "./"; put those below ya-out without
    # changing the explicitly added manifest or event log.
    '--transform=s|^\./|ya-out/|'
    "--directory=$report_dir"
    --add-file=trace-inputs.manifest.json
)

if [ -n "$evlog" ] && [ -f "$evlog" ] && [ ! -L "$evlog" ]; then
    evlog=$(realpath -- "$evlog")
    tar_args+=(
        "--directory=$(dirname "$evlog")"
        "--add-file=$(basename "$evlog")"
    )
fi

tar_args+=(
    "--directory=$ya_out"
    "--files-from=$file_list"
)

tar "${tar_args[@]}"
mv "$temporary" "$output"
