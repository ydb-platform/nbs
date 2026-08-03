#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 REPORT_DIR YA_OUT [EVLOG]" >&2
    exit 2
fi

report_dir=$1
ya_out=$2
evlog=${3:-}

if [ -e "$report_dir" ] && [ ! -d "$report_dir" ]; then
    echo "Trace report path is not a directory: $report_dir" >&2
    exit 2
fi
if [ ! -d "$ya_out" ]; then
    echo "Ya output directory does not exist: $ya_out" >&2
    exit 2
fi

mkdir -p -- "$report_dir"
report_dir=$(realpath -- "$report_dir")
ya_out=$(realpath -- "$ya_out")
file_list="$report_dir/trace-inputs.files"
output="$report_dir/trace-inputs.tar.gz"
temporary=

cleanup() {
    [ -z "$temporary" ] || rm -f -- "$temporary"
    if [ ! -d "$file_list" ] || [ -L "$file_list" ]; then
        rm -f -- "$file_list"
    fi
}
trap cleanup EXIT

rm -f -- "$output"
if [ -e "$file_list" ] || [ -L "$file_list" ]; then
    if [ ! -f "$file_list" ] || [ -L "$file_list" ]; then
        echo "Trace input file list is unsafe: $file_list" >&2
        exit 2
    fi
else
    # Rendering may fail before Python can emit its selected paths (for
    # example, when an optional tracing dependency is unavailable).
    (
        cd "$ya_out"
        find . -type f -name ytest.report.trace -print0
    ) > "$file_list"
fi

include_evlog=0
if [ -n "$evlog" ] && [ -f "$evlog" ] && [ ! -L "$evlog" ]; then
    include_evlog=1
fi
if [ ! -s "$file_list" ] && [ "$include_evlog" -eq 0 ]; then
    exit 0
fi

temporary=$(mktemp "$report_dir/.trace-inputs.XXXXXX.tar.gz.tmp")
tar_args=(
    --create
    "--file=$temporary"
    --gzip
    # Archive exactly the Python-selected files even if one becomes a directory.
    --no-recursion
    --null
    # Read NUL-delimited paths literally. In particular, do not unquote names
    # or interpret a leading '-' as another tar option.
    --verbatim-files-from
    --owner=0
    --group=0
    --numeric-owner
    '--mode=u=rw,go=r'
    # Python-listed paths start with "./"; store them below ya-out/.
    '--transform=s|^\./|ya-out/|'
)

if [ "$include_evlog" -eq 1 ]; then
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
