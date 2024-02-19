#!/usr/bin/env bash

d="/root"
scripts="${d}/runner"
nbspath="$d/github/blockstore/nbs"
cwd=$(pwd)

lockfile="/var/tmp/_run_all.lock"
if { set -C; true 2>/dev/null > $lockfile; }; then
    # shellcheck disable=SC2064
    trap "rm -f $lockfile; echo 'lock file removed'" EXIT
else
    echo "lock file exists…"
    exit
fi

cd $nbspath &&
git reset --hard &&
git pull

"${nbspath}/ya" gc cache

logs_root="/var/www/build/logs"
logs_dir="${logs_root}/run_$(date +%y_%m_%d__%H)" &&
rm -rf "$logs_dir" &&
mkdir -p "$logs_dir"
find  "${logs_root}" -maxdepth 1 -mtime +7 -type d -exec rm -rf {} \;

lineArr=()
while IFS='' read -r line; do lineArr+=("$line"); done < <((grep -E -lir --include=ya.make "(PY3TEST|UNITTEST|Y_BENCHMARK|G_BENCHMARK|GO_X?TEST_SRCS)" "$nbspath/cloud"))
for line in "${lineArr[@]}"; do
    echo "run test " "$line"
    ${scripts}/run_test.sh $nbspath "$line" "$logs_dir"
done

echo "generate report"
$scripts/generate_report.py "$logs_dir" $scripts/github_report.xsl $scripts/tests_index.xsl

function clean_bin () {
    keyword=$1
    find_args=$2
    lineArr=()
    while IFS='' read -r line; do lineArr+=("$line"); done < <(grep -E -lir --include=ya.make "$keyword" "$nbspath")
    for line in "${lineArr[@]}"; do
        subdir=$(echo "$line" | awk -F $nbspath '{print $2}' | sed -r 's/(.*)\/[^/]+$/\1/')
        dir="${logs_dir}${subdir}"
        if [ -e "${dir}" ]; then
            # shellcheck disable=SC2086
            find "${dir}" $find_args -type f -exec rm -f {} \;
        fi
    done
}

echo "clean"
clean_bin "(PROGRAM)" "-mindepth 1 -maxdepth 1"
clean_bin "(PACKAGE)" "-mindepth 1"
clean_bin "(DLL)" "-mindepth 1 -maxdepth 1"
git clean -f ./

cd "$cwd" || exit
