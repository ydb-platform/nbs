#!/usr/bin/env bash

nbs_path=$1
test_path=$2
logs_root=$3

subdir=$(echo "$test_path" | awk -F "$nbs_path" '{print $2}' | sed -r 's/(.*)\/[^/]+$/\1/')
logs_dir="${logs_root}/${subdir}"
result_xml="ya-make-junit-result.xml"

mkdir -p "$logs_root"
"${nbs_path}/ya" make -A -j10 --keep-going --output "$logs_root" --junit "${logs_dir}/${result_xml}" "$test_path"

find "${logs_dir}" -maxdepth 1 -mindepth 1 -type f -not -name "${result_xml}" -exec rm {} \;
find "${logs_dir}" -type f -exec chmod +r {} \;
