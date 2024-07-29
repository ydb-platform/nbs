#!/usr/bin/env bash

COMMON=$(cat common_params.txt)
TEST=$(cat $1)
RESULTS="$2"
NODES="$3"
mkdir -p $(dirname "$RESULTS")
mv -f "$RESULTS" "${RESULTS}.BAK"

pids=()
i=0
while read line
do
    ssh -n -J sb.exp.einebox.net:57333 -l astr -o StrictHostKeyChecking=no $line "${COMMON} ${TEST}" >> "${RESULTS}" && echo "finished ${line}" &
    pids[${i}]=$!
    echo "started on ${line}"
    i=$((i + 1))
done < <(cat "$NODES")

for pid in ${pids[*]}
do
    echo "waiting for $pid"
    wait $pid
done
