#!/usr/bin/env bash

COMMON=$(cat common_params.txt)
if [ -f ssh_params.txt ]
then
    SSH_PARAMS=$(cat ssh_params.txt)
    echo "read aux ssh params: ${SSH_PARAMS}"
else
    SSH_PARAMS=""
fi
TEST=$(cat $1)
RESULTS="$2"
NODES="$3"
mkdir -p $(dirname "$RESULTS")
mv -f "$RESULTS" "${RESULTS}.BAK"

pids=()
i=0
while read line
do
    ssh -n "$line" "$SSH_PARAMS" "${COMMON} ${TEST}" >> "${RESULTS}" && echo "finished ${line}" &
    pids[${i}]=$!
    echo "started on ${line}"
    i=$((i + 1))
done < <(cat "$NODES")

for pid in ${pids[*]}
do
    echo "waiting for $pid"
    wait $pid
done
