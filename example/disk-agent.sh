#!/usr/bin/env bash

if [[ -z $LOCATION_FILE ]]; then
    echo "LOCATION_FILE variable is not set"
    exit 1
fi

if [[ -z $DISK_AGENT_FILE ]]; then
    echo "DISK_AGENT_FILE variable is not set"
    exit 1
fi

source ./prepare_disk-agent.sh || exit 1

start_disk-agent \
    --location-file $LOCATION_FILE \
    --disk-agent-file $DISK_AGENT_FILE
