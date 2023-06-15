#!/usr/bin/env bash

DATA_DIR="data"
CLIENT="./blockstore-client"

# 32GiB
$CLIENT createvolume --storage-media-kind ssd --blocks-count 8388608 --disk-id vol0
