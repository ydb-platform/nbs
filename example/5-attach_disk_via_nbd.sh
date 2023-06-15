#!/usr/bin/env bash

NBD="./blockstore-nbd"
SOCK="vol0.sock"

sudo modprobe nbd
touch $SOCK
sudo $NBD --device-mode endpoint --disk-id vol0 --access-mode rw --mount-mode local --connect-device /dev/nbd0 --listen-path $SOCK >logs/nbd.log 2>&1
