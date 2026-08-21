#!/usr/bin/env bash

rm -rf data
rm -rf certs
rm -rf logs

rm -f backups.*/{*.txt,*.json}

rm -f nbs/nbs-disk-agent-*.txt
rm -f nbs/nbs-disk-registry.txt
rm -f nbs/nbs-location-*.txt
