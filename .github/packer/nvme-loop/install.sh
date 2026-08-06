#!/bin/bash

set -eu

pushd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null

mkdir -p /etc/nvme-loop

jq '.user_name = env.USER_NAME' ./config.json > /etc/nvme-loop/config.json

cp ./nvme-loop-setup.sh /usr/local/bin/nvme-loop-setup.sh
cp ./nvme-loop-cleanup.sh /usr/local/bin/nvme-loop-cleanup.sh

cp ./nvme-loop.service /etc/systemd/system/nvme-loop.service

systemctl daemon-reload
systemctl enable nvme-loop
systemctl start nvme-loop || systemctl status nvme-loop

popd > /dev/null
