#!/bin/bash

set -eu

pushd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null

apt-get install -y --no-install-recommends \
    rdma-core libibverbs1 librdmacm1 ibverbs-providers \
    ibverbs-utils rdmacm-utils

if ! find /lib/modules -name 'rdma_rxe.ko*' | grep -q .; then
    echo "rdma_rxe kernel module is not installed"
    exit 1
fi

mkdir -p /etc/soft-roce

cp ./config.json /etc/soft-roce/config.json

cp ./soft-roce-setup.sh /usr/local/bin/soft-roce-setup.sh
cp ./soft-roce-cleanup.sh /usr/local/bin/soft-roce-cleanup.sh

cp ./soft-roce.service /etc/systemd/system/soft-roce.service

systemctl daemon-reload
systemctl enable soft-roce
# the kernel running during the image build may differ from the one
# the image boots with, so a failure to start here is not fatal
systemctl start soft-roce || systemctl status soft-roce || true

popd > /dev/null
