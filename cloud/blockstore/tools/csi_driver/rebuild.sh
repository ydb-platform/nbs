#!/bin/bash -e

echo "=== build driver ==="
rm -f cmd/nbs-csi-driver/nbs-csi-driver
~/workspace/nbs/ya make -r ./cmd/nbs-csi-driver/

echo "=== prepare driver ==="
DRIVER_PATH=$(readlink -f "./cmd/nbs-csi-driver/nbs-csi-driver")
rm cmd/nbs-csi-driver/nbs-csi-driver
cp $DRIVER_PATH cmd/nbs-csi-driver/

echo "=== upload driver ==="
docker build -t ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1 .
docker push ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1

echo "=== done ==="
