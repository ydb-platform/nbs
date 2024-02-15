#!/bin/bash -e

echo "=== build driver ==="
rm -f cmd/nbs-csi-driver/nbs-csi-driver
~/workspace/nbs/ya make -r ./cmd/nbs-csi-driver/

echo "=== prepare driver ==="
DRIVER_PATH=$(readlink -f "./cmd/nbs-csi-driver/nbs-csi-driver")
rm cmd/nbs-csi-driver/nbs-csi-driver
cp $DRIVER_PATH cmd/nbs-csi-driver/

echo "=== upload driver ==="
docker build -t cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1 .
docker push cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1

echo "=== done ==="
