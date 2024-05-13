#!/bin/bash -e

TAG=$1  # e.g. "v0.15"

echo "=== build driver ==="
rm -f cmd/nbs-csi-driver/nbs-csi-driver
~/workspace/nbs/ya make -r ./cmd/nbs-csi-driver/

echo "=== prepare driver ==="
DRIVER_PATH=$(readlink -f "./cmd/nbs-csi-driver/nbs-csi-driver")
rm cmd/nbs-csi-driver/nbs-csi-driver
cp $DRIVER_PATH cmd/nbs-csi-driver/

echo "=== upload driver ==="
docker build -t nbs-csi-driver:$TAG .

docker tag nbs-csi-driver:$TAG cr.ai.nebius.cloud/crnvrq2hg2rgmbj3ebr9/nbs-csi-driver:$TAG
docker push cr.ai.nebius.cloud/crnvrq2hg2rgmbj3ebr9/nbs-csi-driver:$TAG

### upload to "cr.nemax.nebius.cloud/crn1ek47v0dag8d666jc" if needed
# ~/nebo/nobi/kubevirt/scripts/push_local_image.sh nbs-csi-driver:$TAG

echo "=== done ==="
