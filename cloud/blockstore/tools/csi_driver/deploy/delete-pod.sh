#!/bin/bash

MODE=$1 # fs/blk

echo "=== remove pods ==="
kubectl -n nbs-csi-driver delete pod/my-pod-$MODE
echo "=== remove pvcs ==="
kubectl -n nbs-csi-driver delete pvc my-pvc-$MODE
echo "=== get all ==="
kubectl -n nbs-csi-driver get all
echo "=== done ==="
