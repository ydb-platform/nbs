#!/bin/bash

MODE=$1 # fs/blk

echo "=== create pvc ==="
kubectl -n nbs-csi-driver apply -f ./example/pvc-$MODE.yaml
echo "=== create pod ==="
kubectl -n nbs-csi-driver apply -f ./example/pod-$MODE.yaml
echo "=== get all ==="
kubectl -n nbs-csi-driver get all
echo "=== done ==="
