#!/bin/bash

echo "=== remove driver-node ==="
kubectl -n nbs delete daemonset.apps/nbs-csi-driver-node
echo "=== remove driver-controller ==="
kubectl -n nbs delete deployment.apps/nbs-csi-driver-controller
echo "=== get all ==="
kubectl -n nbs get all

echo "=== update driver ==="
docker build -t nbs-csi-driver:latest ../../

echo "=== create driver-controller ==="
kubectl -n nbs apply -f ./manifests/5-csi-deployment.yaml
echo "=== create driver-node ==="
kubectl -n nbs apply -f ./manifests/6-csi-daemonset.yaml
echo "=== get all ==="
kubectl -n nbs get all

echo "=== done ==="
