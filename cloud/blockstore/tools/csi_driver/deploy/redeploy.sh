#!/bin/bash

echo "=== remove driver-node ==="
kubectl -n nbs-csi-driver delete daemonset.apps/nbs-csi-driver-node
echo "=== remove driver-controller ==="
kubectl -n nbs-csi-driver delete deployment.apps/nbs-csi-driver-controller
echo "=== get all ==="
kubectl -n nbs-csi-driver get all

echo "=== update driver ==="
minikube ssh -- docker pull ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1

echo "=== create driver-controller ==="
kubectl -n nbs-csi-driver apply -f ./controller/controller.yaml
echo "=== create driver-node ==="
kubectl -n nbs-csi-driver apply -f ./node/node.yaml
echo "=== get all ==="
kubectl -n nbs-csi-driver get all

echo "=== done ==="
