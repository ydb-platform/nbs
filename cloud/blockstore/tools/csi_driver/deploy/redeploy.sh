#!/bin/bash

echo "=== remove driver-node ==="
kubectl -n nbs delete daemonset.apps/nbs-csi-driver-node
echo "=== remove driver-controller ==="
kubectl -n nbs delete deployment.apps/nbs-csi-driver-controller
echo "=== get all ==="
kubectl -n nbs get all

echo "=== update driver ==="
minikube ssh -- docker pull cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1

echo "=== create driver-controller ==="
kubectl -n nbs apply -f ./manifests/7-csi-deployment.yaml
echo "=== create driver-node ==="
kubectl -n nbs apply -f ./manifests/8-csi-daemonset.yaml
echo "=== get all ==="
kubectl -n nbs get all

echo "=== done ==="
