#!/bin/bash

echo "=== remove driver-node ==="
kubectl -n nbs-csi-ns delete daemonset.apps/nbs-csi-driver-node
echo "=== remove driver-controller ==="
kubectl -n nbs-csi-ns delete deployment.apps/nbs-csi-driver-controller
echo "=== get all ==="
kubectl -n nbs-csi-ns get all

echo "=== update driver ==="
minikube ssh -- docker pull cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1

echo "=== create driver-controller ==="
kubectl -n nbs-csi-ns apply -f ./manifests/5-deployment.yaml
echo "=== create driver-node ==="
kubectl -n nbs-csi-ns apply -f ./manifests/6-daemonset.yaml
echo "=== get all ==="
kubectl -n nbs-csi-ns get all

echo "=== done ==="
