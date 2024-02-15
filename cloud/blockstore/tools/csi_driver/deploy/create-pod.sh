#!/bin/bash

MODE=$1 # fs/blk

echo "=== create pvc ==="
kubectl apply -f ./example/pvc-$MODE.yaml
echo "=== create pod ==="
kubectl apply -f ./example/pod-$MODE.yaml
echo "=== get all ==="
kubectl get all
echo "=== done ==="
