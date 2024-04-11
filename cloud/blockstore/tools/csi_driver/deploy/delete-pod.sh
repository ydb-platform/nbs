#!/bin/bash

MODE=$1 # fs/blk

echo "=== remove pods ==="
kubectl delete pod/my-pod-$MODE
echo "=== remove pvcs ==="
kubectl delete pvc my-pvc-$MODE
echo "=== get all ==="
kubectl get all
echo "=== done ==="
