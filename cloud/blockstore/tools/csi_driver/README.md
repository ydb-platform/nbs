# Install local k8s cluster (using Minikube)

\# Install kubectl

```bash
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.29/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.29/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list
sudo apt-get update
sudo apt-get install -y kubectl
```

\# Install minikube

```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo chmod +x minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

\# Install docker

```bash
sudo apt-get update
sudo apt-get install -y docker.io
sudo usermod -aG docker $USER && newgrp docker
```

\# Start a local k8s cluster

```bash
minikube start
```

\# Check k8s cluster

```bash
minikube status
kubectl cluster-info
kubectl get nodes
```

\# Enable nbd devices
```bash
minikube ssh -- sudo modprobe nbd nbds_max=128
minikube ssh -- ls -l /dev/ | grep nbd
```
The last command should show the nbd devices, if not then try to restart minikube
```bash
minikube stop
minikube start
```

# Pull docker image with csi-driver on the node

```bash
minikube ssh
docker login ghcr.io -u $GITHUB_USER
docker pull ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1
exit
```

# Setup k8s cluster

```bash
kubectl apply -f ./deploy/controller/cluster_setup.yaml
kubectl apply -f ./deploy/controller/csi_driver.yaml
kubectl apply -f ./deploy/controller/storage.yaml
kubectl apply -f ./deploy/controller/controller.yaml
kubectl apply -f ./deploy/node/node.yaml
```

# copy nbsd and blockstore-nbd to minikube and run them
```bash
sudo ./nbsd-lightweight --service local --server-file ./server/server.txt --verbose
sudo ./blockstore-nbd --device-mode endpoint --disk-id my-disk --access-mode rw --mount-mode local --connect-device /dev/nbd0 --listen-path /tmp/nbd.sock
```

# Create test pods (1st way)
# - CSI creates pv on controller;
# - CSI mounts pv on node.

```bash
kubectl -n nbs-csi-driver apply -f ./deploy/example/pvc-fs.yaml
kubectl -n nbs-csi-driver apply -f ./deploy/example/pod-fs.yaml

kubectl -n nbs-csi-driver apply -f ./deploy/example/pvc-blk.yaml
kubectl -n nbs-csi-driver apply -f ./deploy/example/pod-blk.yaml
```

# Create test pod (2nd way)
# - CSI mounts pv on node.
#
# Compute-API has to create nbs volume before and passes disk-id to volumeHandle of pv.yaml.
# Also Compute-API can pass any arguments to volumeAttributes of pv.yaml.

```bash
kubectl -n nbs-csi-driver apply -f ./deploy/example_pv/pvc.yaml
kubectl -n nbs-csi-driver apply -f ./deploy/example_pv/pv.yaml
kubectl -n nbs-csi-driver apply -f ./deploy/example_pv/pod.yaml
```

===========================================

# Update driver in docker image.

```bash
# Build docker image
ya make -r ./cmd/nbs-csi-driver
docker login ghcr.io -u $GITHUB_USER
docker build -t ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1 .
docker push ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1

# Check docker image
docker pull ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1
mkdir /tmp/csi
docker run --net host -v /tmp/csi:/csi ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1

# Update docker image on the node
minikube ssh
docker pull ghcr.io/ydb-platform/nbs/nbs-csi-driver:v0.1
exit
```

# Useful commands

```bash
kubectl get nodes
kubectl get all -n nbs-csi-driver
kubectl get pods -n nbs-csi-driver
kubectl describe node -n nbs-csi-driver
kubectl describe pod/nbs-csi-driver-node-xxxxx -n nbs-csi-driver
kubectl logs     pod/nbs-csi-driver-node-xxxxx -n nbs-csi-driver
kubectl delete deployment.apps/nbs-csi-driver-controller -n nbs-csi-driver
kubectl get CSIDriver
minikube ssh
  docker ps
  docker exec -it $MYCONTAINER /bin/bash
```
