# Setup VSCode

./ya ide vscode-go -P workspace/csi-driver --no-gopls-fix cloud/blockstore/tools/csi_driver

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
\# Use --driver=docker to avoid blkUtil.AttachFileDevice failed error
\# https://github.com/kubernetes/minikube/issues/8284

```bash
minikube start --driver=docker
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

# Build csi-driver
```bash
./ya make -r cloud/blockstore/tools/csi_driver

```

# Replace symlink with binary

```bash
DRIVER_PATH=$(readlink -f "./cloud/blockstore/tools/csi_driver/cmd/nbs-csi-driver/nbs-csi-driver")
rm ./cloud/blockstore/tools/csi_driver/cmd/nbs-csi-driver/nbs-csi-driver
cp ${DRIVER_PATH} ./cloud/blockstore/tools/csi_driver/cmd/nbs-csi-driver/nbs-csi-driver
```

# Build docker container with csi-driver

```bash
eval $(minikube docker-env)
docker build -t nbs-csi-driver:latest cloud/blockstore/tools/csi_driver
```

# Setup k8s cluster

```bash
cd cloud/blockstore/tools/csi_driver
kubectl apply -f ./deploy/manifests/0-ns.yaml
kubectl apply -f ./deploy/manifests/1-priorityclass.yaml
kubectl apply -f ./deploy/manifests/2-rbac.yaml
kubectl apply -f ./deploy/manifests/3-csidriver.yaml
kubectl apply -f ./deploy/manifests/4-storageclass.yaml
kubectl apply -f ./deploy/manifests/5-csi-deployment.yaml
kubectl apply -f ./deploy/manifests/6-csi-daemonset.yaml
```

# copy nbsd-lightweight and blockstore-client to minikube and run them
```bash
./ya make -r cloud/blockstore/apps/server_lightweight -DFORCE_STATIC_LINKING=yes
minikube cp cloud/blockstore/apps/server_lightweight/nbsd-lightweight /home/nbsd-lightweight
minikube cp cloud/blockstore/tools/csi_driver/deploy/server.txt /home/server.txt

./ya make -r cloud/blockstore/apps/client -DFORCE_STATIC_LINKING=yes
minikube cp cloud/blockstore/apps/client/blockstore-client /home/blockstore-client
```

```bash
minikube ssh
cd /home
sudo mkdir -p /etc/nbs-server/endpoints
sudo chmod +x nbsd-lightweight
sudo chmod +x blockstore-client
sudo ./nbsd-lightweight --service local --server-file server.txt --verbose
```

# Create test pods (1st way)
# - CSI creates pv on controller;
# - CSI mounts pv on node.

```bash
kubectl apply -f ./deploy/example/pvc-fs.yaml
kubectl apply -f ./deploy/example/pod-fs.yaml

kubectl apply -f ./deploy/example/pvc-blk.yaml
kubectl apply -f ./deploy/example/pod-blk.yaml
```

# Create test pod (2nd way)
# - CSI mounts pv on node.
#
# Compute-API has to create nbs volume before and passes disk-id to volumeHandle of pv.yaml.
# Also Compute-API can pass any arguments to volumeAttributes of pv.yaml.

```bash
minikube ssh
cd /home
./blockstore-client createvolume --storage-media-kind ssd --blocks-count 262144 --block-size 4096 --disk-id my-nbs-volume-id
```

```bash
kubectl apply -f ./deploy/example_pv/pvc.yaml
kubectl apply -f ./deploy/example_pv/pv.yaml
kubectl apply -f ./deploy/example_pv/pod.yaml
```

===========================================

# Useful commands

```bash
kubectl get nodes
kubectl -n nbs get all
kubectl -n nbs get pods
kubectl -n nbs describe node
kubectl -n nbs describe pod/nbs-csi-driver-node-xxxxx
kubectl -n nbs logs pod/nbs-csi-driver-node-xxxxx
kubectl -n nbs logs pod/nbs-csi-driver-node-xxxxx -c csi-nbs-driver
kubectl -n nbs logs pod/nbs-csi-driver-controller-xxxxx -c csi-nbs-driver
kubectl -n nbs delete deployment.apps/nbs-csi-driver-controller
kubectl get CSIDriver
kubectl get --raw "/api/v1/nodes/minikube/proxy/stats/summary"
minikube ssh
  docker ps
  docker exec -it $MYCONTAINER /bin/bash
```
