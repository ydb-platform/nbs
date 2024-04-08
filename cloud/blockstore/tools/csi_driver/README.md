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
docker pull cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1
exit
```

# Setup k8s cluster

```bash
kubectl apply -f ./deploy/manifests/0-ns.yaml
kubectl apply -f ./deploy/manifests/1-priorityclass.yaml
kubectl apply -f ./deploy/manifests/2-rbac.yaml
kubectl apply -f ./deploy/manifests/3-csidriver.yaml
kubectl apply -f ./deploy/manifests/4-storageclass.yaml
# Download docker-config.json from the lockbox under the name docker-config.json
export DOCKER_CONFIG=$(cat docker-config.json)
kubectl create secret generic nbs-puller-secret \
    --namespace nbs \
    --type=kubernetes.io/dockerconfigjson \
    --from-literal=.dockerconfigjson="$DOCKER_CONFIG"
kubectl apply -f ./deploy/manifests/5-nbs-configmap.yaml
kubectl apply -f ./deploy/manifests/6-nbs-daemonset.yaml
kubectl apply -f ./deploy/manifests/7-csi-deployment.yaml
kubectl apply -f ./deploy/manifests/8-csi-daemonset.yaml
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
kubectl apply -f ./deploy/example_pv/pvc.yaml
kubectl apply -f ./deploy/example_pv/pv.yaml
kubectl apply -f ./deploy/example_pv/pod.yaml
```

===========================================

# Update driver in docker image.

```bash
# Build docker image
ya make -r ./cmd/nbs-csi-driver
docker build -t cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1 .
docker push cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1

# Check docker image
docker pull cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1
mkdir /tmp/csi
docker run --net host -v /tmp/csi:/csi cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1

# Update docker image on the node
minikube ssh
docker pull cr.ai.nebius.cloud/crn0l5t3qnnlbpi8de6q/nbs-csi-driver:v0.1
exit
```

# Useful commands

```bash
kubectl get nodes
kubectl -n nbs get all
kubectl -n nbs get pods
kubectl -n nbs describe node
kubectl -n nbs describe pod/nbs-csi-driver-node-xxxxx
kubectl -n nbs logs     pod/nbs-csi-driver-node-xxxxx
kubectl -n nbs delete deployment.apps/nbs-csi-driver-controller
kubectl get CSIDriver
minikube ssh
  docker ps
  docker exec -it $MYCONTAINER /bin/bash
```
