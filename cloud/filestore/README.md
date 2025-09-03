# Prerequisites

### 1. qemu-kvm installed:
```
apt-get install qemu-kvm
```

### 2. user is added into kvm group
```
usermod -a -G kvm $USER
```

# How to build and run

### 1. Building
To build run the following command from the repository root folder:

```bash
./ya make -r -- cloud/filestore/buildall
```

### 2. Configuring
- prepare bin directory
```bash
cd cloud/filestore/bin
./setup.sh
```

### 3. Running services

- to avoid dynamic link errors you need to set `LD_LIBRARY_PATH` to include all necessary libraries:
```bash
export REPO_ROOT=<path-to-repo-root>

export FILESTORE_APPS_PATH=cloud/filestore/buildall/cloud/filestore/apps
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$REPO_ROOT/$FILESTORE_APPS_PATH/client
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$REPO_ROOT/$FILESTORE_APPS_PATH/server
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$REPO_ROOT/$FILESTORE_APPS_PATH/vhost
```

- use initctl.sh to format and initialize kikimr service
```bash
./initctl.sh format initialize
```

- then you should create filestore and can start vhost endpoint
```bash
./initctl.sh create startendpoint
```

### 4. Accessing filestore
- After creating endpoint you can manually run qemu from the test suite in interactive mode
```bash
./run_test_qemu.sh

#Login/Password: qemu/qemupass
qemu@test> mkdir mnt
qemu@test> sudo mount -t virtiofs fs0 mnt    # creates mount point for the filestore
```

- or you can mount filestore localy
```bash
./initctl.sh mount
```

Thanks for flying NFS


# Windows virtiofs

### 1. Preparing windows

- Initial instruction is [here](https://virtio-fs.gitlab.io/howto-windows.html)

Please note that upon installing virtio drivers you should select _only_ virtiofs
or you will render vm unusable after restart

### 2. Preparing environment

- Follow steps 1..3 from [build and run](#how-to-build-and-run)
- Either make your own image or prepare existing one
```bash
ya make cloud/storage/core/tools/testing/qemu/win-image
```

### 3. Prepare and start windows
- After creating endpoint you can manually run qemu w windows
```bash
./run_win_qemu.sh
```

- To connect you can use default RDP port 3389 via Administrator/qemupass!77

If you are running qemu inside dev machine in the cloud with limited external ports you'll need:
- to proxy via any open port(22,80,443)
- to proxy ipv6 to ipv4 net

```bash
sudo socat TCP6-LISTEN:80,fork TCP4:127.0.0.1:3389
```

### 4. Accessing filestore
- By design there should be service mounting filestore starting from drive Z:

- Or you can manually run
```promt
C:\Program Files\Virtio-Win\VioFS>virtiofs.exe -D - -d -1
```

### 5. Bugs and limitations
- So far it supports only one virtiofs drive with no way to specify specific tag
- It doesn't work: NBS-3031


# initctl help
**NOTE**: you can chain commands but they are order sensitive.

```bash
./initctl.sh stop           # stop all running services
./initctl.sh kill           # same but SIGKILL
./initctl.sh format         # format kikimr pdisk

./initctl.sh start          # launches kikimr itself, kikimr storage service & vhost
./initctl.sh initialize     # same as above but initializes kikimr, required at first run and after format cmd
./initctl.sh startlocal     # launches passthrough to local fs storage service & vhost
./initctl.sh startnull      # launches dummy storage service & vhost

./initctl.sh create         # creates filestore, required for local/kikimr services at first run or after format
./initctl.sh mount          # mounts filestore via fuse driver usually at ~/nfs unless configuration changed
./initctl.sh startendpoint  # creates virtiofs endpoint sock usually at /tmp/vhost.sock
./initctl.sh stopendpoint   # stops endpoint
```

# NOTES
- due to multiple grpc issues under thread sanitizer you should run them with ```TSAN_OPTIONS='report_atomic_races=0' ya make ...```
