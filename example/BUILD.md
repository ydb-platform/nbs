# Building NBS from sources

From this repository you can build nbsd, diskagentd, blockstore-client amd blockstore-nbd executables.

## Build Requirements

Only x86_64 architecture is currently supported.
nbsd, diskagentd, blockstore-client amd blockstore-nbd can be built for Ubuntu 18.04, 20.04 and 22.04. Other Linux distributions are likely to work, but additional effort may be needed.

## Prerequisites

Below is a list of packages that need to be installed before building NBS. [How to Build](#how-to-build) section contains step by step instructions to obtain these packages.

 - cmake 3.22+
 - clang-14
 - lld-14
 - git 2.20+
 - python3.8
 - pip3
 - antlr3
 - libaio-dev
 - libidn11-dev
 - ninja 1.10+

We run multiple clang instances in parallel to speed up the process by default. Each instance of clang may use up to 1GB of RAM, and linking the binary may use up to 16GB of RAM, please make sure your build host has enough resources.

## Runtime Requirements
 The following packages are required to run nbsd server:

 - libidn11
 - libaio

# How to Build

## Install dependencies

```bash
sudo apt-get -y install git cmake python3-pip ninja-build antlr3 m4 clang-14 lld-14 libidn11-dev libaio1 libaio-dev llvm-14
sudo pip3 install conan==1.59 grpcio-tools==1.57.0

```

## Create the work directory.
> :warning: Please make sure you have at least 80Gb of free space. We also recommend placing this directory on SSD to reduce build times.

```bash
mkdir ~/nbswork && cd ~/nbswork
```

## Clone the nbs repository.

```bash
git clone https://github.com/ydb-platform/nbs.git
cd nbs
```

## Build - cmake + ninja
```bash
mkdir ../../build && cd ../../build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=../blockstore/nbs/clang.toolchain ../blockstore/nbs
ninja -j 8
```

## Build - ya make

### Build nbsd

To build nbsd run:
```bash
./ya make cloud/blockstore/apps/server
```

A nbsd binary can be found at:
```
cloud/blockstore/apps/server/nbsd
```

### Build diskagentd

To build diskagentd run:
```bash
./ya make cloud/blockstore/apps/disk_agent
```

A diskagentd binary can be found at:
```
cloud/blockstore/apps/disk_agent/diskagentd
```

### Build blockstore-client

To build blockstore-client run:
```bash
./ya make cloud/blockstore/apps/client
```

A blockstore-client binary can be found at:
```
cloud/blockstore/apps/client/blockstore-client
```

### Build blockstore-nbd

To build blockstore-nbd run:
```bash
./ya make cloud/blockstore/tools/nbd
```

A blockstore-nbd binary can be found at:
```
cloud/blockstore/tools/nbd/blockstore-nbd
```

## Run tests

### Build all executable artifacts

```bash
./ya make cloud/blockstore
```

### Run unit tests

To run tests execute:
```bash
./ya make -t cloud/blockstore
```
