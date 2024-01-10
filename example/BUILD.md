# Building NBS from sources

From this repository you can build nbsd, diskagentd, blockstore-client amd blockstore-nbd executables.

## Build Requirements

Only x86_64 architecture is currently supported.
nbsd, diskagentd, blockstore-client and blockstore-nbd can be built for Ubuntu 18.04, 20.04 and 22.04. Other Linux distributions are likely to work, but additional effort may be needed.

> :warning: Please make sure you have at least 80Gb of free space. We also recommend placing this directory on SSD to reduce build time.

> :warning: By default the build system will use as many threads as the number of CPUs to speed up the process. Each thread may use up to 1GB of RAM on compiling, and linking the binary may use up to 16GB of RAM, please make sure that your build host has enough resources to avoid OOM errors. To reduce the number of threads, use the ```-j``` flag with build command.

## Runtime Requirements
 The following packages are required to run nbsd server:

 - libiconv
 - libidn11
 - libaio

# How to Build

## Clone the nbs repository.

```bash
git clone https://github.com/ydb-platform/nbs.git
cd nbs
```

## Build with ya

### Build all

To build all nbs binaries and dependencies just run:
```bash
./ya make cloud/blockstore
```

### Build particular target

If you need to build particular target, provide corresponding path to ```./ya``` utility:

| Target | Command | Result binary path |
| ------------ |     -----      |  ---          |
| **nbsd** | ```./ya make cloud/blockstore/apps/server``` | cloud/blockstore/apps/server/nbsd    |
| **diskagentd** | ```./ya make cloud/blockstore/apps/disk_agent``` | cloud/blockstore/apps/disk_agent/diskagentd |
| **blockstore-client** | ```./ya make cloud/blockstore/apps/client``` | cloud/blockstore/apps/client/blockstore-client |
| **blockstore-nbd** | ```./ya make cloud/blockstore/tools/nbd``` | cloud/blockstore/tools/nbd/blockstore-nbd |

### Run unit tests

To build and run tests execute:
```bash
./ya make -t cloud/blockstore
```

## Build with CMake

Alternative way to build nbs using CMake (deprecated and may not work)

### Prerequirements

Below is a list of packages that need to be installed before building NBS.

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

#### Install dependencies

```bash
sudo apt-get -y install git cmake python3-pip ninja-build antlr3 m4 clang-14 lld-14 libidn11-dev libaio1 libaio-dev llvm-14
sudo pip3 install conan==1.59 grpcio-tools==1.57.0

```

### Configure

1. Make build directory (on the same level as NBS repository directory)
```bash
mkdir nbs_build
```

2. Change Conan's home folder to the build folder for better remote cache hit

```bash
export CONAN_USER_HOME=./nbs_build
```

3. Generate build configuration

```bash
cd nbs_build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=../nbs/clang.toolchain \
  ../nbs
```

### Build all

To build all binary artifacts (tools, test executables, server, etc.), run ```ninja``` without parameters:

```bash
ninja
```

### Run unit tests

To run tests execute:
```bash
ctest
```
