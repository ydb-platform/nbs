# Copilot Instructions for NBS Repository

## Repository Overview

**Network Block Store (NBS) and Network File Store (NFS)** is a Yandex Database (YDB) BlobStorage-based storage platform providing:
- **Blockstore (NBS)**: Reliable thin-provisioned block devices with snapshot support, attachable via NBD, vhost-user-blk, or gRPC
- **Filestore**: POSIX-compliant scalable filesystem mountable via FUSE or virtiofs for VMs
- **Disk Manager**: Snapshot & image service providing control plane over NBS & Filestore

**Repository Size**: ~2.2GB (source code ~40K+ C++ files, 7K+ Go files, 8K+ Python files)

**Primary Languages**: C++ (majority), Go (disk_manager/tasks), Python (tests/tools)

**Build System**: `ya` (Yandex build tool) - requires network access to download build tools from Yandex servers

## Key Architecture Components

### Main Project Directories
- **`cloud/blockstore/`** - Network Block Store implementation (nbsd server, diskagentd, blockstore-client)
- **`cloud/filestore/`** - Network File Store implementation (filestore-server, filestore-vhost, filestore-client)
- **`cloud/disk_manager/`** - Snapshot/image service (Go codebase) with gRPC API
- **`cloud/tasks/`** - Go task processor over YDB (foundation for Disk Manager)
- **`cloud/storage/core/`** - Common libs used by NBS and Filestore
- **`contrib/`** - Third-party OSS libraries with original LICENSE files
- **`library/`** - Common algorithms and data structures
- **`util/`** - Basic utilities (collections, strings, smart pointers, OS wrappers)
- **`build/`** - Build system configuration files
- **`example/`** - Local debugging setup scripts and instructions

### Configuration Files (Root Level)
- **`.clang-format`** - clang-format-18 configuration (REQUIRED for C++ formatting)
- **`.clang-tidy`** - C++ linter configuration
- **`ya.conf`** - ya build system configuration
- **`.pre-commit-config.yaml`** - Pre-commit hooks (check-yaml, end-of-file-fixer, trailing-whitespace)
- **`.githooks/pre-commit`** - Git pre-commit hook checking trailing whitespace
- **`go.mod`** - Go module configuration

## Build Instructions

### Prerequisites

**System Requirements:**
- **Architecture**: x86_64 only
- **OS**: Ubuntu 18.04/20.04/22.04 (other Linux distributions may work with effort)
- **Disk Space**: At least 80GB free (recommend SSD for faster builds)
- **RAM**: Each build thread uses ~1GB; linking uses up to 16GB; adjust threads accordingly
- **Network**: Requires access to `devtools-registry.s3.yandex.net` for ya tool download

**Required Packages:**
```bash
sudo apt-get update
sudo apt-get install -y git wget gnupg lsb-release curl xz-utils \
    tzdata python3-dev python3-pip antlr3 libidn11-dev file \
    qemu-kvm qemu-utils dpkg-dev pigz pbzip2 gdb unzip \
    libiconv libidn11 libaio
```

**Python Dependencies:**
```bash
sudo pip3 install pytest pytest-timeout pytest-xdist setproctitle \
    grpcio grpcio-tools PyHamcrest tornado xmltodict pyarrow boto3 \
    psutil PyGithub cryptography protobuf packaging six pyyaml
```

**Formatter (CRITICAL):**
```bash
sudo apt-get install clang-format-18
```

### Building with ya (Primary Build System)

**IMPORTANT**: The `ya` script is a bootstrapping wrapper that downloads the actual build tool from Yandex servers. If you cannot access `devtools-registry.s3.yandex.net`, the build will fail.

#### Initial Setup
```bash
# Clone with submodules
git clone https://github.com/ydb-platform/nbs.git
cd nbs
git submodule update --init --recursive
```

#### Build All Components
```bash
# Build all NBS binaries (nbsd, diskagentd, blockstore-client, blockstore-nbd, ydbd)
./ya make cloud/blockstore/buildall -r
```

**Build Time**: Initial build can take 30-60+ minutes depending on hardware and thread count.

#### Build Specific Targets
```bash
# YDB storage backend
./ya make contrib/ydb/apps/ydbd

# NBS server (nbsd)
./ya make cloud/blockstore/apps/server

# Disk Agent (diskagentd)
./ya make cloud/blockstore/apps/disk_agent

# Client tools
./ya make cloud/blockstore/apps/client
./ya make cloud/blockstore/tools/nbd

# Filestore server
./ya make cloud/filestore/apps/server

# Filestore vhost
./ya make cloud/filestore/apps/vhost
```

**Output Binary Paths:**
- `contrib/ydb/apps/ydbd/ydbd`
- `cloud/blockstore/apps/server/nbsd`
- `cloud/blockstore/apps/disk_agent/diskagentd`
- `cloud/blockstore/apps/client/blockstore-client`
- `cloud/blockstore/tools/nbd/blockstore-nbd`

#### Common Build Options
- **`-r`** - Build in release mode
- **`-t`** - Run tests
- **`--build=<type>`** - Build type: `debug`, `relwithdebinfo` (default), `release`
- **`--sanitize=<sanitizer>`** - Enable sanitizer: `address`, `thread`, `memory`, `undefined`
- **`-j<N>`** - Use N build threads (default: number of CPUs)
- **`--keep-going`** - Continue on build errors
- **`--stat`** - Show build statistics
- **`--cache-size <size>`** - Set cache size (e.g., `512G`)

### Building with CMake (Deprecated - May Not Work)

CMake build is deprecated and may fail. Use only if ya build is unavailable.

**Additional Dependencies for CMake:**
```bash
sudo apt-get install -y cmake clang-14 lld-14 ninja-build libaio-dev
sudo pip3 install conan==1.59 grpcio-tools==1.57.0
```

**Build Steps:**
```bash
# Outside nbs directory
mkdir nbs_build
export CONAN_USER_HOME=./nbs_build
cd nbs_build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE=../nbs/clang.toolchain \
    ../nbs
ninja
```

## Testing

### Running Tests with ya

**Run All Tests:**
```bash
./ya make -t cloud/blockstore
./ya make -t cloud/filestore
./ya make -t cloud/disk_manager
```

**Test Options:**
- **`--test-size=<size>`** - Filter by size: `small`, `medium`, `large` (can combine: `small,medium`)
- **`--test-type=<type>`** - Filter by type: `unittest`, `gtest`, `py3test`, `py2test`, `pytest`, `flake8`, `black`, `go_test`, `gofmt`, `clang_tidy`
- **`--test-threads=<N>`** - Number of test threads (default: 28 in CI)
- **`--run-all-tests`** - Run all tests including slow ones
- **`--retest`** - Retry failed tests
- **`--keep-going`** - Continue on test failures
- **`--junit <file>`** - Generate JUnit XML report

**Example Commands (from CI):**
```bash
# Build and run small/medium tests
./ya make --test-size=small,medium \
    --test-type=unittest,gtest,py3test \
    --test-threads=64 --build=relwithdebinfo \
    -t cloud/blockstore/ cloud/filestore/

# Run with sanitizers (slower)
./ya make --build=release --sanitize=address \
    --test-size=small -t cloud/blockstore/
```

### Running Tests with CMake
```bash
cd nbs_build
ctest
```

## Code Formatting and Linting

### Format C++ Code (REQUIRED before commit)
```bash
# clang-format-18 uses .clang-format config automatically
clang-format-18 -i <file>.cpp
clang-format-18 -i <file>.h
```

**VSCode Setup**: Install `clangd` extension (llvm-vs-code-extensions.vscode-clangd)

### Run Clang-Tidy
```bash
# Runs as part of test suite
./ya make --test-type=clang_tidy -t cloud/blockstore/
```

### Pre-commit Checks
```bash
# Enable git hooks (checks trailing whitespace)
git config core.hooksPath .githooks

# Run pre-commit manually
pre-commit run --all-files
```

### Python Linting
```bash
# Runs as part of test suite
./ya make --test-type=flake8,black -t cloud/
```

### Go Formatting
```bash
# Runs as part of test suite
./ya make --test-type=gofmt,go_test -t cloud/disk_manager/ cloud/tasks/
```

## Continuous Integration

### GitHub Actions Workflows

**Main PR Workflow** (`.github/workflows/pr.yaml`):
- Triggers on PR to `main` (except docs, examples, `.md` files)
- Checks membership or `ok-to-test` label for external contributors
- Runs builds and tests on self-hosted runners (Nebius VMs)

**Build and Test Workflow** (`.github/workflows/build_and_test_ya.yaml`):
- Default targets: `cloud/blockstore/apps/`, `cloud/filestore/apps/`, `cloud/disk_manager/`, `cloud/tasks/`, `cloud/storage/`
- Default test types: `unittest`, `clang_tidy`, `gtest`, `py3test`, `py2test`, `pytest`, `flake8`, `black`, `go_test`, `gofmt`
- Default test sizes: `small`, `medium`, `large`
- Build presets: `relwithdebinfo` (default), `release-asan`, `release-tsan`, `release-msan`, `release-ubsan`

**PR Labels (affects CI behavior):**
- `ok-to-test` - Required for external contributors to run CI
- `large-tests` - Include large tests (default: small and medium only)
- `blockstore`, `filestore`, `disk_manager`, `tasks` - Run tests only for specific projects
- `asan`, `tsan` - Add address/thread sanitizer builds
- `recheck` - Re-trigger checks without new commit (auto-removed)
- `sleep` - Add 2-hour sleep for debugging
- `allow-downgrade` - Allow dynamic VM preset downgrade if resources unavailable
- `disable_truncate` - Don't truncate .err files (default: 1GiB limit)

**Pre-commit Workflow** (`.github/workflows/pre-commit.yaml`):
- Runs pre-commit hooks (check-yaml, end-of-file-fixer, trailing-whitespace)
- Checks range: `origin/main..HEAD`

### Test Artifacts and Logs

Test results are uploaded to S3: `https://github-actions-s3.website.nemax.nebius.cloud/ydb-platform/nbs/PR-check/<run_id>/<attempt>/<arch>/`

**Directory Structure:**
- `build_logs/` - ya make build logs
- `logs/` - Short test logs
- `test_logs/` - Detailed ya test logs
- `test_reports/` - JUnit reports and debug data
- `summary/` - ya-test.html with test results
- `test_data/` - Test data for failed tests (retained 1 week; other logs: 1 month)

## Common Patterns and Project Structure

### ya.make Files
Each directory with code typically has a `ya.make` file defining the build target:
- **`PROGRAM(...)`** - Executable binary
- **`LIBRARY(...)`** - Static library
- **`DLL(...)`** - Shared library
- **`UNITTEST(...)`** - Unit test
- **`PY3_PROGRAM(...)`** - Python 3 program
- **`GO_LIBRARY(...)`** - Go library
- **`GO_PROGRAM(...)`** - Go program

**Example:**
```
PROGRAM(nbsd)
SRCS(main.cpp)
PEERDIR(
    cloud/blockstore/libs/service
    cloud/storage/core/libs/daemon
)
END()
```

### Code Organization
- **Apps** (`apps/`) - Executable servers and clients
- **Libs** (`libs/`) - Reusable libraries
- **Tests** (`tests/`) - Integration tests
- **Tools** (`tools/`) - Utility programs
- **Config** (`config/`) - Proto configuration specs
- **Public** (`public/`) - Public gRPC API
- **Private** (`private/`) - Private gRPC API

### Protocol Buffers
Proto files are in `*/config`, `*/public`, `*/private` directories. They're auto-generated during build.

## Local Development and Debugging

### Quick Start Example (from `example/README.md`)
```bash
cd example

# Setup environment (creates data directories, generates certs)
./0-setup.sh

# Start YDB storage (in separate terminal)
./1-start_storage.sh

# Initialize storage
./2-init_storage.sh

# Start NBS server (in separate terminal)
./3-start_nbs.sh

# Optional: Start disk agent for non-replicated disks (in separate terminal)
./4-start_disk_agent.sh

# Create a disk
./5-create_disk.sh -k ssd  # Creates vol0

# Attach disk via NBD
sudo ./6-attach_disk.sh --disk-id vol0 -d /dev/nbd0

# Test disk access
sudo dd oflag=direct if=/dev/urandom of=/dev/nbd0 count=5 bs=4096
sudo dd iflag=direct if=/dev/nbd0 of=./result.bin count=5 bs=4096

# Monitor at http://localhost:8766/blockstore/service
```

**Disk Types Supported:**
- `ssd` - Replicated network disk (vol0)
- `nonreplicated` - Non-replicated disk (nbr0)
- `mirror2` - 2x mirror (mrr0)
- `mirror3` - 3x mirror (mrr1)

### VSCode Setup
```bash
# Generate workspace
./vscode_generate_workspace.sh
code nbs.code-workspace

# Enable git hooks
git config core.hooksPath .githooks
```

**Recommended settings.json:**
```json
{
    "files.trimTrailingWhitespace": true,
    "files.trimFinalNewlines": true,
    "files.insertFinalNewline": true,
    "[go]": {
        "editor.rulers": [{"column": 80, "color": "#ff0a0a"}]
    }
}
```

**For Debugging**: Enable static linkage in `~/.ya/ya.conf`:
```
[[target_platform]]
platform_name = "default-linux-x86_64"
build_type = "relwithdebinfo"

[target_platform.flags]
FORCE_STATIC_LINKING="yes"
```

## Known Issues and Workarounds

### ya Build Tool Access
**Issue**: `ya` script requires network access to Yandex servers to download build tools.
**Workaround**: Ensure network access to `devtools-registry.s3.yandex.net` or work from an environment with cached ya tools.

### Thread Sanitizer and gRPC
**Issue**: gRPC has known issues with thread sanitizer.
**Workaround**: Run with `TSAN_OPTIONS='report_atomic_races=0'`

### Memory Requirements
**Issue**: Linking may use up to 16GB RAM, causing OOM errors.
**Workaround**: Reduce build threads using `-j<N>` flag or increase system RAM/swap.

### Dynamic Link Errors
**Issue**: Runtime library path not set when running built binaries.
**Workaround**: Set `LD_LIBRARY_PATH` to include library directories (see filestore README example).

## Summary: Agent Workflow for Making Changes

1. **Always format code** with clang-format-18 for C++ changes
2. **Run pre-commit checks** before committing (`git diff --check` or pre-commit hooks)
3. **Build targets incrementally** - build specific target first to catch compile errors early:
   ```bash
   ./ya make cloud/blockstore/apps/server  # Build just nbsd
   ```
4. **Run relevant tests** for changed components:
   ```bash
   ./ya make -t --test-size=small,medium cloud/blockstore/libs/service
   ```
5. **Check all test types** if modifying core libs:
   ```bash
   ./ya make -t --test-type=unittest,gtest,clang_tidy <target>
   ```
6. **Verify changes locally** using example/ scripts if making changes to runtime behavior
7. **Trust these instructions**: Only search for additional information if instructions are incomplete or found to be incorrect

## Quick Reference

**Build entire project:** `./ya make cloud/blockstore/buildall -r`
**Run tests:** `./ya make -t cloud/blockstore/`
**Format C++:** `clang-format-18 -i file.cpp`
**Build with sanitizer:** `./ya make --sanitize=address cloud/blockstore/apps/server`
**Local setup:** `cd example && ./0-setup.sh`

For detailed documentation, see `/doc/REPOSITORY_STRUCTURE.md`
