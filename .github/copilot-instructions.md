# Copilot Instructions for NBS Repository

## Overview
Network Block Store (NBS) and Network File Store: YDB BlobStorage-based storage platform providing block devices (NBD/vhost-blk/gRPC) and POSIX filesystem (FUSE/virtiofs). ~2.2GB repo with C++ (40K+ files), Go (disk_manager/tasks), Python (tests). Build system: `ya` (requires `devtools-registry.s3.yandex.net` access).

**Key Directories:** `cloud/blockstore/` (NBS), `cloud/filestore/` (NFS), `cloud/disk_manager/` (Go snapshot service), `cloud/storage/core/` (common libs), `contrib/` (OSS deps), `example/` (local dev scripts).

**Root Configs:** `.clang-format` (clang-format-18 REQUIRED), `.clang-tidy`, `ya.conf`, `.pre-commit-config.yaml`, `.githooks/pre-commit` (whitespace checks).

## Build & Test

### Prerequisites
**System:** x86_64, Ubuntu 18.04+, 80GB+ disk (SSD), 16GB+ RAM. **Network:** Access to `devtools-registry.s3.yandex.net` required.
```bash
# Install deps
sudo apt-get install -y git python3-pip antlr3 libidn11-dev qemu-kvm libaio clang-format-18
sudo pip3 install pytest grpcio grpcio-tools boto3 psutil PyGithub cryptography protobuf
```

### Build Commands
```bash
# Clone and build all
git clone https://github.com/ydb-platform/nbs.git && cd nbs
git submodule update --init --recursive
./ya make cloud/blockstore/buildall -r  # 30-60+ min initial build

# Build specific targets
./ya make cloud/blockstore/apps/server     # nbsd → cloud/blockstore/apps/server/nbsd
./ya make cloud/blockstore/apps/disk_agent # diskagentd
./ya make cloud/blockstore/apps/client     # blockstore-client
./ya make cloud/filestore/apps/server      # filestore-server
```

**Options:** `-r` (release), `-t` (run tests), `-j<N>` (threads), `--build=<debug|relwithdebinfo|release>`, `--sanitize=<address|thread|memory|undefined>`, `--keep-going`, `--stat`

### Testing
```bash
# Run tests for component
./ya make -t cloud/blockstore/
./ya make -t --test-size=small,medium --test-type=unittest,gtest,py3test cloud/blockstore/

# Test types: unittest, gtest, py3test, py2test, pytest, flake8, black, go_test, gofmt, clang_tidy
# Test sizes: small, medium, large
```

**Common options:** `--test-threads=<N>` (default CI: 64), `--run-all-tests`, `--retest`, `--junit <file>`

## Code Quality & CI

### Format & Lint (REQUIRED before commit)
```bash
# C++ formatting (MANDATORY)
clang-format-18 -i file.cpp file.h

# Enable git hooks (checks trailing whitespace)
git config core.hooksPath .githooks

# Pre-commit checks
pre-commit run --all-files
```

### CI Workflows
**Main PR:** `.github/workflows/pr.yaml` - Triggers on PR to `main` (skips docs/examples/md files). Requires org membership or `ok-to-test` label for external contributors.

**Build/Test:** `.github/workflows/build_and_test_ya.yaml` - Targets: `cloud/blockstore/apps/`, `cloud/filestore/apps/`, `cloud/disk_manager/`, `cloud/tasks/`, `cloud/storage/`. Default tests: `unittest,clang_tidy,gtest,py3test,py2test,pytest,flake8,black,go_test,gofmt`. Sizes: `small,medium,large`. Preset: `relwithdebinfo`.

**PR Labels:**
- `ok-to-test` - Enable CI for external contributors
- `large-tests` - Include large tests (default: small+medium only)
- `blockstore`/`filestore`/`disk_manager`/`tasks` - Test specific projects only
- `asan`/`tsan` - Add sanitizer builds
- `recheck` - Re-trigger without new commit

**Test Artifacts:** S3 at `https://github-actions-s3.website.nemax.nebius.cloud/ydb-platform/nbs/PR-check/<run_id>/...` - See `build_logs/`, `test_logs/`, `summary/ya-test.html`

## Local Development

### Quick Setup (example/ directory)
```bash
cd example
./0-setup.sh                           # Setup env, generate certs, create data dirs
./1-start_storage.sh                   # Start YDB storage (separate terminal)
./2-init_storage.sh                    # Initialize storage
./3-start_nbs.sh                       # Start NBS server (separate terminal)
./4-start_disk_agent.sh                # Optional: for non-replicated disks
./5-create_disk.sh -k ssd              # Create vol0 (ssd/nonreplicated/mirror2/mirror3)
sudo ./6-attach_disk.sh --disk-id vol0 -d /dev/nbd0
# Test: sudo dd oflag=direct if=/dev/urandom of=/dev/nbd0 count=5 bs=4096
# Monitor: http://localhost:8766/blockstore/service
```

### VSCode Setup
```bash
./vscode_generate_workspace.sh && code nbs.code-workspace
# Install clangd extension. For debugging, add to ~/.ya/ya.conf:
# [[target_platform]]
# platform_name = "default-linux-x86_64"
# build_type = "relwithdebinfo"
# [target_platform.flags]
# FORCE_STATIC_LINKING="yes"
```

## Project Structure

**ya.make files** define build targets: `PROGRAM()`, `LIBRARY()`, `UNITTEST()`, `PY3_PROGRAM()`, `GO_PROGRAM()`, etc. Each uses `PEERDIR()` for dependencies.

**Code Organization:** `apps/` (executables), `libs/` (libraries), `tests/` (integration), `tools/` (utilities), `config/` (protos), `public/` (gRPC API), `private/` (internal gRPC).

## Known Issues & Workarounds

1. **ya tool access:** Requires `devtools-registry.s3.yandex.net` - ensure network access or use cached ya tools
2. **TSAN + gRPC:** Run with `TSAN_OPTIONS='report_atomic_races=0'`
3. **OOM during linking:** Reduce threads with `-j<N>` or increase RAM/swap
4. **Runtime libs:** Set `LD_LIBRARY_PATH` for built binaries if needed
5. **CMake build:** Deprecated, may not work - use ya

## Agent Workflow

1. **Format C++ with clang-format-18** (mandatory)
2. **Run pre-commit checks** (`git diff --check` or hooks)
3. **Build incrementally:** `./ya make cloud/blockstore/apps/server` to catch errors early
4. **Test changed components:** `./ya make -t --test-size=small,medium cloud/blockstore/libs/service`
5. **Verify locally** using `example/` scripts for runtime changes
6. **Trust these instructions** - only search if incomplete/incorrect

**Quick Ref:** Build all: `./ya make cloud/blockstore/buildall -r` | Test: `./ya make -t cloud/blockstore/` | Format: `clang-format-18 -i file.cpp` | Sanitizer: `./ya make --sanitize=address <target>` | Docs: `/doc/REPOSITORY_STRUCTURE.md`

