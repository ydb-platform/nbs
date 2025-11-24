# Copilot Instructions for NBS Repository

## Overview
Network Block Store (NBS) and Network File Store: YDB BlobStorage-based storage platform.
- **NBS**: Block devices via NBD/vhost-blk/gRPC
- **NFS**: POSIX filesystem via FUSE/virtiofs
- **Size**: ~2.2GB repo with C++ (40K+ files), Go (disk_manager/tasks), Python (tests)
- **Build**: `ya` tool (requires `devtools-registry.s3.yandex.net` access)

**Key Directories:** `cloud/blockstore/` (NBS), `cloud/filestore/` (NFS), `cloud/disk_manager/` (Go snapshot service), `cloud/storage/core/` (common libs), `contrib/` (OSS deps), `example/` (local dev scripts).

**Root Configs:** `.clang-format` (clang-format-18 REQUIRED), `.clang-tidy`, `ya.conf`, `.pre-commit-config.yaml`, `.githooks/pre-commit` (whitespace checks).

## Build & Test

### Prerequisites
**System:** x86_64, Ubuntu 20.04+ (18.04 EOL but may work), 80GB+ disk (SSD), 16GB+ RAM. **Network:** Access to `devtools-registry.s3.yandex.net` required.
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

## Code Style & Quality

### C++ Style (MANDATORY - clang-format-18 required)
```bash
# Format all modified C++ files before commit
clang-format-18 -i $(git diff --name-only --diff-filter=AM | grep -E '\.(cpp|h)$')
clang-format-18 --dry-run -Werror file.cpp  # Verify without modifying
```

**Naming Conventions** (from .clang-tidy):
- Classes/Structs: `TCamelCase`, Interfaces: `ICamelCase`, Enums: `ECamelCase` (constants: `UPPER_CASE`)
- Namespaces: `NCamelCase`, Functions: `CamelCase`, Variables: `camelBack`, Macros: `UPPER_CASE`

**Style Rules**: 80 columns max, 4-space indent, custom braces (after class/function/struct, not namespace)

### Linting & Pre-Commit (REQUIRED)
```bash
git config core.hooksPath .githooks           # Enable hooks (checks whitespace)
pre-commit run --all-files                    # Validate YAML, whitespace, newlines
./ya make --test-type=clang_tidy -t <target>  # C++ linting
./ya make --test-type=flake8,black -t cloud/  # Python
./ya make --test-type=gofmt -t cloud/disk_manager/  # Go
```

### Code Review Checklist (verify before PR)
- [ ] clang-format-18 applied to all C++ files
- [ ] Naming follows conventions (T/I/E/N prefixes, camelBack)
- [ ] No trailing whitespace (`git diff --check` passes)
- [ ] Line length ≤80 columns
- [ ] Tests added/updated and passing (small/medium: `./ya make -t --test-size=small,medium`)
- [ ] Builds without warnings
- [ ] No clang-tidy warnings in modified code
- [ ] Complex logic has comments, no unexplained commented-out code

## CI Workflows
**Main PR:** `.github/workflows/pr.yaml` - Triggers on PR to `main` (skips docs/examples/md files). Requires org membership or `ok-to-test` label for external contributors.

**Build/Test:** `.github/workflows/build_and_test_ya.yaml` - Targets: `cloud/blockstore/apps/`, `cloud/filestore/apps/`, `cloud/disk_manager/`, `cloud/tasks/`, `cloud/storage/`. Default tests: `unittest,clang_tidy,gtest,py3test,py2test,pytest,flake8,black,go_test,gofmt`. Sizes: `small,medium,large`. Preset: `relwithdebinfo`.

**PR Labels:**
- `ok-to-test` - Enable CI for external contributors
- `large-tests` - Include large tests (default: small+medium only)
- `blockstore`/`filestore`/`disk_manager`/`tasks` - Test specific projects only
- `asan`/`tsan` - Add sanitizer builds
- `recheck` - Re-trigger without new commit

**Test Artifacts:** S3 at `https://github-actions-s3.website.nemax.nebius.cloud/ydb-platform/nbs/PR-check/<run_id>/...` (find run_id in Actions tab) - See `build_logs/`, `test_logs/`, `summary/ya-test.html`

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
# Test (WARNING: destructive, only use on test disks): sudo dd oflag=direct if=/dev/urandom of=/dev/nbd0 count=5 bs=4096
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

**Making changes with code style/review focus:**

1. **Before coding:** Review existing code style in target area, check related tests
2. **While coding:** Follow naming (TCamelCase/ICamelCase/camelBack), keep ≤80 cols, comment complex logic
3. **Before commit (MANDATORY):**
   ```bash
   clang-format-18 -i $(git diff --name-only --diff-filter=AM | grep -E '\.(cpp|h)$')
   git diff --check  # No trailing whitespace
   ./ya make <target>  # Build without warnings
   ./ya make -t --test-size=small,medium --test-type=unittest,clang_tidy <target>
   ```
4. **Self-review:** Check naming conventions, no debug/commented code, tests cover edge cases
5. **Verify checklist:** Format ✓, naming ✓, whitespace ✓, tests ✓, builds ✓, linting ✓
6. **Local testing:** Use `example/` scripts for runtime verification if needed

**Quick Ref:** Build all: `./ya make cloud/blockstore/buildall -r` | Test: `./ya make -t cloud/blockstore/` | Format: `clang-format-18 -i file.cpp` | Sanitizer: `./ya make --sanitize=address <target>` | Docs: `doc/REPOSITORY_STRUCTURE.md`
