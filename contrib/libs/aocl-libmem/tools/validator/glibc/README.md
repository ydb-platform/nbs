# GLIBC Functionality Tests for AOCL-LibMem

This directory contains a comprehensive test suite for validating AOCL-LibMem implementations against glibc's official test suite.

## Overview

The `glibc_validator.sh` script automates the following:
1. Downloads fresh glibc test files from the official glibc repository
2. Applies minimal patches to make tests compile standalone
3. Compiles all test executables
4. Runs tests with `LD_PRELOAD` to test AOCL-LibMem implementations

## Prerequisites

- GCC compiler
- curl (for downloading test files)
- AOCL-LibMem built and installed in `build/lib/`

## Building AOCL-LibMem

Before running tests, build the library:

```bash
cd /path/to/aocl-libmem
mkdir -p build
cd build
cmake ..
make && make install
```

This will create `build/lib/libaocl-libmem.so.5.1.1`.

## Running All Tests

The script can be run from any directory:

```bash
# From the tools/validator/glibc directory:
cd tools/validator/glibc
./glibc_validator.sh

# Or from the build directory:
cd build
./tools/validator/glibc/glibc_validator.sh

# Or from the repository root:
./tools/validator/glibc/glibc_validator.sh
```

This will:
- Download test files for all supported functions (memcpy, strcpy, strlen, etc.)
- Apply patches to make tests compile
- Compile all tests into `build/test/glibc-functionality-tests/bin/`
- Run each test with LD_PRELOAD
- Display a summary of passed/failed tests

## Running Individual Tests

After running the validator script once, you can run individual tests manually:

### Run a specific test with AOCL-LibMem

```bash
# From the repository root
LD_PRELOAD=build/lib/libaocl-libmem.so.5.1.1 build/test/glibc-functionality-tests/bin/test-strlen
```

### Run a specific test without AOCL-LibMem (system glibc)

```bash
# From the repository root
build/test/glibc-functionality-tests/bin/test-strlen
```
**Memory functions:**
- memcpy, mempcpy, memmove, memset, memcmp, memchr

**String functions:**
- strcpy, strncpy, strcmp, strncmp, strcat, strncat
- strstr, strspn, strlen, strnlen, strchr, strrchr

## Test Output Structure

```
build/test/glibc-functionality-tests/
├── test-*.c                    # Downloaded test source files
├── test-string.h               # Common test header (patched)
├── test-memcpy-support.h       # Support header for memcpy tests
└── bin/
    ├── test-strlen             # Compiled test executables
    ├── test-strcpy
    ├── test-memcpy
    └── ...
```
## Re-downloading Fresh Tests

The script detects if test files already exist and skips downloading. To force a fresh download:

```bash
# Remove existing tests
rm -rf build/test/glibc-functionality-tests/

# Run the script again
./glibc_validator.sh
```

## Test Script Options

The script is self-contained and requires no command-line arguments. It automatically detects its location and calculates the correct paths to:

- **Test directory:** `<repo_root>/build/test/glibc-functionality-tests/`
- **Shared library:** `<repo_root>/build/lib/libaocl-libmem.so.5.1.1`
- **CMakeLists.txt:** `<repo_root>/src/CMakeLists.txt` (for function list)

The script works correctly whether run from the source tree (`tools/validator/glibc/`) or the build tree (`build/tools/validator/glibc/`).

## Notes

- Tests are downloaded from the official glibc git repository (sourceware.org)
- Minimal patches are applied to make tests compile standalone
- Tests use LD_PRELOAD to intercept function calls and redirect to AOCL-LibMem
- All tests run in a single pass; there's no need to run individual compilations