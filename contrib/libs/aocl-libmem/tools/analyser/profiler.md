# AOCL-LibMem Profiler Tool

## Description
The AOCL-LibMem Profiler is a powerful tool that provides detailed insights into memory function usage patterns by tracing calls to standard C library memory functions. The tool collects data on function call counts, size distributions, and memory alignment, enabling performance analysis and optimization of memory operations in your applications.

Two profiler implementations are provided:

| | **C++ Tool (`prof_libmem`)** | **Python Tool (`prof_libmem.py`)** |
|---|---|---|
| **Runtime Dependencies** | None (zero external library dependencies) | Python 3.6+, BCC/bpfcc ≥ 0.8.0 |
| **BPF Approach** | Precompiled embedded BPF object + raw kernel syscalls | Runtime BPF C-text generation/compilation via BCC |
| **Portability** | Static-linked C++ binary, runs on any compatible Linux kernel | Requires matching BCC version for kernel compatibility |
| **IFUNC Handling** | Built-in runtime resolution + ELF fallback + alias deduplication | BCC-managed symbol resolution |
| **Recommended For** | Production use, environments without Python/BCC | Quick prototyping, environments with BCC already installed |

---

## Features
- Traces memory-related libc functions (memcpy, mempcpy, memcmp, memmove, memset, etc.)
- Supports both distribution tracking (with size histograms) and call counting
- Analyzes memory alignment of source and destination pointers (configurable boundary)
- Can attach to running processes or launch new applications
- Multi-thread safe — properly tracks and aggregates data across all threads in target processes
- Provides periodic reporting at configurable intervals
- Filters results by process ID for focused analysis
- Supports tracing specific functions only
- Automatic detection and merging of shared implementations (e.g., `memcpy + memmove`)

### Traced Functions

The profiler traces **17 libc functions** in two categories:

**Distribution Functions (10)** — tracked with size histograms:
`memcpy`, `memmove`, `mempcpy`, `memcmp`, `memset`, `memchr`, `strncpy`, `strncmp`, `strncat`, `strnlen`

**Count-Only Functions (7)** — tracked by call count (enabled with `-c`):
`strcpy`, `strcmp`, `strcat`, `strlen`, `strchr`, `strstr`, `strspn`

---

## Command Line Options

Both tools (`prof_libmem` and `prof_libmem.py`) share the same command-line interface:

```
Usage: prof_libmem [OPTIONS] [-e CMD [ARGS...]]

Trace calls to libc memory/string functions

Options:
  -i, --interval SECONDS      Reporting interval (default: 5)
  -p, --pid PID               Trace only this PID
  -f, --function FUNC         Trace only specified functions
  -v, --verbose               Increase verbosity
  -t, --time SECONDS          Total time to run
  -c, --track-count           Track count-only functions
  -o, --output FILE           Output file
  -a, --check-alignment [ALIGNMENT]
                               Check memory alignment at specified boundary.
                               Default: 64B if no value given.
                               Accepts: 16, 32, 64, 4096 (bytes) or
                               16B, 32B, 64B, 1KB, 4KB, 2MB, 1GB.
                               Only the single specified alignment is checked.
  -h, --help                  Show this help

  -e, --exec CMD [ARGS...]    Execute and trace command (must be last option)

Note: The -e option must be the last option specified. All arguments
      after -e are treated as the executable and its arguments.
```

| Option | Description |
|--------|-------------|
| `-i, --interval SECONDS` | Reporting interval in seconds (default: 5) |
| `-p, --pid PID` | Trace only a specific process ID |
| `-f, --function FUNC` | Trace only specified functions (repeatable) |
| `-v, --verbose` | Increase verbosity (repeatable for more detail) |
| `-t, --time SECONDS` | Total time in seconds to run the trace |
| `-c, --track-count` | Also track count-only functions (strcpy, strcmp, etc.) |
| `-o, --output FILE` | Output file for logging results (default: stdout) |
| `-a, --check-alignment [ALIGNMENT]` | Check memory alignment at the specified boundary. Default: 64B if no value given. Accepts numeric values (e.g., `64`, `4096`) or human-readable forms (`64B`, `4KB`, `2MB`, `1GB`). Only the single specified alignment is checked — e.g., `-a 4KB` reports only 4096-byte alignment, not 64B or 32B. |
| `-e, --exec CMD [ARGS...]` | Execute and trace the specified command (**must be the last option**) |
| `-h, --help` | Show help message |

> **Note:** The Python tool additionally supports `-d, --debug-log FILE` for writing detailed debug logs to a separate file.

## Usage Examples

All examples below work with both `prof_libmem` (C++) and `prof_libmem.py` (Python). Replace the command name as appropriate for your setup.

```bash
# Trace a running process
sudo ./prof_libmem -p 1234

# Launch and trace an application
sudo ./prof_libmem -v -i 2 -e ./my_application arg1 arg2

# Check 64-byte alignment (default when no value given)
sudo ./prof_libmem -a -e ./my_program

# Check 1KB (1024-byte) alignment
sudo ./prof_libmem -a 1024 -e ./my_program
sudo ./prof_libmem -a 1KB -e ./my_program

# Check 4KB page alignment
sudo ./prof_libmem -a 4KB -e ./my_program

# Check 2MB huge-page alignment
sudo ./prof_libmem -a 2MB -e ./my_program

# Trace only memcpy and memset with count-only functions enabled
sudo ./prof_libmem -f memcpy -f memset -c -e ./my_program

# Save output to file
sudo ./prof_libmem -o profile.log -t 30 -e ./my_program

# Track count-only functions in a running process
sudo ./prof_libmem -p 1234 -c

# High verbosity for troubleshooting
sudo ./prof_libmem -v -v -e ./myapp

# Save to file with 10s interval for 30s
sudo ./prof_libmem -i 10 -t 30 -o profile.log -e ./myapp
```

---

## C++ Profiler (`prof_libmem`)

### Overview
The C++ profiler is the recommended production tool. It uses a precompiled embedded BPF object and raw kernel syscalls (`bpf()`, `perf_event_open`, tracefs) — no external libraries (no libbpf, no BCC, no LLVM) are needed at runtime. The binary is statically linked against `libstdc++` and `libgcc` for maximum portability.

### Requirements
- Root privileges (for BPF operations)
- Linux kernel with eBPF, uprobes, perf events, and tracefs/debugfs (`/sys/kernel/debug/tracing`)
- No additional packages required at runtime

### Build Requirements
- CMake ≥ 3.10
- C++14-capable compiler (GCC or Clang)
- `clang` (for BPF program compilation at build time only)
- `xxd` (for embedding BPF object as byte array at build time only)

### Glibc 2.34 Portability (Critical for Distribution)

> **⚠ Important — Read Before Building:**
> The compiled `prof_libmem` binary **hard-links against the glibc version of the build
> host**. When built on a system with glibc ≥ 2.34 (e.g., Ubuntu 22.04+, Debian 12+,
> Fedora 36+), the binary pulls in glibc 2.34+ symbols (`pthread_once`,
> `__pthread_key_create`, `__libc_start_main`) and possibly glibc 2.35+ symbols
> (`_dl_find_object`). These symbols originate from the **static `libstdc++.a`** that
> was itself compiled against the host glibc.
>
> **The resulting binary will NOT run on older systems** (Ubuntu 20.04, Debian 10,
> CentOS 7/8, RHEL 7/8, SLES 12/15, etc.) and will fail at startup with:
> ```
> /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.34' not found
> ```

To produce a **portable binary** that runs on a wider range of Linux distributions,
**you must build on a system with glibc < 2.34**:

| Build Environment | Glibc Version | Resulting Binary Compatibility |
|---|---|---|
| CentOS 7 / RHEL 7 | 2.17 | glibc 2.17+ (maximum compatibility) |
| Ubuntu 18.04 (Bionic) | 2.27 | glibc 2.27+ |
| Debian 10 (Buster) | 2.28 | glibc 2.28+ |
| Ubuntu 20.04 (Focal) | 2.31 | glibc 2.31+ (recommended) |
| Ubuntu 22.04+ / Debian 12+ | 2.34–2.38 | glibc 2.34+ only |

**Option 1 — Build on an older host (recommended):**
```bash
# On Ubuntu 20.04 / Debian 10 / CentOS 7:
cd /path/to/aocl-libmem
cmake -DALMEM_TOOLS=ON -B build && cd build
make tools
```

**Option 2 — Use a Docker container:**
```bash
docker run --rm -v $(pwd):/work -w /work ubuntu:20.04 bash -c "
    apt-get update && apt-get install -y cmake g++ clang xxd &&
    cmake -DALMEM_TOOLS=ON -B build && cd build && make tools
"
```

**Verify glibc requirements of a compiled binary:**
```bash
objdump -T build/tools/analyser/prof_libmem | grep GLIBC | awk '{print $5}' | sort -u
```

If only `GLIBC_2.2.5` through `GLIBC_2.31` appear, the binary is portable across most
enterprise Linux distributions.

### Building

The profiler is built as part of the AOCL-LibMem tools. From the **project root**:

```bash
cmake -DALMEM_TOOLS=ON -B build
cd build
make tools          # builds prof_libmem + profiler_test
```

> **Note:** The `-DALMEM_TOOLS=ON` flag is required to configure the profiler
> build targets. Without it, the default `cmake` and `make` will not include any
> profiler-related configuration or output.

The compiled binaries are placed in `<build_folder>/tools/analyser/`:
- `prof_libmem` — the profiler executable
- `profiler_test` — the correctness test application

### Key Capabilities

#### IFUNC Resolution
The C++ profiler resolves GNU Indirect Functions (IFUNCs) at attach time using `dlopen`/`dlsym` with an ELF symbol table fallback. This ensures the uprobe is attached to the actual optimized implementation selected by the CPU at runtime, not the IFUNC resolver stub.

#### Alias Deduplication (Shared Implementation Merging)
After resolving all function addresses, the profiler detects when multiple functions resolve to the same address. When this occurs (commonly `memcpy` and `memmove` sharing the same glibc implementation), the functions are merged into a single entry displayed as `memcpy + memmove`. Only one BPF probe is attached to avoid double-counting. On systems where `memcpy` and `memmove` have different implementations, they remain as separate entries.

---

## Python Profiler (`prof_libmem.py`)

### Overview
The Python profiler uses the BCC (BPF Compiler Collection) framework to dynamically generate, compile, and load BPF programs at runtime. It provides the same command-line interface and profiling functionality as the C++ tool but requires the Python/BCC stack.

### Requirements
- Python 3.6+
- BCC Tools (python3-bpfcc) version 0.8.0 or higher, preferably higher than 0.8.0 for better compatibility with newer kernels
- Root privileges (for BPF operations)
- Minimum kernel version 4.19.0 for eBPF tools

### Limitations Compared to C++ Tool
1. **Heavy runtime dependencies**: Requires Python 3, BCC/bpfcc library, and indirectly LLVM/Clang (BCC uses them internally for runtime BPF compilation).
2. **Version fragility**: Multiple compatibility workarounds are needed in the code for different BCC versions (e.g., Python 3.10 `collections.MutableMapping` patch, BCC cleanup suppression for ≤ 0.12.0, `bcc_symbol_option` struct workaround for older libbcc layouts).
3. **Kernel compatibility issues**: BCC 0.8.0 has known `inline_asm` compilation failures on kernels ≥ 5.4.0. Upgrading BCC is required for newer kernels.
4. **Runtime compilation overhead**: BPF programs are compiled from C source at every invocation, adding startup latency.

### Kernel Compatibility

| Kernel Version | BCC ≥ 0.8.0 | BCC > 0.8.0 |
|---|---|---|
| 4.19.0 | ✅ Confirmed working | ✅ |
| 4.20.0 – 5.3.x | ⚠️ Untested | ✅ Recommended |
| 5.4.0+ | ❌ `inline_asm` errors | ✅ Confirmed working |

### Installation

#### Check Existing BCC Version
```bash
# Check via pip
pip show bcc

# Check programmatically
python3 -c "import collections, collections.abc; collections.MutableMapping = collections.abc.MutableMapping; import bcc; print('BCC version:', bcc.__version__)"
```

#### Install from Distribution Packages

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install -y python3-bpfcc
```

**RHEL/CentOS/Fedora:**
```bash
sudo dnf install -y python3-bcc
```

**SUSE/openSUSE:**
```bash
sudo zypper install -y python3-bcc
```

#### Install via pip
```bash
pip install bcc==0.1.10  # or latest available version
```

#### Install from Source
```bash
# Install build dependencies (Ubuntu/Debian)
sudo apt install -y build-essential cmake git python3-dev \
    libbpf-dev libelf-dev zlib1g-dev libfl-dev

# Clone and build BCC
git clone https://github.com/iovisor/bcc.git
cd bcc && git checkout v0.8.0  # or latest
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr
make -j$(nproc)
sudo make install
```

---

## Understanding the Output

The tool provides three types of output:

### 1. Size Distribution Histograms
For functions with size parameters (memcpy, memset, etc.):
```
====== memcpy + memmove (1500 calls) ======
     (unit)           : count    distribution
         0 -> 1       : 0        |                                        |
         2 -> 3       : 0        |                                        |
         4 -> 7       : 120      |******                                  |
         8 -> 15      : 60       |***                                     |
        16 -> 31      : 402      |********************                    |
        32 -> 63      : 950      |***********************************************|
```

### 2. Call Counts
For count-only functions (when using `-c`):
```
strlen: 2445 calls
strcpy: 352 calls
```

### 3. Alignment Statistics
When using `-a` (default 64B) or `-a ALIGNMENT` (custom boundary), only the single
specified alignment is checked. For example, `-a 4KB` checks 4096-byte alignment only,
not 64B or 32B.

**Example with `-a` (default 64B):**
```
  Memory Alignment Statistics (64B boundary):
  Alignment Type            Count     Percentage Distribution
  -----------------------------------------------------------------
  Src 64B-aligned         : 1520       63.12%    |*************|
  Dst 64B-aligned         : 1820       75.60%    |***************|
  Both 64B-aligned        : 1320       54.85%    |***********|
  Neither aligned         : 380        15.79%    |***|
```

**Example with `-a 4KB`:**
```
  Memory Alignment Statistics (4KB boundary):
  Alignment Type            Count     Percentage Distribution
  -----------------------------------------------------------------
  Src 4KB-aligned         : 320        13.30%    |***|
  Dst 4KB-aligned         : 410        17.04%    |***|
  Both 4KB-aligned        : 280        11.63%    |**|
  Neither aligned         : 1956       81.30%    |****************|
```

### 4. Function Call Distribution Summary
```
--- Function Call Distribution ---
Function            Calls         Percent  Distribution
---------------------------------------------
memcpy + memmove   32450         45.21%   |***********************|
strlen             18940         26.40%   |*************|
strcmp              10230         14.26%   |*******|
strncpy            5620          7.83%    |****|
memset             3500          4.88%    |**|
memcmp             1020          1.42%    |*|

Total function calls: 71760
```

---

## memcpy + memmove: Shared Implementation and Excess Count Exceptions

### Shared Implementation in glibc

On most glibc versions, `memcpy()` and `memmove()` resolve to the **same implementation address** at runtime. This is because glibc's optimized `memcpy` already handles overlapping regions (the defining characteristic of `memmove`), so both symbols point to identical code.

**Profiler behavior:**
- The C++ profiler (`prof_libmem`) automatically detects this at probe attach time by comparing resolved addresses after IFUNC resolution.
- When the addresses match, the two functions are **merged** into a single entry displayed as `memcpy + memmove`.
- Only **one BPF uprobe** is attached to the shared address, so each call is counted exactly once regardless of whether the application called `memcpy()` or `memmove()`.
- On systems where the implementations differ (e.g., some non-glibc C libraries or custom builds), they remain as separate entries with independent probes.

### Excess Count Exceptions

When profiling `memcpy` or `memmove` (or any traced function), the profiler may report **more calls than the application explicitly made**. This is expected behavior caused by the following sources:

#### 1. Internal glibc Calls
The BPF uprobe is attached to the libc function entry point and captures **all** calls to that address within the traced process, including:
- Internal calls from `malloc`/`free`/`realloc` (memory allocator internals)
- Internal calls from `fprintf`/`printf`/`snprintf` (stdio internals)
- Internal calls from `pthread_create`/`pthread_join` (threading internals)
- Internal calls from dynamic linker operations (`dlopen`, symbol resolution)
- Any other glibc-internal use of `memcpy`/`memmove`/`memset`

These internal calls typically add **2–10 extra calls** in single-threaded scenarios.

#### 2. Multithreaded Amplification
In multithreaded applications, the excess from glibc internals **scales with the number of threads**, because each `pthread_create` and thread-local setup path may invoke memory functions internally. For example:
- 30 threads × ~1.6 extra calls/thread ≈ ~48 additional calls beyond the expected count.

#### 3. IFUNC Resolver Invocations
On first call to an IFUNC symbol, the dynamic linker invokes the resolver function, which may itself call memory functions. This adds a small one-time overhead (typically 1–3 extra calls per IFUNC-resolved function).

#### 4. PLT/GOT Resolution
The Procedure Linkage Table (PLT) stub executes on the first call to a dynamically linked function before the GOT entry is populated. This initial resolution may trigger additional internal memory function calls.

### Verification Tolerances

The correctness verification framework accounts for these exceptions using the following tolerances:

| Check Type | Tolerance | Details |
|---|---|---|
| **Call Count** | `max(50, 10% of expected)` extra calls allowed | `expected ≤ actual ≤ expected + max(50, 10% × expected)` |
| **Alignment Statistics** | ±10% | `abs(actual - expected) ≤ max(1, 10% × expected)` |
| **Size Distribution Buckets** | `actual ≥ expected` | Allows extra entries from glibc-internal calls |

**Example:** If a test makes 1000 explicit `memcpy` calls:
- Expected: 1000 calls
- Acceptable range: 1000 – 1100 calls (1000 + max(50, 100))
- The extra calls come from glibc-internal memory operations in the traced process

---

## Correctness Test Suite

The profiler includes a comprehensive correctness test suite to validate call counting, size distribution accuracy, alignment tracking, and multithreading behavior across **all 18 supported functions**.

### Components

All test suite files are located in the `tools/analyser/` directory alongside the profiler source:

| File | Purpose |
|---|---|
| `profiler_test.c` | Workload generator (C source) — compiled to `profiler_test` binary |
| `profiler_test_runner.sh` | Automated test runner script — orchestrates profiler + test app + verification |
| `profiler_verifier.py` | Output verifier script — compares profiler output against expected values |

> **Note:** `profiler_test_runner.sh` and `profiler_verifier.py` are standalone scripts
> shipped as separate files in `tools/analyser/`. They do not need to be built — run them
> directly with `sudo ./profiler_test_runner.sh` (requires root for BPF).

#### `profiler_test_runner.sh`

The test runner script orchestrates end-to-end correctness testing. It:
1. Checks root privileges (BPF requirement)
2. Auto-builds binaries if missing (`cmake .. && make`)
3. Iterates over all 18 functions across four test categories (call count, multithreaded, alignment, alignment+threading)
4. Captures profiler output and extracts `EXPECTED_*` metadata emitted by the test application
5. Invokes `profiler_verifier.py` to compare actual vs expected results
6. Reports per-test PASS/FAIL and a final summary

Failed test logs are saved to `/tmp/prof_test_<function>_<calls>_<threads>.log` for manual inspection.

#### `profiler_verifier.py`

The verification script parses the profiler output and compares it against the expected values from the test application's metadata. It performs three types of checks:

- **Call count verification** — allows extra calls from glibc internals using a tolerance of `max(50, 10% of expected)`. Ensures `expected ≤ actual ≤ expected + tolerance`.
- **Size distribution verification** — checks that each histogram bucket count is `≥` the expected count (extra entries from glibc-internal calls are acceptable).
- **Alignment verification** — checks that "Both aligned" and "Neither aligned" counts match the expected values within ±10% tolerance.

The script accepts two positional arguments:
```bash
python3 profiler_verifier.py <profiler_output_file> <test_stderr_file>
```

### Building the Test Suite

From the **project root** (recommended):
```bash
cmake -DALMEM_TOOLS=ON -B build
cd build
make tools          # builds prof_libmem + profiler_test
```

The test application binary is placed in `<build_folder>/tools/analyser/profiler_test`.

### Running the Test Application Directly

```
Usage: profiler_test <func_name> <calls> [threads] [unaligned_calls]

Modes:
  Basic:     profiler_test <func> <calls> [threads]
  Alignment: profiler_test <func> <aligned_calls> 1 <unaligned_calls>
```

**Basic mode** — makes a known number of calls for call-count verification:
```bash
./build/tools/analyser/profiler_test memcpy 1000        # 1000 memcpy calls, 1 thread
./build/tools/analyser/profiler_test memset 500 4       # 500 memset calls × 4 threads
./build/tools/analyser/profiler_test strlen 200         # 200 strlen calls, 1 thread
```

**Alignment mode** — makes aligned + unaligned calls for alignment verification:
```bash
# 100 aligned + 50 unaligned = 150 total calls
./build/tools/analyser/profiler_test memcpy 100 1 50

# 200 aligned + 200 unaligned = 400 total calls
./build/tools/analyser/profiler_test memmove 200 1 200
```

When the 5th argument (unaligned_calls) is provided, the test runs in alignment mode:
- Makes `<aligned_calls>` calls with both src and dst pointers on **64-byte aligned** boundaries (using the static aligned buffers directly)
- Makes `<unaligned_calls>` calls with both src and dst pointers **offset by 3 bytes** from the 64-byte boundary
- Emits `EXPECTED_ALIGNED` and `EXPECTED_UNALIGNED` metadata for automated verification

**Supported function names:**
- Distribution: `memset`, `memcpy`, `mempcpy`, `memmove`, `memcmp`, `memchr`, `strncpy`, `strncmp`, `strncat`, `strnlen`
- Count-only: `strcpy`, `strcmp`, `strcat`, `strlen`, `strchr`, `strrchr`, `strstr`, `strspn`

The test application outputs expected metadata to `stderr` for automated verification:

Basic/multithreaded mode:
```
EXPECTED_FUNCTION=memcpy
EXPECTED_CALLS=4000
TEST_TYPE=multithreaded
NUM_THREADS=4
CALLS_PER_THREAD=1000
```

Alignment mode:
```
EXPECTED_FUNCTION=memcpy
EXPECTED_CALLS=150
TEST_TYPE=alignment
EXPECTED_ALIGNED=100
EXPECTED_UNALIGNED=50
```

### Running the Automated Test Suite

```bash
# Must be run as root (BPF requires root privileges)
sudo ./profiler_test_runner.sh
```

The test runner automatically:
1. Verifies root privileges
2. Checks that the profiler binaries are available (build with `make tools`)
3. Runs **four test categories** covering all functions, threading, and alignment:

#### Test Category 1: Call Count Tests (All 18 Functions)
Tests each of the 18 supported functions with 500 calls in a single thread:
- Distribution functions (10) are tested with default profiler flags (`-vv -t 5`)
- Count-only functions (8) are tested with the `-c` flag to enable count tracking

#### Test Category 2: Multithreaded Tests (All 18 Functions)
Tests **every** function across multiple thread counts (`1`, `2`, `4`, `$(nproc)`), each with 1000 calls per thread:
- Distribution functions (10): `memset`, `memcpy`, `mempcpy`, `memmove`, `memcmp`, `memchr`, `strncpy`, `strncmp`, `strncat`, `strnlen`
- Count-only functions (8): `strcpy`, `strcmp`, `strcat`, `strlen`, `strchr`, `strrchr`, `strstr`, `strspn` (with `-c` flag)

Validates that the profiler correctly aggregates counts across all threads for every supported function.

#### Test Category 3: Alignment Tests (All 10 Distribution Functions)
Tests all 10 distribution functions using the alignment test mode with 100 aligned + 100 unaligned calls. The profiler is run with `-a 64` to enable alignment tracking.

- **Dual-pointer functions** (7): `memcpy`, `mempcpy`, `memmove`, `memcmp`, `strncpy`, `strncmp`, `strncat` — both src and dst alignment is verified
- **Single-pointer functions** (3): `memset` (dst-only), `memchr` (src-only), `strnlen` (src-only) — exercises aligned vs unaligned buffer paths

The verifier checks that the profiler's "Both aligned" count matches the expected aligned count (±10% tolerance) and the "Neither aligned" count matches the expected unaligned count.

#### Test Category 4: Alignment + Threading Combined Tests
Tests key dual-pointer functions (`memcpy`, `memmove`, `memcmp`, `strncpy`) with both alignment tracking and multithreading enabled (2 and 4 threads). Validates that alignment statistics remain accurate under concurrent access.

### Test Matrix Summary

| Category | Functions Tested | Variations | Tests |
|---|---|---|---|
| 1. Call Count | All 17 | 1 thread × 500 calls | 17 |
| 2. Multithreaded | All 17 | 4 thread counts × 1000 calls/thread | 68 |
| 3. Alignment | All 10 distribution | 100 aligned + 100 unaligned | 10 |
| 4. Align + Thread | 4 key dual-pointer | 2 thread counts × 200 calls | 8 |
| **Total** | | | **~103** |

> **Note:** The exact total depends on the number of unique thread counts on your system (the `$(nproc)` value may duplicate `4` on a 4-core machine, reducing the count slightly).

**Example commands for each category:**

```bash
# Category 1 — Call Count (single-threaded, 500 calls)
sudo ./prof_libmem -vv -t 5 -e ./profiler_test memcpy 500

# Category 2 — Multithreaded (4 threads × 1000 calls/thread)
sudo ./prof_libmem -vv -t 5 -e ./profiler_test memcpy 1000 4

# Category 3 — Alignment (100 aligned + 100 unaligned, single-threaded)
sudo ./prof_libmem -vv -a 64 -t 5 -e ./profiler_test memcpy 100 1 100

# Category 4 — Alignment + Threading (200 aligned calls, 4 threads)
sudo ./prof_libmem -vv -a 64 -t 5 -e ./profiler_test memcpy 200 4
```

### Interpreting Test Results

```
===================================================================
                        Test Summary
===================================================================
Total tests: 103
Passed: 103
Failed: 0
===================================================================
✓ All tests PASSED!
```

If a test fails, the output file is saved at `/tmp/prof_test_<function>_<calls>_<threads>.log` for manual inspection.

### Expected Excess Counts in Tests

When running the test suite, the following behaviors are **expected and accounted for**:

| Scenario | Excess Source | Typical Extra Calls |
|---|---|---|
| Any single-threaded test | glibc internal `memcpy`/`memset` from `malloc`, `printf`, etc. | 2–10 calls |
| Multithreaded `memcpy` / `memmove` | `pthread_create` internals + per-thread allocator calls | ~1.6 × num_threads |
| Multithreaded `memset` | Thread stack initialization, TLS setup | ~1–2 × num_threads |
| `memcpy` when `memmove` shares impl | Merged as `memcpy + memmove`; single probe captures both | Combined count (not doubled) |
| First call to any IFUNC function | IFUNC resolver + dynamic linker overhead | 1–3 one-time calls |

The verification script (`profiler_verifier.py`) automatically applies the appropriate tolerances (see [Verification Tolerances](#verification-tolerances) above), so these excess counts do not cause false test failures.

---

## Initialization Process

When using the `-e` option to launch and trace an application, the profiler:

1. Launches the application in a suspended state
2. Attaches BPF probes to all specified functions
3. Waits for all probes to be properly attached and initialized
4. Resumes the application once everything is ready

This ensures that all function calls are captured from the very beginning of the application's execution.

## Tips

1. **Prefer the C++ tool** (`prof_libmem`) for production use — it has zero runtime dependencies and handles IFUNC/alias deduplication automatically.
2. **Use the Python tool** (`prof_libmem.py`) only when BCC is already available and you need rapid iteration or BCC-specific features.
3. **Use verbose mode** (`-v` or `-vv`) to see IFUNC resolution details, shared implementation merging, and probe attachment status.
4. **Start with short intervals** to quickly verify that tracing is working properly.
5. **For performance optimization**, focus on the functions with the highest call counts or those handling large memory blocks.
6. **Check alignment statistics** to identify potential performance bottlenecks. Use `-a` for the default 64B check, or specify a boundary like `-a 4KB` or `-a 2MB` to check page/huge-page alignment. Only the single specified alignment is reported.
7. **Multi-thread support is built-in**. The profiler uses atomic BPF operations to ensure accurate counting across all threads.
8. **System-wide tracing** can be resource-intensive; prefer specifying a target process (`-p PID`) when possible.
9. **Always place the `-e` option at the end** of your command line to avoid confusion between the profiler's arguments and the executable's arguments.
10. **Check the log for messages** like "Successfully attached X/Y probes" to confirm the profiler is working correctly.
11. **When `memcpy + memmove` appears** in the output, it means glibc uses the same implementation for both — this is normal and the count represents the combined total.

## Notes

- Both tools require root privileges to use BPF features.
- Some functions may not be traceable in all environments or applications.
- For executables with very short lifetimes, you may need to increase verbosity to debug issues.
- Memory alignment checking works for both distribution-tracked functions and count-only functions that handle pointers.
- All `memmove()` calls will be tracked under `memcpy + memmove` when glibc has a common implementation for both. On systems with separate implementations, they are tracked independently.

## Troubleshooting

### BPF Compilation Errors (Python tool only)
If you encounter errors like "expected '(' after 'asm'" or "Failed to compile BPF text", this indicates a compatibility issue between your BCC version and kernel version. This is commonly seen with BCC 0.8.0 on kernel ≥ 5.4.0.

**Solutions:**
1. **Switch to the C++ tool** (`prof_libmem`) which has no BCC dependency
2. **Upgrade BCC** using distribution-specific methods:
   - Debian/Ubuntu: `sudo apt install -t buster-backports python3-bpfcc`
   - RHEL/CentOS/Fedora: `sudo dnf update python3-bcc`
   - SUSE/openSUSE: `sudo zypper update python3-bcc`
3. **Update kernel headers**: `sudo apt install linux-headers-$(uname -r)` (or equivalent)

### Function Not Found Errors
Some functions may not be available in all C library versions or may be inlined by the compiler. Use `-v` to see resolution details.

### Permission Errors
Ensure you're running the profiler with `sudo` as BPF operations require root privileges.

### No Data Collected
If the profiler runs but shows no function calls:
1. Verify the target process is actively using memory functions
2. Check that the process hasn't exited before profiling starts
3. Use verbose mode (`-v`) to see detailed attachment information

### Unexpected Call Counts
If the profiler reports more calls than expected, see [Excess Count Exceptions](#excess-count-exceptions) above. This is normal behavior caused by glibc-internal memory function calls.
