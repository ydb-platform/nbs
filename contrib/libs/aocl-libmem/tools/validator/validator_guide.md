# AOCL-LibMem Validator Guide

## Description

The AOCL-LibMem Validator Framework performs functional validation of LibMem
supported functions by checking data integrity of source and destination memory.

Three validator binaries are shipped, all driven by the same ctest suite:

| Binary | Source | Purpose |
|---|---|---|
| `libmem_validator_unified` | Unified C++    | Refactored framework: traits + registry + dynamic dispatch |
| `libmem_validator_gtest`   | Unified C++ / GTest-style | Interactive runner for one named test at a time |
| `libmem_validator`         | Legacy C       | Original implementation, kept for cross-checks |

You can run validation in three ways:

1. Driver script: `build_dir/test/validator.py`
2. Direct binary invocation (any of the three above)
3. `ctest` utility from `build_dir/test/`

---

## Diagnostic output (Verbose mode)

All three validators support extra per-test diagnostics, gated by a single
umbrella CMake flag:

```
-DLIBMEM_TOOLS_VERBOSE=ON   # turns on diagnostics for validator AND benchmark
-DLIBMEM_TOOLS_VERBOSE=OFF  # default
```

Build with diagnostics enabled:

```bash
cd /path/to/build
cmake -DLIBMEM_TOOLS_VERBOSE=ON .
make
```

The flag wires up the appropriate compile defines for each binary
(`LIBMEM_VERBOSE` for the unified C++ binaries, `LIBMEM_VALIDATOR_DEBUG` for the
legacy C binary).

When enabled, each test logs:

- **VEC_SZ** -- vector size in bytes (32 for AVX2, 64 for AVX512)
- **Function name** -- which LibMem function is under test
- **Test size** -- the size parameter being validated
- **Alignment mode** -- single alignment vs all combinations
- **Individual alignment tests** -- per (src, dst) pair when in alignment mode

Example output:

```
[DEBUG] libmem_validator started
[DEBUG] VEC_SZ = 64 bytes
[DEBUG] Function: memcpy
[DEBUG] Size: 1024
[DEBUG] Alignment check mode: All alignments
[DEBUG] Testing alignment - src: 0, dst: 0
[DEBUG] Testing alignment - src: 0, dst: 1
...
[DEBUG] Testing alignment - src: 63, dst: 63
```

> Diagnostic modes are intended for debugging the validator itself, not for
> bulk validation runs.

---

## Test categories

All three binaries cover the same logical test categories. The ctest suite
schedules each category against both legacy and unified validators.

- **Iterator testing** -- step by 1 B from `0` to `PAGE_SIZE` [0B-4KB]
- **Shift testing** -- powers-of-2 size sweep [1B, 2B, 4B, ... 32MB]
- **Alignment testing** -- all src/dst alignment combinations [0..63 x 0..63] up to 4KB
- **Threshold testing** (AMD-only) -- L2 and NT-store boundary sweeps
- **ZEN5/ZEN6 aligned-vector thresholds** -- when applicable

> Note: `memcmp`, `strcmp`, and `strncmp` runs take longer because every byte
> must be compared.

---

## Ctest utility

`ctest` is invoked from `build_dir/test/`. Every test is registered twice --
once against the legacy binary, once against the unified binary -- and tagged
with the corresponding ctest **label** (`legacy` or `unified`). Test names also
carry a matching name prefix (`legacy_*`, `unified_*`).

This gives you two equivalent filtering mechanisms:

- `-R <regex>` -- substring/regex on **test name**
- `-L <label>` -- exact match on the **label** (recommended for variant scoping)

The two can be combined; they AND together.

### Arguments

```
$ ctest -R <regex> -E <exclude regex> -L <label> -LE <exclude label> -j [<jobs>]

-R <regex>    Examples:
              "^legacy_"               run all legacy tests
              "^unified_"              run all unified tests
              "memcpy"                 run both legacy and unified for memcpy
              "memcpy_iter"            run both legacy and unified iter tests for memcpy
              "unified_memcpy_align"   run unified memcpy alignment tests
              "legacy_memcpy|legacy_strcpy"   multiple patterns ORed together

-L <label>    "legacy"   run all legacy tests
              "unified"  run all unified tests

-LE <label>   exclude tests carrying this label (e.g. `-LE legacy`)

-j <jobs>     max concurrent processes [0..$(nproc)]
```

### Quick reference

| Goal                                          | Command                              |
|-----------------------------------------------|--------------------------------------|
| Run everything (legacy + unified)             | `ctest`                              |
| Run unified only                              | `ctest -L unified`                   |
| Run legacy only                               | `ctest -L legacy`                    |
| Run both variants of `memcpy_iter`            | `ctest -R memcpy_iter`               |
| Run unified `memcpy_iter` only                | `ctest -R memcpy_iter -L unified` <br/> *or* `ctest -R unified_memcpy_iter` |
| Run legacy `memcpy_iter` only                 | `ctest -R memcpy_iter -L legacy` <br/> *or* `ctest -R legacy_memcpy_iter`   |
| Everything except legacy                      | `ctest -LE legacy`                   |
| List configured labels                        | `ctest --print-labels`               |
| Failure logs only                             | `ctest --progress --output-on-failure -j $(nproc)` |
| Exclude iter + shift from a function          | `ctest -R "memcpy" -E "iter\|shift"` |
| Save run log with timestamp                   | `ctest -R memcpy_shift -O ctest_results_$(date +%Y_%m_%d_%H%M%S).log` |

### Reports

```
build_dir/test/Testing/Temporary/LastTest.log
```

---

## Validator binaries (direct invocation)

All three binaries take the same positional layout for the basic case:

```bash
<binary> <function> <size> [src_align] [dst_align] [all_alignments]
```

- **function** -- one of: `memcpy`, `mempcpy`, `memmove`, `memset`, `memcmp`, `memchr`,
  `strcpy`, `strncpy`, `strcmp`, `strncmp`, `strlen`, `strnlen`, `strcat`, `strncat`,
    `strstr`, `strspn`, `strchr`, `strrchr`
- **size** -- size parameter for the function
- **src_align** / **dst_align** -- alignment offsets (default `0`)
- **all_alignments** -- `1` to sweep every (src, dst) combination

### `libmem_validator_unified` (unified C++)

Adds discovery flags on top of the common CLI:

```bash
./tools/validator/libmem_validator_unified memcpy 1024 0 0 1
./tools/validator/libmem_validator_unified memset 64 0 0 1

./tools/validator/libmem_validator_unified --list-functions
./tools/validator/libmem_validator_unified --list-tests memcpy
./tools/validator/libmem_validator_unified --help
```

### `libmem_validator_gtest` (unified, GTest-style)

Same arguments plus a `--test=<name>` selector for running one named test:

```bash
./tools/validator/libmem_validator_gtest memcpy 64
./tools/validator/libmem_validator_gtest memmove 40 --test=BackwardOverlap
./tools/validator/libmem_validator_gtest memcpy 128 5 3
./tools/validator/libmem_validator_gtest memcpy 128 0 0 1   # 4096 alignment combos
./tools/validator/libmem_validator_gtest --list-tests
./tools/validator/libmem_validator_gtest --help
```

### `libmem_validator` (legacy C)

```bash
./tools/validator/libmem_validator memcpy 1024 0 0 1   # all alignments
./tools/validator/libmem_validator memcpy 1024 0 0 0   # single, clean run
```

---

## Validator script (`validator.py`)

A Python driver for ad-hoc, non-standard size sweeps. Reports are written to:

```
build_dir/test/out/<libmem_function>/<time-stamp-counter>/<validation_report.csv>
```

### Arguments

```
$ ./validator.py -r [start] [end] -a [src] [dst] -t [iterator] <mem_function>

-r Range       [start] [end] in bytes
-a alignment   [src] [dst] alignments (default: 64 B for both)
-t iterator    iteration pattern (default: "2x" of starting size -- '<<1')
<function>     memcpy, memset, memcmp, memmove, mempcpy, memchr,
               strcpy, strncpy, strcmp, strncmp, strlen, strnlen,
               strcat, strncat, strstr, strspn, strchr, strrchr
```

### Example

```
$ ./validator.py -r 8 64 -a 5 8 -t "+1" memcpy
# Validation from 8B -> 64B, src align 5, dst align 8, +1 byte iterator

> Validation for size [8]  in progress...
> Validation for size [9]  in progress...
> Validation for size [10] in progress...
...
```

Run `./validator.py -h` for the full option list.
