# Silk ya.make patches

This directory contains everything needed to install silk into this repo and
make it compilable with the ya build system.

## Usage

You provide a path to a fresh silk checkout, the script does the rest:

```bash
bash contrib/libs/silk/ya_make_patches/apply.sh /path/to/silk-checkout
```

This will:

1. Wipe the contents of `contrib/libs/silk/` except this `ya_make_patches/`
   directory
2. Copy the silk source from `/path/to/silk-checkout/` into
   `contrib/libs/silk/`
3. Apply source patches from `ya_make_patches/patches/`
4. Lay down ya.make / config / stub files from `ya_make_patches/overlay/`

After it finishes, build with:

```bash
ya make --build=debug contrib/libs/silk
ya test --build=debug contrib/libs/silk
```

## Layout

```
ya_make_patches/
├── apply.sh                          # The install/update script
├── README.md                         # This file
├── patches/                          # Source patches applied in order
│   ├── 01-gitignore.patch
│   ├── 02-fiber-uring24-compat.patch
│   ├── 03-rseq-register-per-thread.patch
│   ├── 04-fiber-cxa-get-globals-arcadia-libcxxrt.patch
│   └── 05-fiber-uring24-sqes-sz.patch
└── overlay/                          # Files copied verbatim into silk tree
    ├── ya.make
    ├── include/sys/rseq.h            # Stub for ya include checker
    └── src/
        ├── fibers/{ya.make, tests/ya.make, tests/silk_test_env.cpp}
        └── util/{ya.make, tests/ya.make, tests/silk_test_env.cpp}
```

## What each patch does

- **01-gitignore**: appends `*.orig` and `contrib/` to silk's `.gitignore`
  so a re-run of `apply.sh` doesn't leave patch-created `.orig` backup
  files or vendored contrib trees behind in a fresh silk checkout.
- **02-fiber-uring24-compat**: casts the `io_uring_enter2` arg pointer to
  `sigset_t*`. Silk targets liburing 2.9 where the arg is `void*`; the repo
  has 2.4 where it is `sigset_t*`. The cast is safe because the kernel
  interprets the pointer based on the `IORING_ENTER_EXT_ARG` flag.
- **03-rseq-register-per-thread**: replaces silk's `rseq_init()` in
  `silk::initialize()` with a call to a new `silk::ensureRseqRegistered()`
  helper defined in `include/silk/util/platform.h`, and calls the same
  helper as the first thing `getCurrentProcessor()` does. The helper
  wraps `rseq_register_current_thread()` behind a `thread_local` bool so
  it runs at most once per thread, and its `inline` linkage keeps the
  guard shared across every TU that touches it. Silk targets glibc 2.35+
  (where the C library auto-registers rseq for every thread) and reads
  `cpu_id` through the librseq-provided `__rseq_offset` TLS slot; on the
  repo's older glibc targets nobody registers rseq, so `__rseq_offset`
  stays zero, `getCurrentProcessor` returns garbage, and every downstream
  `Perf::processorState[cpu]` access lands out of bounds and segfaults.
  Silk's own docs (`docs/scheduler.md`, "Proxy Fibers") explicitly
  support arbitrary application threads calling fiber APIs, so the
  registration has to be lazy on any thread the first time it enters
  silk — pre-registering only silk-spawned scheduler / worker threads
  is not enough.
- **04-fiber-cxa-get-globals-arcadia-libcxxrt**: gates silk's Itanium ABI
  `__cxxabiv1::__cxa_get_globals` redeclaration on
  `!defined(Y_CXA_EH_GLOBALS_COMPLETE)`. Arcadia's libcxxrt already
  declares that symbol in `<cxxabi.h>` without `noexcept` and marks the
  fact with `Y_CXA_EH_GLOBALS_COMPLETE`; silk's `noexcept`-tagged
  redeclaration then clashes on the exception specification.
- **05-fiber-uring24-sqes-sz**: replaces `ring.sq.sqes_sz` in
  `accountRingMemoryMappings` with `ring_entries * sizeof(io_uring_sqe)`.
  Silk targets liburing 2.9 where `sqes_sz` records the length of the sqes
  mapping; the repo has 2.4 without that field. The computed expression is
  exactly the length 2.4 itself mmaps and munmaps for the sqes array.

If a future silk version is built against a newer liburing or librseq, the
corresponding patch can be dropped. Patches 02 and 05 can be dropped
together once the repo's liburing reaches 2.6+. Patch 03 can be dropped only once every
target machine ships glibc 2.35+ (or silk itself learns to register rseq
per thread upstream). Patch 04 can be dropped once silk upstream either
drops the redeclaration or gates it on Arcadia's macro.

## Dependencies referenced by the overlay ya.make files

- `contrib/libs/liburing` (2.4) — io_uring
- `contrib/libs/backtrace` — libbacktrace for stack symbolization
- `contrib/libs/librseq` — restartable sequences
- `contrib/restricted/boost/context/fcontext_impl` — Boost.Context asm
- `contrib/restricted/boost/intrusive` — Boost.Intrusive containers
- `contrib/restricted/googletest/googletest` — Google Test
