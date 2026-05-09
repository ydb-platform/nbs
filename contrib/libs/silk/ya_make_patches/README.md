# Silk ya.make patches

This directory contains everything needed to make a fresh silk checkout
compilable with the ya build system.

## When to use

After updating the silk submodule/source to a new version, run:

```bash
bash contrib/libs/silk/ya_make_patches/apply.sh
```

## What it does

1. **Source patches:**
   - Adds `#include <type_traits>` to `memory-pool.h` (libc++ doesn't
     transitively include it from `<memory>`)
   - Casts `io_uring_enter2` arg to `sigset_t*` for liburing 2.4 API
     compatibility (silk targets 2.9 where the signature is `void*`)

2. **Generated config files:**
   - `contrib/libbacktrace/config.h` — autoconf-equivalent defines for
     Linux/ELF/x86-64
   - `contrib/libbacktrace/backtrace-supported.h` — feature flags

3. **Stub headers for ya include checker:**
   - `include/sys/rseq.h` — aliases `__rseq_offset` to librseq's
     `rseq_offset` (ya sysroot doesn't have glibc's version)
   - `contrib/libbacktrace/sys/link.h` — empty stub; libbacktrace only
     includes it under `#ifdef HAVE_SYS_LINK_H` which we don't define

4. **ya.make files** for all libraries and tests

5. **Test environment files** (`silk_test_env.cpp`) that call
   `silk::initialize()` / `silk::FiberScheduler::initialize()` via
   gtest's `AddGlobalTestEnvironment`

6. **Downloads `boost/intrusive/set.hpp`** if missing from the repo's
   partial boost checkout

## Dependencies

The ya.make files reference these existing repo libraries:
- `contrib/libs/liburing` (2.4) — io_uring
- `contrib/restricted/boost/context/fcontext_impl` — Boost.Context asm
- `contrib/restricted/boost/intrusive` — Boost.Intrusive containers
- `contrib/restricted/googletest/googletest` — Google Test

## Verifying

```bash
ya make --build=debug contrib/libs/silk
ya test --build=debug contrib/libs/silk/src/util/tests contrib/libs/silk/src/fibers/tests
```

## Notes on liburing version mismatch

Silk targets liburing 2.9 where `io_uring_enter2` takes `void *arg`.
Our repo has liburing 2.4 where it takes `sigset_t *sig`. The kernel
interprets the pointer based on the `IORING_ENTER_EXT_ARG` flag, so
the `reinterpret_cast` is safe. If the repo upgrades to liburing ≥2.5,
the cast patch can be dropped.
