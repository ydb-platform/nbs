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
│   ├── 01-memory-pool-type-traits.patch
│   ├── 02-fiber-uring24-compat.patch
│   └── 03-init-skip-rseq-init.patch
└── overlay/                          # Files copied verbatim into silk tree
    ├── ya.make
    ├── include/sys/rseq.h            # Stub for ya include checker
    └── src/
        ├── fibers/{ya.make, tests/ya.make, tests/silk_test_env.cpp}
        └── util/{ya.make, tests/ya.make, tests/silk_test_env.cpp}
```

## What each patch does

- **01-memory-pool-type-traits**: adds `#include <type_traits>` to
  `memory-pool.h`. Required because the repo's libc++ doesn't transitively
  pull it in via `<memory>`.
- **02-fiber-uring24-compat**: casts the `io_uring_enter2` arg pointer to
  `sigset_t*`. Silk targets liburing 2.9 where the arg is `void*`; the repo
  has 2.4 where it is `sigset_t*`. The cast is safe because the kernel
  interprets the pointer based on the `IORING_ENTER_EXT_ARG` flag.
- **03-init-skip-rseq-init**: removes the `rseq_init()` call from
  `silk::initialize()`. The repo's older librseq auto-registers via
  constructor and doesn't expose `rseq_init()` publicly.

If a future silk version is built against a newer liburing or librseq, the
corresponding patch can be dropped.

## Dependencies referenced by the overlay ya.make files

- `contrib/libs/liburing` (2.4) — io_uring
- `contrib/libs/backtrace` — libbacktrace for stack symbolization
- `contrib/libs/librseq` — restartable sequences
- `contrib/restricted/boost/context/fcontext_impl` — Boost.Context asm
- `contrib/restricted/boost/intrusive` — Boost.Intrusive containers
- `contrib/restricted/googletest/googletest` — Google Test
