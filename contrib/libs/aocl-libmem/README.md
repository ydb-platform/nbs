# AOCL-LibMem
AOCL-LibMem is a Linux library of data movement and manipulation functions (such as memcpy
and strcpy) highly optimized for AMD Zen micro-architecture. This library has multiple
implementations of each function that can be chosen based on the application requirements as per
alignments, instruction choice, threshold values, and tunable parameters. By default, this library will
choose the best fit implementation based on the underlying micro-architectural support for CPU
features and instructions.

This release of AOCL-LibMem library supports the following functions:
+ memcpy
+ mempcpy
+ memmove
+ memset
+ memcmp
+ memchr
+ strcpy
+ strncpy
+ strcmp
+ strncmp
+ strlen
+ strnlen
+ strcat
+ strncat
+ strstr
+ strspn
+ strchr

# INSTALLATION
INSTALLATION FROM AOCL-LIBMEM GIT REPOSITORY:

After downloading the latest stable release from the git repository, https://github.com/amd/aocl-libmem, follow the steps from the BUILD_RUN.md, to configure and build it for  all released AMD Zen CPU architecutres.

`Note:` _Binaries can be built based on the cpu ISA features: AVX2 and AVX512 with help of Cmake flag: ALMEM_ARCH. It is recommended not to load/run the AVX512 library on a non-AVX512 machine as it will lead to crash due to unsupported instructions._

`Note:` _By default, the library uses page-boundary-safe unmasked vector loads for small sizes to maximize throughput (the same technique used by glibc). A **strict-bounds build variant** is available via the `ALMEM_STRICT_BOUNDS=Y` CMake option, which enforces masked loads that never load beyond the caller-specified buffer size. See [BUILD_RUN.md](BUILD_RUN.md) for details._

`Note:` _IFUNC may only be compatible with the most recent compiler toolchains to manage runtime dependencies effectively._

`Note:` _The dynamic loading library APIs, such as dlopen, dlclose, and dlsym, have been incorporated into Glibc starting from version 2.34.Therefore, if you compile the library on a system using Glibc 2.34 or newer and then execute it on a system with an older version of Glibc,you will encounter runtime issues related to symbol resolution for these functions.To avoid such issues, it is recommended, if you build the library with older version of Glibc (prior to 2.34) then execute it on systems with the same version of Glibc.If built on Glibc 2.34 or newer, ensure that the target system also has Glibc 2.34 or newer._

For detailed instructions on how to configure, build, install, please refer to the AOCL User Guide on https://developer.amd.com/amd-aocl

Upstream repository contains LibMem reference manual that has all the information you need to get started with LibMem, including installation directions, usage examples, and a complete API reference.You may also find a copy of the document here on github.

## Build and Run:
**Refer to [BUILD_RUN](https://github.com/amd/aocl-libmem/blob/main/BUILD_RUN.md)**

## User Guide:
**Refer to [User_Guide](https://github.com/amd/aocl-libmem/blob/main/docs/User_Guide.md)**

## Validator Tool Guide:
**Refer to tools/validator/validator_guide.md**

## AOCL-LibMem Benchmarking Tool Guide:
**Refer to tools/benchmark/benchmark_guide.md**

Also, please read the LICENSE file for information on copying and distributing this software.

# Technical Support
Please email toolchainsupport@amd.com for questions, issues and feedback on LibMem.

Please submit your questions, feature requests, and bug reports on the [GitHub issues](https://github.com/amd/aocl-libmem/issues) page.
