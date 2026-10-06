# AOCL-LibMem Benchmark Guide

## Description
- AOCL-LibMem Benchmarking Framework is used for performance analysis of LibMem supported functions  against the  Glibc installed host machine under test.
- AOCL-LibMem Benchmarking Tool supports the below benchmark framework and their respecitve modes.
    - TinyMemBench:

        External benchmark tool which uses the existing tbm framework.
    - GoogleBench: hot & cold cached modes

        A microbenchmark support library for running memory benchmarks.
    - FleetBench:

        Google's production-representative memory benchmark. Supports three modes with different versions:
        | Mode | Version | Cache Levels | Workloads | Description |
        |------|---------|--------------|-----------|-------------|
        | `-c` (Cached) | v0.2 | L1 only | 9 Google workloads (A,B,D,L,M,Q,S,U,W) | Single random offset, same for src/dst |
        | `-m` (Multi-cache) | v0.3.3 | L1, L2, LLC, Cold | 10 distributions (0-8 + Fleet) | Independent random offsets for src/dst |
        | `-a` (Alignment) | latest | L1, L2, LLC, Cold, Mixed | 10 distributions (0-8 + Fleet) | Alignment-aware benchmarking |

        Supported functions: `memcpy`, `memmove`, `memset`, `memcmp`
    - DCPerf:

        Meta's datacenter benchmark suite supporting WDL (workload-driven latency) benchmarks for memcpy/memset and AI benchmarks for rebatch/tensor operations.
        
        **Note**: DCPerf benchmarks require sudo/root privileges to run.

- Result will be generated in the format of csv and .png(in case of graph reports)

## Report Description
  - Performance reports will be stored in this PATH
```
build_dir/test/out/<libmem_function>/<time-stamp-counter>/
```
  - Gives the Throughput gains in %age.

## Build_and_Compilers
- Linux:
  - Compilers: AOCC and GCC

## Benchmark Framework Setup
- Python 3.10 and above
- Python dependency packages
    - numpy  (version 1.24.2 or greater)
    - pandas (verion 1.5.3 or greater)

    STEPS for installing the required packages:

      pip3 install pandas
      pip3 install numpy
- FleetBench dependency package
    - Bazel (any version below 8.0.0) needs to be installed manually
    - **Note**: FleetBench with Bazel 8.0.0 causes workspace deprecated build issues due to WORKSPACE file migration to Bzlmod

    STEPS for installing Bazel using Bazelisk (Works across all Linux distributions):

        $ sudo apt install unzip curl  # For Ubuntu/Debian (use appropriate package manager for other distros)
        $ curl -LO "https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-amd64"
        $ chmod +x bazelisk-linux-amd64
        $ sudo mv bazelisk-linux-amd64 /usr/local/bin/bazel
        $ export USE_BAZEL_VERSION=7.1.2
        $ bazel --version

    **⚠️ CAUTION**: Setting the Bazel version using bazelisk may reset to the latest version in new terminal sessions. You might need to run `export USE_BAZEL_VERSION=7.1.2` again or add it to your `~/.bashrc` file for persistence:

        $ echo 'export USE_BAZEL_VERSION=7.1.2' >> ~/.bashrc
        $ source ~/.bashrc

    **Alternative installation methods for specific distributions:**

    On Ubuntu/Debian:

        $ sudo apt install curl gnupg
        $ curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg
        $ sudo mv bazel.gpg /etc/apt/trusted.gpg.d/
        $ echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
        $ sudo apt update && sudo apt install bazel

    On CentOS and RHEL:

  1.    Install dependency packages:

       $ sudo yum install epel-release
       $ sudo yum install gcc gcc-c++ java-1.8.0-openjdk-devel
  2.    Download the Bazel 6 binary installer from the Bazel releases page:

	   $ wget https://github.com/bazelbuild/bazel/releases/download/6.0.0/bazel-6.0.0-installer-linux-x86_64.sh
  3.    Install commands

       $ chmod +x bazel-6.0.0-installer-linux-x86_64.sh
       $ sudo ./bazel-6.0.0-installer-linux-x86_64.sh


## Running Bench framework

    $ ./bench.py <benchmark_name> <common_options> <benchmark_specific_options>
      <benchmark_name>  = {gbm,tbm,fbm,dcperf}
                          gbm          Googlebench
                          tbm          TinyMembench
                          fbm          Fleetbench
                          dcperf       DCPerf (WDL and AI benchmarks)

      <common_options>  = -x<core_id> -r [start] [end] -t "<iterator_value>" <LibMem_function> -perf [p,g,b,d] -bestperf

                        -x <core_id> : Enter the CPU core on which you want to run the benchmark.
                        -r [start] [end] : start and end size range in Bytes.(Not applicable for Fleetbench)
                                           Format: NUMBER[UNIT]
                                           where UNIT can be B, KB, MB, or GB(case insensitive).
                                           The default unit is Bytes.
                        -t "iter_value"  : increments the start size by "iter_value".
                                           (0 stands for size<<1; other +ve integers stands for incremental iterations.)
                        LibMem_function  : mem and str functions
                                          (memcpy,memmove,memset,memcmp,memchr,mempcpy,
                                          strcpy,strncpy,strcmp,strncmp,strlen,strnlen,strcat,strncat,strspn,strstr,strchr,strrchr)
                        -perf            : Performance report type
                                          l - Performance analysis for LibMem
                                          g - Performance analysis for Glibc
                                          c - Comparison report between LibMem old and new
                                          d - Defalut report Glibc vs. LibMem
                        -bestperf        : Runs benchmark 3 times and selects the best throughput
                                          for each size from those iterations (specific to GBM and TBM)

      <GBM_specific_option> = -m <mode> -a <align> -s <cache_spill> -p <page_option> -o <overlap> -preload <y,n> -i<repetitions> -w<warm_up time>

                            -m <h, c>    : hot  & cold cache behaviour
                            -a <a, u, d> : aligned (src and dst alignment are equal)
                                           un-alinged (src and str alignment are NOT equal)
                                           default alignment is random.
                            -s <l, m>    : Less spill and more spill (applicable with align mode only)
                            -p <x, t>    : Page-cross and Page-Tail scenario
                            -o <f, b, d> : [Memmove only]Forward overlap, Backward overlap and Default overlap
                                          (Default is 'd',both forward and backward overlaps)
                          -preload <y,n> : Running with LD_PRELOAD option = y & Running with static binaries = n
                          -i<repetitions>: Number of repetitions for consistent performance runs
                        -w<warm_up time> : Minimum Warmup time in seconds.
                        NOTE: -a and -p are mutually exclusive options

      <FBM_specific_option> = -mem_alloc <tcmalloc, glibc> -n<repetitions> -c|-m|-a -w<workload> --enable-aslr

                              -mem_alloc : Specify the memory allocator(default = glibc)
                          -n<repetitions>: Number of repetitions for consistent performance runs(default = 10)
                          -c             : Cached mode (v0.2) - L1 cache only, 9 Google workloads
                          -m             : Multi-cache mode (v0.3.3) - L1/L2/LLC/Cold, 10 workloads
                          -a             : Alignment mode (latest) - L1/L2/LLC/Cold/Mixed, alignment-aware
                          -w <workload>  : Run individual workload (0-8 for specific distribution,
                                          'fleet' for fleet aggregate in -m/-a modes).
                                          Default: run all workloads.
                          --enable-aslr  : Enable ASLR during the benchmark run.
                                          By default ASLR is disabled via
                                          'setarch $(uname -m) -R' for run-to-run consistency.
                          NOTE: Reported throughput is the MEDIAN of all repetitions.

      <TBM_specific_option> = None

      <DCPerf_specific_option> = [func] [sub_func]
                          func         : Benchmark type (optional, default='wdl')
                                        wdl - Workload-driven latency benchmarks
                                        ai  - AI benchmarks
                          sub_func     : Specific function/type to benchmark (optional)
                                        For 'wdl': memcpy, memset (defaults to both if not specified)
                                        For 'ai': rebatch, tensor

    Examples:
    Benchmark Help option
    $ ./bench.py -h
    $ ./bench.py dcperf -h

    Running Google Benchmark
    $ ./bench.py gbm memcpy -r 8B 16B -m c -t "1" -x 16
    Runs the Google Benchmark for Cold cache Memcpy for sizes[8,9,..16] on core -16

    $ ./bench.py gbm memcpy -r 8B 32KB -s m -x 16
    Runs GBM for Hot cache memcpy with More-cache spill

    Running TinyMembench
    $ ./bench.py tbm strcpy -r 8B 4KB -x 47
    Runs tinymembench for strcpy function fro sizes [8, 16, 32,..4096B] on core - 47

    Running Fleetbench
    $ ./bench.py fbm memset -x 47 -n 100
    Runs fleetbench (default: alignment/latest mode) for memset on core-47 for 100 repetitions
    (ASLR is disabled by default via 'setarch $(uname -m) -R'; median throughput is reported)

    $ ./bench.py fbm -c memcpy -x 47 -n 50
    Runs fleetbench v0.2 (cached, L1 only) for memcpy on core-47, all 9 Google workloads

    $ ./bench.py fbm -m memcpy -x 47 -n 10
    Runs fleetbench v0.3.3 (multi-cache) for memcpy on core-47, all workloads across L1/L2/LLC/Cold

    $ ./bench.py fbm -a memcpy -x 47 -n 3 -w 3
    Runs fleetbench latest (alignment mode) for memcpy workload-3 only on core-47, 3 repetitions

    $ ./bench.py fbm -m memset -x 47 -w fleet -perf l
    Runs fleetbench v0.3.3 for memset, fleet-aggregate workload only, LibMem performance

    $ ./bench.py fbm -a memmove -x 47 -n 5
    Runs fleetbench latest (alignment) for memmove, all workloads, 5 repetitions

    $ ./bench.py fbm -a memcpy -x 47 --enable-aslr
    Runs fleetbench latest with ASLR enabled (skips 'setarch -R' wrapper)

    Running DCPerf (Requires sudo privileges)
    $ sudo ./bench.py dcperf wdl -x 47
    Runs DCPerf WDL benchmarks for both memcpy and memset on core - 47

    $ sudo ./bench.py dcperf wdl memcpy -x 47
    Runs DCPerf WDL benchmarks for memcpy only on core - 47

    $ sudo ./bench.py dcperf ai rebatch -x 47
    Runs DCPerf AI rebatch benchmark on core - 47
