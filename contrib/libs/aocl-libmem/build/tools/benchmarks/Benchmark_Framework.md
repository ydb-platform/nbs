# Build & Run Guide for **_AOCL-LibMem Benchmarking Tool_**

## Description
- AOCL-LibMem Benchmarking Framework is used for performance analysis of LibMem supported functions  against the  Glibc installed host machine under test.
- AOCL-LibMem Benchmarking Tool supports the below benchmark framework and their respecitve modes.
    - TinyMemBench:

        External benchmark tool which uses the existing tbm framework.
    - GoogleBench: cached & uncached modes

        A microbenchmark support library for running memory benchmarks.
    - FleetBench:

        By making use of existing probability data set, function call to a specific size is being made.

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
    - Bazel-version 6 needs to be installed manually

    STEPS for installing Bazel:

    On Ubuntu :

        $ sudo apt install curl gnupg
        $ curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg
        $ sudo mv bazel.gpg /etc/apt/trusted.gpg.d/
        $ echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
        $ sudo apt update && sudo apt install bazel

    On CentOS and REHL :

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
      <benchmark_name>  = {gbm,tbm,fbm}
                          gbm          Googlebench
                          tbm          TinyMembench
                          fbm          Fleetbench

      <common_options>  = -x<core_id> -r [start] [end] -t "<iterator_value>" <LibMem_function> -perf [p,g,b,d] -bestperf

                        -x <core_id> : Enter the CPU core on which you want to run the benchmark.
                        -r [start] [end] : start and end size range in Bytes.(Not applicable for Fleetbench)
                                           Format: NUMBER[UNIT]
                                           where UNIT can be B, KB, MB, or GB(case insensitive).
                                           The default unit is Bytes.
                        -t "iter_value"  : increments the start size by "iter_value".
                                           (0 stands for size<<1; other +ve integers stands for incremental iterations.)
                        LibMem_function  : mem and str functions
                                          (memcpy,memmove,memset,memcmp,memchr,mempcpy
                                          strcpy,strncpy,strcmp,strncmp,strlen,strcat,strncat,strspn,strstr,strchr)
                        -perf            : Performance report type
                                          l - Performance analysis for LibMem
                                          g - Performance analysis for Glibc
                                          c - Comparison report between LibMem old and new
                                          d - Defalut report Glibc vs. LibMem
                        -bestperf        : Runs benchmark 3 times and selects the best throughput
                                          for each size from those iterations (specific to GBM and TBM)

      <GBM_specific_option> = -m <mode> -a <align> -s <cache_spill> -p <page_option> -o <overlap> -preload <y,n> -i<repetitions> -w<warm_up time>

                            -m <c, u>    : cached  & uncached behaviour
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

      <FBM_specific_option> = -mem_alloc <tcmalloc, glibc> -i<repetitions>
                              -mem_alloc : Specify the memory allocator(default = glibc)
                          -i<repetitions>: Number of repetitions for consistent performance runs(default = 100)

      <TBM_specific_option> = None

    Examples:
    Benchmark Help option
    $ ./bench.py -h

    Running Google Benchmark
    $ ./bench.py gbm memcpy -r 8B 16B -m u -t "1" -x 16
    Runs the Google Benchmark for Un-Cached Mode Memcpy for sizes[8,9,..16] on core -16

    $ ./bench.py gbm memcpy -r 8B 32KB -s m -x 16
    Runs GBM for Cached memcpy with More-cache spill

    Running TinyMembench
    $ ./bench.py tbm strcpy -r 8B 4KB -x 47
    Runs tinymembench for strcpy function fro sizes [8, 16, 32,..4096B] on core - 47

    Running Fleetbench
    $ ./bench.py fbm memset -x 47 -i 100
    Runs fleetbench for memset benchmarking on core - 47 for 100 iterations
