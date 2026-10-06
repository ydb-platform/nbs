"""
 Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.

 Redistribution and use in source and binary forms, with or without modification,
 are permitted provided that the following conditions are met:
 1. Redistributions of source code must retain the above copyright notice,
    this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.
 3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software without
    specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
"""

import subprocess
import os
from os import environ as env
from libmem_defs import *
from gbm import GBM


class NewGBM(GBM):
    """
    Extends GBM to compile and run gbench_main.cpp (redesigned benchmark framework)
    instead of the monolithic gbench.cpp. All result parsing, CSV generation, and
    gains calculation are inherited unchanged from GBM/BaseBench.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.overlap = str(self.MYPARSER['ARGS'].get('overlap', 'd'))
        self.backend = str(self.MYPARSER['ARGS'].get('backend', 'p'))
        self.layout = str(self.MYPARSER['ARGS'].get('layout', 'i'))
        self.heap = str(self.MYPARSER['ARGS'].get('heap', ''))

    def _setup_compilation(self):
        """Compile gbench_main.cpp with the new framework include paths"""
        # Paths relative to self.path (build/tools/benchmark/external/gbench/)
        #   ../../include           -> build/tools/benchmark/include/  (benchmark headers)
        #   ../../../common/include -> build/tools/common/include/     (common headers)
        command = [
            "g++",
            "-std=c++17",
            "-O3",
            "-Wno-deprecated-declarations",
            "gbench_main.cpp",
            "-isystem", "benchmark/include",
            "-I", "../../include",
            "-I", "../../../common/include",
            "-mclflushopt",
        ]

        linker_flags = [
            "-Lbenchmark/build/src",
            "-lbenchmark",
            "-lpthread",
        ]

        if AVX512_FEATURE_ENABLED:
            command.insert(1, "-DAVX512_FEATURE_ENABLED")

        if VERBOSE_ENABLED:
            command.insert(1, "-DLIBMEM_VERBOSE")

        if self.preload == 'y':
            command.extend(linker_flags + ["-o", "googlebench"])
        else:
            command_amd = command.copy()
            command_amd.extend(["-L" + LIBMEM_ARCHIVE_PATH, "-l:libaocl-libmem.a"]
                               + linker_flags + ["-o", "googlebench_amd"])

            command_glibc = command.copy()
            command_glibc.extend(["-static-libgcc", "-static-libstdc++"]
                                 + linker_flags + ["-o", "googlebench_glibc"])

        if self.preload == 'y':
            subprocess.run(command, cwd=self.path, check=True)
        else:
            subprocess.run(command_amd, cwd=self.path, check=True)
            subprocess.run(command_glibc, cwd=self.path, check=True)

    def _build_bench_args(self):
        """Build the positional argument list for gbench_main.cpp (11 args)"""
        return [
            str(self.func),
            str(self.memory_operation),
            str(self.ranges[0]),
            str(self.ranges[1]),
            str(self.iterator),
            str(self.align),
            str(self.spill),
            str(self.page),
            str(self.overlap),
            str(self.backend),
            str(self.layout),
        ]

    def _apply_heap_preload(self, base_preload):
        """Combine library preload with optional heap runtime preload."""
        parts = [p for p in [base_preload, self.heap] if p]
        env['LD_PRELOAD'] = ':'.join(parts)
        if self.heap:
            print(f"  Heap runtime: {self.heap}")

    def gbm_run(self):
        if self.bestperf:
            return self.get_best_throughput_from_multiple_runs(self.variant)

        if self.variant == "amd":
            self.LibMemVersion = subprocess.check_output(
                "file " + LIBMEM_BIN_PATH +
                "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell=True)
            self._apply_heap_preload(LIBMEM_BIN_PATH)
            libmem_version = self.LibMemVersion.decode('utf-8').strip() \
                if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
            print("NewGBM : Running Benchmark on AOCL-LibMem " + libmem_version)
        else:
            self.GlibcVersion = subprocess.check_output(
                "ldd --version | awk '/ldd/{print $NF}'", shell=True)
            self._apply_heap_preload('')
            glibc_version = self.GlibcVersion.decode('utf-8').strip() \
                if isinstance(self.GlibcVersion, bytes) else str(self.GlibcVersion).strip()
            print("NewGBM : Running Benchmark on GLIBC " + glibc_version)

        if self.ranges[0] == 0:
            self.ranges[0] = 1

        bench_args = self._build_bench_args()
        gbench_flags = [
            "--benchmark_repetitions=" + str(self.repetitions),
            "--benchmark_min_warmup_time=" + str(self.warm_up),
            "--benchmark_counters_tabular=true",
        ]

        with open(self.result_dir + '/gb' + str(self.variant) + '.txt', 'w') as g:
            if self.preload == 'y':
                binary = "./googlebench"
            else:
                binary = "./googlebench_" + self.variant

            subprocess.run(
                ["taskset", "-c", str(self.core), binary] + gbench_flags + bench_args,
                cwd=self.path, env=env, check=True, stdout=g, stderr=subprocess.PIPE)

    def get_best_throughput_from_multiple_runs(self, variant, num_runs=3):
        """Override to pass all 11 args to the new benchmark binary"""
        all_runs_data = []

        if variant == "amd":
            if not hasattr(self, 'LibMemVersion') or not self.LibMemVersion:
                self.LibMemVersion = subprocess.check_output(
                    "file " + LIBMEM_BIN_PATH +
                    "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell=True)
            libmem_version = self.LibMemVersion.decode('utf-8').strip() \
                if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
            print(f"\nNEWGBM : Running AOCL-LibMem {libmem_version} benchmark with {num_runs} iterations...")
        else:
            if not hasattr(self, 'GlibcVersion') or not self.GlibcVersion:
                self.GlibcVersion = subprocess.check_output(
                    "ldd --version | awk '/ldd/{print $NF}'", shell=True)
            glibc_version = self.GlibcVersion.decode('utf-8').strip() \
                if isinstance(self.GlibcVersion, bytes) else str(self.GlibcVersion).strip()
            print(f"\nNEWGBM : Running Glibc {glibc_version} benchmark with {num_runs} iterations...")

        for run_idx in range(num_runs):
            print(f"  Running iteration {run_idx + 1}/{num_runs}...")

            if variant == "amd":
                self._apply_heap_preload(LIBMEM_BIN_PATH)
            else:
                self._apply_heap_preload('')

            ranges = self.ranges.copy()
            if ranges[0] == 0:
                ranges[0] = 1

            bench_args = self._build_bench_args()
            bench_args[2] = str(ranges[0])
            bench_args[3] = str(ranges[1])

            gbench_flags = [
                "--benchmark_repetitions=" + str(self.repetitions),
                "--benchmark_min_warmup_time=" + str(self.warm_up),
                "--benchmark_counters_tabular=true",
            ]

            output_file = f'{self.result_dir}/gb{variant}_run{run_idx}.txt'
            with open(output_file, 'w') as g:
                if self.preload == 'y':
                    binary = "./googlebench"
                else:
                    binary = "./googlebench_" + variant

                subprocess.run(
                    ["taskset", "-c", str(self.core), binary] + gbench_flags + bench_args,
                    cwd=self.path, check=True, stdout=g, stderr=subprocess.PIPE)

            size_values = subprocess.run(
                [f"grep '_mean' gb{variant}_run{run_idx}.txt | grep -Eo '/[0-9]+_mean' | grep -Eo '[0-9]+'"],
                cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            throughput_values = subprocess.run(
                [f"grep '_mean' gb{variant}_run{run_idx}.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"],
                cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()

            converted_throughput = throughput_values.copy()
            self.throughput_converter(converted_throughput)

            run_data = {}
            for i, size in enumerate(size_values):
                if i < len(converted_throughput):
                    run_data[int(size)] = converted_throughput[i]
            all_runs_data.append(run_data)

        if not all_runs_data:
            return [], []

        all_sizes = list(all_runs_data[0].keys())
        best_throughputs = []
        for size in all_sizes:
            throughputs_for_size = [run_data.get(size, 0) for run_data in all_runs_data]
            best_throughputs.append(max(throughputs_for_size))

        final_output = f'{self.result_dir}/gb{variant}.txt'
        with open(final_output, 'w') as f:
            f.write("Best performance results from 3 iterations:\n")
            for size, throughput in zip(all_sizes, best_throughputs):
                unit = "G/s" if throughput >= 1 else "M/s"
                display_throughput = throughput if throughput >= 1 else throughput * 1000
                f.write(f"/{size}_mean {display_throughput:.2f} {unit}\n")

        return [str(size) for size in all_sizes], best_throughputs
