"""
 Copyright (C) 2023-25 Advanced Micro Devices, Inc. All rights reserved.

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
from pandas import read_csv
import argparse
import re
import csv
import datetime
from statistics import mean
from libmem_defs import *
import sys
sys.path.insert(0, '../tools/benchmarks')
from bench import BaseBench


class GBM(BaseBench):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # GBM-specific attributes
        self.align = str(self.MYPARSER['ARGS']['align'])
        self.spill = str(self.MYPARSER['ARGS']['spill'])
        self.page = str(self.MYPARSER['ARGS']['page'])
        self.path = "../tools/benchmarks/external/gbench/"
        self.isExist = ""
        self.memory_operation = self.MYPARSER['ARGS']['memory_operation']
        self.iterator = self.MYPARSER['ARGS']['iterator']
        self.preload = self.MYPARSER['ARGS']['preload']
        self.repetitions = self.MYPARSER['ARGS']['repetitions']
        self.warm_up = self.MYPARSER['ARGS']['warm_up']

    def __call__(self):
        self.isExist = os.path.exists(self.path + "/benchmark")
        if (not self.isExist):
            print("Downloading and Configuring GoogleBench")
            subprocess.run(["git", "clone", "-c", "advice.detachedHead=false", "-b", "v1.8.0", "https://github.com/google/benchmark.git"], cwd=self.path)
            subprocess.run(["git", "clone", "-b", "v1.13.x", "https://github.com/google/googletest.git", "benchmark/googletest"], cwd=self.path)
            subprocess.run(['cmake', '-E', 'make_directory', 'build'], cwd=self.path + "/benchmark")
            subprocess.run(['cmake', '-E', 'chdir', 'build', 'cmake', '-DBENCHMARK_DOWNLOAD_DEPENDENCIES=on', '-DCMAKE_BUILD_TYPE=Release', '../'], cwd=self.path + "/benchmark")
            subprocess.run(['cmake', '--build', 'build', '--config', 'Release'], cwd=self.path + "/benchmark")

        self._setup_compilation()

        if (self.perf == 'd'):
            self._run_default_performance()
        elif (self.perf == 'c'):
            self._run_comparison_performance()
        elif (self.perf == 'l'):
            self._run_libmem_performance()
        elif (self.perf == 'g'):
            self._run_glibc_performance()

    def _setup_compilation(self):
        """Setup compilation commands for GoogleBench"""
        command = [
            "g++",
            "-Wno-deprecated-declarations",
            "gbench.cpp",
            "-isystem", "benchmark/include",
            "-mclflushopt",
        ]

        linker_flags = [
            "-Lbenchmark/build/src",
            "-lbenchmark",
            "-lpthread",
        ]

        # Passing AVX512_FEATURE_ENABLED for VEC_SZ computation
        if AVX512_FEATURE_ENABLED:
            command.insert(1, "-DAVX512_FEATURE_ENABLED")

        # If PRELOAD option is enabled, set the output file name
        if self.preload == 'y':
            command.extend(linker_flags + ["-o", "googlebench"])
        # If static build, set the output file name
        else:
            # Generate the command for running LibMem by adding libmem static flags
            command_amd = command.copy()
            command_amd.extend(["-L" + LIBMEM_ARCHIVE_PATH, "-l:libaocl-libmem.a"] + linker_flags + ["-o", "googlebench_amd"])

            # Generate the command for running Glibc by adding glibc static flags
            command_glibc = command.copy()
            command_glibc.extend(["-static-libgcc", "-static-libstdc++"] + linker_flags + ["-o", "googlebench_glibc"])

        if self.preload == 'y':
            subprocess.run(command, cwd=self.path)
        else:
            # Compile and generate the executables
            subprocess.run(command_amd, cwd=self.path)
            subprocess.run(command_glibc, cwd=self.path)

    def _run_default_performance(self):
        """Run default performance analysis (Glibc vs LibMem)"""
        print("Benchmarking of "+str(self.func)+" for size range["+str(self.ranges[0])+"-"+str(self.ranges[1])+"] on "+str(self.bench_name))

        if self.bestperf:
            print("\nRunning in best performance mode - will run both Glibc and LibMem benchmarks with 3 iterations each...")
            self.variant = "glibc"
            glibc_size_values, self.glibc_throughput_values = self.gbm_run()
            self.variant = "amd"
            amd_size_values, self.amd_throughput_values = self.gbm_run()
            self.size_values = [int(val) for val in glibc_size_values]
        else:
            self.variant = "glibc"
            self.gbm_run()
            self.variant = "amd"
            self.gbm_run()

            values = subprocess.run(["grep '_mean' gbglibc.txt | grep -Eo '/[0-9]+_mean' | grep -Eo '[0-9]+'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            self.size_values = [int(val) for val in values]
            self.amd_throughput_values = subprocess.run(["grep '_mean' gbamd.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            self.glibc_throughput_values = subprocess.run(["grep '_mean' gbglibc.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()

            # Converting the M/s values to G/s
            self.throughput_converter(self.amd_throughput_values)
            self.throughput_converter(self.glibc_throughput_values)

        self.gains = self.calculate_gains(self.amd_throughput_values, self.glibc_throughput_values)

        # Converting sizes to B,KB,MB for reports
        self.data_unit()

        # Write CSV with common method
        glibc_version, libmem_version = self.get_version_strings()
        headers = ["Size", f"Glibc-{glibc_version}", f"LibMem-{libmem_version}", "GAINS"]
        data_rows = list(zip(self.size_unit, self.glibc_throughput_values, self.amd_throughput_values, self.gains))

        self.write_comparison_csv(f"{self.result_dir}/{self.bench_name}throughput_values.csv", headers, data_rows)
        self.print_result()

    def _run_comparison_performance(self):
        """Run comparison between old and new LibMem versions"""
        # Read the gbamd.txt file from old_perf_dir and new_perf_dir
        values = subprocess.run(["grep '_mean' gbamd.txt | grep -Eo '/[0-9]+_mean' | grep -Eo '[0-9]+'"], cwd=self.old_perf_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
        self.size_values = [int(val) for val in values]
        self.amd_throughput_old_values = subprocess.run(["grep '_mean' gbamd.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"], cwd=self.old_perf_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
        self.amd_throughput_new_values = subprocess.run(["grep '_mean' gbamd.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"], cwd=self.new_perf_dir, shell=True, capture_output=True, text=True).stdout.splitlines()

        self.throughput_converter(self.amd_throughput_old_values)
        self.throughput_converter(self.amd_throughput_new_values)

        self.gains = self.calculate_gains(self.amd_throughput_new_values, self.amd_throughput_old_values)

        # Converting sizes to B,KB,MB for reports
        self.data_unit()

        # Write CSV with common method
        headers = ["Size", "LibMem - OLD", "LibMem - NEW", "GAINS"]
        data_rows = list(zip(self.size_unit, self.amd_throughput_old_values, self.amd_throughput_new_values, self.gains))

        self.write_comparison_csv(f"{self.result_dir}/{self.bench_name}throughput_values.csv", headers, data_rows)
        self.print_result()

    def _run_libmem_performance(self):
        """Run LibMem-only performance analysis"""
        print("Performance analysis for AOCL-LibMem - "+str(self.func)+" for size range["+str(self.ranges[0])+"-"+str(self.ranges[1])+"] on "+str(self.bench_name))
        self.variant = "amd"

        if self.bestperf:
            print("\nRunning in best performance mode - will run AOCL-LibMem benchmark with 3 iterations...")
            size_values, self.amd_throughput_values = self.gbm_run()
            self.size_values = [int(val) for val in size_values]
        else:
            self.gbm_run()
            values = subprocess.run(["grep '_mean' gbamd.txt | grep -Eo '/[0-9]+_mean' | grep -Eo '[0-9]+'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            self.size_values = [int(val) for val in values]
            self.amd_throughput_values = subprocess.run(["grep '_mean' gbamd.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            self.throughput_converter(self.amd_throughput_values)

        self.data_unit()

        headers = ["Size", "Throughput"]
        data_rows = list(zip(self.size_unit, self.amd_throughput_values))

        self.write_comparison_csv(f"{self.result_dir}/perf_values.csv", headers, data_rows)
        self.print_result_perf()

    def _run_glibc_performance(self):
        """Run Glibc-only performance analysis"""
        print("Performance analysis for GLIBC - "+str(self.func)+" for size range["+str(self.ranges[0])+"-"+str(self.ranges[1])+"] on "+str(self.bench_name))
        self.variant = "glibc"

        if self.bestperf:
            print("\nRunning in best performance mode - will run Glibc benchmark with 3 iterations...")
            size_values, self.glibc_throughput_values = self.gbm_run()
            self.size_values = [int(val) for val in size_values]
        else:
            self.gbm_run()
            values = subprocess.run(["grep '_mean' gbglibc.txt | grep -Eo '/[0-9]+_mean' | grep -Eo '[0-9]+'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            self.size_values = [int(val) for val in values]
            self.glibc_throughput_values = subprocess.run(["grep '_mean' gbglibc.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            self.throughput_converter(self.glibc_throughput_values)

        self.data_unit()

        headers = ["Size", "Throughput"]
        data_rows = list(zip(self.size_unit, self.glibc_throughput_values))

        self.write_comparison_csv(f"{self.result_dir}/perf_values.csv", headers, data_rows)
        self.print_result_perf()

    def get_best_throughput_from_multiple_runs(self, variant, num_runs=3):
        """Run benchmark multiple times and return the best throughput for each size"""
        all_runs_data = []

        # Print which benchmark is running
        if variant == "amd":
            libmem_version = self.LibMemVersion.decode('utf-8').strip() if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
            print(f"\nRunning AOCL-LibMem {libmem_version} benchmark with {num_runs} iterations...")
        else:
            glibc_version = self.GlibcVersion.decode('utf-8').strip() if isinstance(self.GlibcVersion, bytes) else str(self.GlibcVersion).strip()
            print(f"\nRunning Glibc {glibc_version} benchmark with {num_runs} iterations...")

        for run_idx in range(num_runs):
            print(f"  Running iteration {run_idx + 1}/{num_runs}...")

            # Set up environment
            if variant == "amd":
                if not hasattr(self, 'LibMemVersion'):
                    self.LibMemVersion = subprocess.check_output("file " + LIBMEM_BIN_PATH + \
                        "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell=True)
                env['LD_PRELOAD'] = LIBMEM_BIN_PATH
            else:
                if not hasattr(self, 'GlibcVersion'):
                    self.GlibcVersion = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'", shell=True)
                env['LD_PRELOAD'] = ''

            # size zero will result in 0 throughput.
            ranges = self.ranges.copy()
            if (ranges[0] == 0):
                ranges[0] = 1

            # Run benchmark for this iteration
            output_file = f'{self.result_dir}/gb{variant}_run{run_idx}.txt'
            with open(output_file, 'w') as g:
                if self.preload == 'y':
                    subprocess.run(["taskset", "-c", str(self.core), "./googlebench", "--benchmark_repetitions="+str(self.repetitions), "--benchmark_min_warmup_time="+str(self.warm_up), "--benchmark_counters_tabular=true", str(self.func), str(self.memory_operation), str(ranges[0]), str(ranges[1]), str(self.iterator), str(self.align)], cwd=self.path, env=env, check=True, stdout=g, stderr=subprocess.PIPE)
                else:
                    subprocess.run(["taskset", "-c", str(self.core), "./googlebench"+"_"+variant, "--benchmark_repetitions="+str(self.repetitions), "--benchmark_min_warmup_time="+str(self.warm_up), "--benchmark_counters_tabular=true", str(self.func), str(self.memory_operation), str(ranges[0]), str(ranges[1]), str(self.iterator), str(self.align)], cwd=self.path, check=True, stdout=g, stderr=subprocess.PIPE)

            # Parse results from this run
            size_values = subprocess.run([f"grep '_mean' gb{variant}_run{run_idx}.txt | grep -Eo '/[0-9]+_mean' | grep -Eo '[0-9]+'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            throughput_values = subprocess.run([f"grep '_mean' gb{variant}_run{run_idx}.txt | grep -Eo '[0-9]+(\\.[0-9]+)?[GM]/s'"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()

            # Convert throughput values
            converted_throughput = throughput_values.copy()
            self.throughput_converter(converted_throughput)

            # Store this run's data
            run_data = {}
            for i, size in enumerate(size_values):
                if i < len(converted_throughput):
                    run_data[int(size)] = converted_throughput[i]
            all_runs_data.append(run_data)

        # Find best throughput for each size across all runs
        if not all_runs_data:
            return [], []

        all_sizes = list(all_runs_data[0].keys())
        best_throughputs = []

        for size in all_sizes:
            throughputs_for_size = [run_data.get(size, 0) for run_data in all_runs_data]
            best_throughput = max(throughputs_for_size)
            best_throughputs.append(best_throughput)

        # Create final output file with best results
        final_output = f'{self.result_dir}/gb{variant}.txt'
        with open(final_output, 'w') as f:
            f.write("Best performance results from 3 iterations:\n")
            for i, (size, throughput) in enumerate(zip(all_sizes, best_throughputs)):
                unit = "G/s" if throughput >= 1 else "M/s"
                display_throughput = throughput if throughput >= 1 else throughput * 1000
                f.write(f"/{size}_mean {display_throughput:.2f} {unit}\n")

        return [str(size) for size in all_sizes], best_throughputs

    def gbm_run(self):
        if self.bestperf:
            # Use best performance mode
            return self.get_best_throughput_from_multiple_runs(self.variant)

        if self.variant == "amd":
            self.LibMemVersion = subprocess.check_output("file " + LIBMEM_BIN_PATH + \
                "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell=True)
            env['LD_PRELOAD'] = LIBMEM_BIN_PATH
            libmem_version = self.LibMemVersion.decode('utf-8').strip() if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
            print("GBM : Running Benchmark on AOCL-LibMem "+libmem_version)
        else:
            self.GlibcVersion = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'", shell=True)
            env['LD_PRELOAD'] = ''
            glibc_version = self.GlibcVersion.decode('utf-8').strip() if isinstance(self.GlibcVersion, bytes) else str(self.GlibcVersion).strip()
            print("GBM : Running Benchmark on GLIBC "+glibc_version)

        # size zero will result in 0 throughput.
        if (self.ranges[0] == 0):
            self.ranges[0] = 1

        with open(self.result_dir+'/gb'+str(self.variant)+'.txt', 'w') as g:
            if self.preload == 'y':
                subprocess.run(["taskset", "-c", str(self.core), "./googlebench", "--benchmark_repetitions="+str(self.repetitions), "--benchmark_min_warmup_time="+str(self.warm_up), "--benchmark_counters_tabular=true", str(self.func), str(self.memory_operation), str(self.ranges[0]), str(self.ranges[1]), str(self.iterator), str(self.align)], cwd=self.path, env=env, check=True, stdout=g, stderr=subprocess.PIPE)
            else:
                subprocess.run(["taskset", "-c", str(self.core), "./googlebench"+"_"+self.variant, "--benchmark_repetitions="+str(self.repetitions), "--benchmark_min_warmup_time="+str(self.warm_up), "--benchmark_counters_tabular=true", str(self.func), str(self.memory_operation), str(self.ranges[0]), str(self.ranges[1]), str(self.iterator), str(self.align)], cwd=self.path, check=True, stdout=g, stderr=subprocess.PIPE)

    def print_result(self):
        """Print benchmark comparison results"""
        print("BENCHMARK: " + str(self.bench_name))
        print("    SIZE     : GAINS")
        print("    ----------------")
        for size, gain in zip(self.size_unit, self.gains):
            print(f"    {size:8} : {gain:>4}")
        print(f"*** Test reports copied to directory [{self.result_dir}] ***")

    def print_result_perf(self):
        """Print performance-only results"""
        print("BENCHMARK: " + str(self.bench_name))
        print("    SIZE     : THROUGHPUT")
        print("    ---------------------")
        throughput_values = getattr(self, 'amd_throughput_values', None) or getattr(self, 'glibc_throughput_values', None)
        if throughput_values:
            for size, throughput in zip(self.size_unit, throughput_values):
                unit = "G/s" if throughput >= 1 else "M/s"
                display_throughput = throughput if throughput >= 1 else throughput * 1000
                print(f"    {size:8} : {display_throughput:>7.2f} {unit}")
        print(f"*** Test reports copied to directory [{self.result_dir}] ***")
