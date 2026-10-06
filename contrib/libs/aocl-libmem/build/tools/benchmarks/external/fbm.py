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


class FBM(BaseBench):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # FBM-specific attributes
        self.path = "../tools/benchmarks/external/"
        self.isExist = ""
        self.last = 9
        self.allocator = self.MYPARSER['ARGS']['mem_alloc']
        self.repetitions = self.MYPARSER['ARGS']['repetitions']
        self.amd_throughput_value = []
        self.glibc_throughput_value = []
        self.amd_raw = []
        self.glibc_raw = []
        self.amd_old_raw = []
        self.amd_new_raw = []
        self.data = ["Google A", "Google B", "Google D",
                   "Google L", "Google M", "Google Q",
                   "Google S", "Google U", "Google W"]

    def __call__(self):
        # Check if bestperf option was used and show warning
        if self.MYPARSER['ARGS'].get('best_performance', False):
            print("\n" + "="*80)
            print("WARNING: FleetBench (fbm) does not support the -bestperf option.")
            print("The benchmark will run with default behavior.")
            print("Use -bestperf with 'gbm' or 'tbm' for best performance mode.")
            print("="*80 + "\n")

        # Check if bazel is available
        try:
            subprocess.run(["bazel", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Error: Bazel is not installed or not available in PATH")
            print("Please install bazel to use FleetBench (fbm) benchmark")
            print("Installation instructions: https://bazel.build/install")
            return False

        self.isExist = os.path.exists(self.path + '/fleetbench')
        if (not self.isExist):
            print("Preparing Fleetbench benchmark")
            try:
                subprocess.run(["git", "clone", "-c", "advice.detachedHead=false", "-b", "v0.2", "https://github.com/google/fleetbench.git"], cwd=self.path, check=True)
                print("FleetBench repository cloned successfully")
                subprocess.run(["bazel", "build", "-c", "opt", "//fleetbench/libc:mem_benchmark", "--cxxopt=-Wno-deprecated-declarations", "--cxxopt=-Wno-unused-variable", "--cxxopt=-Wno-changes-meaning"], cwd=self.path + "/fleetbench", check=True)
                print("FleetBench built successfully")
            except subprocess.CalledProcessError as e:
                print(f"Error during FleetBench setup: {e}")
                print("Please ensure bazel is installed and working properly")
                return False

        # Verify the executable exists
        executable_path = self.path + "/fleetbench/bazel-bin/fleetbench/libc/mem_benchmark"
        if not os.path.exists(executable_path):
            print(f"Error: FleetBench executable not found at {executable_path}")
            print("Attempting to rebuild FleetBench...")
            try:
                subprocess.run(["bazel", "build", "-c", "opt", "//fleetbench/libc:mem_benchmark", "--cxxopt=-Wno-deprecated-declarations", "--cxxopt=-Wno-unused-variable", "--cxxopt=-Wno-changes-meaning"], cwd=self.path + "/fleetbench", check=True)
                print("FleetBench rebuilt successfully")
                if not os.path.exists(executable_path):
                    print(f"Error: FleetBench executable still not found after rebuild")
                    return False
            except subprocess.CalledProcessError as e:
                print(f"Error rebuilding FleetBench: {e}")
                return False

        if (self.perf == 'd'):
            self._run_default_performance()
        elif (self.perf == 'c'):
            self._run_comparison_performance()
        elif (self.perf == 'l'):
            self._run_libmem_performance()
        elif (self.perf == 'g'):
            self._run_glibc_performance()

        return True

    def _run_default_performance(self):
        """Run default performance analysis (Glibc vs LibMem)"""
        print("Benchmarking of "+str(self.func)+" for DATA Distributions["+str(self.data[0])+"-"+str(self.data[-1])+"] on "+str(self.bench_name))

        self.variant = "glibc"
        self.fbm_run()
        self.variant = "amd"
        self.fbm_run()

        for i in range(0, self.last):
            self.glibc_raw.append(subprocess.run(["sed", "-n", r"/mean/{s/.*bytes_per_second=\([^ ]*\).*/\1/p;q}", "fb_"+str(i)+"glibc.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines())
            self.amd_raw.append(subprocess.run(["sed", "-n", r"/mean/{s/.*bytes_per_second=\([^ ]*\).*/\1/p;q}", "fb_"+str(i)+"amd.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines())

        self.amd_raw = [item[0] for item in self.amd_raw]
        self.glibc_raw = [item[0] for item in self.glibc_raw]

        # Convert throughput values using base class method
        self.amd_throughput_value = self._convert_throughput_fbm(self.amd_raw)
        self.glibc_throughput_value = self._convert_throughput_fbm(self.glibc_raw)

        self.gains = self.calculate_gains(self.amd_throughput_value, self.glibc_throughput_value)

        # Write CSV with common method
        glibc_version, libmem_version = self.get_version_strings()
        headers = ["Size", f"Glibc-{glibc_version}", f"LibMem-{libmem_version}", "GAINS"]
        data_rows = list(zip(self.data, self.glibc_throughput_value, self.amd_throughput_value, self.gains))

        self.write_comparison_csv(f"{self.result_dir}/{self.bench_name}throughput_values.csv", headers, data_rows)
        self.print_result()

    def _run_comparison_performance(self):
        """Run comparison between old and new LibMem versions"""
        # Read the fb_amd.txt files from old_perf_dir and new_perf_dir
        for i in range(0, self.last):
            self.amd_old_raw.append(subprocess.run(["sed", "-n", r"/mean/{s/.*bytes_per_second=\([^ ]*\).*/\1/p;q}", "fb_"+str(i)+"amd.txt"], cwd=self.old_perf_dir, capture_output=True, text=True).stdout.splitlines())
            self.amd_new_raw.append(subprocess.run(["sed", "-n", r"/mean/{s/.*bytes_per_second=\([^ ]*\).*/\1/p;q}", "fb_"+str(i)+"amd.txt"], cwd=self.new_perf_dir, capture_output=True, text=True).stdout.splitlines())

        self.amd_old_raw = [item[0] for item in self.amd_old_raw]
        self.amd_new_raw = [item[0] for item in self.amd_new_raw]

        # Convert throughput values
        self.amd_old_throughput = self._convert_throughput_fbm(self.amd_old_raw)
        self.amd_new_throughput = self._convert_throughput_fbm(self.amd_new_raw)

        self.gains = self.calculate_gains(self.amd_new_throughput, self.amd_old_throughput)

        # Write CSV with common method
        headers = ["Size", "LibMem - OLD", "LibMem - NEW", "GAINS"]
        data_rows = list(zip(self.data, self.amd_old_throughput, self.amd_new_throughput, self.gains))

        self.write_comparison_csv(f"{self.result_dir}/{self.bench_name}throughput_values.csv", headers, data_rows)
        self.print_result()

    def _run_libmem_performance(self):
        """Run LibMem-only performance analysis"""
        print("Performance analysis for AOCL-LibMem - "+str(self.func)+" for DATA Distributions["+str(self.data[0])+"-"+str(self.data[-1])+"] on "+str(self.bench_name))
        self.variant = "amd"
        self.fbm_run()

        for i in range(0, self.last):
            self.amd_raw.append(subprocess.run(["sed", "-n", r"/mean/{s/.*bytes_per_second=\([^ ]*\).*/\1/p;q}", "fb_"+str(i)+"amd.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines())

        self.amd_raw.append(subprocess.run(["sed", "-n", r"/mean/{s/.*bytes_per_second=\([^ ]*\).*/\1/p;q}", "fb_"+str(i)+"amd.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines())
        self.amd_raw = [item[0] for item in self.amd_raw]

        # Convert throughput values
        self.amd_throughput_value = self._convert_throughput_fbm(self.amd_raw)

        headers = ["Size", "Throughput"]
        data_rows = list(zip(self.data, self.amd_throughput_value))

        self.write_comparison_csv(f"{self.result_dir}/perf_values.csv", headers, data_rows)
        self.print_result_perf()

    def _run_glibc_performance(self):
        """Run Glibc-only performance analysis"""
        print("Performance analysis for GLIBC - "+str(self.func)+" for DATA Distributions["+str(self.data[0])+"-"+str(self.data[-1])+"] on "+str(self.bench_name))
        self.variant = "glibc"
        self.fbm_run()

        for i in range(0, self.last):
            self.glibc_raw.append(subprocess.run(["sed", "-n", r"/mean/{s/.*bytes_per_second=\([^ ]*\).*/\1/p;q}", "fb_"+str(i)+"glibc.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines())

        self.glibc_raw = [item[0] for item in self.glibc_raw]

        # Convert throughput values
        self.glibc_throughput_value = self._convert_throughput_fbm(self.glibc_raw)

        headers = ["Size", "Throughput"]
        data_rows = list(zip(self.data, self.glibc_throughput_value))

        self.write_comparison_csv(f"{self.result_dir}/perf_values.csv", headers, data_rows)
        self.print_result_perf()

    def _convert_throughput_fbm(self, value):
        """Convert FleetBench throughput values to consistent G/s format"""
        converted_values = []
        for val in value:
            if 'G/s' in val:
                # Already in G/s, just extract the numeric value
                numeric_val = float(val.replace('G/s', ''))
                converted_values.append(numeric_val)
            else:
                # Assume M/s, convert to G/s
                numeric_val = float(val.replace('M/s', ''))
                converted_values.append(numeric_val / 1000)
        return converted_values

    def fbm_run(self):
        """Run FleetBench benchmark"""
        if self.variant == "amd":
            self.LibMemVersion = subprocess.check_output("file " + LIBMEM_BIN_PATH + \
                "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell=True)
            env['LD_PRELOAD'] = LIBMEM_BIN_PATH
            libmem_version = self.LibMemVersion.decode('utf-8').strip() if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
            print("FBM : Running Benchmark on AOCL-LibMem "+libmem_version)
        else:
            self.GlibcVersion = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'", shell=True)
            env.pop('LD_PRELOAD', None)
            glibc_version = self.GlibcVersion.decode('utf-8').strip() if isinstance(self.GlibcVersion, bytes) else str(self.GlibcVersion).strip()
            print("FBM : Running Benchmark on GLIBC "+glibc_version)

        for i in range(0, self.last):
            with open(self.result_dir + '/fb_' + str(i) + str(self.variant) + '.txt', 'w') as g:
                if self.allocator == 'tcmalloc':
                    cmd = ["taskset", "-c", str(self.core), "./bazel-bin/fleetbench/libc/mem_benchmark", "--benchmark_min_time=0.5", "--benchmark_repetitions="+str(self.repetitions), "--benchmark_display_aggregates_only=true", "--benchmark_filter=" + str(self.func) + ".*" + str(i)]
                else:
                    env["TCMALLOC_LARGE_ALLOC_REPORT_THRESHOLD"] = str(10737418240)
                    cmd = ["taskset", "-c", str(self.core), "./bazel-bin/fleetbench/libc/mem_benchmark", "--benchmark_min_time=0.5", "--benchmark_repetitions="+str(self.repetitions), "--benchmark_display_aggregates_only=true", "--benchmark_filter=" + str(self.func) + ".*" + str(i)]

                try:
                    subprocess.run(cmd, cwd=self.path + "/fleetbench", env=env, check=True, stdout=g, stderr=subprocess.PIPE)
                except subprocess.CalledProcessError as e:
                    print(f"Error running FleetBench command: {e}")
                    print(f"Command: {' '.join(cmd)}")
                    print(f"Working directory: {self.path}/fleetbench")
                    # Check if executable exists
                    executable_path = os.path.join(self.path, "fleetbench", "bazel-bin", "fleetbench", "libc", "mem_benchmark")
                    if not os.path.exists(executable_path):
                        print(f"Executable not found at: {executable_path}")
                        print("Available files in bazel-bin directory:")
                        bazel_bin_path = os.path.join(self.path, "fleetbench", "bazel-bin")
                        if os.path.exists(bazel_bin_path):
                            for root, dirs, files in os.walk(bazel_bin_path):
                                for file in files:
                                    if "mem_benchmark" in file:
                                        print(f"  Found: {os.path.join(root, file)}")
                    raise

    def print_result(self):
        """Print benchmark comparison results"""
        print("BENCHMARK: " + str(self.bench_name))
        print("  Distributions : GAINS")
        print("    ----------------")
        for size, gain in zip(self.data, self.gains):
            print(f"    {size:8} : {gain:>4}")
        print(f"*** Test reports copied to directory [{self.result_dir}] ***")

    def print_result_perf(self):
        """Print performance-only results"""
        print("BENCHMARK: " + str(self.bench_name))
        print("  Distributions : THROUGHPUT")
        print("    ---------------------")
        throughput_values = getattr(self, 'amd_throughput_value', None) or getattr(self, 'glibc_throughput_value', None)
        if throughput_values:
            for size, throughput in zip(self.data, throughput_values):
                print(f"    {size:8} : {throughput:>10}")
        print(f"*** Test reports copied to directory [{self.result_dir}] ***")

