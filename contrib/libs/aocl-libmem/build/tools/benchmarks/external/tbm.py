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
import sys  # Add sys import
from os import environ as env
from pandas import read_csv
import argparse
import re
import csv
import datetime
import filecmp
from statistics import mean
from libmem_defs import *
sys.path.insert(0, '../tools/benchmarks')
from bench import BaseBench

class TBM(BaseBench):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # TBM-specific attributes
        self.path = "../tools/benchmarks/external/tinybench"
        self.isExist = ""
        self.iterator = self.MYPARSER['ARGS']['iterator']

    def apply_tbm_patch(self):
        try:
            # Run patch command directly on the file
            result = subprocess.run(
                f"cd {self.path}/tinymembench && patch -p0 < tbm.patch",
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print("Patch applied successfully")
        except subprocess.CalledProcessError as e:
            print("Failed to apply patch:", e)
            print("Patch stderr:", e.stderr.decode())
            return False

        return True

    def __call__(self):
        self.isExist = os.path.exists(self.path + '/tinymembench')
        if (not self.isExist):
            print("Preparing Tinymem benchmark")
            subprocess.run(["git", "clone", "https://github.com/ssvb/tinymembench.git"], cwd=self.path)
            os.system("cp ../tools/benchmarks/external/tinybench/tbm.patch ../tools/benchmarks/external/tinybench/tinymembench/")
            # Apply patch to main.c, util.c, util.h and Makefiles
            if not self.apply_tbm_patch():
                print("Failed to patch TBM files")
                print("Please apply the tbm.patch manually and compile the tinymembench")
                print("Exiting...")
                sys.exit(1)

            try:
                # Run the command and capture the result
                result = subprocess.run(["make"], cwd=self.path + "/tinymembench", check=True)
            except subprocess.CalledProcessError as e:
                # Print the error message and exit the program if the command fails
                print(f"Command failed with return code {e.returncode}")
                sys.exit(1)
            print("prepared TINYMEMBENCH")

        if (self.perf == 'd'):
            self._run_default_performance()
        elif (self.perf == 'c'):
            self._run_comparison_performance()
        elif (self.perf == 'l'):
            self._run_libmem_performance()
        elif (self.perf == 'g'):
            self._run_glibc_performance()

    def _run_default_performance(self):
        """Run default performance analysis (Glibc vs LibMem)"""
        print("Benchmarking of "+str(self.func)+" for size range["+str(self.ranges[0])+"-"+str(self.ranges[1])+"] on "+str(self.bench_name))

        if self.bestperf:
            print("\nRunning in best performance mode - will run both Glibc and LibMem benchmarks with 3 iterations each...")
            self.variant = "glibc"
            glibc_size_values, self.glibc = self.tiny_run()
            self.variant = "amd"
            amd_size_values, self.amd = self.tiny_run()
            self.size_values = glibc_size_values
        else:
            self.variant = "glibc"
            self.tiny_run()
            self.variant = "amd"
            self.tiny_run()

            with open(self.result_dir + "/amd.txt", "r") as input_file:
                self.data = input_file.read()
            self.size_values = subprocess.run(["sed", "-n", r"s/SIZE: \([0-9]*\) B.*/\1/p", "amd.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines()

            # Extract the throughput values using the sed command
            self.amd_throughput_values = subprocess.run(["sed", "-n", r"s/.*:\s*\([0-9.]*\) MB\/s.*/\1/p", "amd.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines()
            self.glibc_throughput_values = subprocess.run(["sed", "-n", r"s/.*:\s*\([0-9.]*\) MB\/s.*/\1/p", "glibc.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines()

            self.amd = [eval(i) / 1000 for i in self.amd_throughput_values]
            self.glibc = [eval(i) / 1000 for i in self.glibc_throughput_values]

        self.gains = self.calculate_gains(self.amd, self.glibc)

        # Converting sizes to B,KB,MB for reports
        self.data_unit()

        # Write CSV with common method
        glibc_version, libmem_version = self.get_version_strings()
        headers = ["Size", f"Glibc-{glibc_version}", f"LibMem-{libmem_version}", "GAINS"]
        data_rows = list(zip(self.size_unit, self.glibc, self.amd, self.gains))

        self.write_comparison_csv(f"{self.result_dir}/{self.bench_name}throughput_values.csv", headers, data_rows)
        self.print_result()

    def _run_comparison_performance(self):
        """Run comparison between old and new LibMem versions"""
        # Read the amd.txt file from old_perf_dir and new_perf_dir
        self.size_values = subprocess.run(["sed", "-n", r"s/SIZE: \([0-9]*\) B.*/\1/p", "amd.txt"], cwd=self.old_perf_dir, capture_output=True, text=True).stdout.splitlines()
        self.amd_throughput_old_values = subprocess.run(["sed", "-n", r"s/.*:\s*\([0-9.]*\) MB\/s.*/\1/p", "amd.txt"], cwd=self.old_perf_dir, capture_output=True, text=True).stdout.splitlines()
        self.amd_throughput_new_values = subprocess.run(["sed", "-n", r"s/.*:\s*\([0-9.]*\) MB\/s.*/\1/p", "amd.txt"], cwd=self.new_perf_dir, capture_output=True, text=True).stdout.splitlines()

        self.amd_old = [eval(i) / 1000 for i in self.amd_throughput_old_values]
        self.amd_new = [eval(i) / 1000 for i in self.amd_throughput_new_values]

        self.gains = self.calculate_gains(self.amd_new, self.amd_old)

        # Converting sizes to B,KB,MB for reports
        self.data_unit()

        headers = ["Size", "LibMem - OLD", "LibMem - NEW", "GAINS"]
        data_rows = list(zip(self.size_unit, self.amd_old, self.amd_new, self.gains))

        self.write_comparison_csv(f"{self.result_dir}/{self.bench_name}throughput_values.csv", headers, data_rows)
        self.print_result()

    def _run_libmem_performance(self):
        """Run LibMem-only performance analysis"""
        print("Performance analysis for AOCL-LibMem - "+str(self.func)+" for size range["+str(self.ranges[0])+"-"+str(self.ranges[1])+"] on "+str(self.bench_name))
        self.variant = "amd"

        if self.bestperf:
            print("Running in best performance mode (3 iterations)...")
            size_values, self.amd = self.tiny_run()
            self.size_values = size_values
        else:
            self.tiny_run()
            self.size_values = subprocess.run(["sed", "-n", r"s/SIZE: \([0-9]*\) B.*/\1/p", "amd.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines()
            self.amd_throughput_values = subprocess.run(["sed", "-n", r"s/.*:\s*\([0-9.]*\) MB\/s.*/\1/p", "amd.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines()
            self.amd = [eval(i) / 1000 for i in self.amd_throughput_values]

        self.data_unit()

        headers = ["Size", "Throughput"]
        data_rows = list(zip(self.size_unit, self.amd))

        self.write_comparison_csv(f"{self.result_dir}/perf_values.csv", headers, data_rows)
        self.print_result_perf()

    def _run_glibc_performance(self):
        """Run Glibc-only performance analysis"""
        print("Performance analysis for GLIBC - "+str(self.func)+" for size range["+str(self.ranges[0])+"-"+str(self.ranges[1])+"] on "+str(self.bench_name))
        self.variant = "glibc"

        if self.bestperf:
            print("Running in best performance mode (3 iterations)...")
            size_values, self.glibc = self.tiny_run()
            self.size_values = size_values
        else:
            self.tiny_run()
            self.size_values = subprocess.run(["sed", "-n", r"s/SIZE: \([0-9]*\) B.*/\1/p", "glibc.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines()
            self.glibc_throughput_values = subprocess.run(["sed", "-n", r"s/.*:\s*\([0-9.]*\) MB\/s.*/\1/p", "glibc.txt"], cwd=self.result_dir, capture_output=True, text=True).stdout.splitlines()
            self.glibc = [eval(i) / 1000 for i in self.glibc_throughput_values]

        self.data_unit()

        headers = ["Size", "Throughput"]
        data_rows = list(zip(self.size_unit, self.glibc))

        self.write_comparison_csv(f"{self.result_dir}/perf_values.csv", headers, data_rows)
        self.print_result_perf()

    def get_best_throughput_from_multiple_runs_tbm(self, variant, num_runs=3):
        """Run TBM benchmark multiple times and return the best throughput for each size"""
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

            # Run benchmark for this iteration
            output_file = f'{self.result_dir}/{variant}_run{run_idx}.txt'
            with open(output_file, 'w') as g:
                subprocess.run(["taskset", "-c", str(self.core), "./tinymembench", str(self.func), str(self.ranges[0]), str(self.ranges[1]), str(self.iterator)], cwd=self.path + "/tinymembench", env=env, check=True, stdout=g, stderr=subprocess.PIPE)

            # Parse results from this run
            size_values = subprocess.run([f"sed -n 's/SIZE: \\([0-9]*\\) B.*/\\1/p' {variant}_run{run_idx}.txt"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()
            throughput_values = subprocess.run([f"sed -n 's/.*:\\s*\\([0-9.]*\\) MB\\/s.*/\\1/p' {variant}_run{run_idx}.txt"], cwd=self.result_dir, shell=True, capture_output=True, text=True).stdout.splitlines()

            # Convert to G/s and store this run's data
            run_data = {}
            for i, size in enumerate(size_values):
                if i < len(throughput_values):
                    throughput_gbps = float(throughput_values[i]) / 1000
                    run_data[int(size)] = throughput_gbps
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
        final_output = f'{self.result_dir}/{variant}.txt'
        with open(final_output, 'w') as f:
            f.write("Best performance results from 3 iterations:\n")
            for i, (size, throughput) in enumerate(zip(all_sizes, best_throughputs)):
                f.write(f"SIZE: {size} B\n")
                f.write(f"{self.func}: {throughput * 1000:.2f} MB/s\n")

        return [str(size) for size in all_sizes], best_throughputs

    def tiny_run(self):
        """Run TinyMemBench benchmark"""
        if self.bestperf:
            # Use best performance mode
            return self.get_best_throughput_from_multiple_runs_tbm(self.variant)

        if self.variant == "amd":
            self.LibMemVersion = subprocess.check_output("file " + LIBMEM_BIN_PATH + \
                "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell=True)
            env['LD_PRELOAD'] = LIBMEM_BIN_PATH
            libmem_version = self.LibMemVersion.decode('utf-8').strip() if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
            print("TBM : Running Benchmark on AOCL-LibMem "+libmem_version)
        else:
            self.GlibcVersion = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'", shell=True)
            env['LD_PRELOAD'] = ''
            glibc_version = self.GlibcVersion.decode('utf-8').strip() if isinstance(self.GlibcVersion, bytes) else str(self.GlibcVersion).strip()
            print("TBM : Running Benchmark on GLIBC "+glibc_version)

        with open(self.result_dir + '/' + str(self.variant) + '.txt', 'w') as g:
            subprocess.run(["taskset", "-c", str(self.core), "./tinymembench", str(self.func), str(self.ranges[0]), str(self.ranges[1]), str(self.iterator)], cwd=self.path + "/tinymembench", env=env, check=True, stdout=g, stderr=subprocess.PIPE)

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
        throughput_values = getattr(self, 'amd', None) or getattr(self, 'glibc', None)
        if throughput_values:
            for size, throughput in zip(self.size_unit, throughput_values):
                unit = "G/s" if throughput >= 1 else "M/s"
                display_throughput = throughput if throughput >= 1 else throughput * 1000
                print(f"    {size:8} : {display_throughput:>7.2f} {unit}")
        print(f"*** Test reports copied to directory [{self.result_dir}] ***")
