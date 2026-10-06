"""
 Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.

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
import re
import csv
import sys
sys.path.insert(0, '../tools/benchmark')
from bench import BaseBench, _detect_l3_cache_bytes

# Functions supported by FleetBench libc benchmarks
FBM_SUPPORTED_FUNCS = ['memcpy', 'memmove', 'memset', 'memcmp']

# mem_benchmark asserts 2 * L3 <= kMixedBenchmarkBufferSize (1 GiB), so cap L3 at 512 MiB.
_FBM_MIXED_BUFFER_BYTES = 1024 * 1024 * 1024   # kMixedBenchmarkBufferSize
_FBM_MAX_L3_BYTES = _FBM_MIXED_BUFFER_BYTES // 2

# Version configurations for each FBM mode
FBM_VERSIONS = {
    'c': {
        'name': 'Cached (v0.2)',
        'tag': 'v0.2',
        'dir_name': 'fleetbench-v0.2',
        'repo_url': 'https://github.com/google/fleetbench.git',
        'workloads': ['0', '1', '2', '3', '4', '5', '6', '7', '8'],
        'workload_labels': {
            '0': 'Google_A', '1': 'Google_B', '2': 'Google_D',
            '3': 'Google_L', '4': 'Google_M', '5': 'Google_Q',
            '6': 'Google_S', '7': 'Google_U', '8': 'Google_W',
        },
        'cache_levels': [None],
        'throughput_unit': 'G/s',
        'bazel_extra_flags': [
            '--cxxopt=-Wno-deprecated-declarations',
            '--cxxopt=-Wno-unused-variable',
            '--cxxopt=-Wno-changes-meaning',
        ],
    },
    'm': {
        'name': 'Multi-cache (v0.3.3)',
        'tag': 'v0.3.3',
        'dir_name': 'fleetbench-v0.3.3',
        'repo_url': 'https://github.com/google/fleetbench.git',
        'workloads': ['0', '1', '2', '3', '4', '5', '6', '7', '8', 'Fleet'],
        'workload_labels': {
            '0': 'Dist_0', '1': 'Dist_1', '2': 'Dist_2',
            '3': 'Dist_3', '4': 'Dist_4', '5': 'Dist_5',
            '6': 'Dist_6', '7': 'Dist_7', '8': 'Dist_8',
            'Fleet': 'Fleet',
        },
        'cache_levels': ['L1', 'L2', 'LLC', 'Cold'],
        'throughput_unit': 'Gi/s',
        'bazel_extra_flags': [],
    },
    'a': {
        'name': 'Alignment (latest)',
        'tag': None,
        'commit': '26a70e275434c4e1173bc195c084a24a975e9555',
        'dir_name': 'fleetbench-latest',
        'repo_url': 'https://github.com/google/fleetbench.git',
        'workloads': ['0', '1', '2', '3', '4', '5', '6', '7', '8', 'Fleet'],
        'workload_labels': {
            '0': 'Dist_0', '1': 'Dist_1', '2': 'Dist_2',
            '3': 'Dist_3', '4': 'Dist_4', '5': 'Dist_5',
            '6': 'Dist_6', '7': 'Dist_7', '8': 'Dist_8',
            'Fleet': 'Fleet',
        },
        'cache_levels': ['L1', 'L2', 'LLC', 'Cold', 'Mixed'],
        'throughput_unit': 'Gi/s',
        'bazel_extra_flags': [
            '--custom_malloc=@bazel_tools//tools/cpp:malloc',
        ],
    },
}


class FBM(BaseBench):
    """FleetBench Memory Benchmark wrapper supporting three versions:
       -c (Cached/v0.2), -m (Multi-cache/v0.3.3), -a (Alignment/latest)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.path = "../tools/benchmark/external/"
        self.fbm_mode = self.MYPARSER['ARGS'].get('fbm_mode', 'a')
        self.workload = self.MYPARSER['ARGS'].get('workload', None)
        self.allocator = self.MYPARSER['ARGS'].get('mem_alloc', 'glibc')
        self.repetitions = self.MYPARSER['ARGS'].get('repetitions', 10)
        self.enable_aslr = self.MYPARSER['ARGS'].get('enable_aslr', False)

        # Load version configuration
        self.version = FBM_VERSIONS[self.fbm_mode]
        self.fleet_dir = os.path.abspath(os.path.join(self.path, self.version["dir_name"]))
        self.executable = os.path.abspath(os.path.join(
            self.fleet_dir, 'bazel-bin/fleetbench/libc/mem_benchmark'))

        # Determine workloads to run
        if self.workload is not None:
            wl_key = ('Fleet' if self.workload == 'fleet'
                      else self.workload)
            if wl_key not in self.version['workloads']:
                print(f"ERROR: Workload '{self.workload}' not available "
                      f"in mode '-{self.fbm_mode}'.")
                print(f"  Valid workloads: {self.version['workloads']}")
                self.workloads = []
            else:
                self.workloads = [wl_key]
        else:
            self.workloads = self.version['workloads']

        self.workload_labels = self.version['workload_labels']
        self.cache_levels = self.version['cache_levels']

    def __call__(self):
        # Validate function is supported
        if self.func not in FBM_SUPPORTED_FUNCS:
            print(f"ERROR: FleetBench only supports: {FBM_SUPPORTED_FUNCS}")
            print(f"  '{self.func}' is not supported.")
            return False

        if not self.workloads:
            return False

        # Warn about bestperf
        if self.bestperf:
            print("\nWARNING: FleetBench (fbm) does not support "
                  "-bestperf option.")

        # Check bazel
        if not self._check_bazel():
            return False

        # Clone and build if needed
        if not self._ensure_built():
            return False

        # Dispatch based on perf mode
        if self.perf == 'd':
            self._run_default_performance()
        elif self.perf == 'l':
            self._run_libmem_performance()
        elif self.perf == 'g':
            self._run_glibc_performance()
        elif self.perf == 'c':
            self._run_comparison_performance()

        return True

    # ------------------------------------------------------------------
    # Setup: Bazel check, clone, build
    # ------------------------------------------------------------------

    def _check_bazel(self):
        """Verify Bazel is available and version < 8."""
        try:
            result = subprocess.run(
                ["bazel", "--version"],
                capture_output=True, text=True, check=True)
            ver = re.search(r'bazel (\d+)', result.stdout)
            if ver and int(ver.group(1)) >= 8:
                print("ERROR: Bazel 8+ is not compatible with FleetBench.")
                print("  Use Bazel 7.x  "
                      "(export USE_BAZEL_VERSION=7.4.1)")
                return False
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("ERROR: Bazel not found. Please install Bazel 7.x.")
            return False

    def _ensure_built(self):
        """Clone and build FleetBench if the executable doesn't exist."""
        if not os.path.isdir(self.fleet_dir):
            print(f"\nCloning FleetBench {self.version['name']} ...")
            clone_cmd = ["git", "clone",
                         "-c", "advice.detachedHead=false"]
            if self.version['tag']:
                clone_cmd += ["-b", self.version['tag']]
            clone_cmd += [self.version['repo_url'], self.fleet_dir]
            try:
                subprocess.run(clone_cmd, check=True)
            except subprocess.CalledProcessError as e:
                print(f"  Clone failed: {e}")
                return False

            # Checkout specific commit if configured
            if self.version.get('commit'):
                try:
                    subprocess.run(
                        ["git", "checkout", self.version['commit']],
                        cwd=self.fleet_dir, check=True)
                except subprocess.CalledProcessError as e:
                    print(f"  Checkout failed: {e}")
                    return False

            # Patch fleetbench-latest for GCC compatibility
            # GCC cannot handle '#pragma GCC unroll' with template parameters
            if self.fbm_mode == 'a':
                self._patch_gcc_unroll()

        if not os.path.isfile(self.executable):
            print(f"\nBuilding FleetBench {self.version['name']} ...")
            build_cmd = (
                ["bazel", "build", "-c", "opt",
                 "//fleetbench/libc:mem_benchmark"]
                + self.version['bazel_extra_flags']
            )
            try:
                subprocess.run(build_cmd, cwd=self.fleet_dir, check=True)
            except subprocess.CalledProcessError as e:
                print(f"  Build failed: {e}")
                return False

        if not os.path.isfile(self.executable):
            print(f"ERROR: Executable not found: {self.executable}")
            return False

        return True

    # ------------------------------------------------------------------
    # Filter construction
    # ------------------------------------------------------------------

    def _patch_gcc_unroll(self):
        """Patch common.h to disable #pragma GCC unroll for GCC.
        GCC does not support unroll with template parameters (kRep)."""
        common_h = os.path.join(self.fleet_dir,
                                'fleetbench/common/common.h')
        if not os.path.isfile(common_h):
            return
        with open(common_h, 'r') as f:
            src = f.read()
        old_line = '#define FLEETBENCH_UNROLL_LOOP(x) FLEETBENCH_PRAGMA(GCC unroll x)'
        new_line = '#define FLEETBENCH_UNROLL_LOOP(x) // GCC: unroll disabled (template param unsupported)'
        if old_line in src:
            src = src.replace(old_line, new_line)
            with open(common_h, 'w') as f:
                f.write(src)
            print("  Patched common.h for GCC compatibility.")

    def _func_filter_name(self):
        """Return function name as used in benchmark filter strings."""
        if self.fbm_mode == 'c':
            return self.func              # lowercase: memcpy
        return self.func.capitalize()     # Memcpy

    def _build_filter(self, workload):
        """Build --benchmark_filter value for one workload."""
        fn = self._func_filter_name()
        if self.fbm_mode == 'c':
            return f"BM_Memory/{fn}/{workload}"
        elif self.fbm_mode == 'm':
            return f"BM_{fn}_{workload}_"
        else:  # 'a'
            return f"BM_LIBC_{fn}_{workload}_"

    def _cache_size_flags(self):
        """Pin --L3_size to the per-CCX L3 for LLC-tier modes.

        FleetBench's auto-detected per-socket L3 overshoots the int32 limit in
        mem_benchmark on large-cache/SMT-off parts and aborts. The per-CCX
        value matches GBM/TBM and the NT thresholds. v0.2 'cached' has no LLC
        tier, so nothing is injected there.
        """
        if 'LLC' not in self.cache_levels:
            return []
        l3_bytes = _detect_l3_cache_bytes()
        if not l3_bytes:
            return []
        # Stay within mem_benchmark's mixed-buffer assert (2 * L3 <= 1 GiB).
        l3_bytes = min(l3_bytes, _FBM_MAX_L3_BYTES)
        return [f"--L3_size={l3_bytes}"]

    # ------------------------------------------------------------------
    # Benchmark execution
    # ------------------------------------------------------------------

    def _run_benchmark(self, variant):
        """Run the benchmark for all selected workloads.

        Args:
            variant: 'amd' (LD_PRELOAD libmem) or 'glibc' (system).

        Returns:
            dict  {(workload, cache_level): throughput_float}
        """
        if variant == "amd":
            try:
                from libmem_defs import LIBMEM_BIN_PATH
                self.LibMemVersion = subprocess.check_output(
                    "file " + LIBMEM_BIN_PATH +
                    "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'",
                    shell=True)
                env['LD_PRELOAD'] = LIBMEM_BIN_PATH
                print(f"  LD_PRELOAD={LIBMEM_BIN_PATH}")
            except ImportError:
                print("ERROR: libmem_defs not found. "
                      "Build aocl-libmem first.")
                return {}
        else:
            self.GlibcVersion = subprocess.check_output(
                "ldd --version | awk '/ldd/{print $NF}'", shell=True)
            env.pop('LD_PRELOAD', None)

        all_results = {}
        cache_flags = self._cache_size_flags()
        if cache_flags:
            print(f"  cache override: {' '.join(cache_flags)}")
        for wl in self.workloads:
            bm_filter = self._build_filter(wl)
            cmd = ["taskset", "-c", str(self.core)]
            if not self.enable_aslr:
                # Disable ASLR for run-to-run consistency.
                cmd += ["setarch", os.uname().machine, "-R"]
            cmd += [
                self.executable,
                "--benchmark_min_time=1.0",
                "--benchmark_min_warmup_time=0.5",
                f"--benchmark_repetitions={self.repetitions}",
                "--benchmark_display_aggregates_only=true",
                f"--benchmark_filter={bm_filter}",
            ]
            cmd += cache_flags

            if self.allocator != 'tcmalloc':
                env["TCMALLOC_LARGE_ALLOC_REPORT_THRESHOLD"] = \
                    str(10737418240)

            label = self.workload_labels.get(wl, wl)
            print(f"    [{variant}] {self.func} workload={label} ...")

            try:
                result = subprocess.run(
                    cmd, cwd=self.fleet_dir, env=env,
                    capture_output=True, text=True, check=True)
                parsed = self._parse_output(result.stdout, wl)
                all_results.update(parsed)
            except subprocess.CalledProcessError as e:
                print(f"    ERROR workload {wl}: "
                      f"{e.stderr[:200] if e.stderr else e}")

        # Clean up LD_PRELOAD
        env.pop('LD_PRELOAD', None)

        # Persist raw results
        self._save_results(all_results, variant)
        return all_results

    # ------------------------------------------------------------------
    # Output parsing
    # ------------------------------------------------------------------

    def _parse_output(self, output, workload):
        """Extract median throughput from benchmark stdout.

        Returns dict  {(workload, cache_level): float}
        """
        results = {}
        for line in output.splitlines():
            if '_median' not in line or 'bytes_per_second=' not in line:
                continue

            bps = re.search(r'bytes_per_second=(\S+)', line)
            if not bps:
                continue
            throughput = self._convert_throughput(bps.group(1))

            if self.fbm_mode == 'c':
                results[(workload, None)] = throughput
            else:
                # Extract cache level from benchmark name
                # e.g. BM_Memcpy_0_L1_median
                name_m = re.match(r'(\S+)_median', line)
                if name_m:
                    cache = name_m.group(1).rsplit('_', 1)[-1]
                    results[(workload, cache)] = throughput

        return results

    @staticmethod
    def _convert_throughput(val):
        """Convert throughput string like '54.44Gi/s' to float."""
        if 'Gi/s' in val:
            return float(val.replace('Gi/s', ''))
        elif 'G/s' in val:
            return float(val.replace('G/s', ''))
        elif 'Mi/s' in val:
            return float(val.replace('Mi/s', '')) / 1024.0
        elif 'M/s' in val:
            return float(val.replace('M/s', '')) / 1000.0
        try:
            return float(val)
        except ValueError:
            return 0.0

    # ------------------------------------------------------------------
    # Result persistence
    # ------------------------------------------------------------------

    def _save_results(self, results, variant):
        """Save results to CSV for later comparison (-perf c)."""
        filepath = os.path.join(self.result_dir,
                                f"results_{variant}.csv")
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['workload', 'cache', 'throughput'])
            for (wl, cache), tp in sorted(results.items()):
                writer.writerow([wl, cache if cache else '', tp])

    def _load_results(self, dirpath, variant):
        """Load previously saved results CSV."""
        filepath = os.path.join(dirpath, f"results_{variant}.csv")
        results = {}
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                wl = row['workload']
                cache = row['cache'] if row['cache'] else None
                results[(wl, cache)] = float(row['throughput'])
        return results

    # ------------------------------------------------------------------
    # Performance run modes
    # ------------------------------------------------------------------

    def _run_default_performance(self):
        """Glibc vs LibMem comparison (perf=d)."""
        mode_name = self.version['name']
        unit = self.version['throughput_unit']
        print(f"\n{'='*70}")
        print(f" FleetBench {mode_name} | {self.func} | "
              f"Glibc vs LibMem")
        print(f"{'='*70}")

        print("\n[Glibc]")
        glibc = self._run_benchmark("glibc")
        print("\n[LibMem]")
        amd = self._run_benchmark("amd")

        if not glibc or not amd:
            print("ERROR: Failed to collect results.")
            return

        gv, lv = self.get_version_strings()
        headers, rows = self._build_comparison_table(
            glibc, amd,
            f"Glibc-{gv} ({unit})", f"LibMem-{lv} ({unit})")

        csv_file = os.path.join(
            self.result_dir,
            f"{self.bench_name}_throughput_values.csv")
        self.write_comparison_csv(csv_file, headers, rows)
        self._print_table(headers, rows)
        print(f"\n*** Results saved to [{self.result_dir}] ***")

    def _run_libmem_performance(self):
        """LibMem-only run (perf=l)."""
        mode_name = self.version['name']
        unit = self.version['throughput_unit']
        print(f"\n{'='*70}")
        print(f" FleetBench {mode_name} | {self.func} | LibMem only")
        print(f"{'='*70}")

        amd = self._run_benchmark("amd")
        if not amd:
            print("ERROR: Failed to collect results.")
            return

        headers, rows = self._build_single_table(
            amd, f"Throughput ({unit})")
        csv_file = os.path.join(self.result_dir, "perf_values.csv")
        self.write_comparison_csv(csv_file, headers, rows)
        self._print_table(headers, rows)
        print(f"\n*** Results saved to [{self.result_dir}] ***")

    def _run_glibc_performance(self):
        """Glibc-only run (perf=g)."""
        mode_name = self.version['name']
        unit = self.version['throughput_unit']
        print(f"\n{'='*70}")
        print(f" FleetBench {mode_name} | {self.func} | Glibc only")
        print(f"{'='*70}")

        glibc = self._run_benchmark("glibc")
        if not glibc:
            print("ERROR: Failed to collect results.")
            return

        headers, rows = self._build_single_table(
            glibc, f"Throughput ({unit})")
        csv_file = os.path.join(self.result_dir, "perf_values.csv")
        self.write_comparison_csv(csv_file, headers, rows)
        self._print_table(headers, rows)
        print(f"\n*** Results saved to [{self.result_dir}] ***")

    def _run_comparison_performance(self):
        """Compare old vs new LibMem runs (perf=c)."""
        mode_name = self.version['name']
        unit = self.version['throughput_unit']
        print(f"\n{'='*70}")
        print(f" FleetBench {mode_name} | {self.func} | "
              f"Old vs New LibMem")
        print(f"{'='*70}")

        try:
            old = self._load_results(self.old_perf_dir, "amd")
            new = self._load_results(self.new_perf_dir, "amd")
        except FileNotFoundError as e:
            print(f"ERROR: Could not load results: {e}")
            return

        headers, rows = self._build_comparison_table(
            old, new, f"Old ({unit})", f"New ({unit})")

        csv_file = os.path.join(
            self.result_dir, f"{self.bench_name}_comparison.csv")
        self.write_comparison_csv(csv_file, headers, rows)
        self._print_table(headers, rows)
        print(f"\n*** Results saved to [{self.result_dir}] ***")

    # ------------------------------------------------------------------
    # Table construction helpers
    # ------------------------------------------------------------------

    def _build_comparison_table(self, res_a, res_b, lbl_a, lbl_b):
        """Build headers + rows for a two-column comparison."""
        has_cache = self.cache_levels[0] is not None

        if has_cache:
            headers = ["Workload", "Cache", lbl_a, lbl_b, "GAIN"]
        else:
            headers = ["Workload", lbl_a, lbl_b, "GAIN"]

        rows = []
        for wl in self.workloads:
            for cache in self.cache_levels:
                key = (wl, cache)
                va = res_a.get(key, 0)
                vb = res_b.get(key, 0)
                gain = (f"{round(((vb - va) / va) * 100)}%"
                        if va else "N/A")
                label = self.workload_labels.get(wl, wl)
                if has_cache:
                    rows.append([label, cache,
                                 round(va, 4), round(vb, 4), gain])
                else:
                    rows.append([label,
                                 round(va, 4), round(vb, 4), gain])
        return headers, rows

    def _build_single_table(self, results, value_label):
        """Build headers + rows for a single-value table."""
        has_cache = self.cache_levels[0] is not None

        if has_cache:
            headers = ["Workload", "Cache", value_label]
        else:
            headers = ["Workload", value_label]

        rows = []
        for wl in self.workloads:
            for cache in self.cache_levels:
                key = (wl, cache)
                val = results.get(key, 0)
                label = self.workload_labels.get(wl, wl)
                if has_cache:
                    rows.append([label, cache, round(val, 4)])
                else:
                    rows.append([label, round(val, 4)])
        return headers, rows

    @staticmethod
    def _print_table(headers, rows):
        """Pretty-print a table to stdout."""
        col_w = [len(str(h)) for h in headers]
        for row in rows:
            for i, v in enumerate(row):
                col_w[i] = max(col_w[i], len(str(v)))

        hdr = " | ".join(
            f"{str(h):>{w}}" for h, w in zip(headers, col_w))
        sep = "-+-".join("-" * w for w in col_w)
        print(f"\n  {hdr}")
        print(f"  {sep}")
        for row in rows:
            line = " | ".join(
                f"{str(v):>{w}}" for v, w in zip(row, col_w))
            print(f"  {line}")
