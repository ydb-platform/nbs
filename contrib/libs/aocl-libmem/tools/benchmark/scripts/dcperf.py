#!/usr/bin/env python3
"""
DCPerf Benchmark Module

This module provides an interface to run DCPerf benchmarks (WDL and AI)
similar to the GoogleBench module (gbm.py).
"""

import os
import subprocess
import sys
import re
import csv
import json
from pathlib import Path
from os import environ as env

# Add parent directory to path to import BaseBench
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bench import BaseBench

# Import libmem definitions
try:
    from libmem_defs import LIBMEM_BIN_PATH
except ImportError:
    LIBMEM_BIN_PATH = "../../lib/libaocl-libmem.so"


class DCPerf(BaseBench):
    """DCPerf benchmark class for running WDL and AI benchmarks."""
    
    def __init__(self, **kwargs):
        """Initialize DCPerf benchmark instance.
        Args:
            **kwargs: Keyword arguments including MYPARSER and ARGS dicts
        """
        super().__init__(**kwargs)
        # Get benchmark type from func argument (wdl or ai)
        self.app_type = self.func  # func will be 'wdl' or 'ai'
        
        # Get the sub_func if specified (memcpy/memset for wdl, rebatch/tensor for ai)
        sub_func = self.MYPARSER.get('ARGS', {}).get('sub_func', None)

        # Determine which functions to run based on app_type and sub_func
        if self.app_type == 'ai':
            # For AI, check if sub_func is specified (rebatch/tensor)
            if sub_func in ['rebatch', 'tensor']:
                self.ai_benchmark = sub_func
            else:
                self.ai_benchmark = None  # Will prompt user or run all
            self.functions = []  # AI doesn't use the functions list
        elif self.app_type == 'wdl':
            # For WDL, check if sub_func is specified (memcpy/memset), else run both
            if sub_func in ['memcpy', 'memset']:
                self.functions = [sub_func]
            else:
                self.functions = ['memcpy', 'memset']
            self.ai_benchmark = None
        else:
            # Fallback: run both for unknown types
            self.functions = ['memcpy', 'memset']
            self.ai_benchmark = None

        # Get core_id
        self.core_id = self.MYPARSER.get('ARGS', {}).get('core_id', None)

        # Paths
        self.dcperf_base = Path(__file__).parent / "DCPerf"
        self.dcperf_repo = "https://github.com/facebookresearch/DCPerf.git"
        self.dcperf_branch = "v2-beta"
        self.dcperf_commit = "cbd654ac129ddb87d7c647bf663838522e0a23fc"  # Specific commit for patch compatibility
        self.patch_path = Path(__file__).parent / "wdl_bench_memcpy_memset_only.patch"

        # LibMem library path for LD_PRELOAD
        self.libmem_path = Path(LIBMEM_BIN_PATH)

        # Benchmark paths
        self.benchpress_cli = self.dcperf_base / "benchpress_cli.py"
        self.wdl_bench_path = self.dcperf_base / "benchmarks" / "wdl_bench"
        
    def __call__(self):
        """Execute the DCPerf benchmark workflow."""
        print(f"\n{'='*60}")
        print(f"DCPerf Benchmark - {self.app_type.upper()} Suite")
        
        if self.app_type == 'ai':
            if self.ai_benchmark:
                print(f"AI Benchmark: {self.ai_benchmark}")
            if self.core_id is not None:
                print(f"Core ID: {self.core_id}")
            print(f"{'='*60}\n")
            
            # Setup DCPerf repository
            if not self._setup_dcperf_repo():
                print("Error: Failed to setup DCPerf repository")
                return False
            
            # Apply optimization patch for WDL and AI benchmarks
            if not self._apply_patch():
                print("Warning: Patch application failed or already applied")
            
            # Run AI benchmarks
            return self._run_ai_benchmarks()
        
        print(f"Functions: {', '.join(self.functions)}")
        if self.core_id is not None:
            print(f"Core ID: {self.core_id}")
        print(f"{'='*60}\n")

        # Setup DCPerf repository
        if not self._setup_dcperf_repo():
            print("Error: Failed to setup DCPerf repository")
            return False

        # Apply optimization patch for WDL and AI benchmarks
        if not self._apply_patch():
            print("Warning: Patch application failed or already applied")

        # Install benchmarks
        if not self._install_benchmark():
            print("Error: Failed to install benchmarks")
            return False

        # Run benchmarks
        if self.app_type == 'wdl':
            return self._run_wdl_benchmarks()
        else:
            print(f"Error: Unknown app_type '{self.app_type}'")
            return False
    
    def _setup_dcperf_repo(self):
        """Clone DCPerf repository at specific commit if it doesn't exist.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if self.dcperf_base.exists():
            print(f"DCPerf repository already exists at {self.dcperf_base}")
            # Verify we're at the correct commit
            try:
                result = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(self.dcperf_base),
                    capture_output=True,
                    text=True,
                    check=True
                )
                current_commit = result.stdout.strip()
                if current_commit != self.dcperf_commit:
                    print(f"Warning: Repository is at commit {current_commit[:12]}, expected {self.dcperf_commit[:12]}")
                else:
                    print(f"Repository is at correct commit: {self.dcperf_commit[:12]}")
            except subprocess.CalledProcessError:
                print("Warning: Could not verify repository commit")
            return True
        
        print(f"Cloning DCPerf repository from {self.dcperf_repo}...")
        print(f"Checking out specific commit: {self.dcperf_commit[:12]}")
        try:
            # Clone with depth 1 but get the specific commit
            # First, try shallow clone of the branch
            cmd = [
                "git", "clone",
                "--branch", self.dcperf_branch,
                self.dcperf_repo,
                str(self.dcperf_base)
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            
            # Then checkout the specific commit
            checkout_cmd = ["git", "checkout", self.dcperf_commit]
            subprocess.run(
                checkout_cmd,
                cwd=str(self.dcperf_base),
                check=True,
                capture_output=True,
                text=True
            )
            
            print(f"Successfully cloned DCPerf repository at commit {self.dcperf_commit[:12]}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error cloning repository: {e}")
            print(f"stderr: {e.stderr}")
            return False
    
    def _apply_patch(self):
        """Apply the WDL and AI optimization patch.
        
        Returns:
            bool: True if successful or already applied, False on error
        """
        if not self.patch_path.exists():
            print(f"Warning: Patch file not found at {self.patch_path}")
            return False
        
        print(f"Applying optimization patch for WDL and AI benchmarks...")
        try:
            # Check if patch is already applied by looking for our marker comment
            install_script = self.dcperf_base / "packages" / "wdl_bench" / "install_wdl_bench.sh"
            with open(install_script, 'r') as f:
                content = f.read()
                if "# Skip system package installation" in content or "BENCHMARKS=(memcpy_benchmark memset_benchmark)" in content:
                    print("Patch appears to be already applied (optimized install script detected)")
                    return True
            
            # Check if patch can be applied
            check_cmd = ["git", "apply", "--check", str(self.patch_path)]
            check_result = subprocess.run(
                check_cmd,
                cwd=str(self.dcperf_base),
                capture_output=True,
                text=True
            )
            
            if check_result.returncode != 0:
                print(f"Patch check failed: {check_result.stderr}")
                print("Attempting to apply anyway...")
            
            # Apply the patch
            apply_cmd = ["git", "apply", str(self.patch_path)]
            result = subprocess.run(
                apply_cmd,
                cwd=str(self.dcperf_base),
                check=True,
                capture_output=True,
                text=True
            )
            print("Patch applied successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error applying patch: {e}")
            print(f"stderr: {e.stderr}")
            return False
    
    def _install_benchmark(self):
        """Install the DCPerf benchmarks.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if self.app_type == 'wdl':
            install_script = self.dcperf_base / "packages" / "wdl_bench" / "install_wdl_bench.sh"
        elif self.app_type == 'ai':
            print("AI benchmark installation not yet implemented")
            return False
        else:
            print(f"Unknown app_type: {self.app_type}")
            return False
        
        if not install_script.exists():
            print(f"Error: Install script not found at {install_script}")
            return False
        
        # Check if already installed
        if self.app_type == 'wdl' and self.wdl_bench_path.exists():
            memcpy_bench = self.wdl_bench_path / "memcpy_benchmark"
            memset_bench = self.wdl_bench_path / "memset_benchmark"
            if memcpy_bench.exists() and memset_bench.exists():
                print("WDL benchmarks already installed")
                return True
        
        print(f"Installing {self.app_type.upper()} benchmarks...")
        print("This may take several minutes on first run...")
        try:
            result = subprocess.run(
                ["bash", str(install_script)],
                cwd=str(self.dcperf_base),
                check=True,
                capture_output=True,
                text=True
            )
            print(f"Successfully installed {self.app_type.upper()} benchmarks")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error installing benchmarks: {e}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")
            return False
    
    def _parse_memcpy_output(self, output):
        """Parse memcpy benchmark output to extract glibc performance data.
        
        Args:
            output: String output from memcpy_benchmark
            
        Returns:
            tuple: (benchmark_names, throughput_values_in_Gs)
        """
        benchmark_names = []
        throughput_values = []
        
        # Pattern to match glibc benchmark lines only (not folly)
        # Example: bench(0_to_7_HOT_glibc)                                     3.65ns   274.29M
        pattern = r'bench\(([^)]+_glibc)\)\s+(\d+\.?\d*)([npu]s)\s+(\d+\.?\d*)([KMG])'
        
        for line in output.split('\n'):
            match = re.search(pattern, line)
            if match:
                name = match.group(1)
                iters_value = float(match.group(4))
                iters_unit = match.group(5)
                
                # Convert to G/s (iterations per second in billions)
                if iters_unit == 'M':
                    throughput_gs = iters_value / 1000.0  # Convert M to G
                elif iters_unit == 'G':
                    throughput_gs = iters_value
                elif iters_unit == 'K':
                    throughput_gs = iters_value / 1000000.0  # Convert K to G
                else:
                    throughput_gs = iters_value
                
                benchmark_names.append(name)
                throughput_values.append(throughput_gs)
        
        return benchmark_names, throughput_values
    
    def _parse_memset_output(self, output):
        """Parse memset benchmark output to extract performance data.
        
        Args:
            output: String output from memset_benchmark
            
        Returns:
            tuple: (sizes, throughput_values_in_Gs)
        """
        sizes = []
        throughput_values = []
        
        # Pattern to match memset lines (not folly)
        # Example: memset: size=1024                                           8.80ns   113.67M
        pattern = r'^\s*memset:\s+size=(\d+)\s+(\d+\.?\d*)([npu]s)\s+(\d+\.?\d*)([KMG])'
        
        for line in output.split('\n'):
            match = re.search(pattern, line)
            if match:
                size = int(match.group(1))
                iters_value = float(match.group(4))
                iters_unit = match.group(5)
                
                # Convert to G/s (iterations per second in billions)
                # Same logic as memcpy - don't multiply by size
                if iters_unit == 'M':
                    throughput_gs = iters_value / 1000.0  # Convert M to G
                elif iters_unit == 'G':
                    throughput_gs = iters_value
                elif iters_unit == 'K':
                    throughput_gs = iters_value / 1000000.0  # Convert K to G
                else:
                    throughput_gs = iters_value
                
                sizes.append(size)
                throughput_values.append(throughput_gs)
        
        return sizes, throughput_values
    def _run_wdl_benchmarks(self):
        """Run WDL benchmarks with and without LD_PRELOAD, compare results."""
        results = []
        append_csv = False
        
        for func in self.functions:
            if func not in ['memcpy', 'memset']:
                print(f"Warning: Unknown function '{func}', skipping...")
                continue
                
            benchmark_name = f"{func}_benchmark"
            bench_path = self.dcperf_base / "benchmarks" / "wdl_bench" / benchmark_name
            
            if not bench_path.exists():
                print(f"Error: {bench_path} not found. Please ensure benchmarks are built.")
                results.append((benchmark_name, False))
                continue
            
            print(f"\n{'='*60}")
            print(f"Benchmarking {func} on {self.bench_name}")
            print(f"{'='*60}\n")
            
            # Run without LD_PRELOAD (glibc)
            print("Running with system glibc...")
            env_glibc = os.environ.copy()
            env_glibc['LD_PRELOAD'] = ''
            
            cmd = ["taskset", "-c", str(self.core_id) if self.core_id is not None else "0", str(bench_path)]
            
            try:
                result_glibc = subprocess.run(
                    cmd,
                    cwd=str(self.dcperf_base),
                    env=env_glibc,
                    check=True,
                    capture_output=True,
                    text=True
                )
                
                # Save glibc output
                glibc_output_file = f'{self.result_dir}/dcperf_glibc_{func}.txt'
                with open(glibc_output_file, 'w') as f:
                    f.write(result_glibc.stdout)
                
                print(f"Glibc run completed, output saved to {glibc_output_file}")
                
            except subprocess.CalledProcessError as e:
                print(f"Error running {benchmark_name} with glibc: {e}")
                results.append((benchmark_name, False))
                continue
            
            # Run with LD_PRELOAD (libmem)
            print("\nRunning with AOCL-LibMem (LD_PRELOAD)...")
            env_amd = os.environ.copy()
            env_amd['LD_PRELOAD'] = LIBMEM_BIN_PATH
            
            try:
                # Get LibMem version
                self.LibMemVersion = subprocess.check_output(
                    "file " + LIBMEM_BIN_PATH + " | awk -F 'so.' '/libaocl-libmem.so/{print $3}'",
                    shell=True
                )
                libmem_version = self.LibMemVersion.decode('utf-8').strip() if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
                print(f"Using AOCL-LibMem {libmem_version}")
                
                result_amd = subprocess.run(
                    cmd,
                    cwd=str(self.dcperf_base),
                    env=env_amd,
                    check=True,
                    capture_output=True,
                    text=True
                )
                
                # Save amd output
                amd_output_file = f'{self.result_dir}/dcperf_amd_{func}.txt'
                with open(amd_output_file, 'w') as f:
                    f.write(result_amd.stdout)
                
                print(f"LibMem run completed, output saved to {amd_output_file}")
                
            except subprocess.CalledProcessError as e:
                print(f"Error running {benchmark_name} with libmem: {e}")
                results.append((benchmark_name, False))
                continue
            
            # Parse outputs and compare for both memcpy and memset
            # If running both functions, append for the second and later
            self._generate_comparison_report(func, result_glibc.stdout, result_amd.stdout, append_csv=append_csv)
            append_csv = True
            
            results.append((benchmark_name, True))
        
        return all(success for _, success in results)
    
    def _generate_comparison_report(self, func, glibc_output, amd_output, append_csv=False):
        """Generate comparison report similar to gbm.py.
        
        Args:
            func: Function name (memcpy or memset)
            glibc_output: Output from glibc run
            amd_output: Output from libmem run
        """
        # Parse outputs based on function type
        if func == 'memcpy':
            glibc_names, glibc_throughput = self._parse_memcpy_output(glibc_output)
            amd_names, amd_throughput = self._parse_memcpy_output(amd_output)
            label_col = "Benchmark"
        elif func == 'memset':
            glibc_names, glibc_throughput = self._parse_memset_output(glibc_output)
            amd_names, amd_throughput = self._parse_memset_output(amd_output)
            label_col = "Size"
        else:
            print(f"Warning: Unknown function type '{func}'")
            return
        
        if not glibc_names or not amd_names:
            print("Warning: Could not parse benchmark output")
            return
        
        # Calculate gains
        gains = self.calculate_gains(amd_throughput, glibc_throughput)
        
        # Get version strings
        try:
            glibc_version = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'", shell=True)
            glibc_version = glibc_version.decode('utf-8').strip() if isinstance(glibc_version, bytes) else str(glibc_version).strip()
        except:
            glibc_version = "unknown"
        
        libmem_version = self.LibMemVersion.decode('utf-8').strip() if isinstance(self.LibMemVersion, bytes) else str(self.LibMemVersion).strip()
        
        # Write CSV with common method - use iters/s for both
        throughput_unit = "iters/s"
        headers = [label_col, f"Glibc-{glibc_version} ({throughput_unit})", f"LibMem-{libmem_version} ({throughput_unit})", "GAINS"]
        data_rows = list(zip(glibc_names, glibc_throughput, amd_throughput, gains))
        
        csv_file = f"{self.result_dir}/{self.bench_name}_throughput_values.csv"
        self.write_comparison_csv(csv_file, headers, data_rows, append=append_csv)
        
        # Print results
        print(f"\n{'='*60}")
        print(f"BENCHMARK: {self.bench_name} - {func}")
        print(f"{'='*60}")
        print(f"{label_col:<40} : GAINS")
        print(f"{'-'*60}")
        for name, gain in zip(glibc_names, gains):
            # Format display name
            if func == 'memcpy':
                display_name = str(name).replace('_glibc', '').replace('bench(', '').replace(')', '')
            else:  # memset - format size with B/KB units
                size = int(name)
                if size >= 1024:
                    display_name = f"{size // 1024}KB"
                else:
                    display_name = f"{size}B"
            print(f"{display_name:<40} : {gain:>4}")
        print(f"\n*** Test reports copied to directory [{self.result_dir}] ***")

    def _run_ai_benchmarks(self):
        """Run AI benchmarks (rebatch or tensor).
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.benchpress_cli.exists():
            print(f"Error: benchpress_cli.py not found at {self.benchpress_cli}")
            return False
        
        if not self.ai_benchmark:
            print("Error: No AI benchmark specified. Please specify 'rebatch' or 'tensor'")
            return False
        
        if self.ai_benchmark == 'rebatch':
            return self._run_rebatch_benchmark()
        elif self.ai_benchmark == 'tensor':
            return self._run_tensor_benchmark()
        else:
            print(f"Error: Unknown AI benchmark '{self.ai_benchmark}'")
            return False
    
    def _run_rebatch_benchmark(self):
        """Run rebatch AI benchmark with both glibc and LD_PRELOAD.
        
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"\n{'='*60}")
        print("Installing and Running Rebatch Benchmarks")
        print(f"{'='*60}\n")
        
        # Check and install rebatch_a
        if not self._check_and_install_rebatch('rebatch_a'):
            print("Error: Failed to install rebatch_a")
            return False
        
        # Check and install rebatch_b
        if not self._check_and_install_rebatch('rebatch_b'):
            print("Error: Failed to install rebatch_b")
            return False
        
        # Store results for CSV generation
        results = {
            'rebatch_a': {'glibc': None, 'libmem': None},
            'rebatch_b': {'glibc': None, 'libmem': None}
        }
        
        # Run rebatch_a with glibc
        print("\n" + "="*60)
        print("Running rebatch_a with GLIBC")
        print("="*60)
        bw_a_glibc = self._run_rebatch('rebatch_a', use_ld_preload=False)
        if bw_a_glibc is None:
            print("Error: Failed to run rebatch_a with glibc")
            return False
        results['rebatch_a']['glibc'] = bw_a_glibc
        
        # Run rebatch_a with LD_PRELOAD
        print("\n" + "="*60)
        print("Running rebatch_a with LD_PRELOAD (LibMem)")
        print("="*60)
        bw_a_libmem = self._run_rebatch('rebatch_a', use_ld_preload=True)
        if bw_a_libmem is None:
            print("Error: Failed to run rebatch_a with LibMem")
            return False
        results['rebatch_a']['libmem'] = bw_a_libmem
        
        # Run rebatch_b with glibc
        print("\n" + "="*60)
        print("Running rebatch_b with GLIBC")
        print("="*60)
        bw_b_glibc = self._run_rebatch('rebatch_b', use_ld_preload=False)
        if bw_b_glibc is None:
            print("Error: Failed to run rebatch_b with glibc")
            return False
        results['rebatch_b']['glibc'] = bw_b_glibc
        
        # Run rebatch_b with LD_PRELOAD
        print("\n" + "="*60)
        print("Running rebatch_b with LD_PRELOAD (LibMem)")
        print("="*60)
        bw_b_libmem = self._run_rebatch('rebatch_b', use_ld_preload=True)
        if bw_b_libmem is None:
            print("Error: Failed to run rebatch_b with LibMem")
            return False
        results['rebatch_b']['libmem'] = bw_b_libmem
        
        # Generate CSV report with BW data and gains
        self._generate_rebatch_csv(results)
        
        print(f"\n*** Rebatch benchmarks completed successfully ***")
        return True
    
    def _parse_rebatch_output(self, output):
        """Parse rebatch benchmark output to extract bandwidth.
        
        Args:
            output: The stdout from benchpress_cli.py (JSON format)
            
        Returns:
            float: Bandwidth in GB/s, or None if parsing fails
        """
        try:
            # Parse JSON output
            import json
            data = json.loads(output.strip())
            
            # Extract bandwidth from metrics
            if 'metrics' in data and 'bandwidth' in data['metrics']:
                bw = float(data['metrics']['bandwidth'])
                return bw
            
            return None
            
        except (ValueError, json.JSONDecodeError, KeyError) as e:
            print(f"Error parsing rebatch output: {e}")
            return None
    
    def _generate_rebatch_csv(self, results):
        """Generate CSV file with rebatch results and gains.
        
        Args:
            results: Dictionary with structure:
                     {'rebatch_a': {'glibc': bw, 'libmem': bw},
                      'rebatch_b': {'glibc': bw, 'libmem': bw}}
        """
        csv_file = f'{self.result_dir}/dcperf_ai_rebatch_comparison.csv'
        
        print(f"\nGenerating CSV report: {csv_file}")
        
        try:
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow(['Benchmark', 'GAINS %'])
                writer.writerow([])  # Empty row for better readability
                
                # Calculate and write gains for Model A
                if 'rebatch_a' in results:
                    glibc_a = results['rebatch_a']['glibc']
                    libmem_a = results['rebatch_a']['libmem']
                    
                    if glibc_a and libmem_a:
                        gains_a = ((libmem_a - glibc_a) / glibc_a) * 100
                        writer.writerow(['Model A', f'{gains_a:.2f}%'])
                
                # Calculate and write gains for Model B
                if 'rebatch_b' in results:
                    glibc_b = results['rebatch_b']['glibc']
                    libmem_b = results['rebatch_b']['libmem']
                    
                    if glibc_b and libmem_b:
                        gains_b = ((libmem_b - glibc_b) / glibc_b) * 100
                        writer.writerow(['Model B', f'{gains_b:.2f}%'])
            
            print(f"✓ CSV report generated: {csv_file}")
            
            # Print summary to console
            print("\n" + "="*60)
            print("AI_WDL- REBATCH BENCHMARK RESULTS")
            print("="*60)
            print(f"{'Benchmark':<30} {'GAINS %':>15}")
            print("-"*60)
            
            if 'rebatch_a' in results:
                glibc_a = results['rebatch_a']['glibc']
                libmem_a = results['rebatch_a']['libmem']
                if glibc_a and libmem_a:
                    gains_a = ((libmem_a - glibc_a) / glibc_a) * 100
                    print(f"{'Model A':<30} {gains_a:>14.2f}%")
            
            if 'rebatch_b' in results:
                glibc_b = results['rebatch_b']['glibc']
                libmem_b = results['rebatch_b']['libmem']
                if glibc_b and libmem_b:
                    gains_b = ((libmem_b - glibc_b) / glibc_b) * 100
                    print(f"{'Model B':<30} {gains_b:>14.2f}%")
            
            print("="*60)
            
        except Exception as e:
            print(f"Error generating CSV: {e}")
    
    def _check_and_install_rebatch(self, rebatch_name):
        """Check if rebatch benchmark is installed, install if not.
        
        Args:
            rebatch_name: Name of rebatch benchmark (rebatch_a or rebatch_b)
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Check if already installed by trying to run benchpress_cli.py with list
        # This is a simple check - you might want to make it more robust
        print(f"\nChecking if {rebatch_name} is installed...")
        
        # Try to install (benchpress will skip if already installed)
        print(f"Installing {rebatch_name}...")
        try:
            cmd = ["python3", str(self.benchpress_cli), "-b", "ai", "install", rebatch_name]
            result = subprocess.run(
                cmd,
                cwd=str(self.dcperf_base),
                check=True,
                capture_output=True,
                text=True
            )
            print(f"✓ {rebatch_name} installation completed")
            if result.stdout:
                print(result.stdout)
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error installing {rebatch_name}: {e}")
            if e.stdout:
                print(f"stdout: {e.stdout}")
            if e.stderr:
                print(f"stderr: {e.stderr}")
            return False
    
    def _run_rebatch(self, rebatch_name, use_ld_preload=False):
        """Run a rebatch benchmark.
        
        Args:
            rebatch_name: Name of rebatch benchmark (rebatch_a or rebatch_b)
            use_ld_preload: Whether to use LD_PRELOAD with libmem
            
        Returns:
            float: Bandwidth in GB/s if successful, None otherwise
        """
        try:
            cmd = ["python3", str(self.benchpress_cli), "-b", "ai", "run", rebatch_name]
            
            # Add core pinning if specified
            if self.core_id is not None:
                cmd = ["taskset", "-c", str(self.core_id)] + cmd
            
            # Setup environment with or without LD_PRELOAD
            env = os.environ.copy()
            if use_ld_preload:
                env['LD_PRELOAD'] = str(self.libmem_path)
            
            result = subprocess.run(
                cmd,
                cwd=str(self.dcperf_base),
                env=env,
                check=True,
                capture_output=True,
                text=True
            )
            
            print(result.stdout)
            
            # Save output to file
            lib_type = "amd" if use_ld_preload else "glibc"
            output_file = f'{self.result_dir}/{lib_type}_dcperf_ai_{rebatch_name}.txt'
            with open(output_file, 'w') as f:
                f.write(result.stdout)
            
            # Parse output to extract BW
            bw = self._parse_rebatch_output(result.stdout)
            if bw is None:
                print(f"Warning: Could not extract BW from {rebatch_name} output")
                return None
            
            lib_display = "AMD LibMem" if use_ld_preload else "GLIBC"
            print(f"\n✓ {rebatch_name} ({lib_display}) completed: BW = {bw:.4f} GB/s")
            print(f"✓ Output saved to {output_file}")
            return bw
            
        except subprocess.CalledProcessError as e:
            print(f"Error running {rebatch_name}: {e}")
            if e.stdout:
                print(f"stdout: {e.stdout}")
            if e.stderr:
                print(f"stderr: {e.stderr}")
            return None
    
    def _run_tensor_benchmark(self):
        """Run tensor (deser) AI benchmark with both glibc and LD_PRELOAD.
        
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"\n{'='*60}")
        print("Installing and Running Tensor (Deser) Benchmarks")
        print(f"{'='*60}\n")
        
        # Check and install deser_a
        if not self._check_and_install_deser('deser_a'):
            print("Error: Failed to install deser_a")
            return False
        
        # Check and install deser_b
        if not self._check_and_install_deser('deser_b'):
            print("Error: Failed to install deser_b")
            return False
        
        # Store results for CSV generation
        results = {
            'deser_a': {'glibc': None, 'libmem': None},
            'deser_b': {'glibc': None, 'libmem': None}
        }
        
        # Run deser_a with glibc
        print("\n" + "="*60)
        print("Running deser_a with GLIBC")
        print("="*60)
        throughput_a_glibc = self._run_deser('deser_a', use_ld_preload=False)
        if throughput_a_glibc is None:
            print("Error: Failed to run deser_a with glibc")
            return False
        results['deser_a']['glibc'] = throughput_a_glibc
        
        # Run deser_a with LD_PRELOAD
        print("\n" + "="*60)
        print("Running deser_a with LD_PRELOAD (LibMem)")
        print("="*60)
        throughput_a_libmem = self._run_deser('deser_a', use_ld_preload=True)
        if throughput_a_libmem is None:
            print("Error: Failed to run deser_a with LibMem")
            return False
        results['deser_a']['libmem'] = throughput_a_libmem
        
        # Run deser_b with glibc
        print("\n" + "="*60)
        print("Running deser_b with GLIBC")
        print("="*60)
        throughput_b_glibc = self._run_deser('deser_b', use_ld_preload=False)
        if throughput_b_glibc is None:
            print("Error: Failed to run deser_b with glibc")
            return False
        results['deser_b']['glibc'] = throughput_b_glibc
        
        # Run deser_b with LD_PRELOAD
        print("\n" + "="*60)
        print("Running deser_b with LD_PRELOAD (LibMem)")
        print("="*60)
        throughput_b_libmem = self._run_deser('deser_b', use_ld_preload=True)
        if throughput_b_libmem is None:
            print("Error: Failed to run deser_b with LibMem")
            return False
        results['deser_b']['libmem'] = throughput_b_libmem
        
        # Generate CSV report with throughput data and gains
        self._generate_deser_csv(results)
        
        print(f"\n*** Tensor (Deser) benchmarks completed successfully ***")
        return True
    
    def _check_and_install_deser(self, deser_name):
        """Check if deser benchmark is installed, install if not.
        
        Args:
            deser_name: Name of deser benchmark (deser_a or deser_b)
            
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"\nChecking if {deser_name} is installed...")
        
        # Try to install (benchpress will skip if already installed)
        print(f"Installing {deser_name}...")
        try:
            cmd = ["python3", str(self.benchpress_cli), "-b", "ai", "install", deser_name]
            result = subprocess.run(
                cmd,
                cwd=str(self.dcperf_base),
                check=True,
                capture_output=True,
                text=True
            )
            print(f"✓ {deser_name} installation completed")
            if result.stdout:
                print(result.stdout)
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error installing {deser_name}: {e}")
            if e.stdout:
                print(f"stdout: {e.stdout}")
            if e.stderr:
                print(f"stderr: {e.stderr}")
            return False
    
    def _run_deser(self, deser_name, use_ld_preload=False):
        """Run a deser benchmark.
        
        Args:
            deser_name: Name of deser benchmark (deser_a or deser_b)
            use_ld_preload: Whether to use LD_PRELOAD with libmem
            
        Returns:
            float: Throughput (messages/sec) if successful, None otherwise
        """
        try:
            cmd = ["python3", str(self.benchpress_cli), "-b", "ai", "run", deser_name]
            
            # Add core pinning if specified
            if self.core_id is not None:
                cmd = ["taskset", "-c", str(self.core_id)] + cmd
            
            # Setup environment with or without LD_PRELOAD
            env = os.environ.copy()
            if use_ld_preload:
                env['LD_PRELOAD'] = str(self.libmem_path)
            
            result = subprocess.run(
                cmd,
                cwd=str(self.dcperf_base),
                env=env,
                check=True,
                capture_output=True,
                text=True
            )
            
            print(result.stdout)
            
            # Save output to file
            lib_type = "amd" if use_ld_preload else "glibc"
            output_file = f'{self.result_dir}/{lib_type}_dcperf_ai_{deser_name}.txt'
            with open(output_file, 'w') as f:
                f.write(result.stdout)
            
            # Parse output to extract throughput
            throughput = self._parse_deser_output(result.stdout)
            if throughput is None:
                print(f"Warning: Could not extract throughput from {deser_name} output")
                return None
            
            lib_display = "AMD LibMem" if use_ld_preload else "GLIBC"
            print(f"\n✓ {deser_name} ({lib_display}) completed: Throughput = {throughput:.4f} Mops/sec")
            print(f"✓ Output saved to {output_file}")
            return throughput
            
        except subprocess.CalledProcessError as e:
            print(f"Error running {deser_name}: {e}")
            if e.stdout:
                print(f"stdout: {e.stdout}")
            if e.stderr:
                print(f"stderr: {e.stderr}")
            return None
    
    def _parse_deser_output(self, output):
        """Parse deser benchmark output to extract throughput.
        
        Args:
            output: The stdout from benchpress_cli.py (JSON format)
            
        Returns:
            float: Throughput in Mops/sec (Million operations per second), or None if parsing fails
        """
        try:
            # Parse JSON output
            data = json.loads(output)
            
            # Extract throughput (Mops/sec - Million operations per second)
            if 'metrics' in data and 'Mops/sec' in data['metrics']:
                throughput = float(data['metrics']['Mops/sec'])
                return throughput
            else:
                print("Warning: 'metrics.Mops/sec' not found in output")
                return None
                
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON output: {e}")
            return None
        except (KeyError, ValueError) as e:
            print(f"Error extracting throughput: {e}")
            return None
    
    def _generate_deser_csv(self, results):
        """Generate CSV report for deser benchmarks with gains calculation.
        
        Args:
            results: Dictionary with structure:
                     {'deser_a': {'glibc': float, 'libmem': float},
                      'deser_b': {'glibc': float, 'libmem': float}}
        """
        try:
            csv_file = f'{self.result_dir}/dcperf_ai_deser_comparison.csv'
            print(f"\nGenerating CSV report: {csv_file}")
            
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Benchmark', 'GAINS %'])
                writer.writerow([])  # Empty row for better readability
                
                # Calculate and write gains for Model A (deser_a)
                if 'deser_a' in results:
                    glibc_a = results['deser_a']['glibc']
                    libmem_a = results['deser_a']['libmem']
                    
                    if glibc_a and libmem_a:
                        gains_a = ((libmem_a - glibc_a) / glibc_a) * 100
                        writer.writerow(['Model A', f'{gains_a:.2f}%'])
                
                # Calculate and write gains for Model B (deser_b)
                if 'deser_b' in results:
                    glibc_b = results['deser_b']['glibc']
                    libmem_b = results['deser_b']['libmem']
                    
                    if glibc_b and libmem_b:
                        gains_b = ((libmem_b - glibc_b) / glibc_b) * 100
                        writer.writerow(['Model B', f'{gains_b:.2f}%'])
            
            print(f"✓ CSV report generated: {csv_file}")
            
            # Print summary to console
            print("\n" + "="*60)
            print("AI_WDL- TENSOR (DESER) BENCHMARK RESULTS")
            print("="*60)
            print(f"{'Benchmark':<30} {'GAINS %':>15}")
            print("-"*60)
            
            if 'deser_a' in results:
                glibc_a = results['deser_a']['glibc']
                libmem_a = results['deser_a']['libmem']
                if glibc_a and libmem_a:
                    gains_a = ((libmem_a - glibc_a) / glibc_a) * 100
                    print(f"{'Model A':<30} {gains_a:>14.2f}%")
            
            if 'deser_b' in results:
                glibc_b = results['deser_b']['glibc']
                libmem_b = results['deser_b']['libmem']
                if glibc_b and libmem_b:
                    gains_b = ((libmem_b - glibc_b) / glibc_b) * 100
                    print(f"{'Model B':<30} {gains_b:>14.2f}%")
            
            print("="*60)
            
        except Exception as e:
            print(f"Error generating CSV: {e}")


if __name__ == "__main__":
    print("This module should be imported and used via bench.py")
