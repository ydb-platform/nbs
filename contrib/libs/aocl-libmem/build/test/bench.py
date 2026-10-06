#!/usr/bin/python3
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
import sys

sys.path.insert(0, '../tools/benchmarks/external')
import GetParser as ParserConfig

def check_root_access():
    try:
        subprocess.run(['sudo', '-n', 'true'], check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError:
        return False

def run_rdmsr(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = result.stdout.decode('utf-8').strip()
        return output
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        return None

def parse_msr_output(output):
    # Convert the hexadecimal output to a binary string, ensuring it's at least 64 bits long
    binary_value = bin(int(output, 16))[2:].zfill(64)
    # Extract the relevant bits (0-9)
    parsed_info = {
        "PrefetchAggressivenessProfile": binary_value[-10:-7],  # Bits 9:7
        "MasterEnable": binary_value[-11],                      # Bit 6
        "UpDown": binary_value[-12],                            # Bit 5
        "Reserved": binary_value[-13],                          # Bit 4
        "L2Stream": binary_value[-14],                          # Bit 3
        "L1Region": binary_value[-15],                          # Bit 2
        "L1Stride": binary_value[-16],                          # Bit 1
        "L1Stream": binary_value[-17],                          # Bit 0
    }
    return parsed_info

def get_msr_info(core_id):
    command = f"sudo rdmsr -c 0xc0000108 -p {core_id}"
    output = run_rdmsr(command)
    if output:
        return parse_msr_output(output)
    return None

def get_prefetch_aggressiveness_level(bits):
    # Map the binary string to the actual level names
    levels = {
        '000': 'Level 0 - least aggressive prefetch profile',
        '001': 'Level 1',
        '010': 'Level 2',
        '011': 'Level 3 - most aggressive prefetch profile',
        '100': 'Reserved - Default, aggressive prefetch profile is disabled',
        '101': 'Reserved - Default, aggressive prefetch profile is disabled',
        '110': 'Reserved - Default, aggressive prefetch profile is disabled',
        '111': 'Reserved - Default, aggressive prefetch profile is disabled',
    }
    return levels.get(bits, 'Unknown')

def parse_size_with_unit(size_str):
    """
    Parse a string representing a size with units (B, KB, MB, GB) and convert to bytes.
    Examples: '8B', '16KB', '9MB', '1GB'

    Args:
        size_str (str): A string representing size with unit

    Returns:
        int: Size in bytes

    Raises:
        argparse.ArgumentTypeError: If the format is invalid
    """
    # Define pattern to match number followed by optional unit
    pattern = r'^(\d+)(?:([KMGT]?B))?$'
    match = re.match(pattern, size_str, re.IGNORECASE)

    if not match:
        raise argparse.ArgumentTypeError(
            f"Invalid size format: {size_str}. Expected format: NUMBER[UNIT] "
            "where UNIT is one of B, KB, MB, GB (case insensitive)"
        )

    value, unit = match.groups()
    value = int(value)
    # If SIZE is negative or zero, raise an error
    if value <= 0:
        raise argparse.ArgumentTypeError(f"Size must be a positive integer: {size_str}")

    # Convert to bytes based on unit
    if unit is None or unit.upper() == 'B':
        return value
    elif unit.upper() == 'KB':
        return value * 1024
    elif unit.upper() == 'MB':
        return value * 1024 * 1024
    elif unit.upper() == 'GB':
        return value * 1024 * 1024 * 1024
    else:
        raise argparse.ArgumentTypeError(f"Unknown unit: {unit}")

class BaseBench:
    """Base class containing common benchmarking functionality"""

    def __init__(self, **kwargs):
        # Initialize common version tracking
        self.LibMemVersion = ''
        self.GlibcVersion = ''
        self.size_unit = []
        self.gains = []

        # Initialize common arguments if provided
        if kwargs:
            self.ARGS = kwargs
            self.MYPARSER = self.ARGS["ARGS"]

            # Common attributes from command line
            self.func = self.MYPARSER['ARGS']['func']
            self.ranges = self.MYPARSER['ARGS']['range']
            self.core = self.MYPARSER['ARGS']['core_id']
            self.bench_name = self.MYPARSER['ARGS']['bench_name']
            self.result_dir = self.MYPARSER['ARGS']['result_dir']
            self.perf = self.MYPARSER['ARGS']['perf']
            self.bestperf = self.MYPARSER['ARGS'].get('best_performance', False)

            # Initialize common properties
            self.size_values = []
            self.variant = "amd"  # Default variant

            # Performance comparison directories
            if self.perf in ['c', 'b']:
                self.old_perf_dir = self.MYPARSER['ARGS']['old_perf_dir']
                self.new_perf_dir = self.MYPARSER['ARGS']['new_perf_dir']

    def throughput_converter(self, value):
        """Convert throughput values from M/s to G/s where applicable"""
        for i in range(len(value)):
            if 'M/s' in value[i]:
                value[i] = float(value[i].replace('M/s', ''))
                value[i] = round(value[i] / 1000, 5)
            elif 'G/s' in value[i]:
                value[i] = float(value[i].replace('G/s', ''))
        return value

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

    def data_unit(self):
        """Convert sizes from bytes to appropriate units (B, KB, MB)"""
        self.size_unit = []
        for size in self.size_values:
            # Convert string to int if necessary
            size_int = int(size) if isinstance(size, str) else size
            if size_int < 1024:
                self.size_unit.append(f"{size_int}B")
            elif size_int < 1048576:
                self.size_unit.append(f"{size_int // 1024}KB")
            else:
                self.size_unit.append(f"{size_int // 1048576}MB")

    def calculate_gains(self, new_values, old_values):
        """Calculate percentage gains between two sets of values"""
        gains = []
        for i in range(len(new_values)):
            if old_values[i] != 0:
                gain = round(((new_values[i] - old_values[i]) / old_values[i]) * 100)
                gains.append(f"{gain}%")
            else:
                gains.append("N/A")
        return gains

    def write_comparison_csv(self, filename, headers, data_rows):
        """Write comparison results to CSV file"""
        with open(filename, "w", newline="") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(headers)
            for row in data_rows:
                writer.writerow(row)

    def get_version_strings(self):
        """Get formatted version strings for LibMem and Glibc"""
        # Ensure versions are retrieved if not already set
        if not hasattr(self, 'GlibcVersion') or not self.GlibcVersion:
            try:
                import subprocess
                self.GlibcVersion = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'", shell=True)
            except:
                self.GlibcVersion = "Unknown"

        if not hasattr(self, 'LibMemVersion') or not self.LibMemVersion:
            try:
                import subprocess
                import sys
                import os
                # Add the test directory to path to import libmem_defs
                test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../test")
                if test_dir not in sys.path:
                    sys.path.insert(0, test_dir)
                from libmem_defs import LIBMEM_BIN_PATH
                self.LibMemVersion = subprocess.check_output("file " + LIBMEM_BIN_PATH + \
                    "| awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell=True)
            except:
                self.LibMemVersion = "Unknown"

        # Convert to string and clean up
        if isinstance(self.GlibcVersion, bytes):
            glibc_version = self.GlibcVersion.decode('utf-8').strip()
        else:
            glibc_version = str(self.GlibcVersion).strip()

        if isinstance(self.LibMemVersion, bytes):
            libmem_version = self.LibMemVersion.decode('utf-8').strip()
        else:
            libmem_version = str(self.LibMemVersion).strip()

        # Handle empty strings
        glibc_version = glibc_version if glibc_version else "Unknown"
        libmem_version = libmem_version if libmem_version else "Unknown"

        return glibc_version, libmem_version

    def print_result(self):
        """Print benchmark results - to be implemented by subclasses"""
        pass

    def print_result_perf(self):
        """Print performance-only results - to be implemented by subclasses"""
        pass

class Bench:
    """
    The object of this class will run the specified benchmark framework
    based on the command given by the user
    """

    def __init__(self, **kwargs):
        self.ARGS = kwargs
        self.MYPARSER = self.ARGS["ARGS"]

    def __call__(self, *args, **kwargs):
        # Import benchmark classes dynamically to avoid circular imports
        if(self.MYPARSER['benchmark']=='tbm'):
            from tbm import TBM
            TBM_execute = TBM(ARGS=self.ARGS, class_obj=self)
            TBM_execute() #Status:Success/Failure
        elif(self.MYPARSER['benchmark']=='gbm'):
            from gbm import GBM
            GBM_execute = GBM(ARGS=self.ARGS, class_obj=self)
            GBM_execute() #Status:Success/Failure
        elif(self.MYPARSER['benchmark']=='fbm'):
            from fbm import FBM
            FBM_execute = FBM(ARGS=self.ARGS, class_obj=self)
            FBM_execute() #Status:Success/Failure

libmem_memory = ['memcpy', 'memmove', 'memset', 'memcmp', 'mempcpy']
libmem_string = ['strcpy', 'strncpy', 'strcmp', 'strncmp', 'strlen', 'strcat', 'strncat', 'strspn', 'strstr', 'memchr', 'strchr']
libmem_funcs = libmem_memory + libmem_string

def run_command(cmd):
    """Runs a shell command and returns the output."""
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
        output = f"Command '{e.cmd}' returned non-zero exit status {e.returncode}\n{e.output.decode()}"
    return output

def collect_system_info(output_file, core_id):
    sudo_flag = check_root_access()

    """
    Collect system information and write to the specified output file.
    """
    with open(output_file, "w") as f:
        f.write("=========================================\n")
        f.write(" System Information Collection \n")
        f.write("=========================================\n")

        # Linux Kernel Version
        f.write("\n==== Linux Kernel Version ====\n")
        f.write(run_command("uname -r"))

        # Linux Distribution Details
        f.write("\n==== Linux Distribution Details ====\n")
        lsb_release = run_command("lsb_release -a")
        if lsb_release:
            f.write(lsb_release)
        else:
            f.write("lsb_release command not found; using /etc/os-release:\n")
            f.write(run_command("cat /etc/os-release"))

        # BIOS and Microcode (if logged) Version
        f.write("\n==== BIOS And MicroCode Info ====\n")
        f.write("\n ---- Bios Information ----\n")
        if sudo_flag:
            f.write(run_command("sudo dmidecode -t bios 2>/dev/null | grep -E 'Vendor:|Version:|Release Date:|Address:'"))
        else:
            f.write("Failed to retrieve Bios Information as user does not have root privileges.\n")
        f.write("\n---- MicroCode version ----\n")
        f.write(run_command("grep 'microcode' /proc/cpuinfo | head -n 1"))

        # Cache Details
        f.write("\n==== Cache Details ====\n")
        f.write(run_command("lscpu -C"))

        # CPU Details
        f.write("\n==== CPU Details ====\n")
        f.write("\n---- Frequency Scaling Governor and Fixed Frequency Check ----\n")
        f.write("Current CPU frequency scaling governor: " \
                + run_command(f"cat /sys/devices/system/cpu/cpu{core_id}/cpufreq/scaling_governor"))
        f.write("Current Frequency of the CPU: " + \
                run_command(f"cat /sys/devices/system/cpu/cpu{core_id}/cpufreq/scaling_cur_freq"))
        f.write("Min Frequency of the CPU: " + \
                run_command(f"cat /sys/devices/system/cpu/cpu{core_id}/cpufreq/scaling_min_freq"))
        f.write("Max Frequency of the CPU: " + \
                run_command(f"cat /sys/devices/system/cpu/cpu{core_id}/cpufreq/scaling_max_freq"))
        f.write("Note: If min_freq is equal to max_freq, system is in fixed frequency mode.\n")

        f.write("\n---- CPU Model Name (from /proc/cpuinfo) ----\n")
        f.write(run_command("grep -m 1 'model name' /proc/cpuinfo"))

        f.write("\n---- Detailed CPU Information ----\n")
        f.write(run_command("lscpu"))

        # Prefetch Information
        f.write("\n==== Prefetch Information ====\n")
        if sudo_flag:
            f.write("rdmsr output: " + run_command(f"sudo rdmsr -c 0xc0000108 -p {core_id}"))
            msr_data = get_msr_info(core_id)
            if msr_data:
                prefetch_level = get_prefetch_aggressiveness_level(msr_data['PrefetchAggressivenessProfile'])
                f.write(f"  Prefetch Aggressiveness Profile: {msr_data['PrefetchAggressivenessProfile']} ({prefetch_level})\n")
                f.write(f"  Master Enable (to enable Prefetch Aggressiveness Profiles): {'Enabled' if msr_data['MasterEnable'] == '1' else 'Disabled'}\n")
                f.write(f"  UpDown Prefetcher (the prefetcher that uses memory access history to determine whether to fetch the next or previous line into the L2 cache): {'Enabled' if msr_data['UpDown'] == '1' else 'Disabled'}\n")
                f.write(f"  L2 Stream Prefetcher (the prefetcher that uses history of memory access patterns to fetch additional sequential lines into L2 cache): {'Enabled' if msr_data['L2Stream'] == '0' else 'Disabled'}\n")
                f.write(f"  L1 Region Prefetcher (the prefetcher that uses memory access history to fetch additional lines into L1 cache): {'Enabled' if msr_data['L1Region'] == '0' else 'Disabled'}\n")
                f.write(f"  L1 Stride Prefetcher (prefetcher that uses memory access history of individual instructions to fetch additional lines into L1 cache): {'Enabled' if msr_data['L1Stride'] == '0' else 'Disabled'}\n")
                f.write(f"  L1 Stream Prefetcher (the stream prefetcher that uses history of memory access patterns to fetch additional sequential lines into L1 cache): {'Enabled' if msr_data['L1Stream'] == '0' else 'Disabled'}\n")
            else:
                f.write("Failed to retrieve Prefetch information.\n")
        else:
            f.write("Failed to retrieve Prefetch information as user does not have root privileges.\n")

        # Memory Information
        f.write("\n==== Memory Information ====\n")
        f.write(run_command("free -h"))
        f.write("\nAdditional details from /proc/meminfo (first few lines):\n")
        f.write(run_command("head -n 5 /proc/meminfo"))

        # Memory Speed Information
        f.write("\n ----Memory Speed Information ----\n")
        if sudo_flag:
            f.write(run_command("sudo dmidecode -t memory | grep -i 'Speed' | sort | uniq"))
        else:
            f.write("Failed to retrieve memory Speed Information as user does not have root privileges.\n")

        # Cache Type Information
        f.write("\n ----Cache Type Information ----\n")
        if sudo_flag:
            f.write(run_command("sudo dmidecode -t cache | uniq"))
        else:
            f.write("Failed to retrieve the cache type information as user does not have root privileges.\n")

        # Disk/Storage Information
        f.write("\n==== Disk/Storage Information ====\n")
        f.write(run_command("df -h"))
        f.write("\n-> lsblk:\n")
        f.write(run_command("lsblk"))

        f.write("\n=========================================\n")
        f.write("  System Info Collection Complete\n")
        f.write("=========================================\n")

def main():
    """
    Arguments are captured and stored to variable.
    """
    available_cores = subprocess.check_output("lscpu | grep 'CPU(s):' | \
         awk '{print $2}' | head -n 1", shell=True).decode('utf-8').strip()

    parser = argparse.ArgumentParser(prog='bench', description='This program will perform the benchmarking: TBM, GBM, FBM',
                                     epilog="See './bench.py [gbm, tbm, fbm] -h' for more information on a specific benchmark")

    # Create subparsers for different benchmarking tools
    subparsers = parser.add_subparsers(dest='benchmark', required=True)

    # Common arguments for all benchmarks
    common_parser = argparse.ArgumentParser(add_help=False)

    common_parser.add_argument("func", help="LibMem supported functions",
                            type=str, choices = libmem_funcs,default="memcpy")

    common_parser.add_argument("-r", "--range", nargs = 2, help="range of data\
                                lengths to be benchmarked. Format: NUMBER[UNIT]\
                                where UNIT is optional and can be B, KB, MB, or GB (case insensitive).\
                                Examples: '8B', '16KB', '9MB', '1GB'\
                                Memory functions [8B - 32MB]\
                                String functions [8B - 4KB]\
                                Memory functions can be benchmarked upto 1GB",
                            type=parse_size_with_unit)
    common_parser.add_argument("-perf", help = "performance runs for LibMem.\
                            Default is performance benchmarking comparison. \
                            l - Performance analysis for LibMem.\
                            g - Performance analysis for Glibc only.\
                            c - Comparison report between old and new LibMem runs.\
                            d - Defalut report Glibc vs LibMem.",
                            type = str, choices = ['l', 'g', 'c','d'], default = 'd')

    common_parser.add_argument("-t", "--iterator", help = "iteration pattern for a \
                            given range of data sizes. Default is shift left\
                            by 1 of starting size - '<<1'.",
                            type = int, default = 0)

    common_parser.add_argument("-x", "--core_id",
                               help=f"CPU core_id on which benchmark has to be performed.\
                            Default choice of core-id is 8. Valid range is \
                            [0..{int(available_cores) - 1}]",
                            type=int,default=8)

    common_parser.add_argument("-sys", "--system_info", help = "logs system_info details like cpu freq,\
                                cache info, bios info, etc.", action="store_true")

    common_parser.add_argument("-bestperf", "--best_performance", help = "runs benchmark 3 times and selects \
                                the best throughput for each size from those iterations", action="store_true")

    # Subparser for GBM with additional options
    gbm_parser = subparsers.add_parser('gbm', parents=[common_parser], help='GoogleBench Benchmarking Tool')
    group = gbm_parser.add_mutually_exclusive_group()
    gbm_parser.add_argument("-m", "--mode", help = "type of benchmarking mode:\
                            c - cached, u - un-cached",\
                            type = str, choices = ['c', 'u'], \
                            default = 'c')

    #Align and Page are mutually_exclusive options
    group.add_argument("-a", "--align", help = "alignemnt of source\
                                and destination addresses: a - aligned\
                                u - unaligned, and d - default alignment\
                                is random.",\
                                type = str, choices = ['a','u','d'],  default = 'd')

    group.add_argument("-p", "--page", help = "Page boundary options\
                                x - page_cross, t - page_tail",\
                                type = str, choices = ['x','t'],  default = 'n')

    gbm_parser.add_argument("-s", "--spill", help = "cache spilling mode applicable only for\
                                aligned and unaligned: l - less_spill,\
                                m - more_spill, default is no-spill",\
                                type = str, choices = ['l', 'm',],  default = 'n')

    # Only add the overlap argument for memmove function
    class OverlapAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            # This will only be called if the argument is actually provided
            if 'func' in namespace and namespace.func != 'memmove':
                parser.error("The -o/--overlap option can only be used with the memmove function")
            setattr(namespace, self.dest, values)

    gbm_parser.add_argument("-o", "--overlap", help = "choose the overlapping behavior for memmove only: \
                                f - forward overlap, b - backward overlap, d - both (default)",\
                                type = str, choices = ['f', 'b', 'd'], default = 'd',
                                action=OverlapAction)

    gbm_parser.add_argument("-i", "--repetitions", help = "Number of repitations for\
                            performance measurement. Default value is \
                            set to 10 iterations.",
                        type = int, default = 10)

    gbm_parser.add_argument("-w", "--warm_up", help = "time in seconds\
                                Default value is set to 1sec.",
                            type = float, default = 1)

    gbm_parser.add_argument("-preload", help = "Enables LD_PRELOAD for running bench",
                          type = str, choices = ['y', 'n'], default = 'y')

    # Subparser for TBM
    tbm_parser = subparsers.add_parser('tbm', parents=[common_parser], help='TinyMembench Benchmarking Tool')

    # Subparser for FBM
    fbm_parser = subparsers.add_parser('fbm', parents=[common_parser], help='Fleetbench Benchmarking Tool')

    fbm_parser.add_argument("-mem_alloc", help="specify the memory allocator for FleetBench",
                               type=str, choices=['tcmalloc', 'glibc'], default='glibc')

    fbm_parser.add_argument("-i", "--repetitions", help = "Number of repitations for\
                            performance measurement. Default value is \
                            set to 100 iterations.",
                        type = int, default = 100)
    args = parser.parse_args()

    # Ensure memory_operation is set only if mode exists
    if hasattr(args, 'mode'):
        args.memory_operation = args.mode
        if args.memory_operation not in ['c', 'u']:
            args.memory_operation = 'c'
    else:
        args.memory_operation = 'c'

    if args.benchmark == 'gbm':
        args.bench_name = 'GooglBench_Cached'
        if args.memory_operation == 'u':
            args.bench_name = 'GooglBench_UnCached'
    elif args.benchmark == 'tbm':
        args.bench_name = 'TinyMemBench'
    elif args.benchmark == 'fbm':
        args.bench_name = 'FleeteBench'

    args.result_dir = 'out/' + args.bench_name + '/' + args.func + '/' \
        + datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

    # Create result directory
    os.makedirs(args.result_dir, exist_ok=False)

    # Prompt for two directory paths when -perf c option is used
    if hasattr(args, 'perf') and args.perf == 'c':
        print("\nPerformance comparison mode (Old vs New LibMem) selected.")
        print("Enter paths to directories containing perf_values.csv files:")

        # Get path to first directory (old version)
        old_dir_path = input("Enter path to OLD LibMem perf_values.csv directory: ").strip()
        while not os.path.isdir(old_dir_path):
            print(f"Error: Directory '{old_dir_path}' does not exist or is not accessible.")
            old_dir_path = input("Enter path to OLD LibMem perf_values.csv directory: ").strip()

        # Get path to second directory (new version)
        new_dir_path = input("Enter path to NEW LibMem perf_values.csv directory: ").strip()
        while not os.path.isdir(new_dir_path):
            print(f"Error: Directory '{new_dir_path}' does not exist or is not accessible.")
            new_dir_path = input("Enter path to NEW LibMem perf_values.csv directory: ").strip()

        # Store the paths in args for later use
        args.old_perf_dir = old_dir_path
        args.new_perf_dir = new_dir_path

        print(f"Using OLD LibMem perf_values.csv from: {old_dir_path}")
        print(f"Using NEW LibMem perf_values.csv from: {new_dir_path}")

    if getattr(args, 'system_info', False):
        # Create a timestamped output file for system info
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(args.result_dir, f"system_info_{timestamp}.txt")
        # Collect system information
        collect_system_info(output_file, args.core_id)

    # Set the default range based on the func argument
    if args.range is None:
        if args.func in libmem_string:
            args.range = [8, 4096]  # 8B to 4KB for string functions
        else:
            args.range = [8, 32 * 1024 * 1024]  # 8B to 32MB for memory functions

    # Check if the range is valid
    if args.range[0] > args.range[1] :
        raise argparse.ArgumentTypeError(
            f"The first size must be less than or equal to the second size: {args.range[0]} < {args.range[1]}"
        )

    return vars(args)

if __name__ == "__main__":
    try:
        # Check if numactl is installed
        subprocess.check_output(['which', 'numactl'])
    except subprocess.CalledProcessError:
        # numactl is not installed, exiting the program
        print("numactl utility NOT found. Please install it.")
        exit(1)
    myparser = main()

    obj = Bench(ARGS=myparser)
    obj()
