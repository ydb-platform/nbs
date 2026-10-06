#!/usr/bin/env python3
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

# Fix for BCC compatibility with Python 3.10+
# BCC version 0.12.0 has a bug where it imports MutableMapping from collections
# instead of collections.abc, which causes ImportError in Python 3.10+
import sys
import collections
import collections.abc
import ctypes as ct
import os
import time
import argparse
import getpass
import logging
import subprocess
import signal
import tempfile
import stat
import atexit
import warnings
from io import StringIO  # For capturing print output
import datetime  # For timestamping log files
import platform  # For system information
import socket    # For hostname information

# Add the system dist-packages path for Python 3.10 to find BCC
dist_packages_path = '/usr/lib/python3/dist-packages'
if dist_packages_path not in sys.path:
    sys.path.insert(0, dist_packages_path)

# Monkey patch to make MutableMapping available in collections module
if not hasattr(collections, 'MutableMapping'):
    collections.MutableMapping = collections.abc.MutableMapping

# Global variables for BCC version info (will be set later)
BCC_VERSION = None
BCC_NEEDS_CLEANUP_FIX = False
BPF = None
bcc_symbol = None
bcc_symbol_option = None
lib = None
my_bcc_symbol_option = None


def print_bcc_error_and_exit(error_type, **kwargs):
    """
    Print BCC-related error messages and exit

    Args:
        error_type: 'version_too_old' or 'import_failed'
        **kwargs: Additional parameters like 'error', 'current_version', 'min_version'
    """
    print("")

    if error_type == 'version_too_old':
        current_version = kwargs.get('current_version', 'Unknown')
        min_version = kwargs.get('min_version', '0.8.0')
        print(f"ERROR: BCC version {current_version} is too old.")
        print(f"Minimum required version: {min_version}")
    elif error_type == 'import_failed':
        error = kwargs.get('error', 'Unknown error')
        print(f"Failed to import BCC: {error}")

    print("")
    print("Please install BCC for your Linux distribution.")
    print("See installation instructions in 'profiler.md'")
    sys.exit(1)


def initialize_bcc():
    """
    Initialize BCC with version detection and conditional fixes

    Minimum BCC version required: 0.8.0
    - Versions below 0.8.0 lack essential eBPF features needed for uprobe attachment
    - Versions 0.8.0 and above support all required functionality
    - Versions 0.12.0 and below require cleanup error suppression patches
    """
    global BCC_VERSION, BCC_NEEDS_CLEANUP_FIX, BPF, bcc_symbol, bcc_symbol_option, lib

    # Minimum required BCC version (must support all eBPF features used by this profiler)
    MIN_BCC_VERSION = (0, 8, 0)

    # Import BCC with version detection and conditional fixes
    try:
        import bcc
        BCC_VERSION = getattr(bcc, '__version__', 'Unknown')
        print(f"Detected BCC version: {BCC_VERSION}")

        # Check minimum version requirement
        if BCC_VERSION != 'Unknown':
            try:
                # Parse version string (e.g., "0.12.0" -> (0, 12, 0))
                version_parts = tuple(map(int, BCC_VERSION.split('.')))

                # Check if version meets minimum requirement
                if version_parts < MIN_BCC_VERSION:
                    min_version_str = '.'.join(map(str, MIN_BCC_VERSION))
                    print_bcc_error_and_exit('version_too_old',
                                            current_version=BCC_VERSION,
                                            min_version=min_version_str)
                else:
                    min_version_str = '.'.join(map(str, MIN_BCC_VERSION))
                    print(f"BCC version {BCC_VERSION} meets minimum requirement ({min_version_str})")

            except (ValueError, AttributeError):
                # If version parsing fails, we can't verify compatibility
                print(f"WARNING: Could not parse BCC version '{BCC_VERSION}'")
                print(f"Cannot verify minimum version requirement. Proceeding with caution...")
        else:
            # If version is unknown, we can't verify compatibility
            print("WARNING: BCC version unknown")
            print("Cannot verify minimum version requirement. Proceeding with caution...")

        # Check if this is a version that needs the cleanup fix
        BCC_NEEDS_CLEANUP_FIX = False
        if BCC_VERSION != 'Unknown':
            try:
                # Parse version string (e.g., "0.12.0" -> (0, 12, 0))
                version_parts = tuple(map(int, BCC_VERSION.split('.')))
                # Versions 0.12.0 and below have cleanup issues
                if version_parts <= (0, 12, 0):
                    BCC_NEEDS_CLEANUP_FIX = True
                    print(f"BCC version {BCC_VERSION} requires cleanup error suppression")
                else:
                    print(f"BCC version {BCC_VERSION} should not need cleanup fixes")
            except (ValueError, AttributeError):
                # If version parsing fails, assume we need the fix for safety
                BCC_NEEDS_CLEANUP_FIX = True
                print(f"Could not parse BCC version '{BCC_VERSION}', applying cleanup fix as precaution")
        else:
            # If version is unknown, apply the fix for safety
            BCC_NEEDS_CLEANUP_FIX = True
            print("BCC version unknown, applying cleanup fix as precaution")

    except ImportError as e:
        print_bcc_error_and_exit('import_failed', error=e)

    from bcc import BPF
    from bcc.libbcc import lib, bcc_symbol, bcc_symbol_option

    # Apply BPF cleanup patch only if needed
    if BCC_NEEDS_CLEANUP_FIX:
        print("Applying BCC cleanup error suppression patch...")

        # Monkey patch BPF cleanup to suppress errors
        original_bpf_cleanup = BPF.cleanup

        def silent_bpf_cleanup(self):
            """Wrapper around BPF.cleanup that suppresses error output"""
            try:
                # Temporarily redirect stderr to suppress cleanup errors
                old_stderr = sys.stderr
                with open(os.devnull, 'w') as devnull:
                    sys.stderr = devnull
                    try:
                        original_bpf_cleanup(self)
                    except:
                        pass  # Ignore all cleanup errors
                    finally:
                        sys.stderr = old_stderr
            except:
                pass  # Ignore any other errors

        # Apply the monkey patch
        BPF.cleanup = silent_bpf_cleanup
    else:
        print("BCC cleanup patch not needed for this version")

    return bcc, BPF, lib, bcc_symbol, bcc_symbol_option

LOG = logging.getLogger(__name__)

# Work around a bug in older versions of bcc.libbcc
# This affects BCC versions with incomplete bcc_symbol_option structure
# Note: This check will be performed after BCC is initialized
def check_libbcc_workaround():
    """Check if libBCC workaround is needed and apply if necessary"""
    global my_bcc_symbol_option

    if len(bcc_symbol_option._fields_) == 3:
        print(f"Applying libBCC workaround for BCC version {BCC_VERSION}")
        class patched_bcc_symbol_option(ct.Structure):
            _fields_ = [
                ('use_debug_file', ct.c_int),
                ('check_debug_file_crc', ct.c_int),
                ('lazy_symbolize', ct.c_int),
                ('use_symbol_type', ct.c_uint),
            ]
        my_bcc_symbol_option = patched_bcc_symbol_option
    else:
        print(f"libBCC workaround not needed for BCC version {BCC_VERSION}")
        my_bcc_symbol_option = bcc_symbol_option

    return my_bcc_symbol_option


def sum_percpu(percpu_values):
    """Sum per-CPU values from BPF_PERCPU_ARRAY map lookup.

    Per-CPU BPF maps return one value per CPU core. This function
    aggregates them into a single total, providing a lock-free,
    low-overhead alternative to atomic counters.

    Args:
        percpu_values: An iterable of per-CPU values returned by
            BPF_PERCPU_ARRAY table lookup, or a single ctypes value
            for non-per-CPU maps (fallback).

    Returns:
        The sum of all per-CPU values as an integer.
    """
    try:
        # BCC returns per-CPU values as an iterable (list/array)
        # Each element may be a ctypes value with .value attribute
        return sum(v.value if hasattr(v, 'value') else int(v) for v in percpu_values)
    except TypeError:
        # Fallback for non-per-CPU maps (single ctypes value)
        if hasattr(percpu_values, 'value'):
            return percpu_values.value
        return int(percpu_values)


# Number of log2 histogram buckets (covers sizes from 0 to 2^64)
LOG2_HIST_BUCKETS = 65


def print_log2_hist_percpu(table, label="size:"):
    """Print a log2 histogram from a BPF_PERCPU_ARRAY with LOG2_HIST_BUCKETS entries.

    Aggregates per-CPU values for each bucket and displays a distribution
    chart matching the format used by BCC's built-in print_log2_hist().

    Args:
        table: A BPF_PERCPU_ARRAY table with LOG2_HIST_BUCKETS entries,
            where each entry holds per-CPU u64 counters.
        label: Column label for the size axis (default: "size:").
    """
    vals = {}
    for i in range(LOG2_HIST_BUCKETS):
        try:
            total = sum_percpu(table[ct.c_int(i)])
            if total > 0:
                vals[i] = total
        except (KeyError, IndexError):
            continue

    if not vals:
        return

    max_val = max(vals.values())
    max_bar = 40

    print(f"     {label:<20} : count     distribution")
    for bucket in sorted(vals.keys()):
        count = vals[bucket]
        if bucket == 0:
            low, high = 0, 0
        else:
            low = 1 << (bucket - 1)
            high = (1 << bucket) - 1

        bar_len = int(count * max_bar / max_val) if max_val > 0 else 0
        bar = '*' * bar_len
        padding = ' ' * (max_bar - bar_len)

        print(f"     {low:>9} -> {high:<12} : {count:<8} |{bar}{padding}|")


def hist_percpu_sum(table):
    """Return the total count across all buckets of a per-CPU histogram.

    Args:
        table: A BPF_PERCPU_ARRAY table with LOG2_HIST_BUCKETS entries.

    Returns:
        The sum of all per-CPU values across all buckets.
    """
    total = 0
    for i in range(LOG2_HIST_BUCKETS):
        try:
            total += sum_percpu(table[ct.c_int(i)])
        except (KeyError, IndexError):
            continue
    return total


class FuncInfo():
    STT_GNU_IFUNC = 1 << 10
    def __init__(self, libname, name, symbol, argSZ = 3, argSRC = 2, argDST = 1):
        self.libname = libname
        self.name = name
        self.symbol = symbol
        self.argSZ = argSZ    # Argument index for size parameter
        self.argSRC = argSRC  # Argument index for source pointer
        self.argDST = argDST  # Argument index for destination pointer
        self.is_indirect = False
        self.indirect_symbol = None

    def attach_point(self):
        """ Returns a tuple to compare if multiple FuncInfo would attach to the same point """
        return (self.libname, self.indirect_func_offset if self.is_indirect else self.symbol)

    def resolve_symbol(self):
        LOG.debug("Resolving symbol for function: %s (library: %s, symbol: %s)", self.name, self.libname, self.symbol)
        new_symbol = self._get_indirect_function_sym(self.libname, self.symbol)
        if not new_symbol:
            LOG.debug('%s is not an indirect function', self.name)
            self.is_indirect = False
        else:
            LOG.debug('%s IS an indirect function', self.name)
            self.is_indirect = True
            self.indirect_symbol = new_symbol
            LOG.debug("Indirect symbol resolved: name=%s, offset=0x%x", new_symbol.name, new_symbol.offset)
            self._find_impl_func_offset()

        # Log the final resolution status
        if self.is_indirect:
            LOG.debug("Function %s resolved as indirect with offset: 0x%x", self.name, self.indirect_func_offset)
        else:
            LOG.debug("Function %s resolved as direct symbol: %s", self.name, self.symbol)

    def attach(self, b, pid, fn_name):
        try:
            if self.is_indirect:
                LOG.debug("Attaching to indirect function %s at address 0x%x in PID %d",
                          self.name, self.indirect_func_offset, pid)
                b.attach_uprobe(name=ct.cast(self.indirect_symbol.module, ct.c_char_p).value,
                           addr=self.indirect_func_offset, fn_name=fn_name, pid=pid)
            else:
                LOG.debug("Attaching to function %s in %s for PID %d",
                          self.symbol, self.libname, pid)
                b.attach_uprobe(name=self.libname, sym=self.symbol, fn_name=fn_name, pid=pid)
            return True
        except Exception as e:
            LOG.error("Failed to attach to %s: %s", self.name, str(e))
            return False

    def _get_indirect_function_sym(self, module, symname):
        LOG.debug("Resolving indirect function symbol: module=%s, symbol=%s", module, symname)
        sym = bcc_symbol()
        sym_op = my_bcc_symbol_option()
        sym_op.use_debug_file = 1
        sym_op.check_debug_file_crc = 1
        sym_op.lazy_symbolize = 1
        sym_op.use_symbol_type = FuncInfo.STT_GNU_IFUNC
        ct.set_errno(0)
        retval = lib.bcc_resolve_symname(
                module.encode(),
                symname.encode(),
                0x0,
                0,
                ct.cast(ct.byref(sym_op), ct.POINTER(bcc_symbol_option)),
                ct.byref(sym),
        )
        LOG.debug('Got sym name: %s, offset: 0x%x', sym.name, sym.offset)
        LOG.debug("Lookup for func %s returned %d", symname, retval)
        if retval < 0:
            LOG.debug("Failed to resolve symbol %s as IFUNC in module %s. ERRNO: %d (this is expected for regular symbols)", symname, module, ct.get_errno())
            return None
        else:
            LOG.debug("Resolved symbol: name=%s, offset=0x%x", sym.name, sym.offset)
            return sym

    def _find_impl_func_offset(self):
        LOG.debug("Finding implementation offset for function: %s", self.name)
        resolv_func_addr = None
        impl_func_addr = None

        SUBMIT_FUNC_ADDR_BPF_TEXT = """
#include <uapi/linux/ptrace.h>

BPF_PERF_OUTPUT(impl_func_addr);
void submit_impl_func_addr(struct pt_regs *ctx) {
    u64 addr = PT_REGS_RC(ctx);
    impl_func_addr.perf_submit(ctx, &addr, sizeof(addr));
}

BPF_PERF_OUTPUT(resolv_func_addr);
int submit_resolv_func_addr(struct pt_regs *ctx) {
    u64 rip = PT_REGS_IP(ctx);
    resolv_func_addr.perf_submit(ctx, &rip, sizeof(rip));
    return 0;
}
"""

        def set_impl_func_addr(cpu, data, size):
            addr = ct.cast(data, ct.POINTER(ct.c_uint64)).contents.value
            nonlocal impl_func_addr
            impl_func_addr = addr

        def set_resolv_func_addr(cpu, data, size):
            addr = ct.cast(data, ct.POINTER(ct.c_uint64)).contents.value
            nonlocal resolv_func_addr
            resolv_func_addr = addr

        LOG.debug("Attaching BPF probes to resolve implementation offset for %s", self.name)
        try:
            b = BPF(text=SUBMIT_FUNC_ADDR_BPF_TEXT)
            b.attach_uprobe(name=self.libname, addr=self.indirect_symbol.offset, fn_name=b'submit_resolv_func_addr', pid=os.getpid())
            b['resolv_func_addr'].open_perf_buffer(set_resolv_func_addr)
            b.attach_uretprobe(name=self.libname, addr=self.indirect_symbol.offset, fn_name=b"submit_impl_func_addr", pid=os.getpid())
            b['impl_func_addr'].open_perf_buffer(set_impl_func_addr)

            LOG.debug("Waiting for the first call to %s to resolve addresses", self.name)
            libc = ct.CDLL("libc.so.6")

            input_buf = ct.create_string_buffer(b"test\0")
            output_buf = ct.create_string_buffer(10)
            libc.__getattr__(self.name)(output_buf, input_buf, 4)

            max_poll_attempts = 50  # Timeout after 50 attempts (~5 seconds)
            poll_attempt = 0
            while True:
                try:
                    if resolv_func_addr and impl_func_addr:
                        b.detach_uprobe(name=self.libname, addr=self.indirect_symbol.offset, pid=os.getpid())
                        b.detach_uretprobe(name=self.libname, addr=self.indirect_symbol.offset, pid=os.getpid())
                        b.cleanup()
                        break
                    b.perf_buffer_poll(timeout=100)  # 100ms timeout per poll
                    poll_attempt += 1
                    if poll_attempt >= max_poll_attempts:
                        LOG.error("Timed out waiting for IFUNC resolution of %s after %d attempts", self.name, max_poll_attempts)
                        b.cleanup()
                        break
                except KeyboardInterrupt:
                    LOG.warning("Interrupted while resolving %s", self.name)
                    exit()

            LOG.debug("IFUNC resolution completed for %s", self.name)
            LOG.debug("\tResolver function address      : 0x%x", resolv_func_addr)
            LOG.debug("\tResolver function offset       : 0x%x", self.indirect_symbol.offset)
            LOG.debug("\tFunction implementation address: 0x%x", impl_func_addr)
            impl_func_offset = impl_func_addr - resolv_func_addr + self.indirect_symbol.offset
            LOG.debug("\tFunction implementation offset : 0x%x", impl_func_offset)
            self.indirect_func_offset = impl_func_offset
        except Exception as e:
            LOG.error("Failed to resolve implementation offset for %s: %s", self.name, str(e))
            self.indirect_func_offset = 0  # Fallback to a default offset
            LOG.warning("Fallback: Function %s assigned offset: 0x%x", self.name, self.indirect_func_offset)


def parse_alignment_value(alignment_str):
    """Parse an alignment value string and return the alignment in bytes.

    Accepts numeric values (interpreted as bytes) or human-readable forms
    with suffixes: B, KB, MB, GB.

    Args:
        alignment_str: A string like '64', '64B', '4KB', '2MB', '1GB'.

    Returns:
        The alignment value in bytes as an integer.

    Raises:
        ValueError: If the alignment string is invalid or the value is not
            a power of two.
    """
    if alignment_str is None:
        return 0

    s = alignment_str.strip().upper()

    # Map of suffixes to multipliers
    suffixes = {
        'GB': 1024 * 1024 * 1024,
        'MB': 1024 * 1024,
        'KB': 1024,
        'B': 1,
    }

    multiplier = 1
    for suffix, mult in suffixes.items():
        if s.endswith(suffix):
            s = s[:-len(suffix)].strip()
            multiplier = mult
            break

    try:
        value = int(s)
    except ValueError:
        raise ValueError(
            f"Invalid alignment value: '{alignment_str}'. "
            f"Expected a number optionally followed by B, KB, MB, or GB "
            f"(e.g., 64, 64B, 4KB, 2MB, 1GB).")

    alignment_bytes = value * multiplier

    if alignment_bytes <= 0:
        raise ValueError(f"Alignment must be a positive value, got {alignment_bytes}")

    # Check that alignment is a power of two
    if (alignment_bytes & (alignment_bytes - 1)) != 0:
        raise ValueError(
            f"Alignment must be a power of two, got {alignment_bytes} bytes. "
            f"Valid examples: 16, 32, 64, 128, 256, 512, 1024, 4096, etc.")

    return alignment_bytes


def format_alignment_label(alignment_bytes):
    """Return a human-readable label for the given alignment in bytes.

    Args:
        alignment_bytes: Alignment value in bytes (must be a power of two).

    Returns:
        A string like '64B', '4KB', '2MB', or '1GB'.
    """
    if alignment_bytes >= 1024 * 1024 * 1024 and alignment_bytes % (1024 * 1024 * 1024) == 0:
        return f"{alignment_bytes // (1024 * 1024 * 1024)}GB"
    elif alignment_bytes >= 1024 * 1024 and alignment_bytes % (1024 * 1024) == 0:
        return f"{alignment_bytes // (1024 * 1024)}MB"
    elif alignment_bytes >= 1024 and alignment_bytes % 1024 == 0:
        return f"{alignment_bytes // 1024}KB"
    else:
        return f"{alignment_bytes}B"


def _build_bpf_text(functions_dist, functions_cnt, target_pid=-1, alignment_bytes=0, verbose=False, track_timing=False):
    text = """
#include <uapi/linux/ptrace.h>

/*
 * Per-CPU maps eliminate cross-CPU cache-line contention.
 * No atomic operations needed - each CPU increments its own copy.
 * Userspace aggregates per-CPU values when reading.
 */
"""

    # Add debug counter for verbose mode (per-CPU for lock-free operation)
    if verbose:
        text += """
// Debug counter - per-CPU for lock-free, low-overhead profiling
BPF_PERCPU_ARRAY(total_calls, u64, 1);
"""

    # Add a counter to track global function call distribution
    text += """
// Per-CPU function call counters for lock-free distribution analysis
"""

    # Combine all functions for more unified processing
    all_functions = functions_dist + functions_cnt

    # Add per-CPU counters for all functions (no atomics needed)
    for func in functions_dist:
        text += f"BPF_PERCPU_ARRAY(dist_{func.name}, u64, 1);\n"
    for func in functions_cnt:
        text += f"BPF_PERCPU_ARRAY(callCount_{func.name}, u64, 1);\n"

    # Add per-CPU alignment counters if requested
    if alignment_bytes > 0:
        alignment_mask = alignment_bytes - 1
        alignment_label = format_alignment_label(alignment_bytes)
        text += f"\n// Per-CPU memory alignment counters ({alignment_label} alignment)\n"
        for func in all_functions:
            if func.argSRC > 0:
                text += f"BPF_PERCPU_ARRAY(aligned_src_{func.name}, u64, 1);\n"
            if func.argDST > 0:
                text += f"BPF_PERCPU_ARRAY(aligned_dst_{func.name}, u64, 1);\n"
            if func.argSRC > 0 and func.argDST > 0:
                text += f"BPF_PERCPU_ARRAY(aligned_both_{func.name}, u64, 1);\n"

    # Add timing tracking maps if requested (per-CPU for lock-free operation)
    if track_timing:
        text += "\n// Per-CPU timing tracking maps\n"
        text += """
// Structure to store timing context (timestamp and size)
struct timing_ctx {
    u64 start_ts;
    u32 size_bucket;
};

// Map to store timing context for each thread
BPF_HASH(timing_context, u64, struct timing_ctx);

"""

        # Add per-CPU timing sum arrays by size ranges for distribution functions
        text += "// Per-CPU total time accumulator for each size bucket\n"
        for func in functions_dist:
            text += f"BPF_PERCPU_ARRAY(timing_sum_{func.name}, u64, 64);\n"
            text += f"BPF_PERCPU_ARRAY(timing_count_{func.name}, u64, 64);\n"

        # For count functions, we track total time since there's no size parameter
        for func in functions_cnt:
            text += f"BPF_PERCPU_ARRAY(timing_total_{func.name}, u64, 1);\n"

    text += "\n// Function implementations\n"

    # Generate code for ALL functions using a unified approach
    for func in all_functions:
        # Determine if this is a distribution function
        is_dist_func = func in functions_dist

        # Add per-CPU histogram array for distribution functions
        # Using BPF_PERCPU_ARRAY with LOG2_HIST_BUCKETS entries for log2 distribution
        if is_dist_func:
            text += f"BPF_PERCPU_ARRAY(lenHist_{func.name}, u64, {LOG2_HIST_BUCKETS});\n"

        text += f"""
int count_{func.name}(struct pt_regs *ctx) {{
    int zero = 0;
"""

        # Add conditional PID check only if a specific PID is targeted
        if target_pid > 0:
            text += """
    // Check if we're in the target process
    u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (pid != {0}) {{
        return 0;
    }}
""".format(target_pid)

        # Combined code path for both timing and non-timing modes
        if is_dist_func:
            # Distribution functions with size parameters
            text += """
"""
            # Conditional timing: Record timestamp FIRST if timing is enabled
            if track_timing:
                text += """    // TIMING: Record start timestamp FIRST (minimizes overhead)
    u64 start_ts = bpf_ktime_get_ns();

"""

            # Common code for all distribution functions (per-CPU, no atomics needed)
            text += """    // Get size argument and compute bucket
    size_t len = PT_REGS_PARM{0}(ctx);
    u32 bucket_idx = bpf_log2l(len);

    // Update per-CPU histogram and counter (no atomics needed)
    u64 *hist_count = lenHist_{1}.lookup(&bucket_idx);
    if (hist_count) (*hist_count)++;
    u64 *func_count = dist_{1}.lookup(&zero);
    if (func_count) (*func_count)++;
""".format(func.argSZ, func.name)

            # Conditional timing context storage
            if track_timing:
                text += """
    // Store timing context (only when timing enabled)
    u64 tid = bpf_get_current_pid_tgid();
    struct timing_ctx ctx_data = {};
    ctx_data.start_ts = start_ts;
    ctx_data.size_bucket = bucket_idx;
    timing_context.update(&tid, &ctx_data);
"""
        else:
            # Count functions without size parameters (per-CPU, no atomics needed)
            text += """    // Update per-CPU function counter (no atomics needed)
    u64 *count = callCount_{0}.lookup(&zero);
    if (count) (*count)++;
""".format(func.name)

            # Conditional timing for count functions
            if track_timing:
                text += """
    // TIMING: Record timestamp and context (only when timing enabled)
    u64 start_ts = bpf_ktime_get_ns();
    u64 tid = bpf_get_current_pid_tgid();
    struct timing_ctx ctx_data = {};
    ctx_data.start_ts = start_ts;
    ctx_data.size_bucket = 0;
    timing_context.update(&tid, &ctx_data);
"""

        # Add debug counter for verbose mode (per-CPU, no atomics needed)
        if verbose:
            text += """
    // Update per-CPU total call counter for debugging (no atomics needed)
    u64 *total = total_calls.lookup(&zero);
    if (total) (*total)++;
"""
        else:
            text += "    // Debug counter disabled in non-verbose mode\n"

        # Add alignment checks if requested - this is common for both function types
        if alignment_bytes > 0:
            # Add source alignment check
            if func.argSRC > 0:
                text += """
    // Source pointer %s alignment check
    u64 src_ptr = PT_REGS_PARM%d(ctx);
    bool src_aligned = (src_ptr & 0x%xULL) == 0;

    if (src_aligned) {
        u64 *src_count = aligned_src_%s.lookup(&zero);
        if (src_count) (*src_count)++;
    }
""" % (alignment_label, func.argSRC, alignment_mask, func.name)

            # Add destination alignment check
            if func.argDST > 0:
                text += """
    // Destination pointer %s alignment check (per-CPU, no atomics needed)
    u64 dst_ptr = PT_REGS_PARM%d(ctx);
    bool dst_aligned = (dst_ptr & 0x%xULL) == 0;

    if (dst_aligned) {
        u64 *dst_count = aligned_dst_%s.lookup(&zero);
        if (dst_count) (*dst_count)++;
    }
""" % (alignment_label, func.argDST, alignment_mask, func.name)

            # Add both-aligned check if both source and destination are specified
            if func.argSRC > 0 and func.argDST > 0:
                text += """
    // Both source and destination %s aligned check (per-CPU, no atomics needed)
    if (src_aligned && dst_aligned) {
        u64 *both_count = aligned_both_%s.lookup(&zero);
        if (both_count) (*both_count)++;
    }
""" % (alignment_label, func.name)

        # Close the function
        text += """
    return 0;
}
"""

    # Add return probe functions for timing tracking
    if track_timing:
        text += "\n// Return probe functions for timing tracking\n"

        for func in all_functions:
            is_dist_func = func in functions_dist

            text += f"""
int count_{func.name}_return(struct pt_regs *ctx) {{
    u64 tid = bpf_get_current_pid_tgid();
    struct timing_ctx *ctx_data = timing_context.lookup(&tid);

    if (ctx_data == 0) {{
        return 0;  // No timing context recorded
    }}

    // Calculate elapsed time in nanoseconds
    u64 delta = bpf_ktime_get_ns() - ctx_data->start_ts;
    u32 size_bucket = ctx_data->size_bucket;

    // Clean up the timing context
    timing_context.delete(&tid);

"""

            if is_dist_func:
                # For distribution functions, accumulate time in the appropriate size bucket (per-CPU)
                text += f"""    // Accumulate time in the per-CPU size bucket (no atomics needed)
    u64 *time_sum = timing_sum_{func.name}.lookup(&size_bucket);
    if (time_sum) (*time_sum) += delta;

    // Increment per-CPU count for this size bucket
    u64 *time_count = timing_count_{func.name}.lookup(&size_bucket);
    if (time_count) (*time_count)++;

"""
            else:
                # For count functions, accumulate total time (per-CPU)
                text += f"""    // Accumulate per-CPU total time for count functions (no atomics needed)
    int zero = 0;
    u64 *total_time = timing_total_{func.name}.lookup(&zero);
    if (total_time) (*total_time) += delta;

"""

            text += """    return 0;
}
"""

    return text


def dedup_functions(all_funcs):
    keys = dict()
    for func in all_funcs:
        if func.attach_point() in keys.keys():
            LOG.warning("%s is a duplicate target to %s. Skipping tracing.", func.name, keys.get(func.attach_point()).name)
        else:
            keys[func.attach_point()] = func

    return keys.values()


def main():
    TARGET_DIST_FUNCTIONS = [
        # libname, name, symbol, size arg, src arg, dst arg
        FuncInfo('c', 'memcpy',  'memcpy'),
        FuncInfo('c', 'mempcpy', 'mempcpy'),
        FuncInfo('c', 'memcmp',  'memcmp'),
        FuncInfo('c', 'memmove', 'memmove'),
        FuncInfo('c', 'memset',  'memset'),
        FuncInfo('c', 'memchr',  'memchr'),
        FuncInfo('c', 'strncpy', 'strncpy'),
        FuncInfo('c', 'strncmp', 'strncmp'),
        FuncInfo('c', 'strncat', 'strncat')
    ]
    TARGET_CNT_FUNCTIONS = [
        # Functions without size parameters
        FuncInfo('c', 'strcpy',  'strcpy', argSZ = 0),
        FuncInfo('c', 'strcmp',  'strcmp', argSZ = 0),
        FuncInfo('c', 'strcat',  'strcat', argSZ = 0),
        FuncInfo('c', 'strlen',  'strlen', argSZ = 0, argSRC = 0),
        FuncInfo('c', 'strchr',  'strchr', argSZ = 0),
        FuncInfo('c', 'strstr',  'strstr', argSZ = 0),
    ]

    # Combine function names for argument parsing
    all_func_names = [f.name for f in TARGET_DIST_FUNCTIONS + TARGET_CNT_FUNCTIONS]

    p = argparse.ArgumentParser(
        prog = 'prof_libmem',
        description = 'Trace all calls to aocl-libmem replacable functions')
    p.add_argument('-i', '--interval', default=5, type=int,
        help = 'How often (in seconds) to report data')
    p.add_argument('-p', '--pid', type=int, default=-1,
        help = 'Trace only this PID, or -1 to trace entire system')
    p.add_argument('-f', '--functions', action='append',
        choices=all_func_names,  # Updated to include all function names
        help = 'Trace only functions listed')
    p.add_argument('-v', '--verbose', action='count', default=0)
    p.add_argument('-e', '--exec', nargs=argparse.REMAINDER,
        help = 'Executable and arguments to run and trace')
    p.add_argument('-t', '--time', type=int, default=None,
        help = 'Total time (in seconds) to run the trace')
    p.add_argument('-c', '--track-count-functions', action='store_true',
        help = 'Also track functions without size parameters (count only)')
    p.add_argument('-o', '--output', type=str, default=None,
        help = 'Output file for logging results (default: stdout)')
    p.add_argument('-d', '--debug-log', type=str, default=None,
        help = 'Separate file for detailed debug logs')
    p.add_argument('-a', '--check-alignment', type=str, default=None, nargs='?', const='64B',
        metavar='ALIGNMENT',
        help = 'Check memory alignment of arguments at the specified boundary. '
               'Default: 64B if no value given. '
               'Accepts values like 16, 32, 64, 128, 256, 512, 1024, 4096 (in bytes) '
               'or human-readable forms: 16B, 32B, 64B, 128B, 256B, 512B, 1KB, 2KB, 4KB, 2MB, 1GB. '
               'Only the single specified alignment is checked (e.g., 4KB checks 4096-byte alignment only).')
    p.add_argument('-T', '--track-timing', action='store_true',
        help = 'Track execution time for each function call and create timing histograms by size ranges')
    p.add_argument('-P', '--perf-profile', action='store_true',
        help = 'Enable perf-based profiling for low-overhead aggregate statistics (categorizes kernel vs library calls)')

    args = p.parse_args()

    # Parse and validate alignment value if provided
    alignment_bytes = 0
    alignment_label = ""
    if args.check_alignment:
        try:
            alignment_bytes = parse_alignment_value(args.check_alignment)
            alignment_label = format_alignment_label(alignment_bytes)
            print(f"Alignment check configured: {alignment_label} ({alignment_bytes} bytes, mask=0x{alignment_bytes - 1:x})")
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

    # Initialize BCC now that we know we need it (not just showing help)
    bcc, BPF, lib, bcc_symbol, bcc_symbol_option = initialize_bcc()

    # Check and apply libBCC workaround if needed
    my_bcc_symbol_option = check_libbcc_workaround()

    # Configure logging with file output if specified
    log_handlers = [logging.StreamHandler()]
    original_stdout = sys.stdout  # Save the original stdout for restoration later

    # Debug log file setup
    if args.debug_log:
        try:
            # Create a debug file handler with more detailed formatting
            debug_handler = logging.FileHandler(args.debug_log, mode='w')
            debug_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'))

            # Set this handler to DEBUG level regardless of overall verbosity
            debug_handler.setLevel(logging.DEBUG)
            log_handlers.append(debug_handler)

            print(f"Debug logs are being written to: {args.debug_log}")
        except Exception as e:
            print(f"Warning: Could not open debug log file {args.debug_log}: {e}")
            print("Debug logs will only be shown based on verbosity level")

    # Regular output file setup (existing code)
    if args.output:
        try:
            # Create a file handler for the log file
            file_handler = logging.FileHandler(args.output, mode='w')
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            log_handlers.append(file_handler)

            # redirect stdout to the log file
            class TeeOutput:
                def __init__(self, filename):
                    self.terminal = sys.stdout
                    self.log_file = open(filename, "a")  # Append mode to work with the logging
                    self.file_open = True

                def write(self, message):
                    self.terminal.write(message)
                    if (self.file_open):
                        try:
                            self.log_file.write(message)
                            self.log_file.flush()  # Ensure immediate writing
                        except (ValueError, IOError) as e:
                            # Handle file errors gracefully
                            pass

                def flush(self):
                    self.terminal.flush()
                    if (self.file_open):
                        try:
                            self.log_file.flush()
                        except (ValueError, IOError):
                            pass

                def close(self):
                    if (self.file_open):
                        try:
                            self.log_file.close()
                            self.file_open = False
                        except:
                            pass

                # Ensure proper cleanup when the object is garbage collected
                def __del__(self):
                    self.close()

            # Create and store the tee object so we can close it properly later
            tee_output = TeeOutput(args.output)
            sys.stdout = tee_output

            print(f"Output is being logged to: {args.output}")
            LOG.info(f"Logging started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"Warning: Could not open output file {args.output}: {e}")
            print("Continuing with stdout only")

    # Set the logger level based on verbosity
    if args.verbose >= 2:
        LOG.setLevel(logging.DEBUG)
    elif args.verbose:
        LOG.setLevel(logging.INFO)
    else:
        # Even with low verbosity, write DEBUG to debug log if specified
        LOG.setLevel(logging.INFO if args.debug_log else logging.WARNING)

    # Apply all log handlers
    for handler in log_handlers:
        LOG.addHandler(handler)

    # Print detailed header information
    current_time = datetime.datetime.now()
    header = f"""
=======================================================================
AOCL-LibMem Profiler - Execution Log
=======================================================================
Started at: {current_time.strftime('%Y-%m-%d %H:%M:%S')}

Runtime Information:
- Host: {socket.gethostname()}
- OS: {platform.system()} {platform.release()} ({platform.version()})
- Python: {platform.python_version()}
- User: {getpass.getuser()}
- BCC Version: {BCC_VERSION} (cleanup fix: {'enabled' if BCC_NEEDS_CLEANUP_FIX else 'not needed'})

Command-line parameters:
- Interval: {args.interval} seconds
"""

    if args.pid > 0:
        header += f"- Tracing PID: {args.pid}\n"

    if args.exec:
        header += f"- Executing: {' '.join(args.exec)}\n"

    if args.functions:
        header += f"- Tracing specific functions: {', '.join(args.functions)}\n"
    else:
        header += "- Tracing all supported memory functions\n"

    if args.track_count_functions:
        header += "- Including count-only functions (strcpy, strcmp, etc.)\n"

    if args.time:
        header += f"- Time limit: {args.time} seconds\n"

    if args.output:
        header += f"- Output log: {args.output}\n"

    if args.debug_log:
        header += f"- Debug log file: {args.debug_log}\n"

    if alignment_bytes > 0:
        header += f"- Checking memory alignment of arguments ({alignment_label} boundary)\n"

    if args.track_timing:
        header += "- Tracking execution time with timing histograms\n"

    if args.perf_profile:
        header += "- Perf profiling enabled (low-overhead sampling)\n"

    header += "- Verbosity level: " + ("Low" if args.verbose == 0 else
                                     "Medium" if args.verbose == 1 else
                                     "High")
    header += "\n=======================================================================\n"

    print(header)
    LOG.info("Profiler initialized with parameters: %s", str(args)[10:-1])  # Remove the "Namespace(" prefix and ")" suffix

    if args.functions:
        # Only trace functions that are explicitly listed
        dist_funcs = [x for x in TARGET_DIST_FUNCTIONS if x.name in args.functions]
        count_funcs = [x for x in TARGET_CNT_FUNCTIONS if x.name in args.functions]

        # If -f specified functions from count functions but -c not specified,
        # enable count function tracking automatically
        if count_funcs and not args.track_count_functions:
            LOG.info("Enabling count function tracking because count functions were specified in -f")
            args.track_count_functions = True
    else:
        # Trace all distribution functions by default, and optionally the count functions
        dist_funcs = TARGET_DIST_FUNCTIONS
        count_funcs = TARGET_CNT_FUNCTIONS if args.track_count_functions else []

    # Check if user is root
    if getpass.getuser() != 'root':
        LOG.error("This application must be run with superuser privledges")
        return

    LOG.info("Starting Symbol resolution")
    # Resolve symbols for both function types
    for func in dist_funcs + count_funcs:
        func.resolve_symbol()

    # Deduplicate functions separately for each type
    unique_dist_funcs = list(dedup_functions(dist_funcs))
    unique_count_funcs = list(dedup_functions(count_funcs))

    LOG.info("Attaching BPF tracers")
    LOG.info("Distribution functions: %s", [f.name for f in unique_dist_funcs])
    LOG.info("Count-only functions: %s", [f.name for f in unique_count_funcs])

    # Initialize these variables
    proc = None
    pgid = -1
    target_pid = -1

    if args.exec:
        COMMAND_TO_RUN = args.exec
        LOG.info("Launching command: {' '.join(COMMAND_TO_RUN)}")
        sys.stdout.flush()
        # Get the current process environment settings
        current_env = os.environ.copy()
        # Launch the process in a new session (its own process group)
        # Capture stdout/stderr in case the application prints anything
        proc = subprocess.Popen(
            COMMAND_TO_RUN,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            text = True,
            start_new_session = True,
            env = current_env
        )
        os.kill(proc.pid, signal.SIGSTOP)
        sys.stdout.flush()
        LOG.info("Process launched. PID: {proc.pid}")
        target_pid = proc.pid
        # Give the OS a moment to register the stopped state
        # time.sleep(0.5)

    else:
        target_pid = args.pid


    # Generate BPF code with proper function lists, including verbose and timing flags
    bpf_text = _build_bpf_text(unique_dist_funcs, unique_count_funcs, target_pid,
                              alignment_bytes, args.verbose > 0, args.track_timing)

    # Debug the generated BPF code if verbose
    if args.verbose >= 2:
        LOG.debug("Generated BPF program:\n%s", bpf_text)

    # Compile the eBPF program with error handling
    try:
        LOG.info("Compiling eBPF program...")
        b = BPF(text=bpf_text)
        LOG.info("eBPF program compiled successfully")
    except Exception as e:
        LOG.error("Failed to compile eBPF program: %s", str(e))
        LOG.error("Please refer to the installation and troubleshooting guide in 'profiler.md'")
        if args.verbose >= 2:
            LOG.error("eBPF program that failed to compile:\n%s", bpf_text)

        sys.exit(1)

    # Track successful attachments
    successful_attaches = 0

    # Attach distribution functions
    for funcInfo in unique_dist_funcs:
        fn_name='count_{}'.format(funcInfo.name).encode()
        if funcInfo.attach(b, target_pid, fn_name):
            funcInfo.histo = b['lenHist_{}'.format(funcInfo.name)]
            funcInfo.dist_counter = b['dist_{}'.format(funcInfo.name)]
            funcInfo.type = "dist"
            successful_attaches += 1
        else:
            LOG.error("Could not attach to %s, skipping", funcInfo.name)

    # Attach count-only functions
    for funcInfo in unique_count_funcs:
        try:
            fn_name='count_{}'.format(funcInfo.name).encode()
            if funcInfo.attach(b, target_pid, fn_name):
                try:
                    funcInfo.call_counter = b['callCount_{}'.format(funcInfo.name)]
                    # Per-CPU maps start at 0 on all CPUs, no initialization needed
                    LOG.debug("Successfully initialized per-CPU counter for %s", funcInfo.name)
                    funcInfo.type = "call"
                    successful_attaches += 1
                except Exception as e:
                    LOG.error("Failed to initialize counter for %s: %s", funcInfo.name, str(e))
            else:
                LOG.error("Could not attach to %s, skipping", funcInfo.name)
        except Exception as e:
            LOG.error("Error during function attachment for %s: %s", funcInfo.name, str(e))

    # Attach return probes for timing tracking if requested
    if args.track_timing:
        LOG.info("Attaching return probes for timing tracking...")
        timing_attach_count = 0

        for funcInfo in unique_dist_funcs + unique_count_funcs:
            try:
                ret_fn_name = 'count_{}_return'.format(funcInfo.name).encode()
                if funcInfo.is_indirect:
                    b.attach_uretprobe(name=ct.cast(funcInfo.indirect_symbol.module, ct.c_char_p).value,
                                      addr=funcInfo.indirect_func_offset, fn_name=ret_fn_name, pid=target_pid)
                else:
                    b.attach_uretprobe(name=funcInfo.libname, sym=funcInfo.symbol, fn_name=ret_fn_name, pid=target_pid)
                timing_attach_count += 1
                LOG.debug("Attached return probe for %s", funcInfo.name)
            except Exception as e:
                LOG.error("Failed to attach return probe for %s: %s", funcInfo.name, str(e))

        LOG.info("Successfully attached %d return probes for timing", timing_attach_count)

    if successful_attaches == 0:
        LOG.error("Failed to attach any probes. Exiting.")
        if args.exec:
            signal_handler(signal.SIGTERM, None)
        return

    LOG.info("Successfully attached %d/%d probes", successful_attaches, len(unique_dist_funcs) + len(unique_count_funcs))

    # Initialize perf profiling if requested
    perf_proc = None
    perf_data_file = None

    if args.perf_profile and args.exec:
        # Create temporary file for perf data
        perf_data_file = tempfile.NamedTemporaryFile(prefix='perf_', suffix='.data', delete=False)
        perf_data_path = perf_data_file.name
        perf_data_file.close()

        LOG.info("Starting perf record for low-overhead sampling...")
        # Use perf record with call-graph sampling at 999 Hz
        # -F 999: Sample at 999 Hz (high frequency for good coverage, but not too high)
        # -g: Record call graphs
        # -p: Target specific PID
        perf_cmd = ['perf', 'record', '-F', '999', '-g', '-p', str(target_pid), '-o', perf_data_path]

        try:
            perf_proc = subprocess.Popen(
                perf_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            LOG.info(f"Perf recording started (PID: {perf_proc.pid}), output file: {perf_data_path}")
            time.sleep(0.5)  # Give perf time to initialize
        except Exception as e:
            LOG.error(f"Failed to start perf record: {e}")
            LOG.warning("Continuing without perf profiling")
            args.perf_profile = False
            if perf_data_file:
                try:
                    os.unlink(perf_data_path)
                except:
                    pass

    # --- Resume the Process ---
    if args.exec:
        LOG.info("\nResuming process {target_pid} (SIGCONT)...")
        sys.stdout.flush()
        try:
            os.kill(target_pid, signal.SIGCONT)
            LOG.info("Process resumed.")
        except ProcessLookupError:
            LOG.error("Error: Process {target_pid} not found during SIGCONT. Did it exit while suspended?")
        except Exception as e:
            LOG.error("Error sending SIGCONT to process {target_pid}: {e}")

    sys.stdout.flush()


    # Debug counter to see total calls - only access if verbose mode is enabled
    total_calls = b["total_calls"] if args.verbose > 0 else None

    # Update print_call_distribution function to ensure accurate counts
    def print_call_distribution():
        print("\n--- Function Call Distribution (by Count) ---")

        # Collect all function call counts
        function_counts = []
        total_func_calls = 0

        # Get distribution counts for distribution functions
        for func in unique_dist_funcs:
            if hasattr(func, 'dist_counter'):
                # Read the counter value directly from BPF map for consistency
                count = sum_percpu(func.dist_counter[0])
                if count > 0:  # Only include functions with non-zero counts
                    function_counts.append((func.name, count))
                    total_func_calls += count

        # Get distribution counts for count-only functions
        for func in unique_count_funcs:
            if hasattr(func, 'call_counter'):
                # Sum per-CPU values for consistent count
                count = sum_percpu(func.call_counter[0])
                if count > 0:  # Only include functions with non-zero counts
                    function_counts.append((func.name, count))
                    total_func_calls += count

        # Sort by count in descending order
        function_counts.sort(key=lambda x: x[1], reverse=True)

        if total_func_calls == 0:
            print("No function calls recorded yet.")
            return

        # Calculate percentages and print
        max_name_length = max([len(name) for name, _ in function_counts]) if function_counts else 10
        print(f"{'Function':<{max_name_length+2}} {'Calls':<12} {'Percent':<8} {'Distribution'}")
        print("-" * (max_name_length + 40))

        for name, count in function_counts:
            percentage = (count / total_func_calls) * 100
            bar_length = int(percentage / 2)  # Scale to reasonable length
            bar = '*' * bar_length
            print(f"{name:<{max_name_length+2}} {count:<12} {percentage:>6.2f}%  |{bar}")

        # Show the sum and the global total for verification
        print(f"\nTotal function calls (sum of all functions): {total_func_calls}")

        if args.verbose > 0 and total_calls is not None:
            global_total = sum_percpu(total_calls[0])
            print(f"Global counter value: {global_total}")

            # If there's a discrepancy, log a warning
            if global_total != total_func_calls and global_total > 0:
                print(f"Note: There's a {abs(global_total - total_func_calls) / max(global_total, 1) * 100:.2f}% difference between the sum and global counter.")

    def print_time_distribution():
        """Print distribution of time spent across functions"""
        if not args.track_timing:
            return

        print("\n--- Function Time Distribution (by Execution Time) ---")

        # Collect all function time data
        function_times = []
        total_time_ns = 0

        # Get timing data for distribution functions
        for func in unique_dist_funcs:
            if hasattr(func, 'dist_counter'):
                count = sum_percpu(func.dist_counter[0])
                if count > 0:
                    # Sum per-CPU time across all size buckets
                    timing_sum_map = b[f'timing_sum_{func.name}']
                    func_total_time = sum(sum_percpu(timing_sum_map[i]) for i in range(64))

                    if func_total_time > 0:
                        function_times.append((func.name, func_total_time, count))
                        total_time_ns += func_total_time

        # Get timing data for count-only functions
        for func in unique_count_funcs:
            if hasattr(func, 'call_counter'):
                count = sum_percpu(func.call_counter[0])
                if count > 0:
                    func_total_time = sum_percpu(b[f'timing_total_{func.name}'][0])

                    if func_total_time > 0:
                        function_times.append((func.name, func_total_time, count))
                        total_time_ns += func_total_time

        # Sort by time in descending order
        function_times.sort(key=lambda x: x[1], reverse=True)

        if total_time_ns == 0:
            print("No timing data recorded yet.")
            return

        # Calculate percentages and print
        max_name_length = max([len(name) for name, _, _ in function_times]) if function_times else 10
        print(f"{'Function':<{max_name_length+2}} {'Total Time (µs)':<18} {'Avg (µs)':<12} {'Percent':<8} {'Distribution'}")
        print("-" * (max_name_length + 65))

        for name, time_ns, count in function_times:
            time_us = time_ns / 1000.0
            avg_us = time_us / count if count > 0 else 0
            percentage = (time_ns / total_time_ns) * 100
            bar_length = int(percentage / 2)  # Scale to reasonable length
            bar = '*' * bar_length
            print(f"{name:<{max_name_length+2}} {time_us:>16,.2f}  {avg_us:>10,.2f}  {percentage:>6.2f}%  |{bar}")

        # Show the total time
        total_time_ms = total_time_ns / 1_000_000.0
        total_time_s = total_time_ns / 1_000_000_000.0
        print(f"\nTotal execution time: {total_time_ns:,} ns ({total_time_ms:,.2f} ms / {total_time_s:.6f} s)")

        # Summary notes footer
        print("\nSummary Notes:")
        print("⚠️  Timing measurements include eBPF probe overhead (~100-500ns per call)")
        print("📊 Use timing data for relative comparisons, not absolute performance measurements")

    # Update the print_function_stats function to handle histograms correctly

    def print_function_stats(include_distribution=False):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n--- Function Statistics at {timestamp} ---")

        # Print histograms for distribution functions
        for func in unique_dist_funcs:
            # Always use dist_counter for consistency (aggregate per-CPU values)
            func_calls = sum_percpu(func.dist_counter[0]) if hasattr(func, 'dist_counter') else 0
            if func_calls == 0:
                continue  # Skip functions with no calls

            print(f"\n{'=' * 30} {func.name} ({func_calls} calls) {'=' * 30}")

            if hasattr(func, 'histo'):
                # Store the current histogram values before printing
                try:
                    # Calculate histogram sum directly from the histogram data
                    # This will give us the actual number of entries recorded in the histogram
                    if args.verbose > 0:
                        hist_count = hist_percpu_sum(func.histo)

                    # Print the per-CPU histogram (aggregates across all CPUs)
                    print_log2_hist_percpu(func.histo, '     size:')

                    # Add information about histogram counts
                    if args.verbose > 0:
                        print(f"Histogram entries: {hist_count}")

                        # If there's a significant discrepancy between histogram and counter
                        if abs(hist_count - func_calls) > func_calls * 0.01:  # More than 1% difference
                            print(f"Warning: Histogram count ({hist_count}) differs from call counter ({func_calls})")
                            print(f"         This may be due to lost events or BPF histogram limitations")

                    # Add alignment statistics if available
                    if alignment_bytes > 0:
                        print_alignment_stats(b, func.name)

                    # Add timing statistics if available
                    if args.track_timing:
                        print_timing_stats(b, func.name)

                except Exception as e:
                    LOG.error(f"Error handling histogram for {func.name}: {str(e)}")

        # Print call counts for count-only functions with more robust error handling
        for func in unique_count_funcs:
            try:
                if hasattr(func, 'call_counter'):
                    # Aggregate per-CPU call counter values
                    call_value = sum_percpu(func.call_counter[0])

                    if call_value == 0:
                        continue  # Skip functions with no calls

                    print(f"\n{'=' * 30} {func.name} ({call_value} calls) {'=' * 30}")

                    # Add alignment statistics for count-only functions if available
                    if alignment_bytes > 0 and (func.argSRC > 0 or func.argDST > 0):
                        print_alignment_stats(b, func.name)

                    # Add timing statistics for count-only functions if available
                    if args.track_timing:
                        print_timing_stats(b, func.name)
                else:
                    LOG.warning("No call_counter attribute for function %s", func.name)
            except Exception as e:
                LOG.error("Error processing counter for %s: %s", func.name, str(e))

        # Add distribution if requested
        if include_distribution:
            print_call_distribution()

            # Add time distribution if timing is enabled
            if args.track_timing:
                print_time_distribution()

        print("-" * 60)

    # Update alignment stats function to ensure consistency
    def print_alignment_stats(b, func_name):
        """Print alignment statistics for a function at the configured alignment boundary"""
        try:
            # Find the function object to get parameter information
            func = next((f for f in unique_dist_funcs + unique_count_funcs if f.name == func_name), None)
            if not func:
                LOG.error(f"Could not find function object for {func_name}")
                return

            # Get the total calls to this function
            if hasattr(func, 'dist_counter'):
                total_calls = sum_percpu(func.dist_counter[0])
            elif hasattr(func, 'call_counter'):
                total_calls = sum_percpu(func.call_counter[0])
            else:
                LOG.error(f"No counter attribute for function {func_name}")
                return

            if total_calls == 0:
                return  # Nothing to report

            print(f"\n  Memory Alignment Statistics ({alignment_label} boundary):")
            print(f"  {'Alignment Type':<25} {'Count':<10} {'Percentage':<10} {'Distribution'}")
            print("  " + "-" * 65)

            # Get per-CPU alignment counts only for the parameters that are tracked
            has_src = func.argSRC > 0
            has_dst = func.argDST > 0

            src_aligned = 0
            dst_aligned = 0
            both_aligned = 0

            if has_src:
                src_aligned = sum_percpu(b[f'aligned_src_{func_name}'][0])
                src_pct = (src_aligned / total_calls) * 100
                src_bar = '*' * int(src_pct / 5)  # Scale to reasonable length
                print(f"  Src {alignment_label}-aligned   : {src_aligned:<10} {src_pct:>6.2f}%    |{src_bar}")
            if has_dst:
                dst_aligned = sum_percpu(b[f'aligned_dst_{func_name}'][0])
                dst_pct = (dst_aligned / total_calls) * 100
                dst_bar = '*' * int(dst_pct / 5)
                print(f"  Dst {alignment_label}-aligned   : {dst_aligned:<10} {dst_pct:>6.2f}%    |{dst_bar}")
            if has_src and has_dst:
                both_aligned = sum_percpu(b[f'aligned_both_{func_name}'][0])
                both_pct = (both_aligned / total_calls) * 100
                both_bar = '*' * int(both_pct / 5)
                print(f"  Both {alignment_label}-aligned  : {both_aligned:<10} {both_pct:>6.2f}%    |{both_bar}")

                # Calculate not-aligned only when we have both src and dst
                not_aligned = total_calls - (src_aligned + dst_aligned - both_aligned)
                not_pct = (not_aligned / total_calls) * 100
                not_bar = '*' * int(not_pct / 5)
                print(f"  Neither aligned     : {not_aligned:<10} {not_pct:>6.2f}%    |{not_bar}")

        except Exception as e:
            LOG.error(f"Error printing alignment stats for {func_name}: {str(e)}")

    def print_timing_stats(b, func_name):
        """Print timing statistics for a function correlated with size ranges"""
        if not args.track_timing:
            return

        try:
            # Find the function object
            func = next((f for f in unique_dist_funcs + unique_count_funcs if f.name == func_name), None)
            if not func:
                LOG.error(f"Could not find function object for {func_name}")
                return

            # Get the total calls to this function
            if hasattr(func, 'dist_counter'):
                total_calls = sum_percpu(func.dist_counter[0])
            elif hasattr(func, 'call_counter'):
                total_calls = sum_percpu(func.call_counter[0])
            else:
                return

            if total_calls == 0:
                return  # Nothing to report

            print("\n  Timing Statistics (by size range):")

            # Check if this is a distribution function or count function
            is_dist_func = func in unique_dist_funcs

            if is_dist_func:
                # For distribution functions, show timing data correlated with size buckets
                timing_sum = b[f'timing_sum_{func_name}']
                timing_count = b[f'timing_count_{func_name}']
                size_histogram = func.histo

                # Collect data for each size bucket that has entries
                timing_data = []
                total_time_all_buckets = 0

                for bucket_idx in range(LOG2_HIST_BUCKETS):
                    count = sum_percpu(size_histogram[ct.c_int(bucket_idx)])
                    if count > 0:
                        time_sum_ns = sum_percpu(timing_sum[bucket_idx])
                        time_count_val = sum_percpu(timing_count[bucket_idx])

                        if time_count_val > 0:
                            avg_time_ns = time_sum_ns / time_count_val
                        else:
                            avg_time_ns = 0

                        total_time_all_buckets += time_sum_ns

                        # Calculate size range for this bucket
                        if bucket_idx == 0:
                            size_range = "0-1"
                        else:
                            size_min = 2 ** (bucket_idx - 1)
                            size_max = (2 ** bucket_idx) - 1
                            size_range = f"{size_min}-{size_max}"

                        timing_data.append((bucket_idx, size_range, count, time_sum_ns, avg_time_ns))

                if timing_data:
                    # Print header
                    print(f"  {'Size Range':<20} {'Calls':<10} {'Total Time (ns)':<20} {'Avg Time (ns)':<15} {'Avg Time (µs)'}")
                    print("  " + "-" * 90)

                    # Print each size bucket's timing data
                    for bucket_idx, size_range, count, total_time, avg_time in timing_data:
                        avg_time_us = avg_time / 1000.0
                        print(f"  {size_range:<20} {count:<10} {total_time:<20,} {avg_time:<15,.0f} {avg_time_us:>10.2f}")

                    # Print summary
                    print(f"  {'-'*20} {'-'*10} {'-'*20} {'-'*15} {'-'*11}")
                    overall_avg = total_time_all_buckets / total_calls if total_calls > 0 else 0
                    overall_avg_us = overall_avg / 1000.0
                    print(f"  {'Total/Average':<20} {total_calls:<10} {total_time_all_buckets:<20,} {overall_avg:<15,.0f} {overall_avg_us:>10.2f}")
                else:
                    print("  No timing data collected for any size range")

            else:
                # For count functions, show total time and average
                timing_total = sum_percpu(b[f'timing_total_{func_name}'][0])
                if timing_total > 0:
                    avg_time = timing_total / total_calls
                    avg_time_us = avg_time / 1000.0
                    print(f"  Total time: {timing_total:,} ns ({timing_total/1e9:.6f} s)")
                    print(f"  Average time per call: {avg_time:,.0f} ns ({avg_time_us:.2f} µs)")
                else:
                    print("  No timing data collected")

        except Exception as e:
            LOG.error(f"Error printing timing stats for {func_name}: {str(e)}")

    def parse_perf_report(perf_data_path):
        """Parse perf report and categorize samples by kernel vs library"""
        LOG.info("Parsing perf report...")

        try:
            # Run perf report to get symbol breakdown
            # Use --no-children to get self percentages (not cumulative from call stacks)
            perf_report_cmd = ['perf', 'report', '-i', perf_data_path, '--stdio', '-n', '--percent-limit', '0.01', '--no-children']
            result = subprocess.run(perf_report_cmd, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                LOG.error(f"Perf report failed: {result.stderr}")
                return None

            # Parse the output to categorize samples
            # Note: Perf percentages are already normalized (0-100%)
            kernel_samples = 0
            library_samples = 0

            all_function_samples = {}  # Track ALL functions with their percentages
            kernel_functions = {}  # Kernel functions separately
            library_functions = {}  # Library functions separately

            lines = result.stdout.split('\n')
            for line in lines:
                # Skip header and empty lines
                if not line.strip() or line.startswith('#'):
                    continue

                # Parse lines like: "  45.67%  1234  program  libc.so.6  [.] memcpy"
                parts = line.split()
                if len(parts) < 4:
                    continue

                # Try to extract percentage, library name, and function name
                try:
                    if '%' in parts[0]:
                        percent_str = parts[0].rstrip('%')
                        percent = float(percent_str)  # This is already 0-100%

                        # Find the library/module name and function name
                        func_name = None
                        library_name = None
                        is_kernel = False

                        for i, part in enumerate(parts):
                            if part == '[k]':  # Kernel symbol
                                is_kernel = True
                                if i + 1 < len(parts):
                                    func_name = parts[i + 1]
                                # Library name is before [k] marker
                                if i > 0:
                                    library_name = parts[i - 1]
                                break
                            elif part == '[.]':  # User-space symbol
                                is_kernel = False
                                if i + 1 < len(parts):
                                    func_name = parts[i + 1]
                                # Library name is before [.] marker
                                if i > 0:
                                    library_name = parts[i - 1]
                                break

                        if func_name:
                            # Create a unique key combining library and function
                            if library_name:
                                display_name = f"{func_name} ({library_name})"
                            else:
                                display_name = func_name

                            # Track all functions - percent is already 0-100%
                            category = 'kernel' if is_kernel else 'library'
                            all_function_samples[display_name] = {
                                'percent': percent,  # Already a percentage (0-100)
                                'category': category,
                                'function': func_name,
                                'library': library_name or 'unknown'
                            }

                            # Categorize by kernel vs library - just sum the percentages
                            if is_kernel:
                                kernel_samples += percent
                                kernel_functions[display_name] = percent
                            else:
                                library_samples += percent
                                library_functions[display_name] = percent

                except (ValueError, IndexError):
                    continue

            # kernel_samples and library_samples are already summed percentages
            # No need to normalize - they're already in the 0-100% range
            kernel_percent = kernel_samples
            library_percent = library_samples

            return {
                'kernel_percent': kernel_percent,
                'library_percent': library_percent,
                'all_functions': all_function_samples,
                'kernel_functions': kernel_functions,
                'library_functions': library_functions
            }

        except subprocess.TimeoutExpired:
            LOG.error("Perf report parsing timed out")
            return None
        except Exception as e:
            LOG.error(f"Error parsing perf report: {e}")
            return None

    def print_perf_summary():
        """Print perf profiling summary with kernel vs library categorization"""
        if not args.perf_profile or not perf_data_file:
            return

        # Stop perf recording
        if perf_proc and perf_proc.poll() is None:
            LOG.info("Stopping perf record...")
            perf_proc.terminate()
            try:
                perf_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                perf_proc.kill()
                perf_proc.wait()

        # Parse the perf data
        perf_stats = parse_perf_report(perf_data_path)

        if perf_stats:
            print("\n" + "=" * 70)
            print("Perf Profiling Summary (Sampling-based, Low Overhead)")
            print("=" * 70)

            kernel_pct = perf_stats['kernel_percent']
            library_pct = perf_stats['library_percent']
            other_pct = 100.0 - kernel_pct - library_pct

            print(f"\n{'Category':<20} {'Percentage':<12} {'Distribution'}")
            print("-" * 60)

            # Kernel calls
            kernel_bar = '*' * int(kernel_pct / 2)
            print(f"{'Kernel calls':<20} {kernel_pct:>10.2f}%  |{kernel_bar}")

            # Library calls
            library_bar = '*' * int(library_pct / 2)
            print(f"{'Library calls':<20} {library_pct:>10.2f}%  |{library_bar}")

            # Other (if any)
            if other_pct > 0.1:
                other_bar = '*' * int(other_pct / 2)
                print(f"{'Other':<20} {other_pct:>10.2f}%  |{other_bar}")

            # Library function distribution - showing only user-space library calls
            if perf_stats['library_functions']:
                print("\n" + "=" * 70)
                print("Library Function Call Distribution (User-Space Only)")
                print("=" * 70)

                # Get only library functions and sort by percentage (descending)
                sorted_lib_funcs = sorted(
                    [(name, data) for name, data in perf_stats['all_functions'].items()
                     if data['category'] == 'library'],
                    key=lambda x: x[1]['percent'],
                    reverse=True
                )

                # Display library functions table
                print(f"\n{'#':<4} {'Function':<30} {'Library':<25} {'Percent':<10} {'Bar'}")
                print("-" * 85)

                for idx, (func_name, func_data) in enumerate(sorted_lib_funcs[:20], 1):  # Top 20 library functions
                    percent = func_data['percent']  # This is already a percentage from perf
                    library = func_data['library']
                    func_only = func_data['function']

                    # percent is already the actual percentage value, use it directly
                    func_pct = percent
                    bar = '*' * int(func_pct / 2)

                    # Truncate long names for better display
                    func_display = func_only[:30] if len(func_only) <= 30 else func_only[:27] + '...'
                    lib_display = library[:25] if len(library) <= 25 else library[:22] + '...'

                    print(f"{idx:<4} {func_display:<30} {lib_display:<25} {func_pct:>8.2f}%  |{bar}")

                # Summary breakdown by category
                print("\nCategory Breakdown:")
                print(f"  Kernel functions: {len(perf_stats['kernel_functions'])} unique functions")
                print(f"  Library functions: {len(perf_stats['library_functions'])} unique functions")

                # Top kernel functions
                if perf_stats['kernel_functions']:
                    print("\n  Top Kernel Functions:")
                    sorted_kernel = sorted(perf_stats['kernel_functions'].items(),
                                         key=lambda x: x[1], reverse=True)
                    for func_name, percent in sorted_kernel[:5]:
                        # percent is already the actual percentage, use directly
                        print(f"    {func_name:<20} {percent:>6.2f}%")

                # Top library functions
                if perf_stats['library_functions']:
                    print("\n  Top Library Functions:")
                    sorted_library = sorted(perf_stats['library_functions'].items(),
                                          key=lambda x: x[1], reverse=True)
                    for func_name, percent in sorted_library[:5]:
                        # percent is already the actual percentage, use directly
                        print(f"    {func_name:<20} {percent:>6.2f}%")

            print("\nPerf Summary Notes:")
            print("📊 Perf uses statistical sampling (low overhead, approximate counts)")
            print("🔍 Provides holistic view of kernel vs library function consumption")
            print("⚡ [K] = Kernel function, [L] = Library function")
            print("💡 Complements eBPF exact counting with system-wide context")

        # Clean up perf data file
        try:
            os.unlink(perf_data_path)
            LOG.debug(f"Removed perf data file: {perf_data_path}")
        except Exception as e:
            LOG.warning(f"Could not remove perf data file {perf_data_path}: {e}")

    # Cleanup function for proper shutdown
    def cleanup_resources():
        # Print perf summary if enabled
        if args.perf_profile:
            print_perf_summary()

        # Restore stdout and close log file if using output redirection
        if args.output:
            if hasattr(sys.stdout, 'close'):
                sys.stdout.close()
            sys.stdout = original_stdout
            print(f"Log file closed: {args.output}")

        # Close handlers for the debug log
        if args.debug_log:
            for handler in LOG.handlers:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename.endswith(args.debug_log):
                    handler.close()
                    LOG.removeHandler(handler)
            print(f"Debug log file closed: {args.debug_log}")

    # Signal handler for graceful termination
    def signal_handler(signum, frame):
        sig_name = signal.Signals(signum).name
        LOG.info(f"Received signal {sig_name} ({signum}), performing cleanup...")

        # Dump statistics before exiting
        print(f"\n--- Final Statistics (triggered by {sig_name}) ---")
        # Updated function call - removed total_calls parameter
        print_function_stats(include_distribution=True)

        # Terminate the traced process if we launched it
        if args.exec and proc and proc.poll() is None:
            LOG.info("Terminating traced process")
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                LOG.warning("Process did not terminate gracefully, killing it")
                proc.kill()
                proc.wait()

        # Clean up resources
        cleanup_resources()

        print(f"\nExiting due to {sig_name} signal")
        # Use os._exit rather than sys.exit to ensure immediate termination
        os._exit(128 + signum)

    # Register signal handlers for various termination signals
    signal.signal(signal.SIGINT, signal_handler)    # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)   # kill command
    signal.signal(signal.SIGHUP, signal_handler)    # Terminal closed

    # Handle SIGTSTP (Ctrl+Z) specially - can't use the normal handler
    def sigtstp_handler(signum, frame):
        print("\nCtrl+Z detected. Dumping current stats before suspending...")
        # Updated function call - removed total_calls parameter
        print_function_stats(include_distribution=True)
        print("\nYou can resume with 'fg' or terminate with 'kill %<job_id>'")
        # Default action for SIGTSTP is to suspend the process
        os.kill(os.getpid(), signal.SIGSTOP)

    signal.signal(signal.SIGTSTP, sigtstp_handler)  # Ctrl+Z

    LOG.info("Beginning tracing")
    start_time = time.time()
    last_print_time = start_time

    try:
        # For traced executables, monitor process exit continuously
        if args.exec:
            iterations = 0
            poll_interval = 0.1  # Check every 100ms for responsiveness

            while True:
                time.sleep(poll_interval)
                iterations += 1

                # CRITICAL: Check if child process exited (highest priority)
                if proc.poll() is not None:
                    LOG.info("Target process has exited with code: %d", proc.returncode)
                    # Give a brief moment for final eBPF events to be collected
                    time.sleep(0.3)
                    # Exit immediately - don't wait for interval
                    break

                # Print statistics at the specified interval only if still running
                if iterations * poll_interval >= args.interval:
                    iterations = 0  # Reset counter

                    # Check if we're getting any calls at all - only in verbose mode
                    if args.verbose > 0 and total_calls is not None:
                        call_count = sum_percpu(total_calls[0])
                        LOG.info("Total function calls detected: %d", call_count)
                    else:
                        # For non-verbose mode, sum per-CPU function-specific counters
                        call_count = sum([sum_percpu(func.dist_counter[0]) for func in unique_dist_funcs if hasattr(func, 'dist_counter')] +
                                        [sum_percpu(func.call_counter[0]) for func in unique_count_funcs if hasattr(func, 'call_counter')])

                    print('%-8s\n' % time.strftime('%H:%M:%S'), end='')

                    # Add elapsed time to each interval output
                    if args.verbose > 0:
                        elapsed = time.time() - start_time
                        hours, remainder = divmod(elapsed, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        print(f'Time: {time.strftime("%H:%M:%S")} (Elapsed: {int(hours)}h {int(minutes)}m {int(seconds)}s)\n')

                    # If we have a specific executable and get no calls,
                    # print a helpful message
                    if call_count == 0:
                        print("\nNo function calls detected. Possible causes:")
                        print("- The target program isn't using the traced functions")
                        print("- The target program finished too quickly")
                        print("- There might be issues with attaching to the specific functions")
                        print("\nTry running a program with more memory operations or increasing verbosity with -v")

                # Check if the specified run time has elapsed
                if args.time and (time.time() - start_time) >= args.time:
                    LOG.info("Specified run time has elapsed. Stopping trace.")
                    break
        else:
            # For PID tracing, use the original interval-based approach
            while True:
                time.sleep(args.interval)

                # Check if the target process is still alive (for specific PID tracing)
                if args.pid > 0:
                    try:
                        os.kill(args.pid, 0)  # Signal 0 = check existence only
                    except ProcessLookupError:
                        LOG.info("Target process %d has exited. Stopping trace.", args.pid)
                        break
                    except PermissionError:
                        pass  # Process exists but we lost permission (still alive)

                # Check if we're getting any calls at all - only in verbose mode
                if args.verbose > 0 and total_calls is not None:
                    call_count = sum_percpu(total_calls[0])
                    LOG.info("Total function calls detected: %d", call_count)
                else:
                    call_count = sum([sum_percpu(func.dist_counter[0]) for func in unique_dist_funcs if hasattr(func, 'dist_counter')] +
                                    [sum_percpu(func.call_counter[0]) for func in unique_count_funcs if hasattr(func, 'call_counter')])

                print('%-8s\n' % time.strftime('%H:%M:%S'), end='')

                if args.verbose > 0:
                    elapsed = time.time() - start_time
                    hours, remainder = divmod(elapsed, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    print(f'Time: {time.strftime("%H:%M:%S")} (Elapsed: {int(hours)}h {int(minutes)}m {int(seconds)}s)\n')

                # Check if the specified run time has elapsed
                if args.time and (time.time() - start_time) >= args.time:
                    LOG.info("Specified run time has elapsed. Stopping trace.")
                    break

    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt received, exiting gracefully")
    finally:
        # Add a summary footer with total runtime
        end_time = time.time()
        total_elapsed = end_time - start_time
        hours, remainder = divmod(total_elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)

        footer = f"""
=======================================================================
AOCL-LibMem Profiler - Summary
=======================================================================
Started at: {current_time.strftime('%Y-%m-%d %H:%M:%S')}
Ended at:   {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s
=======================================================================
"""
        print(footer)

        # Print final summary
        print("\nFinal summary:")
        # Updated function call - removed total_calls parameter
        print_function_stats(include_distribution=True)

        # Use the cleanup_resources function for final cleanup
        cleanup_resources()

main()
