#!/usr/bin/env python3
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

from bcc import BPF
from bcc.libbcc import lib, bcc_symbol, bcc_symbol_option

import ctypes as ct
import sys, os
import time
import argparse
import getpass
import logging
import subprocess
import signal
import tempfile
import os
import stat
from io import StringIO  # For capturing print output
import datetime  # For timestamping log files
import platform  # For system information
import socket    # For hostname information
LOG = logging.getLogger(__name__)

# Work around a bug in older versions of bcc.libbcc
if len(bcc_symbol_option._fields_) == 3:
    LOG.warning("Adding workaround for old libBCC")
    class patched_bcc_symbol_option(ct.Structure):
        _fields_ = [
            ('use_debug_file', ct.c_int),
            ('check_debug_file_crc', ct.c_int),
            ('lazy_symbolize', ct.c_int),
            ('use_symbol_type', ct.c_uint),
        ]
    my_bcc_symbol_option = patched_bcc_symbol_option
else:
    my_bcc_symbol_option = bcc_symbol_option

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
            LOG.error("Failed to resolve symbol %s in module %s. ERRNO: %d", symname, module, ct.get_errno())
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

            while True:
                try:
                    if resolv_func_addr and impl_func_addr:
                        b.detach_uprobe(name=self.libname, addr=self.indirect_symbol.offset, pid=os.getpid())
                        b.detach_uretprobe(name=self.libname, addr=self.indirect_symbol.offset, pid=os.getpid())
                        b.cleanup()
                        break
                    b.perf_buffer_poll()
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


def _build_bpf_text(functions_dist, functions_cnt, target_pid=-1, check_alignment=False, verbose=False):
    text = """
#include <uapi/linux/ptrace.h>

// Helper function to increment a counter atomically
static inline void increment_counter(u64 *counter) {
    if (counter) {
        __sync_fetch_and_add(counter, 1);
    }
}
"""

    # Add debug counter for verbose mode
    if verbose:
        text += """
// Debug counter - only enabled in verbose mode
BPF_ARRAY(total_calls, u64, 1);
"""

    # Add a counter to track global function call distribution
    text += """
// Global function call counters for distribution analysis
"""

    # Combine all functions for more unified processing
    all_functions = functions_dist + functions_cnt

    # Add counters for all functions
    for func in functions_dist:
        text += f"BPF_ARRAY(dist_{func.name}, u64, 1);\n"
    for func in functions_cnt:
        text += f"BPF_ARRAY(callCount_{func.name}, u64, 1);\n"

    # Add alignment counters if requested
    if check_alignment:
        text += "\n// Memory alignment counters (64-byte alignment)\n"
        for func in all_functions:
            if func.argSRC > 0:
                text += f"BPF_ARRAY(aligned_src_{func.name}, u64, 1);\n"
            if func.argDST > 0:
                text += f"BPF_ARRAY(aligned_dst_{func.name}, u64, 1);\n"
            if func.argSRC > 0 and func.argDST > 0:
                text += f"BPF_ARRAY(aligned_both_{func.name}, u64, 1);\n"

    text += "\n// Function implementations\n"

    # Generate code for ALL functions using a unified approach
    for func in all_functions:
        # Determine if this is a distribution function
        is_dist_func = func in functions_dist

        # Add histogram only for distribution functions
        if is_dist_func:
            text += f"BPF_HISTOGRAM(lenHist_{func.name});\n"

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

        # For distribution functions, add histogram code
        if is_dist_func:
            text += """
    // Get size argument
    size_t len = PT_REGS_PARM{0}(ctx);

    // Convert to log2 bucket and use atomic increment
    u32 bucket_idx = bpf_log2l(len);
    u64 *hist_count = (u64*)lenHist_{1}.lookup(&bucket_idx);
    increment_counter(hist_count);

    // Update distribution counter for this function
    u64 *func_count = dist_{1}.lookup(&zero);
    increment_counter(func_count);
""".format(func.argSZ, func.name)
        else:
            # For count functions
            text += """
    // Update function-specific counter
    u64 *count = callCount_{0}.lookup(&zero);
    increment_counter(count);

""".format(func.name)

        # Add debug counter for verbose mode
        if verbose:
            text += """
    // Update total call counter for debugging (only in verbose mode)
    u64 *total = total_calls.lookup(&zero);
    increment_counter(total);
"""
        else:
            text += "    // Debug counter disabled in non-verbose mode\n"

        # Add alignment checks if requested - this is common for both function types
        if check_alignment:
            # Add source alignment check
            if func.argSRC > 0:
                text += """
    // Source pointer alignment check
    u64 src_ptr = PT_REGS_PARM%d(ctx);
    bool src_aligned = (src_ptr & 0x3F) == 0;

    if (src_aligned) {
        u64 *src_count = aligned_src_%s.lookup(&zero);
        increment_counter(src_count);
    }
""" % (func.argSRC, func.name)

            # Add destination alignment check
            if func.argDST > 0:
                text += """
    // Destination pointer alignment check
    u64 dst_ptr = PT_REGS_PARM%d(ctx);
    bool dst_aligned = (dst_ptr & 0x3F) == 0;

    if (dst_aligned) {
        u64 *dst_count = aligned_dst_%s.lookup(&zero);
        increment_counter(dst_count);
    }
""" % (func.argDST, func.name)

            # Add both-aligned check if both source and destination are specified
            if func.argSRC > 0 and func.argDST > 0:
                text += """
    // Both source and destination aligned check
    if (src_aligned && dst_aligned) {
        u64 *both_count = aligned_both_%s.lookup(&zero);
        increment_counter(both_count);
    }
""" % func.name

        # Close the function
        text += """
    return 0;
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
        FuncInfo('c', 'memcpy',  'memcpy@@GLIBC_2.14'),
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
    p.add_argument('-a', '--check-alignment', action='store_true',
        help = 'Check memory alignment of arguments (64-byte boundary)')

    args = p.parse_args()

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

    if args.check_alignment:
        header += "- Checking memory alignment of arguments (64-byte boundary)\n"

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


    # Generate BPF code with proper function lists, including verbose flag
    bpf_text = _build_bpf_text(unique_dist_funcs, unique_count_funcs, target_pid,
                              args.check_alignment, args.verbose > 0)

    # Debug the generated BPF code if verbose
    if args.verbose >= 2:
        LOG.debug("Generated BPF program:\n%s", bpf_text)

    b = BPF(text=bpf_text)

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
                    # Initialize counter to 0 immediately to ensure it's valid
                    if funcInfo.call_counter[0].value != 0:
                        funcInfo.call_counter[0] = ct.c_ulonglong(0)
                    LOG.debug("Successfully initialized counter for %s", funcInfo.name)
                    funcInfo.type = "call"
                    successful_attaches += 1
                except Exception as e:
                    LOG.error("Failed to initialize counter for %s: %s", funcInfo.name, str(e))
            else:
                LOG.error("Could not attach to %s, skipping", funcInfo.name)
        except Exception as e:
            LOG.error("Error during function attachment for %s: %s", funcInfo.name, str(e))

    if successful_attaches == 0:
        LOG.error("Failed to attach any probes. Exiting.")
        if args.exec:
            signal_handler(signal.SIGTERM, None)
        return

    LOG.info("Successfully attached %d/%d probes", successful_attaches, len(unique_dist_funcs) + len(unique_count_funcs))

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
        print("\n--- Function Call Distribution ---")

        # Collect all function call counts
        function_counts = []
        total_func_calls = 0

        # Get distribution counts for distribution functions
        for func in unique_dist_funcs:
            if hasattr(func, 'dist_counter'):
                # Read the counter value directly from BPF map for consistency
                count = func.dist_counter[0].value
                if count > 0:  # Only include functions with non-zero counts
                    function_counts.append((func.name, count))
                    total_func_calls += count

        # Get distribution counts for count-only functions
        for func in unique_count_funcs:
            if hasattr(func, 'call_counter'):
                # Read the counter value directly from BPF map for consistency
                count = func.call_counter[0].value
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
            global_total = total_calls[0].value
            print(f"Global counter value: {global_total}")

            # If there's a discrepancy, log a warning
            if global_total != total_func_calls and global_total > 0:
                print(f"Note: There's a {abs(global_total - total_func_calls) / max(global_total, 1) * 100:.2f}% difference between the sum and global counter.")

    # Update the print_function_stats function to handle histograms correctly

    def print_function_stats(include_distribution=False):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n--- Function Statistics at {timestamp} ---")

        # Print histograms for distribution functions
        for func in unique_dist_funcs:
            # Always use dist_counter for consistency
            func_calls = func.dist_counter[0].value if hasattr(func, 'dist_counter') else 0
            if func_calls == 0:
                continue  # Skip functions with no calls

            print(f"\n{'=' * 30} {func.name} ({func_calls} calls) {'=' * 30}")

            if hasattr(func, 'histo'):
                # Store the current histogram values before printing
                try:
                    # Calculate histogram sum directly from the histogram data
                    # This will give us the actual number of entries recorded in the histogram
                    if args.verbose > 0:
                        hist_count = sum(v.value for v in func.histo.values())

                    # Capture the histogram output to ensure it's also logged to file
                    output = StringIO()
                    old_stdout = sys.stdout
                    sys.stdout = output

                    # Print the actual histogram
                    func.histo.print_log2_hist('     size:')

                    # Add information about histogram counts
                    if args.verbose > 0:
                        print(f"Histogram entries: {hist_count}")

                        # If there's a significant discrepancy between histogram and counter
                        if abs(hist_count - func_calls) > func_calls * 0.01:  # More than 1% difference
                            print(f"Warning: Histogram count ({hist_count}) differs from call counter ({func_calls})")
                            print(f"         This may be due to lost events or BPF histogram limitations")

                    sys.stdout = old_stdout
                    print(output.getvalue().rstrip())

                    # Add alignment statistics if available
                    if args.check_alignment:
                        print_alignment_stats(b, func.name)

                except Exception as e:
                    LOG.error(f"Error handling histogram for {func.name}: {str(e)}")

        # Print call counts for count-only functions with more robust error handling
        for func in unique_count_funcs:
            try:
                if hasattr(func, 'call_counter'):
                    # Always use call_counter for consistency (not func.counter)
                    call_value = func.call_counter[0].value

                    if call_value == 0:
                        continue  # Skip functions with no calls

                    print(f"\n{'=' * 30} {func.name} ({call_value} calls) {'=' * 30}")

                    # Reset individual counter if needed but not distribution counter
                    if hasattr(func, 'call'):
                        func.call_counter[0] = ct.c_ulonglong(0)

                    # Add alignment statistics for count-only functions if available
                    if args.check_alignment and (func.argSRC > 0 or func.argDST > 0):
                        print_alignment_stats(b, func.name)
                else:
                    LOG.warning("No call_counter attribute for function %s", func.name)
            except Exception as e:
                LOG.error("Error processing counter for %s: %s", func.name, str(e))

        # Add distribution if requested
        if include_distribution:
            print_call_distribution()

        print("-" * 60)

    # Update alignment stats function to ensure consistency
    def print_alignment_stats(b, func_name):
        """Print alignment statistics for a function"""
        try:
            # Find the function object to get parameter information
            func = next((f for f in unique_dist_funcs + unique_count_funcs if f.name == func_name), None)
            if not func:
                LOG.error(f"Could not find function object for {func_name}")
                return

            # Get the total calls to this function
            if hasattr(func, 'dist_counter'):
                total_calls = func.dist_counter[0].value
            elif hasattr(func, 'call_counter'):
                total_calls = func.call_counter[0].value
            else:
                LOG.error(f"No counter attribute for function {func_name}")
                return

            if total_calls == 0:
                return  # Nothing to report

            print(f"{'Alignment Type':<20} {'Count':<10} {'Percentage':<10} {'Distribution'}")
            print("-" * 60)

            # Get alignment counts only for the parameters that are tracked
            has_src = func.argSRC > 0
            has_dst = func.argDST > 0

            src_aligned = 0
            dst_aligned = 0
            both_aligned = 0

            if has_src:
                src_aligned = b[f'aligned_src_{func_name}'][0].value
                src_pct = (src_aligned / total_calls) * 100
                src_bar = '*' * int(src_pct / 5)  # Scale to reasonable length
                print(f"Source aligned    : {src_aligned:<10} {src_pct:>6.2f}%    |{src_bar}")
            if has_dst:
                dst_aligned = b[f'aligned_dst_{func_name}'][0].value
                dst_pct = (dst_aligned / total_calls) * 100
                dst_bar = '*' * int(dst_pct / 5)
                print(f"Dest aligned      : {dst_aligned:<10} {dst_pct:>6.2f}%    |{dst_bar}")
            if has_src and has_dst:
                both_aligned = b[f'aligned_both_{func_name}'][0].value
                both_pct = (both_aligned / total_calls) * 100
                both_bar = '*' * int(both_pct / 5)
                print(f"Both aligned      : {both_aligned:<10} {both_pct:>6.2f}%    |{both_bar}")

                # Calculate not-aligned only when we have both src and dst
                not_aligned = total_calls - (src_aligned + dst_aligned - both_aligned)
                not_pct = (not_aligned / total_calls) * 100
                not_bar = '*' * int(not_pct / 5)
                print(f"Neither aligned   : {not_aligned:<10} {not_pct:>6.2f}%    |{not_bar}")

        except Exception as e:
            LOG.error(f"Error printing alignment stats for {func_name}: {str(e)}")

    # Cleanup function for proper shutdown
    def cleanup_resources():
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
        while True:
            time.sleep(args.interval)

            # Check if we're getting any calls at all - only in verbose mode
            if args.verbose > 0 and total_calls is not None:
                call_count = total_calls[0].value
                LOG.info("Total function calls detected: %d", call_count)
            else:
                # For non-verbose mode, we can estimate by summing function-specific counters
                call_count = sum([func.dist_counter[0].value for func in unique_dist_funcs if hasattr(func, 'dist_counter')] +
                                [func.call_counter[0].value for func in unique_count_funcs if hasattr(func, 'call_counter')])

            print('%-8s\n' % time.strftime('%H:%M:%S'), end='')

            # Add elapsed time to each interval output
            if args.verbose > 0:
                elapsed = time.time() - start_time
                hours, remainder = divmod(elapsed, 3600)
                minutes, seconds = divmod(remainder, 60)
                print(f'Time: {time.strftime("%H:%M:%S")} (Elapsed: {int(hours)}h {int(minutes)}m {int(seconds)}s)\n')

            # If we have a specific executable and get no calls after some intervals,
            # print a helpful message
            if args.exec and call_count == 0:
                print("\nNo function calls detected. Possible causes:")
                print("- The target program isn't using the traced functions")
                print("- The target program finished too quickly")
                print("- There might be issues with attaching to the specific functions")
                print("\nTry running a program with more memory operations or increasing verbosity with -v")

            # Check if child is still running if we launched it
            if args.exec and proc.poll() is not None:
                LOG.info("Target process has exited with code: %d", proc.returncode)
                break

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
