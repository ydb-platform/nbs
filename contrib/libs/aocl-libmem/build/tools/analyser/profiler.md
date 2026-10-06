# AOCL-LibMem Profiler Tool

## Description
The AOCL-LibMem Profiler is a powerful tool that provides detailed insights into memory function usage patterns by tracing calls to standard C library memory functions. The tool collects data on function call counts, size distributions, and memory alignment, enabling performance analysis and optimization of memory operations in your applications.

## Features
- Traces memory-related libc functions (memcpy, mempcpy, memcmp, memmove, memset, etc.)
- Supports both distribution tracking (with size histograms) and call counting
- Analyzes memory alignment of source and destination pointers (64-byte boundaries)
- Can attach to running processes or launch new applications
- Multi-thread safe - properly tracks and aggregates data across all threads in target processes
- Provides periodic reporting at configurable intervals
- Filters results by process ID for focused analysis
- Supports tracing specific functions only

## Requirements
- Python 3.6+
- BCC Tools (python3-bpfcc)
- Root privileges (for BPF operations)
- Recommended linux kernel version 5.15 for eBPF tools
### Installation
```bash
# Install BCC tools
sudo apt-get install -y python3-bpfcc
```

## Basic Usage

### Trace a Running Process
```bash
# Trace an existing process with PID 1234
sudo ./prof_libmem.py -p 1234

# Trace with more verbose output
sudo ./prof_libmem.py -v -p 1234

# Trace with alignment checking
sudo ./prof_libmem.py -p 1234 -a
```

### Launch and Trace an Application
```bash
# Launch and trace an application
sudo ./prof_libmem.py -e ./my_application arg1 arg2

# Launch and trace with alignment checking
sudo ./prof_libmem.py -a -e ./my_application arg1 arg2
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `-i SECONDS`, `--interval SECONDS` | Reporting interval in seconds (default: 5) |
| `-p PID`, `--pid PID` | Trace only a specific process ID |
| `-f FUNCTION`, `--functions FUNCTION` | Trace only specific functions (can be used multiple times) |
| `-v`, `--verbose` | Increase verbosity (can be used multiple times) |
| `-e COMMAND`, `--exec COMMAND` | Execute and trace the specified command |
| `-t SECONDS`, `--time SECONDS` | Total time in seconds to run the trace |
| `-c`, `--track-count-functions` | Also track functions without size parameters (count only) |
| `-o FILE`, `--output FILE` | Output file for logging results (default: stdout) |
| `-a`, `--check-alignment` | Check memory alignment of function arguments (64-byte boundary) |

## Initialization Process

When using the `-e` option to launch and trace an application, the profiler:

1. Launches application in suspended state
2. Attaches BPF probes to the specified functions
3. Waits for all probes to be properly attached and initialized
4. Resumes application once everything is ready

The default initialization delay is 10 seconds, which is sufficient for most applications. This ensures that all function calls are captured from the very beginning of your application's execution.

## Use Case Examples

### Example 1: Trace Specific Functions in a Running Application
To trace only `memcpy` and `memset` in a process with PID 1234:

```bash
sudo ./prof_libmem.py -p 1234 -f memcpy -f memset
```

### Example 2: Launch an Application with Tracing for a Limited Time
To execute an application and trace it for 30 seconds with reports every 10 seconds:

```bash
sudo ./prof_libmem.py -i 10 -t 30 -e ./myapp arg1 arg2
```

### Example 3: Track Count-Only Functions
To also track functions like strcpy, strlen in addition to the default memory functions:

```bash
sudo ./prof_libmem.py -p 1234 -c
```

### Example 4: Save Results to a File
To record all profiling data to a log file:

```bash
sudo ./prof_libmem.py -o profile_output.log -e ./myapp
```

### Example 5: High Verbosity for Troubleshooting
For maximum diagnostic information:

```bash
sudo ./prof_libmem.py -v -v -e ./myapp
```

### Example 6: Check Memory Alignment
To analyze alignment of source and destination pointers (important for performance):

```bash
sudo ./prof_libmem.py -p 1234 -a
```

## Understanding the Output

The tool provides three types of output:

1. **Histograms** for functions with size parameters (like memcpy, memset):
```
memcpy length:
     (unit)           : count    distribution
         0 -> 1       : 0        |                                        |
         2 -> 3       : 0        |                                        |
         4 -> 7       : 120      |******                                  |
         8 -> 15      : 60       |***                                     |
        16 -> 31      : 402      |********************                    |
        32 -> 63      : 950      |***********************************************|
```

2. **Call counts** for functions without size parameters (when using -c):
```
strlen: 2445 calls
strcpy: 352 calls
```

3. **Alignment statistics** (when using -a) for analyzing memory access patterns:
```
--- Memory Alignment Statistics (64-byte alignment) ---
Alignment Type       Count      Percentage  Distribution
------------------------------------------------------------
Source aligned    : 1520       63.12%      |*************|
Dest aligned      : 1820       75.60%      |***************|
Both aligned      : 1320       54.85%      |***********|
Neither aligned   : 380        15.79%      |***|

Total calls: 2406
```

Additionally, the profiler generates a summary showing the distribution of all traced function calls:

```
--- Function Call Distribution ---
Function       Calls         Percent  Distribution
---------------------------------------------
memcpy         32450         45.21%   |***********************|
strlen         18940         26.40%   |*************|
strcmp         10230         14.26%   |*******|
strncpy        5620          7.83%    |****|
memset         3500          4.88%    |**|
memcmp         1020          1.42%    |*|

Total function calls: 71760
```

## Tips

1. **Use verbose mode** (`-v`) when troubleshooting or when you need more details about the tracing process.
2. **Start with short intervals** to quickly verify that tracing is working properly.
3. **For performance optimization**, focus on the functions with the highest call counts or those handling large memory blocks.
4. **Check alignment statistics** to identify potential performance bottlenecks. Operations where both source and destination are aligned on 64-byte boundaries typically perform better, particularly for larger memory operations.
5. **Multi-thread support is built-in**. The profiler uses atomic operations to ensure accurate counting even when multiple threads call the same functions simultaneously.
6. **System-wide tracing** can be resource-intensive; prefer specifying a target process when possible.
7. **For timing-sensitive applications**, consider adding your own delay at the beginning of your program to ensure all probes are attached before critical code executes.
8. **Check the log for messages** like "Successfully attached X/Y probes" to confirm the profiler is working correctly.
9. **Always place the `-e` option at the end of your command line** to avoid confusion between the profiler's arguments and the executable's arguments.

## Notes

- The tool requires root privileges to use BPF features.
- Some functions may not be traceable in all environments or applications.
- For executables with very short lifetimes, you may need to increase verbosity to debug issues.
- Memory alignment checking works for both distribution-tracked functions and count-only functions that handle pointers.
- All memmove() calls will be tracked under memcpy() as Glibc has common implementation for both these functions.
