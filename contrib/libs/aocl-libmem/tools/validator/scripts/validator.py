#!/usr/bin/python3
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
import argparse
import datetime
import sys

# Import configuration from CMake-generated file
try:
    from libmem_defs import LIBMEM_BIN_PATH
except ImportError:
    # Fallback if libmem_defs.py is not found
    LIBMEM_BIN_PATH = ""
    print("Warning: libmem_defs.py not found. Set LD_PRELOAD manually or run from build/test directory.")

# Validator executable path (relative to build/test directory)
VALIDATOR_PATH = '../tools/validator/libmem_validator'

# Supported memory functions
LIBMEM_FUNCS = [
    'memcpy', 'mempcpy', 'memmove', 'memset', 'memcmp', 'memchr',
    'strcpy', 'strncpy', 'strcmp', 'strncmp', 'strlen', 'strnlen',
    'strcat', 'strncat', 'strstr', 'strspn', 'strchr', 'strrchr'
]


class ValidatorConfig:
    """Configuration for validator execution."""

    def __init__(self, args):
        self.func = args.func
        self.start_size = args.range[0]
        self.end_size = args.range[1]
        self.src_align = args.align[0]
        self.dst_align = args.align[1]
        self.iterator = args.iterator
        self.all_alignments = getattr(args, 'all_alignments', False)
        self.verbose = getattr(args, 'verbose', False)


def build_command(config, size):
    """
    Build the command line for the validator.

    Args:
        config: ValidatorConfig object
        size: Current size to test

    Returns:
        List of command arguments
    """
    cmd = [
        VALIDATOR_PATH,
        config.func,
        str(size),
        str(config.src_align),
        str(config.dst_align)
    ]

    if config.all_alignments:
        cmd.append('1')  # all_alignments flag

    return cmd


def run_validation(config, output_file):
    """
    Run validation for all sizes in the configured range.

    Args:
        config: ValidatorConfig object
        output_file: File object to write results

    Returns:
        Tuple of (success_count, failure_count, total_count)
    """
    size = config.start_size
    success_count = 0
    failure_count = 0
    total_count = 0

    # Handle size=0 with shift iterator (would cause infinite loop)
    if size == 0 and config.iterator == '<<1':
        total_count += 1
        result = run_single_validation(config, size, output_file)
        if result:
            success_count += 1
        else:
            failure_count += 1
        size = 1

    # Iterate through sizes
    while size <= config.end_size:
        total_count += 1
        result = run_single_validation(config, size, output_file)
        if result:
            success_count += 1
        else:
            failure_count += 1

        # Apply iterator
        try:
            size = eval(f'size {config.iterator}')
        except Exception as e:
            print(f"Error evaluating iterator '{config.iterator}': {e}")
            break

    return success_count, failure_count, total_count


def run_single_validation(config, size, output_file):
    """
    Run validation for a single size.

    Args:
        config: ValidatorConfig object
        size: Size to test
        output_file: File object to write results

    Returns:
        True if validation passed, False otherwise
    """
    cmd = build_command(config, size)

    if config.verbose:
        print(f'   > Running: {" ".join(cmd)}')
    else:
        print(f'   > Validation for size [{size}] in progress...')

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            timeout=600  # 10 minute timeout per size
        )

        output = result.stdout.decode('utf-8', errors='replace')
        output_file.write(output)
        output_file.write('\n')

        # Check for failure indicators
        if 'ERROR' in output or 'FAILED' in output or result.returncode != 0:
            if config.verbose:
                print(f'   ! FAILED at size {size}')
            return False

        return True

    except subprocess.TimeoutExpired:
        print(f'   ! TIMEOUT at size {size}')
        output_file.write(f'TIMEOUT at size {size}\n')
        return False
    except FileNotFoundError:
        print(f'   ! Validator not found: {cmd[0]}')
        print(f'     Make sure to build the validator first.')
        output_file.write(f'ERROR: Validator not found: {cmd[0]}\n')
        return False
    except Exception as e:
        print(f'   ! Error at size {size}: {e}')
        output_file.write(f'ERROR at size {size}: {e}\n')
        return False


def whole_number(value):
    """Validate that value is a non-negative integer."""
    try:
        value = int(value)
        if value < 0:
            raise argparse.ArgumentTypeError(f"{value} is not a whole number")
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value} is not a whole number")
    return value

def main():
    parser = argparse.ArgumentParser(
        prog='validator.py',
        description='AOCL-LibMem Data Validator - Batch validation for memory functions.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Positional argument
    parser.add_argument(
        "func",
        type=str,
        choices=LIBMEM_FUNCS,
        help="Memory function to validate"
    )

    # Size range
    parser.add_argument(
        "-r", "--range",
        nargs=2,
        type=whole_number,
        default=[8, 64 * 1024 * 1024],
        metavar=('START', 'END'),
        help="Range of data sizes to test (default: 8 to 64MB)"
    )

    # Alignment
    parser.add_argument(
        "-a", "--align",
        nargs=2,
        type=whole_number,
        default=[0, 0],
        metavar=('SRC', 'DST'),
        help="Source and destination alignment offsets (default: 0 0)"
    )

    # Iterator pattern
    parser.add_argument(
        "-t", "--iterator",
        type=str,
        default='<<1',
        help="Iteration pattern for sizes (default: '<<1' for doubling)"
    )

    parser.add_argument(
        "--all-alignments",
        action='store_true',
        help="Test all alignment combinations (0-63 for both src and dst)"
    )

    parser.add_argument(
        "-v", "--verbose",
        action='store_true',
        help="Enable verbose output"
    )

    args = parser.parse_args()

    # Create configuration
    config = ValidatorConfig(args)

    # Print header
    print("=" * 60)
    print("AOCL-LibMem Data Validator")
    print("=" * 60)
    print(f"Function:    {config.func}")
    print(f"Size range:  {config.start_size} - {config.end_size}")
    print(f"Alignments:  src={config.src_align}, dst={config.dst_align}" +
          (" (testing all)" if config.all_alignments else ""))
    print(f"Iterator:    {config.iterator}")
    print("=" * 60)

    # Set up environment
    if LIBMEM_BIN_PATH:
        env['LD_PRELOAD'] = LIBMEM_BIN_PATH
        print(f"\nUsing library: {LIBMEM_BIN_PATH}")
    else:
        print("\nWarning: No library path set. Using system default.")

    # Create output directory
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    result_dir = os.path.join('out', config.func, timestamp)
    os.makedirs(result_dir, exist_ok=True)

    # Report file
    report_file = os.path.join(result_dir, 'validation_report.txt')

    # Run validation
    print(f"\n>>> Starting validation...\n")

    with open(report_file, 'w') as output:
        output.write(f"# AOCL-LibMem Validation Report\n")
        output.write(f"# Function: {config.func}\n")
        output.write(f"# Size range: {config.start_size} - {config.end_size}\n")
        output.write(f"# Alignments: src={config.src_align}, dst={config.dst_align}\n")
        output.write(f"# Iterator: {config.iterator}\n")
        output.write(f"# Timestamp: {timestamp}\n")
        output.write("=" * 60 + "\n\n")

        success_count, failure_count, total_count = run_validation(config, output)

    # Generate summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total tests:  {total_count}")
    print(f"Passed:       {success_count}")
    print(f"Failed:       {failure_count}")

    status = 'SUCCESS' if failure_count == 0 else 'FAILURE'
    print(f"\nStatus: {status}")
    print(f"\nResults saved to: {result_dir}")

    # Write config file
    config_file = os.path.join(result_dir, 'test.config')
    with open(config_file, 'w') as f:
        f.write(f"libmem_function: {config.func}\n")
        f.write(f"data_range: [{config.start_size}, {config.end_size}]\n")
        f.write(f"alignment: (src={config.src_align}, dst={config.dst_align})\n")
        f.write(f"all_alignments: {config.all_alignments}\n")
        f.write(f"iterator: {config.iterator}\n")
        f.write(f"timestamp: {timestamp}\n")

    # Write summary file
    summary_file = os.path.join(result_dir, 'test_summary.log')
    with open(summary_file, 'w') as f:
        f.write(f"Test Status: {status}\n")
        f.write(f"Total: {total_count}, Passed: {success_count}, Failed: {failure_count}\n")
        f.write(f"\nValidation {'completed successfully' if failure_count == 0 else 'found failures'}.\n")
        f.write(f"See validation_report.txt for details.\n")

    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
