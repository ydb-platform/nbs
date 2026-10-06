#!/usr/bin/env python3
# Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its contributors
#    may be used to endorse or promote products derived from this software without
#    specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""
Profiler Output Verification Script

Parses prof_libmem output and compares against expected values
from the test application's stderr output.
"""

import sys
import re
from typing import Dict, Tuple

def parse_expected_values(stderr_file: str) -> Dict[str, str]:
    """Parse EXPECTED_* lines from test application stderr."""
    expected = {}
    try:
        with open(stderr_file, 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line:
                    key, value = line.split('=', 1)
                    expected[key] = value
    except FileNotFoundError:
        print(f"Warning: {stderr_file} not found")
    return expected

def parse_profiler_output(output_file: str) -> Dict[str, any]:
    """Parse prof_libmem output to extract function stats."""
    stats = {
        'functions': {},
        'total_calls': 0,
        'buckets': {},
        'alignment': {},       # global (last-writer-wins, kept for compat)
        'func_alignment': {},  # per-function alignment data
    }

    try:
        with open(output_file, 'r') as f:
            content = f.read()

            # Extract function call counts
            # Pattern: ====== function_name (N calls) ======
            # Also handles merged names like "memcpy + memmove (503 calls)"
            func_pattern = r'=+ ([\w][\w +]*[\w]) \((\d+) calls\) =+'
            for match in re.finditer(func_pattern, content):
                func_name = match.group(1).strip()
                call_count = int(match.group(2))
                stats['functions'][func_name] = call_count
                stats['total_calls'] += call_count
                # For merged names like "memcpy + memmove", also index
                # by each individual function name for lookup convenience
                if ' + ' in func_name:
                    for part in func_name.split(' + '):
                        part = part.strip()
                        if part not in stats['functions']:
                            stats['functions'][part] = call_count

            # Split content into per-function sections for scoped parsing
            section_pattern = r'(=+ [\w][\w +]*[\w] \(\d+ calls\) =+)(.*?)(?==+ [\w][\w +]*[\w] \(\d+ calls\) =+|--- Function Call Distribution|$)'
            for sec_match in re.finditer(section_pattern, content, re.DOTALL):
                sec_header = sec_match.group(1)
                sec_body = sec_match.group(2)

                # Get the function name from the header
                hdr_match = re.search(r'=+ ([\w][\w +]*[\w]) \(\d+ calls\) =+', sec_header)
                if not hdr_match:
                    continue
                sec_func = hdr_match.group(1).strip()

                # Extract size distribution buckets from this section
                bucket_pattern = r'(\d+)\s*->\s*(\d+)\s*:\s*(\d+)'
                for match in re.finditer(bucket_pattern, sec_body):
                    high = int(match.group(2))
                    count = int(match.group(3))
                    bucket_num = (high + 1).bit_length() - 1
                    if bucket_num not in stats['buckets']:
                        stats['buckets'][bucket_num] = 0
                    stats['buckets'][bucket_num] += count

                # Extract alignment stats from this section
                # Handles: "Both 64B-aligned", "Src 64B-aligned", "Neither aligned", etc.
                align_pattern = r'(Src\s+\S+-aligned|Dst\s+\S+-aligned|Both\s+\S+-aligned|Neither aligned|Source aligned|Dest aligned|Both aligned)\s*:\s*(\d+)'
                sec_alignment = {}
                for match in re.finditer(align_pattern, sec_body):
                    raw_type = match.group(1).strip()
                    count = int(match.group(2))
                    # Normalize to canonical keys
                    if raw_type.startswith('Src') or raw_type.startswith('Source'):
                        key = 'Source aligned'
                    elif raw_type.startswith('Dst') or raw_type.startswith('Dest'):
                        key = 'Dest aligned'
                    elif raw_type.startswith('Both'):
                        key = 'Both aligned'
                    else:
                        key = 'Neither aligned'
                    sec_alignment[key] = count
                    stats['alignment'][key] = count  # global (compat)

                if sec_alignment:
                    stats['func_alignment'][sec_func] = sec_alignment
                    # Also index by individual parts of merged names
                    if ' + ' in sec_func:
                        for part in sec_func.split(' + '):
                            part = part.strip()
                            if part not in stats['func_alignment']:
                                stats['func_alignment'][part] = sec_alignment

    except FileNotFoundError:
        print(f"Error: {output_file} not found")

    return stats

def verify_call_count(expected: Dict, stats: Dict, allow_extra: bool = True) -> Tuple[bool, str]:
    """Verify call count for the target function matches expected.

    Compares the target function's call count (not total across all
    functions) against expected, since glibc may internally call other
    traced functions (e.g. strstr internally uses strchr, strlen, memcmp).

    Args:
        expected: Expected values dict
        stats: Actual stats dict
        allow_extra: If True, profiler may capture more calls than expected (glibc internals)
    """
    if 'EXPECTED_CALLS' not in expected:
        return True, "No expected call count specified"

    expected_calls = int(expected['EXPECTED_CALLS'])
    target_func = expected.get('EXPECTED_FUNCTION', '')

    # Look up the target function's call count (not the total across all functions)
    actual_calls = stats['functions'].get(target_func, 0)

    # If not found directly, check if the target is part of a merged entry
    if actual_calls == 0:
        for fname, count in stats['functions'].items():
            if target_func in fname.split(' + '):
                actual_calls = count
                break

    if allow_extra:
        # Profiler captures ALL calls including glibc internals (malloc, fprintf, pthread_create)
        # Allow up to 10% extra calls or 50 calls (whichever is larger)
        max_extra = max(50, int(expected_calls * 0.10))
        if expected_calls <= actual_calls <= expected_calls + max_extra:
            return True, f"Call count for {target_func}: {actual_calls} (expected {expected_calls}, +{actual_calls - expected_calls} overhead)"
        elif actual_calls < expected_calls:
            return False, f"Call count TOO LOW for {target_func}: {actual_calls} < expected {expected_calls} (missed calls!)"
        else:
            return False, f"Call count TOO HIGH for {target_func}: {actual_calls} > expected {expected_calls + max_extra} (tolerance exceeded)"
    else:
        # Strict check - must be exact (±5% tolerance)
        tolerance = max(1, int(expected_calls * 0.05))
        if abs(actual_calls - expected_calls) <= tolerance:
            return True, f"Call count for {target_func}: {actual_calls} (expected {expected_calls})"
        else:
            return False, f"Call count mismatch for {target_func}: {actual_calls} vs expected {expected_calls}"

def verify_size_distribution(expected: Dict, stats: Dict) -> Tuple[bool, str]:
    """Verify size distribution buckets match expected."""
    messages = []
    all_pass = True

    for key, value in expected.items():
        if key.startswith('EXPECTED_BUCKET_'):
            bucket_num = int(key.split('_')[2])
            expected_count = int(value)
            actual_count = stats['buckets'].get(bucket_num, 0)

            # Must have at least the expected count (may have more due to rounding)
            if actual_count >= expected_count:
                messages.append(f"  Bucket {bucket_num}: {actual_count} >= {expected_count} ✓")
            else:
                messages.append(f"  Bucket {bucket_num}: {actual_count} < {expected_count} ✗")
                all_pass = False

    if messages:
        return all_pass, "Size distribution:\n" + "\n".join(messages)
    return True, "No size distribution specified"

def verify_alignment(expected: Dict, stats: Dict) -> Tuple[bool, str]:
    """Verify alignment statistics match expected for the target function.

    Handles two output formats:
      - Dual-operand functions (memcpy, memcmp, strncpy, etc.): reports
        Src/Dst/Both/Neither aligned lines
      - Single-operand functions (memset, memchr, strnlen, strlen, etc.):
        reports only Src aligned (the one buffer's alignment)
    """
    if 'EXPECTED_ALIGNED' not in expected:
        return True, "No alignment test specified"

    expected_aligned = int(expected.get('EXPECTED_ALIGNED', 0))
    expected_unaligned = int(expected.get('EXPECTED_UNALIGNED', 0))
    target_func = expected.get('EXPECTED_FUNCTION', '')

    # Look up alignment stats for the target function specifically
    # (not global stats which may be overwritten by overhead functions)
    func_align = stats.get('func_alignment', {}).get(target_func, {})
    if not func_align:
        # Fallback: check merged names
        for fname, align_data in stats.get('func_alignment', {}).items():
            if target_func in fname.split(' + '):
                func_align = align_data
                break
    if not func_align:
        # Last resort: use global alignment stats
        func_align = stats.get('alignment', {})

    messages = []
    all_pass = True

    # Determine if this is a single-operand function (only Src aligned, no Both/Neither)
    has_both = 'Both aligned' in func_align
    has_src = 'Source aligned' in func_align

    # Tolerance: allow up to 20% or at least 5 extra calls (overhead from glibc)
    tolerance = max(5, int(expected_aligned * 0.20))

    if has_both:
        # Dual-operand function: use Both aligned / Neither aligned
        actual_both = func_align.get('Both aligned', 0)
        actual_neither = func_align.get('Neither aligned', 0)

        if abs(actual_both - expected_aligned) <= tolerance:
            messages.append(f"  Both aligned: {actual_both} ≈ {expected_aligned} ✓")
        else:
            messages.append(f"  Both aligned: {actual_both} vs expected {expected_aligned} ✗")
            all_pass = False

        if abs(actual_neither - expected_unaligned) <= tolerance:
            messages.append(f"  Neither aligned: {actual_neither} ≈ {expected_unaligned} ✓")
        else:
            messages.append(f"  Neither aligned: {actual_neither} vs expected {expected_unaligned} ✗")
            all_pass = False
    elif has_src:
        # Single-operand function: only Src aligned reported
        # "aligned" = Src aligned count, "unaligned" = total - Src aligned
        actual_aligned = func_align.get('Source aligned', 0)
        func_total = stats['functions'].get(target_func, 0)
        actual_unaligned = func_total - actual_aligned

        if abs(actual_aligned - expected_aligned) <= tolerance:
            messages.append(f"  Src aligned: {actual_aligned} ≈ {expected_aligned} ✓")
        else:
            messages.append(f"  Src aligned: {actual_aligned} vs expected {expected_aligned} ✗")
            all_pass = False

        if abs(actual_unaligned - expected_unaligned) <= tolerance:
            messages.append(f"  Src unaligned: {actual_unaligned} ≈ {expected_unaligned} ✓")
        else:
            messages.append(f"  Src unaligned: {actual_unaligned} vs expected {expected_unaligned} ✗")
            all_pass = False
    else:
        messages.append(f"  No alignment data found for {target_func}")
        all_pass = False

    return all_pass, f"Alignment stats for {target_func}:\n" + "\n".join(messages)

def main():
    if len(sys.argv) < 3:
        print("Usage: profiler_verifier.py <profiler_output> <test_stderr>")
        sys.exit(1)

    output_file = sys.argv[1]
    stderr_file = sys.argv[2]

    # Parse expected values and profiler output
    expected = parse_expected_values(stderr_file)
    stats = parse_profiler_output(output_file)

    if not expected:
        print("FAIL: No expected values found (test application may have failed to execute)")
        sys.exit(1)

    test_type = expected.get('TEST_TYPE', 'basic')

    print(f"Test Type: {test_type}")
    print(f"Expected function: {expected.get('EXPECTED_FUNCTION', 'N/A')}")
    print("")

    # Run appropriate verifications
    all_passed = True

    # Check call count (but skip for size_distribution - bucket accuracy is what matters)
    if test_type != 'size_distribution':
        passed, msg = verify_call_count(expected, stats)
        print(msg)
        if not passed:
            all_passed = False
        print("")
    else:
        # For size distribution, just report the count (validation is bucket-based)
        expected_calls = int(expected.get('EXPECTED_CALLS', 0))
        print(f"Call count: {stats['total_calls']} (expected {expected_calls} + glibc internals)")
        print("(Validation based on bucket counts, not total)")
        print("")

    # Check size distribution if applicable
    if test_type == 'size_distribution':
        passed, msg = verify_size_distribution(expected, stats)
        print(msg)
        if not passed:
            all_passed = False
        print("")

    # Check alignment if applicable
    if test_type == 'alignment':
        passed, msg = verify_alignment(expected, stats)
        print(msg)
        if not passed:
            all_passed = False
        print("")

    # custom_alignment: derive EXPECTED_ALIGNED/EXPECTED_UNALIGNED from
    # ALIGN_SRC_BYTES / ALIGN_DST_BYTES, then run the same verification.
    # The profiler is invoked with -a 64 (64-byte boundary) in the test runner.
    if test_type == 'custom_alignment':
        align_src = int(expected.get('ALIGN_SRC_BYTES', 0))
        align_dst = int(expected.get('ALIGN_DST_BYTES', 0))
        total_calls = int(expected.get('EXPECTED_CALLS', 0))
        profiler_boundary = 64  # matches -a 64 in test runner

        # If allocated alignment >= profiler boundary and is a multiple,
        # every call lands on a 64-byte boundary for that operand.
        src_ok = align_src > 0 and align_src % profiler_boundary == 0
        dst_ok = align_dst > 0 and align_dst % profiler_boundary == 0

        if src_ok and dst_ok:
            expected['EXPECTED_ALIGNED'] = str(total_calls)
            expected['EXPECTED_UNALIGNED'] = '0'
        elif src_ok or dst_ok:
            expected['EXPECTED_ALIGNED'] = str(total_calls)
            expected['EXPECTED_UNALIGNED'] = '0'
        else:
            expected['EXPECTED_ALIGNED'] = '0'
            expected['EXPECTED_UNALIGNED'] = str(total_calls)

        passed, msg = verify_alignment(expected, stats)
        print(msg)
        if not passed:
            all_passed = False
        print("")

    # Multithreaded specific info (validation already done by verify_call_count)
    if test_type == 'multithreaded':
        num_threads = int(expected.get('NUM_THREADS', 1))
        calls_per_thread = int(expected.get('CALLS_PER_THREAD', 0))
        print(f"Multithreaded test: {num_threads} threads × {calls_per_thread} calls/thread")
        print(f"Expected total: {num_threads * calls_per_thread}")
        print(f"Actual total: {stats['total_calls']}")
        print("(Count validation already performed by verify_call_count above)")
        print("")

    sys.exit(0 if all_passed else 1)

if __name__ == '__main__':
    main()
