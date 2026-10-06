#!/bin/bash
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

# ===================================================================
# AOCL-LibMem Profiler — Comprehensive Correctness Test Runner
# ===================================================================
# Tests all 17 supported functions across three categories:
#   1. Call Count Tests         — all 17 functions, single-threaded
#   2. Multithreaded Tests      — all 17 functions, multiple thread counts
#   3. Alignment Tests          — all 10 distribution functions
#   4. Alignment + Threading    — key dual-pointer functions with threads
# ===================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Locate binaries: check the script directory first (installed layout),
# then fall back to a build/ subdirectory (source-tree layout).
if [ -f "$SCRIPT_DIR/prof_libmem" ]; then
    PROFILER="$SCRIPT_DIR/prof_libmem"
    TEST_APP="$SCRIPT_DIR/profiler_test"
elif [ -f "$SCRIPT_DIR/build/prof_libmem" ]; then
    PROFILER="$SCRIPT_DIR/build/prof_libmem"
    TEST_APP="$SCRIPT_DIR/build/profiler_test"
else
    PROFILER=""
    TEST_APP=""
fi
VERIFIER="$SCRIPT_DIR/profiler_verifier.py"

# ── All 17 supported functions ──────────────────────────────────────
DIST_FUNCS="memset memcpy mempcpy memmove memcmp memchr strncpy strncmp strncat strnlen"
COUNT_FUNCS="strcpy strcmp strcat strlen strchr strstr strspn"

# Dual-pointer distribution functions (src + dst) for alignment tests
DUAL_PTR_FUNCS="memcpy mempcpy memmove memcmp strncpy strncmp strncat"

# Single-pointer distribution functions (src-only or dst-only)
SINGLE_PTR_FUNCS="memset memchr strnlen"

# Thread counts to test
THREAD_COUNTS="1 2 4 $(nproc)"

# ── Preflight checks ───────────────────────────────────────────────
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Must run as root (BPF requires root privileges)${NC}"
    exit 1
fi

if [ -z "$PROFILER" ] || [ ! -f "$PROFILER" ] || [ ! -f "$TEST_APP" ]; then
    echo -e "${RED}Error: prof_libmem or profiler_test not found.${NC}"
    echo "Build them first with:  cmake -DALMEM_TOOLS=ON -B build && cd build && make tools"
    exit 1
fi

# Verify the test app can actually execute (catch glibc version mismatches etc.)
_preflight_out=$("$TEST_APP" --help 2>&1 || true)
if echo "$_preflight_out" | grep -qiE 'GLIBC.*not found|cannot open shared|No such file|Exec format'; then
    echo -e "${RED}Error: Test application cannot execute on this system:${NC}"
    echo "$_preflight_out" | head -3
    echo ""
    echo "The test binary was likely compiled against a newer glibc than is"
    echo "available on this system.  Rebuild on this machine:"
    echo "  cmake -DALMEM_TOOLS=ON -B build && cd build && make tools"
    exit 1
fi

echo "==================================================================="
echo "    AOCL-LibMem Profiler — Comprehensive Correctness Test"
echo "==================================================================="
echo "Profiler : $PROFILER"
echo "Test app : $TEST_APP"
echo "Verifier : $VERIFIER"
echo "System CPUs: $(nproc)"
echo "==================================================================="
echo ""

# ── Helper: run a basic / multithreaded test ────────────────────────
run_test() {
    local func_name=$1
    local calls=$2
    local threads=${3:-1}
    local prof_flags="${4:--vv -t 5}"
    local test_id="${func_name}_${calls}_${threads}"

    echo -e "${YELLOW}Testing: $func_name ($calls calls, $threads thread(s))${NC}"

    local output_file="/tmp/prof_test_${test_id}.log"
    local stderr_file="/tmp/prof_stderr_${test_id}.log"

    # Run profiler with test app
    $PROFILER $prof_flags -e $TEST_APP $func_name $calls $threads \
        > "$output_file" 2>&1 &
    local prof_pid=$!

    sleep 0.5
    wait $prof_pid 2>/dev/null
    local prof_rc=$?

    # Extract expected values
    grep -E "EXPECTED|TEST_TYPE|NUM_THREADS|CALLS_PER_THREAD" \
        "$output_file" > "$stderr_file" 2>/dev/null || true

    # Quick sanity: if no EXPECTED lines found, the test app likely failed
    if ! grep -q "EXPECTED_FUNCTION" "$stderr_file" 2>/dev/null; then
        echo -e "${RED}✗ FAILED (test app did not emit expected values; exit=$prof_rc)${NC}"
        return 1
    fi

    # Verify
    if [ -f "$VERIFIER" ]; then
        local verifier_output
        verifier_output=$(python3 "$VERIFIER" "$output_file" "$stderr_file" 2>&1)
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ PASSED${NC}"
            return 0
        else
            echo -e "${RED}✗ FAILED (see $output_file)${NC}"
            echo "$verifier_output" | head -5
            return 1
        fi
    else
        echo -e "${YELLOW}  (Manual verification required)${NC}"
        return 0
    fi
}

# ── Helper: run an alignment test ───────────────────────────────────
run_alignment_test() {
    local func_name=$1
    local aligned_calls=$2
    local unaligned_calls=$3
    local test_id="align_${func_name}_${aligned_calls}_${unaligned_calls}"

    echo -e "${YELLOW}Testing alignment: $func_name " \
        "($aligned_calls aligned + $unaligned_calls unaligned)${NC}"

    local output_file="/tmp/prof_test_${test_id}.log"
    local stderr_file="/tmp/prof_stderr_${test_id}.log"

    # Run profiler with alignment test mode (-a 64)
    $PROFILER -vv -a 64 -t 5 \
        -e $TEST_APP $func_name $aligned_calls 1 $unaligned_calls \
        > "$output_file" 2>&1 &
    local prof_pid=$!

    sleep 0.5
    wait $prof_pid 2>/dev/null
    local prof_rc=$?

    # Extract expected values (including EXPECTED_ALIGNED / EXPECTED_UNALIGNED)
    grep -E "EXPECTED|TEST_TYPE|EXPECTED_ALIGNED|EXPECTED_UNALIGNED" \
        "$output_file" > "$stderr_file" 2>/dev/null || true

    # Quick sanity: if no EXPECTED lines found, the test app likely failed
    if ! grep -q "EXPECTED_FUNCTION" "$stderr_file" 2>/dev/null; then
        echo -e "${RED}✗ FAILED (test app did not emit expected values; exit=$prof_rc)${NC}"
        return 1
    fi

    # Verify
    if [ -f "$VERIFIER" ]; then
        local verifier_output
        verifier_output=$(python3 "$VERIFIER" "$output_file" "$stderr_file" 2>&1)
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ PASSED${NC}"
            return 0
        else
            echo -e "${RED}✗ FAILED (see $output_file)${NC}"
            echo "$verifier_output" | head -5
            return 1
        fi
    else
        echo -e "${YELLOW}  (Manual verification required)${NC}"
        return 0
    fi
}

# ── Helper: run alignment + multithreaded combined test ─────────────
run_alignment_threaded_test() {
    local func_name=$1
    local aligned_calls=$2
    local unaligned_calls=$3
    local threads=$4
    local test_id="align_mt_${func_name}_${aligned_calls}_${unaligned_calls}_t${threads}"

    echo -e "${YELLOW}Testing alignment+threading: $func_name " \
        "($aligned_calls aligned + $unaligned_calls unaligned, $threads threads)${NC}"

    local output_file="/tmp/prof_test_${test_id}.log"
    local stderr_file="/tmp/prof_stderr_${test_id}.log"

    # Use new-style options: aligned src+dst with threads
    $PROFILER -vv -a 64 -t 5 \
        -e $TEST_APP $func_name $aligned_calls \
        --align-src=64B --align-dst=64B --threads=$threads \
        > "$output_file" 2>&1 &
    local prof_pid=$!

    sleep 0.5
    wait $prof_pid 2>/dev/null
    local prof_rc=$?

    grep -E "EXPECTED|TEST_TYPE|NUM_THREADS|CALLS_PER_THREAD|ALIGN" \
        "$output_file" > "$stderr_file" 2>/dev/null || true

    # Quick sanity: if no EXPECTED lines found, the test app likely failed
    if ! grep -q "EXPECTED_FUNCTION" "$stderr_file" 2>/dev/null; then
        echo -e "${RED}✗ FAILED (test app did not emit expected values; exit=$prof_rc)${NC}"
        return 1
    fi

    if [ -f "$VERIFIER" ]; then
        local verifier_output
        verifier_output=$(python3 "$VERIFIER" "$output_file" "$stderr_file" 2>&1)
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ PASSED${NC}"
            return 0
        else
            echo -e "${RED}✗ FAILED (see $output_file)${NC}"
            echo "$verifier_output" | head -5
            return 1
        fi
    else
        echo -e "${YELLOW}  (Manual verification required)${NC}"
        return 0
    fi
}

# ── Track statistics ────────────────────────────────────────────────
total_tests=0
passed_tests=0

# ===================================================================
# 1. Call Count Tests — All 17 Functions (single-threaded)
# ===================================================================
echo "==================================================================="
echo "1. Call Count Tests (All 17 Functions, single-threaded)"
echo "==================================================================="

# Distribution functions (10)
for func in $DIST_FUNCS; do
    total_tests=$((total_tests + 1))
    if run_test $func 500 1; then
        passed_tests=$((passed_tests + 1))
    fi
done

# Count-only functions (7) — need -c flag
for func in $COUNT_FUNCS; do
    total_tests=$((total_tests + 1))
    if run_test $func 500 1 "-vv -c -t 5"; then
        passed_tests=$((passed_tests + 1))
    fi
done

# ===================================================================
# 2. Multithreaded Tests — All 17 Functions
# ===================================================================
echo ""
echo "==================================================================="
echo "2. Multithreaded Tests (All 17 Functions)"
echo "==================================================================="

# Distribution functions × all thread counts
for func in $DIST_FUNCS; do
    for threads in $THREAD_COUNTS; do
        total_tests=$((total_tests + 1))
        if run_test $func 1000 $threads; then
            passed_tests=$((passed_tests + 1))
        fi
    done
done

# Count-only functions × all thread counts (with -c flag)
for func in $COUNT_FUNCS; do
    for threads in $THREAD_COUNTS; do
        total_tests=$((total_tests + 1))
        if run_test $func 1000 $threads "-vv -c -t 5"; then
            passed_tests=$((passed_tests + 1))
        fi
    done
done

# ===================================================================
# 3. Alignment Tests — All 10 Distribution Functions
# ===================================================================
echo ""
echo "==================================================================="
echo "3. Alignment Tests (All 10 Distribution Functions)"
echo "==================================================================="

# Dual-pointer functions: both src and dst alignment verified
for func in $DUAL_PTR_FUNCS; do
    total_tests=$((total_tests + 1))
    if run_alignment_test $func 100 100; then
        passed_tests=$((passed_tests + 1))
    fi
done

# Single-pointer functions: alignment test still exercises aligned vs
# unaligned paths (dst-only for memset, src-only for memchr/strnlen)
for func in $SINGLE_PTR_FUNCS; do
    total_tests=$((total_tests + 1))
    if run_alignment_test $func 100 100; then
        passed_tests=$((passed_tests + 1))
    fi
done

# ===================================================================
# 4. Alignment + Threading Combined Tests (Key Dual-Pointer Functions)
# ===================================================================
echo ""
echo "==================================================================="
echo "4. Alignment + Threading Combined Tests"
echo "==================================================================="

for func in memcpy memmove memcmp strncpy; do
    for threads in 2 4; do
        total_tests=$((total_tests + 1))
        if run_alignment_threaded_test $func 200 0 $threads; then
            passed_tests=$((passed_tests + 1))
        fi
    done
done

# ===================================================================
# Summary
# ===================================================================
echo ""
echo "==================================================================="
echo "                        Test Summary"
echo "==================================================================="
echo "Total tests: $total_tests"
echo -e "Passed: ${GREEN}$passed_tests${NC}"
echo -e "Failed: ${RED}$((total_tests - passed_tests))${NC}"
echo "==================================================================="

if [ $passed_tests -eq $total_tests ]; then
    echo -e "${GREEN}✓ All tests PASSED!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests FAILED${NC}"
    exit 1
fi
