# Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.
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

# ============================================================================
# Benchmark framework build configuration
# ============================================================================

# Documentation
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/benchmark_guide.md
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/)

# ---------------------------------------------------------------------------
# Python orchestration scripts  (source: scripts/)
# ---------------------------------------------------------------------------

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/bench.py
    DESTINATION ${CMAKE_BINARY_DIR}/test)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/benchmark.py
    DESTINATION ${CMAKE_BINARY_DIR}/test)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/tbm.py
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/fbm.py
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/gbm.py
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/new_gbm.py
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/dcperf.py
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/get_parser.py
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/)

# ---------------------------------------------------------------------------
# Legacy benchmark source  (source: legacy/)
# ---------------------------------------------------------------------------

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/legacy/gbench.cpp
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/gbench)

# ---------------------------------------------------------------------------
# Redesigned benchmark framework  (source: src/, include/)
# ---------------------------------------------------------------------------

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/gbench_main.cpp
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/gbench)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/core
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/include)

# ---------------------------------------------------------------------------
# Patches  (source: patches/)
# ---------------------------------------------------------------------------

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/patches/tbm.patch
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external/tinybench)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/patches/wdl_bench_memcpy_memset_only.patch
    DESTINATION ${CMAKE_BINARY_DIR}/tools/benchmark/external)
