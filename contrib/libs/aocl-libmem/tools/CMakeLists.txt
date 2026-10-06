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

# libmem tools

# Detect CPU features BEFORE adding subdirectories so libmem_common
# (and all consumers) can use HAS_AVX512 at configure time.
set(CPU_DETECT_SRC_FILES "${CMAKE_SOURCE_DIR}/tools/detect_cpu_info.c")
set(CPU_INCLUDE "${CMAKE_SOURCE_DIR}/include")
set(AVX512_FEATURE_ENABLED "False")

try_run(HAS_AVX512 COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "feature_avx512")
if (HAS_AVX512)
    set(AVX512_FEATURE_ENABLED "True")
endif()

# Per-CCD L3 size in MB; used by bench.py for default range = 2 x L3.
try_run(L3_CACHE_MB COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "l3_cache")

# Hard cap on per-benchmark size. Single source of truth for C++ and Python.
set(LIBMEM_MAX_BENCHMARK_SIZE_BYTES 1073741824)   # 1 GiB
add_compile_definitions(LIBMEM_MAX_BENCHMARK_SIZE_BYTES=${LIBMEM_MAX_BENCHMARK_SIZE_BYTES})

option(LIBMEM_TOOLS_VERBOSE "Enable verbose debug output for validator and benchmark" OFF)
set(VERBOSE_ENABLED "False")
if(LIBMEM_TOOLS_VERBOSE)
    set(VERBOSE_ENABLED "True")
endif()

add_subdirectory(common)
add_subdirectory(benchmark)
add_subdirectory(analyser)
add_subdirectory(validator)
add_subdirectory(graph_generator)

# Top-level "tools" target
if(ALMEM_TOOLS)
    add_custom_target(tools
        DEPENDS prof_libmem profiler_test
        COMMENT "Building all AOCL-LibMem tools (profiler, test suite)"
    )
endif()

# Generate libmem_defs.py with library paths and feature flags
file(WRITE ${CMAKE_BINARY_DIR}/test/libmem_defs.py "LIBMEM_BIN_PATH='${CMAKE_INSTALL_PREFIX}/lib/lib${PROJECT_NAME}.so'\n")
file(APPEND ${CMAKE_BINARY_DIR}/test/libmem_defs.py "LIBMEM_ARCHIVE_PATH='${CMAKE_INSTALL_PREFIX}/lib'\n")
file(APPEND ${CMAKE_BINARY_DIR}/test/libmem_defs.py "AVX512_FEATURE_ENABLED = ${AVX512_FEATURE_ENABLED}\n")
file(APPEND ${CMAKE_BINARY_DIR}/test/libmem_defs.py "VERBOSE_ENABLED = ${VERBOSE_ENABLED}\n")
file(APPEND ${CMAKE_BINARY_DIR}/test/libmem_defs.py "L3_CACHE_MB = ${L3_CACHE_MB}\n")
file(APPEND ${CMAKE_BINARY_DIR}/test/libmem_defs.py "MAX_BENCHMARK_SIZE = ${LIBMEM_MAX_BENCHMARK_SIZE_BYTES}\n")
