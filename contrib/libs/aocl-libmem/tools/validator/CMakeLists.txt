# Copyright (C) 2023-26 Advanced Micro Devices, Inc. All rights reserved.

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
# Validator build configuration
# ============================================================================

# Documentation and support files
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/validator_guide.md
    DESTINATION ${CMAKE_BINARY_DIR}/tools/validator/)

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tools/validator/glibc/)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/glibc/glibc_validator.sh
    DESTINATION ${CMAKE_BINARY_DIR}/tools/validator/glibc/)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/validator.py
    DESTINATION ${CMAKE_BINARY_DIR}/test)

install(PROGRAMS ${CMAKE_BINARY_DIR}/tools/validator/CTestTestfile.cmake
    DESTINATION ${CMAKE_BINARY_DIR}/test)


# ---------------------------------------------------------------------------
# Legacy C validator  (source: legacy/)
# ---------------------------------------------------------------------------
add_executable(libmem_validator ${CMAKE_CURRENT_SOURCE_DIR}/legacy/libmem_validator.c)

target_compile_options(libmem_validator PRIVATE -Wall -Wextra -O0)
# Diagnostic output for legacy and unified validators is driven by the same
# umbrella flag (LIBMEM_TOOLS_VERBOSE, declared in tools/CMakeLists.txt).
# The internal C-source macro name (LIBMEM_VALIDATOR_DEBUG) is preserved
# only because the legacy .c uses it at #ifdef sites.
if(LIBMEM_TOOLS_VERBOSE)
    target_compile_definitions(libmem_validator PRIVATE LIBMEM_VALIDATOR_DEBUG=1)
    message(STATUS "libmem_validator: DEBUG mode enabled")
endif()
target_link_libraries(libmem_validator m)


# ---------------------------------------------------------------------------
# Modular C++ validator
# ---------------------------------------------------------------------------
add_executable(libmem_validator_unified
    ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
)

target_compile_features(libmem_validator_unified PRIVATE cxx_std_17)
target_compile_options(libmem_validator_unified PRIVATE -Wall -Wextra -O0)

target_include_directories(libmem_validator_unified PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(libmem_validator_unified libmem_common)

if(LIBMEM_TOOLS_VERBOSE)
    target_compile_definitions(libmem_validator_unified PRIVATE LIBMEM_VERBOSE=1)
endif()

target_link_libraries(libmem_validator_unified m)

message(STATUS "Building validators: libmem_validator (C), libmem_validator_unified (C++)")


# ---------------------------------------------------------------------------
# Dynamic GTest validator
# ---------------------------------------------------------------------------
add_executable(libmem_validator_gtest
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gtest/gtest_main.cpp
)

target_compile_features(libmem_validator_gtest PRIVATE cxx_std_17)
target_compile_options(libmem_validator_gtest PRIVATE -Wall -Wextra -O0)

target_include_directories(libmem_validator_gtest PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(libmem_validator_gtest libmem_common)
target_link_libraries(libmem_validator_gtest m)


# ============================================================================
# CTest configuration
# ============================================================================

# ---------------------------------------------------------------------------
# Helper functions for test generation
# ---------------------------------------------------------------------------

# Generate powers of 2 sizes
function(generate_power_of_2_sizes OUTPUT_VAR START END)
    set(values "")
    set(current ${START})
    while(${current} LESS_EQUAL ${END})
        list(APPEND values ${current})
        math(EXPR current "${current} * 2")
    endwhile()
    set(${OUTPUT_VAR} ${values} PARENT_SCOPE)
endfunction()

# Generate threshold boundary values
function(generate_threshold_values OUTPUT_VAR THRESHOLD RANGE VALUES_LIST)
    set(result "")
    math(EXPR th_start "${THRESHOLD} - ${RANGE}")
    math(EXPR th_end "${THRESHOLD} + ${RANGE}")

    foreach(value IN LISTS VALUES_LIST)
        math(EXPR temp_value "${th_start} + ${value}")
        if(temp_value LESS_EQUAL ${THRESHOLD})
            list(APPEND result ${temp_value})
        endif()
    endforeach()

    foreach(value IN LISTS VALUES_LIST)
        math(EXPR temp_value "${THRESHOLD} + ${value}")
        if(temp_value LESS_EQUAL ${th_end})
            list(APPEND result ${temp_value})
        endif()
    endforeach()

    set(${OUTPUT_VAR} ${result} PARENT_SCOPE)
endfunction()

# Generate power-of-2 offset values for threshold testing
function(generate_offset_values OUTPUT_VAR MAX_SHIFT)
    set(values "")
    foreach(shift RANGE 0 ${MAX_SHIFT})
        math(EXPR value "1 << ${shift}")
        list(APPEND values ${value})
    endforeach()
    set(${OUTPUT_VAR} ${values} PARENT_SCOPE)
endfunction()

# Add validator tests for a list of functions and sizes
# PREFIX (e.g. "legacy_" or "unified_") is also used as the ctest LABEL
# (trailing underscore stripped), enabling `ctest -L legacy` / `ctest -L unified`.
function(add_validator_tests PREFIX VALIDATOR_EXEC FUNC_LIST SIZE_LIST SUFFIX)
    set(extra_args ${ARGN})
    string(REGEX REPLACE "_$" "" test_label "${PREFIX}")

    foreach(pattern IN LISTS FUNC_LIST)
        string(REGEX REPLACE "[^A-Za-z0-9]" "_" sanitized_pattern "${pattern}")
        foreach(size IN LISTS SIZE_LIST)
            set(test_name "${PREFIX}${sanitized_pattern}_${SUFFIX}_${size}B")
            add_test(
                NAME "${test_name}"
                COMMAND env LD_PRELOAD=${LIBMEM_PATH}
                ${VALIDATOR_EXEC} ${pattern} ${size} ${extra_args}
            )
            set_tests_properties("${test_name}"
                PROPERTIES
                FAIL_REGULAR_EXPRESSION "ERROR"
                LABELS "${test_label}"
            )
        endforeach()
    endforeach()
endfunction()

# Add shift tests with readable names (KB, MB) and timeout
function(add_shift_tests PREFIX VALIDATOR_EXEC FUNC_LIST SIZE_LIST TIMEOUT)
    string(REGEX REPLACE "_$" "" test_label "${PREFIX}")

    foreach(pattern IN LISTS FUNC_LIST)
        string(REGEX REPLACE "[^A-Za-z0-9]" "_" sanitized_pattern "${pattern}")
        foreach(size IN LISTS SIZE_LIST)
            if(${size} LESS 1024)
                set(value ${size})
                set(unit "B")
            elseif(${size} LESS 1048576)
                math(EXPR value "${size}/1024")
                set(unit "KB")
            else()
                math(EXPR value "${size}/1048576")
                set(unit "MB")
            endif()

            set(test_name "${PREFIX}${sanitized_pattern}_shift_${value}${unit}")
            add_test(
                NAME "${test_name}"
                COMMAND env LD_PRELOAD=${LIBMEM_PATH}
                ${VALIDATOR_EXEC} ${pattern} ${size}
            )
            set_tests_properties("${test_name}"
                PROPERTIES
                TIMEOUT ${TIMEOUT}
                FAIL_REGULAR_EXPRESSION "ERROR"
                LABELS "${test_label}"
            )
        endforeach()
    endforeach()
endfunction()

# Add tests for both legacy and unified validators
macro(add_dual_validator_tests TEST_TYPE FUNC_LIST SIZE_LIST)
    set(_extra_args ${ARGN})

    add_validator_tests("legacy_" "${VALIDATOR_EXEC}" "${FUNC_LIST}" "${SIZE_LIST}" "${TEST_TYPE}" ${_extra_args})
    add_validator_tests("unified_" "${VALIDATOR_UNIFIED_EXEC}" "${FUNC_LIST}" "${SIZE_LIST}" "${TEST_TYPE}" ${_extra_args})
endmacro()

# ---------------------------------------------------------------------------
# Range-based registration (one ctest per size range, not per byte)
# ---------------------------------------------------------------------------
# The binaries loop sizes internally when given "START:END", so the manifest
# stays tiny (fast ctest startup) with identical coverage.
# LIBMEM_VALIDATOR_RANGE_SHARDS splits each range into N contiguous windows
# for more `ctest -j` parallelism and narrower `--rerun-failed`; N=1 is the
# smallest manifest.
set(LIBMEM_VALIDATOR_RANGE_SHARDS "1" CACHE STRING
    "Number of ctest shards per (function,type,validator) size range")

# Register range tests for one validator across a list of functions.
function(add_validator_range_tests PREFIX VALIDATOR_EXEC FUNC_LIST RANGE_START RANGE_END SUFFIX)
    set(extra_args ${ARGN})
    string(REGEX REPLACE "_$" "" test_label "${PREFIX}")

    set(shards ${LIBMEM_VALIDATOR_RANGE_SHARDS})
    if(shards LESS 1)
        set(shards 1)
    endif()

    math(EXPR span "${RANGE_END} - ${RANGE_START} + 1")
    math(EXPR chunk "(${span} + ${shards} - 1) / ${shards}")
    if(chunk LESS 1)
        set(chunk 1)
    endif()

    foreach(pattern IN LISTS FUNC_LIST)
        string(REGEX REPLACE "[^A-Za-z0-9]" "_" sanitized_pattern "${pattern}")
        set(shard_start ${RANGE_START})
        while(shard_start LESS_EQUAL ${RANGE_END})
            math(EXPR shard_end "${shard_start} + ${chunk} - 1")
            if(shard_end GREATER ${RANGE_END})
                set(shard_end ${RANGE_END})
            endif()

            set(test_name "${PREFIX}${sanitized_pattern}_${SUFFIX}_${shard_start}_${shard_end}B")
            add_test(
                NAME "${test_name}"
                COMMAND env LD_PRELOAD=${LIBMEM_PATH}
                ${VALIDATOR_EXEC} ${pattern} ${shard_start}:${shard_end} ${extra_args}
            )
            # TIMEOUT bounds a hung/crashed range test so one bad size cannot
            # block the whole suite (matches the shift tests' MAX_TIMEOUT).
            set_tests_properties("${test_name}"
                PROPERTIES
                TIMEOUT ${MAX_TIMEOUT}
                FAIL_REGULAR_EXPRESSION "ERROR"
                LABELS "${test_label}"
            )

            math(EXPR shard_start "${shard_end} + 1")
        endwhile()
    endforeach()
endfunction()

# Register range tests for both legacy and unified validators.
macro(add_dual_validator_range_tests TEST_TYPE FUNC_LIST RANGE_START RANGE_END)
    set(_extra_args ${ARGN})

    add_validator_range_tests("legacy_" "${VALIDATOR_EXEC}" "${FUNC_LIST}" ${RANGE_START} ${RANGE_END} "${TEST_TYPE}" ${_extra_args})
    add_validator_range_tests("unified_" "${VALIDATOR_UNIFIED_EXEC}" "${FUNC_LIST}" ${RANGE_START} ${RANGE_END} "${TEST_TYPE}" ${_extra_args})
endmacro()

# Add shift tests for both validators
macro(add_dual_shift_tests FUNC_LIST SIZE_LIST TIMEOUT)
    add_shift_tests("legacy_" "${VALIDATOR_EXEC}" "${FUNC_LIST}" "${SIZE_LIST}" "${TIMEOUT}")
    add_shift_tests("unified_" "${VALIDATOR_UNIFIED_EXEC}" "${FUNC_LIST}" "${SIZE_LIST}" "${TIMEOUT}")
endmacro()

# ---------------------------------------------------------------------------
# CPU/ISA detection and test variables
# ---------------------------------------------------------------------------

set(AMD_CPU 1)
set(ARCH_ZEN5 5)
set(ARCH_ZEN6 6)
set(CPU_DETECT_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/../detect_cpu_info.c")
set(CPU_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../../include")

try_run(CPU_VENDOR COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
    COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "vendor")

if (${CPU_VENDOR} MATCHES ${AMD_CPU})
    set(CPU "AMD")
endif()

try_run(HAS_AVX512 COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "feature_avx512")
if (HAS_AVX512)
    SET(ISA "AVX512")
    target_compile_definitions(libmem_validator PRIVATE AVX512_FEATURE_ENABLED)
    try_run(ARCH_VER COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
    COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "arch")
    if (${ARCH_VER} MATCHES ${ARCH_ZEN6})
        SET(ARCH "ZEN6")
    elseif (${ARCH_VER} MATCHES ${ARCH_ZEN5})
        SET(ARCH "ZEN5")
    endif()
else()
    try_run(HAS_AVX2 COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "feature_avx2")
    if (HAS_AVX2)
        SET(ISA "AVX2")
    endif()
endif()

set(PAGE_SIZE 4096)
set(MAX_SIZE 33554432)
set(MAX_TIMEOUT 12000)
set(LIBMEM_PATH ${CMAKE_INSTALL_PREFIX}/lib/libaocl-libmem.so)
set(VALIDATOR_EXEC ${CMAKE_BINARY_DIR}/tools/validator/libmem_validator)
set(VALIDATOR_UNIFIED_EXEC ${CMAKE_BINARY_DIR}/tools/validator/libmem_validator_unified)

set(supp_func "memcpy" "memcmp" "memset" "memmove" "mempcpy" "memchr"
    "strcpy" "strncpy" "strcmp" "strncmp" "strlen" "strnlen" "strcat" "strncat" "strstr" "strspn" "strchr" "strrchr")

set(load_store_func "memcpy" "mempcpy" "memmove")
set(store_func "memset")

# ---------------------------------------------------------------------------
# Test registration
# ---------------------------------------------------------------------------

enable_testing()

generate_power_of_2_sizes(SHIFT_SIZES 1 ${MAX_SIZE})

message(STATUS "Adding validator tests for: ${supp_func}")

# Exhaustive 0..PAGE_SIZE byte sweeps as ranges (looped inside the binary).
add_dual_validator_range_tests("iter" "${supp_func}" 0 ${PAGE_SIZE})
add_dual_shift_tests("${supp_func}" "${SHIFT_SIZES}" "${MAX_TIMEOUT}")
add_dual_validator_range_tests("align" "${supp_func}" 0 ${PAGE_SIZE} 0 0 1)

message(STATUS "Basic validator tests added: iterator, shift, and alignment tests")

# ---------------------------------------------------------------------------
# AMD-specific threshold testing
# ---------------------------------------------------------------------------
if(CPU STREQUAL "AMD")
    message(STATUS "Adding AMD-specific threshold tests")

    try_run(L2_CACHE_512KB COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "l2_cache")
    math(EXPR L2_CACHE "${L2_CACHE_512KB} * 512 * 1024")

    try_run(L3_CACHE_MB COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "l3_cache")
    math(EXPR L3_CACHE "${L3_CACHE_MB} * 1024 * 1024")

    try_run(NT_TH_MB COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "nt_threshold")
    math(EXPR NT_TH "${NT_TH_MB} * 1024 * 1024")

    if(ISA STREQUAL "AVX512")
        set(TH_RANGE 512)
        set(MAX_SHIFT 9)
    elseif(ISA STREQUAL "AVX2")
        set(TH_RANGE 128)
        set(MAX_SHIFT 7)
    else()
        message(FATAL_ERROR "SKIPPING Threshold validation - unsupported ISA")
    endif()

    generate_offset_values(OFFSET_VALUES ${MAX_SHIFT})

    generate_threshold_values(L2_TH_SIZES ${L2_CACHE} ${TH_RANGE} "${OFFSET_VALUES}")
    add_dual_validator_tests("thresholdL2" "${load_store_func}" "${L2_TH_SIZES}" 0 0 1)

    generate_threshold_values(NT_TH_SIZES ${NT_TH} ${TH_RANGE} "${OFFSET_VALUES}")
    add_dual_validator_tests("threshold_nt" "${load_store_func}" "${NT_TH_SIZES}" 0 0 1)

    if(ISA STREQUAL "AVX2")
        generate_threshold_values(MEMSET_NT_SIZES ${L3_CACHE} ${TH_RANGE} "${OFFSET_VALUES}")
        add_dual_validator_tests("threshold_nt" "${store_func}" "${MEMSET_NT_SIZES}" 0 0 1)
    endif()

    message(STATUS "AMD threshold tests added: L2 and NT thresholds")

    # ZEN5/ZEN6-SPECIFIC THRESHOLD TESTING
    if(ARCH STREQUAL "ZEN5" OR ARCH STREQUAL "ZEN6")
        message(STATUS "Adding ${ARCH}-specific threshold tests")

        try_run(ALIGNED_VEC_CPY_TH_KB COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
            COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "aligned_vec_cpy_threshold")
        math(EXPR ALIGNED_VEC_CPY_TH "${ALIGNED_VEC_CPY_TH_KB} * 1024")
        generate_threshold_values(ALIGNED_VEC_CPY_SIZES ${ALIGNED_VEC_CPY_TH} ${TH_RANGE} "${OFFSET_VALUES}")
        add_dual_validator_tests("aligned_vector" "${load_store_func}" "${ALIGNED_VEC_CPY_SIZES}" 0 0 1)

        try_run(ALIGNED_VEC_SET_TH_KB COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
            COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "aligned_vec_set_threshold")
        math(EXPR ALIGNED_VEC_SET_TH "${ALIGNED_VEC_SET_TH_KB} * 1024")
        generate_threshold_values(ALIGNED_VEC_SET_SIZES ${ALIGNED_VEC_SET_TH} ${TH_RANGE} "${OFFSET_VALUES}")
        add_dual_validator_tests("aligned_vector" "${store_func}" "${ALIGNED_VEC_SET_SIZES}" 0 0 1)

        try_run(ZEN5_NT_TH_MB COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
            COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "zen5_nt_store_threshold")
        math(EXPR ZEN5_NT_TH "${ZEN5_NT_TH_MB} * 1024 * 1024")
        generate_threshold_values(ZEN5_NT_SIZES ${ZEN5_NT_TH} ${TH_RANGE} "${OFFSET_VALUES}")
        add_dual_validator_tests("threshold_nt" "${store_func}" "${ZEN5_NT_SIZES}" 0 0 1)

        message(STATUS "${ARCH}-specific threshold tests added")
    endif()

endif()

message(STATUS "Validator test configuration complete")
