# Copyright (C) 2022-26 Advanced Micro Devices, Inc. All rights reserved.
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

include(CheckCCompilerFlag)
check_c_compiler_flag("-march=znver1"    CONFIG_COMPILER_HAS_ZEN1)
check_c_compiler_flag("-march=znver2"    CONFIG_COMPILER_HAS_ZEN2)
check_c_compiler_flag("-march=znver3"    CONFIG_COMPILER_HAS_ZEN3)
check_c_compiler_flag("-march=znver4"    CONFIG_COMPILER_HAS_ZEN4)
check_c_compiler_flag("-march=znver5"    CONFIG_COMPILER_HAS_ZEN5)
check_c_compiler_flag("-march=znver6"    CONFIG_COMPILER_HAS_ZEN6)


set(ARCH_UNDF 255)
set(AMD_CPU 1)
set(CPU_DETECT_SRC_FILES "${LIBMEM_ROOT}/tools/detect_cpu_info.c")
set(CPU_INCLUDE "${LIBMEM_ROOT}/include")
set(CPU_FEATURES AVX512 AVX2 ERMS)

foreach(FEATURE ${CPU_FEATURES})
    string(TOLOWER ${FEATURE} FEATURE_LOWER)
    try_run(HAS_${FEATURE} COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
            COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "feature_${FEATURE_LOWER}")

    if (HAS_${FEATURE})
        message(STATUS "HAS_${FEATURE}: True")
    else ()
        message(STATUS "HAS_${FEATURE}: False")
    endif()
endforeach()

# Warn about compiling AVX512 code on AVX2-only machines
if (NOT HAS_AVX512)
    set(AVX512_WARNING_MSG "but this machine does NOT support AVX512. Running this binary on an AVX2-only machine will result in an ILLEGAL INSTRUCTION error.")
    # Check if ALMEM_ISA is set to avx512
    if (ALMEM_ISA AND "${ALMEM_ISA}" STREQUAL "avx512")
        message(WARNING "ALMEM_ISA is set to 'avx512', ${AVX512_WARNING_MSG}")
    endif()
    # Check if ALMEM_ARCH is set to zen4, zen5, or zen6 (AVX512 architectures)
    if (ARCH_VER AND ARCH_VER GREATER_EQUAL 4)
        message(WARNING "ALMEM_ARCH is set to 'zen${ARCH_VER}' which uses AVX512 instructions, ${AVX512_WARNING_MSG}")
    endif()
endif()

if (ALMEM_ARCH)
        message(STATUS "## Compiling for specific Zen architecture: Zen${ARCH_VER}...")
elseif (ALMEM_ISA)
        if (${ALMEM_ISA} MATCHES "avx512")
            message(STATUS "## Compiling for AVX512 CPUs...")
        elseif (${ALMEM_ISA} MATCHES "avx2")
            message(STATUS "## Compiling for AVX2 CPUs...")
        else ()
            message(WARNING " *** Invalid ISA options")
            message(STATUS "## Compiling for system default...")
            set(ALMEM_ISA "system")
        endif ()
elseif (ALMEM_DYN_DISPATCH)
    add_definitions(-DALMEM_DYN_DISPATCH)
    if (ALMEM_TUNABLES)
        add_definitions(-DALMEM_TUNABLES)
        add_definitions(-DAVX512_FEATURE_ENABLED)
        set(AVX512_FEATURE_AVAILABLE ON)
        message(STATUS "## Compiling for Tunable options with Dynamic CPU Dispatching...")
    else ()
        message(STATUS "## Compiling for Dynamic CPU Dispatching...")
    endif ()
else ()
    #Detect the CPU Vendor
    try_run(CPU_VENDOR COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
        COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "vendor")

    if (${CPU_VENDOR} MATCHES ${AMD_CPU})
        #Detect the CPU architecture
        try_run(ARCH_VER COMPILERESULT "${CMAKE_BINARY_DIR}/temp" SOURCES ${CPU_DETECT_SRC_FILES}
            COMPILE_DEFINITIONS -I${CPU_INCLUDE} ARGS "arch")


        if (${ARCH_VER} LESS ${ARCH_UNDF})
            message(STATUS "## Compiling for Native CPU - ZEN${ARCH_VER} ...")
        endif()
    else ()
        message(STATUS "Non AMD CPU detected. Going ahead with system defaults ...")
    endif()
endif()


# Compiler Warning flags
set(_CC_Warning_Flags -Wall -Wextra -Werror -Wunused-macros -Wundef)
# Compiler Exempted Warning flags
set(_CC_Exempted_Warning_Flags -Wno-unused-function)
# Compiler Optimization flags
set(_CC_Optimization_Flags -O3 -mcmodel=small -flto=auto)
# Compiler Code Generation flags
set(_CC_Code_Gen_Flags -fno-common)
# Code block alignment flags
set(_CC_Code_Alignment_Flags -falign-loops=64 -falign-functions=64)
# Label and Jump alignment compiler options are not supported in Clang
if (NOT ${CMAKE_C_COMPILER_ID}  STREQUAL "Clang")
    list(APPEND _CC_Code_Alignment_Flags -falign-labels=64 -falign-jumps=64:16:8)
endif ()
# Linker flag for IFUNC symbol binding support
set(_CC_Shared_Flags -Wl,-Bsymbolic)

set(_CC_Flags ${_CC_Warning_Flags} ${_CC_Exempted_Warning_Flags} ${_CC_Optimization_Flags} ${_CC_Code_Gen_Flags} ${_CC_Code_Alignment_Flags})

if(ALMEM_GCOV)
  list(APPEND _CC_Flags -fprofile-arcs -ftest-coverage)
endif()

add_compile_options(${_CC_Flags})

# Compiler exceptions
if ((${CMAKE_C_COMPILER_ID}  STREQUAL "Clang") AND ALMEM_TUNABLES)
    add_definitions(-DALMEM_CMPSD_CLANG_WORKAROUND)
endif ()

# Add source files
set(SOURCE_FILES libmem.c)

# Memory Functions
set(SUPP_MEM_FUNCS memcpy mempcpy memmove memset memcmp memchr)

# String Functions
set(SUPP_STR_FUNCS strcpy strncpy strcmp strncmp strcat strncat strstr strlen strnlen strchr strrchr strspn)

# Dynamic Dispatching Supported Functions
set(DYN_DISP_SUPP_FUNCS ${SUPP_MEM_FUNCS} ${SUPP_STR_FUNCS})

if (ALMEM_LOGGING)
    list(APPEND SOURCE_FILES logger.c)
endif ()

if (ALMEM_ARCH)
    if (ALMEM_ARCH_LOWER MATCHES "zen")
        add_subdirectory(uarch)
    endif ()
elseif (ALMEM_ISA)
    if (ALMEM_ISA MATCHES "avx")
        add_subdirectory(isa)
    endif ()
else ()
    add_subdirectory(uarch)
    if (ALMEM_DYN_DISPATCH)
        add_subdirectory(system)
    endif ()
    if (ALMEM_TUNABLES)
        list(APPEND SOURCE_FILES env_parser.c)
        add_subdirectory(tunable)
    endif ()
endif ()

# common object lib
add_library(libmem_objlib OBJECT ${SOURCE_FILES})
target_include_directories(libmem_objlib PRIVATE ${LIBMEM_ROOT}/include)
set_target_properties(libmem_objlib PROPERTIES POSITION_INDEPENDENT_CODE ON)

set(OBJ-LIST libmem_objlib)
# create object-list
if (ALMEM_ISA)
    if (ALMEM_ISA MATCHES "avx")
        list(APPEND OBJ-LIST isa_objlib)
    endif ()
else ()
    list(APPEND OBJ-LIST uarch_objlib)
    if (ALMEM_DYN_DISPATCH)
        list(APPEND OBJ-LIST system_objlib)
    endif ()
    if (ALMEM_TUNABLES)
        list(APPEND OBJ-LIST tunable_objlib)
    endif()
endif ()

# shared lib
add_library(${PROJECT_NAME} SHARED)
target_link_options(${PROJECT_NAME} PRIVATE ${_CC_Flags} ${_CC_Shared_Flags})
target_link_libraries(${PROJECT_NAME} PRIVATE ${OBJ-LIST})

# static lib
add_library("${PROJECT_NAME}_static" STATIC)
target_link_libraries("${PROJECT_NAME}_static" PRIVATE ${OBJ-LIST})

# Unified AOCL build: merge LibMem's object files into the single libaocl.
# LTO is disabled on these objects because GNU ld cannot link IFUNC objects that
# are emitted as LTO bitcode; turning it off keeps them as real, linkable code.
if(AOCL_TB_UNIFIED_BUILD)
    if(NOT COMMAND aocl_tb_add_objects)
        message(FATAL_ERROR "AOCL_TB_UNIFIED_BUILD requires aocl_tb_add_objects() to be defined by the parent project")
    endif()
    foreach(_almem_ol IN LISTS OBJ-LIST)
        set_target_properties(${_almem_ol} PROPERTIES INTERPROCEDURAL_OPTIMIZATION OFF)
    endforeach()
    aocl_tb_add_objects(${OBJ-LIST})
endif()

# renaming static binary
set_target_properties("${PROJECT_NAME}_static" PROPERTIES OUTPUT_NAME ${PROJECT_NAME})

set_target_properties(${PROJECT_NAME} PROPERTIES VERSION ${PROJECT_VERSION})

# Only override the install prefix for a top-level standalone build; forcing this
# cache var in a subproject would clobber the parent project's CMAKE_INSTALL_PREFIX.
if(NOT AOCL_LIBMEM_SUBPROJECT AND CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR} CACHE PATH "..." FORCE)
endif()

# Install LibMem's own libraries only in a standalone build; as a subproject or in
# the unified libaocl the objects already live there, so skip to avoid duplicates.
if(NOT AOCL_TB_UNIFIED_BUILD AND NOT AOCL_LIBMEM_SUBPROJECT)
  install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)
  install(TARGETS "${PROJECT_NAME}_static" ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)
endif()
