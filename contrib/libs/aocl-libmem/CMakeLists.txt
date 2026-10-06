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

cmake_minimum_required(VERSION 3.22)
# Auto-install git hooks
if(EXISTS "${CMAKE_SOURCE_DIR}/.githooks/pre-push")
    if(NOT EXISTS "${CMAKE_SOURCE_DIR}/.git/hooks/pre-push")
        file(COPY "${CMAKE_SOURCE_DIR}/.githooks/pre-push"
             DESTINATION "${CMAKE_SOURCE_DIR}/.git/hooks/"
             FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
        message(STATUS "✓ Git pre-push hook installed automatically")
    endif()
endif()

if(EXISTS "${CMAKE_SOURCE_DIR}/.githooks/pre-commit")
    if(NOT EXISTS "${CMAKE_SOURCE_DIR}/.git/hooks/pre-commit")
        file(COPY "${CMAKE_SOURCE_DIR}/.githooks/pre-commit"
             DESTINATION "${CMAKE_SOURCE_DIR}/.git/hooks/"
             FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
        message(STATUS "✓ Git pre-commit hook installed automatically")
    endif()
endif()
# avoid build in root directory
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
	message(FATAL_ERROR “In-source build detected!”)
endif()

# set the project name and version
set(LIBMEM_VERSION_STRING 5.3.2)

project(aocl-libmem VERSION ${LIBMEM_VERSION_STRING} LANGUAGES C CXX DESCRIPTION
                        "Library of AMD optimized string/memory functions")

# Target-based build support: when built as a subproject, CMAKE_SOURCE_DIR
# points at the parent (aocl) project, so cache our own root in LIBMEM_ROOT for
# subdirectory include paths and set AOCL_LIBMEM_SUBPROJECT to skip the tools.
set(LIBMEM_ROOT "${CMAKE_CURRENT_SOURCE_DIR}" CACHE INTERNAL "AOCL-LibMem source root")
if(NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(AOCL_LIBMEM_SUBPROJECT ON)
  message(STATUS "[aocl-libmem] building as a subproject; tools/bench utilities are skipped")
endif()

string(TIMESTAMP BUILD_DATE "%Y%m%d")

set(LIBMEM_BUILD_VERSION_STR
    "AOCL-LibMem ${LIBMEM_VERSION_STRING} Build ${BUILD_DATE}")

add_definitions(-DLIBMEM_BUILD_VERSION="${LIBMEM_BUILD_VERSION_STR}")

set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 17)

option(ALMEM_DYN_DISPATCH "CPU Dispatching" OFF)

option(ALMEM_ISA "ISA_TYPE" OFF)

option(ALMEM_LOGGING "Enable Logger" OFF)

option(ALMEM_TUNABLES "Enable Tunable Options" OFF)

option(ALMEM_GCOV "Enable Code Coverage" OFF)

set(ALMEM_ARCH "" CACHE STRING "Force specific Zen architecture (zen1, zen2, zen3, zen4, zen5, zen6). Leave empty for auto-detection.")
# Define supported Zen version range
set(MIN_ZEN_VER 1)
set(MAX_ZEN_VER 6)


option(ALMEM_STRICT_BOUNDS "Enable strict-bounds variant (masked loads only)" OFF)
if (ALMEM_STRICT_BOUNDS)
    add_definitions(-DALMEM_STRICT_BOUNDS)
    message(STATUS "Strict-bounds build variant enabled")
endif ()

if (ALMEM_LOGGING)
add_definitions(-DALMEM_LOGGER)
# uncomment the below for debug logs, LOG_LEVEL=DEBUG.
#add_definitions(-DLOG_LEVEL=4)
endif ()

if (ENABLE_TUNABLES) #retaining for backward compatibility
    set(ALMEM_TUNABLES ON)
endif()

# Validate and process ALMEM_ARCH option
if (ALMEM_ARCH)
    string(TOLOWER ${ALMEM_ARCH} ALMEM_ARCH_LOWER)
    # Build regex pattern dynamically based on MIN_ZEN_VER and MAX_ZEN_VER
    set(ZEN_RANGE "")
    foreach(i RANGE ${MIN_ZEN_VER} ${MAX_ZEN_VER})
        if(ZEN_RANGE)
            string(APPEND ZEN_RANGE "|${i}")
        else()
            string(APPEND ZEN_RANGE "${i}")
        endif()
    endforeach()

    if (ALMEM_ARCH_LOWER MATCHES "^zen(${ZEN_RANGE})$")
        string(REGEX REPLACE "^zen([0-9]+)$" "\\1" ARCH_VER "${ALMEM_ARCH_LOWER}")
        message(STATUS "ALMEM_ARCH: Forcing build for Zen${ARCH_VER}")
    else ()
        message(WARNING "Invalid ALMEM_ARCH value: '${ALMEM_ARCH}'. Must be one of: zen${MIN_ZEN_VER} to zen${MAX_ZEN_VER}. Falling back to next available option...")
        set(ALMEM_ARCH "")
    endif ()
endif()

#reset ISA for invalid options
if (NOT (${ALMEM_ISA} STREQUAL "avx2" OR ${ALMEM_ISA} STREQUAL "avx512"))
    if (${ALMEM_ISA} MATCHES "avx")
        message("InValid ISA option; Going ahead with next feasible option")
    endif ()
    set(ALMEM_ISA "")
endif ()

# ALMEM_ARCH has highest precedence
if (ALMEM_ARCH)
    if (ALMEM_ISA)
        message(WARNING "ALMEM_ARCH has precedence over ISA; Disabling ISA option...")
        set(ALMEM_ISA "")
    endif ()
    if (ALMEM_DYN_DISPATCH)
        message(WARNING "ALMEM_ARCH has precedence over DYN_DISPATCH; Disabling Dynamic CPU dispatching...")
        set(ALMEM_DYN_DISPATCH OFF)
    endif ()
    if (ALMEM_TUNABLES)
        message(WARNING "ALMEM_ARCH has precedence over TUNABLES; Disabling TUNABLES option...")
        set(ALMEM_TUNABLES OFF)
    endif ()
endif()

if (ALMEM_ISA) #ISA has precedence over DYN_DISPATCH
    if (ALMEM_DYN_DISPATCH)
        message(WARNING "ISA has precedence over DYN_DISPATCH; Disabling Dynamic CPU dispatching...")
        set(ALMEM_DYN_DISPATCH OFF)
    endif ()
    if (ALMEM_TUNABLES)
        message(WARNING "ISA has precedence over TUNABLES; Disabling TUNABLES option...")
        set(ALMEM_TUNABLES OFF)
    endif()
endif()

if (ALMEM_TUNABLES)
    set(ALMEM_DYN_DISPATCH ON)
endif()

# let the build system know the source directory
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)

# tools are bench/validator/python utilities; skip them in a subproject build
if(NOT AOCL_LIBMEM_SUBPROJECT)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tools)
endif()

file(WRITE ${CMAKE_BINARY_DIR}/version.h ${LIBMEM_BUILD_VERSION_STR})
