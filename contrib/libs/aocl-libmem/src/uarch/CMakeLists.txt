# Copyright (C) 2024-26 Advanced Micro Devices, Inc. All rights reserved.
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

set(UARCH_SUPP_FUNCS ${DYN_DISP_SUPP_FUNCS})

add_library(uarch_objlib INTERFACE)

if (ALMEM_DYN_DISPATCH)
    set(uarch_list RANGE ${MIN_ZEN_VER} ${MAX_ZEN_VER})
else ()
    # Use specific architecture version (either user-specified via ALMEM_ARCH or auto-detected)
    set(uarch_list ${ARCH_VER})
endif()

foreach(uarch ${uarch_list})

    set(UARCH_SRC_FILES "")
    foreach(func ${UARCH_SUPP_FUNCS})
    list(APPEND UARCH_SRC_FILES
        zen${uarch}/${func}_zen${uarch}.c
        )
    endforeach()

    add_library(lib_zen${uarch} OBJECT ${UARCH_SRC_FILES})
    target_include_directories(lib_zen${uarch} PRIVATE ${LIBMEM_ROOT}/include)

    set(_MODULE_CC_FLAGS ${_CC_Flags}  -mno-vzeroupper)
    if (${CONFIG_COMPILER_HAS_ZEN${uarch}})
        list(APPEND _MODULE_CC_FLAGS -march=znver${uarch})
    else ()
        message(WARNING " *** Compiler doesn't support arch specific flag for zen${uarch} CPU\n"
                        "Proceeding with generic ISA flags... ")
        # zen4/zen5/zen6 always require AVX512, or enable AVX512 if system supports it
        if (${uarch} GREATER_EQUAL 4 OR ${HAS_AVX512})
            list(APPEND _MODULE_CC_FLAGS -mavx512f -mavx512bw -mavx512vbmi -mavx512vl)
        else ()
            list(APPEND _MODULE_CC_FLAGS -mavx2)
        endif ()
        list(APPEND _MODULE_CC_FLAGS -mbmi -mbmi2 -mlzcnt)
    endif ()
    set_target_properties(lib_zen${uarch} PROPERTIES POSITION_INDEPENDENT_CODE ON COMPILE_OPTIONS "${_MODULE_CC_FLAGS}")

    # Per-function code-layout tuning (function/label/jump/loop alignment and
    # disabled LTO/block-reordering) validated only on Zen5. Guard it to Zen5 so
    # other architectures keep the compiler/global defaults.
    if (${uarch} EQUAL 5)
        if (CMAKE_C_COMPILER_ID STREQUAL "GNU")
            set(_LARGE_FUNC_ALIGN "-falign-functions=4096;-falign-labels=1;-falign-jumps=1;-falign-loops=1;-fno-reorder-blocks-and-partition")
            set(_SMALL_FUNC_ALIGN "-falign-functions=64;-falign-labels=1;-falign-jumps=1;-falign-loops=1;-fno-reorder-blocks-and-partition")
        else ()
            set(_LARGE_FUNC_ALIGN "-falign-functions=4096")
            set(_SMALL_FUNC_ALIGN "-falign-functions=64")
        endif ()

        set(_LARGE_FUNCS memcpy memset)
        set(_LARGE_FUNCS_NOLTO memmove)
        set(_SMALL_FUNCS memcmp memchr mempcpy strcpy strlen strnlen strncpy strcmp strncmp strchr strcat strncat strspn strstr)

        foreach(_func ${_LARGE_FUNCS})
            set_source_files_properties(zen${uarch}/${_func}_zen${uarch}.c PROPERTIES
                COMPILE_OPTIONS "${_LARGE_FUNC_ALIGN}")
        endforeach()
        foreach(_func ${_LARGE_FUNCS_NOLTO})
            set_source_files_properties(zen${uarch}/${_func}_zen${uarch}.c PROPERTIES
                COMPILE_OPTIONS "${_LARGE_FUNC_ALIGN};-fno-lto")
        endforeach()
        foreach(_func ${_SMALL_FUNCS})
            set_source_files_properties(zen${uarch}/${_func}_zen${uarch}.c PROPERTIES
                COMPILE_OPTIONS "${_SMALL_FUNC_ALIGN}")
        endforeach()
    endif ()

    target_sources(uarch_objlib INTERFACE $<TARGET_OBJECTS:lib_zen${uarch}>)
endforeach()
