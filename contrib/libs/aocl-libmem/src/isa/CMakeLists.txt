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

set(ISA_SUPP_FUNCS ${DYN_DISP_SUPP_FUNCS})

set(ISA_SRC_FILES "")

foreach(func ${ISA_SUPP_FUNCS})
list(APPEND ISA_SRC_FILES
    ${ALMEM_ISA}/${func}_${ALMEM_ISA}.c
    )
endforeach()

add_library(isa_objlib OBJECT ${ISA_SRC_FILES})
target_include_directories(isa_objlib PUBLIC ${LIBMEM_ROOT}/include)

set(_MODULE_CC_FLAGS ${_CC_Flags} -mbmi -mbmi2 -mlzcnt -mno-vzeroupper)
if (${ALMEM_ISA} MATCHES "avx512")
    list(APPEND _MODULE_CC_FLAGS -mavx512f -mavx512bw -mavx512vbmi -mavx512vl)
else ()
    list(APPEND _MODULE_CC_FLAGS -mavx2)
endif ()

set_target_properties(isa_objlib PROPERTIES POSITION_INDEPENDENT_CODE "ON" COMPILE_OPTIONS "${_MODULE_CC_FLAGS}")
