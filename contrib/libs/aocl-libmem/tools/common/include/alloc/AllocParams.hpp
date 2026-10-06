/* Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef LIBMEM_COMMON_ALLOC_PARAMS_HPP
#define LIBMEM_COMMON_ALLOC_PARAMS_HPP

/**
 * @file AllocParams.hpp
 * @brief Allocation request parameters.
 *
 * AllocParams expresses what the caller wants from the allocation
 * infrastructure.  All fields have safe defaults that produce
 * cache-line-aligned, PosixMemalign buffers with no page positioning.
 *
 * Rules:
 *   - page_mode and alignment offsets are mutually exclusive.
 *     When page_mode != None, src_offset/dst_offset/base_align are ignored.
 *   - PageGuarded forces PosixMemalign backend regardless of the
 *     backend field (mprotect requires compatible page-aligned memory).
 *   - cross_offset is only used when page_mode == PageCross.
 */

#include "alloc/MemBlock.hpp"
#include <cstdint>

namespace libmem {
namespace common {

enum class PageMode : uint8_t {
    None,           // standard allocation, no page positioning
    PageCross,      // data straddles a page boundary
    PageTail,       // data positioned at end of page (throughput measurement)
    PageGuarded,    // data at tail of page + PROT_NONE guard (overrun detection)
};

enum class BaseAlign : uint8_t {
    CacheLine,      // 64-byte aligned base
    Page,           // 4096-byte aligned base
};

struct AllocParams {
    BackendType  backend       = BackendType::PosixMemalign;
    PageMode     page_mode     = PageMode::None;
    BaseAlign    base_align    = BaseAlign::CacheLine;
    uint32_t     src_offset    = 0;
    uint32_t     dst_offset    = 0;
    uint32_t     cross_offset  = 0;
    bool         contiguous    = false;  // dual only: single block instead of two allocs
};

} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_ALLOC_PARAMS_HPP
