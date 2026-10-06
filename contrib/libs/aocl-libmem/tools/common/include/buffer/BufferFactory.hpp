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

#ifndef LIBMEM_COMMON_BUFFER_FACTORY_HPP
#define LIBMEM_COMMON_BUFFER_FACTORY_HPP

/**
 * @file BufferFactory.hpp
 * @brief Factory functions for allocating test buffers.
 *
 * Three entry points, one per buffer relationship:
 *   allocateSingle  -- one buffer (src == dst)
 *   allocateDual    -- two separate non-overlapping buffers
 *   allocateOverlap -- one allocation with overlapping src/dst
 *
 * Each function handles page mode (None / PageCross / PageGuarded)
 * internally.  The caller only specifies sizes, offsets, and an
 * AllocParams configuration.
 */

#include "alloc/AllocParams.hpp"
#include "buffer/BufferPair.hpp"
#include "config/Constants.hpp"
#include <sys/mman.h>

namespace libmem {
namespace common {

namespace detail {

inline size_t base_alignment(const AllocParams& p) {
    return (p.base_align == BaseAlign::Page) ? PAGE_SZ : CACHE_LINE_SZ;
}

} // namespace detail

// ============================================================================
// allocateSingle -- one buffer, src == dst
// ============================================================================

inline BufferPair allocateSingle(size_t size, const AllocParams& params = {}) {
    BufferPair buf;
    buf.size = size;

    if (params.page_mode == PageMode::PageGuarded) {
        size_t pages = (size + PAGE_SZ - 1) / PAGE_SZ;
        size_t total_pages = pages + 2;
        size_t alloc_size = total_pages * PAGE_SZ;

        buf.src_block = mem_alloc(BackendType::PosixMemalign, PAGE_SZ, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        size_t usable = (total_pages - 1) * PAGE_SZ;

        uint8_t* guard = base + usable;
        if (mprotect(guard, PAGE_SZ, PROT_NONE) != 0) {
            mem_free(buf.src_block);
            buf.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
            return buf;
        }
        buf.guard_page = guard;
        buf.guard_page_size = PAGE_SZ;

        buf.src = base + usable - size;
        buf.dst = buf.src;
        return buf;
    }

    if (params.page_mode == PageMode::PageTail) {
        size_t alloc_size = PAGE_SZ + size;
        buf.src_block = mem_alloc(params.backend, PAGE_SZ, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        size_t page_rem = size % PAGE_SZ;
        uint32_t offset = page_rem ? static_cast<uint32_t>(PAGE_SZ - page_rem) : 0;
        buf.src = base + offset;
        buf.dst = buf.src;
        return buf;
    }

    if (params.page_mode == PageMode::PageCross) {
        size_t alloc_size = PAGE_SZ + size;
        buf.src_block = mem_alloc(params.backend, PAGE_SZ, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        buf.src = base + PAGE_SZ - params.cross_offset;
        buf.dst = buf.src;
        return buf;
    }

    // PageMode::None
    size_t align = detail::base_alignment(params);
    size_t alloc_size = 2 * CACHE_LINE_SZ + params.src_offset + size;
    buf.src_block = mem_alloc(params.backend, align, alloc_size);
    if (!buf.src_block.ptr) return buf;

    uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
    buf.src = base + CACHE_LINE_SZ + params.src_offset;
    buf.dst = buf.src;
    return buf;
}

// ============================================================================
// allocateDual -- two separate non-overlapping buffers
// ============================================================================

inline BufferPair allocateDual(size_t src_size, size_t dst_size,
                                const AllocParams& params = {}) {
    BufferPair buf;
    buf.size = src_size;

    if (params.page_mode == PageMode::PageGuarded) {
        size_t total_data = src_size + dst_size + 2 * CACHE_LINE_SZ;
        size_t pages = (total_data + PAGE_SZ - 1) / PAGE_SZ;
        size_t total_pages = pages + 2;
        size_t alloc_size = total_pages * PAGE_SZ;

        buf.src_block = mem_alloc(BackendType::PosixMemalign, PAGE_SZ, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        size_t usable = (total_pages - 1) * PAGE_SZ;

        uint8_t* guard = base + usable;
        if (mprotect(guard, PAGE_SZ, PROT_NONE) != 0) {
            mem_free(buf.src_block);
            buf.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
            return buf;
        }
        buf.guard_page = guard;
        buf.guard_page_size = PAGE_SZ;

        buf.dst = base + usable - dst_size;
        buf.src = buf.dst - CACHE_LINE_SZ - src_size;
        return buf;
    }

    if (params.page_mode == PageMode::PageTail) {
        size_t src_alloc = PAGE_SZ + src_size;
        size_t dst_alloc = PAGE_SZ + dst_size;

        buf.src_block = mem_alloc(params.backend, PAGE_SZ, src_alloc);
        if (!buf.src_block.ptr) return buf;

        buf.dst_block = mem_alloc(params.backend, PAGE_SZ, dst_alloc);
        if (!buf.dst_block.ptr) {
            mem_free(buf.src_block);
            buf.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
            return buf;
        }

        size_t src_rem = src_size % PAGE_SZ;
        size_t dst_rem = dst_size % PAGE_SZ;
        uint32_t src_off = src_rem ? static_cast<uint32_t>(PAGE_SZ - src_rem) : 0;
        uint32_t dst_off = dst_rem ? static_cast<uint32_t>(PAGE_SZ - dst_rem) : 0;
        buf.src = static_cast<uint8_t*>(buf.src_block.ptr) + src_off;
        buf.dst = static_cast<uint8_t*>(buf.dst_block.ptr) + dst_off;
        return buf;
    }

    if (params.page_mode == PageMode::PageCross) {
        size_t src_alloc = PAGE_SZ + src_size;
        size_t dst_alloc = PAGE_SZ + dst_size;

        buf.src_block = mem_alloc(params.backend, PAGE_SZ, src_alloc);
        if (!buf.src_block.ptr) return buf;

        buf.dst_block = mem_alloc(params.backend, PAGE_SZ, dst_alloc);
        if (!buf.dst_block.ptr) {
            mem_free(buf.src_block);
            buf.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
            return buf;
        }

        uint8_t* src_base = static_cast<uint8_t*>(buf.src_block.ptr);
        uint8_t* dst_base = static_cast<uint8_t*>(buf.dst_block.ptr);
        buf.src = src_base + PAGE_SZ - params.cross_offset;
        buf.dst = dst_base + PAGE_SZ - params.cross_offset;
        return buf;
    }

    // PageMode::None
    size_t align = detail::base_alignment(params);

    // Each buffer gets a full CACHE_LINE_SZ of head padding and CACHE_LINE_SZ
    // of tail padding around [buf, buf+size). The head padding keeps the
    // BOUNDARY_BYTES guard writes (and any implementation over-read before
    // the buffer) inside the malloc chunk even when the alignment offset is
    // zero; the tail padding does the same on the far end regardless of how
    // large the offset within the vector is.
    if (params.contiguous) {
        size_t src_region = CACHE_LINE_SZ + params.src_offset +
                            ((src_size + align - 1) / align) * align +
                            CACHE_LINE_SZ;
        size_t alloc_size = src_region + params.dst_offset +
                            dst_size + CACHE_LINE_SZ;

        buf.src_block = mem_alloc(params.backend, align, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        buf.src = base + CACHE_LINE_SZ + params.src_offset;
        buf.dst = base + src_region + params.dst_offset;
        return buf;
    }

    // Independent -- two separate allocations (default)
    size_t src_alloc = 2 * CACHE_LINE_SZ + params.src_offset + src_size;
    size_t dst_alloc = 2 * CACHE_LINE_SZ + params.dst_offset + dst_size;

    buf.src_block = mem_alloc(params.backend, align, src_alloc);
    if (!buf.src_block.ptr) return buf;

    buf.dst_block = mem_alloc(params.backend, align, dst_alloc);
    if (!buf.dst_block.ptr) {
        mem_free(buf.src_block);
        buf.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
        return buf;
    }

    buf.src = static_cast<uint8_t*>(buf.src_block.ptr) + CACHE_LINE_SZ + params.src_offset;
    buf.dst = static_cast<uint8_t*>(buf.dst_block.ptr) + CACHE_LINE_SZ + params.dst_offset;
    return buf;
}

// ============================================================================
// allocateOverlap -- one allocation, src and dst overlap with controlled gap
// ============================================================================

inline BufferPair allocateOverlap(size_t size, size_t overlap_offset,
                                   const AllocParams& params = {}) {
    BufferPair buf;
    buf.size = size;

    size_t total = size + overlap_offset + CACHE_LINE_SZ;

    if (params.page_mode == PageMode::PageGuarded) {
        size_t pages = (total + PAGE_SZ - 1) / PAGE_SZ;
        size_t total_pages = pages + 2;
        size_t alloc_size = total_pages * PAGE_SZ;

        buf.src_block = mem_alloc(BackendType::PosixMemalign, PAGE_SZ, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        size_t usable = (total_pages - 1) * PAGE_SZ;

        uint8_t* guard = base + usable;
        if (mprotect(guard, PAGE_SZ, PROT_NONE) != 0) {
            mem_free(buf.src_block);
            buf.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
            return buf;
        }
        buf.guard_page = guard;
        buf.guard_page_size = PAGE_SZ;

        buf.dst = base + usable - size;
        buf.src = buf.dst - overlap_offset;
        return buf;
    }

    if (params.page_mode == PageMode::PageTail) {
        size_t alloc_size = PAGE_SZ + total;
        buf.src_block = mem_alloc(params.backend, PAGE_SZ, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        size_t page_rem = total % PAGE_SZ;
        uint32_t offset = page_rem ? static_cast<uint32_t>(PAGE_SZ - page_rem) : 0;
        buf.src = base + offset;
        buf.dst = buf.src + overlap_offset;
        return buf;
    }

    if (params.page_mode == PageMode::PageCross) {
        size_t alloc_size = PAGE_SZ + total;
        buf.src_block = mem_alloc(params.backend, PAGE_SZ, alloc_size);
        if (!buf.src_block.ptr) return buf;

        uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
        buf.src = base + PAGE_SZ - params.cross_offset;
        buf.dst = buf.src + overlap_offset;
        return buf;
    }

    // PageMode::None
    size_t align = detail::base_alignment(params);
    size_t alloc_size = CACHE_LINE_SZ + total;
    buf.src_block = mem_alloc(params.backend, align, alloc_size);
    if (!buf.src_block.ptr) return buf;

    uint8_t* base = static_cast<uint8_t*>(buf.src_block.ptr);
    buf.src = base + CACHE_LINE_SZ;
    buf.dst = buf.src + overlap_offset;
    return buf;
}

} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_BUFFER_FACTORY_HPP
