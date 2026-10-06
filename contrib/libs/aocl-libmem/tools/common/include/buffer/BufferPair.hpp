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

#ifndef LIBMEM_COMMON_BUFFER_PAIR_HPP
#define LIBMEM_COMMON_BUFFER_PAIR_HPP

/**
 * @file BufferPair.hpp
 * @brief RAII wrapper for allocated src/dst buffer pairs.
 *
 * Stores MemBlock tokens from mem_alloc and passes them back unchanged
 * to mem_free.  This guarantees correct deallocation metadata (alignment,
 * size, backend) for every backend type.
 *
 * Non-copyable, movable.
 */

#include "alloc/MemBlock.hpp"
#include <cstdint>
#include <cstddef>
#include <sys/mman.h>

namespace libmem {
namespace common {

struct BufferPair {
    uint8_t*  src;
    uint8_t*  dst;
    size_t    size;

    MemBlock  src_block;
    MemBlock  dst_block;

    uint8_t*  guard_page;
    size_t    guard_page_size;

    bool valid() const { return src_block.ptr != nullptr; }

    uint8_t* srcAllocBase() const { return static_cast<uint8_t*>(src_block.ptr); }
    size_t   srcAllocSize() const { return src_block.size; }
    uint8_t* dstAllocBase() const { return static_cast<uint8_t*>(dst_block.ptr); }
    size_t   dstAllocSize() const { return dst_block.size; }

    BufferPair()
        : src(nullptr), dst(nullptr), size(0)
        , src_block{nullptr, 0, 0, BackendType::PosixMemalign}
        , dst_block{nullptr, 0, 0, BackendType::PosixMemalign}
        , guard_page(nullptr), guard_page_size(0) {}

    ~BufferPair() {
        if (dst_block.ptr)
            mem_free(dst_block);
        if (src_block.ptr) {
            if (guard_page)
                mprotect(guard_page, guard_page_size, PROT_READ | PROT_WRITE);
            mem_free(src_block);
        }
    }

    BufferPair(const BufferPair&) = delete;
    BufferPair& operator=(const BufferPair&) = delete;

    BufferPair(BufferPair&& o) noexcept
        : src(o.src), dst(o.dst), size(o.size)
        , src_block(o.src_block), dst_block(o.dst_block)
        , guard_page(o.guard_page), guard_page_size(o.guard_page_size)
    {
        o.src = nullptr;
        o.dst = nullptr;
        o.size = 0;
        o.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
        o.dst_block = {nullptr, 0, 0, BackendType::PosixMemalign};
        o.guard_page = nullptr;
        o.guard_page_size = 0;
    }

    BufferPair& operator=(BufferPair&& o) noexcept {
        if (this != &o) {
            if (dst_block.ptr) mem_free(dst_block);
            if (src_block.ptr) {
                if (guard_page)
                    mprotect(guard_page, guard_page_size, PROT_READ | PROT_WRITE);
                mem_free(src_block);
            }

            src             = o.src;
            dst             = o.dst;
            size            = o.size;
            src_block       = o.src_block;
            dst_block       = o.dst_block;
            guard_page      = o.guard_page;
            guard_page_size = o.guard_page_size;

            o.src = nullptr;
            o.dst = nullptr;
            o.size = 0;
            o.src_block = {nullptr, 0, 0, BackendType::PosixMemalign};
            o.dst_block = {nullptr, 0, 0, BackendType::PosixMemalign};
            o.guard_page = nullptr;
            o.guard_page_size = 0;
        }
        return *this;
    }
};

} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_BUFFER_PAIR_HPP
