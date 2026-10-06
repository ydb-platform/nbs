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

#ifndef LIBMEM_COMMON_ALLOC_MEMBLOCK_HPP
#define LIBMEM_COMMON_ALLOC_MEMBLOCK_HPP

/**
 * @file MemBlock.hpp
 * @brief Raw allocation backend abstraction.
 *
 * Every buffer allocation in the tools framework ultimately calls
 * mem_alloc / mem_free.  Adding a new backend requires:
 *   1. Add an enum value to BackendType
 *   2. Add a case in mem_alloc
 *   3. Add a case in mem_free
 * No other files need to change.
 */

#include <cstdlib>
#include <cstddef>
#include <cstdint>
#include <new>

namespace libmem {
namespace common {

enum class BackendType : uint8_t {
    PosixMemalign,  // posix_memalign / free
    OperatorNew,    // ::operator new(sz, align_val_t) / ::operator delete(ptr, align_val_t)
    // Future: MmapAnon, JemallocDirect, TcmallocDirect, MallocAlign
};

struct MemBlock {
    void*       ptr;
    size_t      size;
    size_t      alignment;
    BackendType backend;
};

inline MemBlock mem_alloc(BackendType type, size_t alignment, size_t size) {
    MemBlock blk{nullptr, size, alignment, type};

    switch (type) {
    case BackendType::PosixMemalign: {
        void* p = nullptr;
        if (posix_memalign(&p, alignment, size) != 0)
            p = nullptr;
        blk.ptr = p;
        break;
    }
    case BackendType::OperatorNew: {
        try {
            blk.ptr = ::operator new(size, std::align_val_t{alignment});
        } catch (const std::bad_alloc&) {
            blk.ptr = nullptr;
        }
        break;
    }
    }

    return blk;
}

inline void mem_free(const MemBlock& blk) {
    if (!blk.ptr) return;

    switch (blk.backend) {
    case BackendType::PosixMemalign:
        std::free(blk.ptr);
        break;
    case BackendType::OperatorNew:
        ::operator delete(blk.ptr, std::align_val_t{blk.alignment});
        break;
    }
}

inline const char* backend_name(BackendType type) {
    switch (type) {
    case BackendType::PosixMemalign: return "posix_memalign";
    case BackendType::OperatorNew:   return "operator_new";
    }
    return "unknown";
}

} // namespace common
} // namespace libmem

#endif // LIBMEM_COMMON_ALLOC_MEMBLOCK_HPP
