#pragma once

#include "public.h"

#include <util/memory/alloc.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ICachingAllocator
{
    struct TNode;

    struct TBlock
    {
        void* Data;
        size_t Len;
        TNode* Node;
    };

    virtual ~ICachingAllocator() = default;

    virtual TBlock Allocate(size_t len) = 0;
    virtual void Release(TBlock block) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICachingAllocatorPtr CreateCachingAllocator(
    IAllocator* upstream,
    size_t pageSize,
    size_t maxPageCount,
    size_t pageDropSize);

ICachingAllocatorPtr CreateSyncCachingAllocator(
    IAllocator* upstream,
    size_t pageSize,
    size_t maxPageCount,
    size_t pageDropSize);

}   // namespace NCloud::NBlockStore
