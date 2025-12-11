#pragma once

#include <cloud/storage/core/libs/common/alloc.h>

#include <util/generic/vector.h>
#include <util/memory/alloc.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

enum class EAllocatorTag
{
    BlobIndexBlobMap,
    BlobIndexBlockList,
    BlobIndexDirtyBlobs,
    BlobIndexGarbageMap,
    BlobIndexRangeMap,
    DisjointRangeMap,
    MixedIndexBlockMap,
    MixedIndexOverwrittenBlobIds,
    FreshBlockMap,

    Max
};

TProfilingAllocator* GetAllocatorByTag(EAllocatorTag tag);

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename... TArgs>
T* NewImpl(IAllocator* allocator, TArgs&&... args)
{
    auto block = allocator->Allocate(sizeof(T));
    Y_ABORT_UNLESS(block.Data);
    return ::new ((void*)block.Data) T(std::forward<TArgs>(args)...);
}

template <typename T>
void DeleteImpl(IAllocator* allocator, T* p)
{
    if (p) {
        p->~T();
        allocator->Release({p, sizeof(T)});
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
