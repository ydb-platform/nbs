#pragma once

#include <cloud/storage/core/libs/common/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EAllocatorTag
{
    BlobMetaMap,
    BlockList,
    CompactionMap,
    DeletionMarkers,
    FreshBytes,
    GarbageQueue,
    ReadAheadCache,
    InMemoryNodeIndexCache,
    LargeBlocks,

    Max
};

////////////////////////////////////////////////////////////////////////////////

using TFileStoreAllocRegistry = TProfilingAllocatorRegistry<EAllocatorTag>;

}   // namespace NCloud::NFileStore::NStorage
