#pragma once

#include "public.h"

#include "tablet_schema.h"
#include "tablet_state.h"

#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/block_list.h>
#include <cloud/filestore/libs/storage/tablet/model/channels.h>
#include <cloud/filestore/libs/storage/tablet/model/compaction_map.h>
#include <cloud/filestore/libs/storage/tablet/model/fresh_blocks.h>
#include <cloud/filestore/libs/storage/tablet/model/fresh_bytes.h>
#include <cloud/filestore/libs/storage/tablet/model/garbage_queue.h>
#include <cloud/filestore/libs/storage/tablet/model/mixed_blocks.h>
#include <cloud/filestore/libs/storage/tablet/model/range_locks.h>
#include <cloud/filestore/libs/storage/tablet/model/read_ahead.h>
#include <cloud/filestore/libs/storage/tablet/model/throttler_logger.h>
#include <cloud/filestore/libs/storage/tablet/model/throttling_policy.h>
#include <cloud/filestore/libs/storage/tablet/model/truncate_queue.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TIndexTabletState::TImpl
{
    TSessionList Sessions;
    TSessionList OrphanSessions;
    TSessionMap SessionById;
    TSessionOwnerMap SessionByOwner;
    TSessionClientMap SessionByClient;
    TSessionHistoryList SessionHistoryList;

    TNodeRefsByHandle NodeRefsByHandle;

    // TODO: move to TSession
    TSessionHandleMap HandleById;
    TSessionLockMap LockById;
    TSessionLockMultiMap LocksByHandle;

    TWriteRequestList WriteBatch;

    TRangeLocks RangeLocks;
    TFreshBytes FreshBytes;
    TFreshBlocks FreshBlocks;
    TMixedBlocks MixedBlocks;
    TCompactionMap CompactionMap;
    TGarbageQueue GarbageQueue;
    TTruncateQueue TruncateQueue;
    TReadAheadCache ReadAheadCache;

    TCheckpointStore Checkpoints;
    TChannels Channels;

    IBlockLocation2RangeIndexPtr RangeIdHasher;

    TThrottlingPolicy ThrottlingPolicy;

    TImpl(const TFileStoreAllocRegistry& registry)
        : FreshBytes(registry.GetAllocator(EAllocatorTag::FreshBytes))
        , FreshBlocks(registry.GetAllocator(EAllocatorTag::FreshBlocks))
        , MixedBlocks(registry.GetAllocator(EAllocatorTag::BlobMetaMap))
        , CompactionMap(registry.GetAllocator(EAllocatorTag::CompactionMap))
        , GarbageQueue(registry.GetAllocator(EAllocatorTag::GarbageQueue))
        , ReadAheadCache(registry.GetAllocator(EAllocatorTag::ReadAheadCache))
        , ThrottlingPolicy(TThrottlerConfig())
    {}
};

}   // namespace NCloud::NFileStore::NStorage
