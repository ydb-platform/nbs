#pragma once

#include "public.h"

#include "tablet_schema.h"
#include "tablet_state.h"
#include "tablet_state_cache.h"

#include <cloud/filestore/libs/storage/model/range.h>
#include <cloud/filestore/libs/storage/tablet/model/block_list.h>
#include <cloud/filestore/libs/storage/tablet/model/channels.h>
#include <cloud/filestore/libs/storage/tablet/model/compaction_map.h>
#include <cloud/filestore/libs/storage/tablet/model/fresh_blocks.h>
#include <cloud/filestore/libs/storage/tablet/model/fresh_bytes.h>
#include <cloud/filestore/libs/storage/tablet/model/garbage_queue.h>
#include <cloud/filestore/libs/storage/tablet/model/large_blocks.h>
#include <cloud/filestore/libs/storage/tablet/model/mixed_blocks.h>
#include <cloud/filestore/libs/storage/tablet/model/node_index_cache.h>
#include <cloud/filestore/libs/storage/tablet/model/node_ref.h>
#include <cloud/filestore/libs/storage/tablet/model/range_locks.h>
#include <cloud/filestore/libs/storage/tablet/model/read_ahead.h>
#include <cloud/filestore/libs/storage/tablet/model/shard_balancer.h>
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
    ui64 MaxSessionHistoryEntryId = 1;

    TNodeToSessionStat NodeToSessionStat;

    TWriteRequestList WriteBatch;

    TRangeLocks RangeLocks;
    TFreshBytes FreshBytes;
    TFreshBlocks FreshBlocks;
    TMixedBlocks MixedBlocks;
    TLargeBlocks LargeBlocks;
    TCompactionMap CompactionMap;
    TGarbageQueue GarbageQueue;
    TTruncateQueue TruncateQueue;
    TReadAheadCache ReadAheadCache;
    TNodeIndexCache NodeIndexCache;
    TInMemoryIndexState InMemoryIndexState;
    TSet<ui64> OrphanNodeIds;
    TSet<TString> PendingNodeCreateInShardNames;
    THashSet<TNodeRefKey, TNodeRefKeyHash> LockedNodeRefs;

    TCheckpointStore Checkpoints;
    TChannels Channels;

    IBlockLocation2RangeIndexPtr RangeIdHasher;

    TThrottlingPolicy ThrottlingPolicy;

    IShardBalancerPtr ShardBalancer;

    TImpl(const TFileStoreAllocRegistry& registry)
        : FreshBytes(registry.GetAllocator(EAllocatorTag::FreshBytes))
        , FreshBlocks(registry.GetAllocator(EAllocatorTag::FreshBlocks))
        , MixedBlocks(registry.GetAllocator(EAllocatorTag::BlobMetaMap))
        , LargeBlocks(registry.GetAllocator(EAllocatorTag::LargeBlocks))
        , CompactionMap(registry.GetAllocator(EAllocatorTag::CompactionMap))
        , GarbageQueue(registry.GetAllocator(EAllocatorTag::GarbageQueue))
        , ReadAheadCache(registry.GetAllocator(EAllocatorTag::ReadAheadCache))
        , NodeIndexCache(registry.GetAllocator(EAllocatorTag::NodeIndexCache))
        , InMemoryIndexState(registry.GetAllocator(EAllocatorTag::InMemoryNodeIndexCache))
        , ThrottlingPolicy(TThrottlerConfig())
    {}
};

}   // namespace NCloud::NFileStore::NStorage
