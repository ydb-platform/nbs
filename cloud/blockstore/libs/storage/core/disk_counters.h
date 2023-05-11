#pragma once

#include "histogram.h"
#include "storage_request_counters.h"

#include <cloud/storage/core/libs/diagnostics/solomon_counters.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EPublishingPolicy
{
    All,
    Repl,
    NonRepl
};

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS(xxx, ...)                       \
    xxx(BytesCount,             Generic, Permanent,                __VA_ARGS__)\
    xxx(IORequestsInFlight,     Generic, Permanent,                __VA_ARGS__)\
// BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS(xxx, ...)                         \
    xxx(MixedBytesCount,        Generic, Permanent,                __VA_ARGS__)\
    xxx(MergedBytesCount,       Generic, Permanent,                __VA_ARGS__)\
    xxx(FreshBytesCount,        Generic, Permanent,                __VA_ARGS__)\
    xxx(UntrimmedFreshBlobBytesCount,    Generic, Permanent        __VA_ARGS__)\
    xxx(UsedBytesCount,         Generic, Permanent,                __VA_ARGS__)\
    xxx(LogicalUsedBytesCount,  Generic, Permanent,                __VA_ARGS__)\
    xxx(IORequestsQueued,       Generic, Expiring,                 __VA_ARGS__)\
    xxx(UsedBlocksMapMemSize,   Generic, Expiring,                 __VA_ARGS__)\
    xxx(MixedIndexCacheMemSize, Generic, Expiring,                 __VA_ARGS__)\
    xxx(CheckpointBytes,        Generic, Permanent,                __VA_ARGS__)\
    xxx(AlmostFullChannelCount, Generic, Expiring,                 __VA_ARGS__)\
    xxx(FreshBlocksInFlight,    Generic, Expiring,                 __VA_ARGS__)\
    xxx(FreshBlocksQueued,      Generic, Expiring,                 __VA_ARGS__)\
    xxx(CleanupQueueBytes,      Generic, Permanent,                __VA_ARGS__)\
    xxx(GarbageQueueBytes,      Generic, Permanent,                __VA_ARGS__)\
    xxx(CompactionScore,        Max,     Permanent,                __VA_ARGS__)\
    xxx(CompactionGarbageScore, Max,     Permanent,                __VA_ARGS__)\
    xxx(ChannelHistorySize,     Max,     Permanent,                __VA_ARGS__)\
// BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_NONREPL_PART_SIMPLE_COUNTERS(xxx, ...)                      \
    xxx(HasBrokenDevice,        Generic, Permanent,                __VA_ARGS__)\
    xxx(HasBrokenDeviceSilent,  Generic, Permanent,                __VA_ARGS__)\
// BLOCKSTORE_NONREPL_PART_SIMPLE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(xxx, ...)                     \
    xxx(BytesWritten,               Generic, Permanent,            __VA_ARGS__)\
    xxx(BytesRead,                  Generic, Permanent,            __VA_ARGS__)\
    xxx(SysBytesWritten,            Generic, Permanent,            __VA_ARGS__)\
    xxx(SysBytesRead,               Generic, Permanent,            __VA_ARGS__)\
    xxx(BatchCount,                 Generic, Permanent,            __VA_ARGS__)\
    xxx(UncompressedBytesWritten,   Generic, Permanent,            __VA_ARGS__)\
    xxx(CompressedBytesWritten,     Generic, Permanent,            __VA_ARGS__)\
// BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS

#define BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(xxx, ...)                        \
    xxx(Flush,                                                     __VA_ARGS__)\
    xxx(AddBlobs,                                                  __VA_ARGS__)\
    xxx(Compaction,                                                __VA_ARGS__)\
    xxx(Cleanup,                                                   __VA_ARGS__)\
    xxx(CollectGarbage,                                            __VA_ARGS__)\
    xxx(DeleteGarbage,                                             __VA_ARGS__)\
    xxx(TrimFreshLog,                                              __VA_ARGS__)\
    xxx(AddConfirmedBlobs,                                         __VA_ARGS__)\
    xxx(AddUnconfirmedBlobs,                                       __VA_ARGS__)\
// BLOCKSTORE_REPL_PART_REQUEST_COUNTERS

#define BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_ONLY_COUNT(xxx, ...)             \
    xxx(MetadataRebuild,                                           __VA_ARGS__)\
    xxx(MetadataRebuildBatch,                                      __VA_ARGS__)\
    xxx(CreateCheckpoint,                                          __VA_ARGS__)\
    xxx(DeleteCheckpoint,                                          __VA_ARGS__)\
    xxx(GetChangedBlocks,                                          __VA_ARGS__)\
// BLOCKSTORE_REPL_PART_REQUEST_COUNTERS

#define BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(xxx, ...)                   \
    xxx(ReadBlocks,                                                __VA_ARGS__)\
    xxx(WriteBlocks,                                               __VA_ARGS__)\
    xxx(ZeroBlocks,                                                __VA_ARGS__)\
    xxx(DescribeBlocks,                                            __VA_ARGS__)\
    xxx(ChecksumBlocks,                                            __VA_ARGS__)\
// BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE

#define BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(xxx, ...)     \
    xxx(WriteBlob,                                                 __VA_ARGS__)\
    xxx(ReadBlob,                                                  __VA_ARGS__)\
// BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE


#define BLOCKSTORE_REPL_PART_ACTOR_COUNTERS(xxx, ...)                          \
    xxx(ActorQueue,                                                __VA_ARGS__)\
    xxx(MailboxQueue,                                              __VA_ARGS__)\
// BLOCKSTORE_REPL_PARTITION_ACTOR_COUNTERS

#define BLOCKSTORE_REPL_PART_HIST_COUNTERS(xxx, ...)                           \
    BLOCKSTORE_REPL_PART_ACTOR_COUNTERS(xxx,                       __VA_ARGS__)\
// BLOCKSTORE_REPL_PART)PERCENTILE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_SELF_COMMON_SIMPLE_COUNTERS(xxx, ...)                \
    xxx(MaxReadBandwidth,           Generic, Permanent,            __VA_ARGS__)\
    xxx(MaxWriteBandwidth,          Generic, Permanent,            __VA_ARGS__)\
    xxx(MaxReadIops,                Generic, Permanent,            __VA_ARGS__)\
    xxx(MaxWriteIops,               Generic, Permanent,            __VA_ARGS__)\
    xxx(MaxUsedQuota,               Generic, Permanent,            __VA_ARGS__)\
    xxx(LastVolumeLoadTime,         Generic, Permanent,            __VA_ARGS__)\
    xxx(LastVolumeStartTime,        Generic, Permanent,            __VA_ARGS__)\

#define BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS(xxx, ...)            \
    xxx(ThrottlerRejectedRequests,  Generic, Expiring,             __VA_ARGS__)\
    xxx(ThrottlerPostponedRequests, Generic, Expiring,             __VA_ARGS__)\
    xxx(ThrottlerSkippedRequests,   Generic, Expiring,             __VA_ARGS__)\
    xxx(UsedQuota,                  Generic, Permanent,            __VA_ARGS__)\
// BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS

#define BLOCKSTORE_VOLUME_SELF_COMMON_REQUEST_COUNTERS(xxx, ...)               \
    xxx(ReadBlocks,                                                __VA_ARGS__)\
    xxx(WriteBlocks,                                               __VA_ARGS__)\
    xxx(ZeroBlocks,                                                __VA_ARGS__)\
    xxx(DescribeBlocks,                                            __VA_ARGS__)\
// BLOCKSTORE_VOLUME_SELF_REQUEST_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS(xxx, ...)                  \
    xxx(RealMaxWriteBandwidth,      Generic, Permanent,            __VA_ARGS__)\
    xxx(PostponedQueueWeight,       Generic, Expiring,             __VA_ARGS__)\
    xxx(BPFreshIndexScore,          Generic, Expiring,             __VA_ARGS__)\
    xxx(BPCompactionScore,          Generic, Expiring,             __VA_ARGS__)\
    xxx(BPDiskSpaceScore,           Generic, Expiring,             __VA_ARGS__)\
    xxx(VBytesCount,                Generic, Permanent,            __VA_ARGS__)\
    xxx(PartitionCount,             Generic, Permanent,            __VA_ARGS__)\
// BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_NONREPL_VOLUME_SELF_SIMPLE_COUNTERS(xxx, ...)               \
    xxx(MigrationStarted,           Generic, Permanent,            __VA_ARGS__)\
    xxx(MigrationProgress,          Generic, Permanent,            __VA_ARGS__)\
    xxx(ResyncStarted,              Generic, Permanent,            __VA_ARGS__)\
    xxx(ResyncProgress,             Generic, Permanent,            __VA_ARGS__)\
// BLOCKSTORE_NONREPL_VOLUME_SELF_SIMPLE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDiskCounters
{
    struct {
#define BLOCKSTORE_SIMPLE_COUNTER(name, type, policy, ...)                     \
    TSimpleCounter name{                                                       \
        TSimpleCounter::ECounterType::type,                                    \
        ECounterExpirationPolicy::policy                                       \
    };                                                                         \
// BLOCKSTORE_SIMPLE_COUNTER

        BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
        BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
        BLOCKSTORE_NONREPL_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER
    } Simple;

    struct {
#define BLOCKSTORE_CUMULATIVE_COUNTER(name, type, policy, ...)                 \
    TCumulativeCounter name{                                                   \
        TCumulativeCounter::ECounterType::type,                                \
        ECounterExpirationPolicy::policy                                       \
    };                                                                         \
// BLOCKSTORE_CUMULATIVE_COUNTER

        BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER
    } Cumulative;

    struct {
#define BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER(name, ...)                  \
        TRequestCounters<THistogram<TRequestUsTimeBucketsLowResolution>> name;\
// BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER

        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER)
        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_ONLY_COUNT(BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER)
        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER)
#undef BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                 \
        TRequestCounters<THistogram<TRequestUsTimeBuckets>> name;             \
// BLOCKSTORE_REQUEST_COUNTER

        BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER
    } RequestCounters;

    struct {
#define BLOCKSTORE_HIST_COUNTER(name, ...)                                     \
        THistogram<TQueueSizeBuckets> name;                                    \
// BLOCKSTORE_HIST_COUNTER

        BLOCKSTORE_REPL_PART_HIST_COUNTERS(BLOCKSTORE_HIST_COUNTER)
#undef BLOCKSTORE_HIST_COUNTER
    } Histogram;

    EPublishingPolicy Policy;

    TPartitionDiskCounters(EPublishingPolicy policy)
        : Policy(policy)
    {}

    void Add(const TPartitionDiskCounters& source);
    void AggregateWith(const TPartitionDiskCounters& source);
    void Register(NMonitoring::TDynamicCountersPtr counters, bool aggregate);
    void Publish(TInstant now);
    void Reset();
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeSelfCounters
{
    struct
    {
#define BLOCKSTORE_SIMPLE_COUNTER(name, type, policy, ...)                    \
    TSimpleCounter name{                                                      \
        TSimpleCounter::ECounterType::type,                                   \
        ECounterExpirationPolicy::policy                                      \
    };                                                                        \
// BLOCKSTORE_SIMPLE_COUNTER

        BLOCKSTORE_VOLUME_SELF_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
        BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
        BLOCKSTORE_NONREPL_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER
    } Simple;

    struct
    {
#define BLOCKSTORE_CUMULATIVE_COUNTER(name, type, policy, ...)                \
    TCumulativeCounter name{                                                  \
        TCumulativeCounter::ECounterType::type,                               \
        ECounterExpirationPolicy::policy                                      \
    };                                                                        \
// BLOCKSTORE_CUMULATIVE_COUNTER

        BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER
    } Cumulative;

    struct
    {
#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                 \
        THistogram<TRequestUsTimeBuckets> name;                                 \
// BLOCKSTORE_REQUEST_COUNTER

        BLOCKSTORE_VOLUME_SELF_COMMON_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER
    } RequestCounters;

    EPublishingPolicy Policy;

    TVolumeSelfCounters(EPublishingPolicy policy)
        : Policy(policy)
    {}

    void Add(const TVolumeSelfCounters& source);
    void AggregateWith(const TVolumeSelfCounters& source);
    void Register(NMonitoring::TDynamicCountersPtr counters, bool aggregate);
    void Publish(TInstant now);
    void Reset();
};

////////////////////////////////////////////////////////////////////////////////

using TPartitionDiskCountersPtr = std::unique_ptr<TPartitionDiskCounters>;
using TVolumeSelfCountersPtr = std::unique_ptr<TVolumeSelfCounters>;

////////////////////////////////////////////////////////////////////////////////

TPartitionDiskCountersPtr CreatePartitionDiskCounters(EPublishingPolicy policy);
TVolumeSelfCountersPtr CreateVolumeSelfCounters(EPublishingPolicy policy);

}   // namespace NCloud::NBlockStore::NStorage
