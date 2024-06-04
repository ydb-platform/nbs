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
    DiskRegistryBased,
};

template <typename TBase>
struct TMemberWithMeta: public TBase
{
    const char* Name = {};
    EPublishingPolicy PublishingPolicy = {};
    ERequestCounterOption CounterOption = {};

    TMemberWithMeta() = default;

    template <typename... TArgs>
    TMemberWithMeta(
            const char* name,
            EPublishingPolicy publishingPolicy,
            TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , Name(name)
        , PublishingPolicy(publishingPolicy)
    {}

    TMemberWithMeta(
            const char* name,
            EPublishingPolicy publishingPolicy,
            ERequestCounterOption counterOption = ERequestCounterOption())
        : TBase()
        , Name(name)
        , PublishingPolicy(publishingPolicy)
        , CounterOption(counterOption)
    {}

    TMemberWithMeta(const TMemberWithMeta& rh) = default;
    TMemberWithMeta(TMemberWithMeta&& rh) = default;

    TMemberWithMeta& operator=(const TMemberWithMeta& rh) = default;
    TMemberWithMeta& operator=(TMemberWithMeta&& rh) = default;
};

struct TSimpleDiskCounters
{
    using TCounter = TMemberWithMeta<TSimpleCounter>;
    using TCounterPtr = TCounter TSimpleDiskCounters::*;

    // Common
    TCounter BytesCount{
        "BytesCount",
        EPublishingPolicy::All,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter IORequestsInFlight{
        "IORequestsInFlight",
        EPublishingPolicy::All,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // BlobStorage based
    TCounter MixedBytesCount{
        "MixedBytesCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MergedBytesCount{
        "MergedBytesCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter FreshBytesCount{
        "FreshBytesCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter UntrimmedFreshBlobBytesCount{
        "UntrimmedFreshBlobBytesCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter UsedBytesCount{
        "UsedBytesCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter LogicalUsedBytesCount{
        "LogicalUsedBytesCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter IORequestsQueued{
        "IORequestsQueued",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter UsedBlocksMapMemSize{
        "UsedBlocksMapMemSize",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter MixedIndexCacheMemSize{
        "MixedIndexCacheMemSize",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter CheckpointBytes{
        "CheckpointBytes",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter AlmostFullChannelCount{
        "AlmostFullChannelCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter FreshBlocksInFlight{
        "FreshBlocksInFlight",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter FreshBlocksQueued{
        "FreshBlocksQueued",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter CleanupQueueBytes{
        "CleanupQueueBytes",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter GarbageQueueBytes{
        "GarbageQueueBytes",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionScore{
        "CompactionScore",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionGarbageScore{
        "CompactionGarbageScore",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter ChannelHistorySize{
        "ChannelHistorySize",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionRangeCountPerRun{
        "CompactionRangeCountPerRun",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter UnconfirmedBlobCount{
        "UnconfirmedBlobCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ConfirmedBlobCount{
        "ConfirmedBlobCount",
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // DiskRegistry based
    TCounter HasBrokenDevice{
        "HasBrokenDevice",
        EPublishingPolicy::DiskRegistryBased,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter HasBrokenDeviceSilent{
        "HasBrokenDeviceSilent",
        EPublishingPolicy::DiskRegistryBased,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TCounterPtr All[] = {
        &TSimpleDiskCounters::BytesCount,
        &TSimpleDiskCounters::IORequestsInFlight,

        &TSimpleDiskCounters::MixedBytesCount,
        &TSimpleDiskCounters::MergedBytesCount,
        &TSimpleDiskCounters::FreshBytesCount,
        &TSimpleDiskCounters::UntrimmedFreshBlobBytesCount,
        &TSimpleDiskCounters::UsedBytesCount,
        &TSimpleDiskCounters::LogicalUsedBytesCount,
        &TSimpleDiskCounters::IORequestsQueued,
        &TSimpleDiskCounters::UsedBlocksMapMemSize,
        &TSimpleDiskCounters::MixedIndexCacheMemSize,
        &TSimpleDiskCounters::CheckpointBytes,
        &TSimpleDiskCounters::AlmostFullChannelCount,
        &TSimpleDiskCounters::FreshBlocksInFlight,
        &TSimpleDiskCounters::FreshBlocksQueued,
        &TSimpleDiskCounters::CleanupQueueBytes,
        &TSimpleDiskCounters::GarbageQueueBytes,
        &TSimpleDiskCounters::CompactionScore,
        &TSimpleDiskCounters::CompactionGarbageScore,
        &TSimpleDiskCounters::ChannelHistorySize,
        &TSimpleDiskCounters::CompactionRangeCountPerRun,
        &TSimpleDiskCounters::UnconfirmedBlobCount,
        &TSimpleDiskCounters::ConfirmedBlobCount,

        &TSimpleDiskCounters::HasBrokenDevice,
        &TSimpleDiskCounters::HasBrokenDeviceSilent,
    };
    static constexpr size_t MemberCount = std::size(All);
};
static_assert(
    sizeof(TSimpleDiskCounters) ==
    sizeof(TSimpleDiskCounters::TCounter) * TSimpleDiskCounters::MemberCount);

#define BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS(xxx, ...)                  \
    xxx(ScrubbingThroughput, Generic, Permanent,                   __VA_ARGS__)\
// BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(xxx, ...)                     \
    xxx(BytesWritten,               Generic, Permanent,            __VA_ARGS__)\
    xxx(BytesRead,                  Generic, Permanent,            __VA_ARGS__)\
    xxx(SysBytesWritten,            Generic, Permanent,            __VA_ARGS__)\
    xxx(SysBytesRead,               Generic, Permanent,            __VA_ARGS__)\
    xxx(RealSysBytesWritten,        Generic, Permanent,            __VA_ARGS__)\
    xxx(RealSysBytesRead,           Generic, Permanent,            __VA_ARGS__)\
    xxx(BatchCount,                 Generic, Permanent,            __VA_ARGS__)\
    xxx(UncompressedBytesWritten,   Generic, Permanent,            __VA_ARGS__)\
    xxx(CompressedBytesWritten,     Generic, Permanent,            __VA_ARGS__)\
    xxx(CompactionByReadStats,      Generic, Permanent,            __VA_ARGS__)\
    xxx(CompactionByBlobCountPerRange,     Generic, Permanent,     __VA_ARGS__)\
    xxx(CompactionByBlobCountPerDisk,      Generic, Permanent,     __VA_ARGS__)\
    xxx(CompactionByGarbageBlocksPerRange, Generic, Permanent,     __VA_ARGS__)\
    xxx(CompactionByGarbageBlocksPerDisk,  Generic, Permanent,     __VA_ARGS__)\
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
    xxx(ConfirmBlobs,                                              __VA_ARGS__)\
// BLOCKSTORE_REPL_PART_REQUEST_COUNTERS

#define BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(xxx, ...)                   \
    xxx(ReadBlocks,     true,                                      __VA_ARGS__)\
    xxx(WriteBlocks,    false,                                     __VA_ARGS__)\
    xxx(ZeroBlocks,     false,                                     __VA_ARGS__)\
    xxx(DescribeBlocks, false,                                     __VA_ARGS__)\
    xxx(ChecksumBlocks, false,                                     __VA_ARGS__)\
    // BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE

#define BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(xxx, ...)     \
    xxx(WriteBlob,                                                 __VA_ARGS__)\
    xxx(ReadBlob,                                                  __VA_ARGS__)\
    xxx(PatchBlob,                                                 __VA_ARGS__)\
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
    xxx(LastVolumeLoadTime,         Max,     Permanent,            __VA_ARGS__)\
    xxx(LastVolumeStartTime,        Max,     Permanent,            __VA_ARGS__)\
    xxx(HasStorageConfigPatch,      Generic, Permanent,            __VA_ARGS__)\
    xxx(LongRunningReadBlob,        Generic, Expiring,             __VA_ARGS__)\
    xxx(LongRunningWriteBlob,       Generic, Expiring,             __VA_ARGS__)\
    xxx(UseFastPath,                Generic, Permanent,            __VA_ARGS__)\


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
    xxx(BPCleanupScore,             Generic, Expiring,             __VA_ARGS__)\
    xxx(VBytesCount,                Generic, Permanent,            __VA_ARGS__)\
    xxx(PartitionCount,             Generic, Permanent,            __VA_ARGS__)\
// BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS(xxx, ...)               \
    xxx(MigrationStarted,           Generic, Permanent,            __VA_ARGS__)\
    xxx(MigrationProgress,          Generic, Permanent,            __VA_ARGS__)\
    xxx(ResyncStarted,              Generic, Permanent,            __VA_ARGS__)\
    xxx(ResyncProgress,             Generic, Permanent,            __VA_ARGS__)\
// BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDiskCounters
{
    TSimpleDiskCounters Simple;

    struct {
#define BLOCKSTORE_CUMULATIVE_COUNTER(name, type, policy, ...)                 \
    TCumulativeCounter name{                                                   \
        TCumulativeCounter::ECounterType::type,                                \
        ECounterExpirationPolicy::policy                                       \
    };                                                                         \
// BLOCKSTORE_CUMULATIVE_COUNTER

        BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
        BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER
    } Cumulative;

    struct {
#define BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER(name, ...)                  \
        TRequestCounters<THistogram<TRequestUsTimeBucketsLowResolution>> name;\
// BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER

        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_LOW_RESOLUTION_COUNTER)
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
        BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
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
