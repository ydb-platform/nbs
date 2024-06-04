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
            ERequestCounterOption counterOption)
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
};
static_assert(
    sizeof(TSimpleDiskCounters) == sizeof(TSimpleDiskCounters::TCounter) *
                                       std::size(TSimpleDiskCounters::All));

struct TCumulativeDiskCounters
{
    using TCounter = TMemberWithMeta<TCumulativeCounter>;
    using TCounterPtr = TCounter TCumulativeDiskCounters::*;

    // BlobStorage based
    TCounter BytesWritten{
        "BytesWritten",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter BytesRead{
        "BytesRead",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter SysBytesWritten{
        "SysBytesWritten",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter SysBytesRead{
        "SysBytesRead",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter RealSysBytesWritten{
        "RealSysBytesWritten",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter RealSysBytesRead{
        "RealSysBytesRead",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter BatchCount{
        "BatchCount",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter UncompressedBytesWritten{
        "UncompressedBytesWritten",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompressedBytesWritten{
        "CompressedBytesWritten",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByReadStats{
        "CompactionByReadStats",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByBlobCountPerRange{
        "CompactionByBlobCountPerRange",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByBlobCountPerDisk{
        "CompactionByBlobCountPerDisk",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByGarbageBlocksPerRange{
        "CompactionByGarbageBlocksPerRange",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByGarbageBlocksPerDisk{
        "CompactionByGarbageBlocksPerDisk",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // DiskRegistry based
    TCounter ScrubbingThroughput{
        "ScrubbingThroughput",
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TCounterPtr All[] = {
        &TCumulativeDiskCounters::BytesWritten,
        &TCumulativeDiskCounters::BytesRead,
        &TCumulativeDiskCounters::SysBytesWritten,
        &TCumulativeDiskCounters::SysBytesRead,
        &TCumulativeDiskCounters::RealSysBytesWritten,
        &TCumulativeDiskCounters::RealSysBytesRead,
        &TCumulativeDiskCounters::BatchCount,
        &TCumulativeDiskCounters::UncompressedBytesWritten,
        &TCumulativeDiskCounters::CompressedBytesWritten,
        &TCumulativeDiskCounters::CompactionByReadStats,
        &TCumulativeDiskCounters::CompactionByBlobCountPerRange,
        &TCumulativeDiskCounters::CompactionByBlobCountPerDisk,
        &TCumulativeDiskCounters::CompactionByGarbageBlocksPerRange,
        &TCumulativeDiskCounters::CompactionByGarbageBlocksPerDisk,

        &TCumulativeDiskCounters::ScrubbingThroughput,
    };
};
static_assert(
    sizeof(TCumulativeDiskCounters) ==
    sizeof(TCumulativeDiskCounters::TCounter) *
        std::size(TCumulativeDiskCounters::All));

struct THistogramRequestCounters
{
    using TLowResCounter = TMemberWithMeta<
        TRequestCounters<THistogram<TRequestUsTimeBucketsLowResolution>>>;
    using TLowResCounterPtr = TLowResCounter THistogramRequestCounters::*;

    using THighResCounter =
        TMemberWithMeta<TRequestCounters<THistogram<TRequestUsTimeBuckets>>>;
    using THighResCounterPtr = THighResCounter THistogramRequestCounters::*;

    // BlobStorage based
    TLowResCounter Flush{
        "Flush",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter AddBlobs{
        "AddBlobs",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter Compaction{
        "Compaction",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter Cleanup{
        "Cleanup",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter CollectGarbage{
        "CollectGarbage",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter DeleteGarbage{
        "DeleteGarbage",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter TrimFreshLog{
        "TrimFreshLog",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter AddConfirmedBlobs{
        "AddConfirmedBlobs",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter AddUnconfirmedBlobs{
        "AddUnconfirmedBlobs",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TLowResCounter ConfirmBlobs{
        "ConfirmBlobs",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};

    // BlobStorage based with kind and size
    TLowResCounter WriteBlob{
        "WriteBlob",
        EPublishingPolicy::Repl,
        ERequestCounterOption::HasKind};
    TLowResCounter ReadBlob{
        "ReadBlob",
        EPublishingPolicy::Repl,
        ERequestCounterOption::HasKind};
    TLowResCounter PatchBlob{
        "PatchBlob",
        EPublishingPolicy::Repl,
        ERequestCounterOption::HasKind};

    static constexpr TLowResCounterPtr AllLowResCounters[] = {
        &THistogramRequestCounters::Flush,
        &THistogramRequestCounters::AddBlobs,
        &THistogramRequestCounters::Compaction,
        &THistogramRequestCounters::Cleanup,
        &THistogramRequestCounters::CollectGarbage,
        &THistogramRequestCounters::DeleteGarbage,
        &THistogramRequestCounters::TrimFreshLog,
        &THistogramRequestCounters::AddConfirmedBlobs,
        &THistogramRequestCounters::AddUnconfirmedBlobs,
        &THistogramRequestCounters::ConfirmBlobs,

        &THistogramRequestCounters::WriteBlob,
        &THistogramRequestCounters::ReadBlob,
        &THistogramRequestCounters::PatchBlob,
    };

    THighResCounter ReadBlocks{
        "ReadBlocks",
        EPublishingPolicy::Repl,
        ERequestCounterOption::HasVoidBytes};
    THighResCounter WriteBlocks{
        "WriteBlocks",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    THighResCounter ZeroBlocks{
        "ZeroBlocks",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    THighResCounter DescribeBlocks{
        "DescribeBlocks",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    THighResCounter ChecksumBlocks{
        "ChecksumBlocks",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};

    static constexpr THighResCounterPtr AllHighResCounters[] = {
        &THistogramRequestCounters::ReadBlocks,
        &THistogramRequestCounters::WriteBlocks,
        &THistogramRequestCounters::ZeroBlocks,
        &THistogramRequestCounters::DescribeBlocks,
        &THistogramRequestCounters::ChecksumBlocks,
    };
};

static_assert(
    sizeof(THistogramRequestCounters) ==
    (sizeof(THistogramRequestCounters::TLowResCounter) *
         std::size(THistogramRequestCounters::AllLowResCounters) +
     sizeof(THistogramRequestCounters::THighResCounter) *
         std::size(THistogramRequestCounters::AllHighResCounters)));



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
    TCumulativeDiskCounters Cumulative;
    THistogramRequestCounters RequestCounters;

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
