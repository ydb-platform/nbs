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

    static constexpr TCounterPtr AllCounters[] = {
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
    sizeof(TSimpleDiskCounters) ==
    sizeof(TSimpleDiskCounters::TCounter) *
        std::size(TSimpleDiskCounters::AllCounters));

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

    static constexpr TCounterPtr AllCounters[] = {
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
        std::size(TCumulativeDiskCounters::AllCounters));

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
        EPublishingPolicy::All,
        ERequestCounterOption::HasVoidBytes};
    THighResCounter WriteBlocks{
        "WriteBlocks",
        EPublishingPolicy::All,
        ERequestCounterOption{}};
    THighResCounter ZeroBlocks{
        "ZeroBlocks",
        EPublishingPolicy::All,
        ERequestCounterOption{}};
    THighResCounter DescribeBlocks{
        "DescribeBlocks",
        EPublishingPolicy::All,
        ERequestCounterOption{}};
    THighResCounter ChecksumBlocks{
        "ChecksumBlocks",
        EPublishingPolicy::All,
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

struct THistogramCounters
{
    using TCounter = TMemberWithMeta<THistogram<TQueueSizeBuckets>>;
    using TCounterPtr = TCounter THistogramCounters::*;

    // BlobStorage based
    TCounter ActorQueue{
        "ActorQueue",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};
    TCounter MailboxQueue{
        "MailboxQueue",
        EPublishingPolicy::Repl,
        ERequestCounterOption{}};

    static constexpr TCounterPtr AllCounters[] = {
        &THistogramCounters::ActorQueue,
        &THistogramCounters::MailboxQueue,
    };
};

static_assert(
    sizeof(THistogramCounters) ==
    sizeof(THistogramCounters::TCounter) *
        std::size(THistogramCounters::AllCounters));

////////////////////////////////////////////////////////////////////////////////

struct TVolumeSelfSimpleCounters
{
    using TCounter = TMemberWithMeta<TSimpleCounter>;
    using TCounterPtr = TCounter TVolumeSelfSimpleCounters::*;

    // Common
    TCounter MaxReadBandwidth{
        "MaxReadBandwidth",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxWriteBandwidth{
        "MaxWriteBandwidth",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxReadIops{
        "MaxReadIops",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxWriteIops{
        "MaxWriteIops",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxUsedQuota{
        "MaxUsedQuota",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter LastVolumeLoadTime{
        "LastVolumeLoadTime",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter LastVolumeStartTime{
        "LastVolumeStartTime",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter HasStorageConfigPatch{
        "HasStorageConfigPatch",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter LongRunningReadBlob{
        "LongRunningReadBlob",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter LongRunningWriteBlob{
        "LongRunningWriteBlob",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter UseFastPath{
        "UseFastPath",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // BlobStorage-based
    TCounter RealMaxWriteBandwidth{
        "RealMaxWriteBandwidth",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter PostponedQueueWeight{
        "PostponedQueueWeight",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPFreshIndexScore{
        "BPFreshIndexScore",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPCompactionScore{
        "BPCompactionScore",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPDiskSpaceScore{
        "BPDiskSpaceScore",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPCleanupScore{
        "BPCleanupScore",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter VBytesCount{
        "VBytesCount",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter PartitionCount{
        "PartitionCount",
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // DiskRegistry-based
    TCounter MigrationStarted{
        "MigrationStarted",
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MigrationProgress{
        "MigrationProgress",
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ResyncStarted{
        "ResyncStarted",
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ResyncProgress{
        "ResyncProgress",
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TCounterPtr AllCounters[] = {
        &TVolumeSelfSimpleCounters::MaxReadBandwidth,
        &TVolumeSelfSimpleCounters::MaxWriteBandwidth,
        &TVolumeSelfSimpleCounters::MaxReadIops,
        &TVolumeSelfSimpleCounters::MaxWriteIops,
        &TVolumeSelfSimpleCounters::MaxUsedQuota,
        &TVolumeSelfSimpleCounters::LastVolumeLoadTime,
        &TVolumeSelfSimpleCounters::LastVolumeStartTime,
        &TVolumeSelfSimpleCounters::HasStorageConfigPatch,
        &TVolumeSelfSimpleCounters::LongRunningReadBlob,
        &TVolumeSelfSimpleCounters::LongRunningWriteBlob,
        &TVolumeSelfSimpleCounters::UseFastPath,

        &TVolumeSelfSimpleCounters::RealMaxWriteBandwidth,
        &TVolumeSelfSimpleCounters::PostponedQueueWeight,
        &TVolumeSelfSimpleCounters::BPFreshIndexScore,
        &TVolumeSelfSimpleCounters::BPCompactionScore,
        &TVolumeSelfSimpleCounters::BPDiskSpaceScore,
        &TVolumeSelfSimpleCounters::BPCleanupScore,
        &TVolumeSelfSimpleCounters::VBytesCount,
        &TVolumeSelfSimpleCounters::PartitionCount,

        &TVolumeSelfSimpleCounters::MigrationStarted,
        &TVolumeSelfSimpleCounters::MigrationProgress,
        &TVolumeSelfSimpleCounters::ResyncStarted,
        &TVolumeSelfSimpleCounters::ResyncProgress,
    };
};
static_assert(
    sizeof(TVolumeSelfSimpleCounters) ==
    sizeof(TVolumeSelfSimpleCounters::TCounter) *
        std::size(TVolumeSelfSimpleCounters::AllCounters));

struct TVolumeSelfCumulativeCounters
{
    using TCounter = TMemberWithMeta<TCumulativeCounter>;
    using TCounterPtr = TCounter TVolumeSelfCumulativeCounters::*;

    // Common
    TCounter ThrottlerRejectedRequests{
        "ThrottlerRejectedRequests",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter ThrottlerPostponedRequests{
        "ThrottlerPostponedRequests",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter ThrottlerSkippedRequests{
        "ThrottlerSkippedRequests",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter UsedQuota{
        "UsedQuota",
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TCounterPtr AllCounters[] = {
        &TVolumeSelfCumulativeCounters::ThrottlerRejectedRequests,
        &TVolumeSelfCumulativeCounters::ThrottlerPostponedRequests,
        &TVolumeSelfCumulativeCounters::ThrottlerSkippedRequests,
        &TVolumeSelfCumulativeCounters::UsedQuota,
    };
};
static_assert(
    sizeof(TVolumeSelfCumulativeCounters) ==
    sizeof(TVolumeSelfCumulativeCounters::TCounter) *
        std::size(TVolumeSelfCumulativeCounters::AllCounters));

struct TVolumeSelfRequestCounters
{
    using TCounter = TMemberWithMeta<THistogram<TRequestUsTimeBuckets>>;
    using TCounterPtr = TCounter TVolumeSelfRequestCounters::*;

    // Common
    TCounter ReadBlocks{
        "ReadBlocks",
        EPublishingPolicy::All,
        ERequestCounterOption{}};
    TCounter WriteBlocks{
        "WriteBlocks",
        EPublishingPolicy::All,
        ERequestCounterOption{}};
    TCounter ZeroBlocks{
        "ZeroBlocks",
        EPublishingPolicy::All,
        ERequestCounterOption{}};
    TCounter DescribeBlocks{
        "DescribeBlocks",
        EPublishingPolicy::All,
        ERequestCounterOption{}};

    static constexpr TCounterPtr AllCounters[] = {
        &TVolumeSelfRequestCounters::ReadBlocks,
        &TVolumeSelfRequestCounters::WriteBlocks,
        &TVolumeSelfRequestCounters::ZeroBlocks,
        &TVolumeSelfRequestCounters::DescribeBlocks,
    };
};
static_assert(
    sizeof(TVolumeSelfRequestCounters) ==
    sizeof(TVolumeSelfRequestCounters::TCounter) *
        std::size(TVolumeSelfRequestCounters::AllCounters));

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDiskCounters
{
    TSimpleDiskCounters Simple;
    TCumulativeDiskCounters Cumulative;
    THistogramRequestCounters RequestCounters;
    THistogramCounters Histogram;

    EPublishingPolicy Policy;

    explicit TPartitionDiskCounters(EPublishingPolicy policy)
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
    TVolumeSelfSimpleCounters Simple;
    TVolumeSelfCumulativeCounters Cumulative;
    TVolumeSelfRequestCounters RequestCounters;

    EPublishingPolicy Policy;

    explicit TVolumeSelfCounters(EPublishingPolicy policy)
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
