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
    EPublishingPolicy PublishingPolicy = {};
    ERequestCounterOption CounterOption = {};

    TMemberWithMeta() = default;

    template <typename... TArgs>
    explicit TMemberWithMeta(
            EPublishingPolicy publishingPolicy,
            TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , PublishingPolicy(publishingPolicy)
    {}

    TMemberWithMeta(
            EPublishingPolicy publishingPolicy,
            ERequestCounterOption counterOption)
        : TBase()
        , PublishingPolicy(publishingPolicy)
        , CounterOption(counterOption)
    {}

    TMemberWithMeta(const TMemberWithMeta& rh) = default;
    TMemberWithMeta(TMemberWithMeta&& rh) = default;

    TMemberWithMeta& operator=(const TMemberWithMeta& rh) = default;
    TMemberWithMeta& operator=(TMemberWithMeta&& rh) = default;
};

template <typename TMemberPtr>
struct TMemberMeta
{
    TStringBuf Name;
    TMemberPtr MemberPtr{};

    auto& GetValue(auto& object)
    {
        return object.*MemberPtr;
    }
};

template <typename TMemberPtr>
struct TMemberMetaWithTag: public TMemberMeta<TMemberPtr>
{
    TStringBuf Tag;
};

namespace NDetail {

template <auto TMemberPtr>
consteval auto ExtractMemberPtr()
{
    return TMemberPtr;
}

}   // namespace NDetail

template <auto TMemberPtr>
consteval auto MakeMeta()
{
    TMemberMeta<decltype(NDetail::ExtractMemberPtr<TMemberPtr>())> result;

    // Extract member name from __PRETTY_FUNCTION__.
    // __PRETTY_FUNCTION__ looks like
    // xxx MakeMeta() [T = &NStorage::TSimpleDiskCounters::BytesCount]
    std::string_view left = "::";
    std::string_view right = "]";
    std::string_view raw = __PRETTY_FUNCTION__;
    auto start = raw.rfind(left);
    auto end = raw.rfind(right);
    result.Name = raw.substr(start + left.size(), end - start - left.size());

    // Store member ptr.
    result.MemberPtr = NDetail::ExtractMemberPtr<TMemberPtr>();
    return result;
}

template <auto TMemberPtr>
consteval auto MakeMetaWithTag(TStringBuf name, TStringBuf tag)
{
    TMemberMetaWithTag<decltype(NDetail::ExtractMemberPtr<TMemberPtr>())>
        result;
    result.Name = name;
    result.Tag = tag;

    result.MemberPtr = NDetail::ExtractMemberPtr<TMemberPtr>();
    return result;
}

struct TSimpleDiskCounters
{
    using TCounter = TMemberWithMeta<TSimpleCounter>;
    using TMeta = TMemberMeta<TCounter TSimpleDiskCounters::*>;

    // Common
    TCounter BytesCount{
        EPublishingPolicy::All,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter IORequestsInFlight{
        EPublishingPolicy::All,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // BlobStorage based
    TCounter MixedBytesCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MergedBytesCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter FreshBytesCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter UntrimmedFreshBlobBytesCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter UsedBytesCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter LogicalUsedBytesCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter IORequestsQueued{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter UsedBlocksMapMemSize{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter MixedIndexCacheMemSize{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter CheckpointBytes{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter AlmostFullChannelCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter FreshBlocksInFlight{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter FreshBlocksQueued{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter CleanupQueueBytes{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter GarbageQueueBytes{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionScore{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionGarbageScore{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter ChannelHistorySize{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionRangeCountPerRun{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter UnconfirmedBlobCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ConfirmedBlobCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ReadBlobDeadlineCount{
        EPublishingPolicy::Repl,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};

    // DiskRegistry based
    TCounter HasBrokenDevice{
        EPublishingPolicy::DiskRegistryBased,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter HasBrokenDeviceSilent{
        EPublishingPolicy::DiskRegistryBased,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ScrubbingProgress{
        EPublishingPolicy::DiskRegistryBased,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ChecksumMismatches{
        EPublishingPolicy::DiskRegistryBased,
        TSimpleCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TMeta AllCounters[] = {
        MakeMeta<&TSimpleDiskCounters::BytesCount>(),
        MakeMeta<&TSimpleDiskCounters::IORequestsInFlight>(),

        MakeMeta<&TSimpleDiskCounters::MixedBytesCount>(),
        MakeMeta<&TSimpleDiskCounters::MergedBytesCount>(),
        MakeMeta<&TSimpleDiskCounters::FreshBytesCount>(),
        MakeMeta<&TSimpleDiskCounters::UntrimmedFreshBlobBytesCount>(),
        MakeMeta<&TSimpleDiskCounters::UsedBytesCount>(),
        MakeMeta<&TSimpleDiskCounters::LogicalUsedBytesCount>(),
        MakeMeta<&TSimpleDiskCounters::IORequestsQueued>(),
        MakeMeta<&TSimpleDiskCounters::UsedBlocksMapMemSize>(),
        MakeMeta<&TSimpleDiskCounters::MixedIndexCacheMemSize>(),
        MakeMeta<&TSimpleDiskCounters::CheckpointBytes>(),
        MakeMeta<&TSimpleDiskCounters::AlmostFullChannelCount>(),
        MakeMeta<&TSimpleDiskCounters::FreshBlocksInFlight>(),
        MakeMeta<&TSimpleDiskCounters::FreshBlocksQueued>(),
        MakeMeta<&TSimpleDiskCounters::CleanupQueueBytes>(),
        MakeMeta<&TSimpleDiskCounters::GarbageQueueBytes>(),
        MakeMeta<&TSimpleDiskCounters::CompactionScore>(),
        MakeMeta<&TSimpleDiskCounters::CompactionGarbageScore>(),
        MakeMeta<&TSimpleDiskCounters::ChannelHistorySize>(),
        MakeMeta<&TSimpleDiskCounters::CompactionRangeCountPerRun>(),
        MakeMeta<&TSimpleDiskCounters::UnconfirmedBlobCount>(),
        MakeMeta<&TSimpleDiskCounters::ConfirmedBlobCount>(),
        MakeMeta<&TSimpleDiskCounters::ReadBlobDeadlineCount>(),

        MakeMeta<&TSimpleDiskCounters::HasBrokenDevice>(),
        MakeMeta<&TSimpleDiskCounters::HasBrokenDeviceSilent>(),
        MakeMeta<&TSimpleDiskCounters::ScrubbingProgress>(),
        MakeMeta<&TSimpleDiskCounters::ChecksumMismatches>(),
    };
};
static_assert(
    sizeof(TSimpleDiskCounters) ==
    sizeof(TSimpleDiskCounters::TCounter) *
        std::size(TSimpleDiskCounters::AllCounters));

struct TCumulativeDiskCounters
{
    using TCounter = TMemberWithMeta<TCumulativeCounter>;
    using TMeta = TMemberMeta<TCounter TCumulativeDiskCounters::*>;

    // BlobStorage based
    TCounter BytesWritten{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter BytesRead{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter SysBytesWritten{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter SysBytesRead{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter RealSysBytesWritten{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter RealSysBytesRead{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter BatchCount{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter UncompressedBytesWritten{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompressedBytesWritten{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByReadStats{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByBlobCountPerRange{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByBlobCountPerDisk{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByGarbageBlocksPerRange{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter CompactionByGarbageBlocksPerDisk{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // DiskRegistry based
    TCounter ScrubbingThroughput{
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TMeta AllCounters[] = {
        MakeMeta<&TCumulativeDiskCounters::BytesWritten>(),
        MakeMeta<&TCumulativeDiskCounters::BytesRead>(),
        MakeMeta<&TCumulativeDiskCounters::SysBytesWritten>(),
        MakeMeta<&TCumulativeDiskCounters::SysBytesRead>(),
        MakeMeta<&TCumulativeDiskCounters::RealSysBytesWritten>(),
        MakeMeta<&TCumulativeDiskCounters::RealSysBytesRead>(),
        MakeMeta<&TCumulativeDiskCounters::BatchCount>(),
        MakeMeta<&TCumulativeDiskCounters::UncompressedBytesWritten>(),
        MakeMeta<&TCumulativeDiskCounters::CompressedBytesWritten>(),
        MakeMeta<&TCumulativeDiskCounters::CompactionByReadStats>(),
        MakeMeta<&TCumulativeDiskCounters::CompactionByBlobCountPerRange>(),
        MakeMeta<&TCumulativeDiskCounters::CompactionByBlobCountPerDisk>(),
        MakeMeta<&TCumulativeDiskCounters::CompactionByGarbageBlocksPerRange>(),
        MakeMeta<&TCumulativeDiskCounters::CompactionByGarbageBlocksPerDisk>(),

        MakeMeta<&TCumulativeDiskCounters::ScrubbingThroughput>(),
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
    using TLowResMeta =
        TMemberMeta<TLowResCounter THistogramRequestCounters::*>;

    using THighResCounter =
        TMemberWithMeta<TRequestCounters<THistogram<TRequestUsTimeBuckets>>>;
    using THighResMeta =
        TMemberMeta<THighResCounter THistogramRequestCounters::*>;

    // BlobStorage based
    TLowResCounter Flush{EPublishingPolicy::Repl};
    TLowResCounter AddBlobs{EPublishingPolicy::Repl};
    TLowResCounter Compaction{EPublishingPolicy::Repl};
    TLowResCounter Cleanup{EPublishingPolicy::Repl};
    TLowResCounter CollectGarbage{EPublishingPolicy::Repl};
    TLowResCounter DeleteGarbage{EPublishingPolicy::Repl};
    TLowResCounter TrimFreshLog{EPublishingPolicy::Repl};
    TLowResCounter AddConfirmedBlobs{EPublishingPolicy::Repl};
    TLowResCounter AddUnconfirmedBlobs{EPublishingPolicy::Repl};
    TLowResCounter ConfirmBlobs{EPublishingPolicy::Repl};

    // BlobStorage based with kind and size
    TLowResCounter WriteBlob{
        EPublishingPolicy::Repl,
        ERequestCounterOption::HasKind};
    TLowResCounter ReadBlob{
        EPublishingPolicy::Repl,
        ERequestCounterOption::HasKind};
    TLowResCounter PatchBlob{
        EPublishingPolicy::Repl,
        ERequestCounterOption::HasKind};

    static constexpr TLowResMeta AllLowResCounters[] = {
        MakeMeta<&THistogramRequestCounters::Flush>(),
        MakeMeta<&THistogramRequestCounters::AddBlobs>(),
        MakeMeta<&THistogramRequestCounters::Compaction>(),
        MakeMeta<&THistogramRequestCounters::Cleanup>(),
        MakeMeta<&THistogramRequestCounters::CollectGarbage>(),
        MakeMeta<&THistogramRequestCounters::DeleteGarbage>(),
        MakeMeta<&THistogramRequestCounters::TrimFreshLog>(),
        MakeMeta<&THistogramRequestCounters::AddConfirmedBlobs>(),
        MakeMeta<&THistogramRequestCounters::AddUnconfirmedBlobs>(),
        MakeMeta<&THistogramRequestCounters::ConfirmBlobs>(),

        MakeMeta<&THistogramRequestCounters::WriteBlob>(),
        MakeMeta<&THistogramRequestCounters::ReadBlob>(),
        MakeMeta<&THistogramRequestCounters::PatchBlob>(),
    };

    THighResCounter ReadBlocks{
        EPublishingPolicy::All,
        ERequestCounterOption::HasVoidBytes};
    THighResCounter WriteBlocks{EPublishingPolicy::All};
    THighResCounter ZeroBlocks{EPublishingPolicy::All};
    THighResCounter DescribeBlocks{EPublishingPolicy::All};
    THighResCounter ChecksumBlocks{EPublishingPolicy::All};
    THighResCounter CopyBlocks{EPublishingPolicy::DiskRegistryBased};

    static constexpr THighResMeta AllHighResCounters[] = {
        MakeMeta<&THistogramRequestCounters::ReadBlocks>(),
        MakeMeta<&THistogramRequestCounters::WriteBlocks>(),
        MakeMeta<&THistogramRequestCounters::ZeroBlocks>(),
        MakeMeta<&THistogramRequestCounters::DescribeBlocks>(),
        MakeMeta<&THistogramRequestCounters::ChecksumBlocks>(),
        MakeMeta<&THistogramRequestCounters::CopyBlocks>(),
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
    using TMeta = TMemberMeta<TCounter THistogramCounters::*>;

    // BlobStorage based
    TCounter ActorQueue{EPublishingPolicy::Repl};
    TCounter MailboxQueue{EPublishingPolicy::Repl};

    static constexpr TMeta AllCounters[] = {
        MakeMeta<&THistogramCounters::ActorQueue>(),
        MakeMeta<&THistogramCounters::MailboxQueue>(),
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
    using TMeta = TMemberMeta<TCounter TVolumeSelfSimpleCounters::*>;

    // Common
    TCounter MaxReadBandwidth{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxWriteBandwidth{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxReadIops{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxWriteIops{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MaxUsedQuota{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter LastVolumeLoadTime{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter LastVolumeStartTime{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Max,
        ECounterExpirationPolicy::Permanent};
    TCounter HasStorageConfigPatch{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter LongRunningReadBlob{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter LongRunningWriteBlob{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter UseFastPath{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // BlobStorage-based
    TCounter RealMaxWriteBandwidth{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter PostponedQueueWeight{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPFreshIndexScore{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPCompactionScore{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPDiskSpaceScore{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter BPCleanupScore{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter VBytesCount{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter PartitionCount{
        EPublishingPolicy::Repl,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    // DiskRegistry-based
    TCounter MigrationStarted{
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter MigrationProgress{
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ResyncStarted{
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};
    TCounter ResyncProgress{
        EPublishingPolicy::DiskRegistryBased,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TMeta AllCounters[] = {
        MakeMeta<&TVolumeSelfSimpleCounters::MaxReadBandwidth>(),
        MakeMeta<&TVolumeSelfSimpleCounters::MaxWriteBandwidth>(),
        MakeMeta<&TVolumeSelfSimpleCounters::MaxReadIops>(),
        MakeMeta<&TVolumeSelfSimpleCounters::MaxWriteIops>(),
        MakeMeta<&TVolumeSelfSimpleCounters::MaxUsedQuota>(),
        MakeMeta<&TVolumeSelfSimpleCounters::LastVolumeLoadTime>(),
        MakeMeta<&TVolumeSelfSimpleCounters::LastVolumeStartTime>(),
        MakeMeta<&TVolumeSelfSimpleCounters::HasStorageConfigPatch>(),
        MakeMeta<&TVolumeSelfSimpleCounters::LongRunningReadBlob>(),
        MakeMeta<&TVolumeSelfSimpleCounters::LongRunningWriteBlob>(),
        MakeMeta<&TVolumeSelfSimpleCounters::UseFastPath>(),

        MakeMeta<&TVolumeSelfSimpleCounters::RealMaxWriteBandwidth>(),
        MakeMeta<&TVolumeSelfSimpleCounters::PostponedQueueWeight>(),
        MakeMeta<&TVolumeSelfSimpleCounters::BPFreshIndexScore>(),
        MakeMeta<&TVolumeSelfSimpleCounters::BPCompactionScore>(),
        MakeMeta<&TVolumeSelfSimpleCounters::BPDiskSpaceScore>(),
        MakeMeta<&TVolumeSelfSimpleCounters::BPCleanupScore>(),
        MakeMeta<&TVolumeSelfSimpleCounters::VBytesCount>(),
        MakeMeta<&TVolumeSelfSimpleCounters::PartitionCount>(),

        MakeMeta<&TVolumeSelfSimpleCounters::MigrationStarted>(),
        MakeMeta<&TVolumeSelfSimpleCounters::MigrationProgress>(),
        MakeMeta<&TVolumeSelfSimpleCounters::ResyncStarted>(),
        MakeMeta<&TVolumeSelfSimpleCounters::ResyncProgress>(),
    };
};
static_assert(
    sizeof(TVolumeSelfSimpleCounters) ==
    sizeof(TVolumeSelfSimpleCounters::TCounter) *
        std::size(TVolumeSelfSimpleCounters::AllCounters));

struct TVolumeSelfCumulativeCounters
{
    using TCounter = TMemberWithMeta<TCumulativeCounter>;
    using TMeta = TMemberMeta<TCounter TVolumeSelfCumulativeCounters::*>;

    // Common
    TCounter ThrottlerRejectedRequests{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter ThrottlerPostponedRequests{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter ThrottlerSkippedRequests{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Expiring};
    TCounter UsedQuota{
        EPublishingPolicy::All,
        TCumulativeCounter::ECounterType::Generic,
        ECounterExpirationPolicy::Permanent};

    static constexpr TMeta AllCounters[] = {
        MakeMeta<&TVolumeSelfCumulativeCounters::ThrottlerRejectedRequests>(),
        MakeMeta<&TVolumeSelfCumulativeCounters::ThrottlerPostponedRequests>(),
        MakeMeta<&TVolumeSelfCumulativeCounters::ThrottlerSkippedRequests>(),
        MakeMeta<&TVolumeSelfCumulativeCounters::UsedQuota>(),
    };
};
static_assert(
    sizeof(TVolumeSelfCumulativeCounters) ==
    sizeof(TVolumeSelfCumulativeCounters::TCounter) *
        std::size(TVolumeSelfCumulativeCounters::AllCounters));

struct TVolumeSelfRequestCounters
{
    using TCounter = TMemberWithMeta<THistogram<TRequestUsTimeBuckets>>;
    using TMeta = TMemberMeta<TCounter TVolumeSelfRequestCounters::*>;

    // Common
    TCounter ReadBlocks{EPublishingPolicy::All};
    TCounter WriteBlocks{EPublishingPolicy::All};
    TCounter ZeroBlocks{EPublishingPolicy::All};
    TCounter DescribeBlocks{EPublishingPolicy::All};

    static constexpr TMeta AllCounters[] = {
        MakeMeta<&TVolumeSelfRequestCounters::ReadBlocks>(),
        MakeMeta<&TVolumeSelfRequestCounters::WriteBlocks>(),
        MakeMeta<&TVolumeSelfRequestCounters::ZeroBlocks>(),
        MakeMeta<&TVolumeSelfRequestCounters::DescribeBlocks>(),
    };
};
static_assert(
    sizeof(TVolumeSelfRequestCounters) ==
    sizeof(TVolumeSelfRequestCounters::TCounter) *
        std::size(TVolumeSelfRequestCounters::AllCounters));

struct TTransportCounters
{
    using TCounter = TMemberWithMeta<TCumulativeCounter>;
    using TMeta = TMemberMetaWithTag<TCounter TTransportCounters::*>;

    TCounter ReadBytes{EPublishingPolicy::All};
    TCounter WriteBytes{EPublishingPolicy::All};
    TCounter ReadCount{EPublishingPolicy::All};
    TCounter WriteCount{EPublishingPolicy::All};

    static constexpr TMeta AllCounters[] = {
        MakeMetaWithTag<&TTransportCounters::ReadBytes>(
            "RequestBytes",
            "ReadBlocks"),
        MakeMetaWithTag<&TTransportCounters::WriteBytes>(
            "RequestBytes",
            "WriteBlocks"),
        MakeMetaWithTag<&TTransportCounters::ReadCount>(
            "Count",
            "ReadBlocks"),
        MakeMetaWithTag<&TTransportCounters::WriteCount>(
            "Count",
            "WriteBlocks"),

    };
};

static_assert(
    sizeof(TTransportCounters) ==
    (sizeof(TTransportCounters::TCounter) *
     std::size(TTransportCounters::AllCounters)));
////////////////////////////////////////////////////////////////////////////////

struct TPartitionDiskCounters
{
    TSimpleDiskCounters Simple;
    TCumulativeDiskCounters Cumulative;
    THistogramRequestCounters RequestCounters;
    THistogramCounters Histogram;
    TTransportCounters Rdma;
    TTransportCounters Interconnect;

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
