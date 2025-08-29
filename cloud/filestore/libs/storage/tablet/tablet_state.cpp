#include "tablet_state_impl.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>

#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

IBlockLocation2RangeIndexPtr CreateHasher(const NProto::TFileSystem& fs)
{
    auto hasher = CreateRangeIdHasher(fs.GetRangeIdHasherType());
    Y_ABORT_UNLESS(
        hasher,
        "[f:%s] unsupported hasher type: %u",
        fs.GetFileSystemId().c_str(),
        fs.GetRangeIdHasherType());

    return hasher;
}

NProto::TFileStorePerformanceProfile GetDefaultPerformanceProfile()
{
    TThrottlerConfig config;
    NProto::TFileStorePerformanceProfile profile;
    profile.SetThrottlingEnabled(config.ThrottlingEnabled);
    profile.SetMaxReadIops(config.DefaultParameters.MaxReadIops);
    profile.SetMaxWriteIops(config.DefaultParameters.MaxWriteIops);
    profile.SetMaxReadBandwidth(config.DefaultParameters.MaxReadBandwidth);
    profile.SetMaxWriteBandwidth(config.DefaultParameters.MaxWriteBandwidth);
    profile.SetBoostTime(config.BoostParameters.BoostTime.MilliSeconds());
    profile.SetBoostRefillTime(
        config.BoostParameters.BoostRefillTime.MilliSeconds());
    profile.SetBoostPercentage(config.BoostParameters.BoostPercentage);
    profile.SetMaxPostponedWeight(config.DefaultThresholds.MaxPostponedWeight);
    profile.SetMaxPostponedTime(
        config.DefaultThresholds.MaxPostponedTime.MilliSeconds());
    profile.SetMaxPostponedCount(config.DefaultThresholds.MaxPostponedCount);
    profile.SetMaxWriteCostMultiplier(
        config.DefaultThresholds.MaxWriteCostMultiplier);
    profile.SetBurstPercentage(config.BurstPercentage);
    profile.SetDefaultPostponedRequestWeight(
        config.DefaultPostponedRequestWeight);
    return profile;
}

bool IsValid(const NProto::TFileStorePerformanceProfile& profile)
{
    return profile.GetMaxReadIops()
        && profile.GetMaxReadBandwidth()
        && profile.GetMaxPostponedWeight()
        && profile.GetMaxPostponedTime()
        && profile.GetDefaultPostponedRequestWeight();
}

ui64 CalculateInMemoryIndexCacheCapacity(
    const ui64 capacity,
    const ui64 maxNodes,
    const ui64 nodesToCapacityRatio)
{
    if (nodesToCapacityRatio == 0) {
        return capacity;
    }
    return Max(capacity, maxNodes / nodesToCapacityRatio);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TIndexTabletState::TIndexTabletState()
    : Impl(new TImpl(AllocatorRegistry))
{}

TIndexTabletState::~TIndexTabletState()
{}

void TIndexTabletState::UpdateLogTag(TString tag)
{
    Impl->FreshBytes.UpdateLogTag(tag);
    LogTag = std::move(tag);
}

void TIndexTabletState::LoadState(
    ui32 generation,
    const TStorageConfig& config,
    const NProto::TFileSystem& fileSystem,
    const NProto::TFileSystemStats& fileSystemStats,
    const NCloud::NProto::TTabletStorageInfo& tabletStorageInfo,
    const TVector<TDeletionMarker>& largeDeletionMarkers,
    const TVector<ui64>& orphanNodeIds,
    const TThrottlerConfig& throttlerConfig)
{
    Generation = generation;
    // https://github.com/ydb-platform/nbs/issues/1714
    // because of possible race in vdisks we should not start with 0
    LastStep = 1;
    LastCollectPerGenerationCounter = 0;

    TruncateBlocksThreshold = config.GetMaxBlocksPerTruncateTx();
    SessionHistoryEntryCount = config.GetSessionHistoryEntryCount();

    ChannelMinFreeSpace = config.GetChannelMinFreeSpace() / 100.;
    ChannelFreeSpaceThreshold = config.GetChannelFreeSpaceThreshold() / 100.;

    LargeDeletionMarkersEnabled = config.GetLargeDeletionMarkersEnabled();
    LargeDeletionMarkerBlocks = config.GetLargeDeletionMarkerBlocks();
    LargeDeletionMarkersThreshold = config.GetLargeDeletionMarkersThreshold();
    LargeDeletionMarkersCleanupThreshold =
        config.GetLargeDeletionMarkersCleanupThreshold();
    LargeDeletionMarkersThresholdForBackpressure =
        config.GetLargeDeletionMarkersThresholdForBackpressure();

    FileSystem.CopyFrom(fileSystem);
    FileSystemStats.CopyFrom(fileSystemStats);
    TabletStorageInfo.CopyFrom(tabletStorageInfo);
    // Changing thresholds in the config may result in a massive amount of
    // deletion markers to be cleaned up.
    // This causes Cleanup to run incessantly for a long time (up to several
    // hours) and it may result in performance degradation.
    // We track the minimal amount of deletion markers since the last tablet
    // reboot and throttle cleanup when the amount of deletion markers is
    // expected to drop below the minimal amount.
    // https://github.com/ydb-platform/nbs/pull/3268
    MinDeletionMarkersCountSinceTabletStart =
        fileSystemStats.GetDeletionMarkersCount();

    if (FileSystemStats.GetLastNodeId() < RootNodeId) {
        FileSystemStats.SetLastNodeId(RootNodeId);
    }

    LoadChannels();

    Impl->RangeIdHasher = CreateHasher(fileSystem);
    Impl->ThrottlingPolicy.Reset(throttlerConfig);
    Impl->ReadAheadCache.Reset(
        config.GetReadAheadCacheMaxNodes(),
        config.GetReadAheadCacheMaxResultsPerNode(),
        config.GetReadAheadCacheRangeSize(),
        config.GetReadAheadMaxGapPercentage(),
        config.GetReadAheadCacheMaxHandlesPerNode());
    Impl->NodeIndexCache.Reset(config.GetNodeIndexCacheMaxNodes());
    Impl->InMemoryIndexState.Reset(
        CalculateInMemoryIndexCacheCapacity(
            config.GetInMemoryIndexCacheNodesCapacity(),
            GetNodesCount(),
            config.GetInMemoryIndexCacheNodesToNodesCapacityRatio()),
        CalculateInMemoryIndexCacheCapacity(
            config.GetInMemoryIndexCacheNodeAttrsCapacity(),
            GetNodesCount(),
            config.GetInMemoryIndexCacheNodesToNodeAttrsCapacityRatio()),
        CalculateInMemoryIndexCacheCapacity(
            config.GetInMemoryIndexCacheNodeRefsCapacity(),
            GetNodesCount(),
            config.GetInMemoryIndexCacheNodesToNodeRefsCapacityRatio()),
        config.GetInMemoryIndexCacheNodeRefsExhaustivenessCapacity());
    Impl->MixedBlocks.Reset(config.GetMixedBlocksOffloadedRangesCapacity());

    for (const auto& deletionMarker: largeDeletionMarkers) {
        Impl->LargeBlocks.AddDeletionMarker(deletionMarker);
    }

    Impl->OrphanNodeIds.insert(orphanNodeIds.begin(), orphanNodeIds.end());

    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    Impl->ShardBalancer = CreateShardBalancer(
        config.GetShardBalancerPolicy(),
        GetBlockSize(),
        config.GetShardBalancerDesiredFreeSpaceReserve(),
        config.GetShardBalancerMinFreeSpaceReserve(),
        TVector<TString>(shardIds.begin(), shardIds.end()));
}

void TIndexTabletState::UpdateConfig(
    TIndexTabletDatabase& db,
    const TStorageConfig& config,
    const NProto::TFileSystem& fileSystem,
    const TThrottlerConfig& throttlerConfig)
{
    FileSystem.CopyFrom(fileSystem);
    db.WriteFileSystem(fileSystem);

    UpdateChannels();

    Impl->RangeIdHasher = CreateHasher(fileSystem);
    Impl->ThrottlingPolicy.Reset(throttlerConfig);

    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    Impl->ShardBalancer = CreateShardBalancer(
        config.GetShardBalancerPolicy(),
        GetBlockSize(),
        config.GetShardBalancerDesiredFreeSpaceReserve(),
        config.GetShardBalancerMinFreeSpaceReserve(),
        TVector<TString>(shardIds.begin(), shardIds.end()));
}

const NProto::TFileStorePerformanceProfile& TIndexTabletState::GetPerformanceProfile() const
{
    if (FileSystem.HasPerformanceProfile() &&
        IsValid(FileSystem.GetPerformanceProfile()))
    {
        return FileSystem.GetPerformanceProfile();
    }

    static const auto defaultProfile = GetDefaultPerformanceProfile();
    return defaultProfile;
}

void TIndexTabletState::DumpStats(IOutputStream& os) const
{
    NProtobufJson::TProto2JsonConfig config;
    config.SetFormatOutput(true);

    NProtobufJson::Proto2Json(
        FileSystemStats,
        os,
        config
    );
}

TMiscNodeStats TIndexTabletState::GetMiscNodeStats() const
{
    return {
        .OrphanNodesCount = static_cast<i64>(Impl->OrphanNodeIds.size()),
    };
}

ui64 TIndexTabletState::CalculateExpectedShardCount() const
{
    if (FileSystem.GetShardNo()) {
        // sharding is flat
        return 0;
    }

    const ui64 currentShardCount = FileSystem.ShardFileSystemIdsSize();
    ui64 autoShardCount = 0;
    if (FileSystem.GetAutomaticShardCreationEnabled()
            && FileSystem.GetShardAllocationUnit())
    {
        autoShardCount = ComputeShardCount(
            FileSystem.GetBlocksCount(),
            FileSystem.GetBlockSize(),
            FileSystem.GetShardAllocationUnit());
    }

    return Max(currentShardCount, autoShardCount);
}

}   // namespace NCloud::NFileStore::NStorage
