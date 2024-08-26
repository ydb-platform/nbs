#include "tablet_state_impl.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
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
    const TThrottlerConfig& throttlerConfig)
{
    Generation = generation;
    // https://github.com/ydb-platform/nbs/issues/1714
    // because of possible race in vdisks we should not start with 0
    LastStep = 1;
    LastCollectCounter = 0;

    TruncateBlocksThreshold = config.GetMaxBlocksPerTruncateTx();
    SessionHistoryEntryCount = config.GetSessionHistoryEntryCount();

    ChannelMinFreeSpace = config.GetChannelMinFreeSpace() / 100.;
    ChannelFreeSpaceThreshold = config.GetChannelFreeSpaceThreshold() / 100.;

    FileSystem.CopyFrom(fileSystem);
    FileSystemStats.CopyFrom(fileSystemStats);
    TabletStorageInfo.CopyFrom(tabletStorageInfo);

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
        config.GetInMemoryIndexCacheNodesCapacity(),
        config.GetInMemoryIndexCacheNodesVerCapacity(),
        config.GetInMemoryIndexCacheNodeAttrsCapacity(),
        config.GetInMemoryIndexCacheNodeAttrsVerCapacity(),
        config.GetInMemoryIndexCacheNodeRefsCapacity(),
        config.GetInMemoryIndexCacheNodeRefsVerCapacity());
}

void TIndexTabletState::UpdateConfig(
    TIndexTabletDatabase& db,
    const NProto::TFileSystem& fileSystem,
    const TThrottlerConfig& throttlerConfig)
{
    FileSystem.CopyFrom(fileSystem);
    db.WriteFileSystem(fileSystem);

    UpdateChannels();

    Impl->RangeIdHasher = CreateHasher(fileSystem);
    Impl->ThrottlingPolicy.Reset(throttlerConfig);
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

TNodeToSessionStat::EKind TNodeToSessionStat::AddRead(
    ui64 nodeId,
    const TString& sessionId)
{
    ++Stat[nodeId].ReadSessions[sessionId];
    return GetKind(nodeId);
}

TNodeToSessionStat::EKind TNodeToSessionStat::AddWrite(
    ui64 nodeId,
    const TString& sessionId)
{
    ++Stat[nodeId].WriteSessions[sessionId];
    return GetKind(nodeId);
}

void TNodeToSessionStat::Clean(
    const TStat::iterator& nodeStatIterator,
    const TString& sessionId)
{
    auto& nodeStat = nodeStatIterator->second;

    {
        const auto it = nodeStat.WriteSessions.find(sessionId);
        if (it != nodeStat.WriteSessions.end() && it->second <= 0) {
            nodeStat.WriteSessions.erase(it);
        }
    }

    {
        const auto it = nodeStat.ReadSessions.find(sessionId);
        if (it != nodeStat.ReadSessions.end() && it->second <= 0) {
            nodeStat.ReadSessions.erase(it);
        }
    }

    if (nodeStat.WriteSessions.empty() && nodeStat.ReadSessions.empty()) {
        Stat.erase(nodeStatIterator);
    }
}

TNodeToSessionStat::EKind TNodeToSessionStat::RemoveRead(
    ui64 nodeId,
    const TString& sessionId)
{
    const auto& nodeStatIterator = Stat.find(nodeId);
    if (nodeStatIterator != Stat.end()) {
        --nodeStatIterator->second.ReadSessions[sessionId];
        Clean(nodeStatIterator, sessionId);
    }
    return GetKind(nodeStatIterator);
}

TNodeToSessionStat::EKind TNodeToSessionStat::RemoveWrite(
    ui64 nodeId,
    const TString& sessionId)
{
    const auto& nodeStatIterator = Stat.find(nodeId);
    if (nodeStatIterator != Stat.end()) {
        --nodeStatIterator->second.WriteSessions[sessionId];
        Clean(nodeStatIterator, sessionId);
    }
    return GetKind(nodeStatIterator);
}

TNodeToSessionStat::EKind TNodeToSessionStat::GetKind(ui64 nodeId) const
{
    const auto& nodeStatIterator = Stat.find(nodeId);
    return GetKind(nodeStatIterator);
}

TNodeToSessionStat::EKind TNodeToSessionStat::GetKind(
    const TStat::const_iterator& nodeStatIterator) const
{
    if (nodeStatIterator == Stat.end()) {
        return EKind::None;
    }
    const auto& nodeStat = nodeStatIterator->second;
    if (nodeStat.WriteSessions.size() > 1) {
        return EKind::NodesOpenForWritingByMultipleSessions;
    }
    if (nodeStat.WriteSessions.size() == 1) {
        return EKind::NodesOpenForWritingBySingleSession;
    }
    if (nodeStat.ReadSessions.size() > 1) {
        return EKind::NodesOpenForReadingByMultipleSessions;
    }
    if (nodeStat.ReadSessions.size() == 1) {
        return EKind::NodesOpenForReadingBySingleSession;
    }
    return EKind::None;
}

}   // namespace NCloud::NFileStore::NStorage
