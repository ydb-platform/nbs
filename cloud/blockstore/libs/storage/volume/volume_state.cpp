#include "volume_state.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>

#include <cloud/storage/core/libs/common/media.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/stream/str.h>
#include <util/system/hostname.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

bool THistoryLogKey::operator == (const THistoryLogKey& rhs) const
{
    return std::tie(Timestamp, SeqNo) == std::tie(rhs.Timestamp, rhs.SeqNo);
}

bool THistoryLogKey::operator != (const THistoryLogKey& rhs) const
{
    return !(*this == rhs);
}

bool THistoryLogKey::operator < (THistoryLogKey rhs) const
{
    return std::tie(Timestamp, SeqNo) < std::tie(rhs.Timestamp, rhs.SeqNo);
}

bool THistoryLogKey::operator >= (THistoryLogKey rhs) const
{
    return !(*this < rhs);
}

////////////////////////////////////////////////////////////////////////////////

ui64 ComputeBlockCount(const NProto::TVolumeMeta& meta)
{
    ui64 blockCount = 0;
    const auto& volumeConfig = meta.GetVolumeConfig();
    for (ui32 index = 0; index < volumeConfig.PartitionsSize(); ++index) {
        blockCount += volumeConfig.GetPartitions(index).GetBlockCount();
    }

    return blockCount;
}

////////////////////////////////////////////////////////////////////////////////

TCachedVolumeMountHistory::TCachedVolumeMountHistory(
        ui32 capacity,
        TVolumeMountHistorySlice records)
    : Capacity(capacity)
    , LastLogRecord(!records.Items.empty() ? records.Items.front().Key : THistoryLogKey())
    , NextOlderRecord(std::move(records.NextOlderRecord))
{
    for (size_t i = 0; i < records.Items.size(); ++i) {
        if (i == Capacity) {
           NextOlderRecord.emplace(records.Items[i].Key);
           break;
        }
        Items.emplace_back(std::move(records.Items[i]));
    }
}

void TCachedVolumeMountHistory::AddHistoryLogItem(
    THistoryLogKey key,
    NProto::TVolumeOperation op)
{
    Items.emplace_front(key, std::move(op));
    if (Items.size() > Capacity) {
        NextOlderRecord = Items.back().Key;
        Items.pop_back();
    }
}

THistoryLogKey TCachedVolumeMountHistory::AllocateHistoryLogKey(TInstant timestamp)
{
    if (LastLogRecord.Timestamp != timestamp) {
        LastLogRecord.Timestamp = timestamp;
        LastLogRecord.SeqNo = 0;
    } else {
        ++LastLogRecord.SeqNo;
    }
    return LastLogRecord;
}

void TCachedVolumeMountHistory::CleanupHistoryIfNeeded(TInstant oldest)
{
    bool haveRemovedItems = false;
    while (Items.size() && (Items.back().Key.Timestamp <= oldest)) {
        Items.pop_back();
        haveRemovedItems = true;
    }
    if (haveRemovedItems) {
        NextOlderRecord.reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

TVolumeState::TVolumeState(
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig,
        NProto::TVolumeMeta meta,
        TVector<TVolumeMetaHistoryItem> metaHistory,
        TVector<TRuntimeVolumeParamsValue> volumeParams,
        TThrottlerConfig throttlerConfig,
        THashMap<TString, TVolumeClientState> infos,
        TCachedVolumeMountHistory mountHistory,
        TVector<TCheckpointRequest> checkpointRequests,
        TFollowerDisks followerDisks,
        TLeaderDisks leaderDisks,
        bool startPartitionsNeeded)
    : StorageConfig(std::move(storageConfig))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , Meta(std::move(meta))
    , MetaHistory(std::move(metaHistory))
    , Config(&Meta.GetConfig())
    , VolumeParams(std::move(volumeParams))
    , ClientInfosByClientId(std::move(infos))
    , ThrottlerConfig(std::move(throttlerConfig))
    , ThrottlingPolicy(Config->GetPerformanceProfile(), ThrottlerConfig)
    , MountHistory(std::move(mountHistory))
    , CheckpointStore(std::move(checkpointRequests), Config->GetDiskId())
    , StartPartitionsNeeded(startPartitionsNeeded)
    , FollowerDisks(std::move(followerDisks))
    , LeaderDisks(std::move(leaderDisks))
{
    Reset();

    for (auto& pair: ClientInfosByClientId) {
        auto& info = pair.second;
        const auto& volumeClientInfo = info.GetVolumeClientInfo();

        if (IsReadWriteMode(volumeClientInfo.GetVolumeAccessMode())) {
            ReadWriteAccessClientId = pair.first;
            MountSeqNumber = volumeClientInfo.GetMountSeqNumber();
            if (volumeClientInfo.GetVolumeAccessMode()
                    == NProto::VOLUME_ACCESS_REPAIR)
            {
                StorageAccessMode = EStorageAccessMode::Repair;
            }
        }

        if (volumeClientInfo.GetVolumeMountMode() == NProto::VOLUME_MOUNT_LOCAL) {
            LocalMountClientId = pair.first;
        }
    }

    const auto lightCheckpoints = CheckpointStore.GetLightCheckpoints();
    if (!lightCheckpoints.empty()) {
        StartCheckpointLight();
    }
}

const TRuntimeVolumeParams& TVolumeState::GetVolumeParams() const
{
    return VolumeParams;
}

TRuntimeVolumeParams& TVolumeState::GetVolumeParams()
{
    return VolumeParams;
}

void TVolumeState::AddLaggingAgent(NProto::TLaggingAgent agent)
{
    Meta.MutableLaggingAgentsInfo()->MutableAgents()->Add(std::move(agent));
}

std::optional<NProto::TLaggingAgent> TVolumeState::RemoveLaggingAgent(
    const TString& agentId)
{
    ResetLaggingAgentMigrationState(agentId);

    auto agentIdPredicate = [&agentId](const auto& info)
    {
        return info.GetAgentId() == agentId;
    };

    auto& laggingAgents = *Meta.MutableLaggingAgentsInfo()->MutableAgents();
    Y_DEBUG_ABORT_UNLESS(CountIf(laggingAgents, agentIdPredicate) <= 1);
    auto it = FindIf(laggingAgents, agentIdPredicate);
    if (it != laggingAgents.end()) {
        NProto::TLaggingAgent laggingAgent = std::move(*it);
        laggingAgents.erase(it);
        return laggingAgent;
    }
    return std::nullopt;
}

bool TVolumeState::HasLaggingAgents() const
{
    return Meta.GetLaggingAgentsInfo().AgentsSize() != 0;
}

bool TVolumeState::HasLaggingInReplica(ui32 replicaIndex) const
{
    for (const auto& agent: Meta.GetLaggingAgentsInfo().GetAgents()) {
        if (agent.GetReplicaIndex() == replicaIndex) {
            return true;
        }
    }
    return false;
}

THashSet<TString> TVolumeState::GetLaggingDevices() const
{
    THashSet<TString> laggingDevices;
    for (const auto& agent: Meta.GetLaggingAgentsInfo().GetAgents()) {
        for (const auto& device: agent.GetDevices()) {
            laggingDevices.insert(device.GetDeviceUUID());
        }
    }
    return laggingDevices;
}

void TVolumeState::UpdateLaggingAgentMigrationState(
    const TString& agentId,
    ui64 cleanBlocks,
    ui64 dirtyBlocks)
{
    CurrentlyMigratingLaggingAgents[agentId] = TLaggingAgentMigrationInfo{
        .CleanBlocks = cleanBlocks,
        .DirtyBlocks = dirtyBlocks,
    };
}

void TVolumeState::ResetLaggingAgentMigrationState(const TString& agentId)
{
    CurrentlyMigratingLaggingAgents.erase(agentId);
}

auto TVolumeState::GetLaggingAgentsMigrationInfo() const
    -> const THashMap<TString, TLaggingAgentMigrationInfo>&
{
    return CurrentlyMigratingLaggingAgents;
}

void TVolumeState::ResetMeta(NProto::TVolumeMeta meta)
{
    Meta = std::move(meta);
    Config = &Meta.GetConfig();

    Reset();
}

void TVolumeState::AddMetaHistory(TVolumeMetaHistoryItem meta)
{
    MetaHistory.push_back(std::move(meta));
}

void TVolumeState::ResetThrottlingPolicy(
    const NProto::TVolumePerformanceProfile& pp)
{
    ThrottlingPolicy.Reset(
        pp,
        TThrottlerConfig(
            ThrottlerConfig.MaxDelay,
            ThrottlerConfig.MaxWriteCostMultiplier,
            ThrottlerConfig.DefaultPostponedRequestWeight,
            ThrottlingPolicy.GetCurrentBoostBudget(),
            ThrottlerConfig.UseDiskSpaceScore));
}

bool TVolumeState::ShouldTrackUsedBlocks() const
{
    if (!IsDiskRegistryMediaKind()) {
        return false;
    }

    const bool overlay = !GetBaseDiskId().empty();
    return overlay;
}

void TVolumeState::Reset()
{
    Partitions.clear();
    PartitionStatInfos.clear();
    PartitionsState = TPartitionInfo::UNKNOWN;
    ForceRepair = false;
    RejectWrite = false;
    TrackUsedBlocks = ShouldTrackUsedBlocks();
    MaskUnusedBlocks = false;
    MaxTimedOutDeviceStateDuration = TDuration::Zero();
    UseRdma = StorageConfig->GetUseRdma()
        || StorageConfig->IsUseRdmaFeatureEnabled(
            Meta.GetConfig().GetCloudId(),
            Meta.GetConfig().GetFolderId(),
            Meta.GetConfig().GetDiskId());
    UseFastPath = false;
    UseRdmaForThisVolume = false;
    AcceptInvalidDiskAllocationResponse = false;
    UseIntermediateWriteBuffer = false;

    if (IsDiskRegistryMediaKind() && Meta.GetDevices().size()) {
        CreatePartitionStatInfo(GetDiskId(), 0);
    }

    if (!IsDiskRegistryMediaKind()) {
        ui32 partitionIndex = 0;
        for (ui64 tabletId: Meta.GetPartitions()) {
            Partitions.emplace_back(
                tabletId,
                Meta.GetConfig(),
                partitionIndex++,
                StorageConfig->GetTabletRebootCoolDownIncrement(),
                StorageConfig->GetTabletRebootCoolDownMax());
            CreatePartitionStatInfo(GetDiskId(), tabletId);
        }
    }

    ResetThrottlingPolicy(Config->GetPerformanceProfile());

    BlockCount = ComputeBlockCount(Meta);

    TStringBuf sit(Meta.GetVolumeConfig().GetTagsStr());
    TStringBuf tagStr;
    while (sit.NextTok(',', tagStr)) {
        TStringBuf tag, value;
        tagStr.Split('=', tag, value);
        if (tag == "repair") {
            ForceRepair = true;
        } else if (tag == "mute-io-errors") {
            Meta.SetMuteIOErrors(true);
        } else if (tag == "accept-invalid-disk-allocation-response") {
            AcceptInvalidDiskAllocationResponse = true;
        } else if (tag == "read-only") {
            RejectWrite = true;
        } else if (tag == "track-used") {
            // XXX beware that used block tracking is not supported for
            // cross-partition writes in multipartition network-ssd/network-hdd
            // volumes
            TrackUsedBlocks = true;
        } else if (tag == "mask-unused") {
            TrackUsedBlocks = true;
            MaskUnusedBlocks = true;
        } else if (tag == "use-rdma") {
            UseRdma = true;
            UseRdmaForThisVolume = true;
        } else if (tag == "max-timed-out-device-state-duration") {
            TDuration::TryParse(value, MaxTimedOutDeviceStateDuration);
        } else if (tag == "use-fastpath") {
            UseFastPath = true;
        } else if (tag == IntermediateWriteBufferTagName) {
            UseIntermediateWriteBuffer = true;
        }
    }

    UseMirrorResync = StorageConfig->GetUseMirrorResync();
    ForceMirrorResync = StorageConfig->GetForceMirrorResync();

    // this filtration is needed due to a bug that caused some disks to have
    // garbage in FreshDeviceIds list
    FilteredFreshDeviceIds = MakeFilteredDeviceIds();

    if (TrackUsedBlocks) {
        AccessUsedBlocks();
    }
}

////////////////////////////////////////////////////////////////////////////////

THashSet<TString> TVolumeState::MakeFilteredDeviceIds() const
{
    const TInstant oldDate = TInstant::ParseIso8601("2023-08-30");
    const auto& ids = Meta.GetFreshDeviceIds();
    if (GetCreationTs() > oldDate) {
        return {ids.begin(), ids.end()};
    }

    THashSet<TString> filtered;
    auto addFreshDevices = [&] (const auto& devices) {
        for (const auto& device: devices) {
            const bool found = Find(
                ids.begin(),
                ids.end(),
                device.GetDeviceUUID()) != ids.end();

            if (found) {
                filtered.insert(device.GetDeviceUUID());
            }
        }
    };

    addFreshDevices(Meta.GetDevices());
    for (const auto& r: Meta.GetReplicas()) {
        addFreshDevices(r.GetDevices());
    }

    return filtered;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeState::FillDeviceInfo(NProto::TVolume& volume) const
{
    NStorage::FillDeviceInfo(
        Meta.GetDevices(),
        Meta.GetMigrations(),
        Meta.GetReplicas(),
        Meta.GetFreshDeviceIds(),
        volume);
}

bool TVolumeState::IsDiskRegistryMediaKind() const
{
    return NCloud::IsDiskRegistryMediaKind(Config->GetStorageMediaKind());
}

bool TVolumeState::HasPerformanceProfileModifications(
    const TStorageConfig& config) const
{
    const NKikimrBlockStore::TVolumeConfig& volumeConfig =
        GetMeta().GetVolumeConfig();

    auto currentPerformanceProfile =
        VolumeConfigToVolumePerformanceProfile(volumeConfig);

    NProto::TVolumePerformanceProfile defaultPerformanceProfile;
    {
        const TVolumeParams volumeParams = ComputeVolumeParams(
            config,
            GetBlockSize(),
            GetBlocksCount(),
            static_cast<NProto::EStorageMediaKind>(
                volumeConfig.GetStorageMediaKind()),
            static_cast<ui32>(volumeConfig.GetPartitions().size()),
            volumeConfig.GetCloudId(),
            volumeConfig.GetFolderId(),
            volumeConfig.GetDiskId(),
            volumeConfig.GetIsSystem(),
            !volumeConfig.GetBaseDiskId().empty());

        NKikimrBlockStore::TVolumeConfig defaultVolumeConfig;
        ResizeVolume(config, volumeParams, {}, {}, defaultVolumeConfig);
        defaultPerformanceProfile =
            VolumeConfigToVolumePerformanceProfile(defaultVolumeConfig);
    }

    using google::protobuf::util::MessageDifferencer;
    return !MessageDifferencer::Equals(
        currentPerformanceProfile,
        defaultPerformanceProfile);
}

////////////////////////////////////////////////////////////////////////////////

TPartitionInfo* TVolumeState::GetPartition(ui64 tabletId)
{
    for (auto& partition: Partitions) {
        if (partition.TabletId == tabletId) {
            return &partition;
        }
    }
    return nullptr;
}

std::optional<ui32> TVolumeState::FindPartitionIndex(
    NActors::TActorId partitionActorId) const
{
    for (ui32 i = 0; i < Partitions.size(); ++i) {
        if (Partitions[i].IsKnownActorId(partitionActorId)) {
            return i;
        }
    }
    return std::nullopt;
}

std::optional<ui64> TVolumeState::FindPartitionTabletId(
    NActors::TActorId partitionActorId) const
{
    for (const auto& partition: Partitions) {
        if (partition.IsKnownActorId(partitionActorId)) {
            return partition.TabletId;
        }
    }
    return std::nullopt;
}

void TVolumeState::SetPartitionsState(TPartitionInfo::EState state)
{
    for (auto& partition: Partitions) {
        partition.State = state;
    }
}

TPartitionInfo::EState TVolumeState::UpdatePartitionsState()
{
    if (IsDiskRegistryMediaKind()) {
        ui64 bytes = 0;
        for (const auto& device: Meta.GetDevices()) {
            bytes += device.GetBlocksCount() * device.GetBlockSize();
        }

        const bool allocated =
            bytes >= Config->GetBlockSize() * Config->GetBlocksCount();
        const bool actorStarted =
            DiskRegistryBasedPartitionActor.GetTop() != TActorId();
        if (allocated && actorStarted) {
            PartitionsState = TPartitionInfo::READY;
        } else {
            PartitionsState = TPartitionInfo::UNKNOWN;
        }

        return PartitionsState;
    }

    ui32 unknown = 0, stopped = 0, started = 0, ready = 0, failed = 0;
    for (const auto& partition: Partitions) {
        switch (partition.State) {
            case TPartitionInfo::UNKNOWN:
                ++unknown;
                break;
            case TPartitionInfo::STOPPED:
                ++stopped;
                break;
            case TPartitionInfo::STARTED:
                ++started;
                break;
            case TPartitionInfo::READY:
                ++ready;
                break;
            case TPartitionInfo::FAILED:
                ++failed;
                break;
        }
    }

    Y_ABORT_UNLESS(unknown + stopped + started + ready + failed == Partitions.size());
    if (unknown) {
        PartitionsState = TPartitionInfo::UNKNOWN;
    } else if (failed) {
        PartitionsState = TPartitionInfo::FAILED;
    } else if (stopped) {
        PartitionsState = TPartitionInfo::STOPPED;
    } else if (started) {
        PartitionsState = TPartitionInfo::STARTED;
    } else {
        PartitionsState = TPartitionInfo::READY;
    }

    return PartitionsState;
}

TString TVolumeState::GetPartitionsError() const
{
    TStringStream out;
    for (const auto& partition: Partitions) {
        if (partition.State == TPartitionInfo::FAILED) {
            if (out.Str()) {
                out << "; ";
            }
            out << "partition: " << partition.TabletId
                << ", error: " << partition.Message;
        }
    }
    return out.Str();
}

void TVolumeState::SetDiskRegistryBasedPartitionActor(
    TActorsStack actors,
    TNonreplicatedPartitionConfigPtr config)
{
    DiskRegistryBasedPartitionActor = std::move(actors);
    NonreplicatedPartitionConfig = std::move(config);
}

////////////////////////////////////////////////////////////////////////////////

TVolumeState::TAddClientResult TVolumeState::AddClient(
    const NProto::TVolumeClientInfo& info,
    const TActorId& pipeServerActorId,
    const TActorId& senderActorId,
    TInstant referenceTimestamp)
{
    const auto& clientId = info.GetClientId();
    const auto rwClientId = ReadWriteAccessClientId;

    TAddClientResult res;

    if (!clientId) {
        res.Error = MakeError(E_ARGUMENT, "ClientId not specified");
        return res;
    }

    bool readWriteAccess = IsReadWriteMode(info.GetVolumeAccessMode());

    if (readWriteAccess && !CanAcceptClient(
                               info.GetFillSeqNumber(),
                               info.GetFillGeneration()))
   {
        res.Error = MakeError(
            E_PRECONDITION_FAILED,
            TStringBuilder()
                << "Client can not be accepted with read-write access"
                << ", new FillSeqNumber: " << info.GetFillSeqNumber()
                << ", current FillSeqNumber: " << Meta.GetFillSeqNumber()
                << ", proposed FillGeneration: " << info.GetFillGeneration()
                << ", actual FillGeneration: " << Meta.GetVolumeConfig().GetFillGeneration()
                << ", is disk filling finished: " << Meta.GetVolumeConfig().GetIsFillFinished());
        return res;
    }

    if (readWriteAccess && ReadWriteAccessClientId && ReadWriteAccessClientId != clientId) {
        if (!CanPreemptClient(
                ReadWriteAccessClientId,
                referenceTimestamp,
                info.GetMountSeqNumber()))
        {
                res.Error = MakeError(
                    E_BS_MOUNT_CONFLICT,
                    TStringBuilder()
                        << "Volume " << Config->GetDiskId().Quote()
                        << " already has connection with read-write access: "
                        << ReadWriteAccessClientId);
                return res;
        }
        res.RemovedClientIds.push_back(ReadWriteAccessClientId);
    }

    bool localMount = (info.GetVolumeMountMode() == NProto::VOLUME_MOUNT_LOCAL);
    if (localMount && LocalMountClientId && LocalMountClientId != clientId) {
        if (!CanPreemptClient(
                LocalMountClientId,
                referenceTimestamp,
                info.GetMountSeqNumber()))
        {
                res.Error = MakeError(
                    E_BS_MOUNT_CONFLICT,
                    TStringBuilder()
                        << "Volume " << Config->GetDiskId().Quote()
                        << " already has connection with local mount: "
                        << LocalMountClientId);
                return res;
        }

        if (!res.RemovedClientIds || (LocalMountClientId != rwClientId)) {
            res.RemovedClientIds.emplace_back(std::move(LocalMountClientId));
        }
    }

    if (readWriteAccess) {
        res.ForceTabletRestart = clientId != ReadWriteAccessClientId ||
                                 ShouldForceTabletRestart(info);
        ReadWriteAccessClientId = clientId;
        MountSeqNumber = info.GetMountSeqNumber();
    }

    if (localMount) {
        LocalMountClientId = clientId;
    }

    auto range = ClientIdsByPipeServerId.equal_range(pipeServerActorId);
    auto it = find_if(range.first, range.second, [&] (const auto& p) {
        return p.second == clientId;
    });
    if (it == range.second) {
        ClientIdsByPipeServerId.emplace(pipeServerActorId, clientId);
    }

    auto [newIt, added] = ClientInfosByClientId.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(clientId),
        std::forward_as_tuple(info));
    auto pipeRes = newIt->second.AddPipe(
        pipeServerActorId,
        senderActorId.NodeId(),
        info.GetVolumeAccessMode(),
        info.GetVolumeMountMode(),
        info.GetMountFlags());

    if (HasError(pipeRes.Error)) {
        return pipeRes.Error;
    }

    if (info.GetVolumeAccessMode() == NProto::VOLUME_ACCESS_REPAIR) {
        StorageAccessMode = EStorageAccessMode::Repair;
    }

    if (added) {
        return res;
    }

    if (!pipeRes.IsNew) {
        res.Error = MakeError(S_ALREADY, "Client already connected to volume");
    }

    return res;
}

TInstant TVolumeState::GetLastActivityTimestamp(const TString& clientId) const
{
    auto it = ClientInfosByClientId.find(clientId);
    if (it == ClientInfosByClientId.end()) {
        return {};
    }

    return TInstant::MicroSeconds(
        it->second.GetVolumeClientInfo().GetLastActivityTimestamp());
}

void TVolumeState::SetLastActivityTimestamp(
    const TString& clientId,
    TInstant ts)
{
    auto it = ClientInfosByClientId.find(clientId);
    if (it != ClientInfosByClientId.end()) {
        it->second.SetLastActivityTimestamp(ts);
    }
}

bool TVolumeState::IsClientStale(
    const TString& clientId,
    TInstant referenceTimestamp) const
{
    auto it = ClientInfosByClientId.find(clientId);
    Y_ABORT_UNLESS(it != ClientInfosByClientId.end());

    return IsClientStale(it->second, referenceTimestamp);
}

bool TVolumeState::IsClientStale(
    const TVolumeClientState& clientInfo,
    TInstant referenceTimestamp) const
{
    return IsClientStale(clientInfo.GetVolumeClientInfo(), referenceTimestamp);
}

bool TVolumeState::IsClientStale(
    const NProto::TVolumeClientInfo& clientInfo,
    TInstant referenceTimestamp) const
{
    auto disconnectTimestamp = TInstant::MicroSeconds(
        clientInfo.GetDisconnectTimestamp());
    // clients which don't correspond to disconnected services are considered
    // active
    if (!disconnectTimestamp) {
        return false;
    }

    // clients which services got disconnected recently enough are considered
    // still active
    TDuration timePassed = referenceTimestamp - disconnectTimestamp;
    return timePassed >= StorageConfig->GetInactiveClientsTimeout();
}

bool TVolumeState::HasActiveClients(TInstant referenceTimestamp) const
{
    for (const auto& client : ClientInfosByClientId) {
        auto disconnectTimestamp = TInstant::MicroSeconds(
            client.second.GetVolumeClientInfo().GetDisconnectTimestamp());
            if (!disconnectTimestamp) {
                return true;
            }
        TDuration timePassed = referenceTimestamp - disconnectTimestamp;
        if (timePassed < StorageConfig->GetInactiveClientsTimeout()) {
            return true;
        }
    }
    return false;
}

bool TVolumeState::IsPreempted(TActorId selfId) const
{
    for (const auto& client: ClientInfosByClientId) {
        if (client.second.IsPreempted(selfId.NodeId())) {
            return true;
        }
    }
    return false;
}

const NProto::TVolumeClientInfo* TVolumeState::GetClient(const TString& clientId) const
{
    auto it = ClientInfosByClientId.find(clientId);
    if (it != ClientInfosByClientId.end()) {
        return &it->second.GetVolumeClientInfo();
    }

    return nullptr;
}

NProto::TError TVolumeState::RemoveClient(
    const TString& clientId,
    const TActorId& pipeServerActorId)
{
    if (!clientId) {
        return MakeError(E_ARGUMENT, "ClientId not specified");
    }

    auto it = ClientInfosByClientId.find(clientId);
    if (it == ClientInfosByClientId.end()) {
        return MakeError(S_ALREADY, "Client is not connected to volume");
    }

    auto& clientInfo = it->second;

    const auto accessMode = clientInfo.GetVolumeClientInfo().GetVolumeAccessMode();
    if (accessMode == NProto::VOLUME_ACCESS_REPAIR) {
        StorageAccessMode = EStorageAccessMode::Default;
    }

    UnmapClientFromPipeServerId(pipeServerActorId, clientId);

    if (ReadWriteAccessClientId == clientId) {
        ReadWriteAccessClientId.clear();
        MountSeqNumber = 0;
    }

    if (LocalMountClientId == clientId) {
        LocalMountClientId.clear();
    }

    clientInfo.RemovePipe(pipeServerActorId, TInstant());

    if (!clientInfo.AnyPipeAlive()) {
        ClientInfosByClientId.erase(it);
    }

    return {};
}

void TVolumeState::SetServiceDisconnected(
    const TActorId& pipeServerActorId,
    TInstant disconnectTime)
{
    // Don't remove this service's clients immediately but set disconnect time
    // for them so that they might be removed if no reconnect occurs soon enough
    auto p = ClientIdsByPipeServerId.equal_range(pipeServerActorId);
    for (auto it = p.first; it != p.second;) {
        const auto& clientId = it->second;
        auto* clientInfo = ClientInfosByClientId.FindPtr(clientId);
        Y_ABORT_UNLESS(clientInfo);
        clientInfo->RemovePipe(pipeServerActorId, disconnectTime);
        ClientIdsByPipeServerId.erase(it++);
    }
}

void TVolumeState::UnmapClientFromPipeServerId(
    const TActorId& pipeServerActorId,
    const TString& clientId)
{
    if (pipeServerActorId) {
        auto range = ClientIdsByPipeServerId.equal_range(pipeServerActorId);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == clientId) {
                ClientIdsByPipeServerId.erase(it);
                return;
            }
        }
    } else {
        auto& servers = ClientIdsByPipeServerId;
        for (auto it = servers.begin(); it != servers.end();) {
            if (it->second == clientId) {
                ClientIdsByPipeServerId.erase(it++);
            } else {
                ++it;
            }
        }
    }
}

TVector<TActorId> TVolumeState::ClearPipeServerIds(TInstant ts)
{
    TVector<TActorId> result;
    for (const auto& client: ClientIdsByPipeServerId) {
        result.push_back(client.first);
    }
    ClientIdsByPipeServerId.clear();
    for (auto& client: ClientInfosByClientId) {
        client.second.RemovePipe({}, ts);
    }

    return result;
}

const THashMultiMap<TActorId, TString>& TVolumeState::GetPipeServerId2ClientId() const
{
    return ClientIdsByPipeServerId;
}

TVector<NProto::TDeviceConfig>
TVolumeState::GetDevicesForAcquireOrRelease() const
{
    const THashSet<TString> deviceIdsToIgnore(
        Meta.GetUnavailableDeviceIds().begin(),
        Meta.GetUnavailableDeviceIds().end());

    const size_t allDevicesCount =
        ((Meta.ReplicasSize() + 1) * Meta.DevicesSize()) +
        Meta.MigrationsSize();

    TVector<NProto::TDeviceConfig> resultDevices;
    resultDevices.reserve(allDevicesCount);

    auto addDeviceIfNeeded = [&](const auto& d)
    {
        if (deviceIdsToIgnore.contains(d.GetDeviceUUID())) {
            return;
        }
        resultDevices.emplace_back(d);
    };

    for (const auto& device: Meta.GetDevices()) {
        addDeviceIfNeeded(device);
    }
    for (const auto& replica: Meta.GetReplicas()) {
        for (const auto& device: replica.GetDevices()) {
            addDeviceIfNeeded(device);
        }
    }
    for (const auto& migration: Meta.GetMigrations()) {
        addDeviceIfNeeded(migration.GetTargetDevice());
    }

    return resultDevices;
}

TCreateFollowerRequestInfo& TVolumeState::AccessCreateFollowerRequestInfo(
    const TLeaderFollowerLink& link)
{
    for (auto& requestInfo: CreateFollowerRequests) {
        if (requestInfo.Link.Match(link)) {
            return requestInfo;
        }
    }

    CreateFollowerRequests.push_back(TCreateFollowerRequestInfo{.Link = link});
    return CreateFollowerRequests.back();
}

void TVolumeState::DeleteCreateFollowerRequestInfo(
    const TLeaderFollowerLink& link)
{
    EraseIf(
        CreateFollowerRequests,
        [&](const TCreateFollowerRequestInfo& requestInfo)
        { return requestInfo.Link.Match(link); });
}

std::optional<TFollowerDiskInfo> TVolumeState::FindFollower(
    const TLeaderFollowerLink& link) const
{
    for (const auto& follower: FollowerDisks) {
        if (follower.Link.Match(link)) {
            return follower;
        }
    }
    return std::nullopt;
}

void TVolumeState::AddOrUpdateFollower(TFollowerDiskInfo follower)
{
    for (auto& followerInfo: FollowerDisks) {
        if (followerInfo.Link.LinkUUID == follower.Link.LinkUUID) {
            followerInfo = std::move(follower);
            return;
        }
    }
    FollowerDisks.push_back(std::move(follower));
}

void TVolumeState::RemoveFollower(const TLeaderFollowerLink& link)
{
    EraseIf(
        FollowerDisks,
        [&](const TFollowerDiskInfo& follower)
        { return follower.Link.Match(link); });
}

const TFollowerDisks& TVolumeState::GetAllFollowers() const
{
    return FollowerDisks;
}

std::optional<TLeaderDiskInfo> TVolumeState::FindLeader(
    const TLeaderFollowerLink& link) const
{
    for (const auto& leader: LeaderDisks) {
        if (leader.Link.Match(link)) {
            return leader;
        }
    }
    return std::nullopt;
}

void TVolumeState::AddOrUpdateLeader(TLeaderDiskInfo leader)
{
    for (auto& leaderInfo: LeaderDisks) {
        if (leaderInfo.Link.Match(leader.Link)) {
            leaderInfo = std::move(leader);
            return;
        }
    }
    LeaderDisks.push_back(std::move(leader));
}

void TVolumeState::RemoveLeader(const TLeaderFollowerLink& link)
{
    EraseIf(
        LeaderDisks,
        [&](const TLeaderDiskInfo& leader) { return leader.Link.Match(link); });
}

const TLeaderDisks& TVolumeState::GetAllLeaders() const
{
    return LeaderDisks;
}

void TVolumeState::UpdateScrubberCounters(TScrubbingInfo counters)
{
    ScrubbingInfo.FullScanCount +=
        ScrubbingInfo.CurrentRange.Start > counters.CurrentRange.Start ? 1 : 0;
    ScrubbingInfo.Running = counters.Running;
    ScrubbingInfo.CurrentRange = counters.CurrentRange;
    ScrubbingInfo.Minors.insert(counters.Minors.begin(), counters.Minors.end());
    ScrubbingInfo.Majors.insert(counters.Majors.begin(), counters.Majors.end());
    ScrubbingInfo.Fixed.insert(counters.Fixed.begin(), counters.Fixed.end());
    ScrubbingInfo.FixedPartial.insert(
        counters.FixedPartial.begin(),
        counters.FixedPartial.end());
}

bool TVolumeState::CanPreemptClient(
    const TString& oldClientId,
    TInstant referenceTimestamp,
    ui64 newClientMountSeqNumber)
{
    return
        IsClientStale(oldClientId, referenceTimestamp) ||
            newClientMountSeqNumber > MountSeqNumber;
}

bool TVolumeState::CanAcceptClient(
    ui64 newFillSeqNumber,
    ui64 proposedFillGeneration)
{
    if (proposedFillGeneration == 0) {
        // TODO: NBS-4425: do not accept client with zero fillGeneration if fill
        // is not finished.
        return true;
    }

    if (Meta.GetVolumeConfig().GetIsFillFinished()) {
        return false;
    }

    if (proposedFillGeneration > 0 &&
            proposedFillGeneration != Meta.GetVolumeConfig().GetFillGeneration()) {
        return false;
    }

    return newFillSeqNumber >= Meta.GetFillSeqNumber();
}

bool TVolumeState::ShouldForceTabletRestart(
    const NProto::TVolumeClientInfo& info) const
{
    return info.GetMountSeqNumber() != MountSeqNumber ||
           info.GetFillSeqNumber() != Meta.GetFillSeqNumber();
}

////////////////////////////////////////////////////////////////////////////////

THistoryLogItem TVolumeState::LogAddClient(
    TInstant timestamp,
    const NProto::TVolumeClientInfo& add,
    const NProto::TError& error,
    const TActorId& pipeServer,
    const TActorId& senderId)
{
    THistoryLogItem res;
    res.Operation.SetTabletHost(FQDNHostName());
    res.Key = MountHistory.AllocateHistoryLogKey(timestamp);

    NProto::TVolumeOperation& op = res.Operation;
    *op.MutableAdd() = add;
    *op.MutableError() = error;
    auto& requesterInfo = *op.MutableRequesterInfo();
    requesterInfo.SetLocalPipeServerId(ToString(pipeServer));
    requesterInfo.SetSenderActorId(ToString(senderId));
    MountHistory.AddHistoryLogItem(res.Key, std::move(op));
    return res;
}

THistoryLogItem TVolumeState::LogRemoveClient(
    TInstant timestamp,
    const TString& clientId,
    const TString& reason,
    const NProto::TError& error)
{
    THistoryLogItem res;
    res.Operation.SetTabletHost(FQDNHostName());
    res.Key = MountHistory.AllocateHistoryLogKey(timestamp);

    NProto::TVolumeOperation& op = res.Operation;
    NProto::TRemoveClientOperation removeInfo;
    removeInfo.SetClientId(clientId);
    removeInfo.SetReason(reason);
    *op.MutableRemove() = removeInfo;
    *op.MutableError() = error;
    MountHistory.AddHistoryLogItem(res.Key, std::move(op));
    return res;
}

////////////////////////////////////////////////////////////////////////////////

EPublishingPolicy TVolumeState::CountersPolicy() const
{
    return IsDiskRegistryMediaKind() ? EPublishingPolicy::DiskRegistryBased
                                     : EPublishingPolicy::Repl;
}

TPartitionStatInfo& TVolumeState::CreatePartitionStatInfo(
    const TString& diskId,
    ui64 tabletId)
{
    PartitionStatInfos.push_back(TPartitionStatInfo(
        diskId,
        tabletId,
        CountersPolicy(),
        DiagnosticsConfig->GetHistogramCounterOptions()));
    return PartitionStatInfos.back();
}

TPartitionStatInfo* TVolumeState::GetPartitionStatInfoByTabletId(ui64 tabletId)
{
    if (IsDiskRegistryMediaKind()) {
        Y_DEBUG_ABORT_UNLESS(tabletId == 0);
        return &PartitionStatInfos.front();
    }

    for (auto& statInfo: PartitionStatInfos) {
        if (statInfo.TabletId == tabletId) {
            return &statInfo;
        }
    }
    return nullptr;
}

TPartitionStatInfo*
TVolumeState::GetPartitionStatByDiskId(const TString& diskId)
{
    for (auto& statInfo: PartitionStatInfos) {
        if (statInfo.DiskId == diskId) {
            return &statInfo;
        }
    }

    for (const auto& [checkpointId, checkpointInfo]:
         GetCheckpointStore().GetActiveCheckpoints())
    {
        if (checkpointInfo.ShadowDiskId == diskId) {
            return &CreatePartitionStatInfo(diskId, 0);
        }
    }

    return nullptr;
}

bool TVolumeState::GetMuteIOErrors() const
{
    return Meta.GetMuteIOErrors();
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeState::SetCheckpointRequestFinished(
    const TCheckpointRequest& request,
    bool completed,
    TString shadowDiskId,
    EShadowDiskState shadowDiskState)
{
    GetCheckpointStore().SetCheckpointRequestFinished(
        request.RequestId,
        completed,
        std::move(shadowDiskId),
        shadowDiskState);
    if (GetCheckpointStore().GetLightCheckpoints().empty()) {
        StopCheckpointLight();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeState::StartCheckpointLight()
{
    if (!CheckpointLight) {
        CheckpointLight = std::make_unique<TCheckpointLight>(BlockCount);
    }
}

void TVolumeState::CreateCheckpointLight(TString checkpointId)
{
    if (!CheckpointLight) {
        StartCheckpointLight();
    }
    CheckpointLight->CreateCheckpoint(checkpointId);
}

void TVolumeState::DeleteCheckpointLight(TString checkpointId)
{
    if (!CheckpointLight) {
        return;
    }
    CheckpointLight->DeleteCheckpoint(checkpointId);
}

void TVolumeState::StopCheckpointLight()
{
    CheckpointLight.reset();
}

bool TVolumeState::HasCheckpointLight() const
{
    return CheckpointLight.get();
}

NProto::TError TVolumeState::FindDirtyBlocksBetweenLightCheckpoints(
    TString lowCheckpointId,
    TString highCheckpointId,
    const TBlockRange64& blockRange,
    TString* mask) const
{
    if (!CheckpointLight) {
        return MakeError(E_PRECONDITION_FAILED, "Light checkpoint is disabled");
    }

    return CheckpointLight->FindDirtyBlocksBetweenCheckpoints(
        std::move(lowCheckpointId),
        std::move(highCheckpointId),
        blockRange,
        mask);
}

void TVolumeState::MarkBlocksAsDirtyInCheckpointLight(const TBlockRange64& blockRange)
{
    if (!CheckpointLight) {
        return;
    }
    CheckpointLight->Set(blockRange);
}

}   // namespace NCloud::NBlockStore::NStorage
