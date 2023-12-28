#include "volume_state.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/stream/str.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TString TPartitionInfo::GetStatus() const
{
    TStringStream out;

    switch (State) {
        default:
        case UNKNOWN:
            out << "UNKNOWN";
            break;
        case STOPPED:
            out << "STOPPED";
            break;
        case STARTED:
            out << "STARTED";
            break;
        case FAILED:
            out << "FAILED";
            break;
        case READY:
            out << "READY";
            break;
    }

    if (Message) {
        out << ": " << Message;
    }

    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

bool THistoryLogKey::operator == (const THistoryLogKey& rhs) const
{
    return (Timestamp == rhs.Timestamp) && (Seqno == rhs.Seqno);
}

bool THistoryLogKey::operator != (const THistoryLogKey& rhs) const
{
    return !(*this == rhs);
}

bool THistoryLogKey::operator < (THistoryLogKey rhs) const
{
    if (Timestamp < rhs.Timestamp) {
        return true;
    }
    if (Timestamp == rhs.Timestamp) {
        return Seqno < rhs.Seqno;
    }
    return false;
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

TVolumeState::TVolumeState(
        TStorageConfigPtr storageConfig,
        NProto::TVolumeMeta meta,
        TVector<TVolumeMetaHistoryItem> metaHistory,
        TVector<TRuntimeVolumeParamsValue> volumeParams,
        TThrottlerConfig throttlerConfig,
        THashMap<TString, TVolumeClientState> infos,
        TDeque<THistoryLogItem> history,
        TVector<TCheckpointRequest> checkpointRequests,
        bool startPartitionsNeeded)
    : StorageConfig(std::move(storageConfig))
    , Meta(std::move(meta))
    , MetaHistory(std::move(metaHistory))
    , Config(&Meta.GetConfig())
    , VolumeParams(std::move(volumeParams))
    , ClientInfosByClientId(std::move(infos))
    , ThrottlerConfig(std::move(throttlerConfig))
    , ThrottlingPolicy(Config->GetPerformanceProfile(), ThrottlerConfig)
    , History(std::move(history))
    , CheckpointStore(std::move(checkpointRequests), Config->GetDiskId())
    , StartPartitionsNeeded(startPartitionsNeeded)
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

void TVolumeState::Reset()
{
    Partitions.clear();
    PartitionStatInfos.clear();
    PartitionsState = TPartitionInfo::UNKNOWN;
    ForceRepair = false;
    RejectWrite = false;
    TrackUsedBlocks = false;
    MaskUnusedBlocks = false;
    MaxTimedOutDeviceStateDuration = TDuration::Zero();
    UseRdma = StorageConfig->GetUseRdma()
        || StorageConfig->IsUseRdmaFeatureEnabled(
            Meta.GetConfig().GetCloudId(),
            Meta.GetConfig().GetFolderId());
    UseRdmaForThisVolume = false;
    AcceptInvalidDiskAllocationResponse = false;

    if (IsDiskRegistryMediaKind(Config->GetStorageMediaKind())) {
        if (Meta.GetDevices().size()) {
            PartitionStatInfos.emplace_back(
                EPublishingPolicy::DiskRegistryBased);
        }
        const auto& encryptionDesc = Meta.GetVolumeConfig().GetEncryptionDesc();
        if (encryptionDesc.GetMode() != NProto::NO_ENCRYPTION) {
            TrackUsedBlocks = true;
        }
    } else {
        for (ui64 tabletId: Meta.GetPartitions()) {
            Partitions.emplace_back(
                tabletId,
                Meta.GetConfig(),
                StorageConfig->GetTabletRebootCoolDownIncrement(),
                StorageConfig->GetTabletRebootCoolDownMax());
            PartitionStatInfos.emplace_back(EPublishingPolicy::Repl);
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
            MaskUnusedBlocks = true;
        } else if (tag == "use-rdma") {
            UseRdma = true;
            UseRdmaForThisVolume = true;
        } else if (tag == "max-timed-out-device-state-duration") {
            TDuration::TryParse(value, MaxTimedOutDeviceStateDuration);
        }
    }

    UseMirrorResync = StorageConfig->GetUseMirrorResync();
    ForceMirrorResync = StorageConfig->GetForceMirrorResync();
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

bool TVolumeState::FindPartitionIndex(ui64 tabletId, ui32& index) const
{
    for (ui32 i = 0; i < Partitions.size(); ++i) {
        if (Partitions[i].TabletId == tabletId) {
            index = i;
            return true;
        }
    }
    return false;
}

void TVolumeState::SetPartitionsState(TPartitionInfo::EState state)
{
    for (auto& partition: Partitions) {
        partition.State = state;
    }
}

TPartitionInfo::EState TVolumeState::UpdatePartitionsState()
{
    if (IsDiskRegistryMediaKind(Config->GetStorageMediaKind())) {
        ui64 bytes = 0;
        for (const auto& device: Meta.GetDevices()) {
            bytes += device.GetBlocksCount() * device.GetBlockSize();
        }

        const bool allocated =
            bytes >= Config->GetBlockSize() * Config->GetBlocksCount();
        const bool actorStarted = !!DiskRegistryBasedPartitionActor;
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
                        << "Volume already has connection with read-write access: "
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
                        << "Volume already has connection with local mount: "
                        << LocalMountClientId);
                return res;
        }

        if (!res.RemovedClientIds || (LocalMountClientId != rwClientId)) {
            res.RemovedClientIds.emplace_back(std::move(LocalMountClientId));
        }
    }

    if (readWriteAccess) {
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
    res.Key = AllocateHistoryLogKey(timestamp);

    NProto::TVolumeOperation& op = res.Operation;
    *op.MutableAdd() = add;
    *op.MutableError() = error;
    auto& requesterInfo = *op.MutableRequesterInfo();
    requesterInfo.SetLocalPipeServerId(ToString(pipeServer));
    requesterInfo.SetSenderActorId(ToString(senderId));

    History.emplace_front(res.Key, op);
    if (History.size() > StorageConfig->GetVolumeHistoryCacheSize()) {
        History.pop_back();
    }
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
    res.Key = AllocateHistoryLogKey(timestamp);

    NProto::TVolumeOperation& op = res.Operation;
    NProto::TRemoveClientOperation removeInfo;
    removeInfo.SetClientId(clientId);
    removeInfo.SetReason(reason);
    *op.MutableRemove() = removeInfo;
    *op.MutableError() = error;
    History.emplace_front(res.Key, op);
    if (History.size() > StorageConfig->GetVolumeHistoryCacheSize()) {
        History.pop_back();
    }
    return res;
}

THistoryLogKey TVolumeState::AllocateHistoryLogKey(TInstant timestamp)
{
    if (LastLogRecord.Timestamp != timestamp) {
        LastLogRecord.Timestamp = timestamp;
        LastLogRecord.Seqno = 0;
    } else {
        ++LastLogRecord.Seqno;
    }
    return LastLogRecord;
}

void TVolumeState::CleanupHistoryIfNeeded(TInstant oldest)
{
    while (History.size() && (History.back().Key.Timestamp > oldest)) {
        History.pop_back();
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeState::FindPartitionStatInfoByOwner(
    const TActorId& actorId,
    ui32& index) const
{
    for (ui32 i = 0; i < PartitionStatInfos.size(); ++i) {
        if (PartitionStatInfos[i].Owner == actorId) {
            index = i;
            return true;
        }
    }
    return false;
}

TPartitionStatInfo* TVolumeState::GetPartitionStatInfoById(ui64 id)
{
    if (IsDiskRegistryMediaKind(Config->GetStorageMediaKind())) {
        if (id >= PartitionStatInfos.size()) {
            return nullptr;
        }
        return &PartitionStatInfos[id];
    } else {
        ui32 index = 0;
        if (FindPartitionIndex(id, index)) {
            return &PartitionStatInfos[index];
        };
        return nullptr;
    }
}

bool TVolumeState::SetPartitionStatActor(ui64 id, const TActorId& actor)
{
    ui32 index = 0;
    if (FindPartitionIndex(id, index)) {
        PartitionStatInfos[index].Owner = actor;
        return true;
    };
    return false;
}

bool TVolumeState::GetMuteIOErrors() const
{
    return Meta.GetMuteIOErrors();
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeState::SetCheckpointRequestFinished(
    const TCheckpointRequest& request,
    bool success,
    TString shadowDiskId)
{
    GetCheckpointStore().SetCheckpointRequestFinished(
        request.RequestId,
        success,
        std::move(shadowDiskId));
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
