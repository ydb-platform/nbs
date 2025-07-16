#include "restore_validator_actor.h"

#include <cloud/blockstore/libs/storage/disk_registry/model/user_notification.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/hfunc.h>

#include <util/generic/algorithm.h>

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NCloud::NBlockStore::NStorage::TDirtyDevice>(
    IOutputStream& o,
    const NCloud::NBlockStore::NStorage::TDirtyDevice& d)
{
    o << "dirty device { " << d.DiskId << " " << d.Id << " }";
}

template <>
void Out<NCloud::NBlockStore::NStorage::TAutomaticallyReplacedDeviceInfo>(
    IOutputStream& o,
    const NCloud::NBlockStore::NStorage::TAutomaticallyReplacedDeviceInfo& info)
{
    o << "replaced device { " << info.DeviceId << " }";
}

template <>
void Out<NCloud::NBlockStore::NStorage::TBrokenDiskInfo>(
    IOutputStream& o,
    const NCloud::NBlockStore::NStorage::TBrokenDiskInfo& info)
{
    o << "broken disk { " << info.DiskId << " }";
}

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore::NStorage::NDiskRegistry {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf RESTORE_PREFIX = "[Restore]";

bool NormalizeLoadState(
    const NActors::TActorContext& ctx,
    TDiskRegistryStateSnapshot& state,
    int component)
{
    auto sortAndTestUnique = [&ctx, component] (auto& container, auto&& getKeyFunction) {
        SortBy(container, getKeyFunction);

        auto itr = std::adjacent_find(
            container.begin(),
            container.end(),
            [&] (auto&& left, auto&& right) {
                return getKeyFunction(left) == getKeyFunction(right);
            });

        if (itr != container.end()) {
            LOG_WARN_S(
                ctx,
                component,
                RESTORE_PREFIX << " Not unique: " << *itr);
            return false;
        }

        return true;
    };

    auto sortAndTestUniqueCmp3Way =
        [&ctx, component] (auto& container, auto&& cmp3Way) {
            Sort(container, [&cmp3Way] (const auto& left, const auto& right) {
                return cmp3Way(left, right) < 0;
            });

            auto itr = std::adjacent_find(
                container.begin(),
                container.end(),
                [&cmp3Way] (const auto& left, const auto& right) {
                    return cmp3Way(left, right) == 0;
                });

            if (itr != container.end()) {
                LOG_WARN_S(
                    ctx,
                    component,
                    RESTORE_PREFIX << " Not unique: " << *itr);
                return false;
            }

            return true;
        };

    // if new fields are added to TDiskRegistryStateSnapshot
    // there will be a compilation error.
    auto& [
        config,
        dirtyDevices,
        agents,
        disks,
        placementGroups,
        brokenDisks,
        disksToReallocate,
        diskStateChanges,
        lastDiskStateSeqNo,
        diskAllocationAllowed,
        disksToCleanup,
        errorNotifications,
        userNotifications,
        outdatedVolumeConfigs,
        suspendedDevices,
        automaticallyReplacedDevices,
        diskRegistryAgentListParams,
        replicasWithRecentlyReplacedDevices
    ] = state;

    Y_UNUSED(lastDiskStateSeqNo);
    Y_UNUSED(diskAllocationAllowed);
    Y_UNUSED(diskStateChanges);
    Y_UNUSED(diskRegistryAgentListParams);

    bool result = true;

    result &= sortAndTestUnique(
        *config.MutableKnownAgents(),
        [] (const auto& config) {
            return config.GetAgentId();
        });

    for (auto& agent: *state.Config.MutableKnownAgents()) {
        result &= sortAndTestUnique(
            *agent.MutableDevices(),
            [] (const auto& config) {
                return config.GetDeviceUUID();
            });
    }

    result &= sortAndTestUnique(
        dirtyDevices,
        [] (const auto& device) {
            return device.Id;
        });

    result &= sortAndTestUnique(
        agents,
        [] (const auto& config) {
            return config.GetAgentId();
        });

    for (auto& agent: agents) {
        result &= sortAndTestUnique(
            *agent.MutableDevices(),
            [] (const auto& config) {
                return config.GetDeviceUUID();
            });
    }

    result &= sortAndTestUnique(
        disks,
        [] (const auto& config) {
            return config.GetDiskId();
        });

    result &= sortAndTestUnique(
        placementGroups,
        [] (const auto& config) {
            return config.GetGroupId();
        });

    result &= sortAndTestUnique(
        brokenDisks,
        [] (const auto& disk) {
            return disk.DiskId;
        });

    result &= sortAndTestUnique(
        disksToReallocate,
        [] (const auto& disk) {
            return disk;
        });

    result &= sortAndTestUnique(
        disksToCleanup,
        [] (const auto& disk) {
            return disk;
        });

    result &= sortAndTestUnique(
        errorNotifications,
        [] (const auto& disk) {
            return disk;
        });

    result &= sortAndTestUniqueCmp3Way(
        userNotifications,
        [] (const auto& l, const auto& r) {
            if (l.GetSeqNo() < r.GetSeqNo()) {
                return -1;
            }

            if (l.GetSeqNo() > r.GetSeqNo()) {
                return 1;
            }

            return GetEntityId(l).compare(GetEntityId(r));
        });

    result &= sortAndTestUnique(
        outdatedVolumeConfigs,
        [] (const auto& disk) {
            return disk;
        });

    result &= sortAndTestUnique(
        suspendedDevices,
        [] (const auto& device) {
            return device.GetId();
        });

    result &= sortAndTestUnique(
        automaticallyReplacedDevices,
        [] (const auto& device) {
            return device.DeviceId;
        });

    return result;
}

TStringBuf NormalizeMirrorId(TStringBuf diskId) {
    return diskId.substr(0, diskId.find('/'));
}

bool CheckMirrorDiskId(
    const TSet<TString>& disksInSS,
    const TVector<NProto::TDiskConfig>& disksInBackup,
    const NProto::TDiskConfig& disk)
{
    const TString& diskId = disk.GetDiskId();
    const TStringBuf baseDiskId = NormalizeMirrorId(diskId);

    if (baseDiskId == diskId) {
        return false;
    }

    if (!disksInSS.contains(baseDiskId)) {
        return false;
    }

    auto itr = FindIf(
        disksInBackup,
        [&baseDiskId] (const NProto::TDiskConfig& config) {
            return config.GetDiskId() == baseDiskId;
        });

    if (itr == disksInBackup.end()) {
        return false;
    }

    return itr->GetCloudId() == disk.GetCloudId()
        && itr->GetFolderId() == disk.GetFolderId()
        && itr->GetBlockSize() == disk.GetBlockSize();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TRestoreValidationActor::TRestoreValidationActor(
        NActors::TActorId owner,
        TRequestInfoPtr request,
        int component,
        TDiskRegistryStateSnapshot snapshot)
    : Owner(std::move(owner))
    , Request(std::move(request))
    , Component(component)
    , ValidSnapshot(std::move(snapshot))
    , DisksWaitedConfirmSize(0)
{
    for (auto& disk: ValidSnapshot.Disks) {
        DisksInBackup.insert(disk.GetDiskId());
    }

    for (auto& agent: ValidSnapshot.Agents) {
        for (auto& device: *agent.MutableDevices()) {
            DevicesInBackup[device.GetDeviceUUID()] = &device;
        }
    }

    Validators.push(&TRestoreValidationActor::StartDisksCheck);
    Validators.push(&TRestoreValidationActor::StartDeviceCheck);
    Validators.push(&TRestoreValidationActor::StartPlacementGroupsCheck);
}

void TRestoreValidationActor::Bootstrap(const NActors::TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (ValidSnapshot.Disks.empty()) {
        LOG_WARN_S(
            ctx,
            Component,
            RESTORE_PREFIX << " No disk's to restore");
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "No disk's to restore"));
        return;
    }

    if (!NormalizeLoadState(ctx, ValidSnapshot, Component)) {
        LOG_WARN_S(
            ctx,
            Component,
            RESTORE_PREFIX << " Normalize new state error");
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Normalize new state error"));
        return;
    }

    NextValidation(ctx);
}

void TRestoreValidationActor::ReplyAndDie(
    const NActors::TActorContext& ctx,
    NProto::TError error)
{
    using TValidationResponse =
        TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse;

    if (HasError(error)) {
        NCloud::Send(
            ctx,
            Owner,
            std::make_unique<TValidationResponse>(
                std::move(error),
                Request));
    } else {
        NCloud::Send(
            ctx,
            Owner,
            std::make_unique<TValidationResponse>(
                std::move(error),
                Request,
                std::move(ValidSnapshot)));
    }

    Die(ctx);
}

template <typename Container>
void TRestoreValidationActor::SetErrorDevicesInBackup(
    const Container& deviceIds,
    TInstant now)
{
    for (const auto& deviceId: deviceIds) {
        SetErrorDeviceInBackup(deviceId, now);
    }
}

void TRestoreValidationActor::SetErrorDeviceInBackup(
    const TString& deviceId,
    TInstant now)
{
    auto itr = DevicesInBackup.find(deviceId);
    if (itr != DevicesInBackup.end()) {
        itr->second->SetState(NProto::DEVICE_STATE_ERROR);
        itr->second->SetStateTs(now.MicroSeconds());
        itr->second->SetStateMessage("Restore error");
    }
}

void TRestoreValidationActor::EraseDiskFromBackup(
    const TString& diskId,
    TInstant now)
{
    DisksInSS.erase(diskId);
    std::erase_if(
        ValidSnapshot.Disks,
        [&diskId, now, this] (auto& config) {
            if (NormalizeMirrorId(config.GetDiskId()) == diskId) {
                DisksInBackup.erase(config.GetDiskId());
                SetErrorDevicesInBackup(config.GetDeviceUUIDs(), now);
                return true;
            }
            return false;
        });
}

void TRestoreValidationActor::NextValidation(const NActors::TActorContext& ctx)
{
    if (Validators.empty()) {
        ReplyAndDie(ctx, MakeError(S_OK));
        return;
    }

    auto validator = Validators.front();
    Validators.pop();
    return (this->*validator)(ctx);
}

void TRestoreValidationActor::StartDisksCheck(
    const NActors::TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        MakeStorageServiceId(),
        std::make_unique<TEvService::TEvListVolumesRequest>());
}

void TRestoreValidationActor::StartPlacementGroupsCheck(
    const NActors::TActorContext& ctx)
{
    EraseIf(ValidSnapshot.PlacementGroups, [&ctx, this] (auto& pg) {
        bool diskIsRemoved = false;
        EraseIf(
            *pg.MutableDisks(),
            [&pg, &ctx, &diskIsRemoved, this] (const auto& disk)
        {
            if (!DisksInBackup.contains(NormalizeMirrorId(disk.GetDiskId()))) {
                LOG_WARN_S(
                    ctx,
                    Component,
                    RESTORE_PREFIX
                        << " DiskID " << disk.GetDiskId().Quote()
                        << " removed from PG " << pg.GetGroupId().Quote());
                diskIsRemoved = true;
                return true;
            }
            return false;
        });
        if (pg.GetDisks().size() == 0) {
            LOG_WARN_S(
                ctx,
                Component,
                RESTORE_PREFIX
                    << " PG " << pg.GetGroupId().Quote() << " is empty");
            return diskIsRemoved;
        }
        return false;
    });

    NextValidation(ctx);
}

void TRestoreValidationActor::StartDeviceCheck(const NActors::TActorContext& ctx)
{
    DisksWaitedConfirmSize = 0;
    for (auto& diskId: DisksInSS) {
        auto request = std::make_unique<TEvVolume::TEvGetVolumeInfoRequest>();
        request->Record.SetDiskId(diskId);
        NCloud::Send(
            ctx,
            MakeVolumeProxyServiceId(),
            std::move(request));
        ++DisksWaitedConfirmSize;
    }
}


bool TRestoreValidationActor::CompareDiskConfigs(
    const NActors::TActorContext& ctx,
    const NKikimrBlockStore::TVolumeConfig& ssConfig,
    const NProto::TDiskConfig& backupConfig)
{
    if (ssConfig.GetBlockSize() != backupConfig.GetBlockSize()) {
        LOG_WARN_S(
            ctx,
            Component,
            RESTORE_PREFIX
                << " DiskID " << ssConfig.GetDiskId().Quote()
                << " block size don't equal. "
                << "SS: " << ssConfig.GetBlockSize()
                << " backup: " << backupConfig.GetBlockSize());
        return false;
    }

    if (ssConfig.GetCloudId() != backupConfig.GetCloudId()) {
        LOG_WARN_S(
            ctx,
            Component,
            RESTORE_PREFIX
                << " DiskID " << ssConfig.GetDiskId().Quote()
                << " cloud id don't equal. "
                << "SS: " << ssConfig.GetCloudId().Quote()
                << " backup: " << backupConfig.GetCloudId().Quote());
        return false;
    }

    if (ssConfig.GetFolderId() != backupConfig.GetFolderId()) {
        LOG_WARN_S(
            ctx,
            Component,
            RESTORE_PREFIX
                << " DiskID " << ssConfig.GetDiskId().Quote()
                << " folder id don't equal. "
                << "SS: " << ssConfig.GetFolderId().Quote()
                << " backup: " << backupConfig.GetFolderId().Quote());
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TRestoreValidationActor::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "Poison Pill"));
}

void TRestoreValidationActor::HandleListVolumesResponse(
    const TEvService::TEvListVolumesResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    DisksInSS = TSet<TString>(
        ev->Get()->Record.GetVolumes().begin(),
        ev->Get()->Record.GetVolumes().end());

    for (auto itr = ValidSnapshot.Disks.begin();
        itr != ValidSnapshot.Disks.end(); )
    {
        if (!DisksInSS.contains(itr->GetDiskId())
            && !CheckMirrorDiskId(
                DisksInSS,
                ValidSnapshot.Disks,
                *itr)
            && !FindPtr(
                ValidSnapshot.DisksToCleanup,
                itr->GetDiskId())
            && !FindPtr(
                ValidSnapshot.DisksToCleanup,
                NormalizeMirrorId(itr->GetDiskId())))
        {
            const bool isShadowDisk =
                !itr->GetCheckpointReplica().GetCheckpointId().empty();
            if (isShadowDisk) {
                LOG_WARN_S(
                    ctx,
                    Component,
                    RESTORE_PREFIX << " ShadowDisk " << itr->GetDiskId().Quote()
                                   << " is found in backup");
            } else {
                LOG_WARN_S(
                    ctx,
                    Component,
                    RESTORE_PREFIX << " DiskID " << itr->GetDiskId().Quote()
                                   << " is found in backup, but not in SS");
            }
            SetErrorDevicesInBackup(itr->GetDeviceUUIDs(), ctx.Now());
            DisksInBackup.erase(itr->GetDiskId());
            itr = ValidSnapshot.Disks.erase(itr);
        } else {
            ++itr;
        }
    }

    for (auto& diskId: DisksInSS) {
        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(diskId));
        ++DisksWaitedConfirmSize;
    }

    if (ValidSnapshot.Disks.size() == 0) {
        ReplyAndDie(ctx,
            MakeError(E_REJECTED, "Zero count of disks for restore"));
    }
}

void TRestoreValidationActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    --DisksWaitedConfirmSize;

    auto nextStepIfComplete = [&ctx, this] () {
        if (ValidSnapshot.Disks.size() == 0) {
            ReplyAndDie(ctx,
                MakeError(E_REJECTED, "Zero count of disks for restore"));
        } else if (DisksWaitedConfirmSize == 0) {
            NextValidation(ctx);
        }};

    auto* msg = ev->Get();
    auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            Component,
            RESTORE_PREFIX
                << " Failed to get disk config from SS: " << FormatError(error));
        ReplyAndDie(ctx, MakeError(E_REJECTED, "Failed to get disk config from SS"));
        return;
    }

    const auto& description = msg->PathDescription;
    const auto& config = description.GetBlockStoreVolumeDescription().GetVolumeConfig();

    const auto& diskId = config.GetDiskId();
    for (const auto& partition: config.GetPartitions()) {
        if (partition.GetType() != NKikimrBlockStore::EPartitionType::NonReplicated) {
            DisksInSS.erase(diskId);
            return nextStepIfComplete();
        }
    }

    if (!DisksInBackup.contains(diskId)) {
        LOG_WARN_S(
            ctx,
            Component,
            RESTORE_PREFIX
                << " DiskID " << diskId.Quote()
                << " is found in SS but not in backup");
        return nextStepIfComplete();
    }

    if (!CompareDiskConfigs(
        ctx,
        config,
        *FindIf(
            ValidSnapshot.Disks,
            [&diskId] (const auto& disk) { return disk.GetDiskId() == diskId; })))
    {
        EraseDiskFromBackup(diskId, ctx.Now());
    }

    nextStepIfComplete();
}

void TRestoreValidationActor::HandleGetVolumeInfoResponse(
    const TEvVolume::TEvGetVolumeInfoResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    --DisksWaitedConfirmSize;

    auto* msg = ev->Get();
    auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            Component,
            RESTORE_PREFIX
                << " Failed to get volume info: " << FormatError(error));
        ReplyAndDie(ctx, MakeError(E_REJECTED, "Failed to get volume info"));
        return;
    }

    const auto& volumeInfo = msg->Record.GetVolume();
    const auto& diskId = volumeInfo.GetDiskId();

    TSet<TString> deviceIds;
    for (const auto& device: volumeInfo.GetDevices()) {
        deviceIds.insert(device.GetDeviceUUID());
    }
    for (const auto& replica: volumeInfo.GetReplicas()) {
        for (const auto& device: replica.GetDevices()) {
            deviceIds.insert(device.GetDeviceUUID());
        }
    }

    bool removeDisk = false;
    for (auto diskConfigItr = ValidSnapshot.Disks.begin();
        diskConfigItr != ValidSnapshot.Disks.end();
        ++diskConfigItr)
    {
        if (NormalizeMirrorId(diskConfigItr->GetDiskId()) == diskId) {
            for (auto& deviceUUID: diskConfigItr->GetDeviceUUIDs()) {
                auto deviceIt = deviceIds.find(deviceUUID);
                if (deviceIt == deviceIds.end()) {
                    LOG_WARN_S(
                        ctx,
                        Component,
                        RESTORE_PREFIX
                            << " Device " << deviceUUID.Quote()
                            << " for disk " << diskConfigItr->GetDiskId().Quote()
                            << " not found in volume");
                    removeDisk = true;
                } else {
                    deviceIds.erase(deviceIt);
                }
            }
        }
    }

    for (auto& deviceId: deviceIds) {
        LOG_WARN_S(
            ctx,
            Component,
            RESTORE_PREFIX
                << " Device " << deviceId.Quote()
                << " for disk " << diskId.Quote()
                << " not found in backup");
        removeDisk = true;
    }

    if (removeDisk) {
        SetErrorDevicesInBackup(deviceIds, ctx.Now());
        EraseDiskFromBackup(diskId, ctx.Now());
    }

    if (ValidSnapshot.Disks.size() == 0) {
        ReplyAndDie(ctx,
            MakeError(E_REJECTED, "Zero count of disks for restore"));
    } else if (DisksWaitedConfirmSize == 0) {
        NextValidation(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TRestoreValidationActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {

        HFunc(
            NActors::TEvents::TEvPoisonPill,
            HandlePoisonPill)
        HFunc(
            TEvService::TEvListVolumesResponse,
            HandleListVolumesResponse)
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse)
        HFunc(
            TEvVolume::TEvGetVolumeInfoResponse,
            HandleGetVolumeInfoResponse)

        default:
            HandleUnexpectedEvent(ev, Component, __PRETTY_FUNCTION__);
            break;
    }
}

}   // NCloud::NBlockStore::NStorage::NDiskRegistry
