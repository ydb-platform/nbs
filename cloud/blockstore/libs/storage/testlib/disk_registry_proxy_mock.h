#pragma once

#include "test_env_state.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_private.h>

#include <contrib/ydb/core/mind/local.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultDeviceBlockSize = 512;
constexpr ui64 DefaultDeviceBlockCount = 1024 * 256;  // = 128MiB

class TDiskRegistryProxyMock final
    : public NActors::TActor<TDiskRegistryProxyMock>
{
private:
    NProto::TDiskRegistryConfig Config;

    TDiskRegistryStatePtr State;

public:
    TDiskRegistryProxyMock(TDiskRegistryStatePtr state)
        : TActor(&TThis::StateWork)
        , State(std::move(state))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            // alloc/dealloc
            HFunc(TEvDiskRegistry::TEvAllocateDiskRequest, HandleAllocateDisk);
            HFunc(
                TEvDiskRegistry::TEvDeallocateDiskRequest,
                HandleDeallocateDisk);
            HFunc(
                TEvDiskRegistry::TEvMarkDiskForCleanupRequest,
                HandleMarkDiskForCleanup);

            // migration
            HFunc(
                TEvDiskRegistry::TEvFinishMigrationRequest,
                HandleFinishMigration);

            // acquire/release
            HFunc(TEvDiskRegistry::TEvAcquireDiskRequest, HandleAcquireDisk);
            HFunc(TEvDiskRegistry::TEvReleaseDiskRequest, HandleReleaseDisk);

            // config
            HFunc(TEvDiskRegistry::TEvUpdateConfigRequest, HandleUpdateConfig);
            HFunc(
                TEvDiskRegistry::TEvDescribeConfigRequest,
                HandleDescribeConfig);

            HFunc(
                TEvDiskRegistry::TEvSetWritableStateRequest,
                HandleSetWritableState);

            HFunc(
                TEvDiskRegistry::TEvBackupDiskRegistryStateRequest,
                HandleBackupDiskRegistryState);

            // states
            HFunc(
                TEvDiskRegistry::TEvChangeDeviceStateRequest,
                HandleChangeDeviceState);
            HFunc(
                TEvDiskRegistry::TEvChangeAgentStateRequest,
                HandleChangeAgentState);

            // placement groups
            HFunc(
                TEvService::TEvCreatePlacementGroupRequest,
                HandleCreatePlacementGroup);
            HFunc(
                TEvDiskRegistry::TEvUpdatePlacementGroupSettingsRequest,
                HandleUpdatePlacementGroupSettings);
            HFunc(
                TEvService::TEvDestroyPlacementGroupRequest,
                HandleDestroyPlacementGroup);
            HFunc(
                TEvService::TEvAlterPlacementGroupMembershipRequest,
                HandleAlterPlacementGroupMembership);
            HFunc(
                TEvService::TEvListPlacementGroupsRequest,
                HandleListPlacementGroups);
            HFunc(
                TEvService::TEvDescribePlacementGroupRequest,
                HandleDescribePlacementGroup);

            HFunc(TEvDiskRegistry::TEvDescribeDiskRequest, HandleDescribeDisk);
            HFunc(
                TEvDiskRegistry::TEvCreateDiskFromDevicesRequest,
                HandleCreateDiskFromDevices);

            HFunc(
                TEvDiskRegistry::TEvStartForceMigrationRequest,
                HandleStartForceMigration);

            HFunc(
                TEvService::TEvResumeDeviceRequest,
                HandleResumeDevice);

            HFunc(
                TEvDiskRegistry::TEvChangeDiskDeviceRequest,
                HandleChangeDiskDevice);

            HFunc(
                TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsRequest,
                HandleUpdateDiskRegistryAgentListParams);

            HFunc(
                TEvDiskRegistry::TEvGetDependentDisksRequest,
                HandleGetDependentDisks);

            HFunc(
                TEvDiskRegistry::TEvAllocateCheckpointRequest,
                HandleAllocateCheckpoint);

            HFunc(
                TEvDiskRegistry::TEvDeallocateCheckpointRequest,
                HandleDeallocateCheckpoint);

            IgnoreFunc(NKikimr::TEvLocal::TEvTabletMetrics);

            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_REGISTRY_PROXY);
        }
    }

    template <typename TDiskProto>
    void ToLogicalBlocks(const TDiskRegistryState::TDisk& disk, TDiskProto& proto)
    {
        for (const auto& device: disk.Devices) {
            auto& dst = *proto.AddDevices();
            dst = device;
            dst.SetBlocksCount(
                device.GetBlocksCount() * device.GetBlockSize() / disk.BlockSize);
            dst.SetBlockSize(disk.BlockSize);
        }

        for (const auto& x: disk.Migrations) {
            auto& migration = *proto.AddMigrations();
            auto& dst = *migration.MutableTargetDevice();
            dst = x.second;
            dst.SetBlocksCount(
                x.second.GetBlocksCount() * x.second.GetBlockSize() / disk.BlockSize);
            dst.SetBlockSize(disk.BlockSize);
            migration.SetSourceDeviceId(x.first);
        }

        for (const auto& replica: disk.Replicas) {
            auto& r = *proto.AddReplicas();
            for (const auto& device: replica) {
                auto& dst = *r.AddDevices();
                dst = device;
                dst.SetBlocksCount(
                    device.GetBlocksCount() * device.GetBlockSize() / disk.BlockSize);
                dst.SetBlockSize(disk.BlockSize);
            }
        }
    }

    void HandleAllocateDisk(
        const TEvDiskRegistry::TEvAllocateDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        auto response = std::make_unique<TEvDiskRegistry::TEvAllocateDiskResponse>();

        TDiskRegistryState::TPlacementGroup* group = nullptr;
        if (msg->Record.GetPlacementGroupId()) {
            group = State->PlacementGroups.FindPtr(msg->Record.GetPlacementGroupId());
            if (!group) {
                response->Record.MutableError()->CopyFrom(
                    MakeError(E_NOT_FOUND, "no such group")
                );
            }
        }

        if (response->Record.HasError()) {
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        if (FAILED(State->CurrentErrorCode)) {
            response->Record.MutableError()->CopyFrom(
                MakeError(State->CurrentErrorCode, "disk allocation has failed")
            );

            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        const auto& diskId = msg->Record.GetDiskId();
        auto& disk = State->Disks[diskId];

        disk.PoolName = msg->Record.GetPoolName();
        disk.MediaKind = msg->Record.GetStorageMediaKind();
        disk.Migrations.clear();
        ui64 bytes = (1 + State->ReplicaCount)
            * msg->Record.GetBlocksCount()
            * msg->Record.GetBlockSize();

        ui32 i = 0;
        while (bytes) {
            ui64 deviceBytes = 0;
            if (i < disk.Devices.size()) {
                deviceBytes = Min(bytes, disk.Devices[i].GetBlocksCount()
                    * disk.Devices[i].GetBlockSize());
            } else {
                if (State->NextDeviceIdx >= State->Devices.size()) {
                    break;
                }

                disk.Devices.push_back(
                    State->Devices[State->NextDeviceIdx++]);
                const auto& device = disk.Devices.back();
                deviceBytes = device.GetBlocksCount() * device.GetBlockSize();
            }

            disk.Replicas.resize(State->ReplicaCount);

            for (auto& replica: disk.Replicas) {
                if (i < replica.size()) {
                    deviceBytes += Min(bytes, replica[i].GetBlocksCount()
                        * replica[i].GetBlockSize());
                } else {
                    if (State->NextDeviceIdx >= State->Devices.size()) {
                        break;
                    }

                    replica.push_back(
                        State->Devices[State->NextDeviceIdx++]);
                    const auto& device = replica.back();
                    deviceBytes +=
                        device.GetBlocksCount() * device.GetBlockSize();
                }
            }

            if (State->MigrationMode != EMigrationMode::Disabled) {
                auto& device = disk.Devices[i];
                auto* mdevice = State->MigrationDevices.FindPtr(device.GetDeviceUUID());
                if (mdevice) {
                    if (State->MigrationMode == EMigrationMode::InProgress) {
                        disk.Migrations[device.GetDeviceUUID()] = *mdevice;
                    } else {
                        UNIT_ASSERT(State->MigrationMode == EMigrationMode::Finish);
                        device = *mdevice;
                    }
                }
            }

            bytes -= Min(bytes, deviceBytes);
            ++i;
        }

        if (bytes) {
            response->Record.MutableError()->CopyFrom(
                MakeError(E_BS_OUT_OF_SPACE, "not enough available devices")
            );
        } else {
            if (group) {
                group->DiskIds.insert(msg->Record.GetDiskId());
                ++group->ConfigVersion;
            }

            ToLogicalBlocks(disk, response->Record);
            response->Record.SetIOMode(disk.IOMode);
            response->Record.SetIOModeTs(disk.IOModeTs.MicroSeconds());
            response->Record.SetMuteIOErrors(disk.MuteIOErrors);
        }

        for (const auto& deviceId: State->DeviceReplacementUUIDs) {
            *response->Record.AddDeviceReplacementUUIDs() = deviceId;
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDeallocateDisk(
        const TEvDiskRegistry::TEvDeallocateDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        State->Disks.erase(ev->Get()->Record.GetDiskId());

        // TODO: remove disk from pg

        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvDiskRegistry::TEvDeallocateDiskResponse>());
    }

    void HandleMarkDiskForCleanup(
        const TEvDiskRegistry::TEvMarkDiskForCleanupRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        State->DisksMarkedForCleanup.insert(ev->Get()->Record.GetDiskId());

        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvDiskRegistry::TEvMarkDiskForCleanupResponse>());
    }

    void HandleFinishMigration(
        const TEvDiskRegistry::TEvFinishMigrationRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        ++State->FinishMigrationRequests;

        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvDiskRegistry::TEvFinishMigrationResponse>());
    }

    void HandleAcquireDisk(
        const TEvDiskRegistry::TEvAcquireDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        const auto& clientId = msg->Record.GetHeaders().GetClientId();
        auto response = std::make_unique<TEvDiskRegistry::TEvAcquireDiskResponse>();

        auto* disk = State->Disks.FindPtr(msg->Record.GetDiskId());

        if (!disk) {
            response->Record.MutableError()->CopyFrom(
                MakeError(E_NOT_FOUND, "disk not found")
            );
        } else if (!IsReadWriteMode(msg->Record.GetAccessMode())) {
            auto it = Find(
                disk->ReaderClientIds.begin(),
                disk->ReaderClientIds.end(),
                clientId
            );

            if (it == disk->ReaderClientIds.end()) {
                disk->ReaderClientIds.push_back(clientId);
            }

            ToLogicalBlocks(*disk, response->Record);
        } else if (!disk->WriterClientId || disk->WriterClientId == clientId)
        {
            disk->WriterClientId = clientId;

            ToLogicalBlocks(*disk, response->Record);
        } else {
            response->Record.MutableError()->CopyFrom(
                MakeError(E_INVALID_STATE, "disk already acquired")
            );
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleReleaseDisk(
        const TEvDiskRegistry::TEvReleaseDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        const auto& clientId = msg->Record.GetHeaders().GetClientId();
        auto response = std::make_unique<TEvDiskRegistry::TEvReleaseDiskResponse>();

        auto* disk = State->Disks.FindPtr(msg->Record.GetDiskId());

        if (!disk) {
            response->Record.MutableError()->CopyFrom(
                MakeError(E_NOT_FOUND, "disk not found")
            );
        } else if (clientId == disk->WriterClientId) {
            disk->WriterClientId = "";
        } else {
            auto it = Find(
                disk->ReaderClientIds.begin(),
                disk->ReaderClientIds.end(),
                clientId
            );

            if (it == disk->ReaderClientIds.end()) {
                response->Record.MutableError()->CopyFrom(
                    MakeError(
                        E_INVALID_STATE,
                        Sprintf(
                            "disk not acquired by client %s",
                            clientId.c_str()
                        )
                    )
                );
            } else {
                disk->ReaderClientIds.erase(it);
            }
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleUpdateConfig(
        const TEvDiskRegistry::TEvUpdateConfigRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        const auto& newConfig = record.GetConfig();

        NProto::TError error;

        const auto currentVersion = Config.GetVersion();

        if (record.GetIgnoreVersion()
            || newConfig.GetVersion() == currentVersion)
        {
            Config = newConfig;
            Config.SetVersion(currentVersion + 1);
        } else {
            error = MakeError(E_FAIL, "Wrong config version");
        }

        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvDiskRegistry::TEvUpdateConfigResponse>(error));
    }

    void HandleDescribeConfig(
        const TEvDiskRegistry::TEvDescribeConfigRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response = std::make_unique<TEvDiskRegistry::TEvDescribeConfigResponse>();

        *response->Record.MutableConfig() = Config;

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleSetWritableState(
        const TEvDiskRegistry::TEvSetWritableStateRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        State->WritableState = ev->Get()->Record.GetState();

        auto response = std::make_unique<
            TEvDiskRegistry::TEvSetWritableStateResponse>();

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleCreatePlacementGroup(
        const TEvService::TEvCreatePlacementGroupRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        if (State->PlacementGroups.contains(record.GetGroupId())) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvCreatePlacementGroupResponse>(
                    MakeError(S_ALREADY, "group already exists")
                )
            );
            return;
        }

        State->PlacementGroups[record.GetGroupId()];

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvCreatePlacementGroupResponse>()
        );
    }

    void HandleUpdatePlacementGroupSettings(
        const TEvDiskRegistry::TEvUpdatePlacementGroupSettingsRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        auto* group = State->PlacementGroups.FindPtr(record.GetGroupId());
        if (!group) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvCreatePlacementGroupResponse>(
                    MakeError(E_NOT_FOUND, "no such group")
                )
            );
            return;
        }

        if (group->ConfigVersion != record.GetConfigVersion()) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvCreatePlacementGroupResponse>(
                    MakeError(
                        E_ABORTED,
                        "config version mismatch"
                    )
                )
            );
            return;
        }

        group->Settings = record.GetSettings();
        ++group->ConfigVersion;

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistry::TEvUpdatePlacementGroupSettingsResponse>()
        );
    }

    void HandleChangeDeviceState(
        const TEvDiskRegistry::TEvChangeDeviceStateRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        auto response = std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateResponse>();

        bool found = false;

        for (auto& x: State->Disks) {
            for (auto& device: x.second.Devices) {
                if (device.GetDeviceUUID() == msg->Record.GetDeviceUUID()) {
                    device.SetState(msg->Record.GetDeviceState());
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            response->Record.MutableError()->CopyFrom(
                MakeError(E_NOT_FOUND, "device not found")
            );
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleChangeAgentState(
        const TEvDiskRegistry::TEvChangeAgentStateRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        State->AgentStates.push_back(std::make_pair(
            msg->Record.GetAgentId(),
            msg->Record.GetAgentState()
        ));

        auto response = std::make_unique<TEvDiskRegistry::TEvChangeAgentStateResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDestroyPlacementGroup(
        const TEvService::TEvDestroyPlacementGroupRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        if (!State->PlacementGroups.contains(record.GetGroupId())) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvDestroyPlacementGroupResponse>(
                    MakeError(S_ALREADY, "no such group")
                )
            );
            return;
        }

        State->PlacementGroups.erase(record.GetGroupId());

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvDestroyPlacementGroupResponse>()
        );
    }

    void HandleAlterPlacementGroupMembership(
        const TEvService::TEvAlterPlacementGroupMembershipRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        auto* group = State->PlacementGroups.FindPtr(record.GetGroupId());
        if (!group) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvAlterPlacementGroupMembershipResponse>(
                    MakeError(E_NOT_FOUND, "no such group")
                )
            );
            return;
        }

        for (const auto& diskId: record.GetDisksToAdd()) {
            if (!State->Disks.contains(diskId)) {
                NCloud::Reply(
                    ctx,
                    *ev,
                    std::make_unique<TEvService::TEvAlterPlacementGroupMembershipResponse>(
                        MakeError(
                            E_NOT_FOUND,
                            Sprintf("DiskToAdd not found: %s", diskId.c_str())
                        )
                    )
                );
                return;
            }
        }

        for (const auto& diskId: record.GetDisksToRemove()) {
            if (!State->Disks.contains(diskId)) {
                NCloud::Reply(
                    ctx,
                    *ev,
                    std::make_unique<TEvService::TEvAlterPlacementGroupMembershipResponse>(
                        MakeError(
                            E_NOT_FOUND,
                            Sprintf("DiskToRemove not found: %s", diskId.c_str())
                        )
                    )
                );
                return;
            }
        }

        for (const auto& diskId: record.GetDisksToAdd()) {
            group->DiskIds.insert(diskId);
        }

        for (const auto& diskId: record.GetDisksToRemove()) {
            group->DiskIds.erase(diskId);
        }

        ++group->ConfigVersion;

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvService::TEvAlterPlacementGroupMembershipResponse>()
        );
    }

    void HandleListPlacementGroups(
        const TEvService::TEvListPlacementGroupsRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        auto response = std::make_unique<TEvService::TEvListPlacementGroupsResponse>();

        for (const auto& x: State->PlacementGroups) {
            *response->Record.AddGroupIds() = x.first;
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDescribePlacementGroup(
        const TEvService::TEvDescribePlacementGroupRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& record = ev->Get()->Record;
        auto* group = State->PlacementGroups.FindPtr(record.GetGroupId());
        if (!group) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvDescribePlacementGroupResponse>(
                    MakeError(E_NOT_FOUND, "no such group")
                )
            );
            return;
        }

        auto response = std::make_unique<TEvService::TEvDescribePlacementGroupResponse>();
        auto* g = response->Record.MutableGroup();
        g->SetGroupId(record.GetGroupId());
        g->SetConfigVersion(group->ConfigVersion);
        g->SetPlacementStrategy(group->PlacementStrategy);
        g->SetPlacementPartitionCount(group->PlacementPartitionCount);
        for (const auto& diskId: group->DiskIds) {
            *g->AddDiskIds() = diskId;
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDescribeDisk(
        const TEvDiskRegistry::TEvDescribeDiskRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        auto response = std::make_unique<TEvDiskRegistry::TEvDescribeDiskResponse>();

        if (State->Disks.find(msg->Record.GetDiskId()) == State->Disks.end()) {
            *response->Record.MutableError() = MakeError(E_NOT_FOUND, "disk not found");
        } else {
            const auto& disk = State->Disks[msg->Record.GetDiskId()];
            for (const auto& device : disk.Devices) {
                auto& dev = *response->Record.MutableDevices()->Add();
                dev = device;
            }
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleBackupDiskRegistryState(
        const TEvDiskRegistry::TEvBackupDiskRegistryStateRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response = std::make_unique<TEvDiskRegistry::TEvBackupDiskRegistryStateResponse>();

        auto& backup = *response->Record.MutableBackup();

        for (const auto& [id, disk]: State->Disks) {
            auto& config = *backup.AddDisks();
            config.SetDiskId(id);
            for (const auto& device: disk.Devices) {
                *config.AddDeviceUUIDs() = device.GetDeviceUUID();
            }

            if (disk.Devices) {
                config.SetBlockSize(disk.Devices[0].GetBlockSize());
            }
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    auto CreateDiskFromDevices(const NProto::TCreateDiskFromDevicesRequest& req)
        -> TResultOrError<ui64>
    {
        const auto& diskId = req.GetVolumeConfig().GetDiskId();
        const auto blockSize = req.GetVolumeConfig().GetBlockSize();

        if (diskId.empty()) {
            return MakeError(E_ARGUMENT, "Empty DiskId");
        }

        if (State->Disks.contains(diskId)) {
            return MakeError(E_ARGUMENT, TStringBuilder() <<
                "disk " << diskId.Quote() << " already exists");
        }

        TVector<NProto::TDeviceConfig> devices;

        ui64 logicalBlockCount = 0;

        for (const auto& d: req.GetDevices()) {
            auto* config = FindIfPtr(
                State->Devices,
                [&] (const auto& x) {
                    if (d.GetDeviceUUID()) {
                        return x.GetDeviceUUID() == d.GetDeviceUUID();
                    }
                    return x.GetAgentId() == d.GetAgentId()
                        && x.GetDeviceName() == d.GetDeviceName();
                });

            if (!config) {
                return MakeError(E_ARGUMENT, "device not found");
            }

            logicalBlockCount +=
                config->GetBlocksCount() * config->GetBlockSize() / blockSize;

            devices.push_back(*config);
        }

        auto& disk = State->Disks[diskId];

        disk.MediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL;
        disk.Devices = std::move(devices);
        disk.BlockSize = blockSize;

        return logicalBlockCount;
    }

    void HandleCreateDiskFromDevices(
        const TEvDiskRegistry::TEvCreateDiskFromDevicesRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto [bc, error] = CreateDiskFromDevices(ev->Get()->Record);

        auto response = std::make_unique<
            TEvDiskRegistry::TEvCreateDiskFromDevicesResponse>(error);
        response->Record.SetBlockCount(bc);

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleResumeDevice(
        const TEvService::TEvResumeDeviceRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto error = MakeError(E_ARGUMENT);

        const auto& msg = ev->Get()->Record;

        for (auto& d: State->Devices) {
            if (d.GetAgentId() == msg.GetAgentId()
                && d.GetDeviceName() == msg.GetPath())
            {
                error = MakeError(S_OK);
                break;
            }
        }

        auto response = std::make_unique<
            TEvService::TEvResumeDeviceResponse>(error);

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleStartForceMigration(
        const TEvDiskRegistry::TEvStartForceMigrationRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto error = MakeError(E_NOT_FOUND);

        const auto& msg = ev->Get()->Record;

        if (State->Disks.contains(msg.GetSourceDiskId())) {
            auto& devices = State->Disks[msg.GetSourceDiskId()].Devices;
            auto itr = FindIf(devices,
                [&msg] (const auto& device) {
                    return device.GetDeviceUUID() == msg.GetSourceDeviceId();
                });
            if (itr != devices.end()) {
                error = MakeError(S_OK);
            }
        }

        auto response = std::make_unique<
            TEvDiskRegistry::TEvStartForceMigrationResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleChangeDiskDevice(
        const TEvDiskRegistry::TEvChangeDiskDeviceRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        const auto& msg = ev->Get()->Record;

        const bool foundTarget = FindIfPtr(
            State->Devices,
            [&msg] (const auto& deviceConfig) {
                return deviceConfig.GetDeviceUUID() == msg.GetTargetDeviceId();
            });

        const bool foundSource = State->Disks.contains(msg.GetDiskId()) &&
            FindIfPtr(State->Disks[msg.GetDiskId()].Devices,
                [&msg] (const auto& device) {
                    return device.GetDeviceUUID() == msg.GetSourceDeviceId();
                });

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistry::TEvChangeDiskDeviceResponse>(
                MakeError(foundTarget && foundSource ? S_OK : E_ARGUMENT)));
    }

    void HandleUpdateDiskRegistryAgentListParams(
        const TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsResponse>(
                MakeError(S_OK)));
    }

    void HandleGetDependentDisks(
        const TEvDiskRegistry::TEvGetDependentDisksRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistry::TEvGetDependentDisksResponse>());
    }

    void HandleAllocateCheckpoint(
        const TEvDiskRegistry::TEvAllocateCheckpointRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto response =
            std::make_unique<TEvDiskRegistry::TEvAllocateCheckpointResponse>();
        response->Record.SetCheckpointDiskId(
            ev->Get()->Record.GetSourceDiskId() +
            ev->Get()->Record.GetCheckpointId());
        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleDeallocateCheckpoint(
        const TEvDiskRegistry::TEvDeallocateCheckpointRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvDiskRegistry::TEvDeallocateCheckpointResponse>());
    }
};

}   // namespace NCloud::NBlockStore::NStorage
