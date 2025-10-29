#pragma once

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_private.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/set.h>

namespace NCloud::NBlockStore::NStorage::NDiskRegistry {

////////////////////////////////////////////////////////////////////////////////

class TRestoreValidationActor final
    : public NActors::TActorBootstrapped<TRestoreValidationActor>
{
private:
    using TValidators =
        void(TRestoreValidationActor::*)(const NActors::TActorContext&);

    const NActors::TActorId Owner;
    const TRequestInfoPtr Request;
    const int Component;

    TDiskRegistryStateSnapshot ValidSnapshot;
    TQueue<TValidators> Validators;

    int DisksWaitedConfirmSize;
    TSet<TString> DisksInSS;
    TSet<TString> DisksInBackup;
    THashMap<TString, NProto::TDeviceConfig*> DevicesInBackup;

public:
    TRestoreValidationActor(
        NActors::TActorId owner,
        TRequestInfoPtr request,
        int component,
        TDiskRegistryStateSnapshot snapshot);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        NProto::TError error);

    template <typename Container>
    void SetErrorDevicesInBackup(const Container& deviceIds, TInstant now);
    void SetErrorDeviceInBackup(const TString& deviceId, TInstant now);
    void EraseDiskFromBackup(const TString& diskId, TInstant now);

    void NextValidation(const NActors::TActorContext& ctx);
    void StartDisksCheck(const NActors::TActorContext& ctx);
    void StartPlacementGroupsCheck(const NActors::TActorContext& ctx);
    void StartDeviceCheck(const NActors::TActorContext& ctx);

    bool CompareDiskConfigs(
        const NActors::TActorContext& ctx,
        const NKikimrBlockStore::TVolumeConfig& ssConfig,
        const NProto::TDiskConfig& backupConfig);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleListVolumesResponse(
        const TEvService::TEvListVolumesResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetVolumeInfoResponse(
        const TEvVolume::TEvGetVolumeInfoResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // NCloud::NBlockStore::NStorage::NDiskRegistry
