#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReallocateActor final
    : public TActorBootstrapped<TReallocateActor>
{
private:
    const TActorId Owner;
    const TRequestInfoPtr Request;
    const TString DiskId;

    NProto::TAllocateDiskRequest Record;
    TChildLogTitle LogTitle;

public:
    TReallocateActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        TString diskId,
        NProto::TAllocateDiskRequest record,
        TChildLogTitle logTitle);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TError error,
        TVector<NProto::TLaggingDevice> laggingDevices);

private:
    STFUNC(StateWork);

    void HandleAllocateDiskResponse(
        const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateDevicesResponse(
        const TEvVolumePrivate::TEvUpdateDevicesResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TReallocateActor::TReallocateActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        TString diskId,
        NProto::TAllocateDiskRequest record,
        TChildLogTitle logTitle)
    : Owner(owner)
    , Request(std::move(request))
    , DiskId(std::move(diskId))
    , Record(std::move(record))
    , LogTitle(std::move(logTitle))
{}

void TReallocateActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<TEvDiskRegistry::TEvAllocateDiskRequest>();
    request->Record = std::move(Record);

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request));
}

void TReallocateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error,
    TVector<NProto::TLaggingDevice> laggingDevices)
{
    auto response = std::make_unique<TEvVolume::TEvReallocateDiskResponse>(
        std::move(error));
    for (auto& laggingDevice: laggingDevices) {
        *response->Record.AddOutdatedLaggingDevices() =
            std::move(laggingDevice);
    }

    NCloud::Reply(ctx, *Request, std::move(response));
    Die(ctx);
}

void TReallocateActor::HandleAllocateDiskResponse(
    const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Disk reallocation failed with error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(msg->GetError()).c_str());

        ReplyAndDie(ctx, msg->GetError(), {});
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Disk reallocation success. %s",
        LogTitle.GetWithTime().c_str(),
        DescribeAllocation(msg->Record).c_str());

    TVector<TDevices> replicas;
    for (auto& msgReplica: *msg->Record.MutableReplicas()) {
        replicas.push_back(std::move(*msgReplica.MutableDevices()));
    }

    TVector<TString> freshDeviceIds;
    for (auto& freshDeviceId: *msg->Record.MutableDeviceReplacementUUIDs()) {
        freshDeviceIds.push_back(std::move(freshDeviceId));
    }

    TVector<TString> removedLaggingDevices;
    for (auto& removedLaggingDevice:
         *msg->Record.MutableRemovedLaggingDevices())
    {
        removedLaggingDevices.push_back(
            std::move(*removedLaggingDevice.MutableDeviceUUID()));
    }

    TVector<TString> unavailableDeviceIds(
        std::make_move_iterator(
            record.MutableUnavailableDeviceUUIDs()->begin()),
        std::make_move_iterator(record.MutableUnavailableDeviceUUIDs()->end()));

    auto request = std::make_unique<TEvVolumePrivate::TEvUpdateDevicesRequest>(
        std::move(*msg->Record.MutableDevices()),
        std::move(*msg->Record.MutableMigrations()),
        std::move(replicas),
        std::move(freshDeviceIds),
        std::move(removedLaggingDevices),
        std::move(unavailableDeviceIds),
        msg->Record.GetIOMode(),
        TInstant::MicroSeconds(msg->Record.GetIOModeTs()),
        msg->Record.GetMuteIOErrors());

    NCloud::Send(ctx, Owner, std::move(request));
}

void TReallocateActor::HandleUpdateDevicesResponse(
    const TEvVolumePrivate::TEvUpdateDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ReplyAndDie(ctx, msg->GetError(), std::move(msg->LaggingDevices));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReallocateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvAllocateDiskResponse,
            HandleAllocateDiskResponse);
        HFunc(
            TEvVolumePrivate::TEvUpdateDevicesResponse,
            HandleUpdateDevicesResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleReallocateDisk(
    const TEvVolume::TEvReallocateDiskRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (UpdateVolumeConfigInProgress) {
        auto response = std::make_unique<TEvVolume::TEvReallocateDiskResponse>(
            MakeError(E_REJECTED, "Update volume config in progress"));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto request = MakeAllocateDiskRequest();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s ReallocateDiskRequest: %s",
        LogTitle.GetWithTime().c_str(),
        request.Utf8DebugString().Quote().c_str());

    NCloud::Register<TReallocateActor>(
        ctx,
        ctx.SelfID,
        std::move(requestInfo),
        GetNewestConfig().GetDiskId(),
        std::move(request),
        LogTitle.GetChild(GetCycleCount()));
}

}   // namespace NCloud::NBlockStore::NStorage
