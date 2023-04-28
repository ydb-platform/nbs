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
    NProto::TAllocateDiskRequest Record;

public:
    TReallocateActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        NProto::TAllocateDiskRequest record);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

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
        NProto::TAllocateDiskRequest record)
    : Owner(owner)
    , Request(std::move(request))
    , Record(std::move(record))
{
    ActivityType = TBlockStoreActivities::VOLUME;
}

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

void TReallocateActor::ReplyAndDie(const TActorContext& ctx, NProto::TError error)
{
    auto response = std::make_unique<TEvVolume::TEvReallocateDiskResponse>(
        std::move(error));

    NCloud::Reply(ctx, *Request, std::move(response));
    Die(ctx);
}

void TReallocateActor::HandleAllocateDiskResponse(
    const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    TVector<TDevices> replicas;
    for (auto& msgReplica: *msg->Record.MutableReplicas()) {
        replicas.push_back(std::move(*msgReplica.MutableDevices()));
    }

    TVector<TString> freshDeviceIds;
    for (auto& freshDeviceId: *msg->Record.MutableDeviceReplacementUUIDs()) {
        freshDeviceIds.push_back(std::move(freshDeviceId));
    }

    auto request = std::make_unique<TEvVolumePrivate::TEvUpdateDevicesRequest>(
        std::move(*msg->Record.MutableDevices()),
        std::move(*msg->Record.MutableMigrations()),
        std::move(replicas),
        std::move(freshDeviceIds),
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

    ReplyAndDie(ctx, msg->GetError());
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
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
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
        msg->CallContext,
        std::move(ev->TraceId));

    auto request = MakeAllocateDiskRequest();

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] ReallocateDiskRequest: %s",
        TabletID(),
        request.Utf8DebugString().Quote().c_str());

    NCloud::Register<TReallocateActor>(
        ctx,
        ctx.SelfID,
        std::move(requestInfo),
        std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage
