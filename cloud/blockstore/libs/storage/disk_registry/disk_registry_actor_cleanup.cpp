#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/volume_label.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCleanupActor final
    : public TActorBootstrapped<TCleanupActor>
{
private:
    const TActorId Owner;
    const TRequestInfoPtr Request;
    const TVector<TString> DiskIds;

    int PendingRequests = 0;

public:
    TCleanupActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        TVector<TString> disks);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx, ui64 index);
    void DeallocateDisk(const TActorContext& ctx, ui64 index);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error = {});

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeallocateDiskResponse(
        const TEvDiskRegistry::TEvDeallocateDiskResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCleanupActor::TCleanupActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        TVector<TString> diskIds)
    : Owner(owner)
    , Request(std::move(request))
    , DiskIds(std::move(diskIds))
{}

void TCleanupActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (ui64 i = 0; i != DiskIds.size(); ++i) {
        DescribeVolume(ctx, i);
    }

    if (!PendingRequests) {
        ReplyAndDie(ctx);
    }
}

void TCleanupActor::ReplyAndDie(const TActorContext& ctx, NProto::TError error)
{
    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvCleanupDisksResponse>(
        std::move(error));

    NCloud::Reply(ctx, *Request, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

void TCleanupActor::DescribeVolume(const TActorContext& ctx, ui64 index)
{
    ++PendingRequests;

    auto request = std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
        DiskIds[index]);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), index);
}

void TCleanupActor::DeallocateDisk(const TActorContext& ctx, ui64 index)
{
    ++PendingRequests;

    const auto& id = DiskIds[index];

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "Deallocate disk " << id.Quote());

    auto request = std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();
    request->Record.SetDiskId(id);

    NCloud::Send(ctx, Owner, std::move(request), index);
}

void TCleanupActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    --PendingRequests;

    Y_ABORT_UNLESS(PendingRequests >= 0);

    const auto* msg = ev->Get();
    const auto index = ev->Cookie;

    if (msg->GetStatus() ==
        MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
    {
        DeallocateDisk(ctx, index);
    } else {
        const auto& id = DiskIds[index];
        LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Disk " << id.Quote() << " is still present in SchemeShard, keep it");
    }

    if (!PendingRequests) {
        ReplyAndDie(ctx);
    }
}

void TCleanupActor::HandleDeallocateDiskResponse(
    const TEvDiskRegistry::TEvDeallocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    --PendingRequests;

    Y_ABORT_UNLESS(PendingRequests >= 0);

    const auto* msg = ev->Get();
    const auto index = ev->Cookie;
    const auto& id = DiskIds[index];

    if (HasError(msg->GetError())) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Deallocate disk " << id.Quote() << " error: " <<
            FormatError(msg->GetError()));
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Deallocate disk " << id.Quote() << " result: " <<
            FormatError(msg->GetError()));
    }

    if (!PendingRequests) {
        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCleanupActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCleanupActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeVolumeResponse);
        HFunc(TEvDiskRegistry::TEvDeallocateDiskResponse, HandleDeallocateDiskResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCleanupDisks(
    const TEvDiskRegistryPrivate::TEvCleanupDisksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CleanupDisks);

    auto actor = NCloud::Register<TCleanupActor>(
        ctx,
        SelfId(),
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        State->GetDisksToCleanup());

    Actors.insert(actor);
}

void TDiskRegistryActor::HandleCleanupDisksResponse(
    const TEvDiskRegistryPrivate::TEvCleanupDisksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ScheduleCleanup(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
