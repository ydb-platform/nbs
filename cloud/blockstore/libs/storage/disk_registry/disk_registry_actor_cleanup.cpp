#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDiskToCleanup
{
    TString DiskId;
    // Zero for disks of native NBS volumes.
    ui64 OwnerVolumeTabletId = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TCleanupActor final
    : public TActorBootstrapped<TCleanupActor>
{
private:
    const TActorId Owner;
    const TChildLogTitle LogTitle;
    const TRequestInfoPtr Request;
    const TVector<TDiskToCleanup> Disks;

    int PendingRequests = 0;

public:
    TCleanupActor(
        const TActorId& owner,
        const TLogTitle& logTitle,
        TRequestInfoPtr request,
        TVector<TDiskToCleanup> disks);

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
        const TLogTitle& logTitle,
        TRequestInfoPtr request,
        TVector<TDiskToCleanup> disks)
    : Owner(owner)
    , LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"TCleanupActor", std::monostate{}}}))
    , Request(std::move(request))
    , Disks(std::move(disks))
{}

void TCleanupActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (ui64 i = 0; i != Disks.size(); ++i) {
        // External volumes are not registered in SchemeShard: the owner marks
        // the disk right before deallocating it, so there is nothing to check.
        if (Disks[i].OwnerVolumeTabletId) {
            DeallocateDisk(ctx, i);
        } else {
            DescribeVolume(ctx, i);
        }
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
        Disks[index].DiskId);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), index);
}

void TCleanupActor::DeallocateDisk(const TActorContext& ctx, ui64 index)
{
    ++PendingRequests;

    const auto& [id, ownerVolumeTabletId] = Disks[index];

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "%s Deallocate disk %s",
        LogTitle.GetWithTime().c_str(),
        id.Quote().c_str());

    auto request = std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();
    request->Record.SetDiskId(id);
    request->Record.SetOwnerVolumeTabletId(ownerVolumeTabletId);

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
        const auto& id = Disks[index].DiskId;
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "%s Disk %s is still present in SchemeShard, keep it",
            LogTitle.GetWithTime().c_str(),
            id.Quote().c_str());
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
    const auto& id = Disks[index].DiskId;

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "%s Deallocate disk %s error: %s",
            LogTitle.GetWithTime().c_str(),
            id.Quote().c_str(),
            FormatError(msg->GetError()).c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "%s Deallocate disk %s result: %s",
            LogTitle.GetWithTime().c_str(),
            id.Quote().c_str(),
            FormatError(msg->GetError()).c_str());
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

    TVector<TDiskToCleanup> disks;
    for (auto& diskId: State->GetDisksToCleanup()) {
        const ui64 ownerVolumeTabletId = State->GetOwnerVolumeTabletId(diskId);
        disks.push_back({std::move(diskId), ownerVolumeTabletId});
    }

    auto actor = NCloud::Register<TCleanupActor>(
        ctx,
        SelfId(),
        LogTitle,
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        std::move(disks));

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
