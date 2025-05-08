#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyActor final
    : public TActorBootstrapped<TDestroyActor>
{
private:
    const TActorId Owner;
    const ui64 TabletID;
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;

    TVector<TString> DiskIds;
    int PendingOperations = 0;

public:
    TDestroyActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TVector<TString> diskIds);

    void Bootstrap(const TActorContext& ctx);

private:
    void DestroyDisks(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error = {});

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleDestroyVolumeResponse(
        const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDestroyActor::TDestroyActor(
        const TActorId& owner,
        const ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TVector<TString> diskIds)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , DiskIds(std::move(diskIds))
{}

void TDestroyActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    DestroyDisks(ctx);
}

void TDestroyActor::ReplyAndDie(const TActorContext& ctx, NProto::TError error)
{
    auto end = std::partition(
        DiskIds.begin(),
        DiskIds.end(),
        [] (const TString& diskId) {
            return !!diskId;
        });
    DiskIds.erase(end, DiskIds.end());

    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvDestroyBrokenDisksResponse>(
        std::move(error),
        RequestInfo->CallContext,
        std::move(DiskIds));

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());
    Die(ctx);
}

void TDestroyActor::DestroyDisks(const TActorContext& ctx)
{
    PendingOperations = DiskIds.size();

    ui64 cookie = 0;
    for (const auto& diskId: DiskIds) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Destroying broken volume: DiskId=%s",
            TabletID,
            diskId.Quote().c_str());

        auto request = std::make_unique<TEvService::TEvDestroyVolumeRequest>();
        request->Record.SetDiskId(diskId);
        request->Record.SetDestroyIfBroken(true);
        NCloud::Send(
            ctx,
            MakeStorageServiceId(),
            std::move(request),
            cookie++
        );
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDestroyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TDestroyActor::HandleDestroyVolumeResponse(
    const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto cookie = ev->Cookie;

    --PendingOperations;

    Y_ABORT_UNLESS(PendingOperations >= 0);

    if (HasError(ev->Get()->Record)) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Volume destruction failed: DiskId=%s, Error=%s",
            TabletID,
            DiskIds[cookie].Quote().c_str(),
            FormatError(ev->Get()->Record.GetError()).c_str());
        DiskIds[cookie] = {};
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Volume destruction succeeded: DiskId=%s",
            TabletID,
            DiskIds[cookie].Quote().c_str());
    }

    if (!PendingOperations) {
        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDestroyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvDestroyVolumeResponse, HandleDestroyVolumeResponse);

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

void TDiskRegistryActor::HandleListBrokenDisks(
    const TEvDiskRegistryPrivate::TEvListBrokenDisksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ListBrokenDisks);

    TVector<TString> diskIds;
    for (const auto& bdi: State->GetBrokenDisks()) {
        diskIds.push_back(bdi.DiskId);
    }
    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvListBrokenDisksResponse>(
        std::move(diskIds)
    );
    Sort(response->DiskIds.begin(), response->DiskIds.end());

    NCloud::Send(ctx, ev->Sender, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::DestroyBrokenDisks(const TActorContext& ctx)
{
    if (BrokenDisksDestructionInProgress || State->GetBrokenDisks().empty()) {
        return;
    }

    BrokenDisksDestructionInProgress = true;

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvDestroyBrokenDisksRequest>();

    auto deadline = ctx.Now() + Config->GetBrokenDiskDestructionDelay();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Scheduled broken disks destruction, now: %lu, deadline: %lu",
        TabletID(),
        ctx.Now().MicroSeconds(),
        deadline.MicroSeconds());

    ctx.Schedule(deadline, request.release());
}

void TDiskRegistryActor::HandleDestroyBrokenDisks(
    const TEvDiskRegistryPrivate::TEvDestroyBrokenDisksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    BLOCKSTORE_DISK_REGISTRY_COUNTER(DestroyBrokenDisks);

    BrokenDisksDestructionStartTs = ctx.Now();

    for (const auto& info: State->GetBrokenDisks()) {
        if (info.TsToDestroy < BrokenDisksDestructionStartTs) {
            DisksBeingDestroyed.push_back(info.DiskId);
        }
    }

    if (!DisksBeingDestroyed) {
        BrokenDisksDestructionInProgress = false;
        DestroyBrokenDisks(ctx);
        return;
    }

    auto actor = NCloud::Register<TDestroyActor>(
        ctx,
        SelfId(),
        TabletID(),
        CreateRequestInfo(
            SelfId(),
            0,
            MakeIntrusive<TCallContext>()
        ),
        Config,
        DisksBeingDestroyed
    );
    Actors.insert(actor);
}

void TDiskRegistryActor::HandleDestroyBrokenDisksResponse(
    const TEvDiskRegistryPrivate::TEvDestroyBrokenDisksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ExecuteTx<TDeleteBrokenDisks>(
        ctx,
        CreateRequestInfo<TEvDiskRegistryPrivate::TDestroyBrokenDisksMethod>(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext
        ),
        std::move(ev->Get()->Disks)
    );
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareDeleteBrokenDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteBrokenDisks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteDeleteBrokenDisks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDeleteBrokenDisks& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->DeleteBrokenDisks(db, std::move(args.Disks));
}

void TDiskRegistryActor::CompleteDeleteBrokenDisks(
    const TActorContext& ctx,
    TTxDiskRegistry::TDeleteBrokenDisks& args)
{
    Y_UNUSED(args);

    BrokenDisksDestructionInProgress = false;
    DisksBeingDestroyed.clear();
    DestroyBrokenDisks(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
