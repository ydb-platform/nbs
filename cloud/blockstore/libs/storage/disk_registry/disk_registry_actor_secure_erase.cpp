#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_registry/model/device_list.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSecureEraseActor final
    : public TActorBootstrapped<TSecureEraseActor>
{
private:
    const TActorId Owner;
    const TRequestInfoPtr Request;
    const TDuration RequestTimeout;

    const TString PoolName;
    TVector<NProto::TDeviceConfig> Devices;
    TVector<TString> CleanDevices;

    int PendingRequests = 0;

public:
    TSecureEraseActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        TDuration requestTimeout,
        TString poolName,
        TVector<NProto::TDeviceConfig> devicesToClean);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error = {});
    void CleanupDevices(const TActorContext& ctx);

private:
    STFUNC(StateErase);
    STFUNC(StateCleanup);

    void HandleSecureEraseDeviceResponse(
        const TEvDiskAgent::TEvSecureEraseDeviceResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCleanupDevicesResponse(
        const TEvDiskRegistryPrivate::TEvCleanupDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleSecureEraseDeviceUndelivery(
        const TEvDiskAgent::TEvSecureEraseDeviceRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSecureEraseActor::TSecureEraseActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        TDuration requestTimeout,
        TString poolName,
        TVector<NProto::TDeviceConfig> devicesToClean)
    : Owner(owner)
    , Request(std::move(request))
    , RequestTimeout(requestTimeout)
    , PoolName(std::move(poolName))
    , Devices(std::move(devicesToClean))
{}

void TSecureEraseActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateErase);

    for (ui64 i = 0; i != Devices.size(); ++i) {
        auto& device = Devices[i];

        auto request = std::make_unique<TEvDiskAgent::TEvSecureEraseDeviceRequest>();
        request->Record.SetDeviceUUID(device.GetDeviceUUID());

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(device.GetNodeId()),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,   // flags
            i,                                        // cookie
            &ctx.SelfID                               // forwardOnNondelivery
        );

        ctx.Send(event.release());

        ++PendingRequests;
    }

    if (RequestTimeout && RequestTimeout != TDuration::Max()) {
        ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());
    }
}

void TSecureEraseActor::ReplyAndDie(const TActorContext& ctx, NProto::TError error)
{
    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvSecureEraseResponse>(
        std::move(error),
        PoolName,
        CleanDevices.size());
    NCloud::Reply(ctx, *Request, std::move(response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

void TSecureEraseActor::CleanupDevices(const TActorContext& ctx)
{
    Become(&TThis::StateCleanup);

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvCleanupDevicesRequest>(
        CleanDevices);

    NCloud::Send(ctx, Owner, std::move(request));
}

void TSecureEraseActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "Secure erase request timed out");

    if (CleanDevices) {
        CleanupDevices(ctx);
    } else {
        ReplyAndDie(ctx);
    }
}

void TSecureEraseActor::HandleSecureEraseDeviceUndelivery(
    const TEvDiskAgent::TEvSecureEraseDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    const auto index = ev->Cookie;
    auto& device = Devices[index];

    LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "Undelivered secure erase request for " << device.GetDeviceUUID().Quote());

    if (--PendingRequests != 0) {
        return;
    }

    if (CleanDevices) {
        CleanupDevices(ctx);
    } else {
        ReplyAndDie(ctx);
    }
}

void TSecureEraseActor::HandleSecureEraseDeviceResponse(
    const TEvDiskAgent::TEvSecureEraseDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    const auto* msg = ev->Get();

    const auto index = ev->Cookie;
    auto& device = Devices[index];

    if (!HasError(msg->GetError())) {
        CleanDevices.push_back(device.GetDeviceUUID());
    } else {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Secure erase device " << device.GetDeviceUUID().Quote() <<
            " failed: " << FormatError(msg->GetError()));
    }

    if (--PendingRequests != 0) {
        return;
    }

    if (CleanDevices) {
        CleanupDevices(ctx);
    } else {
        ReplyAndDie(ctx);
    }
}

void TSecureEraseActor::HandleCleanupDevicesResponse(
    const TEvDiskRegistryPrivate::TEvCleanupDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx);
}

void TSecureEraseActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

STFUNC(TSecureEraseActor::StateErase)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(TEvDiskAgent::TEvSecureEraseDeviceRequest, HandleSecureEraseDeviceUndelivery);
        HFunc(TEvDiskAgent::TEvSecureEraseDeviceResponse, HandleSecureEraseDeviceResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TSecureEraseActor::StateCleanup)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        IgnoreFunc(TEvents::TEvWakeup);

        IgnoreFunc(TEvDiskAgent::TEvSecureEraseDeviceRequest);
        IgnoreFunc(TEvDiskAgent::TEvSecureEraseDeviceResponse);

        HFunc(TEvDiskRegistryPrivate::TEvCleanupDevicesResponse, HandleCleanupDevicesResponse);

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

bool TDiskRegistryActor::PrepareCleanupDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupDevices& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCleanupDevices(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCleanupDevices& args)
{
    TDiskRegistryDatabase db(tx.DB);
    args.SyncDeallocatedDisks =
        State->MarkDevicesAsClean(ctx.Now(), db, args.Devices);
}

void TDiskRegistryActor::CompleteCleanupDevices(
    const TActorContext& ctx,
    TTxDiskRegistry::TCleanupDevices& args)
{
    auto response = std::make_unique<TEvDiskRegistryPrivate::TEvCleanupDevicesResponse>();
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    for (const auto& diskId: args.SyncDeallocatedDisks) {
        ReplyToPendingDeallocations(ctx, diskId);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::SecureErase(const TActorContext& ctx)
{
    auto dirtyDevices = State->GetDirtyDevices();
    EraseIf(
        dirtyDevices,
        [this](auto& device) { return !State->CanSecureErase(device); });

    if (!dirtyDevices) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Nothing to erase",
            TabletID());

        return;
    }

    const auto countBeforeFiltration = dirtyDevices.size();

    dirtyDevices = FilterDevices(
        dirtyDevices,
        Config->GetMaxDevicesToErasePerDeviceNameForDefaultPoolKind(),
        Config->GetMaxDevicesToErasePerDeviceNameForLocalPoolKind(),
        Config->GetMaxDevicesToErasePerDeviceNameForGlobalPoolKind());

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Filtered dirty devices: %lu -> %lu",
        TabletID(),
        countBeforeFiltration,
        dirtyDevices.size());

    auto it = dirtyDevices.begin();
    while (it != dirtyDevices.end()) {
        auto first = it;
        const auto poolName = first->GetPoolName();
        it = std::partition(
            first,
            dirtyDevices.end(),
            [&poolName](const auto& device)
            { return poolName == device.GetPoolName(); });

        auto [_, alreadyInProgress] =
            SecureEraseInProgressPerPool.insert(poolName);
        if (!alreadyInProgress) {
            continue;
        }

        auto request =
            std::make_unique<TEvDiskRegistryPrivate::TEvSecureEraseRequest>(
                poolName,
                TVector<NProto::TDeviceConfig>(
                    std::make_move_iterator(first),
                    std::make_move_iterator(it)),
                Config->GetNonReplicatedSecureEraseTimeout());

        auto deadline =
            Min(SecureEraseStartTs, ctx.Now()) + TDuration::Seconds(5);
        if (deadline > ctx.Now()) {
            LOG_INFO(
                ctx,
                TBlockStoreComponents::DISK_REGISTRY,
                "[%lu] Scheduled secure erase for pool: %s, now: %lu, "
                "deadline: %lu",
                TabletID(),
                poolName.c_str(),
                ctx.Now().MicroSeconds(),
                deadline.MicroSeconds());

            ctx.Schedule(
                deadline,
                std::make_unique<IEventHandle>(ctx.SelfID, ctx.SelfID, request.release()));
        } else {
            LOG_INFO(
                ctx,
                TBlockStoreComponents::DISK_REGISTRY,
                "[%lu] Sending secure erase request for pool: %s",
                TabletID(),
                poolName.c_str());

            NCloud::Send(ctx, ctx.SelfID, std::move(request));
        }
    }
}

void TDiskRegistryActor::HandleSecureErase(
    const TEvDiskRegistryPrivate::TEvSecureEraseRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(SecureErase);

    SecureEraseStartTs = ctx.Now();

    auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received SecureErase request: DirtyDevices=%lu",
        TabletID(),
        msg->DirtyDevices.size());

    auto actor = NCloud::Register<TSecureEraseActor>(
        ctx,
        ctx.SelfID,
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            msg->CallContext
        ),
        msg->RequestTimeout,
        msg->PoolName,
        std::move(msg->DirtyDevices));
    Actors.insert(actor);
}

void TDiskRegistryActor::HandleSecureEraseResponse(
    const TEvDiskRegistryPrivate::TEvSecureEraseResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received SecureErase response: CleanDevices=%lu",
        TabletID(),
        msg->CleanDevices);

    SecureEraseInProgressPerPool.erase(msg->PoolName);
    SecureErase(ctx);
}

void TDiskRegistryActor::HandleCleanupDevices(
    const TEvDiskRegistryPrivate::TEvCleanupDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received CleanupDevices request: Devices=%lu",
        TabletID(),
        msg->Devices.size());

    ExecuteTx<TCleanupDevices>(
        ctx,
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            msg->CallContext
        ),
        msg->Devices);
}

}   // namespace NCloud::NBlockStore::NStorage
