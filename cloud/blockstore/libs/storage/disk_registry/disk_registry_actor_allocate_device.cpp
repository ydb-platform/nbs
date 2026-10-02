#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAllocateDeviceActor final
    : public TActorBootstrapped<TAllocateDeviceActor>
{
private:
    using TAllocateDiskResponsePtr =
        std::unique_ptr<TEvDiskRegistry::TEvAllocateDiskResponse>;

    const TChildLogTitle LogTitle;
    const TActorId Owner;
    const TRequestInfoPtr Request;
    const TDuration RequestTimeout;
    const TString DiskId;
    const NProto::TJournalConfig JournalConfig;
    const TVector<NProto::TDeviceConfig> Devices;

    // The response to the disk allocation request. It is held back until all
    // the devices are acknowledged by their agents.
    TAllocateDiskResponsePtr Response;

    int PendingRequests = 0;

public:
    TAllocateDeviceActor(
        const TLogTitle& logTitle,
        const TActorId& owner,
        TRequestInfoPtr request,
        TDuration requestTimeout,
        TString diskId,
        NProto::TJournalConfig journalConfig,
        TVector<NProto::TDeviceConfig> devices,
        TAllocateDiskResponsePtr response);

    void Bootstrap(const TActorContext& ctx);

private:
    void ConfirmDeviceAllocation(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error = {});

private:
    STFUNC(StateAllocate);
    STFUNC(StateConfirm);

    void HandleAllocateDeviceResponse(
        const TEvDiskAgent::TEvAllocateDeviceResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAllocateDeviceUndelivery(
        const TEvDiskAgent::TEvAllocateDeviceRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfirmDeviceAllocationResponse(
        const TEvDiskRegistryPrivate::TEvConfirmDeviceAllocationResponse::TPtr&
            ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TAllocateDeviceActor::TAllocateDeviceActor(
        const TLogTitle& logTitle,
        const TActorId& owner,
        TRequestInfoPtr request,
        TDuration requestTimeout,
        TString diskId,
        NProto::TJournalConfig journalConfig,
        TVector<NProto::TDeviceConfig> devices,
        TAllocateDiskResponsePtr response)
    : LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"disk", TStringBuf(diskId)}}))
    , Owner(owner)
    , Request(std::move(request))
    , RequestTimeout(requestTimeout)
    , DiskId(std::move(diskId))
    , JournalConfig(std::move(journalConfig))
    , Devices(std::move(devices))
    , Response(std::move(response))
{}

void TAllocateDeviceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateAllocate);

    for (ui64 i = 0; i != Devices.size(); ++i) {
        const auto& device = Devices[i];

        auto request =
            std::make_unique<TEvDiskAgent::TEvAllocateDeviceRequest>();
        request->Record.SetDeviceUUID(device.GetDeviceUUID());
        *request->Record.MutableJournalConfig() = JournalConfig;

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

void TAllocateDeviceActor::ConfirmDeviceAllocation(const TActorContext& ctx)
{
    Become(&TThis::StateConfirm);

    TVector<TString> uuids;
    uuids.reserve(Devices.size());
    for (const auto& device: Devices) {
        uuids.push_back(device.GetDeviceUUID());
    }

    auto request = std::make_unique<
        TEvDiskRegistryPrivate::TEvConfirmDeviceAllocationRequest>(
        DiskId,
        std::move(uuids));

    NCloud::Send(ctx, Owner, std::move(request));
}

void TAllocateDeviceActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "%s Devices are not acknowledged by agents: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        Response = std::make_unique<TEvDiskRegistry::TEvAllocateDiskResponse>(
            std::move(error));
    }

    NCloud::Reply(ctx, *Request, std::move(Response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

void TAllocateDeviceActor::HandleAllocateDeviceResponse(
    const TEvDiskAgent::TEvAllocateDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    const auto* msg = ev->Get();
    const auto& device = Devices[ev->Cookie];

    if (HasError(msg->GetError())) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "allocate device " << device.GetDeviceUUID().Quote()
                    << " failed: " << FormatError(msg->GetError())));
        return;
    }

    if (--PendingRequests == 0) {
        ConfirmDeviceAllocation(ctx);
    }
}

void TAllocateDeviceActor::HandleAllocateDeviceUndelivery(
    const TEvDiskAgent::TEvAllocateDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = Devices[ev->Cookie];

    ReplyAndDie(
        ctx,
        MakeError(
            E_REJECTED,
            TStringBuilder()
                << "allocate device request for "
                << device.GetDeviceUUID().Quote() << " is undelivered"));
}

void TAllocateDeviceActor::HandleConfirmDeviceAllocationResponse(
    const TEvDiskRegistryPrivate::TEvConfirmDeviceAllocationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->GetError());
}

void TAllocateDeviceActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(
        ctx,
        MakeError(E_REJECTED, "allocate device requests timed out"));
}

void TAllocateDeviceActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

STFUNC(TAllocateDeviceActor::StateAllocate)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(
            TEvDiskAgent::TEvAllocateDeviceRequest,
            HandleAllocateDeviceUndelivery);
        HFunc(
            TEvDiskAgent::TEvAllocateDeviceResponse,
            HandleAllocateDeviceResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TAllocateDeviceActor::StateConfirm)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        IgnoreFunc(TEvents::TEvWakeup);

        HFunc(
            TEvDiskRegistryPrivate::TEvConfirmDeviceAllocationResponse,
            HandleConfirmDeviceAllocationResponse);

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

void TDiskRegistryActor::SendAllocateDeviceRequests(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    const TString& diskId,
    const NProto::TJournalConfig& journalConfig,
    TVector<NProto::TDeviceConfig> devices,
    std::unique_ptr<TEvDiskRegistry::TEvAllocateDiskResponse> response)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Sending allocate device requests. DiskId=%s Devices=%lu",
        LogTitle.GetWithTime().c_str(),
        diskId.Quote().c_str(),
        devices.size());

    auto actor = NCloud::Register<TAllocateDeviceActor>(
        ctx,
        LogTitle,
        ctx.SelfID,
        std::move(requestInfo),
        Config->GetAgentRequestTimeout(),
        diskId,
        journalConfig,
        std::move(devices),
        std::move(response));
    Actors.insert(actor);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleConfirmDeviceAllocation(
    const TEvDiskRegistryPrivate::TEvConfirmDeviceAllocationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(ConfirmDeviceAllocation);

    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received ConfirmDeviceAllocation request: DiskId=%s Devices=[%s] "
        "%s",
        LogTitle.GetWithTime().c_str(),
        msg->DiskId.Quote().c_str(),
        JoinStrings(msg->Devices, ", ").c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TConfirmDeviceAllocation>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(msg->DiskId),
        std::move(msg->Devices));
}

bool TDiskRegistryActor::PrepareConfirmDeviceAllocation(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TConfirmDeviceAllocation& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteConfirmDeviceAllocation(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TConfirmDeviceAllocation& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->ConfirmDeviceAllocation(db, args.DiskId, args.Devices);
}

void TDiskRegistryActor::CompleteConfirmDeviceAllocation(
    const TActorContext& ctx,
    TTxDiskRegistry::TConfirmDeviceAllocation& args)
{
    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<
            TEvDiskRegistryPrivate::TEvConfirmDeviceAllocationResponse>());
}

}   // namespace NCloud::NBlockStore::NStorage
