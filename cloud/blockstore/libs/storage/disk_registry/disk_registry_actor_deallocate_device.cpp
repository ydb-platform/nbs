#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDeallocateDeviceActor final
    : public TActorBootstrapped<TDeallocateDeviceActor>
{
private:
    const TChildLogTitle LogTitle;
    const TActorId Owner;
    const TRequestInfoPtr Request;
    const TDuration RequestTimeout;
    const TString DiskId;
    const bool Sync;
    const TVector<NProto::TDeviceConfig> Devices;

    int PendingRequests = 0;

public:
    TDeallocateDeviceActor(
        const TLogTitle& logTitle,
        const TActorId& owner,
        TRequestInfoPtr request,
        TDuration requestTimeout,
        TString diskId,
        bool sync,
        TVector<NProto::TDeviceConfig> devices);

    void Bootstrap(const TActorContext& ctx);

private:
    void DeallocateDisk(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void Done(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDeallocateDeviceResponse(
        const TEvDiskAgent::TEvDeallocateDeviceResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeallocateDeviceUndelivery(
        const TEvDiskAgent::TEvDeallocateDeviceRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDeallocateDeviceActor::TDeallocateDeviceActor(
        const TLogTitle& logTitle,
        const TActorId& owner,
        TRequestInfoPtr request,
        TDuration requestTimeout,
        TString diskId,
        bool sync,
        TVector<NProto::TDeviceConfig> devices)
    : LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"disk", TStringBuf(diskId)}}))
    , Owner(owner)
    , Request(std::move(request))
    , RequestTimeout(requestTimeout)
    , DiskId(std::move(diskId))
    , Sync(sync)
    , Devices(std::move(devices))
{}

void TDeallocateDeviceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    for (ui64 i = 0; i != Devices.size(); ++i) {
        const auto& device = Devices[i];

        auto request =
            std::make_unique<TEvDiskAgent::TEvDeallocateDeviceRequest>();
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

void TDeallocateDeviceActor::DeallocateDisk(const TActorContext& ctx)
{
    auto completed = std::make_unique<
        TEvDiskRegistryPrivate::TEvDeallocateDevicesCompleted>();
    completed->RequestInfo = Request;
    completed->DiskId = DiskId;
    completed->Sync = Sync;

    NCloud::Send(ctx, Owner, std::move(completed));

    Done(ctx);
}

void TDeallocateDeviceActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "%s Devices deallocation is not acknowledged by agents: %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(error).c_str());

    NCloud::Reply(
        ctx,
        *Request,
        std::make_unique<TEvDiskRegistry::TEvDeallocateDiskResponse>(
            std::move(error)));

    Done(ctx);
}

void TDeallocateDeviceActor::Done(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

void TDeallocateDeviceActor::HandleDeallocateDeviceResponse(
    const TEvDiskAgent::TEvDeallocateDeviceResponse::TPtr& ev,
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
                    << "deallocate device " << device.GetDeviceUUID().Quote()
                    << " failed: " << FormatError(msg->GetError())));
        return;
    }

    if (--PendingRequests == 0) {
        DeallocateDisk(ctx);
    }
}

void TDeallocateDeviceActor::HandleDeallocateDeviceUndelivery(
    const TEvDiskAgent::TEvDeallocateDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = Devices[ev->Cookie];

    ReplyAndDie(
        ctx,
        MakeError(
            E_REJECTED,
            TStringBuilder()
                << "deallocate device request for "
                << device.GetDeviceUUID().Quote() << " is undelivered"));
}

void TDeallocateDeviceActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(
        ctx,
        MakeError(E_REJECTED, "deallocate device requests timed out"));
}

void TDeallocateDeviceActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

STFUNC(TDeallocateDeviceActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(
            TEvDiskAgent::TEvDeallocateDeviceRequest,
            HandleDeallocateDeviceUndelivery);
        HFunc(
            TEvDiskAgent::TEvDeallocateDeviceResponse,
            HandleDeallocateDeviceResponse);

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

void TDiskRegistryActor::SendDeallocateDeviceRequests(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    const TString& diskId,
    bool sync,
    TVector<NProto::TDeviceConfig> devices)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Sending deallocate device requests. DiskId=%s Devices=%lu",
        LogTitle.GetWithTime().c_str(),
        diskId.Quote().c_str(),
        devices.size());

    auto actor = NCloud::Register<TDeallocateDeviceActor>(
        ctx,
        LogTitle,
        ctx.SelfID,
        std::move(requestInfo),
        Config->GetAgentRequestTimeout(),
        diskId,
        sync,
        std::move(devices));
    Actors.insert(actor);
}

void TDiskRegistryActor::HandleDeallocateDevicesCompleted(
    const TEvDiskRegistryPrivate::TEvDeallocateDevicesCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Devices deallocation is acknowledged by agents. DiskId=%s",
        LogTitle.GetWithTime().c_str(),
        msg->DiskId.Quote().c_str());

    ExecuteTx<TRemoveDisk>(
        ctx,
        std::move(msg->RequestInfo),
        std::move(msg->DiskId),
        msg->Sync);
}

}   // namespace NCloud::NBlockStore::NStorage
