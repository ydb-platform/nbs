#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResumeDeviceActor final: public TActorBootstrapped<TResumeDeviceActor>
{
private:
    const TActorId Owner;
    const TRequestInfoPtr RequestInfo;
    NProto::TResumeDeviceRequest Request;

public:
    TResumeDeviceActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        NProto::TResumeDeviceRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error = {});

private:
    STFUNC(StateWork);

    void HandleResponse(
        const TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse::TPtr&
            response,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TResumeDeviceActor::TResumeDeviceActor(
    const TActorId& owner,
    TRequestInfoPtr requestInfo,
    NProto::TResumeDeviceRequest request)
    : Owner(owner)
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TResumeDeviceActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<
        TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateRequest>(
        Request.GetAgentId(),
        Request.GetPath(),
        NProto::EDeviceState::DEVICE_STATE_ONLINE,
        /*shouldResumeDevice=*/true,
        Request.GetDryRun());

    NCloud::Send(ctx, Owner, std::move(request));
}

void TResumeDeviceActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        std::make_unique<TEvService::TEvResumeDeviceResponse>(std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TResumeDeviceActor::HandleResponse(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response = std::make_unique<TEvService::TEvResumeDeviceResponse>();
    *response->Record.MutableError() = msg->GetError();
    if (HasError(msg->GetError()) && !Request.GetDryRun()) {
        ReportDiskRegistryResumeDeviceFailed(
            TStringBuilder() << "AgentId: " << Request.GetAgentId()
                             << "; Path: " << Request.GetPath()
                             << "; Error: " << msg->GetErrorReason());
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TResumeDeviceActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TResumeDeviceActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse,
            HandleResponse);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

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

void TDiskRegistryActor::HandleResumeDevice(
    const TEvService::TEvResumeDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const TString& agentId = msg->Record.GetAgentId();
    const TString& path = msg->Record.GetPath();
    bool dryRun = msg->Record.GetDryRun();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received ResumeDevice request: AgentId=%s Path=%s DryRun=%d",
        TabletID(),
        agentId.c_str(),
        path.c_str(),
        dryRun);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    auto actor = NCloud::Register<TResumeDeviceActor>(
        ctx,
        SelfId(),
        std::move(requestInfo),
        ev->Get()->Record);

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
