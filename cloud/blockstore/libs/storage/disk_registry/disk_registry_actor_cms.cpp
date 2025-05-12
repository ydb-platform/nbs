#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCmsRequestActor final
    : public TActorBootstrapped<TCmsRequestActor>
{
private:
    const TActorId Owner;
    const TRequestInfoPtr RequestInfo;

    google::protobuf::RepeatedPtrField<NProto::TAction> Requests;

    int CurrentRequest = 0;

    std::unique_ptr<TEvService::TEvCmsActionResponse> Response;

public:
    TCmsRequestActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        google::protobuf::RepeatedPtrField<NProto::TAction> requests);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx);

    void SendNextRequest(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    template <typename TResponse>
    void HandleCmsActionResponse(
        const TResponse& response,
        const TActorContext& ctx);

    void HandleCmsActionResponseProto(
        const TEvDiskRegistry::TEvGetDependentDisksResponse& response,
        const TActorContext& ctx);

    void HandleUpdateHostDeviceStateResponse(
        const TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateCmsHostStateResponse(
        const TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePurgeHostCmsResponse(
        const TEvDiskRegistryPrivate::TEvPurgeHostCmsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetDependentDisksResponse(
        const TEvDiskRegistry::TEvGetDependentDisksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

TCmsRequestActor::TCmsRequestActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        google::protobuf::RepeatedPtrField<NProto::TAction> requests)
    : Owner(owner)
    , RequestInfo(std::move(requestInfo))
    , Requests(std::move(requests))
    , Response(std::make_unique<TEvService::TEvCmsActionResponse>())
{}

void TCmsRequestActor::Bootstrap(const TActorContext& ctx)
{
    SendNextRequest(ctx);

    Become(&TThis::StateWork);
}

void TCmsRequestActor::SendNextRequest(const TActorContext& ctx)
{
    if (CurrentRequest == Requests.size()) {
        ReplyAndDie(ctx);
        return;
    }

    const auto& action = Requests.Get(CurrentRequest);

    switch (action.GetType()) {
        case NProto::TAction_EType::TAction_EType_REMOVE_HOST: {
            using TRequest =
                TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest;
            auto request = std::make_unique<TRequest>(
                action.GetHost(),
                NProto::EAgentState::AGENT_STATE_WARNING,
                action.GetDryRun());

            NCloud::Send(
                ctx,
                Owner,
                std::move(request));
            break;
        }

        case NProto::TAction_EType::TAction_EType_REMOVE_DEVICE: {
            using TRequest =
                TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateRequest;
            auto request = std::make_unique<TRequest>(
                action.GetHost(),
                action.GetDevice(),
                NProto::EDeviceState::DEVICE_STATE_WARNING,
                /*shouldResumeDevice=*/false,
                action.GetDryRun());

            NCloud::Send(
                ctx,
                Owner,
                std::move(request));

            break;
        }

        case NProto::TAction_EType::TAction_EType_GET_DEPENDENT_DISKS: {
            using TRequest =
                TEvDiskRegistry::TEvGetDependentDisksRequest;
            auto request = std::make_unique<TRequest>();
            request->Record.SetHost(action.GetHost());
            request->Record.SetPath(action.GetDevice());
            request->Record.SetIgnoreReplicatedDisks(true);

            NCloud::Send(
                ctx,
                Owner,
                std::move(request));
            break;
        }

        case NProto::TAction_EType::TAction_EType_ADD_DEVICE: {
            using TRequest =
                TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateRequest;
            auto request = std::make_unique<TRequest>(
                action.GetHost(),
                action.GetDevice(),
                NProto::EDeviceState::DEVICE_STATE_ONLINE,
                /*shouldResumeDevice=*/false,
                action.GetDryRun());

            NCloud::Send(
                ctx,
                Owner,
                std::move(request));
            break;
        }

        case NProto::TAction_EType::TAction_EType_ADD_HOST: {
            using TRequest =
                TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest;
            auto request = std::make_unique<TRequest>(
                action.GetHost(),
                NProto::EAgentState::AGENT_STATE_ONLINE,
                action.GetDryRun());

            NCloud::Send(
                ctx,
                Owner,
                std::move(request));
            break;
        }

        case NProto::TAction_EType::TAction_EType_PURGE_HOST: {
            using TRequest = TEvDiskRegistryPrivate::TEvPurgeHostCmsRequest;
            auto request = std::make_unique<TRequest>(
                action.GetHost(),
                action.GetDryRun());

            NCloud::Send(ctx, Owner, std::move(request));
            break;
        }

        default: {
            auto& result = *Response->Record.MutableActionResults()->Add();
            *result.MutableResult() = MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Wrong action: " << (ui32)action.GetType());
            ++CurrentRequest;
            SendNextRequest(ctx);
        }
    }
}

void TCmsRequestActor::ReplyAndDie(const TActorContext& ctx)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(Response));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
void TCmsRequestActor::HandleCmsActionResponse(
    const TResponse& response,
    const TActorContext& ctx)
{
    auto& result = *Response->Record.MutableActionResults()->Add();
    *result.MutableResult() = response.GetError();
    result.SetTimeout(response.Timeout.Seconds());
    for (auto& diskId: response.DependentDiskIds) {
        *result.AddDependentDisks() = std::move(diskId);
    }

    ++CurrentRequest;
    SendNextRequest(ctx);
}

void TCmsRequestActor::HandlePurgeHostCmsResponse(
    const TEvDiskRegistryPrivate::TEvPurgeHostCmsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleCmsActionResponse(*ev->Get(), ctx);
}

void TCmsRequestActor::HandleCmsActionResponseProto(
    const TEvDiskRegistry::TEvGetDependentDisksResponse& response,
    const TActorContext& ctx)
{
    auto& result = *Response->Record.MutableActionResults()->Add();
    *result.MutableResult() = response.Record.GetError();
    result.SetTimeout(response.Record.GetTimeout());
    for (auto& diskId: response.Record.GetDependentDiskIds()) {
        *result.AddDependentDisks() = std::move(diskId);
    }

    ++CurrentRequest;
    SendNextRequest(ctx);
}

void TCmsRequestActor::HandleUpdateCmsHostStateResponse(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleCmsActionResponse(*ev->Get(), ctx);
}

void TCmsRequestActor::HandleUpdateHostDeviceStateResponse(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleCmsActionResponse(*ev->Get(), ctx);
}

void TCmsRequestActor::HandleGetDependentDisksResponse(
    const TEvDiskRegistry::TEvGetDependentDisksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    HandleCmsActionResponseProto(*ev->Get(), ctx);
}

void TCmsRequestActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    *Response->Record.MutableError() =
        MakeTabletIsDeadError(E_REJECTED, __LOCATION__);

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCmsRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse,
            HandleUpdateHostDeviceStateResponse);

        HFunc(
            TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse,
            HandleUpdateCmsHostStateResponse);

        HFunc(
            TEvDiskRegistryPrivate::TEvPurgeHostCmsResponse,
            HandlePurgeHostCmsResponse);

        HFunc(
            TEvDiskRegistry::TEvGetDependentDisksResponse,
            HandleGetDependentDisksResponse);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}; // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCmsAction(
    const TEvService::TEvCmsActionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    auto actor = NCloud::Register<TCmsRequestActor>(
        ctx,
        SelfId(),
        std::move(requestInfo),
        ev->Get()->Record.GetActions());

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
