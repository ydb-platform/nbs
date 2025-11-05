#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryChangeStateActor final
    : public TActorBootstrapped<TDiskRegistryChangeStateActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TDiskRegistryChangeStateRequest Request;

    NProto::TError Error;

public:
    TDiskRegistryChangeStateActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChangeAgentStateResponse(
        const TEvDiskRegistry::TEvChangeAgentStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleChangeDeviceStateResponse(
        const TEvDiskRegistry::TEvChangeDeviceStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDisableAgentResponse(
        const TEvDiskRegistry::TEvDisableAgentResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryChangeStateActor::TDiskRegistryChangeStateActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TDiskRegistryChangeStateActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        Error = MakeError(E_ARGUMENT, "Failed to parse input");
        ReplyAndDie(ctx);
        return;
    }

    if (Request.GetMessage().empty()) {
        Error = MakeError(E_ARGUMENT, "Message should not be empty");
        ReplyAndDie(ctx);
        return;
    }

    std::unique_ptr<IEventBase> event;

    switch (Request.GetChangeStateCase()) {
        case NPrivateProto::TDiskRegistryChangeStateRequest::kChangeDeviceState: {
            const auto& cs = Request.GetChangeDeviceState();

            if (cs.GetState() > NProto::EDeviceState_MAX) {
                Error = MakeError(
                    E_ARGUMENT,
                    Sprintf("Invalid DeviceState: %u", cs.GetState())
                );
            } else {
                const auto state = static_cast<NProto::EDeviceState>(cs.GetState());
                auto request =
                    std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateRequest>();
                request->Record.SetDeviceUUID(cs.GetDeviceUUID());
                request->Record.SetDeviceState(state);
                request->Record.SetReason("private api: " + Request.GetMessage());
                event.reset(request.release());
            }

            break;
        }

        case NPrivateProto::TDiskRegistryChangeStateRequest::kChangeAgentState: {
            const auto& cs = Request.GetChangeAgentState();

            if (cs.GetState() > NProto::EAgentState_MAX) {
                Error = MakeError(
                    E_ARGUMENT,
                    Sprintf("Invalid AgentState: %u", cs.GetState())
                );
            } else {
                const auto state = static_cast<NProto::EAgentState>(cs.GetState());
                auto request = std::make_unique<TEvDiskRegistry::TEvChangeAgentStateRequest>();
                request->Record.SetAgentId(cs.GetAgentId());
                request->Record.SetAgentState(state);
                request->Record.SetReason("private api: " + Request.GetMessage());
                event.reset(request.release());
            }

            break;
        }

        case NPrivateProto::TDiskRegistryChangeStateRequest::kDisableAgent: {
            const auto& da = Request.GetDisableAgent();

            auto request =
                std::make_unique<TEvDiskRegistry::TEvDisableAgentRequest>();
            request->Record.SetAgentId(da.GetAgentId());
            for (const auto& deviceId: da.GetDeviceUUIDs()) {
                *request->Record.AddDeviceUUIDs() = deviceId;
            }
            event.reset(request.release());

            break;
        }

        default: {
            Error = MakeError(E_ARGUMENT, "ChangeState not set");
            break;
        }
    }

    if (event) {
        Become(&TThis::StateWork);
        ctx.Send(MakeDiskRegistryProxyServiceId(), event.release());
    } else {
        ReplyAndDie(ctx);
    }
}

void TDiskRegistryChangeStateActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TDiskRegistryChangeStateResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_diskregistrychangestate",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryChangeStateActor::HandleChangeDeviceStateResponse(
    const TEvDiskRegistry::TEvChangeDeviceStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryChangeStateActor::HandleChangeAgentStateResponse(
    const TEvDiskRegistry::TEvChangeAgentStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryChangeStateActor::HandleDisableAgentResponse(
    const TEvDiskRegistry::TEvDisableAgentResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskRegistryChangeStateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvChangeDeviceStateResponse,
            HandleChangeDeviceStateResponse);

        HFunc(
            TEvDiskRegistry::TEvChangeAgentStateResponse,
            HandleChangeAgentStateResponse);

        HFunc(
            TEvDiskRegistry::TEvDisableAgentResponse,
            HandleDisableAgentResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateDiskRegistryChangeStateActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TDiskRegistryChangeStateActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
