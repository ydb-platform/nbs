#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDrainNodeActionActor final
    : public TActorBootstrapped<TDrainNodeActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TDrainNodeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void DrainNode(const TActorContext& ctx, bool keepDown);

    void HandleSuccess(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleDrainNodeResponse(
        TEvHiveProxy::TEvDrainNodeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDrainNodeActionActor::TDrainNodeActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TDrainNodeActionActor::Bootstrap(const TActorContext& ctx)
{
    NPrivateProto::TDrainNodeRequest drainRequest;
    if (!google::protobuf::util::JsonStringToMessage(Input, &drainRequest).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    DrainNode(ctx, drainRequest.GetKeepDown());
    Become(&TThis::StateWork);
}

void TDrainNodeActionActor::DrainNode(const TActorContext& ctx, bool keepDown)
{
    auto request =
        std::make_unique<TEvHiveProxy::TEvDrainNodeRequest>(keepDown);

    NCloud::Send(
        ctx,
        MakeHiveProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDrainNodeActionActor::HandleSuccess(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TDrainNodeResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_drainnode",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TDrainNodeActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_drainnode",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDrainNodeActionActor::HandleDrainNodeResponse(
    TEvHiveProxy::TEvDrainNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    HandleSuccess(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDrainNodeActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvHiveProxy::TEvDrainNodeResponse, HandleDrainNodeResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateDrainNodeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TDrainNodeActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
