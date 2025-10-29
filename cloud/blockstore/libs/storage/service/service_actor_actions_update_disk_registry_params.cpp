#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/common/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateDiskRegistryAgentListParamsActor final
    : public TActorBootstrapped<TUpdateDiskRegistryAgentListParamsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TUpdateDiskRegistryAgentListParamsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleUpdateDiskRegistryAgentListParamsResponse(
        const TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TUpdateDiskRegistryAgentListParamsActor::TUpdateDiskRegistryAgentListParamsActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TUpdateDiskRegistryAgentListParamsActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsRequest>(
        RequestInfo->CallContext);

    if (!google::protobuf::util::JsonStringToMessage(Input, request->Record.MutableParams()).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    Become(&TThis::StateWork);
    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TUpdateDiskRegistryAgentListParamsActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_updatediskregistryagentlistparams",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TUpdateDiskRegistryAgentListParamsActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_updatediskregistryagentlistparams",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdateDiskRegistryAgentListParamsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsResponse,
            HandleUpdateDiskRegistryAgentListParamsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TUpdateDiskRegistryAgentListParamsActor::HandleUpdateDiskRegistryAgentListParamsResponse(
    const TEvDiskRegistry::TEvUpdateDiskRegistryAgentListParamsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto error = msg->GetError();
    if (error.GetCode() == E_NOT_FOUND) {
        SetErrorProtoFlag(error, NCloud::NProto::EF_SILENT);
    }

    if (HasError(error)) {
        HandleError(ctx, error);
        return;
    }

    TString response;
    google::protobuf::util::MessageToJsonString(msg->Record, &response);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Execute action private API: disk registry params updater response: %s",
        response.data());

    HandleSuccess(ctx, response);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateUpdateDiskRegistryAgentListParamsActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TUpdateDiskRegistryAgentListParamsActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
