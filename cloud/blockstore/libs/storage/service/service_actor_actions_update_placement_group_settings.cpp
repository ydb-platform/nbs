#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdatePlacementGroupSettingsActor final
    : public TActorBootstrapped<TUpdatePlacementGroupSettingsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NProto::TError Error;

public:
    TUpdatePlacementGroupSettingsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TUpdatePlacementGroupSettingsResponse& proto);

private:
    STFUNC(StateWork);

    void HandleUpdatePlacementGroupSettingsResponse(
        const TEvDiskRegistry::TEvUpdatePlacementGroupSettingsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TUpdatePlacementGroupSettingsActor::TUpdatePlacementGroupSettingsActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TUpdatePlacementGroupSettingsActor::Bootstrap(const TActorContext& ctx)
{
    using TRequest = TEvDiskRegistry::TEvUpdatePlacementGroupSettingsRequest;
    auto request = std::make_unique<TRequest>();

    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record).ok()) {
        Error = MakeError(E_ARGUMENT, "Failed to parse input");
        ReplyAndDie(ctx, {});
        return;
    }

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TUpdatePlacementGroupSettingsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TUpdatePlacementGroupSettingsResponse& proto)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    google::protobuf::util::MessageToJsonString(
        proto,
        response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_updateplacementgroupsettings",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TUpdatePlacementGroupSettingsActor::HandleUpdatePlacementGroupSettingsResponse(
    const TEvDiskRegistry::TEvUpdatePlacementGroupSettingsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx, std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdatePlacementGroupSettingsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvUpdatePlacementGroupSettingsResponse,
            HandleUpdatePlacementGroupSettingsResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateUpdatePlacementGroupSettingsActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TUpdatePlacementGroupSettingsActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
