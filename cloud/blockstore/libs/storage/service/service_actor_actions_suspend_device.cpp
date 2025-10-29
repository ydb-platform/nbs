#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSuspendDeviceActionActor final
    : public TActorBootstrapped<TSuspendDeviceActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TSuspendDeviceActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleSuspendDeviceResponse(
        const TEvDiskRegistry::TEvSuspendDeviceResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSuspendDeviceActionActor::TSuspendDeviceActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TSuspendDeviceActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!Input) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Empty input"));
        return;
    }

    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        ReplyAndDie(ctx,
            MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (!input.Has("DeviceId")) {
        ReplyAndDie(ctx,
            MakeError(E_ARGUMENT, "DeviceId is required"));
        return;
    }

    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvSuspendDeviceRequest>();

    request->Record.SetDeviceId(input["DeviceId"].GetString());

    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TSuspendDeviceActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_suspenddevice",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendDeviceActionActor::HandleSuspendDeviceResponse(
    const TEvDiskRegistry::TEvSuspendDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    ReplyAndDie(ctx, {});
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSuspendDeviceActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvSuspendDeviceResponse,
            HandleSuspendDeviceResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateSuspendDeviceActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TSuspendDeviceActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
