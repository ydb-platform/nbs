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

class TReplaceDeviceActionActor final
    : public TActorBootstrapped<TReplaceDeviceActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;
    TString DeviceId;
    TString DeviceReplacementId;

    NProto::TError Error;

public:
    TReplaceDeviceActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleSuccess(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleReplaceDeviceResponse(
        const TEvDiskRegistry::TEvReplaceDeviceResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TReplaceDeviceActionActor::TReplaceDeviceActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TReplaceDeviceActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!Input) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty input"));
        return;
    }

    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (input.Has("DiskId")) {
        DiskId = input["DiskId"].GetString();
    }

    if (input.Has("DeviceId")) {
        DeviceId = input["DeviceId"].GetString();
    }

    if (input.Has("DeviceReplacementId")) {
        DeviceReplacementId = input["DeviceReplacementId"].GetString();
    }

    Become(&TThis::StateWork);

    auto request = std::make_unique<TEvDiskRegistry::TEvReplaceDeviceRequest>();

    request->Record.SetDiskId(DiskId);
    request->Record.SetDeviceUUID(DeviceId);
    request->Record.SetDeviceReplacementUUID(DeviceReplacementId);

    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TReplaceDeviceActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_replacedevice",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TReplaceDeviceActionActor::HandleSuccess(
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_replacedevice",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TReplaceDeviceActionActor::HandleReplaceDeviceResponse(
    const TEvDiskRegistry::TEvReplaceDeviceResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError());
        return;
    }

    HandleSuccess(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TReplaceDeviceActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvReplaceDeviceResponse,
            HandleReplaceDeviceResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateReplaceDeviceActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TReplaceDeviceActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
