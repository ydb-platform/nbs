#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
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

class TReallocateDiskActionActor final
    : public TActorBootstrapped<TReallocateDiskActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;

    NProto::TError Error;

public:
    TReallocateDiskActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleSuccess(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleReallocateDiskResponse(
        const TEvVolume::TEvReallocateDiskResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TReallocateDiskActionActor::TReallocateDiskActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TReallocateDiskActionActor::Bootstrap(const TActorContext& ctx)
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

    if (!input.Has("DiskId")) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty DiskId"));
        return;
    }

    DiskId = input["DiskId"].GetString();

    Become(&TThis::StateWork);

    auto request = std::make_unique<TEvVolume::TEvReallocateDiskRequest>();
    request->Record.SetDiskId(DiskId);

    ctx.Send(MakeVolumeProxyServiceId(), request.release());
}

void TReallocateDiskActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_reallocatedisk",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TReallocateDiskActionActor::HandleSuccess(
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_reallocatedisk",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TReallocateDiskActionActor::HandleReallocateDiskResponse(
    const TEvVolume::TEvReallocateDiskResponse::TPtr& ev,
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

STFUNC(TReallocateDiskActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvReallocateDiskResponse,
            HandleReallocateDiskResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateReallocateDiskActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TReallocateDiskActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
