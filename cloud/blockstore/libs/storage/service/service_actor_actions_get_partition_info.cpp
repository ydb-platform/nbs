#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetPartitionInfoActionActor final
    : public TActorBootstrapped<TGetPartitionInfoActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;

public:
    TGetPartitionInfoActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void GetPartitionInfo(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleGetPartitionInfoResponse(
        const TEvVolume::TEvGetPartitionInfoResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetPartitionInfoActionActor::TGetPartitionInfoActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TGetPartitionInfoActionActor::Bootstrap(const TActorContext& ctx)
{
    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (input.Has("DiskId")) {
        DiskId = input["DiskId"].GetStringRobust();
    }

    if (!DiskId) {
        HandleError(ctx, MakeError(E_ARGUMENT, "DiskId should be defined"));
        return;
    }

    GetPartitionInfo(ctx);
    Become(&TThis::StateWork);
}

void TGetPartitionInfoActionActor::GetPartitionInfo(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvGetPartitionInfoRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(DiskId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TGetPartitionInfoActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getpartitioninfo",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetPartitionInfoActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getpartitioninfo",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetPartitionInfoActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvGetPartitionInfoResponse, HandleGetPartitionInfoResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TGetPartitionInfoActionActor::HandleGetPartitionInfoResponse(
    const TEvVolume::TEvGetPartitionInfoResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    HandleSuccess(ctx, msg->Record.GetPayload());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateGetPartitionInfoActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TGetPartitionInfoActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
