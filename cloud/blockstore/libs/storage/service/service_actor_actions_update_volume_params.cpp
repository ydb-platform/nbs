#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateVolumeParamsActor final
    : public TActorBootstrapped<TUpdateVolumeParamsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NProto::TUpdateVolumeParamsRequest Request;

public:
    TUpdateVolumeParamsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void UpdateVolumeParams(const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleUpdateVolumeParamsResponse(
        const TEvVolume::TEvUpdateVolumeParamsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TUpdateVolumeParamsActor::TUpdateVolumeParamsActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TUpdateVolumeParamsActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    UpdateVolumeParams(ctx);
}

void TUpdateVolumeParamsActor::UpdateVolumeParams(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending update volume params request for volume %s",
        Request.GetDiskId().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::make_unique<TEvVolume::TEvUpdateVolumeParamsRequest>(
            MakeIntrusive<TCallContext>(),
            std::move(Request))
    );
}

void TUpdateVolumeParamsActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_UpdateVolumeParams",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TUpdateVolumeParamsActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TUpdateVolumeParamsActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TUpdateVolumeParamsActor::HandleUpdateVolumeParamsResponse(
    const TEvVolume::TEvUpdateVolumeParamsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    TString responseStr;
    google::protobuf::util::MessageToJsonString(msg->Record, &responseStr);

    HandleSuccess(ctx, responseStr);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdateVolumeParamsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvUpdateVolumeParamsResponse,
            HandleUpdateVolumeParamsResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateUpdateVolumeParamsActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TUpdateVolumeParamsActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
