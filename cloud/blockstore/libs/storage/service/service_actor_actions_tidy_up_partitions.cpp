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

class TTidyUpPartitionsActionActor final
    : public TActorBootstrapped<TTidyUpPartitionsActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TTidyUpPartitionsRequest Request;

public:
    TTidyUpPartitionsActionActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void TidyUpPartitions(const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleTidyUpPartitionsResponse(
        const TEvVolume::TEvTidyUpPartitionsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TTidyUpPartitionsActionActor::TTidyUpPartitionsActionActor(
    TRequestInfoPtr requestInfo, TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TTidyUpPartitionsActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    TidyUpPartitions(ctx);
}

void TTidyUpPartitionsActionActor::TidyUpPartitions(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
              "Sending tidy-up request for volume %s",
              Request.GetDiskId().Quote().c_str());

    auto request = std::make_unique<TEvVolume::TEvTidyUpPartitionsRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TTidyUpPartitionsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(ResponseSent_Service, RequestInfo->CallContext->LWOrbit,
            "ExecuteAction_tidyuppartitions",
            RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TTidyUpPartitionsActionActor::HandleSuccess(const TActorContext& ctx,
                                                 const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TTidyUpPartitionsActionActor::HandleError(const TActorContext& ctx,
                                               const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TTidyUpPartitionsActionActor::HandleTidyUpPartitionsResponse(
    const TEvVolume::TEvTidyUpPartitionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    NPrivateProto::TTidyUpPartitionsResponse response;

    TString responseStr;
    google::protobuf::util::MessageToJsonString(response, &responseStr);

    HandleSuccess(ctx, responseStr);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TTidyUpPartitionsActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvTidyUpPartitionsResponse,
              HandleTidyUpPartitionsResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE,
                                  __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateTidyUpPartitionsActionActor(
    TRequestInfoPtr requestInfo, TString input)
{
    return {std::make_unique<TTidyUpPartitionsActionActor>(
        std::move(requestInfo), std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
