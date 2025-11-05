#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/checkpoints.pb.h>

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

class TDeleteCheckpointDataActionActor final
    : public TActorBootstrapped<TDeleteCheckpointDataActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TDeleteCheckpointDataRequest Request;

public:
    TDeleteCheckpointDataActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void DeleteCheckpointData(const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleDeleteCheckpointDataResponse(
        const TEvVolume::TEvDeleteCheckpointDataResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDeleteCheckpointDataActionActor::TDeleteCheckpointDataActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TDeleteCheckpointDataActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    if (!Request.GetCheckpointId()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "CheckpointId should be supplied"));
        return;
    }

    DeleteCheckpointData(ctx);
}

void TDeleteCheckpointDataActionActor::DeleteCheckpointData(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending delete checkpoint data request for volume %s",
        Request.GetDiskId().Quote().c_str());

    auto request = std::make_unique<TEvVolume::TEvDeleteCheckpointDataRequest>();
    request->Record.SetDiskId(Request.GetDiskId());
    request->Record.SetCheckpointId(Request.GetCheckpointId());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request)
    );
}

void TDeleteCheckpointDataActionActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_deletecheckpointdata",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TDeleteCheckpointDataActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TDeleteCheckpointDataActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteCheckpointDataActionActor::HandleDeleteCheckpointDataResponse(
    const TEvVolume::TEvDeleteCheckpointDataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    NPrivateProto::TDeleteCheckpointDataResponse response;
    response.MutableError()->CopyFrom(msg->Record.GetError());

    TString responseStr;
    google::protobuf::util::MessageToJsonString(response, &responseStr);

    HandleSuccess(ctx, responseStr);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDeleteCheckpointDataActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvDeleteCheckpointDataResponse, HandleDeleteCheckpointDataResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateDeleteCheckpointDataActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TDeleteCheckpointDataActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
