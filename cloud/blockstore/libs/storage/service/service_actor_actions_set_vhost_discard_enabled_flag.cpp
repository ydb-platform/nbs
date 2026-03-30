#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>

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

class TSetVhostDiscardFlagActionActor final
    : public TActorBootstrapped<TSetVhostDiscardFlagActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TSetVhostDiscardFlagActionRequest Request;

public:
    TSetVhostDiscardFlagActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendRequest(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleSetVhostDiscardFlagResponse(
        const TEvService::TEvSetVhostDiscardFlagResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSetVhostDiscardFlagActionActor::TSetVhostDiscardFlagActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TSetVhostDiscardFlagActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    SendRequest(ctx);
}

void TSetVhostDiscardFlagActionActor::SendRequest(
    const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending SetVhostDiscardFlagRequest for volume %s",
        Request.GetDiskId().Quote().c_str());

    auto request = std::make_unique<TEvService::TEvSetVhostDiscardFlagRequest>(
        Request.GetDiskId(),
        Request.GetVhostDiscardEnabled(),
        Request.GetConfigVersion());

    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
}

void TSetVhostDiscardFlagActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TSetVhostDiscardFlagActionResponse(),
        response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_setvhostdiscardenabledflag",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TSetVhostDiscardFlagActionActor::HandleSetVhostDiscardFlagResponse(
    const TEvService::TEvSetVhostDiscardFlagResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, msg->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSetVhostDiscardFlagActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvService::TEvSetVhostDiscardFlagResponse,
            HandleSetVhostDiscardFlagResponse);

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

TResultOrError<IActorPtr>
TServiceActor::CreateSetVhostDiscardFlagActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TSetVhostDiscardFlagActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
