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

class TSetVhostDiscardEnabledFlagActionActor final
    : public TActorBootstrapped<TSetVhostDiscardEnabledFlagActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TSetVhostDiscardEnabledFlagRequest Request;

public:
    TSetVhostDiscardEnabledFlagActionActor(
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

TSetVhostDiscardEnabledFlagActionActor::TSetVhostDiscardEnabledFlagActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TSetVhostDiscardEnabledFlagActionActor::Bootstrap(const TActorContext& ctx)
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

void TSetVhostDiscardEnabledFlagActionActor::SendRequest(
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
        Request.GetVhostDiscardEnabled());

    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
}

void TSetVhostDiscardEnabledFlagActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TSetVhostDiscardEnabledFlagResponse(),
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

void TSetVhostDiscardEnabledFlagActionActor::HandleSetVhostDiscardFlagResponse(
    const TEvService::TEvSetVhostDiscardFlagResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, msg->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSetVhostDiscardEnabledFlagActionActor::StateWork)
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
TServiceActor::CreateSetVhostDiscardEnabledFlagActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TSetVhostDiscardEnabledFlagActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
