#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeActor final: public TActorBootstrapped<TCheckRangeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TCheckRangeRequest Request;

public:
    TCheckRangeActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TError error,
        NPrivateProto::TCheckRangeResponse response);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleCheckRangeResponse(
        const TEvService::TEvCheckRangeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCheckRangeActor::TCheckRangeActor(TRequestInfoPtr requestInfo, TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    if (!Request.GetBlocksCount()) {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "Blocks count should be supplied"));
        return;
    }

    auto request = std::make_unique<TEvService::TEvCheckRangeRequest>();
    request->Record.SetDiskId(Request.GetDiskId());
    request->Record.SetStartIndex(Request.GetStartIndex());
    request->Record.SetBlocksCount(Request.GetBlocksCount());

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Start check disk range for %s",
        Request.GetDiskId().c_str());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);

    Become(&TThis::StateWork);
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TCheckRangeResponse(),
        msg->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_CheckRange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error,
    NPrivateProto::TCheckRangeResponse response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    google::protobuf::util::MessageToJsonString(
        response,
        msg->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_CheckRange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCheckRangeActor::HandleCheckRangeResponse(
    const TEvService::TEvCheckRangeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = NPrivateProto::TCheckRangeResponse();
    response.MutableStatus()->CopyFrom(ev->Get()->Record.GetStatus());

    return ReplyAndDie(
        ctx,
        ev->Get()->Record.GetError(),
        std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCheckRangeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvCheckRangeResponse, HandleCheckRangeResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateCheckRangeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCheckRangeActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
