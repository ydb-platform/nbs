#include "service_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 QueryLeader = 1;
constexpr ui64 QueryFollower = 2;

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    TString LeaderDiskId;
    TString LeaderShardId;
    TString FollowerDiskId;
    TString FollowerShardId;

    NProto::TError Parse(const TString& inputStr);
};

class TGetLinkStatusActionActor final
    : public TActorBootstrapped<TGetLinkStatusActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    TRequest Request;

public:
    TGetLinkStatusActionActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendRequestToLeader(const TActorContext& ctx);
    void SendRequestToFollower(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleGetLinkStatusResponse(
        const TEvVolume::TEvGetLinkStatusResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError TRequest::Parse(const TString& inputStr)
{
    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(inputStr, &input, false)) {
        return MakeError(E_ARGUMENT, "Input should be in JSON format");
    }

    auto getValue = [&input](const TString& key)
    {
        return input.Has(key) ? input[key].GetStringRobust() : TString();
    };

    LeaderDiskId = getValue("LeaderDiskId");
    LeaderShardId = getValue("LeaderShardId");
    FollowerDiskId = getValue("FollowerDiskId");
    FollowerShardId = getValue("FollowerShardId");

    if (!LeaderDiskId || !FollowerDiskId) {
        return MakeError(
            E_ARGUMENT,
            "LeaderDiskId and FollowerDiskId should be defined");
    }

    return MakeError(S_OK);
}

TGetLinkStatusActionActor::TGetLinkStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TGetLinkStatusActionActor::Bootstrap(const TActorContext& ctx)
{
    auto error = Request.Parse(Input);
    if (HasError(error)) {
        HandleError(ctx, error);
        return;
    }

    SendRequestToLeader(ctx);

    Become(&TThis::StateWork);
}

void TGetLinkStatusActionActor::SendRequestToLeader(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(Request.LeaderDiskId);
    request->Record.SetLeaderDiskId(Request.LeaderDiskId);
    request->Record.SetLeaderShardId(Request.LeaderShardId);
    request->Record.SetFollowerDiskId(Request.FollowerDiskId);
    request->Record.SetFollowerShardId(Request.FollowerShardId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        QueryLeader);
}

void TGetLinkStatusActionActor::SendRequestToFollower(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(Request.FollowerDiskId);
    request->Record.SetLeaderDiskId(Request.LeaderDiskId);
    request->Record.SetLeaderShardId(Request.LeaderShardId);
    request->Record.SetFollowerDiskId(Request.FollowerDiskId);
    request->Record.SetFollowerShardId(Request.FollowerShardId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        QueryFollower);
}

void TGetLinkStatusActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getlinkstatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetLinkStatusActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getlinkstatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetLinkStatusActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvGetLinkStatusResponse, HandleGetLinkStatusResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TGetLinkStatusActionActor::HandleGetLinkStatusResponse(
    const TEvVolume::TEvGetLinkStatusResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (ev->Cookie == QueryLeader && IsNotFoundSchemeShardError(error)) {
        SendRequestToFollower(ctx);
        return;
    }

    if (HasError(error)) {
        HandleError(ctx, error);
        return;
    }

    TString response;
    google::protobuf::util::MessageToJsonString(msg->Record, &response);

    HandleSuccess(ctx, response);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateGetLinkStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TGetLinkStatusActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
