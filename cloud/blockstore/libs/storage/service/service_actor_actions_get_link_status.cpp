#include "service_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/cell_proxy.h>
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
    TString LeaderCellId;
    TString FollowerDiskId;
    TString FollowerCellId;
    TString LinkUUID;

    NProto::TError Parse(const TString& inputStr);
};

class TGetLinkStatusActionActor final
    : public TActorBootstrapped<TGetLinkStatusActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TString CellId;
    TRequest Request;

public:
    TGetLinkStatusActionActor(
        TRequestInfoPtr requestInfo,
        TString input,
        TString cellId);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendRequestToLeader(const TActorContext& ctx);
    void SendRequestToFollower(const TActorContext& ctx);
    void SendRemoteRequest(const TActorContext& ctx, const TString& cellId);
    void HandleRemoteResponse(
        const TEvCellProxy::TEvExecuteActionResponse::TPtr& ev,
        const TActorContext& ctx);

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
    LeaderCellId = getValue("LeaderCellId");
    FollowerDiskId = getValue("FollowerDiskId");
    FollowerCellId = getValue("FollowerCellId");
    LinkUUID = getValue("LinkUUID");

    if (!LeaderDiskId || !FollowerDiskId) {
        return MakeError(
            E_ARGUMENT,
            "LeaderDiskId and FollowerDiskId should be defined");
    }

    return MakeError(S_OK);
}

TGetLinkStatusActionActor::TGetLinkStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input,
    TString cellId)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
    , CellId(std::move(cellId))
{}

void TGetLinkStatusActionActor::Bootstrap(const TActorContext& ctx)
{
    auto error = Request.Parse(Input);
    if (HasError(error)) {
        HandleError(ctx, error);
        return;
    }
    if (!Request.LeaderCellId) {
        Request.LeaderCellId = CellId;
    }
    if (!Request.FollowerCellId) {
        Request.FollowerCellId = Request.LeaderCellId;
    }

    Become(&TThis::StateWork);
    if (CellId == Request.FollowerCellId &&
        Request.FollowerCellId != Request.LeaderCellId)
    {
        SendRequestToFollower(ctx);
    } else {
        SendRequestToLeader(ctx);
    }
}

void TGetLinkStatusActionActor::SendRequestToLeader(const TActorContext& ctx)
{
    if (Request.LeaderCellId != CellId) {
        SendRemoteRequest(ctx, Request.LeaderCellId);
        return;
    }
    auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(Request.LeaderDiskId);
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    request->Record.SetLinkUUID(Request.LinkUUID);
    request->Record.SetLeaderDiskId(Request.LeaderDiskId);
    request->Record.SetLeaderCellId(Request.LeaderCellId);
    request->Record.SetFollowerDiskId(Request.FollowerDiskId);
    request->Record.SetFollowerCellId(Request.FollowerCellId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        QueryLeader);
}

void TGetLinkStatusActionActor::SendRequestToFollower(const TActorContext& ctx)
{
    if (Request.FollowerCellId != CellId) {
        SendRemoteRequest(ctx, Request.FollowerCellId);
        return;
    }
    auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(Request.FollowerDiskId);
    request->Record.MutableHeaders()->SetExactDiskIdMatch(true);
    request->Record.SetLinkUUID(Request.LinkUUID);
    request->Record.SetLeaderDiskId(Request.LeaderDiskId);
    request->Record.SetLeaderCellId(Request.LeaderCellId);
    request->Record.SetFollowerDiskId(Request.FollowerDiskId);
    request->Record.SetFollowerCellId(Request.FollowerCellId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        QueryFollower);
}

void TGetLinkStatusActionActor::SendRemoteRequest(
    const TActorContext& ctx,
    const TString& cellId)
{
    NProto::TGetLinkStatusRequest payload;
    payload.SetLeaderDiskId(Request.LeaderDiskId);
    payload.SetLeaderCellId(Request.LeaderCellId);
    payload.SetFollowerDiskId(Request.FollowerDiskId);
    payload.SetFollowerCellId(Request.FollowerCellId);
    payload.SetLinkUUID(Request.LinkUUID);
    google::protobuf::util::JsonPrintOptions options;
    options.preserve_proto_field_names = true;
    TString input;
    const auto status = google::protobuf::util::MessageToJsonString(
        payload, &input, options);
    if (!status.ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, status.ToString()));
        return;
    }
    NProto::TExecuteActionRequest action;
    action.SetAction("getlinkstatus");
    action.SetInput(std::move(input));
    NCloud::Send(
        ctx,
        MakeCellProxyServiceId(),
        std::make_unique<TEvCellProxy::TEvExecuteActionRequest>(
            RequestInfo->CallContext, cellId, std::move(action)));
}

void TGetLinkStatusActionActor::HandleRemoteResponse(
    const TEvCellProxy::TEvExecuteActionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (HasError(ev->Get()->GetError())) {
        HandleError(ctx, ev->Get()->GetError());
        return;
    }
    HandleSuccess(ctx, ev->Get()->Output);
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
        HFunc(TEvCellProxy::TEvExecuteActionResponse, HandleRemoteResponse);

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

    const bool missingCrossCellLink =
        Request.LeaderCellId != Request.FollowerCellId && !HasError(error) &&
        msg->Record.GetStatus() == NProto::LINK_STATUS_NOT_FOUND;
    if (ev->Cookie == QueryLeader &&
        (IsNotFoundSchemeShardError(error) || missingCrossCellLink))
    {
        SendRequestToFollower(ctx);
        return;
    }

    if (HasError(error)) {
        HandleError(ctx, error);
        return;
    }

    // The tablet identity is private cleanup metadata. Keep the existing JSON
    // action response compatible with clients using strict protobuf JSON parsers.
    auto record = msg->Record;
    record.ClearVolumeTabletId();
    TString response;
    google::protobuf::util::MessageToJsonString(record, &response);

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
        std::move(input),
        CellId)};
}

}   // namespace NCloud::NBlockStore::NStorage
