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

class TCutTabletHistoryActor final: public TActorBootstrapped<TCutTabletHistoryActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TCutTabletHistoryRequest Request;

public:
    TCutTabletHistoryActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        TEvVolume::TEvCutTabletHistoryResponse response);

private:
    STFUNC(StateWork);

    void HandleCutTabletHistory(
        const TEvVolume::TEvCutTabletHistoryResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCutTabletHistoryActor::TCutTabletHistoryActor(TRequestInfoPtr requestInfo, TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TCutTabletHistoryActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    auto request = std::make_unique<TEvVolume::TEvCutTabletHistoryRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Start cut history for disk %s",
        Request.GetDiskId().c_str());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);

    Become(&TThis::StateWork);
}

void TCutTabletHistoryActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TCutTabletHistoryResponse(),
        msg->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_CutTabletHistory",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCutTabletHistoryActor::HandleCutTabletHistory(
    const TEvVolume::TEvCutTabletHistoryResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::move(ev->Get()->Record.GetError()));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCutTabletHistoryActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvCutTabletHistoryResponse, HandleCutTabletHistory);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateCutTabletHistoryActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCutTabletHistoryActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
