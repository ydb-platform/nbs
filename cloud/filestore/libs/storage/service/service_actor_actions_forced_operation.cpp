#include "service_actor.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TForcedOperationActionActor final
    : public TActorBootstrapped<TForcedOperationActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TForcedOperationActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TForcedOperationResponse& response);

private:
    STFUNC(StateWork);

    void HandleForcedOperationResponse(
        const TEvIndexTablet::TEvForcedOperationResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TForcedOperationActionActor::TForcedOperationActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TForcedOperationActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TForcedOperationRequest request;
    if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok()) {
        ReplyAndDie(
            ctx,
            TErrorResponse(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!request.GetFileSystemId()) {
        ReplyAndDie(
            ctx,
            TErrorResponse(E_ARGUMENT, "FileSystem id should be supplied"));
        return;
    }

    auto requestToTablet =
        std::make_unique<TEvIndexTablet::TEvForcedOperationRequest>();

    requestToTablet->Record = std::move(request);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TForcedOperationActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProtoPrivate::TForcedOperationResponse& response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        response.GetError());

    google::protobuf::util::MessageToJsonString(
        response,
        msg->Record.MutableOutput());

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TForcedOperationActionActor::HandleForcedOperationResponse(
    const TEvIndexTablet::TEvForcedOperationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->Record);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TForcedOperationActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvForcedOperationResponse,
            HandleForcedOperationResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE);
            break;
    }
}

} // namespace

IActorPtr TStorageServiceActor::CreateForcedOperationActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TForcedOperationActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
