#include "service_actor.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TConfigureAsFollowerActionActor final
    : public TActorBootstrapped<TConfigureAsFollowerActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TConfigureAsFollowerActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TConfigureAsFollowerResponse& response);

private:
    STFUNC(StateWork);

    void HandleConfigureAsFollowerResponse(
        const TEvIndexTablet::TEvConfigureAsFollowerResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TConfigureAsFollowerActionActor::TConfigureAsFollowerActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TConfigureAsFollowerActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TConfigureAsFollowerRequest request;
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
        std::make_unique<TEvIndexTablet::TEvConfigureAsFollowerRequest>();

    requestToTablet->Record = std::move(request);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TConfigureAsFollowerActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProtoPrivate::TConfigureAsFollowerResponse& response)
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

void TConfigureAsFollowerActionActor::HandleConfigureAsFollowerResponse(
    const TEvIndexTablet::TEvConfigureAsFollowerResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->Record);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TConfigureAsFollowerActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvConfigureAsFollowerResponse,
            HandleConfigureAsFollowerResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE);
            break;
    }
}

} // namespace

IActorPtr TStorageServiceActor::CreateConfigureAsFollowerActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TConfigureAsFollowerActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
