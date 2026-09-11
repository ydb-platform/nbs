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

class TConfigureShardsActionActor final
    : public TActorBootstrapped<TConfigureShardsActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TConfigureShardsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TConfigureShardsResponse& response);

private:
    STFUNC(StateWork);

    void HandleConfigureShardsResponse(
        const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TConfigureShardsActionActor::TConfigureShardsActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TConfigureShardsActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TConfigureShardsRequest request;
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
        std::make_unique<TEvIndexTablet::TEvConfigureShardsRequest>();

    requestToTablet->Record = std::move(request);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TConfigureShardsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProtoPrivate::TConfigureShardsResponse& response)
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

void TConfigureShardsActionActor::HandleConfigureShardsResponse(
    const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->Record);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TConfigureShardsActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvConfigureShardsResponse,
            HandleConfigureShardsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

} // namespace

IActorPtr TStorageServiceActor::CreateConfigureShardsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TConfigureShardsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
