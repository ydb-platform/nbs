#include "service_actor.h"

#include "helpers.h"

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

class TSetHasXAttrsActionActor final
    : public TActorBootstrapped<TSetHasXAttrsActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TSetHasXAttrsActionActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TSetHasXAttrsResponse& response);

private:
    STFUNC(StateWork);

    void HandleSetHasXAttrsResponse(
        const TEvIndexTablet::TEvSetHasXAttrsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSetHasXAttrsActionActor::TSetHasXAttrsActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TSetHasXAttrsActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TSetHasXAttrsRequest request;
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

    LOG_INFO(ctx, TFileStoreComponents::SERVICE,
        "Starting to set HasXAttrs flag for %s",
        request.GetFileSystemId().Quote().c_str());

    auto requestToTablet =
        std::make_unique<TEvIndexTablet::TEvSetHasXAttrsRequest>();
    auto& record = requestToTablet->Record;
    record.SetFileSystemId(request.GetFileSystemId());

    record.SetValue(request.GetValue());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TSetHasXAttrsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProtoPrivate::TSetHasXAttrsResponse& response)
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

void TSetHasXAttrsActionActor::HandleSetHasXAttrsResponse(
    const TEvIndexTablet::TEvSetHasXAttrsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSetHasXAttrsActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvSetHasXAttrsResponse,
            HandleSetHasXAttrsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

NActors::IActorPtr TStorageServiceActor::CreateSetHasXAttrsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TSetHasXAttrsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
