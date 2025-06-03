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

class TListNodeRefsActionActor final
    : public TActorBootstrapped<TListNodeRefsActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TStorageConfigPtr Config;

public:
    TListNodeRefsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TListNodeRefsResponse& response);

private:
    STFUNC(StateWork);

    void HandleListNodeRefsResponse(
        const TEvIndexTablet::TEvListNodeRefsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TListNodeRefsActionActor::TListNodeRefsActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TListNodeRefsActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TListNodeRefsRequest request;
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
        "Start to change storage config of %s",
        request.GetFileSystemId().Quote().c_str());

    auto requestToTablet = std::make_unique<TEvIndexTablet::TEvListNodeRefsRequest>();

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TListNodeRefsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProtoPrivate::TListNodeRefsResponse& response)
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

void TListNodeRefsActionActor::HandleListNodeRefsResponse(
    const TEvIndexTablet::TEvListNodeRefsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (SUCCEEDED(msg->GetStatus())) {
        NCloud::Send(
            ctx,
            ev->Sender,
            std::make_unique<TEvents::TEvPoisonPill>());
    }
    ReplyAndDie(ctx, msg->Record);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TListNodeRefsActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvListNodeRefsResponse,
            HandleListNodeRefsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}

NActors::IActorPtr TStorageServiceActor::CreateListNodeRefsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TListNodeRefsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
