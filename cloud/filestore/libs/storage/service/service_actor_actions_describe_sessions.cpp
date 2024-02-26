#include "service_actor.h"

#include "helpers.h"

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

class TDescribeSessionsActionActor final
    : public TActorBootstrapped<TDescribeSessionsActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TDescribeSessionsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TDescribeSessionsResponse& response);

private:
    STFUNC(StateWork);

    void HandleDescribeSessionsResponse(
        const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeSessionsActionActor::TDescribeSessionsActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{
    ActivityType = TFileStoreActivities::SERVICE_WORKER;
}

void TDescribeSessionsActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TDescribeSessionsRequest request;
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
        std::make_unique<TEvIndexTablet::TEvDescribeSessionsRequest>();

    auto& record = requestToTablet->Record;
    record.SetFileSystemId(request.GetFileSystemId());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TDescribeSessionsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProtoPrivate::TDescribeSessionsResponse& response)
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

void TDescribeSessionsActionActor::HandleDescribeSessionsResponse(
    const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->Record);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeSessionsActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvDescribeSessionsResponse,
            HandleDescribeSessionsResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE);
            break;
    }
}

} // namespace

IActorPtr TStorageServiceActor::CreateDescribeSessionsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TDescribeSessionsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
