#include "service_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetStorageConfigFieldsActionActor final
    : public TActorBootstrapped<TGetStorageConfigFieldsActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TGetStorageConfigFieldsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TGetStorageConfigFieldsResponse& response);

private:
    STFUNC(StateWork);

    void HandleGetStorageConfigFieldsResponse(
        const TEvIndexTablet::TEvGetStorageConfigFieldsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetStorageConfigFieldsActionActor::TGetStorageConfigFieldsActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TGetStorageConfigFieldsActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TGetStorageConfigFieldsRequest request;
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
        std::make_unique<TEvIndexTablet::TEvGetStorageConfigFieldsRequest>();

    auto& record = requestToTablet->Record;
    *record.MutableStorageConfigFields() = request.GetStorageConfigFields();
    record.SetFileSystemId(request.GetFileSystemId());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TGetStorageConfigFieldsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProtoPrivate::TGetStorageConfigFieldsResponse& response)
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

void TGetStorageConfigFieldsActionActor::HandleGetStorageConfigFieldsResponse(
    const TEvIndexTablet::TEvGetStorageConfigFieldsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->Record);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetStorageConfigFieldsActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvGetStorageConfigFieldsResponse,
            HandleGetStorageConfigFieldsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

} // namespace

IActorPtr TStorageServiceActor::CreateGetStorageConfigFieldsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TGetStorageConfigFieldsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
