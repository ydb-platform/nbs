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

class TGetStorageConfigActionActor final
    : public TActorBootstrapped<TGetStorageConfigActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr StorageConfig;
    const TString Input;

public:
    TGetStorageConfigActionActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr storageConfig,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleError(const TActorContext& ctx, NProto::TError error);
    void HandleSuccess(
        const TActorContext& ctx,
        NProto::TStorageConfig config);

private:
    STFUNC(StateWork);

    void HandleGetStorageConfigResponse(
        const TEvIndexTablet::TEvGetStorageConfigResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetStorageConfigActionActor::TGetStorageConfigActionActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr storageConfig,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , StorageConfig(std::move(storageConfig))
    , Input(std::move(input))
{}

void TGetStorageConfigActionActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TGetStorageConfigRequest request;
    if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok()) {
        HandleError(
            ctx,
            MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!request.GetFileSystemId()) {
        HandleSuccess(ctx, StorageConfig->GetStorageConfigProto());
        return;
    }

    auto requestToTablet =
        std::make_unique<TEvIndexTablet::TEvGetStorageConfigRequest>();

    auto& record = requestToTablet->Record;
    record.SetFileSystemId(request.GetFileSystemId());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TThis::StateWork);
}

void TGetStorageConfigActionActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    google::protobuf::util::MessageToJsonString(
        NProtoPrivate::TGetStorageConfigResponse(),
        response->Record.MutableOutput()
    );

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetStorageConfigActionActor::HandleSuccess(
    const TActorContext& ctx,
    NProto::TStorageConfig config)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>();

    google::protobuf::util::MessageToJsonString(
        std::move(config), msg->Record.MutableOutput()
    );

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TGetStorageConfigActionActor::HandleGetStorageConfigResponse(
    const TEvIndexTablet::TEvGetStorageConfigResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError());
        return;
    }

    HandleSuccess(ctx, std::move(*msg->Record.MutableStorageConfig()));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetStorageConfigActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvGetStorageConfigResponse,
            HandleGetStorageConfigResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

} // namespace

IActorPtr TStorageServiceActor::CreateGetStorageConfigActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TGetStorageConfigActionActor>(
        std::move(requestInfo),
        StorageConfig,
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
