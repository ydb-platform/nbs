#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetStorageConfigActor final
    : public TActorBootstrapped<TGetStorageConfigActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr StorageConfig;
    TString Input;

public:
    TGetStorageConfigActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr storageConfig,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleError(const TActorContext& ctx, NProto::TError error);
    void HandleSuccess(
        const TActorContext& ctx,
        NProto::TStorageServiceConfig config);

private:
    STFUNC(StateWork);

    void HandleGetStorageConfigResponse(
        const TEvVolume::TEvGetStorageConfigResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetStorageConfigActor::TGetStorageConfigActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr storageConfig,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , StorageConfig(std::move(storageConfig))
    , Input(std::move(input))
{}

void TGetStorageConfigActor::Bootstrap(const TActorContext& ctx)
{
    NProto::TGetStorageConfigRequest proto;

    if (!google::protobuf::util::JsonStringToMessage(Input, &proto).ok()) {
        HandleError(
            ctx,
            MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!proto.GetDiskId()) {
        HandleSuccess(ctx, StorageConfig->GetStorageConfigProto());
        return;
    }

    auto request =
        std::make_unique<TEvVolume::TEvGetStorageConfigRequest>();
    request->Record.SetDiskId(std::move(*proto.MutableDiskId()));

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TGetStorageConfigActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    google::protobuf::util::MessageToJsonString(
        NProto::TGetStorageConfigResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getstorageconfig",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetStorageConfigActor::HandleSuccess(
    const TActorContext& ctx,
    NProto::TStorageServiceConfig config)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>();

    google::protobuf::util::MessageToJsonString(
        std::move(config), msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getstorageconfig",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TGetStorageConfigActor::HandleGetStorageConfigResponse(
    const TEvVolume::TEvGetStorageConfigResponse::TPtr& ev,
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

STFUNC(TGetStorageConfigActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvGetStorageConfigResponse,
            HandleGetStorageConfigResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateGetStorageConfigActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TGetStorageConfigActor>(
        std::move(requestInfo),
        Config,
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
