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

class TChangeStorageConfigActionActor final
    : public TActorBootstrapped<TChangeStorageConfigActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TChangeStorageConfigActionActor(TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TChangeStorageConfigResponse response);

private:
    STFUNC(StateWork);

    void HandleChangeStorageConfigResponse(
        const TEvVolume::TEvChangeStorageConfigResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TChangeStorageConfigActionActor::TChangeStorageConfigActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TChangeStorageConfigActionActor::Bootstrap(const TActorContext& ctx)
{
    NProto::TChangeStorageConfigRequest request;
    if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok()) {
        ReplyAndDie(ctx, MakeError(
            E_ARGUMENT,
            "Failed to parse input"));
        return;
    }

    if (!request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(
            E_ARGUMENT,
            "DiskId should be supplied"));
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Start to change storage config of %s",
        request.GetDiskId().c_str());

    auto requestToVolume =
        std::make_unique<TEvVolume::TEvChangeStorageConfigRequest>();
    requestToVolume->Record = std::move(request);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(requestToVolume));

    Become(&TThis::StateWork);
}

void TChangeStorageConfigActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(std::move(error));

    google::protobuf::util::MessageToJsonString(
        NProto::TChangeStorageConfigResponse(),
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_changestorageconfig",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

void TChangeStorageConfigActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TChangeStorageConfigResponse response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        response.GetError());

    google::protobuf::util::MessageToJsonString(
        std::move(response), msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_changestorageconfig",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TChangeStorageConfigActionActor::HandleChangeStorageConfigResponse(
    const TEvVolume::TEvChangeStorageConfigResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::move(ev->Get()->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TChangeStorageConfigActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvChangeStorageConfigResponse, HandleChangeStorageConfigResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

} // namespace

TResultOrError<IActorPtr> TServiceActor::CreateChangeStorageConfigActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TChangeStorageConfigActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
