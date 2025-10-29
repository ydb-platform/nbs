#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetCompactionStatusActionActor final
    : public TActorBootstrapped<TGetCompactionStatusActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;
    TString OperationId;

public:
    TGetCompactionStatusActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void GetCompactionStatus(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleGetCompactionStatusResponse(
        TEvVolume::TEvGetCompactionStatusResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetCompactionStatusActionActor::TGetCompactionStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TGetCompactionStatusActionActor::Bootstrap(const TActorContext& ctx)
{
    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (input.Has("DiskId")) {
        DiskId = input["DiskId"].GetStringRobust();
    }

    if (!DiskId) {
        HandleError(ctx, MakeError(E_ARGUMENT, "DiskId should be defined"));
        return;
    }

    if (input.Has("OperationId")) {
        OperationId = input["OperationId"].GetStringRobust();
    }

    if (!OperationId) {
        HandleError(ctx, MakeError(E_ARGUMENT, "OperationId should be defined"));
        return;
    }

    GetCompactionStatus(ctx);
    Become(&TThis::StateWork);
}

void TGetCompactionStatusActionActor::GetCompactionStatus(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvGetCompactionStatusRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(DiskId);
    request->Record.SetOperationId(OperationId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TGetCompactionStatusActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getcompactionstatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetCompactionStatusActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getcompactionstatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TGetCompactionStatusActionActor::HandleGetCompactionStatusResponse(
    TEvVolume::TEvGetCompactionStatusResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    msg->Record.ClearTrace();

    TString response;
    google::protobuf::util::MessageToJsonString(msg->Record, &response);

    HandleSuccess(ctx, response);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetCompactionStatusActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvGetCompactionStatusResponse, HandleGetCompactionStatusResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetCompactionStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TGetCompactionStatusActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
