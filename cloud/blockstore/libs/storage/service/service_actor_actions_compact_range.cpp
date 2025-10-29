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

#include <util/generic/guid.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompactRangeActionActor final
    : public TActorBootstrapped<TCompactRangeActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;
    ui64 StartIndex = 0;
    ui32 BlocksCount = 0;

public:
    TCompactRangeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void CompactRange(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleCompactRangeResponse(
        TEvVolume::TEvCompactRangeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCompactRangeActionActor::TCompactRangeActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TCompactRangeActionActor::Bootstrap(const TActorContext& ctx)
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

    if (input.Has("StartIndex")) {
        StartIndex = input["StartIndex"].GetUIntegerRobust();
    }

    if (input.Has("BlocksCount")) {
        BlocksCount = input["BlocksCount"].GetUIntegerRobust();
    }

    if (!BlocksCount) {
        HandleError(ctx, MakeError(E_ARGUMENT, "BlocksCount should be defined"));
        return;
    }

    CompactRange(ctx);
    Become(&TThis::StateWork);
}

void TCompactRangeActionActor::CompactRange(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvCompactRangeRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(DiskId);
    request->Record.SetStartIndex(StartIndex);
    request->Record.SetBlocksCount(BlocksCount);
    request->Record.SetOperationId(CreateGuidAsString());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCompactRangeActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_compactrange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TCompactRangeActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_compactrange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCompactRangeActionActor::HandleCompactRangeResponse(
    TEvVolume::TEvCompactRangeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }
    msg->Record.ClearTrace();

    TString response;
    google::protobuf::util::MessageToJsonString(msg->Record, &response);

    HandleSuccess(ctx, response);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCompactRangeActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvCompactRangeResponse, HandleCompactRangeResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateCompactRangeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCompactRangeActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
