#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlocksIndexActionActor final
    : public TActorBootstrapped<TDescribeBlocksIndexActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;
    ui32 StartIndex = 0;
    ui32 BlocksCount = 0;

public:
    TDescribeBlocksIndexActionActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeBlocksIndex(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleDescribeBlocksIndexResponse(
        const TEvVolume::TEvDescribeBlocksIndexResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeBlocksIndexActionActor::TDescribeBlocksIndexActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TDescribeBlocksIndexActionActor::Bootstrap(const TActorContext& ctx)
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

    DescribeBlocksIndex(ctx);
    Become(&TThis::StateWork);
}

void TDescribeBlocksIndexActionActor::DescribeBlocksIndex(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvDescribeBlocksIndexRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(DiskId);
    request->Record.SetStartIndex(StartIndex);
    request->Record.SetBlocksCount(BlocksCount);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDescribeBlocksIndexActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_describeblocksindex",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TDescribeBlocksIndexActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_describeblocksindex",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeBlocksIndexActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvDescribeBlocksIndexResponse, HandleDescribeBlocksIndexResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TDescribeBlocksIndexActionActor::HandleDescribeBlocksIndexResponse(
    const TEvVolume::TEvDescribeBlocksIndexResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    TString response;
    google::protobuf::util::MessageToJsonString(msg->Record, &response);

    HandleSuccess(ctx, response);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateDescribeBlocksIndexActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TDescribeBlocksIndexActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
