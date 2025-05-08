#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateDiskBlockSizeActionActor final
    : public TActorBootstrapped<TUpdateDiskBlockSizeActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;
    ui32 BlockSize = 0;
    bool Force = false;

    NProto::TError Error;

public:
    TUpdateDiskBlockSizeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleSuccess(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleUpdateDiskBlockSizeResponse(
        const TEvDiskRegistry::TEvUpdateDiskBlockSizeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TUpdateDiskBlockSizeActionActor::TUpdateDiskBlockSizeActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TUpdateDiskBlockSizeActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!Input) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty input"));
        return;
    }

    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(ctx,
            MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (input.Has("DiskId")) {
        DiskId = input["DiskId"].GetString();
    }
    if (input.Has("BlockSize")) {
        BlockSize = input["BlockSize"].GetUInteger();
    }
    if (input.Has("Force")) {
        Force = input["Force"].GetBoolean();
    }

    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvUpdateDiskBlockSizeRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetBlockSize(BlockSize);
    request->Record.SetForce(Force);

    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TUpdateDiskBlockSizeActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_updatediskblocksize",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TUpdateDiskBlockSizeActionActor::HandleSuccess(
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_updatediskblocksize",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TUpdateDiskBlockSizeActionActor::HandleUpdateDiskBlockSizeResponse(
    const TEvDiskRegistry::TEvUpdateDiskBlockSizeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError());
        return;
    }

    HandleSuccess(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdateDiskBlockSizeActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvUpdateDiskBlockSizeResponse,
            HandleUpdateDiskBlockSizeResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateUpdateDiskBlockSizeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TUpdateDiskBlockSizeActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
