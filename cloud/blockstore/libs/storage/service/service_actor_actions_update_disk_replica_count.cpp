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

class TUpdateDiskReplicaCountActionActor final
    : public TActorBootstrapped<TUpdateDiskReplicaCountActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString MasterDiskId;
    ui32 ReplicaCount = 0;

    NProto::TError Error;

public:
    TUpdateDiskReplicaCountActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleSuccess(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleUpdateDiskReplicaCountResponse(
        const TEvDiskRegistry::TEvUpdateDiskReplicaCountResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TUpdateDiskReplicaCountActionActor::TUpdateDiskReplicaCountActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TUpdateDiskReplicaCountActionActor::Bootstrap(const TActorContext& ctx)
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

    if (input.Has("MasterDiskId")) {
        MasterDiskId = input["MasterDiskId"].GetString();
    }
    if (input.Has("ReplicaCount")) {
        ReplicaCount = input["ReplicaCount"].GetUInteger();
    }

    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvUpdateDiskReplicaCountRequest>();
    request->Record.SetMasterDiskId(MasterDiskId);
    request->Record.SetReplicaCount(ReplicaCount);

    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TUpdateDiskReplicaCountActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_updatediskreplicacount",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TUpdateDiskReplicaCountActionActor::HandleSuccess(
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_updatediskreplicacount",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TUpdateDiskReplicaCountActionActor::HandleUpdateDiskReplicaCountResponse(
    const TEvDiskRegistry::TEvUpdateDiskReplicaCountResponse::TPtr& ev,
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

STFUNC(TUpdateDiskReplicaCountActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvUpdateDiskReplicaCountResponse,
            HandleUpdateDiskReplicaCountResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateUpdateDiskReplicaCountActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TUpdateDiskReplicaCountActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
