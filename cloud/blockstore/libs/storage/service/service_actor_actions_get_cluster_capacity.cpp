#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <library/cpp/json/json_writer.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/interconnect/interconnect.h>
#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetCapacityActor final
    : public TActorBootstrapped<TGetCapacityActor>
{
private:
    const TRequestInfoPtr RequestInfo;

public:
    explicit TGetCapacityActor(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleEmptyList(const TActorContext& ctx);

private:
    STFUNC(StateGetDiskRegistryCapacity);

    void HandleDiskRegistyCapacity(
        const TEvDiskRegistry::TEvGetClusterCapacityResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetCapacityActor::TGetCapacityActor(
    TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TGetCapacityActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateGetDiskRegistryCapacity);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending get nameservice config request");

    // NCloud::Send(
    //     ctx,
    //     GetNameserviceActorId(),
    //     std::make_unique<TEvInterconnect::TEvListNodes>(
    //         /*SubscribeToStaticNodeChanges=*/false));
}

void TGetCapacityActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_GetNameserviceConfig",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetCapacityActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TGetCapacityActor::HandleEmptyList(const TActorContext& ctx)
{
    NProto::TError error;
    error.SetMessage("Nameserivce config is empty.");
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TGetCapacityActor::HandleDiskRegistyCapacity(
    const TEvDiskRegistry::TEvGetClusterCapacityResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::SERVICE,
            "Getting Disk Registry capacity failed: "
            << error.GetMessage());
    }

    if (msg->Record.GetCapacity().empty()) {
        HandleEmptyList(ctx);
        return;
    }

    NPrivateProto::TGetClusterCapacityResponse result;
    for (const NProto::TClusterCapacityInfo& capacityInfo: msg->Record.GetCapacity()) {

        NPrivateProto::TClusterCapacityInfo info;
        info.SetFree(capacityInfo.GetFree());
        auto* capacity = result.AddCapacity();
        *capacity = std::move(info);
    }
    TString output;
    google::protobuf::util::MessageToJsonString(result, &output);
    HandleSuccess(ctx, output);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetCapacityActor::StateGetDiskRegistryCapacity)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvDiskRegistry::TEvGetClusterCapacityResponse, HandleDiskRegistyCapacity);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateGetCapacityActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);
    return {
        std::make_unique<TGetCapacityActor>(std::move(requestInfo))};
}

}   // namespace NCloud::NBlockStore::NStorage
