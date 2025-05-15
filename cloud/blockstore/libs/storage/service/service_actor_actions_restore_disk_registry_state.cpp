#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace google::protobuf::util;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRestoreDiskRegistryStateActor final
    : public TActorBootstrapped<TRestoreDiskRegistryStateActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NProto::TError Error;

public:
    TRestoreDiskRegistryStateActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TRestoreDiskRegistryStateResponse proto
    );

private:
    STFUNC(StateWork);

    void HandleRestoreDiskRegistryStateResponse(
        const TEvDiskRegistry::TEvRestoreDiskRegistryStateResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRestoreDiskRegistryStateActor::TRestoreDiskRegistryStateActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TRestoreDiskRegistryStateActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<
        TEvDiskRegistry::TEvRestoreDiskRegistryStateRequest>();

    auto status = JsonStringToMessage(Input, &request->Record);
    if (!status.ok()) {
        Error = MakeError(
            E_ARGUMENT,
            "Failed to parse input. Status: " + status.ToString());
        ReplyAndDie(ctx, {});
        return;
    }

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TRestoreDiskRegistryStateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TRestoreDiskRegistryStateResponse proto)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    MessageToJsonString(proto, response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_restorediskregistrystate",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TRestoreDiskRegistryStateActor::HandleRestoreDiskRegistryStateResponse(
    const TEvDiskRegistry::TEvRestoreDiskRegistryStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx, std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TRestoreDiskRegistryStateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvRestoreDiskRegistryStateResponse,
            HandleRestoreDiskRegistryStateResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateRestoreDiskRegistryStateActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TRestoreDiskRegistryStateActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
