#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace google::protobuf::util;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TForceMigrationActor final
    : public TActorBootstrapped<TForceMigrationActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NProto::TError Error;

public:
    TForceMigrationActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TStartForceMigrationResponse proto);

private:
    STFUNC(StateWork);

    void HandleStartForceMigrationResponse(
        const TEvDiskRegistry::TEvStartForceMigrationResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TForceMigrationActor::TForceMigrationActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TForceMigrationActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<
        TEvDiskRegistry::TEvStartForceMigrationRequest>();

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

void TForceMigrationActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TStartForceMigrationResponse proto)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    MessageToJsonString(
        proto, response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_migrationdiskregistrydevice",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TForceMigrationActor::HandleStartForceMigrationResponse(
    const TEvDiskRegistry::TEvStartForceMigrationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx, std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TForceMigrationActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvStartForceMigrationResponse,
            HandleStartForceMigrationResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateMigrationDiskRegistryDeviceActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TForceMigrationActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
