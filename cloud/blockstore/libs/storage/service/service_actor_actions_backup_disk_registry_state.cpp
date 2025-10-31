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

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBackupDiskRegistryStateActor final
    : public TActorBootstrapped<TBackupDiskRegistryStateActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NProto::TError Error;

public:
    TBackupDiskRegistryStateActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TBackupDiskRegistryStateResponse proto
    );

private:
    STFUNC(StateWork);

    void HandleBackupDiskRegistryStateResponse(
        const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TBackupDiskRegistryStateActor::TBackupDiskRegistryStateActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TBackupDiskRegistryStateActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvBackupDiskRegistryStateRequest>();

    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record).ok()) {
        Error = MakeError(E_ARGUMENT, "Failed to parse input");
        ReplyAndDie(ctx, {});
        return;
    }

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TBackupDiskRegistryStateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TBackupDiskRegistryStateResponse proto)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    google::protobuf::util::MessageToJsonString(proto, response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_backupdiskregistrystate",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TBackupDiskRegistryStateActor::HandleBackupDiskRegistryStateResponse(
    const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx, std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TBackupDiskRegistryStateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvBackupDiskRegistryStateResponse,
            HandleBackupDiskRegistryStateResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateBackupDiskRegistryStateActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TBackupDiskRegistryStateActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
