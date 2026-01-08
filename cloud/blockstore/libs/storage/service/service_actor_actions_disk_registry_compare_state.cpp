#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompareDiskRegistryStateWithLocalDbActor final
    : public TActorBootstrapped<TCompareDiskRegistryStateWithLocalDbActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NProto::TError Error;

public:
    TCompareDiskRegistryStateWithLocalDbActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

private:
    STFUNC(StateCompareDiskRegistryStateWithLocalDb);

    void HandleCompareDiskRegistryStateWithLocalDbResponse(
        const TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbResponse::
            TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCompareDiskRegistryStateWithLocalDbActor::
    TCompareDiskRegistryStateWithLocalDbActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TCompareDiskRegistryStateWithLocalDbActor::Bootstrap(
    const TActorContext& ctx)
{
    auto request = std::make_unique<
        TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbRequest>();

    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record)
             .ok())
    {
        Error = MakeError(E_ARGUMENT, "Failed to parse input");
        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvExecuteActionResponse>());
        return;
    }

    Become(&TThis::StateCompareDiskRegistryStateWithLocalDb);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TCompareDiskRegistryStateWithLocalDbActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_diskregistrycomparestate",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCompareDiskRegistryStateWithLocalDbActor::
    HandleCompareDiskRegistryStateWithLocalDbResponse(
        const TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbResponse::
            TPtr& ev,
        const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TString output;
    google::protobuf::util::MessageToJsonString(msg->Record, &output);
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(
    TCompareDiskRegistryStateWithLocalDbActor::
        StateCompareDiskRegistryStateWithLocalDb)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbResponse,
            HandleCompareDiskRegistryStateWithLocalDbResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateDiskRegistryCompareStateActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCompareDiskRegistryStateWithLocalDbActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
