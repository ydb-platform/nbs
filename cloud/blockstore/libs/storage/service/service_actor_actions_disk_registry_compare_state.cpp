#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryCompareStateActor final
    : public TActorBootstrapped<TDiskRegistryCompareStateActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TCompareDiskRegistryStateRequest Request;

    NProto::TError Error;

public:
    TDiskRegistryCompareStateActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleCompareStatesResponse(
        const TEvDiskRegistry::TEvCompareDiskRegistryStateResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryCompareStateActor::TDiskRegistryCompareStateActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TDiskRegistryCompareStateActor::Bootstrap(const TActorContext& ctx)
{

    auto request = std::make_unique<TEvDiskRegistry::TEvCompareDiskRegistryStateRequest>(

    );
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        Error = MakeError(E_ARGUMENT, "Failed to parse input");
        ReplyAndDie(ctx);
        return;
    }


    Become(&TThis::StateWork);
    ctx.Send(MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TDiskRegistryCompareStateActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    // google::protobuf::util::MessageToJsonString(
    //     NPrivateProto::TDiskRegistryCompareStateResponse(),
    //     response->Record.MutableOutput()
    // );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_diskregistrycomparestate",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryCompareStateActor::HandleCompareStatesResponse(
    const TEvDiskRegistry::TEvCompareDiskRegistryStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskRegistryCompareStateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvCompareDiskRegistryStateResponse,
            HandleCompareStatesResponse);

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
    return {std::make_unique<TDiskRegistryCompareStateActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
