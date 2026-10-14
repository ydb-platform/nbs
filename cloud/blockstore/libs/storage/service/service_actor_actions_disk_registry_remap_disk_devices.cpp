#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/string/printf.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRemapDiskDevicesActor final
    : public TActorBootstrapped<TRemapDiskDevicesActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    TString Input;

public:
    TRemapDiskDevicesActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleRemapDiskDevicesResponse(
        const TEvDiskRegistry::TEvRemapDiskDevicesResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRemapDiskDevicesActor::TRemapDiskDevicesActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TRemapDiskDevicesActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvRemapDiskDevicesRequest>();

    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record)
             .ok())
    {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TRemapDiskDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
    google::protobuf::util::MessageToJsonString(
        NProto::TRemapDiskDevicesResponse(),
        response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_changediskdevice",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TRemapDiskDevicesActor::HandleRemapDiskDevicesResponse(
    const TEvDiskRegistry::TEvRemapDiskDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TRemapDiskDevicesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvRemapDiskDevicesResponse,
            HandleRemapDiskDevicesResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateRemapDiskDevicesActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TRemapDiskDevicesActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
