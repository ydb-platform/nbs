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

class TCreateMirroredDiskFromDevicesActor final
    : public TActorBootstrapped<TCreateMirroredDiskFromDevicesActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    TString Input;

public:
    TCreateMirroredDiskFromDevicesActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleCreateMirroredDiskFromDevicesResponse(
        const TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCreateMirroredDiskFromDevicesActor::TCreateMirroredDiskFromDevicesActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TCreateMirroredDiskFromDevicesActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesRequest>();

    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record)
             .ok())
    {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TCreateMirroredDiskFromDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
    google::protobuf::util::MessageToJsonString(
        NProto::TCreateMirroredDiskFromDevicesResponse(),
        response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_createmirroreddiskfromdevices",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateMirroredDiskFromDevicesActor::HandleCreateMirroredDiskFromDevicesResponse(
    const TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateMirroredDiskFromDevicesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesResponse,
            HandleCreateMirroredDiskFromDevicesResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateCreateMirroredDiskFromDevicesActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCreateMirroredDiskFromDevicesActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
