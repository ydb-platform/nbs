#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/blockstore/private/api/protos/disk.pb.h>

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

class TGetDiskRegistryTabletInfoActor final
    : public TActorBootstrapped<TGetDiskRegistryTabletInfoActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TGetDiskRegistryTabletInfoActor(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);
private:
    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleGetDrTabletInfoResponse(
        const TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetDiskRegistryTabletInfoActor::TGetDiskRegistryTabletInfoActor(
        TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TGetDiskRegistryTabletInfoActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvDiskRegistryProxy::TEvGetDrTabletInfoRequest>();

    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TGetDiskRegistryTabletInfoActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getdiskregistrytabletinfo",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetDiskRegistryTabletInfoActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getdiskregistrytabletinfo",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TGetDiskRegistryTabletInfoActor::HandleGetDrTabletInfoResponse(
    const TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, msg->GetError());
        return;
    }

    NPrivateProto::TGetDiskRegistryTabletInfoResponse result;
    result.SetTabletId(msg->TabletId);
    TString output;

    google::protobuf::util::MessageToJsonString(result, &output);

    HandleSuccess(ctx, output);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetDiskRegistryTabletInfoActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse,
            HandleGetDrTabletInfoResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetDiskRegistryTabletInfo(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);

    return {std::make_unique<TGetDiskRegistryTabletInfoActor>(
        std::move(requestInfo))};
}

}   // namespace NCloud::NBlockStore::NStorage
