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

class TEnsureDiskRegistryStateIntegrityActor final
    : public TActorBootstrapped<TEnsureDiskRegistryStateIntegrityActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TEnsureDiskRegistryStateIntegrityActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateEnsureDiskRegistryStateIntegrity);

    void HandleEnsureDiskRegistryStateIntegrityResponse(
        const TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityResponse::
            TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TEnsureDiskRegistryStateIntegrityActor::TEnsureDiskRegistryStateIntegrityActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TEnsureDiskRegistryStateIntegrityActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<
        TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityRequest>();

    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record)
             .ok())
    {
        NProto::TError error = MakeError(E_ARGUMENT, "Failed to parse input");
        ReplyAndDie(ctx, error);
        return;
    }

    Become(&TThis::StateEnsureDiskRegistryStateIntegrity);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TEnsureDiskRegistryStateIntegrityActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_ensurediskregistrystateintegrity",
        RequestInfo->CallContext->RequestId);

    auto response = std::make_unique<
        TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityResponse>(error);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TEnsureDiskRegistryStateIntegrityActor::
    HandleEnsureDiskRegistryStateIntegrityResponse(
        const TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityResponse::
            TPtr& ev,
        const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, msg->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(
    TEnsureDiskRegistryStateIntegrityActor::
        StateEnsureDiskRegistryStateIntegrity)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvEnsureDiskRegistryStateIntegrityResponse,
            HandleEnsureDiskRegistryStateIntegrityResponse);

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

TResultOrError<IActorPtr>
TServiceActor::CreateDiskRegistryEnsureStateIntegrityActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TEnsureDiskRegistryStateIntegrityActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
