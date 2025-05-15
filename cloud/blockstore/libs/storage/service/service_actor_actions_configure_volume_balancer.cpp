#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_balancer.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/balancer.pb.h>

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

class TConfigureVolumeBalancerActionActor final
    : public TActorBootstrapped<TConfigureVolumeBalancerActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TConfigureVolumeBalancerRequest Request;

public:
    TConfigureVolumeBalancerActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NPrivateProto::TConfigureVolumeBalancerResponse& resp,
        NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleConfigureVolumeBalancerResponse(
        const TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse::TPtr& ev,
        const TActorContext& ctx);

};

////////////////////////////////////////////////////////////////////////////////

TConfigureVolumeBalancerActionActor::TConfigureVolumeBalancerActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TConfigureVolumeBalancerActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, {}, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    auto request = std::make_unique<TEvVolumeBalancer::TEvConfigureVolumeBalancerRequest>();
    request->Record = std::move(Request);

    NCloud::Send(ctx, MakeVolumeBalancerServiceId(), std::move(request));

    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TConfigureVolumeBalancerActionActor::HandleConfigureVolumeBalancerResponse(
    const TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, ev->Get()->Record, {});
}

void TConfigureVolumeBalancerActionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NPrivateProto::TConfigureVolumeBalancerResponse& resp,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(std::move(error));
    google::protobuf::util::MessageToJsonString(
        resp,
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_changebalancer",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TConfigureVolumeBalancerActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse, HandleConfigureVolumeBalancerResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME_BALANCER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateConfigureVolumeBalancerActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TConfigureVolumeBalancerActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
