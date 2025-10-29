#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPartiallySuspendDiskAgentActor final
    : public TActorBootstrapped<TPartiallySuspendDiskAgentActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TPartiallySuspendDiskAgentActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleSuspendDiskAgentResponse(
        const TEvDiskAgent::TEvPartiallySuspendAgentResponse::TPtr& ev,
        const TActorContext& ctx);

};

////////////////////////////////////////////////////////////////////////////////

TPartiallySuspendDiskAgentActor::TPartiallySuspendDiskAgentActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TPartiallySuspendDiskAgentActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskAgent::TEvPartiallySuspendAgentRequest>();
    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input."));
        return;
    }

    if (request->Record.GetNodeId() == 0) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Empty NodeId."));
        return;
    }

    if (request->Record.GetCancelSuspensionDelay() == 0) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Empty CancelSuspensionDelay."));
        return;
    }

    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeDiskAgentServiceId(request->Record.GetNodeId()),
        std::move(request));
}

void TPartiallySuspendDiskAgentActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TPartiallySuspendDiskAgentActor::HandleSuspendDiskAgentResponse(
    const TEvDiskAgent::TEvPartiallySuspendAgentResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPartiallySuspendDiskAgentActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskAgent::TEvPartiallySuspendAgentResponse,
            HandleSuspendDiskAgentResponse);

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
TServiceActor::CreatePartiallySuspendDiskAgentActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TPartiallySuspendDiskAgentActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
