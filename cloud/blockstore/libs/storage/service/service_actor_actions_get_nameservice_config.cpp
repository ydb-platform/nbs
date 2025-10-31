#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/configs.pb.h>

#include <library/cpp/json/json_writer.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetDynamicNameserverNodes final
    : public TActorBootstrapped<TGetDynamicNameserverNodes>
{
private:
    const TRequestInfoPtr RequestInfo;

public:
    explicit TGetDynamicNameserverNodes(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleEmptyList(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleNameserviceConfig(
        const TEvInterconnect::TEvNodesInfo::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetDynamicNameserverNodes::TGetDynamicNameserverNodes(
    TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TGetDynamicNameserverNodes::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending get nameservice config request");

    NCloud::Send(
        ctx,
        GetNameserviceActorId(),
        std::make_unique<TEvInterconnect::TEvListNodes>(
            /*SubscribeToStaticNodeChanges=*/false));
}

void TGetDynamicNameserverNodes::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_GetNameserviceConfig",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetDynamicNameserverNodes::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TGetDynamicNameserverNodes::HandleEmptyList(const TActorContext& ctx)
{
    NProto::TError error;
    error.SetMessage("Nameserivce config is empty.");
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TGetDynamicNameserverNodes::HandleNameserviceConfig(
    const TEvInterconnect::TEvNodesInfo::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (msg->Nodes.empty()) {
        HandleEmptyList(ctx);
        return;
    }

    NPrivateProto::TGetNameserverNodesResponse result;
    for (const TEvInterconnect::TNodeInfo& nodeInfo: msg->Nodes) {
        NPrivateProto::TNodeInfo info;
        info.SetNodeId(nodeInfo.NodeId);
        info.SetAddress(nodeInfo.Address);
        info.SetHost(nodeInfo.Host);
        info.SetPort(nodeInfo.Port);
        info.SetResolveHost(nodeInfo.ResolveHost);
        info.SetIsStatic(nodeInfo.IsStatic);
        info.SetLocation(nodeInfo.Location.ToString());
        *result.AddNodes() = std::move(info);
    }
    TString output;
    google::protobuf::util::MessageToJsonString(result, &output);
    HandleSuccess(ctx, output);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetDynamicNameserverNodes::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvInterconnect::TEvNodesInfo, HandleNameserviceConfig);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetNameserverNodesActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);
    return {
        std::make_unique<TGetDynamicNameserverNodes>(std::move(requestInfo))};
}

}   // namespace NCloud::NBlockStore::NStorage
