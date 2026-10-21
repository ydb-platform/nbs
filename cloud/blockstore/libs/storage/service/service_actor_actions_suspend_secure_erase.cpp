#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSuspendSecureEraseActor final
    : public TActorBootstrapped<TSuspendSecureEraseActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    TString Input;

    NProto::TError Error;

public:
    TSuspendSecureEraseActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleSetSuspendSecureEraseResponse(
        const TEvDiskRegistry::TEvSetSuspendSecureEraseResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSuspendSecureEraseActor::TSuspendSecureEraseActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TSuspendSecureEraseActor::Bootstrap(const TActorContext& ctx)
{
    NProto::TSetSuspendSecureEraseRequest proto;

    if (!google::protobuf::util::JsonStringToMessage(Input, &proto).ok()) {
        Error = MakeError(E_ARGUMENT, "Failed to parse input");
        ReplyAndDie(ctx);
        return;
    }

    auto request =
        std::make_unique<TEvDiskRegistry::TEvSetSuspendSecureEraseRequest>();
    request->Record.SetState(proto.GetState());

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TSuspendSecureEraseActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    google::protobuf::util::MessageToJsonString(
        NProto::TSetSuspendSecureEraseResponse(),
        response->Record.MutableOutput());

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_diskregistrysuspendsecureerase",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendSecureEraseActor::HandleSetSuspendSecureEraseResponse(
    const TEvDiskRegistry::TEvSetSuspendSecureEraseResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSuspendSecureEraseActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvSetSuspendSecureEraseResponse,
            HandleSetSuspendSecureEraseResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateSuspendSecureEraseActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TSuspendSecureEraseActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
