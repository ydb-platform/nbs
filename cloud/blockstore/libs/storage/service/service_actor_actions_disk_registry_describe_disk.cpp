#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryDescribeDiskActor final
    : public TActorBootstrapped<TDiskRegistryDescribeDiskActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TDiskRegistryDescribeDiskActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyErrorAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleDescribeDiskResponse(
        const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryDescribeDiskActor::TDiskRegistryDescribeDiskActor(
    TRequestInfoPtr requestInfo,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TDiskRegistryDescribeDiskActor::Bootstrap(const TActorContext& ctx)
{
    NProto::TDescribeDiskRequest proto;
    if (!google::protobuf::util::JsonStringToMessage(Input, &proto).ok()) {
        ReplyErrorAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>(
        RequestInfo->CallContext,
        std::move(proto));

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));

    Become(&TThis::StateWork);
}

void TDiskRegistryDescribeDiskActor::ReplyErrorAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryDescribeDiskActor::HandleDescribeDiskResponse(
    const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        ReplyErrorAndDie(ctx, msg->GetError());
        return;
    }

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    google::protobuf::util::MessageToJsonString(
        msg->Record,
        response->Record.MutableOutput());

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskRegistryDescribeDiskActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvDescribeDiskResponse,
            HandleDescribeDiskResponse);

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
TServiceActor::CreateDiskRegistryDescribeDiskActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TDiskRegistryDescribeDiskActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
