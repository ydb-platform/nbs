#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/storage/core/libs/common/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetDiskAgentNodeIdActor final
    : public TActorBootstrapped<TGetDiskAgentNodeIdActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TGetDiskAgentNodeIdActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

private:
    STFUNC(StateWork);

    void HandleGetAgentNodeIdResponse(
        const TEvDiskRegistry::TEvGetAgentNodeIdResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetDiskAgentNodeIdActor::TGetDiskAgentNodeIdActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TGetDiskAgentNodeIdActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvGetAgentNodeIdRequest>();
    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record)
             .ok())
    {
        auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
            MakeError(E_ARGUMENT, "Failed to parse input"));
        ReplyAndDie(ctx, std::move(response));
        return;
    }

    Become(&TThis::StateWork);
    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TGetDiskAgentNodeIdActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TGetDiskAgentNodeIdActor::HandleGetAgentNodeIdResponse(
    const TEvDiskRegistry::TEvGetAgentNodeIdResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto error = msg->GetError();
    if (error.GetCode() == E_NOT_FOUND) {
        // DR doesn't know about agents without disks. Do not trigger fatal
        // error here.
        SetErrorProtoFlag(error, NCloud::NProto::EF_SILENT);
    }

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));

    google::protobuf::util::JsonPrintOptions options;
    options.always_print_primitive_fields = true;
    google::protobuf::util::MessageToJsonString(
        msg->Record,
        response->Record.MutableOutput(),
        options);

    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetDiskAgentNodeIdActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvGetAgentNodeIdResponse,
            HandleGetAgentNodeIdResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetDiskAgentNodeIdActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TGetDiskAgentNodeIdActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
