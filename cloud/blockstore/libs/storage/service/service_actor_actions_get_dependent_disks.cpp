#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_registry/disk_registry_private.h>
#include <cloud/storage/core/libs/common/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetDependentDisksActor final
    : public TActorBootstrapped<TGetDependentDisksActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TGetDependentDisksActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void GetDependentDisks(const TActorContext& ctx, NProto::TGetDependentDisksRequest request);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleGetDependentDisksResponse(
        const TEvDiskRegistry::TEvGetDependentDisksResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetDependentDisksActor::TGetDependentDisksActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TGetDependentDisksActor::Bootstrap(const TActorContext& ctx)
{
    if (!Input) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty input"));
        return;
    }

    NProto::TGetDependentDisksRequest request;
    if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (request.GetHost().empty()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Host should be supplied"));
        return;
    }

    GetDependentDisks(ctx, std::move(request));
}

void TGetDependentDisksActor::GetDependentDisks(
    const TActorContext& ctx,
    NProto::TGetDependentDisksRequest request)
{
    Become(&TThis::StateWork);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending get dependent disks request");

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::make_unique<TEvDiskRegistry::TEvGetDependentDisksRequest>(
            MakeIntrusive<TCallContext>(),
            std::move(request))
    );
}

void TGetDependentDisksActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_GetDependentDisks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetDependentDisksActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

void TGetDependentDisksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TGetDependentDisksActor::HandleGetDependentDisksResponse(
    const TEvDiskRegistry::TEvGetDependentDisksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto error = msg->GetError();
    if (error.GetCode() == E_NOT_FOUND) {
        SetErrorProtoFlag(error, NCloud::NProto::EF_SILENT);
    }

    if (HasError(error)) {
        HandleError(ctx, error);
        return;
    }

    TString result;
    auto status =
        google::protobuf::util::MessageToJsonString(msg->Record, &result);
    if (!status.ok()) {
        HandleError(
            ctx,
            MakeError(
                E_FAIL,
                TStringBuilder() << "Couldn't convert response to JSON: "
                                 << status.ToString()));
        return;
    }
    HandleSuccess(ctx, result);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetDependentDisksActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvGetDependentDisksResponse,
            HandleGetDependentDisksResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetDependentDisksActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TGetDependentDisksActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
