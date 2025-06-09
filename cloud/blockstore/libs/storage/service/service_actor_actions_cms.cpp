#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCmsActor final
    : public TActorBootstrapped<TCmsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TCmsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

private:
    STFUNC(StateWork);

    void HandleCmsActionResponse(
        const TEvService::TEvCmsActionResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCmsActor::TCmsActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TCmsActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvCmsActionRequest>();

    if (!google::protobuf::util::JsonStringToMessage(Input, &request->Record).ok()) {
        auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
            MakeError(E_ARGUMENT, "Failed to parse input"));
        ReplyAndDie(ctx, std::move(response));
        return;
    }

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
}

void TCmsActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_cms",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCmsActor::HandleCmsActionResponse(
    const TEvService::TEvCmsActionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        msg->GetError());

    google::protobuf::util::MessageToJsonString(
        msg->Record,
        response->Record.MutableOutput()
    );

    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCmsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvService::TEvCmsActionResponse,
            HandleCmsActionResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateCmsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCmsActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
