#include "service_actor.h"

#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDrainTabletsActionActor final
    : public TActorBootstrapped<TDrainTabletsActionActor>
{
    TRequestInfoPtr RequestInfo;
    TString Input;

    TDrainTabletsActionActor(TRequestInfoPtr requestInfo, TString input)
        : RequestInfo(std::move(requestInfo))
        , Input(std::move(input))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        NProtoPrivate::TDrainNodeRequest request;
        if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok()) {
            ReplyWithError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
            return;
        }
        DrainTablets(ctx, request.GetKeepDown());
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHiveProxy::TEvDrainNodeResponse, HandleDrainResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void DrainTablets(const TActorContext& ctx, bool keepDown) const
    {
        auto request =
            std::make_unique<TEvHiveProxy::TEvDrainNodeRequest>(keepDown);
        NCloud::Send(
            ctx,
            MakeHiveProxyServiceId(),
            std::move(request),
            RequestInfo->Cookie);
    }

    void HandleDrainResponse(
        TEvHiveProxy::TEvDrainNodeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        if (const auto& error = ev->Get()->GetError(); FAILED(error.GetCode())) {
            ReplyWithError(ctx, error);
            return;
        }
        ReplyWithSuccess(ctx);
    }

    void ReplyWithError(const TActorContext& ctx, const NProto::TError& error)
    {
        auto response =
            std::make_unique<TEvService::TEvExecuteActionResponse>(error);
        ReplyAndDie(ctx, std::move(response));
    }

    void ReplyWithSuccess(const TActorContext& ctx)
    {
        auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
        google::protobuf::util::MessageToJsonString(
            NProtoPrivate::TDrainNodeResponse(),
            response->Record.MutableOutput());
        ReplyAndDie(ctx, std::move(response));
    }

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
    {
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr TStorageServiceActor::CreateDrainTabletActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return std::make_unique<TDrainTabletsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
