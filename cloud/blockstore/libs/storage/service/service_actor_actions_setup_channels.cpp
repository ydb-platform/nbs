#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSetupChannelsActionActor final
    : public TActorBootstrapped<TSetupChannelsActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TStorageConfigPtr Config;

    NPrivateProto::TSetupChannelsRequest Request;
    TLogTitle LogTitle;

public:
    TSetupChannelsActionActor(TRequestInfoPtr requestInfo,
        TString input,
        TStorageConfigPtr config);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TResizeVolumeResponse response);

private:
    STFUNC(StateWork);

    void HandleSetupChannelsResponse(
        const TEvService::TEvResizeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSetupChannelsActionActor::TSetupChannelsActionActor(
        TRequestInfoPtr requestInfo,
        TString input,
        TStorageConfigPtr config)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
    , Config(config)
    , LogTitle(GetCycleCount(), TLogTitle::TServiceRequest{})
{}

void TSetupChannelsActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(
            E_ARGUMENT,
            "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(
            E_ARGUMENT,
            "DiskId should be supplied"));
        return;
    }

    LogTitle.SetDiskId(Request.GetDiskId());

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s Start setup channels",
        LogTitle.GetWithTime().c_str());

    NCloud::NBlockStore::NStorage::RegisterAlterVolumeActor(
        SelfId(),
        RequestInfo->Cookie,
        Config,
        Request,
        ctx);

    Become(&TThis::StateWork);
}

void TSetupChannelsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TSetupChannelsResponse(),
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_setupchannels",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

void TSetupChannelsActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TResizeVolumeResponse response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(response.GetError()));

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TSetupChannelsResponse(),
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_setupchannels",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TSetupChannelsActionActor::HandleSetupChannelsResponse(
    const TEvService::TEvResizeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::move(ev->Get()->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSetupChannelsActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvResizeVolumeResponse, HandleSetupChannelsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

} // namespace


TResultOrError<IActorPtr> TServiceActor::CreateSetupChannelsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TSetupChannelsActionActor>(
        std::move(requestInfo),
        std::move(input),
        Config)};
}

}   // namespace NCloud::NBlockStore::NStorage
