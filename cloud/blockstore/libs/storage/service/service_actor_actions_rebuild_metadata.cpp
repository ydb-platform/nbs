#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <util/generic/guid.h>
#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRebuildMetadataActor final
    : public TActorBootstrapped<TRebuildMetadataActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TRebuildMetadataRequest Request;

public:
    TRebuildMetadataActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TRebuildMetadataResponse response);

private:
    STFUNC(StateWork);

    void HandleRebuildMetadataResponse(
        const TEvVolume::TEvRebuildMetadataResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRebuildMetadataActor::TRebuildMetadataActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TRebuildMetadataActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    if (!Request.GetBatchSize()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Batch size should be supplied"));
        return;
    }

    auto request = std::make_unique<TEvVolume::TEvRebuildMetadataRequest>();
    request->Record.SetDiskId(Request.GetDiskId());
    request->Record.SetBatchSize(Request.GetBatchSize());

    switch (Request.GetMetadataType()) {
        case NPrivateProto::USED_BLOCKS: {
            request->Record.SetMetadataType(NProto::USED_BLOCKS);
            break;
        }
        case NPrivateProto::BLOCK_COUNT: {
            request->Record.SetMetadataType(NProto::BLOCK_COUNT);
            break;
        }
        default: {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "Unknown metadata type"
                        << static_cast<ui32>(Request.GetMetadataType())));
            return;
        }
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Rebilding metadata for %s disk, metadata type: %u",
        Request.GetDiskId().c_str(),
        Request.GetMetadataType());


    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);

    Become(&TThis::StateWork);
}

void TRebuildMetadataActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TRebuildMetadataResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_rebuildmetadata",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);

}

void TRebuildMetadataActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TRebuildMetadataResponse response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(response.GetError()));

    NPrivateProto::TRebuildMetadataResponse actionResponse;

    google::protobuf::util::MessageToJsonString(
        actionResponse,
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_rebuildmetadata",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TRebuildMetadataActor::HandleRebuildMetadataResponse(
    const TEvVolume::TEvRebuildMetadataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::move(ev->Get()->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TRebuildMetadataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvRebuildMetadataResponse,
            HandleRebuildMetadataResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TRebuildMetadataStatusActor final
    : public TActorBootstrapped<TRebuildMetadataStatusActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TGetRebuildMetadataStatusRequest Request;

public:
    TRebuildMetadataStatusActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TGetRebuildMetadataStatusResponse response);

private:
    STFUNC(StateWork);

    void HandleGetRebuildMetadataStatusResponse(
        const TEvVolume::TEvGetRebuildMetadataStatusResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRebuildMetadataStatusActor::TRebuildMetadataStatusActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{
}

void TRebuildMetadataStatusActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    auto request = std::make_unique<TEvVolume::TEvGetRebuildMetadataStatusRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Query rebild metadata progress for %s disk",
        Request.GetDiskId().c_str());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);

    Become(&TThis::StateWork);
}

void TRebuildMetadataStatusActor::ReplyAndDie(const TActorContext& ctx, NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TGetRebuildMetadataStatusResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getrebuildmetadatastatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TRebuildMetadataStatusActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TGetRebuildMetadataStatusResponse response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(response.GetError()));

    NPrivateProto::TGetRebuildMetadataStatusResponse actionResponse;

    auto& progress = *actionResponse.MutableProgress();
    progress.SetProcessed(response.GetProgress().GetProcessed());
    progress.SetTotal(response.GetProgress().GetTotal());
    progress.SetIsCompleted(response.GetProgress().GetIsCompleted());

    google::protobuf::util::MessageToJsonString(
        actionResponse,
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getrebuildmetadatastatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TRebuildMetadataStatusActor::HandleGetRebuildMetadataStatusResponse(
    const TEvVolume::TEvGetRebuildMetadataStatusResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::move(ev->Get()->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TRebuildMetadataStatusActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvGetRebuildMetadataStatusResponse,
            HandleGetRebuildMetadataStatusResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateRebuildMetadataActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {
        std::make_unique<TRebuildMetadataActor>(
            std::move(requestInfo),
            std::move(input))};
}

TResultOrError<IActorPtr> TServiceActor::CreateRebuildMetadataStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TRebuildMetadataStatusActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
