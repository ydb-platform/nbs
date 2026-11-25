#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

void CopyLogoBlobId(
    const NKikimrProto::TLogoBlobID& src,
    NPrivateProto::TLogoBlobID& dst)
{
    dst.SetRawX1(src.GetRawX1());
    dst.SetRawX2(src.GetRawX2());
    dst.SetRawX3(src.GetRawX3());
}

////////////////////////////////////////////////////////////////////////////////

class TScanDiskActor final
    : public TActorBootstrapped<TScanDiskActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

public:
    TScanDiskActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TScanDiskResponse response);

private:
    STFUNC(StateWork);

    void HandleScanDiskResponse(
        const TEvVolume::TEvScanDiskResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskActor::TScanDiskActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TScanDiskActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvScanDiskRequest>();
    auto& record = request->Record;

    if (!google::protobuf::util::JsonStringToMessage(Input, &record).ok()) {
        ReplyAndDie(ctx, MakeError(
            E_ARGUMENT,
            "Failed to parse input"));
        return;
    }

    if (!record.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(
            E_ARGUMENT,
            "DiskId should be supplied"));
        return;
    }

    if (!record.GetBatchSize() && !record.GetCancel()) {
        ReplyAndDie(ctx, MakeError(
            E_ARGUMENT,
            "Batch size should be supplied"));
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "%s scan disk %s (check integrity: %d)",
        record.GetCancel() ? "Cancel" : "Start",
        record.GetDiskId().c_str(),
        record.GetCheckIntegrity());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);

    Become(&TThis::StateWork);
}

void TScanDiskActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TScanDiskResponse(),
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_scandisk",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

void TScanDiskActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TScanDiskResponse response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(response.GetError()));

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TScanDiskResponse(),
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_scandisk",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TScanDiskActor::HandleScanDiskResponse(
    const TEvVolume::TEvScanDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::move(ev->Get()->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvScanDiskResponse,
            HandleScanDiskResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TScanDiskStatusActor final
    : public TActorBootstrapped<TScanDiskStatusActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TGetScanDiskStatusRequest Request;

public:
    TScanDiskStatusActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TGetScanDiskStatusResponse response);

private:
    STFUNC(StateWork);

    void HandleGetScanDiskStatusResponse(
        const TEvVolume::TEvGetScanDiskStatusResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskStatusActor::TScanDiskStatusActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{
}

void TScanDiskStatusActor::Bootstrap(const TActorContext& ctx)
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

    auto request = std::make_unique<TEvVolume::TEvGetScanDiskStatusRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Query scan disk progress for %s disk",
        Request.GetDiskId().c_str());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);

    Become(&TThis::StateWork);
}

void TScanDiskStatusActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TGetScanDiskStatusResponse(),
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getscandiskstatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

void TScanDiskStatusActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TGetScanDiskStatusResponse response)
{
    auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(response.GetError()));

    NPrivateProto::TGetScanDiskStatusResponse actionResponse;

    auto& progress = *actionResponse.MutableProgress();
    const auto& responseProgress = response.GetProgress();
    progress.SetProcessed(responseProgress.GetProcessed());
    progress.SetTotal(responseProgress.GetTotal());
    progress.SetIsCompleted(responseProgress.GetIsCompleted());

    for (const auto& responseBrokenBlob: responseProgress.GetBrokenBlobs()) {
        CopyLogoBlobId(responseBrokenBlob, *progress.AddBrokenBlobs());
    }

    progress.SetCheckIntegrity(responseProgress.GetCheckIntegrity());
    progress.SetScanCommitId(responseProgress.GetScanCommitId());
    progress.SetDeadBlobs(responseProgress.GetDeadBlobs());
    progress.SetCheckedBlobs(responseProgress.GetCheckedBlobs());
    progress.SetReplicatingBlobs(responseProgress.GetReplicatingBlobs());
    progress.SetDegradedBlobs(responseProgress.GetDegradedBlobs());
    progress.SetUncheckedBlobs(responseProgress.GetUncheckedBlobs());
    progress.SetDroppedBlobResults(responseProgress.GetDroppedBlobResults());
    progress.SetCancelled(responseProgress.GetCancelled());

    for (const auto& responseResult: responseProgress.GetBlobResults()) {
        auto& result = *progress.AddBlobResults();
        CopyLogoBlobId(responseResult.GetBlobId(), *result.MutableBlobId());
        result.SetBSGroupId(responseResult.GetBSGroupId());
        result.SetVerdict(
            static_cast<NPrivateProto::TScanDiskBlobResult::EVerdict>(
                responseResult.GetVerdict()));
        result.SetStatus(responseResult.GetStatus());
        result.SetPlacementStatus(responseResult.GetPlacementStatus());
        result.SetDataStatus(responseResult.GetDataStatus());
        result.SetErrorReason(responseResult.GetErrorReason());
        result.SetDataInfo(responseResult.GetDataInfo());
    }

    google::protobuf::util::MessageToJsonString(
        actionResponse,
        msg->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getscandiskstatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(msg));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TScanDiskStatusActor::HandleGetScanDiskStatusResponse(
    const TEvVolume::TEvGetScanDiskStatusResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::move(ev->Get()->Record));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskStatusActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvGetScanDiskStatusResponse,
            HandleGetScanDiskStatusResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateScanDiskActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {
        std::make_unique<TScanDiskActor>(
            std::move(requestInfo),
            std::move(input))};
}

TResultOrError<IActorPtr> TServiceActor::CreateScanDiskStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TScanDiskStatusActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
