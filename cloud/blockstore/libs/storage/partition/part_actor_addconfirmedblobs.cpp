#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

using TRequest = TEvPartitionPrivate::TEvAddBlobsRequest;
using TRequests = TVector<std::unique_ptr<TRequest>>;

class TAddConfirmedBlobsActor final
    : public TActorBootstrapped<TAddConfirmedBlobsActor>
{
private:
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    TRequests Requests;

    size_t RequestsCompleted = 0;

public:
    TAddConfirmedBlobsActor(
        const TActorId& tablet,
        TRequestInfoPtr requestInfo,
        TRequests requests);

    void Bootstrap(const TActorContext& ctx);

private:
    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TAddConfirmedBlobsActor::TAddConfirmedBlobsActor(
        const TActorId& tablet,
        TRequestInfoPtr requestInfo,
        TRequests requests)
    : Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , Requests(std::move(requests))
{}

void TAddConfirmedBlobsActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "AddConfirmedBlobs",
        RequestInfo->CallContext->RequestId);

    for (auto& request: Requests) {
        NCloud::Send(ctx, Tablet, std::move(request));
    }
}

void TAddConfirmedBlobsActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    using TEvent = TEvPartitionPrivate::TEvAddConfirmedBlobsCompleted;
    auto ev = std::make_unique<TEvent>(error);

    ev->ExecCycles = RequestInfo->GetExecCycles();
    ev->TotalCycles = RequestInfo->GetTotalCycles();

    NCloud::Send(ctx, Tablet, std::move(ev));
}

bool TAddConfirmedBlobsActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAndDie(ctx, error);
        return true;
    }

    return false;
}

void TAddConfirmedBlobsActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    NotifyCompleted(ctx, error);

    using TResponse = TEvPartitionPrivate::TEvAddConfirmedBlobsResponse;
    auto response = std::make_unique<TResponse>(error);

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "AddConfirmedBlobs",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAddConfirmedBlobsActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    RequestInfo->AddExecCycles(msg->ExecCycles);

    const auto& error = msg->GetError();
    if (HandleError(ctx, error)) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(RequestsCompleted < Requests.size());
    if (++RequestsCompleted < Requests.size()) {
        return;
    }

    ReplyAndDie(ctx, MakeError(S_OK));
}

void TAddConfirmedBlobsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");
    ReplyAndDie(ctx, error);
}

STFUNC(TAddConfirmedBlobsActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueAddConfirmedBlobsIfNeeded(
    const TActorContext& ctx)
{
    if (State->GetAddConfirmedBlobsState().Status != EOperationStatus::Idle) {
        // already enqueued
        return;
    }

    if (State->GetConfirmedBlobs().empty()) {
        // not ready
        return;
    }

    State->GetAddConfirmedBlobsState().SetStatus(EOperationStatus::Enqueued);

    auto request =
        std::make_unique<TEvPartitionPrivate::TEvAddConfirmedBlobsRequest>(
            MakeIntrusive<TCallContext>(CreateRequestId())
        );

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] AddConfirmedBlobs request sent: %lu",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        request->CallContext->RequestId);

    NCloud::Send(
        ctx,
        SelfId(),
        std::move(request));
}

void TPartitionActor::HandleAddConfirmedBlobs(
    const TEvPartitionPrivate::TEvAddConfirmedBlobsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    using TMethod = TEvPartitionPrivate::TAddConfirmedBlobsMethod;
    auto requestInfo = CreateRequestInfo<TMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        BackgroundTaskStarted_Partition,
        requestInfo->CallContext->LWOrbit,
        "AddConfirmedBlobs",
        static_cast<ui32>(PartitionConfig.GetStorageMediaKind()),
        requestInfo->CallContext->RequestId,
        PartitionConfig.GetDiskId());

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        using TResponse = TEvPartitionPrivate::TEvAddConfirmedBlobsResponse;
        auto response = std::make_unique<TResponse>(
            MakeError(errorCode, std::move(errorReason))
        );

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "AddConfirmedBlobs",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetAddConfirmedBlobsState().Status == EOperationStatus::Started) {
        replyError(
            ctx,
            *requestInfo,
            E_TRY_AGAIN,
            "AddConfirmedBlobs already started");
        return;
    }

    if (State->GetConfirmedBlobs().empty()) {
        replyError(
            ctx,
            *requestInfo,
            S_ALREADY,
            "AddConfirmedBlobs nothing to do");
        return;
    }

    State->GetAddConfirmedBlobsState().SetStatus(EOperationStatus::Started);

    TRequests requests;
    for (const auto& entry: State->GetConfirmedBlobs()) {
        auto commitId = entry.first;
        const auto& blobs = entry.second;
        TVector<TAddMergedBlob> mergedBlobs(Reserve(blobs.size()));

        for (const auto& blob: blobs) {
            mergedBlobs.emplace_back(
                MakePartialBlobId(commitId, blob.UniqueId),
                blob.BlockRange,
                TBlockMask(), // skipMask
                blob.Checksums);
        }

        auto request = std::make_unique<TRequest>(
            requestInfo->CallContext,
            commitId,
            TVector<TAddMixedBlob>(),
            std::move(mergedBlobs),
            TVector<TAddFreshBlob>(),
            ADD_WRITE_RESULT
        );

        requests.push_back(std::move(request));
    }

    auto actor = NCloud::Register<TAddConfirmedBlobsActor>(
        ctx,
        SelfId(),
        requestInfo,
        std::move(requests));

    Actors.Insert(actor);
}

void TPartitionActor::HandleAddConfirmedBlobsCompleted(
    const TEvPartitionPrivate::TEvAddConfirmedBlobsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] AddConfirmedBlobs completed",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    const auto* msg = ev->Get();

    State->GetAddConfirmedBlobsState().SetStatus(EOperationStatus::Idle);

    Actors.Erase(ev->Sender);
    if (FAILED(msg->GetStatus())) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%lu][d:%s] Stop tablet because of AddConfirmedBlobs error: %s",
            TabletID(),
            PartitionConfig.GetDiskId().c_str(),
            FormatError(msg->GetError()).data());

       ReportAddConfirmedBlobsError();
       Suicide(ctx);
       return;
    }

    UpdateCPUUsageStat(
        ctx.Now(),
        CyclesToDurationSafe(msg->ExecCycles).MicroSeconds());
    auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.AddConfirmedBlobs.AddRequest(time);

    EnqueueAddConfirmedBlobsIfNeeded(ctx);
    ProcessCommitQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
