#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <cloud/storage/core/libs/tablet/blob_id.h>
#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCollectGarbageActor final
    : public TActorBootstrapped<TCollectGarbageActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;

    const IProfileLogPtr ProfileLog;
    const TString FileSystemId;

    const TTabletStorageInfoPtr TabletInfo;
    const TVector<ui32> Channels;
    const TVector<TPartialBlobId> NewBlobs;
    const TVector<TPartialBlobId> GarbageBlobs;
    const ui64 LastCollectCommitId;
    const ui64 CollectCommitId;
    const ui32 CollectCounter;
    const bool CleanupWholeHistory;
    ui32 OperationSize = 0;

    NProto::TProfileLogRequestInfo ProfileLogRequest;

    size_t RequestsInFlight = 0;
    NProto::TError Error;

public:
    TCollectGarbageActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        IProfileLogPtr profileLog,
        TString fileSystemId,
        TTabletStorageInfoPtr tabletInfo,
        TVector<ui32> channels,
        TVector<TPartialBlobId> newBlobs,
        TVector<TPartialBlobId> garbageBlobs,
        ui64 lastCollectCommitId,
        ui64 collectCommitId,
        ui32 collectCounter,
        bool cleanupWholeHistory,
        NProto::TProfileLogRequestInfo profileLogRequest);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CollectGarbage(const TActorContext& ctx);
    void HandleCollectGarbageResult(
        const TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
        const TActorContext& ctx);

    void DeleteGarbage(const TActorContext& ctx);
    void HandleDeleteGarbageResponse(
        const TEvIndexTabletPrivate::TEvDeleteGarbageResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleError(NProto::TError error);
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    bool ShouldDeleteGarbage() const
    {
        return !RequestsInFlight
            && !HasError(Error)
            && (NewBlobs.size() || GarbageBlobs.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

TCollectGarbageActor::TCollectGarbageActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        IProfileLogPtr profileLog,
        TString fileSystemId,
        TTabletStorageInfoPtr tabletInfo,
        TVector<ui32> channels,
        TVector<TPartialBlobId> newBlobs,
        TVector<TPartialBlobId> garbageBlobs,
        ui64 lastCollectCommitId,
        ui64 collectCommitId,
        ui32 collectCounter,
        bool cleanupWholeHistory,
        NProto::TProfileLogRequestInfo profileLogRequest)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , ProfileLog(std::move(profileLog))
    , FileSystemId(std::move(fileSystemId))
    , TabletInfo(std::move(tabletInfo))
    , Channels(std::move(channels))
    , NewBlobs(std::move(newBlobs))
    , GarbageBlobs(std::move(garbageBlobs))
    , LastCollectCommitId(lastCollectCommitId)
    , CollectCommitId(collectCommitId)
    , CollectCounter(collectCounter)
    , CleanupWholeHistory(cleanupWholeHistory)
    , ProfileLogRequest(std::move(profileLogRequest))
{
    for (const auto& b: NewBlobs) {
        OperationSize += b.BlobSize();
    }

    for (const auto& b: GarbageBlobs) {
        OperationSize += b.BlobSize();
    }
}

void TCollectGarbageActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "CollectGarbage");

    Become(&TThis::StateWork);
    CollectGarbage(ctx);

    if (ShouldDeleteGarbage()) {
        DeleteGarbage(ctx);
    }
}

void TCollectGarbageActor::CollectGarbage(const TActorContext& ctx)
{
    auto newBlobs = NewBlobs;
    auto garbageBlobs = GarbageBlobs;
    RemoveDuplicates(newBlobs, garbageBlobs, CollectCommitId);

    AddRange(
        CollectCommitId,
        LastCollectCommitId,
        newBlobs.size(),
        garbageBlobs.size(),
        ProfileLogRequest);

    auto requests = BuildGCRequests(
        *TabletInfo,
        Channels,
        newBlobs,
        garbageBlobs,
        CleanupWholeHistory,
        LastCollectCommitId,
        CollectCommitId,
        CollectCounter);

    auto [collectGen, collectStep] = ParseCommitId(CollectCommitId);
    for (ui32 channel: Channels) {
        for (auto& [proxyId, req]: requests.GetRequests(channel)) {
            auto request = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
                TabletInfo->TabletID,       // tablet
                collectGen,                 // record generation
                CollectCounter,             // per generation counter
                channel,                    // collect channel
                true,                       // yes, collect
                collectGen,                 // collect generation
                collectStep,                // collect step
                req.Keep.release(),         // keep list
                req.DoNotKeep.release(),    // do not keep list
                TInstant::Max(),            // deadline
                false,                      // multi collect not allowed
                false);                     // soft barrier

            LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
                "%s %s",
                LogTag.c_str(),
                request->Print(true).c_str());

            SendToBSProxy(ctx, proxyId, request.release());
            ++RequestsInFlight;
        }
    }
}

void TCollectGarbageActor::HandleCollectGarbageResult(
    const TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    HandleError(MakeKikimrError(msg->Status, msg->ErrorReason));

    TABLET_VERIFY(RequestsInFlight);
    if (--RequestsInFlight > 0) {
        return;
    }

    if (!ShouldDeleteGarbage()) {
        ReplyAndDie(ctx, Error);
        return;
    }

    DeleteGarbage(ctx);
}

void TCollectGarbageActor::DeleteGarbage(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
        "%s deleting garbage: %lu %lu",
        LogTag.c_str(),
        NewBlobs.size(),
        GarbageBlobs.size());

    auto request = std::make_unique<TEvIndexTabletPrivate::TEvDeleteGarbageRequest>(
        RequestInfo->CallContext,
        CollectCommitId,
        std::move(NewBlobs),
        std::move(GarbageBlobs));

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TCollectGarbageActor::HandleDeleteGarbageResponse(
    const TEvIndexTabletPrivate::TEvDeleteGarbageResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TCollectGarbageActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TCollectGarbageActor::HandleError(NProto::TError error)
{
    if (HasError(error)) {
        Error = std::move(error);
        ReportCollectGarbageError();
    }
}

void TCollectGarbageActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    // log request
    FinalizeProfileLogRequestInfo(
        std::move(ProfileLogRequest),
        ctx.Now(),
        FileSystemId,
        error,
        ProfileLog);

    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvCollectGarbageCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            1,
            OperationSize,
            ctx.Now() - RequestInfo->StartedTs);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "CollectGarbage");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<TEvIndexTabletPrivate::TEvCollectGarbageResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TCollectGarbageActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvCollectGarbageResult, HandleCollectGarbageResult);
        HFunc(TEvIndexTabletPrivate::TEvDeleteGarbageResponse, HandleDeleteGarbageResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::EnqueueCollectGarbageIfNeeded(const TActorContext& ctx)
{
    const ui64 garbageQueueSize = GetGarbageQueueSize();
    if (garbageQueueSize < Config->GetCollectGarbageThreshold()
            && GetStartupGcExecuted())
    {
        return;
    }

    if (CollectGarbageState.Enqueue()) {
        if (!CollectGarbageState.GetBackoffTimeout()) {
            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s CollectGarbage request sent",
                LogTag.c_str());

            ctx.Send(
                SelfId(),
                new TEvIndexTabletPrivate::TEvCollectGarbageRequest());
        } else {
            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s CollectGarbage request scheduled: %s",
                LogTag.c_str(),
                CollectGarbageState.GetBackoffTimeout().ToString().data());

            ctx.Schedule(
                CollectGarbageState.GetBackoffTimeout(),
                new TEvIndexTabletPrivate::TEvCollectGarbageRequest());
        }
    }
}

void TIndexTabletActor::HandleCollectGarbage(
    const TEvIndexTabletPrivate::TEvCollectGarbageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    NProto::TProfileLogRequestInfo profileLogRequest;
    InitProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreSystemRequest::CollectGarbage,
        ctx.Now());

    auto* msg = ev->Get();

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        msg->CallContext,
        "CollectGarbage",
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    auto replyError = [&] (
        const TActorContext& ctx,
        auto& ev,
        NProto::TProfileLogRequestInfo profileLogRequest,
        const NProto::TError& error)
    {
        // log request
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);

        FILESTORE_TRACK(
            ResponseSent_Tablet,
            ev.Get()->CallContext,
            "CollectGarbage");

        if (ev.Sender != ctx.SelfID) {
            auto response = std::make_unique<TEvIndexTabletPrivate::TEvCollectGarbageResponse>(
                error);
            NCloud::Reply(ctx, ev, std::move(response));
        }
    };

    if (!CollectGarbageState.Start()) {
        replyError(
            ctx,
            *ev,
            std::move(profileLogRequest),
            MakeError(S_ALREADY, "CollectGarbage is in progress"));
        return;
    }

    ui64 collectCommitId = GetCollectCommitId();

    auto newBlobs = GetNewBlobs(collectCommitId);
    auto garbageBlobs = GetGarbageBlobs(collectCommitId);

    if (!newBlobs && !garbageBlobs && GetStartupGcExecuted()) {
        CollectGarbageState.Complete();

        replyError(
            ctx,
            *ev,
            std::move(profileLogRequest),
            MakeError(S_ALREADY, "nothing to collect"));
        return;
    }

    ui64 lastCollectCommitId = GetLastCollectCommitId();
    ui32 collectCounter = NextCollectCounter();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s CollectGarbage started (collect: %lu, new: %u, garbage: %u)",
        LogTag.c_str(),
        collectCommitId,
        static_cast<ui32>(newBlobs.size()),
        static_cast<ui32>(garbageBlobs.size()));

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    auto channels = GetChannels(EChannelDataKind::Mixed);

    auto actor = std::make_unique<TCollectGarbageActor>(
        LogTag,
        ctx.SelfID,
        std::move(requestInfo),
        ProfileLog,
        GetFileSystemId(),
        Info(),
        std::move(channels),
        std::move(newBlobs),
        std::move(garbageBlobs),
        lastCollectCommitId,
        collectCommitId,
        collectCounter,
        !GetStartupGcExecuted(),
        std::move(profileLogRequest));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleCollectGarbageCompleted(
    const TEvIndexTabletPrivate::TEvCollectGarbageCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!HasError(msg->Error)) {
        CollectGarbageState.Complete();
        if (!GetStartupGcExecuted()) {
            SetStartupGcExecuted();
        }

        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s CollectGarbage completed",
            LogTag.c_str());
    } else {
        CollectGarbageState.Fail();
        LOG_ERROR(ctx, TFileStoreComponents::TABLET,
            "%s CollectGarbage failed: %s",
            LogTag.c_str(),
            FormatError(msg->GetError()).c_str());
    }

    WorkerActors.erase(ev->Sender);
    EnqueueCollectGarbageIfNeeded(ctx);

    Metrics.CollectGarbage.Update(msg->Count, msg->Size, msg->Time);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleReleaseCollectBarrier(
    const TEvIndexTabletPrivate::TEvReleaseCollectBarrier::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    auto commitId = ev->Get()->CommitId;
    for (ui32 i = 0; i < ev->Get()->Count; ++i) {
        // We do not check if the barrier was acquired, because the barrier may
        // have already been released by a completed three-stage write operation
        TryReleaseCollectBarrier(commitId);
    }
}

}   // namespace NCloud::NFileStore::NStorage
