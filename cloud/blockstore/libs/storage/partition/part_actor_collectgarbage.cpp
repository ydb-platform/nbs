#include "part_actor.h"

#include <cloud/storage/core/libs/common/format.h>

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NCloud::NStorage;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCollectGarbageActor final
    : public TActorBootstrapped<TCollectGarbageActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TActorId Tablet;
    const TString DiskId;
    const TTabletStorageInfoPtr TabletInfo;
    const ui64 LastGCCommitId;
    const ui64 CollectCommitId;
    const ui32 RecordGeneration;
    const ui32 PerGenerationCounter;

    TVector<TPartialBlobId> NewBlobs;
    TVector<TPartialBlobId> GarbageBlobs;

    TVector<ui32> MixedAndMergedChannels;

    size_t RequestsInFlight = 0;
    NProto::TError Error;

    bool CleanupWholeHistory;

public:
    TCollectGarbageActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        TString diskId,
        TTabletStorageInfoPtr tabletInfo,
        ui64 lastGCCommitId,
        ui64 collectCommitId,
        ui32 recordGeneration,
        ui32 perGenerationCounter,
        TVector<TPartialBlobId> newBlobs,
        TVector<TPartialBlobId> garbageBlobs,
        TVector<ui32> mixedAndMergedChannels,
        bool cleanupWholeHistory);

    void Bootstrap(const TActorContext& ctx);

private:
    void CollectGarbage(const TActorContext& ctx);
    void DeleteGarbage(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    void HandleError(NProto::TError error);
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleCollectGarbageResult(
        const TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeleteGarbageResponse(
        const TEvPartitionPrivate::TEvDeleteGarbageResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCollectGarbageActor::TCollectGarbageActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        TString diskId,
        TTabletStorageInfoPtr tabletInfo,
        ui64 lastGCCommitId,
        ui64 collectCommitId,
        ui32 recordGeneration,
        ui32 perGenerationCounter,
        TVector<TPartialBlobId> newBlobs,
        TVector<TPartialBlobId> garbageBlobs,
        TVector<ui32> mixedAndMergedChannels,
        bool cleanupWholeHistory)
    : RequestInfo(std::move(requestInfo))
    , Tablet(tablet)
    , DiskId(std::move(diskId))
    , TabletInfo(std::move(tabletInfo))
    , LastGCCommitId(lastGCCommitId)
    , CollectCommitId(collectCommitId)
    , RecordGeneration(recordGeneration)
    , PerGenerationCounter(perGenerationCounter)
    , NewBlobs(std::move(newBlobs))
    , GarbageBlobs(std::move(garbageBlobs))
    , MixedAndMergedChannels(std::move(mixedAndMergedChannels))
    , CleanupWholeHistory(cleanupWholeHistory)
{}

void TCollectGarbageActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "CollectGarbage",
        RequestInfo->CallContext->RequestId);

    CollectGarbage(ctx);

    if (!RequestsInFlight) {
        DeleteGarbage(ctx);
    }
}

void TCollectGarbageActor::CollectGarbage(const TActorContext& ctx)
{
    // there could be blobs added and deleted before GC occurs -
    // we should not report them at all
    auto newBlobs = NewBlobs;
    auto garbageBlobs = GarbageBlobs;
    RemoveDuplicates(newBlobs, garbageBlobs, CollectCommitId);

    auto requests = BuildGCRequests(
        *TabletInfo,
        MixedAndMergedChannels,
        newBlobs,
        garbageBlobs,
        CleanupWholeHistory,
        LastGCCommitId,
        CollectCommitId,
        PerGenerationCounter);

    auto collect = ParseCommitId(CollectCommitId);
    for (ui32 channel: MixedAndMergedChannels) {
        for (auto& kv: requests.GetRequests(channel)) {
            auto request = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
                TabletInfo->TabletID,               // tablet
                RecordGeneration,                   // record generation
                PerGenerationCounter,               // per generation counter
                channel,                            // collect channel
                true,                               // yes, collect
                collect.first,                      // collect generation
                collect.second,                     // collect step
                kv.second.Keep.release(),           // keep
                kv.second.DoNotKeep.release(),      // do not keep
                TInstant::Max(),                    // deadline
                false,                              // multi collect not allowed
                false);                             // soft barrier

            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu][d:%s] %s",
                TabletInfo->TabletID,
                DiskId.c_str(),
                request->Print(true).data());

            SendToBSProxy(
                ctx,
                kv.first,
                request.release());

            ++RequestsInFlight;
        }
    }
}

void TCollectGarbageActor::DeleteGarbage(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvDeleteGarbageRequest>(
        RequestInfo->CallContext,
        CollectCommitId,
        std::move(NewBlobs),
        std::move(GarbageBlobs));

    NCloud::Send(
        ctx,
        Tablet,
        std::move(request));
}

void TCollectGarbageActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageCompleted>(error);

    request->ExecCycles = RequestInfo->GetExecCycles();
    request->TotalCycles = RequestInfo->GetTotalCycles();

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TCollectGarbageActor::HandleError(NProto::TError error)
{
    if (FAILED(error.GetCode())) {
        ReportCollectGarbageError();
        Error = std::move(error);
    }
}

void TCollectGarbageActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageResponse>(
        std::move(Error)
    );

    NotifyCompleted(ctx, response->GetError());

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "CollectGarbage",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCollectGarbageActor::HandleCollectGarbageResult(
    const TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    HandleError(MakeKikimrError(msg->Status, msg->ErrorReason));

    Y_ABORT_UNLESS(RequestsInFlight > 0);
    if (--RequestsInFlight > 0) {
        return;
    }

    if (FAILED(Error.GetCode()) || (!NewBlobs && !GarbageBlobs)) {
        ReplyAndDie(ctx);
        return;
    }

    DeleteGarbage(ctx);
}

void TCollectGarbageActor::HandleDeleteGarbageResponse(
    const TEvPartitionPrivate::TEvDeleteGarbageResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    RequestInfo->AddExecCycles(msg->ExecCycles);

    HandleError(msg->GetError());
    ReplyAndDie(ctx);
}

void TCollectGarbageActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

STFUNC(TCollectGarbageActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvCollectGarbageResult, HandleCollectGarbageResult);
        HFunc(TEvPartitionPrivate::TEvDeleteGarbageResponse, HandleDeleteGarbageResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCollectGarbageHardActor final
    : public TActorBootstrapped<TCollectGarbageHardActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TActorId Tablet;
    const TString DiskId;
    const TTabletStorageInfoPtr TabletInfo;
    const ui64 CollectCommitId;
    const ui32 RecordGeneration;
    const ui32 PerGenerationCounter;

    TVector<TPartialBlobId> KnownBlobIds;

    TVector<ui32> MixedAndMergedChannels;

    size_t RequestsInFlight = 0;
    NProto::TError Error;

public:
    TCollectGarbageHardActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        TString diskId,
        TTabletStorageInfoPtr tabletInfo,
        ui64 collectCommitId,
        ui32 recordGeneration,
        ui32 perGenerationCounter,
        TVector<TPartialBlobId> knownBlobIds,
        TVector<ui32> mixedAndMergedChannels);

    void Bootstrap(const TActorContext& ctx);

private:
    void CollectGarbage(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    void HandleError(NProto::TError error);

    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleCollectGarbageResult(
        const TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCollectGarbageHardActor::TCollectGarbageHardActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        TString diskId,
        TTabletStorageInfoPtr tabletInfo,
        ui64 collectCommitId,
        ui32 recordGeneration,
        ui32 perGenerationCounter,
        TVector<TPartialBlobId> knownBlobIds,
        TVector<ui32> mixedAndMergedChannels)
    : RequestInfo(std::move(requestInfo))
    , Tablet(tablet)
    , DiskId(std::move(diskId))
    , TabletInfo(std::move(tabletInfo))
    , CollectCommitId(collectCommitId)
    , RecordGeneration(recordGeneration)
    , PerGenerationCounter(perGenerationCounter)
    , KnownBlobIds(std::move(knownBlobIds))
    , MixedAndMergedChannels(std::move(mixedAndMergedChannels))
{
}

void TCollectGarbageHardActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "CollectGarbage",
        RequestInfo->CallContext->RequestId);

    CollectGarbage(ctx);
    Y_ABORT_UNLESS(RequestsInFlight);
}

void TCollectGarbageHardActor::CollectGarbage(const TActorContext& ctx)
{
    auto requests = BuildGCBarriers(
        *TabletInfo,
        MixedAndMergedChannels,
        KnownBlobIds,
        CollectCommitId);

    for (ui32 channel: MixedAndMergedChannels) {
        for (auto& kv: requests.GetRequests(channel)) {
            auto barrier = ParseCommitId(kv.second.CollectCommitId);

            auto request = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
                TabletInfo->TabletID,               // tablet
                RecordGeneration,                   // record generation
                PerGenerationCounter,               // per generation counter
                channel,                            // collect channel
                true,                               // yes, collect
                barrier.first,                      // collect generation
                barrier.second,                     // collect step
                nullptr,                            // keep
                nullptr,                            // do not keep
                TInstant::Max(),                    // deadline
                false,                              // multi collect not allowed
                true);                              // hard barrier

            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu][d:%s] %s",
                TabletInfo->TabletID,
                DiskId.c_str(),
                request->Print(true).data());

            SendToBSProxy(
                ctx,
                kv.first,
                request.release());

            ++RequestsInFlight;
        }
    }
}

void TCollectGarbageHardActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageCompleted>(error);

    request->ExecCycles = RequestInfo->GetExecCycles();
    request->TotalCycles = RequestInfo->GetTotalCycles();

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TCollectGarbageHardActor::HandleError(NProto::TError error)
{
    if (FAILED(error.GetCode())) {
        ReportCollectGarbageError();
        Error = std::move(error);
    }
}

void TCollectGarbageHardActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageResponse>(
        std::move(Error)
    );

    NotifyCompleted(ctx, response->GetError());

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "CollectGarbage",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCollectGarbageHardActor::HandleCollectGarbageResult(
    const TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    HandleError(MakeKikimrError(msg->Status, msg->ErrorReason));

    Y_ABORT_UNLESS(RequestsInFlight > 0);
    if (--RequestsInFlight > 0) {
        return;
    }

    ReplyAndDie(ctx);
}

void TCollectGarbageHardActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

STFUNC(TCollectGarbageHardActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvCollectGarbageResult, HandleCollectGarbageResult);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueCollectGarbageIfNeeded(const TActorContext& ctx)
{
    if (State->GetCollectGarbageState().Status != EOperationStatus::Idle) {
        // already enqueued
        return;
    }

    if (!State->CollectGarbageHardRequested) {
        ui64 commitId = State->GetCollectCommitId();

        size_t pendingBlobs = State->GetGarbageQueue().GetNewBlobsCount(commitId)
                            + State->GetGarbageQueue().GetGarbageBlobsCount(commitId);

        if (pendingBlobs < Config->GetCollectGarbageThreshold() &&
            State->GetStartupGcExecuted())
        {
            // not ready
            return;
        }
    }

    State->GetCollectGarbageState().SetStatus(EOperationStatus::Enqueued);

    auto request = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageRequest>(
        MakeIntrusive<TCallContext>(CreateRequestId()));

    if (State->GetCollectTimeout()) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s CollectGarbage request scheduled: %lu, %s",
            LogTitle.GetWithTime().c_str(),
            request->CallContext->RequestId,
            FormatDuration(State->GetCollectTimeout()).c_str());

        ctx.Schedule(State->GetCollectTimeout(), request.release());
    } else {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s CollectGarbage request sent: %lu",
            LogTitle.GetWithTime().c_str(),
            request->CallContext->RequestId);

        NCloud::Send(
            ctx,
            SelfId(),
            std::move(request));
    }
}

void TPartitionActor::HandleCollectGarbage(
    const TEvPartitionPrivate::TEvCollectGarbageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        BackgroundTaskStarted_Partition,
        requestInfo->CallContext->LWOrbit,
        "CollectGarbage",
        static_cast<ui32>(PartitionConfig.GetStorageMediaKind()),
        requestInfo->CallContext->RequestId,
        PartitionConfig.GetDiskId());

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        auto response = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "CollectGarbage",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (State->GetCollectGarbageState().Status == EOperationStatus::Started) {
        replyError(ctx, *requestInfo, E_TRY_AGAIN, "collection already started");
        return;
    }

    const ui64 commitId = State->GetCollectCommitId();
    // use tablet generation as record generation
    const ui32 recordGeneration = Executor()->Generation();

    if (State->CollectGarbageHardRequested) {
        State->CollectGarbageHardRequested = false;

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Start hard GC @%lu",
            LogTitle.GetWithTime().c_str(),
            commitId);

        State->GetCollectGarbageState().SetStatus(EOperationStatus::Started);

        AddTransaction<TEvPartitionPrivate::TCollectGarbageMethod>(*requestInfo);

        ExecuteTx(ctx, CreateTx<TCollectGarbage>(requestInfo, commitId));
        return;
    }

    auto newBlobs = State->GetGarbageQueue().GetNewBlobs(commitId);
    auto garbageBlobs = State->GetGarbageQueue().GetGarbageBlobs(commitId);

    if (!newBlobs && !garbageBlobs && State->GetStartupGcExecuted()) {
        State->GetCollectGarbageState().SetStatus(EOperationStatus::Idle);

        replyError(ctx, *requestInfo, S_ALREADY, "nothing to collect");
        return;
    }

    auto nextPerGenerationCounter = State->NextCollectPerGenerationCounter();
    if (nextPerGenerationCounter == InvalidCollectPerGenerationCounter) {
        RebootPartitionOnCollectCounterOverflow(ctx, "CollectGarbage");
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start GC @%lu @%lu (new: %u, garbage: %u)",
        LogTitle.GetWithTime().c_str(),
        commitId,
        nextPerGenerationCounter,
        static_cast<ui32>(newBlobs.size()),
        static_cast<ui32>(garbageBlobs.size()));

    State->GetCollectGarbageState().SetStatus(EOperationStatus::Started);

    TVector<ui32> mixedAndMergedChannels = State->GetChannelsByKind([](auto kind) {
        return kind == EChannelDataKind::Mixed || kind == EChannelDataKind::Merged;
    });

    Y_ABORT_UNLESS(newBlobs || garbageBlobs || !State->GetStartupGcExecuted());
    auto actor = NCloud::Register<TCollectGarbageActor>(
        ctx,
        requestInfo,
        SelfId(),
        PartitionConfig.GetDiskId(),
        Info(),
        State->GetLastCollectCommitId(),
        commitId,
        recordGeneration,
        nextPerGenerationCounter,
        std::move(newBlobs),
        std::move(garbageBlobs),
        std::move(mixedAndMergedChannels),
        !State->GetStartupGcExecuted());
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Partition registered TCollectGarbageActor with id [%lu]",
        LogTitle.GetWithTime().c_str(),
        ToString(actor).c_str());

    Actors.Insert(actor);
}

void TPartitionActor::HandleCollectGarbageCompleted(
    const TEvPartitionPrivate::TEvCollectGarbageCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s GC failed. error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(msg->GetError()).c_str());

        State->RegisterCollectError();
    } else {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s GC completed",
            LogTitle.GetWithTime().c_str());

        State->RegisterCollectSuccess();
        State->SetStartupGcExecuted();
        if (!IsFirstGarbageCollectionCompleted()) {
            SetFirstGarbageCollectionCompleted();
            SendGarbageCollectorCompleted(ctx);
        }
    }

    State->GetCollectGarbageState().SetStatus(EOperationStatus::Idle);

    Actors.Erase(ev->Sender);

    UpdateCPUUsageStat(ctx.Now(), msg->ExecCycles);

    EnqueueCollectGarbageIfNeeded(ctx);

    auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.CollectGarbage.AddRequest(time);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareCollectGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCollectGarbage& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    return db.ReadNewBlobs(args.KnownBlobIds)
        && db.ReadGarbageBlobs(args.KnownBlobIds);
}

void TPartitionActor::ExecuteCollectGarbage(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TCollectGarbage& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteCollectGarbage(
    const TActorContext& ctx,
    TTxPartition::TCollectGarbage& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    auto nextPerGenerationCounter = State->NextCollectPerGenerationCounter();
    if (nextPerGenerationCounter == InvalidCollectPerGenerationCounter) {
        RebootPartitionOnCollectCounterOverflow(ctx, "CollectGarbageHard");
        return;
    }

    TVector<ui32> mixedAndMergedChannels = State->GetChannelsByKind([](auto kind){
        return kind == EChannelDataKind::Mixed || kind == EChannelDataKind::Merged;
    });

    auto actor = NCloud::Register<TCollectGarbageHardActor>(
        ctx,
        args.RequestInfo,
        SelfId(),
        PartitionConfig.GetDiskId(),
        Info(),
        args.CollectCommitId,
        Executor()->Generation(),
        nextPerGenerationCounter,
        std::move(args.KnownBlobIds),
        std::move(mixedAndMergedChannels));
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Partition registered TCollectGarbageHardActor with id %s",
        LogTitle.GetWithTime().c_str(),
        actor.ToString().c_str());

    Actors.Insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
