#include "actor_trimfreshlog.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/tablet/gc_logic.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TTrimFreshLogActor::TTrimFreshLogActor(
        TRequestInfoPtr requestInfo,
        const TActorId& partitionActorId,
        TTabletStorageInfoPtr tabletInfo,
        ui64 trimFreshLogToCommitId,
        ui32 recordGeneration,
        ui32 perGenerationCounter,
        TVector<ui32> freshChannels)
    : RequestInfo(std::move(requestInfo))
    , PartitionActorId(partitionActorId)
    , TabletInfo(std::move(tabletInfo))
    , TrimFreshLogToCommitId(trimFreshLogToCommitId)
    , RecordGeneration(recordGeneration)
    , PerGenerationCounter(perGenerationCounter)
    , FreshChannels(std::move(freshChannels))
{}

void TTrimFreshLogActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "TrimFreshLog",
        RequestInfo->CallContext->RequestId);

    TrimFreshLog(ctx);
}

void TTrimFreshLogActor::TrimFreshLog(const TActorContext& ctx)
{
    const auto tabletId = TabletInfo->TabletID;

    auto barriers = BuildGCBarriers(
        *TabletInfo,
        FreshChannels,
        TVector<TPartialBlobId>(),  // knownBlobIds
        TrimFreshLogToCommitId);

    for (auto channelId: FreshChannels) {
        for (const auto& [bsProxyId, barrier]: barriers.GetRequests(channelId)) {
            auto [barrierGen, barrierStep] = ParseCommitId(barrier.CollectCommitId);

            auto request = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
                tabletId,               // tabletId
                RecordGeneration,       // record generation
                PerGenerationCounter,   // per generation counter
                channelId,              // channel
                true,                   // yes, collect
                barrierGen,             // barrier gen
                barrierStep,            // barrier step
                nullptr,                // keep
                nullptr,                // do not keep
                TInstant::Max(),        // deadline
                false,                  // multicollect not allowed
                false);                 // soft barrier

            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] %s",
                tabletId,
                request->Print(true).data());

            SendToBSProxy(
                ctx,
                bsProxyId,
                request.release());

            ++RequestsInFlight;
        }
    }
}

void TTrimFreshLogActor::NotifyCompleted(const TActorContext& ctx)
{
    using TEvent = TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted;
    auto ev = std::make_unique<TEvent>(Error);

    ev->CommitId = TrimFreshLogToCommitId;
    ev->ExecCycles = RequestInfo->GetExecCycles();
    ev->TotalCycles = RequestInfo->GetTotalCycles();

    NCloud::Send(ctx, PartitionActorId, std::move(ev));
}

void TTrimFreshLogActor::ReplyAndDie(const TActorContext& ctx)
{
    using TResponse = TEvPartitionCommonPrivate::TEvTrimFreshLogResponse;
    auto response = std::make_unique<TResponse>(std::move(Error));

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "TrimFreshLog",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TTrimFreshLogActor::HandleCollectGarbageResult(
    const TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (auto error = MakeKikimrError(msg->Status, msg->ErrorReason);
        HasError(error))
    {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Fresh blobs collect request failed: %u reason: %s",
            TabletInfo->TabletID,
            error.GetCode(),
            error.GetMessage().Quote().c_str());

        ReportTrimFreshLogError();
        Error = std::move(error);
    }

    Y_ABORT_UNLESS(RequestsInFlight > 0);
    if (--RequestsInFlight > 0) {
        return;
    }

    NotifyCompleted(ctx);
    ReplyAndDie(ctx);
}

void TTrimFreshLogActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

STFUNC(TTrimFreshLogActor::StateWork)
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

}   // namespace NCloud::NBlockStore::NStorage
