#include "actor_trimfreshlog.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/tablet/gc_logic.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <util/string/join.h>

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
        bool hard,
        TVector<TPartialBlobId> keep,
        TVector<TPartialBlobId> doNotKeep,
        TVector<ui32> freshChannels,
        TVolumeLabelsConstPtr volumeLabels,
        TDuration timeout)
    : RequestInfo(std::move(requestInfo))
    , PartitionActorId(partitionActorId)
    , TabletInfo(std::move(tabletInfo))
    , TrimFreshLogToCommitId(trimFreshLogToCommitId)
    , RecordGeneration(recordGeneration)
    , PerGenerationCounter(perGenerationCounter)
    , Hard(hard)
    , Keep(std::move(keep))
    , DoNotKeep(std::move(doNotKeep))
    , FreshChannels(std::move(freshChannels))
    , VolumeLabels(std::move(volumeLabels))
    , Timeout(timeout)
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
    const ui64 tabletId = TabletInfo->TabletID;

    const bool hasSomeFlags = !Keep.empty() || !DoNotKeep.empty();
    STORAGE_VERIFY_C(
        !(Hard && hasSomeFlags),
        TWellKnownEntityTypes::TABLET,
        tabletId,
        "Hard barrier with flags, keep: " << Keep.size()
                                          << ", do not keep: "
                                          << DoNotKeep.size());
    STORAGE_VERIFY_C(
        Keep.size() + DoNotKeep.size() <= MaxCollectGarbageFlagsPerMessage,
        TWellKnownEntityTypes::TABLET,
        tabletId,
        "Too many flags, keep: " << Keep.size()
                                 << ", do not keep: " << DoNotKeep.size());

    // Generation 0 never matches the current one, so with cleanupHistory the
    // barrier goes to every group in each channel's history.
    auto requests = BuildGCRequests(
        *TabletInfo,
        FreshChannels,
        Keep,
        DoNotKeep,
        true,                     // cleanupHistory
        0,                        // lastGCCommitId
        TrimFreshLogToCommitId,   // collectCommitId
        PerGenerationCounter);    // collectCounter

    auto [barrierGen, barrierStep] = ParseCommitId(TrimFreshLogToCommitId);
    for (ui32 channelId: FreshChannels) {
        for (auto& [bsProxyId, flags]: requests.GetRequests(channelId)) {
            auto deadline = Timeout ? ctx.Now() + Timeout : TInstant::Max();

            auto request = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
                tabletId,                   // tabletId
                RecordGeneration,           // record generation
                PerGenerationCounter,       // per generation counter
                channelId,                  // channel
                true,                       // yes, collect
                barrierGen,                 // barrier gen
                barrierStep,                // barrier step
                flags.Keep.release(),       // keep
                flags.DoNotKeep.release(),  // do not keep
                deadline,                   // deadline
                false,                      // multicollect not allowed
                Hard);                      // hard barrier

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

    STORAGE_VERIFY_C(
        RequestsInFlight > 0,
        TWellKnownEntityTypes::TABLET,
        tabletId,
        "No CollectGarbage requests to send, trim fresh log to: "
            << barrierGen << ":" << barrierStep << ", fresh channels: ["
            << JoinSeq(", ", FreshChannels) << "], keep: " << Keep.size()
            << ", do not keep: " << DoNotKeep.size());
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
        TCritEventParams critEventParams =
             {{"TabletId", TabletInfo->TabletID}};
        if (msg->Status == NKikimrProto::EReplyStatus::DEADLINE) {
            ReportTrimFreshLogTimeout(
                VolumeLabels,
                FormatError(error),
                critEventParams);
        } else {
            ReportTrimFreshLogError(
                VolumeLabels,
                FormatError(error),
                critEventParams);
        }
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
