#include "drain_actor_companion.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <library/cpp/lwtrace/all.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <ranges>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TDrainActorCompanion::TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        TString loggingId,
        const TRequestBoundsTracker* requestBoundsTracker)
    : RequestsInProgress(requestsInProgress)
    , RequestBoundsTracker(requestBoundsTracker)
    , LoggingId(std::move(loggingId))
{}

TDrainActorCompanion::TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        ui64 tabletID,
        const TRequestBoundsTracker* requestBoundsTracker)
    : TDrainActorCompanion(
          requestsInProgress,
          ToString(tabletID),
          requestBoundsTracker)
{}

void TDrainActorCompanion::HandleDrain(
    const NPartition::TEvPartition::TEvDrainRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "Drain",
        requestInfo->CallContext->RequestId);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Start drain",
        LoggingId.c_str());

    constexpr ui32 MaxDrainRequests = 10;
    if (RequestsInProgress.WriteRequestInProgress() &&
        DrainRequests.size() >= MaxDrainRequests)
    {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<NPartition::TEvPartition::TEvDrainResponse>(
                MakeError(E_REJECTED, "too many drain requests enqueued")));
        return;
    }

    DrainRequests.push_back(std::move(requestInfo));
    DoProcessDrainRequests(ctx);
}

void TDrainActorCompanion::HandleWaitForInFlightWrites(
    const NPartition::TEvPartition::TEvWaitForInFlightWritesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (WaitForInFlightWritesRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                NPartition::TEvPartition::TEvWaitForInFlightWritesResponse>(
                MakeError(
                    E_REJECTED,
                    "Already waiting for in-flight write requests")));
        return;
    }

    WaitForInFlightWritesRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Start wait for in-flight write requests",
        LoggingId.c_str());

    Y_DEBUG_ABORT_UNLESS(!RequestsInProgress.IsWaitingForInFlightWrites());
    RequestsInProgress.WaitForInFlightWrites();
    DoProcessWaitForInFlightWritesRequests(ctx);
}

void TDrainActorCompanion::AddDrainRangeRequest(
    const TActorContext& ctx,
    TRequestInfoPtr reqInfo,
    TBlockRange64 range)
{
    Y_DEBUG_ABORT_UNLESS(RequestBoundsTracker);
    DrainRangeRequests.emplace_back(std::move(reqInfo), range);
    DoProcessDrainRangeRequests(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDrainActorCompanion::ProcessDrainRequests(const TActorContext& ctx)
{
    DoProcessDrainRequests(ctx);
    DoProcessWaitForInFlightWritesRequests(ctx);
    if (RequestBoundsTracker) {
        DoProcessDrainRangeRequests(ctx);
    }
}

void TDrainActorCompanion::DoProcessDrainRequests(const TActorContext& ctx)
{
    if (DrainRequests.empty() || RequestsInProgress.WriteRequestInProgress()) {
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Complete drain",
        LoggingId.c_str());

    for (auto& requestInfo: DrainRequests) {
        auto response =
            std::make_unique<NPartition::TEvPartition::TEvDrainResponse>();

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "Drain",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    }

    DrainRequests.clear();
}

void TDrainActorCompanion::DoProcessWaitForInFlightWritesRequests(
    const TActorContext& ctx)
{
    if (!WaitForInFlightWritesRequest ||
        RequestsInProgress.IsWaitingForInFlightWrites())
    {
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Complete wait for in-flight write requests",
        LoggingId.c_str());

    auto response = std::make_unique<
        NPartition::TEvPartition::TEvWaitForInFlightWritesResponse>();
    NCloud::Reply(ctx, *WaitForInFlightWritesRequest, std::move(response));
    WaitForInFlightWritesRequest.Reset();
}

void TDrainActorCompanion::RemoveDrainRangeRequest(
    const TActorContext& ctx,
    TBlockRange64 range)
{
    Y_DEBUG_ABORT_UNLESS(RequestBoundsTracker);

    auto cancelRemovedRequest = [&](const TDrainRangeInfo& drainRequest)
    {
        if (drainRequest.RangeToDrain != range) {
            return false;
        }

        NCloud::Reply(
            ctx,
            *drainRequest.RequestInfo,
            std::make_unique<
                NPartition::TEvPartition::TEvLockAndDrainRangeResponse>(
                MakeError(E_CANCELLED)));
        return true;
    };

    EraseIf(DrainRangeRequests, cancelRemovedRequest);
}

void TDrainActorCompanion::DoProcessDrainRangeRequests(const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(RequestBoundsTracker);
    if (!RequestBoundsTracker) {
        return;
    }

    TVector<ui64> reqsToErase;

    auto checkNoRequestsInRange = [&](const TDrainRangeInfo& drainRequest)
    {
        if (RequestBoundsTracker->OverlapsWithRequest(
                drainRequest.RangeToDrain))
        {
            return false;
        }

        NCloud::Reply(
            ctx,
            *drainRequest.RequestInfo,
            std::make_unique<
                NPartition::TEvPartition::TEvLockAndDrainRangeResponse>());
        return true;
    };

    EraseIf(DrainRangeRequests, checkNoRequestsInRange);
}

}  // namespace NCloud::NBlockStore::NStorage
