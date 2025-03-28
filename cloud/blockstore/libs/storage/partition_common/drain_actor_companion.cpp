#include "drain_actor_companion.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <library/cpp/lwtrace/all.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TDrainActorCompanion::TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        TString loggingId)
    : RequestsInProgress(requestsInProgress)
    , LoggingId(std::move(loggingId))
{}

TDrainActorCompanion::TDrainActorCompanion(
        IRequestsInProgress& requestsInProgress,
        ui64 tabletID)
    : TDrainActorCompanion(requestsInProgress, ToString(tabletID))
{}

void TDrainActorCompanion::HandleDrain(
    const NPartition::TEvPartition::TEvDrainRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
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
    const NActors::TActorContext& ctx)
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

////////////////////////////////////////////////////////////////////////////////

void TDrainActorCompanion::ProcessDrainRequests(
    const NActors::TActorContext& ctx)
{
    DoProcessDrainRequests(ctx);
    DoProcessWaitForInFlightWritesRequests(ctx);
}

void TDrainActorCompanion::DoProcessDrainRequests(
    const NActors::TActorContext& ctx)
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
    const NActors::TActorContext& ctx)
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

}  // namespace NCloud::NBlockStore::NStorage
