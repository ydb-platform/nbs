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

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Start drain",
        LoggingId.c_str());

    const ui32 maxDrainRequests = 10;
    if (RequestsInProgress.WriteRequestInProgress()
            && DrainRequests.size() >= maxDrainRequests)
    {
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<NPartition::TEvPartition::TEvDrainResponse>(
                MakeError(E_REJECTED, "too many drain requests enqueued")));
        return;
    }

    DrainRequests.push_back(std::move(requestInfo));
    ProcessDrainRequests(ctx);
}

void TDrainActorCompanion::AddDrainRangeRequest(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr reqInfo,
    TBlockRange64 range)
{
    DrainRangeRequests.emplace_back(std::move(reqInfo), range);
    ProcessDrainRangeRequests(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDrainActorCompanion::ProcessDrainRequests(
    const NActors::TActorContext& ctx)
{
    ProcessDrainRangeRequests(ctx);
    if (RequestsInProgress.WriteRequestInProgress()) {
        return;
    }

    for (auto& requestInfo: DrainRequests) {
        auto response =
            std::make_unique<NPartition::TEvPartition::TEvDrainResponse>();

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "Drain",
            requestInfo->CallContext->RequestId);

        LOG_TRACE(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Complete drain",
            LoggingId.c_str());

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    }

    DrainRequests.clear();
}

void TDrainActorCompanion::ProcessDrainRangeRequests(const NActors::TActorContext& ctx)
{
    TVector<ui64> reqsToErase;

    for (size_t i = 0;  i < DrainRangeRequests.size(); ++i) {
        auto& drainReq = DrainRangeRequests[i];

        if(RequestsInProgress.HasWriteRequestInRange(drainReq.RangeToDrain)) {
            continue;
        }

        NCloud::Reply(
            ctx,
            *drainReq.RequestInfo,
            std::make_unique<
                NPartition::TEvPartition::TEvBlockAndDrainRangeResponse>());
        reqsToErase.emplace_back(i);
    }
    for (auto i : reqsToErase | std::views::reverse) {
        std::swap(DrainRangeRequests.back(), DrainRangeRequests[i]);
        DrainRangeRequests.pop_back();
    }
}

void TDrainActorCompanion::CancelDrainRangeRequest(
    const NActors::TActorContext& ctx,
    TBlockRange64 range,
    NActors::TActorId sender)
{
    TVector<ui64> reqsToErase;
    for (size_t i = 0; i < DrainRangeRequests.size(); ++i) {
        auto& drainReq = DrainRangeRequests[i];

        if (DrainRangeRequests[i].RangeToDrain != range ||
            DrainRangeRequests[i].RequestInfo->Sender != sender)
        {
            continue;
        }

        NCloud::Reply(
            ctx,
            *drainReq.RequestInfo,
            std::make_unique<
                NPartition::TEvPartition::TEvBlockAndDrainRangeResponse>(
                MakeError(E_CANCELLED)));
        reqsToErase.emplace_back(i);
    }

    for (auto i: reqsToErase | std::views::reverse) {
        std::swap(DrainRangeRequests.back(), DrainRangeRequests[i]);
        DrainRangeRequests.pop_back();
    }
}

}  // namespace NCloud::NBlockStore::NStorage
