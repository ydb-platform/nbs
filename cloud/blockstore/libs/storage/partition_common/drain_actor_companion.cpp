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

////////////////////////////////////////////////////////////////////////////////

void TDrainActorCompanion::ProcessDrainRequests(
    const NActors::TActorContext& ctx)
{
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

}  // namespace NCloud::NBlockStore::NStorage
