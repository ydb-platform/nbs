#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

void TIndexTabletActor::HandleForcedTabletOperation(
    const TEvIndexTabletPrivate::TEvForcedTabletOperationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedTabletOperation request",
        LogTag.c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    // will lose original request info in case of enqueueing external request
    if (IsForcedOperationRunning()) {
        EnqueueForcedTabletOperation(msg->Mode);
        return;
    }

    StartForcedTabletOperation(
        msg->Mode,
        std::move(msg->OperationId));

    std::unique_ptr<IActor> actor;

    switch (msg->Mode) {
        case TEvIndexTabletPrivate::EForcedTabletOperationMode::Flush:
            break;
        case TEvIndexTabletPrivate::EForcedTabletOperationMode::FlushBytes:
            break;
        case TEvIndexTabletPrivate::EForcedTabletOperationMode::CollectGarbage:
            break;
        default:
            TABLET_VERIFY_C(false, "unexpected forced tablet operation mode");
    }

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleForcedTabletOperationCompleted(
    const TEvIndexTabletPrivate::TEvForcedTabletOperationCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ForcedTabletOperation completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    TABLET_VERIFY(IsForcedOperationRunning());
    WorkerActors.erase(ev->Sender);

    CompleteForcedTabletOperation();
    EnqueueForcedOperationIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
