#include "hive_proxy_actor.h"

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleUnlockTablet(
    const TEvHiveProxy::TEvUnlockTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 tabletId = msg->TabletId;
    ui64 hive = GetHive(ctx, tabletId);

    auto* states = HiveStates.FindPtr(hive);
    auto* state = states ? states->LockStates.FindPtr(tabletId) : nullptr;
    if (!state) {
        // Unlock with a paired lock
        auto error = MakeError(E_ARGUMENT, "Unlock without a matching Lock");
        auto response =
            std::make_unique<TEvHiveProxy::TEvUnlockTabletResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (state->Phase == PHASE_LOCKING) {
        // It is an error to unlock before the lock reply is received
        auto error =
            MakeError(E_ARGUMENT, "Cannot unlock while lock is in progress");
        auto response =
            std::make_unique<TEvHiveProxy::TEvUnlockTabletResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (state->Phase == PHASE_UNLOCKING) {
        // It is an error to make multiple unlock requests
        auto error =
            MakeError(E_ARGUMENT, "Concurrent unlock requests detected");
        auto response =
            std::make_unique<TEvHiveProxy::TEvUnlockTabletResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    state->UnlockRequest = {ev->Sender, ev->Cookie};

    if (state->Phase == PHASE_LOCKED) {
        // It is possible to send unlock request right now
        state->Phase = PHASE_UNLOCKING;
        SendUnlockRequest(ctx, hive, tabletId);
    }
}

void THiveProxyActor::HandleUnlockTabletExecutionResult(
    const TEvHive::TEvUnlockTabletExecutionResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 tabletId = msg->Record.GetTabletID();
    ui64 hive = GetHive(ctx, tabletId);

    auto* states = HiveStates.FindPtr(hive);
    auto* state = states ? states->LockStates.FindPtr(tabletId) : nullptr;
    if (!state || state->Phase != PHASE_UNLOCKING) {
        // Unexpected unlock reply, ignore
        LOG_WARN_S(
            ctx,
            LogComponent,
            "Unexpected unlock reply from hive " << hive << " for tablet "
                                                 << tabletId);
        return;
    }

    STORAGE_VERIFY(
        state->UnlockRequest,
        TWellKnownEntityTypes::TABLET,
        tabletId);

    NProto::TError error;
    if (msg->Record.GetStatus() != NKikimrProto::OK) {
        error = MakeKikimrError(
            msg->Record.GetStatus(),
            msg->Record.GetStatusMessage());
    }

    SendUnlockReply(ctx, state, error);
    states->LockStates.erase(tabletId);
}

}   // namespace NCloud::NStorage
