#include "hive_proxy_actor.h"

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleLockTablet(
    const TEvHiveProxy::TEvLockTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 tabletId = msg->TabletId;
    ui64 hive = GetHive(ctx, tabletId);

    auto& tablets = HiveStates[hive].LockStates;
    auto& state = tablets[tabletId];
    if (state.Owner) {
        ReportHiveProxyConcurrentLockError();
        auto error = MakeError(E_REJECTED, "Concurrent lock requests detected");
        auto response =
            std::make_unique<TEvHiveProxy::TEvLockTabletResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    state.Owner = ev->Sender;
    state.Cookie = ev->Cookie;
    state.TabletId = tabletId;
    state.Phase = PHASE_LOCKING;
    state.LockRequest = {ev->Sender, ev->Cookie};

    SendLockRequest(ctx, hive, tabletId);
}

void THiveProxyActor::HandleLockTabletExecutionResult(
    const TEvHive::TEvLockTabletExecutionResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 tabletId = msg->Record.GetTabletID();
    ui64 hive = GetHive(ctx, tabletId);

    auto* states = HiveStates.FindPtr(hive);
    auto* state = states ? states->LockStates.FindPtr(tabletId) : nullptr;
    if (!state || state->Phase == PHASE_UNLOCKING) {
        // Unexpected lock reply, send an unlock request
        LOG_WARN_S(
            ctx,
            LogComponent,
            "Unexpected lock reply from hive " << hive << " for tablet "
                                               << tabletId);
        SendUnlockRequest(ctx, hive, tabletId);
        return;
    }

    if (state->Phase == PHASE_LOCKED) {
        // Already locked, ignore duplicate results
        LOG_WARN_S(
            ctx,
            LogComponent,
            "Ignored duplicate lock reply from hive " << hive << " for tablet "
                                                      << tabletId);
        return;
    }

    STORAGE_VERIFY(
        state->Phase == PHASE_LOCKING || state->Phase == PHASE_RECONNECT,
        TWellKnownEntityTypes::TABLET,
        tabletId);

    if (msg->Record.GetStatus() != NKikimrProto::OK) {
        auto error = MakeKikimrError(
            msg->Record.GetStatus(),
            msg->Record.GetStatusMessage());

        if (state->Phase == PHASE_LOCKING) {
            STORAGE_VERIFY(
                state->LockRequest,
                TWellKnownEntityTypes::TABLET,
                tabletId);
            STORAGE_VERIFY(
                !state->UnlockRequest,
                TWellKnownEntityTypes::TABLET,
                tabletId);
            SendLockReply(ctx, state, error);
        } else {
            STORAGE_VERIFY(
                !state->LockRequest,
                TWellKnownEntityTypes::TABLET,
                tabletId);
            SendLockLostNotification(ctx, state, error);
            if (state->UnlockRequest) {
                // We know that lock is already lost
                SendUnlockReply(ctx, state, error);
            }
        }

        states->LockStates.erase(tabletId);
        return;
    }

    if (state->Phase == PHASE_LOCKING) {
        STORAGE_VERIFY(
            state->LockRequest,
            TWellKnownEntityTypes::TABLET,
            tabletId);
        STORAGE_VERIFY(
            !state->UnlockRequest,
            TWellKnownEntityTypes::TABLET,
            tabletId);
        SendLockReply(ctx, state);
    } else {
        STORAGE_VERIFY(
            !state->LockRequest,
            TWellKnownEntityTypes::TABLET,
            tabletId);
    }
    state->Phase = PHASE_LOCKED;

    if (state->UnlockRequest) {
        state->Phase = PHASE_UNLOCKING;
        SendUnlockRequest(ctx, hive, tabletId);
    }
}

}   // namespace NCloud::NStorage
