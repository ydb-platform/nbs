#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::CreateFastShard(const TActorContext& ctx)
{
    FastShard = FastShardFactory->CreateShard(
        GetFileSystemId(),
        GetFileSystem().GetFastShardConfig(),
        GetFileSystem().GetShardNo(),
        Executor()->Generation());

    auto* actorSystem = ctx.ActorSystem();
    const auto selfId = SelfId();
    FastShard->Init().Subscribe(
        [actorSystem, selfId](const auto& future)
        {
            actorSystem->Send(
                selfId,
                new TEvIndexTabletPrivate::TEvFastShardInitCompleted(
                    future.GetValue()));
        });
}

void TIndexTabletActor::HandleFastShardInitCompleted(
    const TEvIndexTabletPrivate::TEvFastShardInitCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& error = ev->Get()->Error;
    if (HasError(error)) {
        //
        // A restart would retry the same init against the same storage and
        // most likely fail the same way, so the tablet stays up in the
        // broken state instead: it rejects requests with an error and can
        // be inspected. The storage group already retries retriable errors
        // inside Init.
        //

        ReportFastShardInitFailed(TStringBuilder()
            << LogTag << " FastShard init failed: " << FormatError(error));

        LOG_ERROR_S(ctx, TFileStoreComponents::TABLET,
            LogTag << " Switching tablet to BROKEN state due to the failed"
            << " FastShard init: " << FormatError(error));

        BecomeAux(ctx, STATE_BROKEN);

        // allow pipes to connect
        SignalTabletActive(ctx);

        // resend pending WaitReady requests
        while (WaitReadyRequests) {
            ctx.Send(WaitReadyRequests.front().release());
            WaitReadyRequests.pop_front();
        }

        return;
    }

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " FastShard initialized");

    BecomeAux(ctx, STATE_ADAPTER);

    ScheduleUpdateCounters(ctx);
    ScheduleSyncSessions(ctx);
    ScheduleCleanupSessions(ctx);
    RegisterFileStore(ctx);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Activating tablet");

    // allow pipes to connect
    SignalTabletActive(ctx);

    CompleteStateLoad();

    // resend pending WaitReady requests
    while (WaitReadyRequests) {
        ctx.Send(WaitReadyRequests.front().release());
        WaitReadyRequests.pop_front();
    }

    if (FastShardServer) {
        FastShardServer->RegisterShard(
            GetFileSystemId(),
            FastShard);
    }

    RunRegularTasks(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
