#include "tablet_actor.h"

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

    auto* ass = ctx.ActorSystem();
    const auto selfId = SelfId();
    FastShard->Init().Subscribe(
        [ass, selfId](const auto& future)
        {
            ass->Send(
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
        LOG_ERROR_S(ctx, TFileStoreComponents::TABLET,
            LogTag << " FastShard init failed, restarting: "
            << FormatError(error));
        Suicide(ctx);
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
