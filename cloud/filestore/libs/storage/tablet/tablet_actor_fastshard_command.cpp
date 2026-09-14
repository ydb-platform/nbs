#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <util/stream/str.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleFastShardCommand(
    const TEvIndexTablet::TEvFastShardCommandRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvIndexTablet::TEvFastShardCommandResponse;

    const auto& record = ev->Get()->Record;

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s FastShardCommand: %s",
        LogTag.c_str(),
        record.ShortUtf8DebugString().Quote().c_str());

    //
    // The FastShard check protects against a race with state loading: the
    // shard instance appears only after CompleteAdapterLoadState.
    //

    if (!GetFileSystem().GetIsFastShard() || !FastShard) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TResponse>(
                MakeError(E_INVALID_STATE, "tablet is not a fast shard")));
        return;
    }

    switch (record.GetCommandCase()) {
        case NProtoPrivate::TFastShardCommandRequest::kDumpLayoutJson: {
            //
            // The layout dump is synchronous and does no IO.
            //

            auto response = std::make_unique<TResponse>();
            TStringStream out;
            FastShard->DumpLayoutJson(out);
            response->Record.SetLayoutJson(std::move(out.Str()));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        case NProtoPrivate::TFastShardCommandRequest::kCollectStats: {
            //
            // CollectStats scans the shard's persistent structures and may
            // do page IO, so it completes asynchronously - the reply is
            // sent from the future callback via the actor system.
            //

            auto stats = std::make_shared<NFastShard::TFileSystemShardStats>();
            auto* ass = ctx.ActorSystem();
            const auto sender = ev->Sender;
            const ui64 cookie = ev->Cookie;

            FastShard->CollectStats(stats.get()).Subscribe(
                [ass, sender, cookie, stats] (const auto& f) {
                    auto response = std::make_unique<TResponse>(f.GetValue());
                    if (!HasError(response->Record.GetError())) {
                        auto* s = response->Record.MutableStats();
                        s->SetUsedNodeCount(stats->UsedNodeCount);
                        s->SetTotalNodeCount(stats->TotalNodeCount);
                        s->SetUsedNameCount(stats->UsedNameCount);
                        s->SetTotalNameCount(stats->TotalNameCount);
                        s->SetUsedHandleCount(stats->UsedHandleCount);
                        s->SetTotalHandleCount(stats->TotalHandleCount);
                        s->SetUsedPageCount(stats->UsedPageCount);
                        s->SetTotalPageCount(stats->TotalPageCount);
                    }
                    ass->Send(
                        sender,
                        response.release(),
                        0 /* flags */,
                        cookie);
                });
            return;
        }

        case NProtoPrivate::TFastShardCommandRequest::COMMAND_NOT_SET: {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TResponse>(
                    MakeError(E_ARGUMENT, "command is not set")));
            return;
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
