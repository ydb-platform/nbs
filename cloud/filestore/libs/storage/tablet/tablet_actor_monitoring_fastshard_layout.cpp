#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/model/monpage_helpers.h>

#include <library/cpp/json/writer/json.h>

#include <util/stream/str.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NActors::NMon;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString StatsToJson(const NFastShard::TFileSystemShardStats& stats)
{
    TStringStream ss;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss);
    writer.BeginObject();
    writer.WriteKey("usedNodeCount");
    writer.WriteULongLong(stats.UsedNodeCount);
    writer.WriteKey("totalNodeCount");
    writer.WriteULongLong(stats.TotalNodeCount);
    writer.WriteKey("usedNameCount");
    writer.WriteULongLong(stats.UsedNameCount);
    writer.WriteKey("totalNameCount");
    writer.WriteULongLong(stats.TotalNameCount);
    writer.WriteKey("usedHandleCount");
    writer.WriteULongLong(stats.UsedHandleCount);
    writer.WriteKey("totalHandleCount");
    writer.WriteULongLong(stats.TotalHandleCount);
    writer.WriteKey("usedPageCount");
    writer.WriteULongLong(stats.UsedPageCount);
    writer.WriteKey("totalPageCount");
    writer.WriteULongLong(stats.TotalPageCount);
    writer.EndObject();
    return ss.Str();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleHttpInfo_FastShardStatsJson(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    //
    // The stats action exists only for fast shards (Adapter mode). The
    // FastShard check protects against a race with state loading: the
    // shard instance appears only after CompleteAdapterLoadState.
    //

    if (!GetFileSystem().GetIsFastShard() || !FastShard) {
        const TString message = "tablet is not a fast shard";
        LOG_ERROR_S(
            ctx,
            TFileStoreComponents::TABLET,
            LogTag << " " << message);

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvRemoteJsonInfoRes>(
                JsonError(MakeError(E_INVALID_STATE, message))));
        return;
    }

    //
    // CollectStats scans the shard's persistent structures and may do
    // page IO, so it completes asynchronously - the reply is sent from
    // the future callback via the actor system.
    //

    auto stats = std::make_shared<NFastShard::TFileSystemShardStats>();
    auto* ass = ctx.ActorSystem();
    const auto sender = requestInfo->Sender;
    const ui64 cookie = requestInfo->Cookie;

    FastShard->CollectStats(stats.get()).Subscribe(
        [ass, sender, cookie, stats] (const auto& f) {
            const auto& error = f.GetValue();
            TString json =
                HasError(error) ? JsonError(error) : StatsToJson(*stats);
            ass->Send(
                sender,
                new TEvRemoteJsonInfoRes(std::move(json)),
                0 /* flags */,
                cookie);
        });
}

void TIndexTabletActor::HandleHttpInfo_FastShardLayoutJson(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    //
    // The layout page exists only for fast shards (Adapter mode). The
    // FastShard check protects against a race with state loading: the
    // shard instance appears only after CompleteAdapterLoadState.
    //

    if (!GetFileSystem().GetIsFastShard() || !FastShard) {
        const TString message = "tablet is not a fast shard";
        LOG_ERROR_S(
            ctx,
            TFileStoreComponents::TABLET,
            LogTag << " " << message);

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvRemoteJsonInfoRes>(
                JsonError(MakeError(E_INVALID_STATE, message))));
        return;
    }

    //
    // DumpLayoutHtml is synchronous and does no IO - the layout is
    // fixed after the shard construction - so no worker actor is
    // needed here, unlike in the Directory Viewer.
    //

    TStringStream out;
    FastShard->DumpLayoutJson(out);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<TEvRemoteJsonInfoRes>(std::move(out.Str())));
}

void TIndexTabletActor::HandleHttpInfo_FastShardLayout(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    //
    // The layout page exists only for fast shards (Adapter mode). The
    // FastShard check protects against a race with state loading: the
    // shard instance appears only after CompleteAdapterLoadState.
    //

    if (!GetFileSystem().GetIsFastShard() || !FastShard) {
        const TString message = "tablet is not a fast shard";
        LOG_ERROR_S(
            ctx,
            TFileStoreComponents::TABLET,
            LogTag << " " << message);
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvRemoteHttpInfoRes>(message));
        return;
    }

    //
    // DumpLayoutHtml is synchronous and does no IO - the layout is
    // fixed after the shard construction - so no worker actor is
    // needed here, unlike in the Directory Viewer.
    //

    TStringStream out;
    FastShard->DumpLayoutHtml(out);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<TEvRemoteHttpInfoRes>(std::move(out.Str())));
}

}   // namespace NCloud::NFileStore::NStorage
