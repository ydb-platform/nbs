#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/model/monpage_helpers.h>

#include <util/stream/str.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NActors::NMon;

////////////////////////////////////////////////////////////////////////////////

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
