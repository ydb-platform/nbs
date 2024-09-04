#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleFsync(
    const TEvService::TEvFsyncRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvFsyncResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TStorageServiceActor::HandleFsyncDir(
    const TEvService::TEvFsyncDirRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvFsyncDirResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
