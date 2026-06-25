#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/core/helpers.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGetFileSystemResizeState(
    [[maybe_unused]] const TEvIndexTablet::TEvGetFileSystemResizeStateRequest::
        TPtr& ev,
    [[maybe_unused]] const TActorContext& ctx)
{
    // return
}

void TIndexTabletActor::HandleSetFileSystemResizeState(
    [[maybe_unused]] const TEvIndexTablet::TEvSetFileSystemResizeStateRequest::
        TPtr& ev,
    [[maybe_unused]] const TActorContext& ctx)
{
    // return
}

}   // namespace NCloud::NFileStore::NStorage
