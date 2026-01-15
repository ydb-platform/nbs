#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

// Used only for testing
void TIndexTabletActor::HandleGenerateCommitId(
    const TEvIndexTabletPrivate::TEvGenerateCommitIdRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s GenerateCommitId",
        LogTag.c_str());

    auto response = std::make_unique<TEvIndexTabletPrivate::TEvGenerateCommitIdResponse>();
    response->CommitId = GenerateCommitId();
    if (response->CommitId == InvalidCommitId) {
        return ScheduleRebootTabletOnCommitIdOverflow(ctx, "GenerateCommitId");
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
