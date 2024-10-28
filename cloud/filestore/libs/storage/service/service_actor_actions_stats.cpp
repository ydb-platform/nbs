#include "service_actor.h"

#include "tablet_action_actor.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/public.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr TStorageServiceActor::CreateGetStorageStatsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TGetStorageStatsActor = TTabletActionActor<
        TEvIndexTablet::TEvGetStorageStatsRequest,
        TEvIndexTablet::TEvGetStorageStatsResponse>;
    return std::make_unique<TGetStorageStatsActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
