#include "service_actor.h"

#include "tablet_action_actor.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/public.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////
// Stats

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

////////////////////////////////////////////////////////////////////////////////
// UnsafeNodeOps

IActorPtr TStorageServiceActor::CreateUnsafeDeleteNodeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TUnsafeDeleteNodeActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeDeleteNodeRequest,
        TEvIndexTablet::TEvUnsafeDeleteNodeResponse>;
    return std::make_unique<TUnsafeDeleteNodeActor>(
        std::move(requestInfo),
        std::move(input));
}

IActorPtr TStorageServiceActor::CreateUnsafeUpdateNodeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TUnsafeUpdateNodeActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeUpdateNodeRequest,
        TEvIndexTablet::TEvUnsafeUpdateNodeResponse>;
    return std::make_unique<TUnsafeUpdateNodeActor>(
        std::move(requestInfo),
        std::move(input));
}

IActorPtr TStorageServiceActor::CreateUnsafeGetNodeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TUnsafeGetNodeActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeGetNodeRequest,
        TEvIndexTablet::TEvUnsafeGetNodeResponse>;
    return std::make_unique<TUnsafeGetNodeActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
