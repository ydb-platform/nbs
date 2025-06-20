#include "service_actor.h"

#include "tablet_action_actor.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/public.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////
// Background ops

IActorPtr TStorageServiceActor::CreateForcedOperationActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TForcedOperationActor = TTabletActionActor<
        TEvIndexTablet::TEvForcedOperationRequest,
        TEvIndexTablet::TEvForcedOperationResponse>;
    return std::make_unique<TForcedOperationActor>(
        std::move(requestInfo),
        std::move(input));
}

IActorPtr TStorageServiceActor::CreateForcedOperationStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TForcedOperationStatusActor = TTabletActionActor<
        TEvIndexTablet::TEvForcedOperationStatusRequest,
        TEvIndexTablet::TEvForcedOperationStatusResponse>;
    return std::make_unique<TForcedOperationStatusActor>(
        std::move(requestInfo),
        std::move(input));
}

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

////////////////////////////////////////////////////////////////////////////////
// RestartTablet

IActorPtr TStorageServiceActor::CreateRestartTabletActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TRestartTabletActor = TTabletActionActor<
        TEvIndexTablet::TEvRestartTabletRequest,
        TEvIndexTablet::TEvRestartTabletResponse>;
    return std::make_unique<TRestartTabletActor>(
        std::move(requestInfo),
        std::move(input));
}

////////////////////////////////////////////////////////////////////////////////
// GetFileSystemTopology

IActorPtr TStorageServiceActor::CreateGetFileSystemTopologyActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TGetFileSystemTopologyActor = TTabletActionActor<
        TEvIndexTablet::TEvGetFileSystemTopologyRequest,
        TEvIndexTablet::TEvGetFileSystemTopologyResponse>;
    return std::make_unique<TGetFileSystemTopologyActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage

////////////////////////////////////////////////////////////////////////////////
// ListNodeRefs

IActorPtr TStorageServiceActor::CreateListNodeRefsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TListNodeRefsActionActor = TTabletActionActor<
        TEvIndexTablet::TEvListNodeRefsRequest,
        TEvIndexTablet::TEvListNodeRefsResponse>;
    return std::make_unique<TListNodeRefsActionActor>(
        std::move(requestInfo),
        std::move(input));
}
