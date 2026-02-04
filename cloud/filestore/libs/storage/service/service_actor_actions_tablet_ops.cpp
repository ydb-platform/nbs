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
// UnsafeNodeOps / UnsafeNodeRefOps

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

IActorPtr TStorageServiceActor::CreateUnsafeCreateNodeRefActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TUnsafeCreateNodeRefActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeCreateNodeRefRequest,
        TEvIndexTablet::TEvUnsafeCreateNodeRefResponse>;
    return std::make_unique<TUnsafeCreateNodeRefActor>(
        std::move(requestInfo),
        std::move(input));
}

IActorPtr TStorageServiceActor::CreateUnsafeDeleteNodeRefActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TUnsafeDeleteNodeRefActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeDeleteNodeRefRequest,
        TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse>;
    return std::make_unique<TUnsafeDeleteNodeRefActor>(
        std::move(requestInfo),
        std::move(input));
}

IActorPtr TStorageServiceActor::CreateUnsafeUpdateNodeRefActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TUnsafeUpdateNodeRefActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeUpdateNodeRefRequest,
        TEvIndexTablet::TEvUnsafeUpdateNodeRefResponse>;
    return std::make_unique<TUnsafeUpdateNodeRefActor>(
        std::move(requestInfo),
        std::move(input));
}

IActorPtr TStorageServiceActor::CreateUnsafeGetNodeRefActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TUnsafeGetNodeRefActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeGetNodeRefRequest,
        TEvIndexTablet::TEvUnsafeGetNodeRefResponse>;
    return std::make_unique<TUnsafeGetNodeRefActor>(
        std::move(requestInfo),
        std::move(input));
}

NActors::IActorPtr TStorageServiceActor::CreateUnsafeCreateHandleActor(
        TRequestInfoPtr requestInfo,
        TString input)
{
    using TUnsafeCreateHandleActor = TTabletActionActor<
        TEvIndexTablet::TEvUnsafeCreateHandleRequest,
        TEvIndexTablet::TEvUnsafeCreateHandleResponse>;
    return std::make_unique<TUnsafeCreateHandleActor>(
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

////////////////////////////////////////////////////////////////////////////////
// ReadNodeRefs

IActorPtr TStorageServiceActor::CreateReadNodeRefsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TReadNodeRefsActionActor = TTabletActionActor<
        TEvIndexTablet::TEvReadNodeRefsRequest,
        TEvIndexTablet::TEvReadNodeRefsResponse>;
    return std::make_unique<TReadNodeRefsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

////////////////////////////////////////////////////////////////////////////////
// SetHasXAttrs

IActorPtr TStorageServiceActor::CreateSetHasXAttrsActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TSetHasXAttrsActionActor = TTabletActionActor<
        TEvIndexTablet::TEvSetHasXAttrsRequest,
        TEvIndexTablet::TEvSetHasXAttrsResponse>;

    return std::make_unique<TSetHasXAttrsActionActor>(
        std::move(requestInfo),
        std::move(input));
}

////////////////////////////////////////////////////////////////////////////////
// MarkNodeRefsExhaustive

IActorPtr TStorageServiceActor::CreateMarkNodeRefsExhaustiveActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    using TMarkNodeRefsExhaustiveActionActor = TTabletActionActor<
        TEvIndexTablet::TEvMarkNodeRefsExhaustiveRequest,
        TEvIndexTablet::TEvMarkNodeRefsExhaustiveResponse>;

    return std::make_unique<TMarkNodeRefsExhaustiveActionActor>(
        std::move(requestInfo),
        std::move(input));
}

}   // namespace NCloud::NFileStore::NStorage
