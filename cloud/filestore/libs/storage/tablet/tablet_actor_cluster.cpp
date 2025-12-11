#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAddClusterNode(
    const TEvService::TEvAddClusterNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    auto response = std::make_unique<TEvService::TEvAddClusterNodeResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleRemoveClusterNode(
    const TEvService::TEvRemoveClusterNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    auto response =
        std::make_unique<TEvService::TEvRemoveClusterNodeResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleListClusterNodes(
    const TEvService::TEvListClusterNodesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    auto response = std::make_unique<TEvService::TEvListClusterNodesResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleAddClusterClients(
    const TEvService::TEvAddClusterClientsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    auto response =
        std::make_unique<TEvService::TEvAddClusterClientsResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleRemoveClusterClients(
    const TEvService::TEvRemoveClusterClientsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    auto response =
        std::make_unique<TEvService::TEvRemoveClusterClientsResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleListClusterClients(
    const TEvService::TEvListClusterClientsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    auto response =
        std::make_unique<TEvService::TEvListClusterClientsResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleUpdateCluster(
    const TEvService::TEvUpdateClusterRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    auto response = std::make_unique<TEvService::TEvUpdateClusterResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
