#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleGetDependentDisks(
    const TEvDiskRegistry::TEvGetDependentDisksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(GetDependentDisks);

    auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received GetDependentDisks request: Host=%s, Path=%s",
        TabletID(),
        msg->Record.GetHost().c_str(),
        msg->Record.GetPath().c_str());

    using TResponse = TEvDiskRegistry::TEvGetDependentDisksResponse;

    TVector<TString> diskIds;
    auto error = State->GetDependentDisks(
        msg->Record.GetHost(),
        msg->Record.GetPath(),
        msg->Record.GetIgnoreReplicatedDisks(),
        &diskIds);
    auto response = std::make_unique<TResponse>();
    *response->Record.MutableError() = std::move(error);
    for (const auto& id: diskIds) {
        response->Record.AddDependentDiskIds(std::move(id));
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
