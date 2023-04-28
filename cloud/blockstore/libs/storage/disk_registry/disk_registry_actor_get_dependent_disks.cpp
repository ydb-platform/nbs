#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleGetDependentDisks(
    const TEvDiskRegistryPrivate::TEvGetDependentDisksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(GetDependentDisks);

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received GetDependentDisks request: Host=%s, Path=%s",
        TabletID(),
        msg->Host.c_str(),
        msg->Path.c_str());

    BLOCKSTORE_TRACE_RECEIVED(ctx, &requestInfo->TraceId, this, msg);

    using TResponse = TEvDiskRegistryPrivate::TEvGetDependentDisksResponse;

    TVector<TString> diskIds;
    auto error = State->GetDependentDisks(
        msg->Host,
        msg->Path,
        &diskIds);
    auto response = std::make_unique<TResponse>(std::move(error));
    response->DependentDiskIds = std::move(diskIds);

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
