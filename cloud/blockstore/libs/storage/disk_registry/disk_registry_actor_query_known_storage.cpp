#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleQueryKnownStorage(
    const TEvService::TEvQueryKnownStorageRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    auto response =
        std::make_unique<TEvService::TEvQueryKnownStorageResponse>();

    const THashSet<TString> agentIds{
        record.GetAgentIds().begin(),
        record.GetAgentIds().end()};

    for (const auto& agentId: agentIds) {
        auto [infos, error] = State->QueryKnownStorage(agentId);

        if (HasError(error)) {
            const bool notFound = error.GetCode() == E_NOT_FOUND;
            LOG_LOG(
                ctx,
                notFound ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_ERROR,
                TBlockStoreComponents::DISK_REGISTRY,
                "[%lu] Can't get known storage info for %s: %s",
                TabletID(),
                agentId.Quote().c_str(),
                FormatError(error).c_str());

            continue;
        }

        auto& dst = *response->Record.MutableKnownStorage()->Add();
        dst.SetAgentId(agentId);

        for (auto& info: infos) {
            auto& knownPathInfo = *dst.MutableKnownPathInfos()->Add();
            knownPathInfo.SetPath(info.Path);
            knownPathInfo.SetStoragePoolName(info.PoolName);
            knownPathInfo.SetStoragePoolKind(ToStoragePoolKind(info.PoolKind));
        }
    }
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
