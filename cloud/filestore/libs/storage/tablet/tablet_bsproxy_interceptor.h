#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr {
    class TTabletStorageInfo;
}   // namespace NKikimr

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Installs logging pass-through interceptors over the BS proxy service ids
// for all groups used by the tablet. Idempotent per (node, group).
void EnsureBSProxyInterceptors(
    const NActors::TActorContext& ctx,
    const NKikimr::TTabletStorageInfo& info);

}   // namespace NCloud::NFileStore::NStorage
