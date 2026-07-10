#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr {
    class TTabletStorageInfo;
}   // namespace NKikimr

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TStorageConfig;

struct TBSProxyInterceptorConfig
{
    TBSProxyInterceptorConfig() = default;
    explicit TBSProxyInterceptorConfig(const TStorageConfig& config);

    bool RandomFailuresEnabled = false;
    double FailureProbability = 0.0;
    ui64 RandomFailureSeed = 0;
};

// Installs BS proxy service id interceptors for all groups used by the tablet
// when random failure injection is enabled. Idempotent per group on the local
// actor system.
void InstallBSProxyInterceptors(
    const NActors::TActorContext& ctx,
    const NKikimr::TTabletStorageInfo& info,
    const TBSProxyInterceptorConfig& config);

}   // namespace NCloud::NFileStore::NStorage
