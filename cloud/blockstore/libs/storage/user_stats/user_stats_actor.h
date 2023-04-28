#pragma once

#include <cloud/blockstore/libs/storage/api/user_stats.h>

#include <ydb/core/mon/mon.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NCloud::NBlockStore::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

class TUserStatsActor final
    : public NActors::TActorBootstrapped<TUserStatsActor>
{
private:
    TRWMutex Lock;
    TVector<IUserMetricsSupplierPtr> Providers;

public:
    TUserStatsActor(TVector<IUserMetricsSupplierPtr> providers);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterPages(const NActors::TActorContext& ctx);

    void RenderHtmlInfo(IOutputStream& out) const;

    void OutputJsonPage(IOutputStream& out) const;
    void OutputSpackPage(IOutputStream& out) const;

private:
    STFUNC(StateWork);

    void HandleHttpInfo(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUserStatsProviderCreate(
        const TEvUserStats::TEvUserStatsProviderCreate::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // NCloud::NBlockStore::NStorage::NUserStats
