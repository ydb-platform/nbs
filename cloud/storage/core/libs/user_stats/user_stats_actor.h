#pragma once

#include <cloud/storage/core/libs/api/user_stats.h>

#include <ydb/core/mon/mon.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NCloud::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

class TUserStatsActor final
    : public NActors::TActorBootstrapped<TUserStatsActor>
{
private:
    TRWMutex Lock;
    TVector<IUserMetricsSupplierPtr> Providers;

    const int Component;
    const TString Path;
    const TString Title;

public:
    TUserStatsActor(
        int component,
        TString path,
        TString title,
        TVector<IUserMetricsSupplierPtr> providers);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterPages(const NActors::TActorContext& ctx);

    void RenderHtmlInfo(IOutputStream& out) const;

    void OutputJsonPage(IOutputStream& out) const;
    void OutputSpackPage(IOutputStream& out) const;
    void OutputPrometheusPage(IOutputStream& out) const;

private:
    STFUNC(StateWork);

    void HandleHttpInfo(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUserStatsProviderCreate(
        const TEvUserStats::TEvUserStatsProviderCreate::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // NCloud::NStorage::NUserStats
