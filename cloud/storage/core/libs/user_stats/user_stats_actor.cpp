#include "user_stats_actor.h"

#include "mon_page_wrapper.h"

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/library/actors/core/actor.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/text/text.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/fwd.h>

namespace NCloud::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

TUserStatsActor::TUserStatsActor(
        int component,
        TString path,
        TString title,
        TVector<IUserMetricsSupplierPtr> providers)
    : Providers(std::move(providers))
    , Component(component)
    , Path(std::move(path))
    , Title(std::move(title))
{}

void TUserStatsActor::Bootstrap(const NActors::TActorContext& ctx)
{
    Become(&TThis::StateWork);
    RegisterPages(ctx);
}

void TUserStatsActor::RegisterPages(const NActors::TActorContext& ctx)
{
    auto mon = NKikimr::AppData(ctx)->Mon;
    if (mon) {
        auto* rootPage = mon->RegisterIndexPage(Path, Title);

        mon->RegisterActorPage(rootPage, "user_stats/human", "UserStats",
            true, ctx.ActorSystem(), SelfId());

        mon->Register(new TMonPageWrapper(
            Path + "/user_stats/json",
            [this] (IOutputStream& out) {
                return OutputJsonPage(out);
            }));
        mon->Register(new TMonPageWrapper(
            Path + "/user_stats/spack",
            [this] (IOutputStream& out) {
                return OutputSpackPage(out);
            }));
        mon->Register(new TMonPageWrapper(
            Path + "/user_stats/prometheus",
            [this] (IOutputStream& out) {
                return OutputPrometheusPage(out);
            }));
    }
}

void TUserStatsActor::RenderHtmlInfo(IOutputStream& out) const
{
    auto encoder = NMonitoring::EncoderText(&out);

    encoder->OnStreamBegin();
    {
        TReadGuard g{Lock};

        for (auto&& provider : Providers) {
            provider->Append(TInstant::Zero(), encoder.Get());
        }
    }
    encoder->OnStreamEnd();
}

void TUserStatsActor::OutputJsonPage(IOutputStream& out) const
{
    out << NMonitoring::HTTPOKJSON;
    auto encoder = NMonitoring::EncoderJson(&out);

    encoder->OnStreamBegin();
    {
        TReadGuard g{Lock};

        for (auto&& provider : Providers) {
            provider->Append(TInstant::Zero(), encoder.Get());
        }
    }
    encoder->OnStreamEnd();
}

void TUserStatsActor::OutputSpackPage(IOutputStream& out) const
{
    out << NMonitoring::HTTPOKSPACK;

    auto encoder = NMonitoring::EncoderSpackV1(
        &out,
        NMonitoring::ETimePrecision::SECONDS,
        NMonitoring::ECompression::IDENTITY);

    encoder->OnStreamBegin();
    {
        TReadGuard g{Lock};

        for (auto&& provider : Providers) {
            provider->Append(TInstant::Now(), encoder.Get());
        }
    }
    encoder->OnStreamEnd();
}

void TUserStatsActor::OutputPrometheusPage(IOutputStream& out) const
{
    out << NMonitoring::HTTPOKPROMETHEUS;

    auto encoder = NMonitoring::EncoderPrometheus(&out, "name");

    encoder->OnStreamBegin();
    {
        TReadGuard g{Lock};

        for (auto&& provider : Providers) {
            provider->Append(TInstant::Zero(), encoder.Get());
        }
    }
    encoder->OnStreamEnd();
}

////////////////////////////////////////////////////////////////////////////////

void TUserStatsActor::HandleHttpInfo(
    const NActors::NMon::TEvHttpInfo::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    TStringStream out;
    RenderHtmlInfo(out);

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<NActors::NMon::TEvHttpInfoRes>(out.Str()));
}

void TUserStatsActor::HandleUserStatsProviderCreate(
    const TEvUserStats::TEvUserStatsProviderCreate::TPtr& ev,
    const NActors::TActorContext&)
{
    TEvUserStats::TUserStatsProviderCreate* msg = ev->Get();

    if (msg->Provider) {
        TWriteGuard g{Lock};

        Providers.push_back(msg->Provider);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUserStatsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvUserStats::TEvUserStatsProviderCreate, HandleUserStatsProviderCreate);

        default:
            HandleUnexpectedEvent(ev, Component, __PRETTY_FUNCTION__);
    }
}

}   // NCloud::NStorage::NUserStats
