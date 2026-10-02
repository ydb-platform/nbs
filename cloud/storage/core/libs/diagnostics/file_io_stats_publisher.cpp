#include "file_io_stats_publisher.h"

#include "max_calculator.h"
#include "stats_handler.h"

#include <cloud/storage/core/libs/common/file_io_stats.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/vector.h>

namespace NCloud {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequestCounters
{
    TDynamicCounters::TCounterPtr Count;
    TDynamicCounters::TCounterPtr Errors;
    TDynamicCounters::TCounterPtr Time;
    TDynamicCounters::TCounterPtr MaxTime;
    TDynamicCounters::TCounterPtr RequestBytes;
    TDynamicCounters::TCounterPtr MaxRequestBytes;
    TDynamicCounters::TCounterPtr InProgress;

    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxTimeCalc;
    TMaxPerSecondCalculator<DEFAULT_BUCKET_COUNT> MaxRequestBytesCalc;

    ui64 PrevRequestBytes = 0;

    TRequestCounters(
            const ITimerPtr& timer,
            TDynamicCounters& serviceGroup,
            const TFileIOStats& stats,
            EFileIORequest request)
        : TRequestCounters(
              timer,
              *serviceGroup.GetSubgroup(
                  "request",
                  TString(GetFileIORequestName(request))),
              stats.GetStats(request).RequestBytes)
    {}

    void Update(TFileIOStats& stats, EFileIORequest request)
    {
        const auto snapshot = stats.GetStats(request);

        // cumulative values are published as absolute snapshots
        *Count = snapshot.Count;
        *Errors = snapshot.Errors;
        *Time = snapshot.Time;
        *RequestBytes = snapshot.RequestBytes;
        *InProgress = snapshot.InProgress;

        MaxTimeCalc.Add(stats.ResetMaxTime(request));
        *MaxTime = MaxTimeCalc.NextValue();

        MaxRequestBytesCalc.Add(snapshot.RequestBytes - PrevRequestBytes);
        PrevRequestBytes = snapshot.RequestBytes;
        *MaxRequestBytes = MaxRequestBytesCalc.NextValue();
    }

private:
    TRequestCounters(
            const ITimerPtr& timer,
            TDynamicCounters& group,
            ui64 requestBytes)
        : Count(group.GetCounter("Count", true))
        , Errors(group.GetCounter("Errors", true))
        , Time(group.GetCounter("Time", true))
        , MaxTime(group.GetCounter("MaxTime"))
        , RequestBytes(group.GetCounter("RequestBytes", true))
        , MaxRequestBytes(group.GetCounter("MaxRequestBytes"))
        , InProgress(group.GetCounter("InProgress"))
        , MaxTimeCalc(timer)
        // the rate baseline starts together with the collection
        , MaxRequestBytesCalc(timer)
        , PrevRequestBytes(requestBytes)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TServiceCounters
{
    TFileIOStatsPtr Stats;
    TRequestCounters Read;
    TRequestCounters Write;

    TServiceCounters(
            const ITimerPtr& timer,
            TDynamicCounters& group,
            TFileIOStatsPtr stats)
        : Stats(std::move(stats))
        , Read(timer, group, *Stats, EFileIORequest::Read)
        , Write(timer, group, *Stats, EFileIORequest::Write)
    {}

    void Update()
    {
        Read.Update(*Stats, EFileIORequest::Read);
        Write.Update(*Stats, EFileIORequest::Write);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileIOStatsPublisher final
    : public IStatsHandler
{
private:
    const ITimerPtr Timer;
    const TFileIOStatsRegistryPtr Registry;
    const TDynamicCountersPtr Counters;

    size_t RegisteredCount = 0;
    TVector<TServiceCounters> Services;

public:
    TFileIOStatsPublisher(
            ITimerPtr timer,
            TFileIOStatsRegistryPtr registry,
            TDynamicCountersPtr counters)
        : Timer(std::move(timer))
        , Registry(std::move(registry))
        , Counters(counters->GetSubgroup("component", "file_io"))
    {}

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);

        for (auto& entry: Registry->GetEntries(RegisteredCount)) {
            ++RegisteredCount;

            auto group = Counters->GetSubgroup("backend", entry.Backend)
                             ->GetSubgroup("io_service", entry.ServiceId);

            Services.emplace_back(Timer, *group, std::move(entry.Stats));
        }

        for (auto& service: Services) {
            service.Update();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsHandlerPtr CreateFileIOStatsPublisher(
    ITimerPtr timer,
    TFileIOStatsRegistryPtr registry,
    TDynamicCountersPtr counters)
{
    return std::make_shared<TFileIOStatsPublisher>(
        std::move(timer),
        std::move(registry),
        std::move(counters));
}

}   // namespace NCloud
