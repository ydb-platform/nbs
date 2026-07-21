#pragma once

#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>

#include <map>
namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

enum class ERequestType
{
    ReadData,
    WriteData,
};

////////////////////////////////////////////////////////////////////////////////

class TRequestStats
{
private:
    struct TRequestCounters
    {
        TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxTimeCalculator;
        NMonitoring::TDynamicCounters::TCounterPtr MaxTime;

        TRequestCounters(
            ERequestType requestType,
            NMonitoring::TDynamicCounterPtr counters,
            ITimerPtr timer);
    };

    std::map<ERequestType, TRequestCounters> Counters;

public:
    TRequestStats(
        NMonitoring::TDynamicCounterPtr counters,
        ITimerPtr timer = CreateWallClockTimer());

    void RequestCompleted(ERequestType requestType, TDuration duration);
    void UpdateStats(bool updateCounters);
};

}   // namespace NCloud::NBlockStore::NTesting
