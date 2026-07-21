#include "request_stats.h"

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

const char* GetRequestTypeName(ERequestType requestType)
{
    return requestType == ERequestType::ReadData
        ? "ReadData"
        : "WriteData";
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TRequestStats::TRequestCounters::TRequestCounters(
        ERequestType requestType,
        NMonitoring::TDynamicCounterPtr counters,
        ITimerPtr timer)
    : MaxTimeCalculator(std::move(timer))
    , MaxTime(counters
        ->GetSubgroup("request", GetRequestTypeName(requestType))
        ->GetCounter("MaxTime"))
{}

TRequestStats::TRequestStats(
    NMonitoring::TDynamicCounterPtr counters,
    ITimerPtr timer)
{
    Counters.insert({
        ERequestType::ReadData,
        TRequestCounters(ERequestType::ReadData, counters, timer)});
    Counters.insert({
        ERequestType::WriteData,
        TRequestCounters(ERequestType::WriteData, counters, timer)});
}

void TRequestStats::RequestCompleted(
    ERequestType requestType,
    TDuration duration)
{
    Counters.at(requestType).MaxTimeCalculator.Add(duration.MicroSeconds());
}

void TRequestStats::UpdateStats(bool updateCounters)
{
    for (auto& [_, counters]: Counters) {
        const ui64 maxTime = counters.MaxTimeCalculator.NextValue();
        if (updateCounters) {
            *counters.MaxTime = maxTime;
        }
    }
}

}   // namespace NCloud::NBlockStore::NTesting
