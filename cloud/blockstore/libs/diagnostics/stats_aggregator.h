#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>

#include <functional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IStatsAggregator: public IStartable
{
    using TCommitCallback = std::function<void(
        NMonitoring::TDynamicCountersPtr,
        NMonitoring::TDynamicCountersPtr)>;

    virtual ~IStatsAggregator() = default;

    virtual void AddStats(
        const TString& id,
        const TString& stats,
        TInstant now = TInstant::Now()) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IClientPercentileCalculator: public IStartable
{
    virtual void CalculatePercentiles(
        NMonitoring::TDynamicCountersPtr updatedCounters) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IStatsAggregatorPtr CreateStatsAggregator(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IStatsAggregator::TCommitCallback commitCallback);

IStatsAggregatorPtr CreateStatsAggregatorStub();

IClientPercentileCalculatorPtr CreateClientPercentileCalculator(
    ILoggingServicePtr logging);

void UpdateClientStats(
    NMonitoring::TDynamicCountersPtr updatedCounters,
    NMonitoring::TDynamicCountersPtr baseCounters);

IMetricConsumerPtr CreateStatsParser(NMonitoring::TDynamicCountersPtr counters);

}   // namespace NCloud::NBlockStore
