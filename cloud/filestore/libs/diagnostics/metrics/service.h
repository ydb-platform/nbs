#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

struct TMetricsServiceConfig
{
    TDuration UpdateInterval = TDuration::Max();
};

////////////////////////////////////////////////////////////////////////////////

struct IMetricsService: public IStartable
{
    virtual ~IMetricsService() = default;

    virtual void SetupCounters(NMonitoring::TDynamicCountersPtr root) = 0;

    virtual IMetricsRegistryPtr GetRegistry() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IMetricsServicePtr CreateMetricsService(
    const TMetricsServiceConfig& config,
    ITimerPtr timer,
    ISchedulerPtr scheduler);

}   // namespace NCloud::NFileStore::NMetrics
