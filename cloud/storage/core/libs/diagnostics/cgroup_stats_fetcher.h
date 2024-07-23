#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>

namespace NMonitoring {

struct TCounterForPtr;

};

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IStatsFetcher
    : public IStartable
{
    virtual ~IStatsFetcher() = default;

    virtual TDuration GetCpuWait() = 0;
};

using IStatsFetcherPtr = std::shared_ptr<IStatsFetcher>;

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcherMonitoringSettings
{
    TString CountersGroupName;
    TString ComponentGroupName;
    TString CounterName;
};

IStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString statsFile,
    TCgroupStatsFetcherMonitoringSettings settings);

IStatsFetcherPtr CreateKernelTaskDelayAcctStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring);

IStatsFetcherPtr CreateStatsFetcherStub();

TString BuildCpuWaitStatsFilename(const TString& serviceName);

}   // namespace NCloud::NStorage
