#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>

namespace NMonitoring {

struct TCounterForPtr;

};

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct ICgroupStatsFetcher
    : public IStartable
{
    virtual ~ICgroupStatsFetcher() = default;

    virtual TResultOrError<TDuration> GetCpuWait() = 0;
};

using ICgroupStatsFetcherPtr = std::shared_ptr<ICgroupStatsFetcher>;

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcherMonitoringSettings
{
    TString CountersGroupName;
    TString ComponentGroupName;
    TString CounterName;
};

ICgroupStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString statsFile,
    TCgroupStatsFetcherMonitoringSettings settings);

ICgroupStatsFetcherPtr CreateCgroupStatsFetcherStub();

TString BuildCpuWaitStatsFilename(const TString& serviceName);

}   // namespace NCloud::NStorage
