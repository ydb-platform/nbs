#pragma once

#include "public.h"

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

    virtual TDuration GetCpuWait() = 0;
};

using ICgroupStatsFetcherPtr = std::shared_ptr<ICgroupStatsFetcher>;

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcherSettings
{
    TString StatsFile;
    TString CountersGroupName;
    TString ComponentGroupName;
    TString CounterName;
};

ICgroupStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TCgroupStatsFetcherSettings settings);

ICgroupStatsFetcherPtr CreateCgroupStatsFetcherStub();

}   // namespace NCloud::NStorage
