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

ICgroupStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    TString statsFile);

ICgroupStatsFetcherPtr CreateCgroupStatsFetcherStub();

TString BuildCpuWaitStatsFilename(const TString& serviceName);

ICgroupStatsFetcherPtr BuildCgroupStatsFetcher(
    const TString& cpuWaitServiceName,
    const TString& cpuWaitFilename,
    const TLog& log,
    ILoggingServicePtr logging,
    TString componentName);

}   // namespace NCloud::NStorage
