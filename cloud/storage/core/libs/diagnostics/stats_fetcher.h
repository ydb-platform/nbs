#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/protos/diagnostics.pb.h>

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

    virtual TResultOrError<TDuration> GetCpuWait() = 0;
};

using IStatsFetcherPtr = std::shared_ptr<IStatsFetcher>;

////////////////////////////////////////////////////////////////////////////////

IStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    TString statsFile);

IStatsFetcherPtr CreateTaskStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    int pid);

IStatsFetcherPtr CreateStatsFetcherStub();

TString BuildCpuWaitStatsFilename(const TString& serviceName);

IStatsFetcherPtr BuildStatsFetcher(
    NProto::EStatsFetcherType statsFetcherType,
    TString cpuWaitFilename,
    const TLog& log,
    ILoggingServicePtr logging);

}   // namespace NCloud::NStorage
