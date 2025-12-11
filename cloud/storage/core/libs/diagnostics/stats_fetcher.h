#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/diagnostics.pb.h>

#include <util/datetime/base.h>

namespace NMonitoring {

struct TCounterForPtr;

};

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IStatsFetcher
{
    virtual ~IStatsFetcher() = default;

    virtual TResultOrError<TDuration> GetCpuWait() = 0;
};

using IStatsFetcherPtr = std::shared_ptr<IStatsFetcher>;

////////////////////////////////////////////////////////////////////////////////

IStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    TString statsFile);

IStatsFetcherPtr CreateTaskStatsFetcher(TString componentName, int pid);

IStatsFetcherPtr CreateStatsFetcherStub();

TString BuildCpuWaitStatsFilename(const TString& serviceName);

IStatsFetcherPtr BuildStatsFetcher(
    NProto::EStatsFetcherType statsFetcherType,
    const TString& cpuWaitFilename,
    const TLog& log);

}   // namespace NCloud::NStorage
