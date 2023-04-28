#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ICgroupStatsFetcher
    : public IStartable
{
    virtual ~ICgroupStatsFetcher() = default;

    virtual TDuration GetCpuWait() = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICgroupStatsFetcherPtr CreateCgroupStatsFetcher(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString statsFile);

ICgroupStatsFetcherPtr CreateCgroupStatsFetcherStub();

}   // namespace NCloud::NBlockStore
