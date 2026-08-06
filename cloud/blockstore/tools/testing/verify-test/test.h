#pragma once

#include "private.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ITest
{
    virtual ~ITest() = default;

    virtual int Run() = 0;
    virtual void Stop(int exitCode) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITestPtr CreateTest(TOptionsPtr options, ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore
