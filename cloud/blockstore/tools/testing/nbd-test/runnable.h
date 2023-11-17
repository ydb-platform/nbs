#pragma once

#include "private.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IRunnable
{
    virtual ~IRunnable() = default;

    virtual int Run() = 0;
    virtual void Stop(int exitCode) = 0;
};

}   // namespace NCloud::NBlockStore
