#pragma once

#include "public.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IStatsHandler
{
    virtual ~IStatsHandler() = default;

    virtual void UpdateStats(bool updateIntervalFinished) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IStatsHandlerPtr CreateStatsHandlerStub();

}   // namespace NCloud
