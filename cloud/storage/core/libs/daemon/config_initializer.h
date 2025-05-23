#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerBase
{
    virtual ~TConfigInitializerBase() = default;

    virtual ui32 GetLogDefaultLevel() const = 0;
    virtual ui32 GetMonitoringPort() const = 0;
    virtual TString GetMonitoringAddress() const = 0;
    virtual ui32 GetMonitoringThreads() const = 0;
    virtual TString GetLogBackendFileName() const = 0;
};

}   // namespace NCloud
