#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/events.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct ILimiter
{
    virtual ~ILimiter() = default;

    virtual bool Acquire(size_t requestBytes) = 0;
    virtual void Release(size_t requestBytes) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ILimiterPtr
CreateLimiter(const TLog& log, TContExecutor* e, size_t maxInFlightBytes);

ILimiterPtr CreateLimiterStub();

}   // namespace NCloud::NBlockStore::NBD
