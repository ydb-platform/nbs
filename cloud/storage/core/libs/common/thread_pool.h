#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

ITaskQueuePtr CreateThreadPool(
    const TString& threadName,
    size_t numThreads,
    TString memoryTagScope);

ITaskQueuePtr CreateThreadPool(
    const TString& threadName,
    size_t numThreads);

ITaskQueuePtr CreateLongRunningTaskExecutor(const TString& threadName);

}   // namespace NCloud
