#pragma once

#include "public.h"

#include "scheduler.h"

#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TTestScheduler final
    : public IScheduler
{
private:
    TMutex CallbacksLock;
    TVector<TCallback> Callbacks;

public:
    void Start() override {}
    void Stop() override {}

    void Schedule(
        ITaskQueue* taskQueue,
        TInstant deadline,
        TCallback callback) override;

    void RunAllScheduledTasks();
};

}   // namespace NCloud
