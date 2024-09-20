#pragma once

#include "public.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>
#include <util/system/event.h>
#include <util/system/spinlock.h>
#include <util/thread/pool.h>

#include <functional>

namespace NCloud::NFileStore::NReplay {

////////////////////////////////////////////////////////////////////////////////

class TExecutor
{
    using TActionList = TVector<std::function<void()>>;

private:
    const TAppContext& Context;
    const TActionList Actions;

    TAdaptiveLock Lock;
    TAdaptiveThreadPool ThreadPool;
    TManualEvent ReadyEvent;
    ui32 DoneCount = 0;

public:
    TExecutor(const TAppContext& ctx, TActionList actions);

    void Run();

private:
    NThreading::TFuture<void> RunAction(ui32 action);
};

}   // namespace NCloud::NFileStore::NReplay
