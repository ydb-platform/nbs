#pragma once

#include <util/generic/vector.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/thread/factory.h>

namespace NCloud::NFileStore::NMetrics::NTests {

////////////////////////////////////////////////////////////////////////////////

class TScopedTasks
{
private:
    using TThread = THolder<IThreadFactory::IThread>;

    TVector<TThread> Workers;

    bool Started = false;
    TCondVar StartedCV;
    TMutex Mutex;

public:
    ~TScopedTasks()
    {
        Y_ABORT_UNLESS(Workers.empty());
    }

    void Start()
    {
        with_lock (Mutex) {
            Started = true;
        }
        StartedCV.BroadCast();
    }

    void Stop()
    {
        for (auto& worker: Workers) {
            worker->Join();
        }
        Workers.clear();
    }

    template <typename TFunction>
    void Add(TFunction&& function)
    {
        Workers.push_back(SystemThreadFactory()->Run(
            [this, function = std::forward<TFunction>(function)]
            {
                with_lock (Mutex) {
                    StartedCV.Wait(Mutex, [&] { return Started; });
                }
                function();
            }));
    }
};

}   // namespace NCloud::NFileStore::NMetrics::NTests
