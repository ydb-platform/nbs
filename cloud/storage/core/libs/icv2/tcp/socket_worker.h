#pragma once

#include <util/generic/array_ref.h>
#include <util/network/socket.h>
#include <util/system/mutex.h>

#include <poll.h>

#include <atomic>
#include <thread>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Keep alive until the worker is destroyed.
struct ISocketWorkerHandler
{
    virtual ~ISocketWorkerHandler();

    // Do work until the worker's IsStopping() becomes true, then return.
    virtual void Run() = 0;
    // Called once by Stop(), even if never started.
    virtual void OnStopped();
};

// Start once. Call Stop outside Run() before destruction; it waits for the
// handler and cleanup. Repeated Stop calls are safe.
class TSocketWorker
{
    ISocketWorkerHandler& Handler;
    TSocketHolder WakeReader;
    TSocketHolder WakeWriter;
    TMutex StopMutex;
    std::thread Worker;
    bool Joined = false;
    std::atomic<bool> Stopping = false;

public:
    explicit TSocketWorker(ISocketWorkerHandler& handler);
    ~TSocketWorker();

    void Start();
    // Safe in Run(); does not wait for completion.
    void RequestStop();
    void Stop();

    void Wake();
    bool IsStopping() const;
    // Monitor readability to respond to Wake(); then call DrainWake().
    SOCKET GetWakeSocket() const;
    void DrainWake();
    // Put GetWakeSocket() first in fds. Timeout is in ms; errors return -errno.
    int Poll(TArrayRef<pollfd> fds, int timeout);
};

}   // namespace NCloud::NIcV2
