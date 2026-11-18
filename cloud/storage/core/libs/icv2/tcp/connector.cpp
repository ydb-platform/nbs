#include "connector.h"

#include "socket_helpers.h"
#include "socket_worker.h"

#include <cloud/storage/core/libs/common/thread.h>

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/guard.h>

#include <climits>

namespace NCloud::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TConnectRequest
{
    const NInterconnect::TAddress Address;
    const TMonotonic Deadline;
    NThreading::TPromise<TSocketResult> Promise =
        NThreading::NewPromise<TSocketResult>();
    TSocketPtr Socket;

    void Complete(TSocketResult result)
    {
        Socket.Reset();
        Promise.SetValue(std::move(result));
    }
};

using TRequests = TVector<std::unique_ptr<TConnectRequest>>;

class TConnector final
    : public IConnector
    , private ISocketWorkerHandler
{
    TSocketWorker Worker;
    TMutex QueueMutex;
    TRequests Queued;
    bool Accepting = false;

public:
    TConnector()
        : Worker(*this)
    {}

    ~TConnector() override
    {
        Stop();
    }

    void Start() override
    {
        TGuard<TMutex> guard(QueueMutex);
        Worker.Start();
        Accepting = true;
    }

    void RequestStop() override
    {
        {
            TGuard<TMutex> guard(QueueMutex);
            Accepting = false;
        }
        Worker.RequestStop();
    }

    void Stop() override
    {
        RequestStop();
        Worker.Stop();
    }

    TSocketFuture Connect(
        const NInterconnect::TAddress& address,
        TDuration timeout) override
    {
        auto request = std::make_unique<TConnectRequest>(
            address,
            TMonotonic::Now() + timeout);
        auto future = request->Promise.GetFuture();
        bool wake = false;
        {
            TGuard<TMutex> guard(QueueMutex);
            if (Accepting) {
                wake = Queued.empty();
                Queued.push_back(std::move(request));
            }
        }
        if (request) {
            request->Complete(
                MakeError(E_REJECTED, "TCP connector is not running"));
        } else if (wake) {
            Worker.Wake();
        }
        return future;
    }

private:
    static void Reject(TRequests requests, const NProto::TError& error)
    {
        for (auto& request: requests) {
            request->Complete(error);
        }
    }

    TRequests TakeQueued()
    {
        TRequests queued;
        TGuard<TMutex> guard(QueueMutex);
        queued.swap(Queued);
        return queued;
    }

    static void StartRequest(
        std::unique_ptr<TConnectRequest> request,
        TRequests& pending)
    {
        if (TMonotonic::Now() >= request->Deadline) {
            request->Complete(MakeError(E_TIMEOUT, "TCP connect timed out"));
            return;
        }
        auto [socket, error] = CreateSocket(request->Address);
        if (HasError(error)) {
            request->Complete(std::move(error));
            return;
        }
        request->Socket = std::move(socket);
        const int status = ConnectSocket(*request->Socket, request->Address);
        if (!status) {
            request->Complete(std::move(request->Socket));
        } else if (status == EINPROGRESS || status == EINTR) {
            pending.push_back(std::move(request));
        } else {
            request->Complete(MakeSocketError(status, "connect"));
        }
    }

    void ProcessQueued(TRequests& pending, const NProto::TError& failure)
    {
        auto queued = TakeQueued();
        for (auto& request: queued) {
            if (Worker.IsStopping()) {
                request->Complete(failure);
            } else {
                StartRequest(std::move(request), pending);
            }
        }
    }

    int PollPending(const TRequests& pending, TVector<pollfd>& fds)
    {
        fds[0] = {
            .fd = Worker.GetWakeSocket(),
            .events = POLLIN,
            .revents = 0,
        };
        auto deadline = TMonotonic::Max();
        for (size_t i = 0; i < pending.size(); ++i) {
            fds[i + 1] = {
                .fd = *pending[i]->Socket,
                .events = POLLOUT,
                .revents = 0,
            };
            deadline = Min(deadline, pending[i]->Deadline);
        }
        const auto now = TMonotonic::Now();
        const int timeout =
            pending.empty() ? -1
            : deadline <= now
                ? 0
                : static_cast<int>(
                      Min<ui64>((deadline - now).MilliSeconds() + 1, INT_MAX));
        return Worker.Poll(fds, timeout);
    }

    void ProcessPending(TRequests& pending, const TVector<pollfd>& fds)
    {
        for (size_t i = pending.size(); i-- && !Worker.IsStopping();) {
            NProto::TError error;
            bool complete = false;
            if (TMonotonic::Now() >= pending[i]->Deadline) {
                error = MakeError(E_TIMEOUT, "TCP connect timed out");
                complete = true;
            } else if (fds[i + 1].revents) {
                const int status = pending[i]->Socket->GetConnectStatus();
                if (status) {
                    error = MakeSocketError(status, "connect completion");
                }
                complete = true;
            }
            if (complete) {
                auto request = std::move(pending[i]);
                pending[i] = std::move(pending.back());
                pending.pop_back();
                if (HasError(error)) {
                    request->Complete(std::move(error));
                } else {
                    request->Complete(std::move(request->Socket));
                }
            }
        }
    }

    void Run() override
    {
        NCloud::SetCurrentThreadName("ICV2Connector", 0);
        TRequests pending;
        TVector<pollfd> fds;
        auto failure = MakeError(E_REJECTED, "TCP connector is stopping");
        while (!Worker.IsStopping()) {
            ProcessQueued(pending, failure);
            if (Worker.IsStopping()) {
                break;
            }
            fds.resize(pending.size() + 1);
            const int res = PollPending(pending, fds);
            if (res < 0) {
                failure = MakeSocketError(-res, "connector poll");
                RequestStop();
                break;
            }
            ProcessPending(pending, fds);
        }
        Reject(std::move(pending), failure);
        Reject(TakeQueued(), failure);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IConnectorPtr> CreateConnector()
{
    try {
        return IConnectorPtr(std::make_shared<TConnector>());
    } catch (const TSystemError& error) {
        return MakeSocketError(error.Status(), "create connector resources");
    }
}

}   // namespace NCloud::NIcV2
