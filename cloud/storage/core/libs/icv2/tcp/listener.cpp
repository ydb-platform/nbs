#include "listener.h"

#include "listener_private.h"
#include "socket_helpers.h"
#include "socket_worker.h"

#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/uds/socket_poller.h>

#include <util/generic/yexception.h>

#include <array>

namespace NCloud::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int SocketBacklog = 128;

// Linux may report a pending TCP connection's network error from accept4().
// Leave the listener running and return to the poller for the next attempt.
bool IsTransientAcceptError(int error)
{
    if (error == EAGAIN || error == EWOULDBLOCK || error == EINTR ||
        error == ECONNABORTED)
    {
        return true;
    }
    switch (error) {
        case ENETDOWN:
        case EPROTO:
        case ENOPROTOOPT:
        case EHOSTDOWN:
        case ENONET:
        case EHOSTUNREACH:
        case EOPNOTSUPP:
        case ENETUNREACH:
            return true;
        default:
            return false;
    }
}

class TListener final
    : public IListener
    , private ISocketWorkerHandler
{
    TSocketWorker Worker;
    NStorage::NServer::TSocketPoller SocketPoller;
    TSocketPtr Socket;
    const NInterconnect::TAddress Address;
    const IListenerClientPtr Client;
    const NPrivate::TAcceptErrorInjector InjectAcceptError;

public:
    TListener(
        TSocketPtr socket,
        NInterconnect::TAddress address,
        IListenerClientPtr client,
        NPrivate::TAcceptErrorInjector injectAcceptError)
        : Worker(*this)
        , Socket(std::move(socket))
        , Address(address)
        , Client(std::move(client))
        , InjectAcceptError(std::move(injectAcceptError))
    {
        SocketPoller.WaitRead(*Socket, this);
        SocketPoller.WaitRead(Worker.GetWakeSocket(), nullptr);
    }

    ~TListener() override
    {
        Stop();
    }

    const NInterconnect::TAddress& GetAddress() const override
    {
        return Address;
    }

    void Start() override
    {
        Worker.Start();
    }

    void RequestStop() override
    {
        Worker.RequestStop();
    }

    void Stop() override
    {
        Worker.Stop();
    }

private:
    void Run() override
    {
        NCloud::SetCurrentThreadName("ICV2Acceptor", 0);
        std::array<void*, 2> cookies;
        while (!Worker.IsStopping()) {
            const size_t count =
                SocketPoller.Wait(cookies.data(), cookies.size());
            for (size_t i = 0; i < count && !Worker.IsStopping(); ++i) {
                if (!cookies[i]) {
                    Worker.DrainWake();
                } else {
                    AcceptClients();
                }
            }
        }
        // The worker owns the socket until joined. Release the port even when
        // shutdown was requested by a callback and Stop has not been called
        // yet.
        CloseListener();
    }

    int AcceptSocket()
    {
        if (InjectAcceptError) {
            if (const int error = InjectAcceptError()) {
                errno = error;
                return -1;
            }
        }
        return accept4(*Socket, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    }

    void AcceptClients()
    {
        while (!Worker.IsStopping()) {
            const int fd = AcceptSocket();
            if (fd < 0) {
                const int error = errno;
                if (IsTransientAcceptError(error)) {
                    break;
                }
                Client->OnError(MakeSocketError(error, "accept"));
                RequestStop();
                break;
            }
            auto result =
                PrepareSocket(MakeIntrusive<NInterconnect::TStreamSocket>(fd));
            if (!Worker.IsStopping()) {
                const bool failed = HasError(result.GetError());
                if (failed) {
                    Client->OnError(result.GetError());
                } else {
                    Client->OnConnection(result.ExtractResult());
                }
                if (failed) {
                    RequestStop();
                }
            }
        }
    }

    void CloseListener()
    {
        if (Socket) {
            SocketPoller.Unwait(*Socket);
            Socket.Reset();
        }
    }

    void OnStopped() override
    {
        CloseListener();   // also covers Stop before Start
        SocketPoller.Unwait(Worker.GetWakeSocket());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IListenerClient::~IListenerClient() = default;

TResultOrError<IListenerPtr> NPrivate::CreateListenerWithAcceptError(
    const NInterconnect::TAddress& address,
    int backlog,
    IListenerClientPtr client,
    TAcceptErrorInjector injectAcceptError)
{
    if (!client || backlog <= 0) {
        return MakeError(
            E_ARGUMENT,
            "listener requires a client and positive backlog");
    }
    auto [socket, error] = CreateSocket(address);
    if (HasError(error)) {
        return error;
    }
    const int enabled = 1;
    if (setsockopt(
            *socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            &enabled,
            sizeof(enabled)) < 0)
    {
        return MakeSocketError(errno, "set SO_REUSEADDR");
    }
    if (address.GetFamily() == AF_INET6 && setsockopt(
                                               *socket,
                                               IPPROTO_IPV6,
                                               IPV6_V6ONLY,
                                               &enabled,
                                               sizeof(enabled)) < 0)
    {
        return MakeSocketError(errno, "set IPV6_V6ONLY");
    }
    if (const int res = socket->Bind(address); res < 0) {
        return MakeSocketError(-res, "bind");
    }
    if (const int res = socket->Listen(backlog); res < 0) {
        return MakeSocketError(-res, "listen");
    }
    NInterconnect::TAddress bound;
    socklen_t size = sizeof(bound.Addr);
    if (getsockname(*socket, bound.SockAddr(), &size) < 0) {
        return MakeSocketError(errno, "getsockname");
    }
    try {
        return IListenerPtr(
            std::make_shared<TListener>(
                std::move(socket),
                bound,
                std::move(client),
                std::move(injectAcceptError)));
    } catch (const TSystemError& error) {
        return MakeSocketError(error.Status(), "create listener resources");
    }
}

TResultOrError<IListenerPtr> CreateListener(
    const NInterconnect::TAddress& address,
    IListenerClientPtr client)
{
    return NPrivate::CreateListenerWithAcceptError(
        address,
        SocketBacklog,
        std::move(client));
}

}   // namespace NCloud::NIcV2
