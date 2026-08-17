#include "server.h"

#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/deque.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/spinlock.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

using silk::FiberFuture;
using silk::FiberScheduler;

using NCloud::NProto::TDeviceProtocolRequest;
using NCloud::NProto::TDeviceProtocolResponse;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Constants.

constexpr ui64 MaxMessageSize = 64_MB;
constexpr int SocketBacklog = 128;

////////////////////////////////////////////////////////////////////////////////
// Handler bookkeeping.
//
// Every accepted connection is represented by a THandler which owns the
// accepted fd and the FiberFuture the ConnFiberMain fiber signals on
// completion. THandlerRegistry keeps them so Stop() can shut every live
// connection down and wait for its fiber to actually exit before returning.

struct THandler
{
    int Fd;
    FiberFuture Future;

    explicit THandler(int fd)
        : Fd(fd)
    {}

    ~THandler()
    {
        if (Fd >= 0) {
            ::close(Fd);
        }
    }

    // FiberFuture holds an atomic, so THandler cannot be copied or moved.
    // The registry stores it via std::unique_ptr, giving it a stable
    // address for the whole ConnFiberMain lifetime.
    THandler(const THandler&) = delete;
    THandler& operator=(const THandler&) = delete;
};

class THandlerRegistry
{
public:
    // Consume the handler on success; return it back if the registry is
    // shutting down. The caller then lets the returned handler's dtor
    // close the fd (see the Register() call site for the shutdown/wait
    // logic that keeps the already-spawned fiber from touching a
    // destroyed future).
    std::unique_ptr<THandler> Register(std::unique_ptr<THandler> h)
    {
        with_lock (Lock) {
            if (Stopping) {
                return h;
            }
            Handlers.push_back(std::move(h));
        }
        return nullptr;
    }

    // Drop any entries whose fiber has already finished. Called from the
    // accept loop so the deque doesn't grow without bound while the server
    // is running.
    void Prune()
    {
        with_lock (Lock) {
            for (auto it = Handlers.begin(); it != Handlers.end();) {
                int err = 0;
                if ((*it)->Future.isSet(&err)) {
                    if (err) {
                        SILK_WARN(
                            "conn fd=%d: error: %s",
                            (*it)->Fd,
                            ::strerror(err));
                    }

                    // ~THandler closes the fd.
                    it = Handlers.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

    // Shut down every still-open connection and wait for its fiber to
    // finish. After this returns no ConnFiberMain fiber is still running
    // and no future Register() call will succeed.
    void Stop()
    {
        TDeque<std::unique_ptr<THandler>> local;
        with_lock (Lock) {
            Stopping = true;
            std::swap(local, Handlers);
        }

        // Half-close each fd so any recv/send inside the handler wakes
        // with EOF/EPIPE; the fd itself stays owned by THandler until
        // wait() returns and we let the unique_ptr fall out of scope.
        for (const auto& h: local) {
            int err = 0;
            if (h->Future.isSet(&err)) {
                continue;
            }
            ::shutdown(h->Fd, SHUT_RDWR);
        }

        for (auto& h: local) {
            h->Future.wait();
        }
        // local goes out of scope: unique_ptr dtors close each fd exactly
        // once, after we know no fiber can still reference it.
    }

private:
    TAdaptiveLock Lock;
    TDeque<std::unique_ptr<THandler>> Handlers;
    bool Stopping = false;
};

using THandlerRegistryPtr = std::shared_ptr<THandlerRegistry>;

////////////////////////////////////////////////////////////////////////////////
// Request dispatch.

TDeviceProtocolResponse Dispatch(
    IStorageNode& storage,
    const TDeviceProtocolRequest& req)
{
    TDeviceProtocolResponse resp;

    switch (req.GetRequestCase()) {
#define DISPATCH_REQUEST(name, ...)                                            \
    case TDeviceProtocolRequest::k##name: {                                    \
        *resp.Mutable##name() = storage.name(req.Get##name());                 \
        break;                                                                 \
    }                                                                          \
        // DISPATCH_REQUEST

        SN_METHODS(DISPATCH_REQUEST)

#undef DISPATCH_REQUEST

        case TDeviceProtocolRequest::REQUEST_NOT_SET: {
            auto* err = resp.MutableProtocolError();
            err->SetCode(E_ARGUMENT);
            err->SetMessage("empty device protocol request");
            break;
        }
    }

    return resp;
}

////////////////////////////////////////////////////////////////////////////////
// Per-connection handler fiber.

struct TConnParams
{
    int Fd;
    std::weak_ptr<IStorageNode> Storage;
};

static_assert(sizeof(TConnParams) <= silk::FIBER_PARAMETERS_SIZE);

int ConnFiberMain(TConnParams* params) noexcept
{
    //
    // The fd is owned by THandler in the server's registry, not by this
    // fiber. We just use it and return; the server closes it after our
    // future is set (via ~THandler in Prune() or Stop()).
    //

    int fd = params->Fd;
    auto storage = params->Storage.lock();
    if (!storage) {
        SILK_WARN("conn fd=%d: storage gone before handshake, dropping", fd);
        return ECANCELED;
    }

    for (;;) {
        // Read length prefix.
        ui32 lenBe = 0;
        if (int r = RecvAll(fd, &lenBe, sizeof(lenBe)); r) {
            if (r != EIO) {
                SILK_WARN("conn fd=%d: recv length: %s", fd, ::strerror(r));
            }

            //
            // EIO means the client closed the connection cleanly, or
            // the server half-closed us from Stop().
            //

            return r == EIO ? 0 : r;
        }
        ui32 len = ntohl(lenBe);
        if (len > MaxMessageSize) {
            SILK_WARN(
                "conn fd=%d: oversized message: len=%u, limit=%lu",
                fd,
                len,
                MaxMessageSize);
            return EMSGSIZE;
        }

        // Read request body.
        TString reqBuf;
        reqBuf.ReserveAndResize(len);
        if (int r = RecvAll(fd, reqBuf.begin(), len); r) {
            SILK_WARN("conn fd=%d: recv body: %s", fd, ::strerror(r));
            return r;
        }

        TDeviceProtocolRequest req;
        if (!req.ParseFromString(reqBuf)) {
            SILK_WARN("conn fd=%d: parse request failed, len=%u", fd, len);
            return EBADMSG;
        }

        // Dispatch.
        TDeviceProtocolResponse resp = Dispatch(*storage, req);

        // Echo the outer request id so the client can correlate.
        resp.SetRequestId(req.GetRequestId());

        // Send response.
        TString respBuf;
        const bool serialized = resp.SerializeToString(&respBuf);
        if (!serialized) {
            SILK_ERROR("conn fd=%d: failed to serialize response", fd);
        }

        ui32 respLenBe = htonl(static_cast<ui32>(respBuf.size()));
        if (int r = SendAll(fd, &respLenBe, sizeof(respLenBe)); r) {
            SILK_WARN("conn fd=%d: send resp length: %s", fd, ::strerror(r));
            return r;
        }
        if (int r = SendAll(fd, respBuf.data(), respBuf.size()); r) {
            SILK_WARN("conn fd=%d: send resp body: %s", fd, ::strerror(r));
            return r;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Accept loop fiber.

struct TAcceptParams
{
    int ListenFd;
    int ShutdownFd;
    std::weak_ptr<IStorageNode> Storage;
    std::weak_ptr<THandlerRegistry> Handlers;
};

static_assert(sizeof(TAcceptParams) <= silk::FIBER_PARAMETERS_SIZE);

void RegisterHandler(
    int cfd,
    THandlerRegistry& handlers,
    std::weak_ptr<IStorageNode> storage) noexcept
{
    int one = 1;
    ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    //
    // Package the fd + a future the spawned fiber will signal on
    // exit. THandler owns the fd (its dtor closes it) once we
    // hand it to the registry, so we do not ::close(cfd) on any
    // path below — the registry's Prune()/Stop() will.
    //

    auto handler = std::make_unique<THandler>(cfd);
    FiberFuture* fut = &handler->Future;

    int r = FiberScheduler::run(
        ConnFiberMain,
        TConnParams{.Fd = cfd, .Storage = std::move(storage)},
        fut);
    if (r) {
        SILK_ERROR("spawn handler fiber: %s", ::strerror(r));

        //
        // Fiber wasn't scheduled; the runtime won't touch the
        // future, and no one will read from cfd. Handler dtor
        // closes cfd on scope exit.
        //

        return;
    }

    //
    // Spawn succeeded — the fiber holds a pointer to
    // handler->Future. If the registry is already shutting down
    // we cannot destroy `handler` (that would free the future
    // while the fiber may still write to it): shutdown the fd so
    // the fiber wakes with EOF/EPIPE, wait for it, then let
    // ~THandler close the fd.
    //

    if (auto rejected = handlers.Register(std::move(handler))) {
        SILK_INFO(
            "registry stopping; waking spawned handler fd=%d",
            rejected->Fd);
        ::shutdown(rejected->Fd, SHUT_RDWR);
        rejected->Future.wait();
    }
}

int AcceptFiberMain(TAcceptParams* params) noexcept
{
    int lfd = params->ListenFd;
    int sfd = params->ShutdownFd;
    auto storage = params->Storage.lock();
    auto handlers = params->Handlers.lock();
    if (!storage || !handlers) {
        SILK_WARN("accept fiber: server gone before startup, exiting");
        return ECANCELED;
    }

    for (;;) {
        // Reap completed handlers so the registry deque doesn't grow.
        handlers->Prune();

        sockaddr_in addr{};
        socklen_t addrLen = sizeof(addr);
        int cfd = ::accept4(
            lfd,
            reinterpret_cast<sockaddr*>(&addr),
            &addrLen,
            SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd >= 0) {
            RegisterHandler(cfd, *handlers, storage);
            continue;
        }

        if (errno == EINTR || errno == ECONNABORTED) {
            continue;
        }

        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            const int savedErrno = errno;
            SILK_ERROR(
                "accept fiber: accept4: %s, exiting",
                ::strerror(savedErrno));
            return savedErrno;
        }

        // Wait for connection or shutdown.
        FiberScheduler::IoFuture acceptFuture;
        FiberScheduler::IoFuture shutdownFuture;
        FiberScheduler::poll(lfd, POLLIN, nullptr, &acceptFuture);
        FiberScheduler::poll(sfd, POLLIN, nullptr, &shutdownFuture);

        FiberFuture* futures[] = {&acceptFuture, &shutdownFuture};
        ui64 which = FiberFuture::waitForMultiple(futures, std::size(futures));

        if (which == 1) {
            acceptFuture.cancel();
            acceptFuture.wait();

            //
            // Drain connections whose handshake has already completed:
            // their clients saw connect() succeed, so they must go
            // through the regular handler shutdown (FIN via
            // THandlerRegistry::Stop) instead of being reset when the
            // listening socket is closed. Stop() waits for this fiber
            // before stopping the registry, so every handler
            // registered here is shut down and waited for.
            //

            for (;;) {
                int cfd = ::accept4(
                    lfd,
                    nullptr /* addr */,
                    nullptr /* addrlen */,
                    SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (cfd >= 0) {
                    RegisterHandler(cfd, *handlers, storage);
                    continue;
                }
                if (errno == EINTR || errno == ECONNABORTED) {
                    continue;
                }
                break;
            }
            return 0;
        }

        shutdownFuture.cancel();
        shutdownFuture.wait();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TServer: public IServer
{
public:
    TServer(ui16 port, IStorageNodePtr storage)
        : Port(port)
        , Storage(std::move(storage))
        , Handlers(std::make_shared<THandlerRegistry>())
    {}

    ~TServer() override
    {
        if (ShutdownFd >= 0) {
            ::close(ShutdownFd);
        }
        if (ListenFd >= 0) {
            ::close(ListenFd);
        }
    }

    void Start() override
    {
        ShutdownFd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (ShutdownFd < 0) {
            SILK_ERROR("eventfd: %s", ::strerror(errno));
            return;
        }

        ListenFd = MakeListenSocket();
        if (ListenFd < 0) {
            return;
        }

        SILK_INFO("sn server listening on port %u", Port);

        // Spawn the accept loop as a background fiber.
        int r = FiberScheduler::run(
            AcceptFiberMain,
            TAcceptParams{
                .ListenFd = ListenFd,
                .ShutdownFd = ShutdownFd,
                .Storage = Storage,
                .Handlers = Handlers,
            },
            &AcceptFuture);
        Y_ENSURE(r == 0, "failed to spawn accept fiber: " << ::strerror(r));
        AcceptFiberSpawned = true;
    }

    void Stop() override
    {
        if (ShutdownFd >= 0) {
            ui64 one = 1;
            if (::write(ShutdownFd, &one, sizeof(one)) < 0) {
                SILK_ERROR("shutdown write: %s", ::strerror(errno));
            }
        }

        //
        // Wait for the accept fiber to exit; surface its exit code so
        // that an unclean shutdown (e.g. accept4 failure) is visible in
        // logs. AcceptFuture is only set after AcceptFiberMain returns,
        // so once this returns we know no new handlers can be
        // registered.
        //
        // Skip the wait if Start() bailed before scheduling the fiber
        // (eventfd/socket/bind/listen failure): AcceptFuture stays
        // unset and .wait() would block forever.
        //

        if (AcceptFiberSpawned) {
            const int r = AcceptFuture.wait();
            if (r) {
                SILK_WARN("accept fiber exited with error: %s", ::strerror(r));
            }
        }

        //
        // Half-close every still-live connection and wait for its
        // handler fiber to finish. After this returns there are no
        // ConnFiberMain fibers left running against this server.
        //

        Handlers->Stop();

        SILK_INFO("sn server stopped");
    }

private:
    int MakeListenSocket()
    {
        int fd =
            ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            SILK_ERROR("socket: %s", ::strerror(errno));
            return -1;
        }

        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(Port);
        addr.sin_addr.s_addr = INADDR_ANY;

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            SILK_ERROR("bind: %s", ::strerror(errno));
            ::close(fd);
            return -1;
        }

        if (::listen(fd, SocketBacklog) < 0) {
            SILK_ERROR("listen: %s", ::strerror(errno));
            ::close(fd);
            return -1;
        }
        return fd;
    }

    ui16 Port;
    IStorageNodePtr Storage;
    THandlerRegistryPtr Handlers;
    int ListenFd = -1;
    int ShutdownFd = -1;
    bool AcceptFiberSpawned = false;
    FiberFuture AcceptFuture;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(ui16 port, IStorageNodePtr storage)
{
    return std::make_shared<TServer>(port, std::move(storage));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
