#include "server.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>
#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
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
using namespace NProtoSrv;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Constants.

constexpr ui64 MaxMessageSize = 64_MB;
constexpr int SocketBacklog = 128;

////////////////////////////////////////////////////////////////////////////////

int BindAndListen(int fd, const sockaddr* addr, socklen_t addrLen)
{
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    if (::bind(fd, addr, addrLen) < 0) {
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
// Bridge NThreading::TFuture to silk FiberFuture.

template <typename T>
T WaitFiber(const NThreading::TFuture<T>& future)
{
    FiberFuture fiberFuture;
    future.Subscribe([&fiberFuture](const auto&) { fiberFuture.set(0); });
    fiberFuture.wait();
    return future.GetValue();
}

////////////////////////////////////////////////////////////////////////////////
// Request dispatch.

TResponse Dispatch(IFileSystemShard& shard, const TRequest& req)
{
    TResponse resp;

    switch (req.GetBodyCase()) {
#define DISPATCH_REQUEST(name, ...)                                            \
    case TRequest::k##name: {                                                  \
        auto result = WaitFiber(shard.name(req.Get##name()));                  \
        *resp.Mutable##name() = std::move(result);                             \
        break;                                                                 \
    }                                                                          \
        // DISPATCH_REQUEST

        FAST_SHARD_PRIVATE_METHODS(DISPATCH_REQUEST)
        FAST_SHARD_PUBLIC_METHODS(DISPATCH_REQUEST)

#undef DISPATCH_REQUEST

        case TRequest::BODY_NOT_SET:
            break;
    }

    return resp;
}

////////////////////////////////////////////////////////////////////////////////
// Shard registry — thread-safe map of FileSystemId -> IFileSystemShard.

class TShardRegistry
{
public:
    void Register(const TString& id, IFileSystemShardPtr shard)
    {
        auto guard = Guard(Lock);
        Shards[id] = std::move(shard);
    }

    void Unregister(const TString& id)
    {
        auto guard = Guard(Lock);
        Shards.erase(id);
    }

    IFileSystemShardPtr Find(const TString& id)
    {
        auto guard = Guard(Lock);
        auto it = Shards.find(id);
        return it != Shards.end() ? it->second : nullptr;
    }

private:
    TAdaptiveLock Lock;
    THashMap<TString, IFileSystemShardPtr> Shards;
};

////////////////////////////////////////////////////////////////////////////////
// Per-connection handler fiber.

struct TConnParams
{
    int Fd;
    std::weak_ptr<TShardRegistry> Registry;
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
    auto registry = params->Registry.lock();
    if (!registry) {
        return ECANCELED;
    }

    for (;;) {
        // Read length prefix.
        ui32 lenBe = 0;
        if (int r = RecvAll(fd, &lenBe, sizeof(lenBe)); r) {
            //
            // EIO means the client closed the connection cleanly, or
            // the server half-closed us from Stop().
            //

            return r == EIO ? 0 : r;
        }
        ui32 len = ntohl(lenBe);
        if (len > MaxMessageSize) {
            return EMSGSIZE;
        }

        // Read request body.
        TString reqBuf;
        reqBuf.ReserveAndResize(len);
        if (int r = RecvAll(fd, reqBuf.begin(), len); r) {
            return r;
        }

        TRequest req;
        if (!req.ParseFromString(reqBuf)) {
            return EBADMSG;
        }

        // Route to the right shard.
        TResponse resp;
        auto shard = registry->Find(req.GetFileSystemId());
        if (!shard) {
            SILK_WARN(
                "failed to find shard: %s",
                req.GetFileSystemId().c_str());
            auto* err = resp.MutableError();
            err->SetCode(E_NOT_FOUND);
            err->SetMessage(
                TStringBuilder()
                << "no shard registered for " << req.GetFileSystemId());
        } else {
            resp = Dispatch(*shard, req);
        }

        // Send response.
        TString respBuf;
        const bool serialized = resp.SerializeToString(&respBuf);
        if (!serialized) {
            SILK_ERROR("failed to serialize response");
        }

        ui32 respLenBe = htonl(static_cast<ui32>(respBuf.size()));
        if (int r = SendAll(fd, &respLenBe, sizeof(respLenBe)); r) {
            SILK_WARN("send resp length: %s", ::strerror(r));
            return r;
        }
        if (int r = SendAll(fd, respBuf.data(), respBuf.size()); r) {
            SILK_WARN("send resp body: %s", ::strerror(r));
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
    std::weak_ptr<TShardRegistry> Registry;
    std::weak_ptr<THandlerRegistry> Handlers;
};

static_assert(sizeof(TAcceptParams) <= silk::FIBER_PARAMETERS_SIZE);

void RegisterHandler(
    int cfd,
    THandlerRegistry& handlers,
    std::weak_ptr<TShardRegistry> registry) noexcept
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
        TConnParams{.Fd = cfd, .Registry = std::move(registry)},
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
    auto registry = params->Registry.lock();
    auto handlers = params->Handlers.lock();
    if (!registry || !handlers) {
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
            RegisterHandler(cfd, *handlers, registry);
            continue;
        }

        if (errno == EINTR || errno == ECONNABORTED) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return errno;
        }

        // Wait for connection or shutdown.
        FiberScheduler::IoFuture acceptFuture;
        FiberScheduler::IoFuture shutdownFuture;
        FiberScheduler::poll(lfd, POLLIN, nullptr, &acceptFuture);
        FiberScheduler::poll(sfd, POLLIN, nullptr, &shutdownFuture);

        FiberFuture* futures[] = {&acceptFuture, &shutdownFuture};
        uint64_t which =
            FiberFuture::waitForMultiple(futures, std::size(futures));

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
                    RegisterHandler(cfd, *handlers, registry);
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
    explicit TServer(ui16 port)
        : Port(port)
        , Registry(std::make_shared<TShardRegistry>())
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

        SILK_INFO("fastshard server listening on port %u", Port);

        // Spawn the accept loop as a background fiber.
        int r = FiberScheduler::run(
            AcceptFiberMain,
            TAcceptParams{
                .ListenFd = ListenFd,
                .ShutdownFd = ShutdownFd,
                .Registry = Registry,
                .Handlers = Handlers,
            },
            &AcceptFuture);
        Y_ENSURE(r == 0, "failed to spawn accept fiber: " << ::strerror(r));
        AcceptFiberSpawned = true;
    }

    void Stop() override
    {
        if (ShutdownFd >= 0) {
            uint64_t one = 1;
            if (::write(ShutdownFd, &one, sizeof(one)) < 0) {
                SILK_ERROR("shutdown write: %s", ::strerror(errno));
            }
        }

        //
        // Wait for the accept fiber to exit. AcceptFuture is only set
        // once AcceptFiberMain returns, so once this returns no new
        // handlers can be registered.
        //
        // Skip the wait if Start() bailed before scheduling the fiber
        // (eventfd/socket/bind/listen failure): AcceptFuture stays
        // unset and .wait() would block forever.
        //

        if (AcceptFiberSpawned) {
            AcceptFuture.wait();
        }

        //
        // Half-close every still-live connection and wait for its
        // handler fiber to finish.
        //

        Handlers->Stop();
    }

    void RegisterShard(
        const TString& fileSystemId,
        IFileSystemShardPtr shard) override
    {
        Registry->Register(fileSystemId, std::move(shard));
    }

    void UnregisterShard(const TString& fileSystemId) override
    {
        Registry->Unregister(fileSystemId);
    }

private:
    int MakeListenSocket()
    {
        //
        // Dual-stack listener: an AF_INET6 socket with IPV6_V6ONLY
        // cleared accepts IPv6 connections directly and IPv4 ones as
        // v4-mapped addresses. The clients connect to the FQDN published
        // by the tablet, which resolves to AAAA records in IPv6-only
        // networks - an AF_INET listener is unreachable there.
        //

        int fd =
            ::socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd >= 0) {
            //
            // The IPV6_V6ONLY default depends on net.ipv6.bindv6only, so
            // clear it explicitly.
            //

            int zero = 0;
            int r = ::setsockopt(
                fd,
                IPPROTO_IPV6,
                IPV6_V6ONLY,
                &zero,
                sizeof(zero));
            if (r < 0) {
                SILK_WARN(
                    "clear IPV6_V6ONLY: %s - IPv4 clients will be rejected",
                    ::strerror(errno));
            }

            sockaddr_in6 addr{};
            addr.sin6_family = AF_INET6;
            addr.sin6_port = htons(Port);
            addr.sin6_addr = in6addr_any;

            return BindAndListen(
                fd,
                reinterpret_cast<sockaddr*>(&addr),
                sizeof(addr));
        }

        //
        // Hosts with IPv6 disabled fail socket(AF_INET6) with
        // EAFNOSUPPORT - fall back to an IPv4-only listener.
        //

        SILK_WARN(
            "socket(AF_INET6): %s - falling back to IPv4",
            ::strerror(errno));

        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            SILK_ERROR("socket: %s", ::strerror(errno));
            return -1;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(Port);
        addr.sin_addr.s_addr = INADDR_ANY;

        return BindAndListen(
            fd,
            reinterpret_cast<sockaddr*>(&addr),
            sizeof(addr));
    }

    ui16 Port;
    int ListenFd = -1;
    int ShutdownFd = -1;
    std::shared_ptr<TShardRegistry> Registry;
    THandlerRegistryPtr Handlers;
    bool AcceptFiberSpawned = false;
    FiberFuture AcceptFuture;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(ui16 port)
{
    return std::make_shared<TServer>(port);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
