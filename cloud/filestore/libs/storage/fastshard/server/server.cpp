#include "server.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>

#include <cerrno>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

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
// Async TCP helpers.

int RecvAll(int fd, void* buf, size_t len)
{
    auto* p = static_cast<ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::recv(fd, p + done, len - done, 0);
        if (n > 0) {
            done += static_cast<size_t>(n);
            continue;
        }
        if (n == 0) {
            return EIO;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (int r = FiberScheduler::poll(fd, POLLIN); r) {
                return r;
            }
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        return errno;
    }
    return 0;
}

int SendAll(int fd, const void* buf, size_t len)
{
    const auto* p = static_cast<const ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::send(fd, p + done, len - done, MSG_NOSIGNAL);
        if (n > 0) {
            done += static_cast<size_t>(n);
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (int r = FiberScheduler::poll(fd, POLLOUT); r) {
                return r;
            }
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        return errno;
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////
// Bridge NThreading::TFuture to silk FiberFuture.

template <typename T>
T WaitFiber(const NThreading::TFuture<T>& future)
{
    FiberFuture fiberFuture;
    future.Subscribe([&fiberFuture](const auto&) {
        fiberFuture.set(0);
    });
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
        auto result = WaitFiber(                                               \
            shard.name(req.Get##name()));                                      \
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
    TShardRegistry* Registry;
};
static_assert(sizeof(TConnParams) <= silk::FIBER_PARAMETERS_SIZE);

int ConnFiberMain(TConnParams* params) noexcept
{
    int fd = params->Fd;
    auto* registry = params->Registry;

    // Read length prefix.
    ui32 lenBe = 0;
    if (int r = RecvAll(fd, &lenBe, sizeof(lenBe)); r) {
        ::close(fd);
        return r;
    }
    ui32 len = ntohl(lenBe);
    if (len > MaxMessageSize) {
        ::close(fd);
        return EMSGSIZE;
    }

    // Read request body.
    TString reqBuf;
    reqBuf.ReserveAndResize(len);
    if (int r = RecvAll(fd, reqBuf.begin(), len); r) {
        ::close(fd);
        return r;
    }

    TRequest req;
    if (!req.ParseFromString(reqBuf)) {
        ::close(fd);
        return EBADMSG;
    }

    // Route to the right shard.
    TResponse resp;
    auto shard = registry->Find(req.GetFileSystemId());
    if (!shard) {
        auto* err = resp.MutableError();
        err->SetCode(ENOENT);
        err->SetMessage(
            TStringBuilder() << "no shard registered for "
                << req.GetFileSystemId());
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
        ::close(fd);
        return r;
    }
    if (int r = SendAll(fd, respBuf.data(), respBuf.size()); r) {
        SILK_WARN("send resp body: %s", ::strerror(r));
        ::close(fd);
        return r;
    }

    ::close(fd);
    return 0;
}

////////////////////////////////////////////////////////////////////////////////
// Accept loop fiber.

struct TAcceptParams
{
    int ListenFd;
    int ShutdownFd;
    TShardRegistry* Registry;
};
static_assert(sizeof(TAcceptParams) <= silk::FIBER_PARAMETERS_SIZE);

int AcceptFiberMain(TAcceptParams* params) noexcept
{
    int lfd = params->ListenFd;
    int sfd = params->ShutdownFd;
    auto* registry = params->Registry;

    for (;;) {
        sockaddr_in addr{};
        socklen_t addrLen = sizeof(addr);
        int cfd = ::accept4(
            lfd,
            reinterpret_cast<sockaddr*>(&addr),
            &addrLen,
            SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd >= 0) {
            int one = 1;
            ::setsockopt(
                cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

            int r = FiberScheduler::run(
                ConnFiberMain,
                TConnParams{.Fd = cfd, .Registry = registry},
                nullptr);
            if (r) {
                SILK_ERROR(
                    "spawn handler fiber: %s",
                    ::strerror(r));
                ::close(cfd);
            }
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

        FiberScheduler::run(
            AcceptFiberMain,
            TAcceptParams{
                .ListenFd = ListenFd,
                .ShutdownFd = ShutdownFd,
                .Registry = &Registry,
            });
    }

    void Stop() override
    {
        if (ShutdownFd >= 0) {
            uint64_t one = 1;
            if (::write(ShutdownFd, &one, sizeof(one)) < 0) {
                SILK_ERROR("shutdown write: %s", ::strerror(errno));
            }
        }
    }

    void RegisterShard(
        const TString& fileSystemId,
        IFileSystemShardPtr shard) override
    {
        Registry.Register(fileSystemId, std::move(shard));
    }

    void UnregisterShard(const TString& fileSystemId) override
    {
        Registry.Unregister(fileSystemId);
    }

private:
    int MakeListenSocket()
    {
        int fd = ::socket(
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0);
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

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr))
            < 0)
        {
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
    int ListenFd = -1;
    int ShutdownFd = -1;
    TShardRegistry Registry;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(ui16 port)
{
    return std::make_unique<TServer>(port);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
