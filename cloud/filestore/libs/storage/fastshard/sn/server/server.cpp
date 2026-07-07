#include "server.h"

#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>

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

using NCloud::NProto::TDeviceProtocolRequest;
using NCloud::NProto::TDeviceProtocolResponse;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Constants.

constexpr ui64 MaxMessageSize = 64_MB;
constexpr int SocketBacklog = 128;

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

TDeviceProtocolResponse Dispatch(
    IStorageNode& storage,
    const TDeviceProtocolRequest& req)
{
    TDeviceProtocolResponse resp;

    switch (req.GetRequestCase()) {
#define DISPATCH_REQUEST(name, ...)                                            \
        case TDeviceProtocolRequest::k##name: {                                \
            auto result = WaitFiber(storage.name(req.Get##name()));            \
            *resp.Mutable##name() = std::move(result);                         \
            break;                                                             \
        }                                                                      \
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
    int fd = params->Fd;
    auto storage = params->Storage.lock();
    if (!storage) {
        SILK_WARN(
            "conn fd=%d: storage gone before handshake, dropping",
            fd);
        ::close(fd);
        return ECANCELED;
    }

    for (;;) {
        // Read length prefix.
        ui32 lenBe = 0;
        if (int r = RecvAll(fd, &lenBe, sizeof(lenBe)); r) {
            if (r != EIO) {
                SILK_WARN(
                    "conn fd=%d: recv length: %s",
                    fd,
                    ::strerror(r));
            }
            ::close(fd);
            // EIO means the client closed the connection cleanly.
            return r == EIO ? 0 : r;
        }
        ui32 len = ntohl(lenBe);
        if (len > MaxMessageSize) {
            SILK_WARN(
                "conn fd=%d: oversized message: len=%u, limit=%lu",
                fd,
                len,
                MaxMessageSize);
            ::close(fd);
            return EMSGSIZE;
        }

        // Read request body.
        TString reqBuf;
        reqBuf.ReserveAndResize(len);
        if (int r = RecvAll(fd, reqBuf.begin(), len); r) {
            SILK_WARN(
                "conn fd=%d: recv body: %s",
                fd,
                ::strerror(r));
            ::close(fd);
            return r;
        }

        TDeviceProtocolRequest req;
        if (!req.ParseFromString(reqBuf)) {
            SILK_WARN(
                "conn fd=%d: parse request failed, len=%u",
                fd,
                len);
            ::close(fd);
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
            SILK_ERROR(
                "conn fd=%d: failed to serialize response",
                fd);
        }

        ui32 respLenBe = htonl(static_cast<ui32>(respBuf.size()));
        if (int r = SendAll(fd, &respLenBe, sizeof(respLenBe)); r) {
            SILK_WARN(
                "conn fd=%d: send resp length: %s",
                fd,
                ::strerror(r));
            ::close(fd);
            return r;
        }
        if (int r = SendAll(fd, respBuf.data(), respBuf.size()); r) {
            SILK_WARN(
                "conn fd=%d: send resp body: %s",
                fd,
                ::strerror(r));
            ::close(fd);
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
};
static_assert(sizeof(TAcceptParams) <= silk::FIBER_PARAMETERS_SIZE);

int AcceptFiberMain(TAcceptParams* params) noexcept
{
    int lfd = params->ListenFd;
    int sfd = params->ShutdownFd;
    auto storage = params->Storage.lock();
    if (!storage) {
        SILK_WARN("accept fiber: storage gone before startup, exiting");
        return ECANCELED;
    }

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
            ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

            int r = FiberScheduler::run(
                ConnFiberMain,
                TConnParams{.Fd = cfd, .Storage = storage},
                nullptr /* future */);
            if (r) {
                SILK_ERROR("spawn handler fiber: %s", ::strerror(r));
                ::close(cfd);
            }
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
    TServer(ui16 port, IStorageNodePtr storage)
        : Port(port)
        , Storage(std::move(storage))
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
            },
            &AcceptFuture);
        Y_ENSURE(r == 0, "failed to spawn accept fiber: " << ::strerror(r));
    }

    void Stop() override
    {
        if (ShutdownFd >= 0) {
            uint64_t one = 1;
            if (::write(ShutdownFd, &one, sizeof(one)) < 0) {
                SILK_ERROR("shutdown write: %s", ::strerror(errno));
            }
        }

        // Wait for the accept fiber to exit; surface its exit code so that
        // an unclean shutdown (e.g. accept4 failure) is visible in logs.
        const int r = AcceptFuture.wait();
        if (r) {
            SILK_WARN(
                "accept fiber exited with error: %s",
                ::strerror(r));
        } else {
            SILK_INFO("sn server stopped");
        }
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
    IStorageNodePtr Storage;
    int ListenFd = -1;
    int ShutdownFd = -1;
    FiberFuture AcceptFuture;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(ui16 port, IStorageNodePtr storage)
{
    return std::make_shared<TServer>(port, std::move(storage));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
