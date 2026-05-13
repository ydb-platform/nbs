#include <cloud/storage/core/tools/testing/silk_demo/protos/silk_demo.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>
#include <silk/util/logger.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace NCloud::NStorage::NSilkDemo;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto ErrnoMsg(int err)
{
    return std::system_category().message(err);
}

////////////////////////////////////////////////////////////////////////////////

struct TContext
{
    std::atomic<int> ListenFd = -1;
};

TContext* ContextPtr = nullptr;

////////////////////////////////////////////////////////////////////////////////
// Async TCP helpers built on silk's poll.

// Read exactly len bytes into buf from a non-blocking socket.
// Returns 0 on success, errno on error, EIO on unexpected EOF.
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
            return EIO;   // peer closed
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            int r = FiberScheduler::poll(fd, POLLIN);
            if (r) {
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
            int r = FiberScheduler::poll(fd, POLLOUT);
            if (r) {
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
// Request handlers.

void SetErrno(TResponse& resp, int err)
{
    resp.MutableError()->SetCode(err);
    resp.MutableError()->SetMessage(ErrnoMsg(err));
}

void HandleWrite(const TWriteRequest& req, TResponse& resp)
{
    int fd = ::open(req.GetPath().c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        SetErrno(resp, errno);
        return;
    }

    ui64 written = 0;
    int r = FiberScheduler::write(
        fd,
        req.GetData().data(),
        req.GetData().size(),
        req.GetOffset(),
        &written);

    ::close(fd);

    if (r) {
        SetErrno(resp, r);
        return;
    }
    resp.MutableWrite()->SetBytesWritten(written);
}

void HandleRead(const TReadRequest& req, TResponse& resp)
{
    int fd = ::open(req.GetPath().c_str(), O_RDONLY);
    if (fd < 0) {
        SetErrno(resp, errno);
        return;
    }

    TString buf;
    buf.ReserveAndResize(req.GetLength());

    ui64 bytesRead = 0;
    int r = FiberScheduler::read(
        fd,
        buf.begin(),
        buf.size(),
        req.GetOffset(),
        &bytesRead);

    ::close(fd);

    if (r) {
        SetErrno(resp, r);
        return;
    }
    buf.resize(bytesRead);
    resp.MutableRead()->SetData(std::move(buf));
}

////////////////////////////////////////////////////////////////////////////////
// Per-connection handler fiber.

struct TConnParams
{
    int Fd;
};
static_assert(sizeof(TConnParams) <= silk::FIBER_PARAMETERS_SIZE);

int ConnFiberMain(TConnParams* params) noexcept
{
    int fd = params->Fd;

    try {
        // Read length prefix (4 bytes, big-endian).
        ui32 lenBe = 0;
        if (int r = RecvAll(fd, &lenBe, sizeof(lenBe)); r) {
            SILK_WARN("recv length failed: {}", ErrnoMsg(r));
            ::close(fd);
            return r;
        }
        ui32 len = ntohl(lenBe);
        if (len > 64_MB) {
            SILK_WARN("request too large: {}", len);
            ::close(fd);
            return EMSGSIZE;
        }

        // Read request body.
        TString reqBuf;
        reqBuf.ReserveAndResize(len);
        if (int r = RecvAll(fd, reqBuf.begin(), len); r) {
            SILK_WARN("recv body failed: {}", ErrnoMsg(r));
            ::close(fd);
            return r;
        }

        TRequest req;
        if (!req.ParseFromString(reqBuf)) {
            SILK_WARN("malformed request");
            ::close(fd);
            return EBADMSG;
        }

        TResponse resp;
        switch (req.GetBodyCase()) {
            case TRequest::kWrite:
                HandleWrite(req.GetWrite(), resp);
                break;
            case TRequest::kRead:
                HandleRead(req.GetRead(), resp);
                break;
            default:
                SetErrno(resp, EINVAL);
                break;
        }

        // Send response.
        TString respBuf;
        if (!resp.SerializeToString(&respBuf)) {
            SILK_WARN("can't serialize response");
            return EBADMSG;
        }

        ui32 respLenBe = htonl(static_cast<ui32>(respBuf.size()));
        if (int r = SendAll(fd, &respLenBe, sizeof(respLenBe)); r) {
            SILK_WARN("send length failed: {}", ErrnoMsg(r));
        } else if (int r = SendAll(fd, respBuf.data(), respBuf.size()); r) {
            SILK_WARN("send body failed: {}", ErrnoMsg(r));
        }
    } catch (const std::exception& e) {
        SILK_ERROR("handler exception: {}", e.what());
    }

    ::close(fd);
    return 0;
}

////////////////////////////////////////////////////////////////////////////////
// Accept loop fiber.

struct TAcceptParams
{
    TContext* context;
};
static_assert(sizeof(TAcceptParams) <= silk::FIBER_PARAMETERS_SIZE);

int AcceptFiberMain(TAcceptParams* params) noexcept
{
    auto& listenFd = params->context->ListenFd;
    while (true) {
        int lfd = listenFd.load(std::memory_order_acquire);
        if (lfd < 0) {
            SILK_ERROR("lfd closed: {}", ErrnoMsg(errno));
            break;
        }

        sockaddr_in addr{};
        socklen_t addrLen = sizeof(addr);
        int cfd = ::accept4(
            lfd,
            reinterpret_cast<sockaddr*>(&addr),
            &addrLen,
            SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (int r = FiberScheduler::poll(lfd, POLLIN); r && r != EINTR) {
                    SILK_ERROR("poll(listen) failed: {}", ErrnoMsg(r));
                    break;
                }
                continue;
            }
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            SILK_ERROR("accept failed: {}", ErrnoMsg(errno));
            break;
        }

        int one = 1;
        ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        int r = FiberScheduler::run(ConnFiberMain, TConnParams{cfd}, nullptr);
        if (r) {
            SILK_ERROR("spawn handler fiber failed: {}", ErrnoMsg(r));
            ::close(cfd);
        }
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

int MakeListenSocket(const TString& host, ui16 port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        SILK_ERROR("socket: {}", ErrnoMsg(errno));
        return -1;
    }

    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        SILK_ERROR(
            "inet_pton({}): {}",
            std::string_view(host),
            ErrnoMsg(errno));
        ::close(fd);
        return -1;
    }

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        SILK_ERROR("bind: {}", ErrnoMsg(errno));
        ::close(fd);
        return -1;
    }

    if (::listen(fd, 128) < 0) {
        SILK_ERROR("listen: {}", ErrnoMsg(errno));
        ::close(fd);
        return -1;
    }

    return fd;
}

void HandleSignal(int sig)
{
    Y_UNUSED(sig);

    while (ContextPtr) {
        int fd = ContextPtr->ListenFd.load(std::memory_order_acquire);
        if (fd < 0) {
            break;
        }

        const bool exchanged = ContextPtr->ListenFd.compare_exchange_weak(
            fd,
            -1,
            std::memory_order_acq_rel);
        if (exchanged) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
            break;
        }
    }
}

}   // namespace

int main(int argc, const char** argv)
{
    TString host;
    ui16 port = 0;

    {
        using namespace NLastGetopt;
        TOpts opts;
        opts.AddHelpOption();
        opts.AddLongOption("host", "listen host (IPv4)")
            .RequiredArgument("IP")
            .DefaultValue("127.0.0.1")
            .StoreResult(&host);
        opts.AddLongOption("port", "listen port")
            .RequiredArgument("NUM")
            .Required()
            .StoreResult(&port);
        TOptsParseResultException(&opts, argc, argv);
    }

    ::signal(SIGPIPE, SIG_IGN);
    ::signal(SIGINT, HandleSignal);
    ::signal(SIGTERM, HandleSignal);

    silk::initialize();
    FiberScheduler::initialize();

    auto context = std::make_unique<TContext>();
    context->ListenFd = MakeListenSocket(host, port);
    ContextPtr = context.get();

    if (context->ListenFd < 0) {
        FiberScheduler::destroy();
        silk::destroy();
        return 1;
    }

    SILK_INFO("listening on {}:{}", std::string_view(host), port);

    // Block on the accept loop fiber.
    FiberScheduler::run(AcceptFiberMain, TAcceptParams{context.get()});

    FiberScheduler::destroy();
    silk::destroy();
    return 0;
}
