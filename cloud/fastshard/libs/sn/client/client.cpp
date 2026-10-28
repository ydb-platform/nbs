#include "client.h"

#include <cloud/fastshard/libs/common/context.h>
#include <cloud/fastshard/libs/common/error.h>
#include <cloud/fastshard/libs/ipc/ipc.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/event.h>
#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/mutex.h>
#include <silk/util/logger.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/system/datetime.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <memory>
#include <mutex>

namespace NCloud::NFastShard {

using silk::FiberFuture;
using silk::FiberMutex;
using silk::FiberScheduler;

using NCloud::NProto::TDeviceProtocolRequest;
using NCloud::NProto::TDeviceProtocolResponse;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Polls the fd for the events, but gives up once the context is cancelled.

class TContextPoller final: public IPoller
{
public:
    TContextPoller(TFastShardContext& ctx)
    {
        ctx.Cancel.wait(&Cancelled);
    }

    ~TContextPoller()
    {
        Cancelled.cancel();
        Y_UNUSED(Cancelled.wait());
    }

    int Poll(int fd, uint32_t events) override
    {
        FiberScheduler::IoFuture io;
        FiberScheduler::poll(fd, events, nullptr, &io);

        FiberFuture* futures[] = {&io, &Cancelled};
        FiberFuture::waitForMultiple(futures, 2, &WaitCycles);

        int r = 0;
        if (io.isSet(&r)) {
            return r;
        }

        // Cancelled: take the poll back before its future goes.
        io.cancel();
        Y_UNUSED(io.wait(&WaitCycles));
        return ECANCELED;
    }

    ui64 GetWaitCycles() const
    {
        return WaitCycles;
    }

private:
    silk::FiberEvent::Future Cancelled;
    ui64 WaitCycles = 0;
};

////////////////////////////////////////////////////////////////////////////////

int OpenTcp(const TString& host, ui16 port, IPoller& poller, int* result)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    char portStr[6];
    ui32 printed = snprintf(portStr, sizeof(portStr), "%d", port);
    if (printed >= sizeof(portStr)) {
        return EINVAL;
    }

    addrinfo* res = nullptr;
    int gai = 0;
    {
        //
        // getaddrinfo blocks inside libc (DNS, nsswitch). Run it on the
        // thread-mode worker pool: on a scheduler thread it would stall
        // every fiber homed on this CPU for the whole resolution.
        //

        FiberScheduler::ThreadModeScope scope;
        gai = ::getaddrinfo(host.c_str(), portStr, &hints, &res);
    }
    if (gai != 0) {
        return EHOSTUNREACH;
    }
    Y_DEFER
    {
        ::freeaddrinfo(res);
    };

    int err = EHOSTUNREACH;
    for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
        int fd = ::socket(
            ai->ai_family,
            ai->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
            ai->ai_protocol);
        if (fd < 0) {
            err = errno;
            continue;
        }

        int ret = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (ret < 0 && errno != EINPROGRESS) {
            err = errno;
            ::close(fd);
            continue;
        }
        if (ret < 0) {
            if (int r = poller.Poll(fd, POLLOUT); r) {
                ::close(fd);
                if (r == ECANCELED) {
                    return r;
                }
                err = r;
                continue;
            }
            err = 0;
            socklen_t errLen = sizeof(err);
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errLen);
            if (err) {
                ::close(fd);
                continue;
            }
        }

        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        *result = fd;
        return 0;
    }
    return err;
}

////////////////////////////////////////////////////////////////////////////////
// Pool of idle TCP connections to one host:port. Acquire pops an idle
// connection or opens a new one; a request uses the connection
// exclusively until it releases it back. On an I/O error the caller
// closes the connection instead of releasing it, so the pool never
// stores a connection in an unknown protocol state. The pool is not
// capped: it grows to the maximum number of concurrent requests
// observed.

struct TPooledConnection
{
    int Fd = -1;

    // Whether this connection has completed at least one request;
    // drives the ConnectionsUsed metric.
    bool Used = false;
};

class TConnectionPool
{
public:
    TConnectionPool(
            TString host,
            ui16 port,
            TStorageNodeClientMetricsPtr metrics)
        : Host(std::move(host))
        , Port(port)
        , Metrics(std::move(metrics))
    {}

    ~TConnectionPool()
    {
        for (const auto& conn: Idle) {
            ::close(conn.Fd);
        }
    }

    int Acquire(IPoller& poller, TPooledConnection* conn)
    {
        {
            std::lock_guard g(Lock);
            if (!Idle.empty()) {
                *conn = Idle.back();
                Idle.pop_back();
                return 0;
            }
        }

        //
        // Connect outside the lock: OpenTcp suspends the fiber, and
        // holding a FiberMutex across it would serialize all concurrent
        // connection attempts.
        //

        const TInstant started = TInstant::Now();

        int fd = -1;
        int r = OpenTcp(Host, Port, poller, &fd);
        if (Metrics) {
            Metrics->ConnectTimeUs.fetch_add(
                (TInstant::Now() - started).MicroSeconds());

            if (r == 0) {
                Metrics->ConnectionsCreated.fetch_add(1);
            }
        }

        *conn = {.Fd = fd, .Used = false};
        return r;
    }

    void Release(TPooledConnection conn)
    {
        std::lock_guard g(Lock);
        Idle.push_back(conn);
    }

private:
    const TString Host;
    const ui16 Port;
    const TStorageNodeClientMetricsPtr Metrics;
    FiberMutex Lock;
    TVector<TPooledConnection> Idle;
};

////////////////////////////////////////////////////////////////////////////////
// IStorageNode over a pool of TCP connections to an sn server.

class TStorageNodeClient: public IStorageNode
{
public:
    TStorageNodeClient(
            TString host,
            ui16 port,
            TStorageNodeClientMetricsPtr metrics)
        : Host(std::move(host))
        , Port(port)
        , Metrics(std::move(metrics))
        , Pool(Host, Port, Metrics)
    {}

#define SN_CLIENT_METHOD(name, ...)                                            \
    NCloud::NProto::T##name##Response name(                                    \
        NCloud::NProto::T##name##Request request,                              \
        TFastShardContext& ctx) override                                       \
    {                                                                          \
        TDeviceProtocolRequest wire;                                           \
        wire.SetRequestId(NextRequestId.fetch_add(1));                         \
        *wire.Mutable##name() = std::move(request);                            \
                                                                               \
        TDeviceProtocolResponse resp = Exchange(wire, ctx);                    \
                                                                               \
        NCloud::NProto::T##name##Response out;                                 \
        if (resp.HasProtocolError()) {                                         \
            *out.MutableError() = std::move(*resp.MutableProtocolError());     \
        } else if (resp.Has##name()) {                                         \
            out = std::move(*resp.Mutable##name());                            \
        } else {                                                               \
            *out.MutableError() = MakeError(                                   \
                E_REJECTED,                                                    \
                "sn client: response missing " #name " case");                 \
        }                                                                      \
        return out;                                                            \
    }                                                                          \
    // SN_CLIENT_METHOD

    SN_METHODS(SN_CLIENT_METHOD)

#undef SN_CLIENT_METHOD

private:
    TDeviceProtocolResponse Exchange(
        const TDeviceProtocolRequest& req,
        TFastShardContext& ctx)
    {
        const ui64 started = GetCycleCount();

        TContextPoller poller(ctx);
        auto response = DoExchange(req, poller);

        const ui64 waited = poller.GetWaitCycles();
        ctx.ExecCycles += GetCycleCount() - started - waited;
        ctx.WaitCycles += waited;
        return response;
    }

    TDeviceProtocolResponse DoExchange(
        const TDeviceProtocolRequest& req,
        IPoller& poller)
    {
        //
        // The connection is used by this request exclusively, so the
        // request/response exchange needs no locking. On success the
        // connection returns to the pool; on any error it is closed and
        // dropped — the next request opens a fresh one. A cancelled exchange
        // leaves its response in flight, so we cannot reuse the connection.
        //
        // TODO(#5895): keep the connection once it is multiplexed.
        //

        TPooledConnection conn;
        if (int r = Pool.Acquire(poller, &conn); r) {
            return MakeErrorResponse(
                req.GetRequestId(),
                E_REJECTED,
                TStringBuilder() << "sn client: connect to " << Host << ":"
                    << Port << ": " << SystemErrorText(r));
        }

        const int fd = conn.Fd;

        TString buf;
        Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&buf);

        ui32 lenBe = htonl(static_cast<ui32>(buf.size()));
        if (int r = SendAll(fd, &lenBe, sizeof(lenBe), poller); r) {
            return CloseAndError(fd, req.GetRequestId(), r, "send length");
        }

        if (int r = SendAll(fd, buf.data(), buf.size(), poller); r) {
            return CloseAndError(fd, req.GetRequestId(), r, "send body");
        }

        ui32 respLenBe = 0;
        if (int r = RecvAll(fd, &respLenBe, sizeof(respLenBe), poller); r) {
            return CloseAndError(fd, req.GetRequestId(), r, "recv length");
        }

        ui32 respLen = ntohl(respLenBe);

        TString respBuf;
        respBuf.ReserveAndResize(respLen);
        if (int r = RecvAll(fd, respBuf.begin(), respLen, poller); r) {
            return CloseAndError(fd, req.GetRequestId(), r, "recv body");
        }

        TDeviceProtocolResponse resp;
        if (!resp.ParseFromString(respBuf)) {
            return CloseAndError(
                fd,
                req.GetRequestId(),
                EBADMSG,
                "parse response");
        }

        if (Metrics) {
            Metrics->RequestsCompleted.fetch_add(1);
            if (!conn.Used) {
                Metrics->ConnectionsUsed.fetch_add(1);
            }
        }

        conn.Used = true;

        Pool.Release(conn);
        return resp;
    }

    TDeviceProtocolResponse
    CloseAndError(int fd, ui64 requestId, int err, const char* op)
    {
        SILK_WARN("sn client fd=%d: %s: %s", fd, op, SystemErrorText(err).c_str());
        ::close(fd);
        return MakeErrorResponse(
            requestId,
            E_REJECTED,
            TStringBuilder() << "sn client: " << op << ": " << SystemErrorText(err));
    }

    static TDeviceProtocolResponse
    MakeErrorResponse(ui64 requestId, ui32 code, const TString& msg)
    {
        TDeviceProtocolResponse resp;
        resp.SetRequestId(requestId);
        auto* err = resp.MutableProtocolError();
        err->SetCode(code);
        err->SetMessage(msg);
        return resp;
    }

private:
    const TString Host;
    const ui16 Port;
    const TStorageNodeClientMetricsPtr Metrics;
    TConnectionPool Pool;
    std::atomic<ui64> NextRequestId{1};
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateStorageNodeClient(TString host, ui16 port)
{
    return CreateStorageNodeClient(std::move(host), port, nullptr /* metrics */);
}

IStorageNodePtr CreateStorageNodeClient(
    TString host,
    ui16 port,
    TStorageNodeClientMetricsPtr metrics)
{
    return std::make_shared<TStorageNodeClient>(
        std::move(host),
        port,
        std::move(metrics));
}

}   // namespace NCloud::NFastShard
