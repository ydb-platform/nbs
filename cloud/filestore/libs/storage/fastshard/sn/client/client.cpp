#include "client.h"

#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/mutex.h>
#include <silk/util/logger.h>

#include <util/generic/scope.h>
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

namespace NCloud::NFileStore::NStorage::NFastShard {

using silk::FiberMutex;
using silk::FiberScheduler;

using NCloud::NProto::TDeviceProtocolRequest;
using NCloud::NProto::TDeviceProtocolResponse;

namespace {

////////////////////////////////////////////////////////////////////////////////
// TCP connect from within a fiber. Returns fd on success, -1 on failure.

int OpenTcp(const TString& host, ui16 port)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    char portStr[6];
    ui32 printed = snprintf(portStr, sizeof(portStr), "%d", port);
    if (printed >= sizeof(portStr)) {
        return -1;
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
        return -1;
    }
    Y_DEFER
    {
        ::freeaddrinfo(res);
    };

    for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
        int fd = ::socket(
            ai->ai_family,
            ai->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
            ai->ai_protocol);
        if (fd < 0) {
            continue;
        }

        int ret = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (ret < 0 && errno != EINPROGRESS) {
            ::close(fd);
            continue;
        }
        if (ret < 0) {
            if (FiberScheduler::poll(fd, POLLOUT)) {
                ::close(fd);
                continue;
            }
            int err = 0;
            socklen_t errLen = sizeof(err);
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errLen);
            if (err) {
                ::close(fd);
                continue;
            }
        }

        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        return fd;
    }
    return -1;
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

    TPooledConnection Acquire()
    {
        {
            std::lock_guard g(Lock);
            if (!Idle.empty()) {
                TPooledConnection conn = Idle.back();
                Idle.pop_back();
                return conn;
            }
        }

        //
        // Connect outside the lock: OpenTcp suspends the fiber, and
        // holding a FiberMutex across it would serialize all concurrent
        // connection attempts.
        //

        int fd = OpenTcp(Host, Port);
        if (fd >= 0 && Metrics) {
            Metrics->ConnectionsCreated.fetch_add(1);
        }
        return {.Fd = fd, .Used = false};
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
        NCloud::NProto::T##name##Request request) override                     \
    {                                                                          \
        TDeviceProtocolRequest wire;                                           \
        wire.SetRequestId(NextRequestId.fetch_add(1));                         \
        *wire.Mutable##name() = std::move(request);                            \
                                                                               \
        TDeviceProtocolResponse resp = Exchange(wire);                         \
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
    TDeviceProtocolResponse Exchange(const TDeviceProtocolRequest& req)
    {
        //
        // The connection is used by this request exclusively, so the
        // request/response exchange needs no locking. On success the
        // connection returns to the pool; on any error it is closed and
        // dropped — the next request opens a fresh one.
        //

        TPooledConnection conn = Pool.Acquire();
        if (conn.Fd < 0) {
            return MakeErrorResponse(
                req.GetRequestId(),
                E_REJECTED,
                TStringBuilder() << "sn client: connect to " << Host << ":"
                                 << Port << " failed");
        }
        const int fd = conn.Fd;

        TString buf;
        Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&buf);

        ui32 lenBe = htonl(static_cast<ui32>(buf.size()));
        if (int r = SendAll(fd, &lenBe, sizeof(lenBe)); r) {
            return CloseAndError(fd, req.GetRequestId(), r, "send length");
        }
        if (int r = SendAll(fd, buf.data(), buf.size()); r) {
            return CloseAndError(fd, req.GetRequestId(), r, "send body");
        }

        ui32 respLenBe = 0;
        if (int r = RecvAll(fd, &respLenBe, sizeof(respLenBe)); r) {
            return CloseAndError(fd, req.GetRequestId(), r, "recv length");
        }
        ui32 respLen = ntohl(respLenBe);

        TString respBuf;
        respBuf.ReserveAndResize(respLen);
        if (int r = RecvAll(fd, respBuf.begin(), respLen); r) {
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
        SILK_WARN("sn client fd=%d: %s: %s", fd, op, ::strerror(err));
        ::close(fd);
        return MakeErrorResponse(
            requestId,
            E_REJECTED,
            TStringBuilder() << "sn client: " << op << ": " << ::strerror(err));
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

}   // namespace NCloud::NFileStore::NStorage::NFastShard
