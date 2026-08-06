#include "client.h"

#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/mutex.h>
#include <silk/util/logger.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
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
    if (::getaddrinfo(host.c_str(), portStr, &hints, &res) != 0) {
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
// IStorageNode over a single TCP connection to an sn server.

class TStorageNodeClient: public IStorageNode
{
public:
    TStorageNodeClient(TString host, ui16 port)
        : Host(std::move(host))
        , Port(port)
    {}

    ~TStorageNodeClient() override
    {
        if (Fd >= 0) {
            ::close(Fd);
        }
    }

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
        std::lock_guard g(ConnMutex);

        //
        // Open the socket lazily and reopen it after any I/O error.
        // Every failure path below closes Fd and sets it to -1 so the
        // next call retries the connect.
        //

        if (Fd < 0) {
            Fd = OpenTcp(Host, Port);
            if (Fd < 0) {
                return MakeErrorResponse(
                    req.GetRequestId(),
                    E_REJECTED,
                    TStringBuilder() << "sn client: connect to " << Host << ":"
                                     << Port << " failed");
            }
        }

        TString buf;
        Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&buf);

        ui32 lenBe = htonl(static_cast<ui32>(buf.size()));
        if (int r = SendAll(Fd, &lenBe, sizeof(lenBe)); r) {
            return CloseAndError(req.GetRequestId(), r, "send length");
        }
        if (int r = SendAll(Fd, buf.data(), buf.size()); r) {
            return CloseAndError(req.GetRequestId(), r, "send body");
        }

        ui32 respLenBe = 0;
        if (int r = RecvAll(Fd, &respLenBe, sizeof(respLenBe)); r) {
            return CloseAndError(req.GetRequestId(), r, "recv length");
        }
        ui32 respLen = ntohl(respLenBe);

        TString respBuf;
        respBuf.ReserveAndResize(respLen);
        if (int r = RecvAll(Fd, respBuf.begin(), respLen); r) {
            return CloseAndError(req.GetRequestId(), r, "recv body");
        }

        TDeviceProtocolResponse resp;
        if (!resp.ParseFromString(respBuf)) {
            return CloseAndError(req.GetRequestId(), EBADMSG, "parse response");
        }
        return resp;
    }

    TDeviceProtocolResponse
    CloseAndError(ui64 requestId, int err, const char* op)
    {
        SILK_WARN("sn client fd=%d: %s: %s", Fd, op, ::strerror(err));
        ::close(Fd);
        Fd = -1;
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
    FiberMutex ConnMutex;
    int Fd = -1;
    std::atomic<ui64> NextRequestId{1};
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateStorageNodeClient(TString host, ui16 port)
{
    return std::make_shared<TStorageNodeClient>(std::move(host), port);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
