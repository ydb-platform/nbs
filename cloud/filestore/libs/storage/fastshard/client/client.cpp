#include "client.h"

#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using silk::FiberScheduler;
using namespace NProtoSrv;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint: IEndpoint
{
    int Fd;
    bool Broken = false;

    explicit TEndpoint(int fd)
        : Fd(fd)
    {}

    ~TEndpoint()
    {
        ::close(Fd);
    }

    TResponse Send(const TRequest& req) override
    {
        if (Broken) {
            return ErrorResponse("endpoint is broken", 0 /* err */);
        }

        TString reqBuf;
        Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&reqBuf);

        ui32 lenBe = htonl(static_cast<ui32>(reqBuf.size()));
        int r = SendAll(Fd, &lenBe, sizeof(lenBe));
        if (r != 0) {
            return BreakEndpoint("send length failed", r);
        }

        r = SendAll(Fd, reqBuf.data(), reqBuf.size());
        if (r != 0) {
            return BreakEndpoint("send body failed", r);
        }

        ui32 respLenBe = 0;
        r = RecvAll(Fd, &respLenBe, sizeof(respLenBe));
        if (r != 0) {
            return BreakEndpoint("recv length failed", r);
        }
        ui32 respLen = ntohl(respLenBe);

        TString respBuf;
        respBuf.ReserveAndResize(respLen);
        r = RecvAll(Fd, respBuf.begin(), respLen);
        if (r != 0) {
            return BreakEndpoint("recv body failed", r);
        }

        TResponse resp;
        Y_PROTOBUF_SUPPRESS_NODISCARD resp.ParseFromString(respBuf);
        return resp;
    }

private:
    TResponse BreakEndpoint(const char* op, int err)
    {
        //
        // A partial send/recv loses the message framing, so no other request
        // may reuse this connection - all subsequent Send calls fail fast.
        //

        Broken = true;
        return ErrorResponse(op, err);
    }

    static TResponse ErrorResponse(const char* op, int err)
    {
        TResponse resp;
        *resp.MutableError() = MakeError(
            E_UNAVAILABLE,
            TStringBuilder() << op << ": " << err);
        return resp;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IEndpoint> TClient::Connect(const TString& host, ui16 port)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    char portStr[6];
    ui32 printed = snprintf(portStr, sizeof(portStr), "%d", port);
    Y_ABORT_UNLESS(printed < sizeof(portStr), "printed=%u", printed);

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
        return nullptr;
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
            // EINPROGRESS — wait for connection to complete.
            int r = FiberScheduler::poll(fd, POLLOUT);
            if (r) {
                ::close(fd);
                continue;
            }

            // Check for connect error.
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

        return std::make_shared<TEndpoint>(fd);
    }

    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
