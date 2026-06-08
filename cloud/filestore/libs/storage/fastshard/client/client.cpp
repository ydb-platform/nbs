#include "client.h"

#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>

#include <silk/fibers/fiber.h>

#include <util/generic/string.h>

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

    explicit TEndpoint(int fd)
        : Fd(fd)
    {}

    ~TEndpoint()
    {
        ::close(Fd);
    }

    TResponse Send(const TRequest& req) override
    {
        TString reqBuf;
        Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&reqBuf);

        ui32 lenBe = htonl(static_cast<ui32>(reqBuf.size()));
        int r = SendAll(Fd, &lenBe, sizeof(lenBe));
        Y_ABORT_UNLESS(r == 0, "send length failed: %d", r);
        r = SendAll(Fd, reqBuf.data(), reqBuf.size());
        Y_ABORT_UNLESS(r == 0, "send body failed: %d", r);

        ui32 respLenBe = 0;
        r = RecvAll(Fd, &respLenBe, sizeof(respLenBe));
        Y_ABORT_UNLESS(r == 0, "recv length failed: %d", r);
        ui32 respLen = ntohl(respLenBe);

        TString respBuf;
        respBuf.ReserveAndResize(respLen);
        r = RecvAll(Fd, respBuf.begin(), respLen);
        Y_ABORT_UNLESS(r == 0, "recv body failed: %d", r);

        TResponse resp;
        Y_PROTOBUF_SUPPRESS_NODISCARD resp.ParseFromString(respBuf);
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
    int gai = ::getaddrinfo(host.c_str(), portStr, &hints, &res);
    if (gai != 0) {
        return nullptr;
    }

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
