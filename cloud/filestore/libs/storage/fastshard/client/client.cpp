#include "client.h"

#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>

#include <silk/fibers/fiber.h>

#include <util/generic/string.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using silk::FiberScheduler;
using namespace NProtoSrv;

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(ui16 port)
    : Port(port)
{}

TResponse TClient::Send(const TRequest& req)
{
    int fd = Connect();
    Y_ABORT_UNLESS(fd >= 0, "connect failed");

    TString reqBuf;
    Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&reqBuf);

    ui32 lenBe = htonl(static_cast<ui32>(reqBuf.size()));
    int r = SendAll(fd, &lenBe, sizeof(lenBe));
    Y_ABORT_UNLESS(r == 0, "send length failed");
    r = SendAll(fd, reqBuf.data(), reqBuf.size());
    Y_ABORT_UNLESS(r == 0, "send body failed");

    ui32 respLenBe = 0;
    r = RecvAll(fd, &respLenBe, sizeof(respLenBe));
    Y_ABORT_UNLESS(r == 0, "recv length failed");
    ui32 respLen = ntohl(respLenBe);

    TString respBuf;
    respBuf.ReserveAndResize(respLen);
    r = RecvAll(fd, respBuf.begin(), respLen);
    Y_ABORT_UNLESS(r == 0, "recv body failed");

    TResponse resp;
    Y_PROTOBUF_SUPPRESS_NODISCARD resp.ParseFromString(respBuf);
    ::close(fd);
    return resp;
}

int TClient::Connect()
{
    int fd = ::socket(
        AF_INET,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0);
    if (fd < 0) {
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(Port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    int ret = ::connect(
        fd,
        reinterpret_cast<sockaddr*>(&addr),
        sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        ::close(fd);
        return -1;
    }

    if (ret < 0) {
        // EINPROGRESS — wait for connection to complete.
        int r = FiberScheduler::poll(fd, POLLOUT);
        if (r) {
            ::close(fd);
            return -1;
        }

        // Check for connect error.
        int err = 0;
        socklen_t errLen = sizeof(err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errLen);
        if (err) {
            ::close(fd);
            return -1;
        }
    }

    int one = 1;
    ::setsockopt(
        fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    return fd;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
