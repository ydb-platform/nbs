#include "client.h"

#include <silk/fibers/fiber.h>

#include <util/generic/string.h>

#include <arpa/inet.h>
#include <netinet/in.h>
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
    // Use ThreadModeScope so blocking socket calls don't block
    // the silk scheduler thread.
    FiberScheduler::ThreadModeScope threadMode;

    int fd = Connect();
    Y_ABORT_UNLESS(fd >= 0, "connect failed");

    TString reqBuf;
    Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&reqBuf);

    ui32 lenBe = htonl(static_cast<ui32>(reqBuf.size()));
    SendAll(fd, &lenBe, sizeof(lenBe));
    SendAll(fd, reqBuf.data(), reqBuf.size());

    ui32 respLenBe = 0;
    RecvAll(fd, &respLenBe, sizeof(respLenBe));
    ui32 respLen = ntohl(respLenBe);

    TString respBuf;
    respBuf.ReserveAndResize(respLen);
    RecvAll(fd, respBuf.begin(), respLen);

    TResponse resp;
    Y_PROTOBUF_SUPPRESS_NODISCARD resp.ParseFromString(respBuf);
    ::close(fd);
    return resp;
}

int TClient::Connect()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(Port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (::connect(
            fd,
            reinterpret_cast<sockaddr*>(&addr),
            sizeof(addr)) < 0)
    {
        ::close(fd);
        return -1;
    }
    return fd;
}

void TClient::RecvAll(int fd, void* buf, size_t len)
{
    auto* p = static_cast<ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::recv(fd, p + done, len - done, 0);
        if (n <= 0) {
            break;
        }
        done += static_cast<size_t>(n);
    }
}

void TClient::SendAll(int fd, const void* buf, size_t len)
{
    const auto* p = static_cast<const ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::send(fd, p + done, len - done, MSG_NOSIGNAL);
        if (n <= 0) {
            break;
        }
        done += static_cast<size_t>(n);
    }
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
