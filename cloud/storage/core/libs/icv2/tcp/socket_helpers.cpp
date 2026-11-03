#include "socket_helpers.h"

#include <util/string/builder.h>
#include <util/system/error.h>

#include <netinet/tcp.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeSocketError(int error, const char* operation)
{
    return MakeError(
        MAKE_SYSTEM_ERROR(error),
        TStringBuilder() << operation << ": " << LastSystemErrorText(error));
}

TSocketResult PrepareSocket(TSocketPtr socket)
{
    const int fd = *socket;
    const int enabled = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) < 0)
    {
        return MakeSocketError(errno, "set TCP_NODELAY");
    }
    return socket;
}

TSocketResult CreateSocket(const NInterconnect::TAddress& address)
{
    if (address.GetFamily() != AF_INET && address.GetFamily() != AF_INET6) {
        return MakeError(E_ARGUMENT, "expected an IPv4 or IPv6 TCP address");
    }
    const int fd = ::socket(
        address.GetFamily(),
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0);
    if (fd < 0) {
        return MakeSocketError(errno, "socket");
    }
    return PrepareSocket(MakeIntrusive<NInterconnect::TStreamSocket>(fd));
}

int ConnectSocket(
    const NInterconnect::TStreamSocket& socket,
    const NInterconnect::TAddress& address)
{
    return -socket.Connect(address);
}

}   // namespace NCloud::NIcV2
