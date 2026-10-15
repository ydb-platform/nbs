#include "socket.h"

#include <util/network/socket.h>
#include <util/string/builder.h>
#include <util/system/error.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <optional>

namespace NCloud::NBlockStore::NUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
NProto::TError SetOpt(int fd, int level, int name, T value, TStringBuf what)
{
    if (::setsockopt(fd, level, name, &value, sizeof(value)) < 0) {
        return MakeSystemError(errno, what);
    }
    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeSystemError(int code, TStringBuf message)
{
    return MakeError(
        MAKE_SYSTEM_ERROR(code),
        TStringBuilder() << "(" << LastSystemErrorText(code) << ") "
                         << message);
}

NProto::TError ApplySocketOptions(int fd, const TSocketOptions& options)
{
    if (auto e = SetOpt<int>(fd, IPPROTO_TCP, TCP_NODELAY, 1, "TCP_NODELAY");
        HasError(e))
    {
        return e;
    }
    if (auto e = SetOpt<int>(fd, SOL_SOCKET, SO_KEEPALIVE, 1, "SO_KEEPALIVE");
        HasError(e))
    {
        return e;
    }
    if (options.KeepAliveIdle) {
        if (auto e = SetOpt<int>(
                fd,
                IPPROTO_TCP,
                TCP_KEEPIDLE,
                options.KeepAliveIdle.Seconds(),
                "TCP_KEEPIDLE");
            HasError(e))
        {
            return e;
        }
    }
    if (options.KeepAliveInterval) {
        if (auto e = SetOpt<int>(
                fd,
                IPPROTO_TCP,
                TCP_KEEPINTVL,
                options.KeepAliveInterval.Seconds(),
                "TCP_KEEPINTVL");
            HasError(e))
        {
            return e;
        }
    }
    if (options.KeepAliveCount) {
        if (auto e = SetOpt<int>(
                fd,
                IPPROTO_TCP,
                TCP_KEEPCNT,
                options.KeepAliveCount,
                "TCP_KEEPCNT");
            HasError(e))
        {
            return e;
        }
    }
    if (options.TcpUserTimeout) {
        if (auto e = SetOpt<unsigned>(
                fd,
                IPPROTO_TCP,
                TCP_USER_TIMEOUT,
                options.TcpUserTimeout.MilliSeconds(),
                "TCP_USER_TIMEOUT");
            HasError(e))
        {
            return e;
        }
    }
    return {};
}

TResultOrError<int>
CreateListenSocket(const TString& host, ui16 port, ui32 backlog)
{
    std::optional<TNetworkAddress> address;
    try {
        address.emplace(host, port, AI_PASSIVE);
    } catch (...) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "cannot resolve " << host << ": "
                             << CurrentExceptionMessage());
    }

    NProto::TError last = MakeError(E_FAIL, "no addresses");
    for (auto it = address->Begin(); it != address->End(); ++it) {
        const addrinfo& ai = *it;
        int fd = ::socket(ai.ai_family, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            last = MakeSystemError(errno, "socket");
            continue;
        }
        if (auto e =
                SetOpt<int>(fd, SOL_SOCKET, SO_REUSEADDR, 1, "SO_REUSEADDR");
            HasError(e))
        {
            last = e;
            CloseSocket(fd);
            continue;
        }
        if (::bind(fd, ai.ai_addr, ai.ai_addrlen) < 0) {
            last = MakeSystemError(errno, "bind");
            CloseSocket(fd);
            continue;
        }
        if (::listen(fd, backlog) < 0) {
            last = MakeSystemError(errno, "listen");
            CloseSocket(fd);
            continue;
        }
        return fd;
    }
    return last;
}

TResultOrError<ui16> GetLocalPort(int fd)
{
    sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        return MakeSystemError(errno, "getsockname");
    }
    if (addr.ss_family == AF_INET) {
        return ntohs(reinterpret_cast<sockaddr_in*>(&addr)->sin_port);
    }
    return ntohs(reinterpret_cast<sockaddr_in6*>(&addr)->sin6_port);
}

void CloseSocket(int fd)
{
    if (fd >= 0) {
        ::close(fd);
    }
}

void CloseWithReset(int fd)
{
    if (fd < 0) {
        return;
    }
    linger l{.l_onoff = 1, .l_linger = 0};
    ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));
    ::close(fd);
}

}   // namespace NCloud::NBlockStore::NUring
