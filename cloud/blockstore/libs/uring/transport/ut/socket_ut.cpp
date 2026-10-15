#include <cloud/blockstore/libs/uring/transport/socket.h>

#include <library/cpp/testing/unittest/registar.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <cerrno>

namespace NCloud::NBlockStore::NUring {

Y_UNIT_TEST_SUITE(TUringSocketTest)
{
    Y_UNIT_TEST(ShouldListenOnEphemeralPort)
    {
        auto [fd, error] = CreateListenSocket("127.0.0.1", 0, 16);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto [port, perr] = GetLocalPort(fd);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));
        UNIT_ASSERT(port != 0);
        CloseSocket(fd);
    }

    Y_UNIT_TEST(ShouldApplyOptions)
    {
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(fd >= 0);
        TSocketOptions opts{
            .TcpUserTimeout = TDuration::Seconds(7),
            .KeepAliveIdle = TDuration::Seconds(3),
            .KeepAliveInterval = TDuration::Seconds(2),
            .KeepAliveCount = 4,
        };
        auto error = ApplySocketOptions(fd, opts);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        int value = 0;
        socklen_t len = sizeof(value);
        UNIT_ASSERT(
            ::getsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &value, &len) == 0);
        UNIT_ASSERT_VALUES_EQUAL(7000, value);
        UNIT_ASSERT(
            ::getsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &value, &len) == 0);
        UNIT_ASSERT_VALUES_EQUAL(3, value);
        UNIT_ASSERT(
            ::getsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &value, &len) == 0);
        UNIT_ASSERT_VALUES_EQUAL(2, value);
        UNIT_ASSERT(
            ::getsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &value, &len) == 0);
        UNIT_ASSERT_VALUES_EQUAL(4, value);
        UNIT_ASSERT(
            ::getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, &len) == 0);
        UNIT_ASSERT_VALUES_EQUAL(1, value);
        CloseSocket(fd);
    }

    Y_UNIT_TEST(ShouldFailOnBadHost)
    {
        auto [fd, error] = CreateListenSocket("no.such.host.invalid", 0, 16);
        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST(ShouldReportBindFailureAsPositiveErrno)
    {
        auto [fd, error] = CreateListenSocket("127.0.0.1", 0, 16);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto [port, perr] = GetLocalPort(fd);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));

        auto [fd2, error2] = CreateListenSocket("127.0.0.1", port, 16);
        UNIT_ASSERT_VALUES_EQUAL(
            MAKE_SYSTEM_ERROR(EADDRINUSE),
            error2.GetCode());
        CloseSocket(fd);
    }

    Y_UNIT_TEST(ShouldAcceptOnReportedPort)
    {
        auto [fd, error] = CreateListenSocket("127.0.0.1", 0, 16);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto [port, perr] = GetLocalPort(fd);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));

        int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(client >= 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        UNIT_ASSERT(
            ::connect(
                client,
                reinterpret_cast<sockaddr*>(&addr),
                sizeof(addr)) == 0);
        int accepted = ::accept4(fd, nullptr, nullptr, SOCK_CLOEXEC);
        UNIT_ASSERT(accepted >= 0);

        CloseSocket(accepted);
        CloseSocket(client);
        CloseSocket(fd);
    }
}

}   // namespace NCloud::NBlockStore::NUring
