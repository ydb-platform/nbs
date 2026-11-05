#include "handshake.h"

#include <cloud/blockstore/libs/icv2/transport/protos/handshake.pb.h>

#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/system/error.h>

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/resource.h>
#include <unistd.h>

#include <future>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::TSocketPtr;

using NCloud::NIcV2::IConnectorPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NIcV2::NTests;
using namespace std::chrono_literals;

TString MakeHello(
    bool xdc,
    bool auxiliary,
    const TString& pairId = {},
    ui32 version = 1)
{
    NHandshakeProto::THandshakeHello hello;
    hello.SetProtocolVersion(version);
    hello.SetEnableXdc(xdc);
    hello.SetAuxiliary(auxiliary);
    if (xdc) {
        hello.SetPairId(pairId);
    }
    TString payload;
    Y_PROTOBUF_SUPPRESS_NODISCARD hello.SerializeToString(&payload);
    TString result = "ICV2";
    result.push_back(static_cast<char>(payload.size() >> 8));
    result.push_back(static_cast<char>(payload.size() & 0xff));
    result.append(payload);
    return result;
}

const TString ClientHello = MakeHello(false, false);
const TString ServerHello = ClientHello;

TAcceptHandshakeResult WaitHello(const TAcceptHandshakeFuture& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.GetValue();
}

std::future<void> SubscribeBlocking(
    TAcceptHandshakeFuture future,
    NThreading::TPromise<void> entered,
    NThreading::TPromise<void> release,
    IHandshakeProvider* stop)
{
    return std::async(
        std::launch::async,
        [future, entered, release, stop]() mutable
        {
            future.Subscribe(
                [entered, release, stop](const TAcceptHandshakeFuture&) mutable
                {
                    if (stop) {
                        stop->RequestStop();
                    }
                    entered.SetValue();
                    release.GetFuture().GetValueSync();
                });
        });
}

THandshakeReady GetConnection(const TAcceptHandshakeFuture& future)
{
    auto [connection, error] = WaitHello(future);
    UNIT_ASSERT_C(!HasError(error), FormatError(error));
    return std::get<THandshakeReady>(connection);
}

TAcceptHandshakeFuture
Connect(IHandshakeProvider& handshake, TSocketPtr socket, TDuration timeout)
{
    return handshake.Connect(std::move(socket), timeout)
        .Apply(
            [](const TConnectHandshakeFuture& future) -> TAcceptHandshakeResult
            {
                const auto& result = future.GetValue();
                if (HasError(result.GetError())) {
                    return result.GetError();
                }

                return TAcceptHandshakeOutcome{result.GetResult()};
            });
}

void CheckIncomplete(const TAcceptHandshakeFuture& future)
{
    const auto result = WaitHello(future);
    UNIT_ASSERT_C(!HasError(result.GetError()), FormatError(result.GetError()));
    UNIT_ASSERT(
        std::holds_alternative<THandshakeIncomplete>(result.GetResult()));
}

void SendBytes(const TSocketPtr& socket, TStringBuf bytes)
{
    while (!bytes.empty()) {
        pollfd writable{.fd = *socket, .events = POLLOUT, .revents = 0};
        UNIT_ASSERT_VALUES_EQUAL(poll(&writable, 1, 5000), 1);
        const ssize_t size =
            send(*socket, bytes.data(), bytes.size(), MSG_NOSIGNAL);
        UNIT_ASSERT_C(size > 0, errno);
        bytes.Skip(size);
    }
}

TString ReceiveBytes(const TSocketPtr& socket, size_t size)
{
    TString buffer(size, '\0');
    size_t offset = 0;
    while (offset < size) {
        pollfd readable{.fd = *socket, .events = POLLIN, .revents = 0};
        UNIT_ASSERT_VALUES_EQUAL(poll(&readable, 1, 5000), 1);
        const ssize_t received =
            recv(*socket, buffer.Detach() + offset, size - offset, 0);
        UNIT_ASSERT_C(received > 0, errno);
        offset += received;
    }
    return buffer;
}

struct TFixture
{
    IHandshakeProviderPtr Client;
    IHandshakeProviderPtr Server;

    explicit TFixture(bool clientXdc = false, bool serverXdc = false)
    {
        auto [client, clientError] = CreateHandshakeProvider({clientXdc});
        auto [server, serverError] = CreateHandshakeProvider({serverXdc});
        UNIT_ASSERT_C(!HasError(clientError), FormatError(clientError));
        UNIT_ASSERT_C(!HasError(serverError), FormatError(serverError));
        Client = std::move(client);
        Server = std::move(server);
        bool started = false;
        Y_DEFER
        {
            if (!started) {
                Client->Stop();
                Server->Stop();
            }
        };
        Client->Start();
        Server->Start();
        started = true;
    }

    ~TFixture()
    {
        Client->Stop();
        Server->Stop();
    }

    static std::array<TSocketPtr, 2> MakeSockets()
    {
        TTestListener listener;
        listener.Listener->Start();
        auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
        connector->Start();
        auto outgoing = WaitSocket(connector->Connect(
            listener.Listener->GetAddress(),
            TDuration::Seconds(5)));
        auto incoming = listener.Accept();
        connector->Stop();
        return {std::move(outgoing), std::move(incoming)};
    }
};

struct TXdcFixture: TFixture
{
    TTestListener Listener;
    IConnectorPtr Connector = NCloud::NIcV2::NTests::CreateTestConnector();

    TXdcFixture(bool clientXdc, bool serverXdc)
        : TFixture(clientXdc, serverXdc)
    {
        Listener.Listener->Start();
        Connector->Start();
    }

    ~TXdcFixture()
    {
        Connector->Stop();
    }

    TSocketPtr ConnectSocket()
    {
        return WaitSocket(Connector->Connect(
            Listener.Listener->GetAddress(),
            TDuration::Seconds(5)));
    }

    std::array<TAcceptHandshakeFuture, 2> Begin(
        TDuration timeout = TDuration::Seconds(5))
    {
        auto socket = ConnectSocket();
        auto accepted = Listener.Accept();
        return {
            Connect(*Client, std::move(socket), timeout),
            Server->Accept(std::move(accepted), timeout)};
    }

    TAcceptHandshakeFuture AcceptAuxiliary(
        TDuration timeout = TDuration::Seconds(5))
    {
        return Server->Accept(Listener.Accept(), timeout);
    }
};

TString XdcHello(bool auxiliary, char token)
{
    return MakeHello(true, auxiliary, TString(16, token));
}

NHandshakeProto::THandshakeHello ReceiveHello(const TSocketPtr& socket)
{
    const auto header = ReceiveBytes(socket, 6);
    const size_t size =
        (static_cast<ui8>(header[4]) << 8) | static_cast<ui8>(header[5]);
    const auto body = ReceiveBytes(socket, size);
    NHandshakeProto::THandshakeHello hello;
    UNIT_ASSERT(hello.ParseFromString(body));
    return hello;
}

void CheckXdcLocalAddress(const char* host, const char* localHost)
{
    TFixture fixture(true, true);
    TTestListener listener(host);
    listener.Listener->Start();
    const auto remote = listener.Listener->GetAddress();
    const NInterconnect::TAddress local(localHost, 0);
    const int fd = socket(local.GetFamily(), SOCK_STREAM | SOCK_CLOEXEC, 0);
    UNIT_ASSERT_C(fd >= 0, errno);
    auto socket = MakeIntrusive<NInterconnect::TStreamSocket>(fd);
    UNIT_ASSERT_VALUES_EQUAL(bind(fd, local.SockAddr(), local.Size()), 0);
    UNIT_ASSERT_VALUES_EQUAL(connect(fd, remote.SockAddr(), remote.Size()), 0);
    const int flags = fcntl(fd, F_GETFL);
    UNIT_ASSERT_C(flags >= 0, errno);
    UNIT_ASSERT_VALUES_EQUAL(fcntl(fd, F_SETFL, flags | O_NONBLOCK), 0);

    auto client =
        Connect(*fixture.Client, std::move(socket), TDuration::Seconds(5));
    auto server =
        fixture.Server->Accept(listener.Accept(), TDuration::Seconds(5));
    auto auxiliary =
        fixture.Server->Accept(listener.Accept(), TDuration::Seconds(5));
    CheckIncomplete(auxiliary);
    const auto clientConnection = GetConnection(client);
    const auto serverConnection = GetConnection(server);
    int noPort = 0;
    UNIT_ASSERT_VALUES_EQUAL(
        GetSockOpt(
            *clientConnection.XdcSocket,
            IPPROTO_IP,
            IP_BIND_ADDRESS_NO_PORT,
            noPort),
        0);
    UNIT_ASSERT_VALUES_EQUAL(noPort, 1);
    const auto mainAddress = clientConnection.Socket->GetSockName();
    const auto xdcAddress = clientConnection.XdcSocket->GetSockName();
    UNIT_ASSERT(std::holds_alternative<NInterconnect::TAddress>(mainAddress));
    UNIT_ASSERT(std::holds_alternative<NInterconnect::TAddress>(xdcAddress));
    const auto& main = std::get<NInterconnect::TAddress>(mainAddress);
    const auto& xdc = std::get<NInterconnect::TAddress>(xdcAddress);
    UNIT_ASSERT_VALUES_EQUAL(main.GetAddress(), local.GetAddress());
    UNIT_ASSERT_VALUES_EQUAL(xdc.GetAddress(), main.GetAddress());
    UNIT_ASSERT_VALUES_UNEQUAL(xdc.GetPort(), main.GetPort());
    SendBytes(clientConnection.XdcSocket, "xdc");
    UNIT_ASSERT_VALUES_EQUAL(
        ReceiveBytes(serverConnection.XdcSocket, 3),
        "xdc");
}

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2HandshakeTest)
{
    SIMPLE_UNIT_FORKED_TEST(ReturnsErrorWhenWakeupDescriptorsAreExhausted)
    {
        {
            TSystemError error(EMFILE);
            auto handshake = CreateHandshakeProvider({});
            UNIT_ASSERT(!HasError(handshake.GetError()));
        }

        rlimit original{};
        UNIT_ASSERT_VALUES_EQUAL(getrlimit(RLIMIT_NOFILE, &original), 0);
        auto limited = original;
        limited.rlim_cur = 0;
        UNIT_ASSERT_VALUES_EQUAL(setrlimit(RLIMIT_NOFILE, &limited), 0);
        Y_DEFER
        {
            setrlimit(RLIMIT_NOFILE, &original);
        };

        auto result = CreateHandshakeProvider({});
        UNIT_ASSERT_VALUES_EQUAL(setrlimit(RLIMIT_NOFILE, &original), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            result.GetError().GetCode(),
            MAKE_SYSTEM_ERROR(EMFILE));
        UNIT_ASSERT(!result.GetResult());
    }

    Y_UNIT_TEST(NegotiatesXdcOnTheSamePortOnlyWhenBothPeersEnableIt)
    {
        for (bool clientXdc: {false, true}) {
            for (bool serverXdc: {false, true}) {
                TXdcFixture fixture(clientXdc, serverXdc);
                auto futures = fixture.Begin();
                if (clientXdc && serverXdc) {
                    CheckIncomplete(fixture.AcceptAuxiliary());
                }
                auto client = GetConnection(futures[0]);
                auto server = GetConnection(futures[1]);
                UNIT_ASSERT_VALUES_EQUAL(
                    bool(client.XdcSocket),
                    clientXdc && serverXdc);
                UNIT_ASSERT_VALUES_EQUAL(
                    bool(server.XdcSocket),
                    clientXdc && serverXdc);
                SendBytes(client.Socket, "event");
                UNIT_ASSERT_VALUES_EQUAL(
                    ReceiveBytes(server.Socket, 5),
                    "event");
                if (client.XdcSocket) {
                    CheckSocket(client.XdcSocket);
                    CheckSocket(server.XdcSocket);
                    SendBytes(server.XdcSocket, "payload");
                    UNIT_ASSERT_VALUES_EQUAL(
                        ReceiveBytes(client.XdcSocket, 7),
                        "payload");
                }
            }
        }
    }

    Y_UNIT_TEST(PairsConcurrentConnectionsOfTheSameCells)
    {
        TXdcFixture fixture(true, true);
        std::array<std::array<TAcceptHandshakeFuture, 2>, 8> futures;
        // Submit all main sockets before processing any hello, so accepted
        // auxiliary sockets can be completed in the reverse order afterwards.
        std::array<TSocketPtr, 8> outgoing;
        std::array<TSocketPtr, 8> incoming;
        for (size_t i = 0; i < outgoing.size(); ++i) {
            outgoing[i] = fixture.ConnectSocket();
            incoming[i] = fixture.Listener.Accept();
        }
        for (size_t i = 0; i < outgoing.size(); ++i) {
            futures[i] = {
                Connect(
                    *fixture.Client,
                    std::move(outgoing[i]),
                    TDuration::Seconds(5)),
                fixture.Server->Accept(
                    std::move(incoming[i]),
                    TDuration::Seconds(5))};
        }
        std::array<TSocketPtr, 8> auxiliary;
        for (auto& socket: auxiliary) {
            socket = fixture.Listener.Accept();
        }
        for (size_t i = auxiliary.size(); i--;) {
            CheckIncomplete(fixture.Server->Accept(
                std::move(auxiliary[i]),
                TDuration::Seconds(5)));
        }
        for (size_t i = 0; i < futures.size(); ++i) {
            auto client = GetConnection(futures[i][0]);
            auto server = GetConnection(futures[i][1]);
            const TString bytes(1, static_cast<char>('a' + i));
            SendBytes(client.XdcSocket, bytes);
            UNIT_ASSERT_VALUES_EQUAL(ReceiveBytes(server.XdcSocket, 1), bytes);
        }
    }

    Y_UNIT_TEST(RejectsUnknownAndDuplicatePairIds)
    {
        TXdcFixture fixture(true, true);
        auto first = fixture.ConnectSocket();
        auto primary = fixture.Server->Accept(
            fixture.Listener.Accept(),
            TDuration::Seconds(5));
        SendBytes(first, XdcHello(false, 'a'));
        ReceiveHello(first);
        for (const auto& [hello, expected]:
             {std::pair{XdcHello(true, 'b'), E_REJECTED},
              std::pair{XdcHello(false, 'a'), E_ARGUMENT}})
        {
            auto socket = fixture.ConnectSocket();
            auto result = fixture.Server->Accept(
                fixture.Listener.Accept(),
                TDuration::Seconds(5));
            SendBytes(socket, hello);
            UNIT_ASSERT_VALUES_EQUAL(
                WaitHello(result).GetError().GetCode(),
                expected);
        }
        auto second = fixture.ConnectSocket();
        auto auxiliary = fixture.Server->Accept(
            fixture.Listener.Accept(),
            TDuration::Seconds(5));
        SendBytes(second, XdcHello(true, 'a'));
        ReceiveHello(second);
        auto extra = fixture.ConnectSocket();
        auto occupied = fixture.Server->Accept(
            fixture.Listener.Accept(),
            TDuration::Seconds(5));
        SendBytes(extra, XdcHello(true, 'a'));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(occupied).GetError().GetCode(),
            E_REJECTED);
        SendBytes(first, TString(1, '\x01') + "event");
        UNIT_ASSERT(!primary.Wait(TDuration::MilliSeconds(30)));
        SendBytes(second, TString(1, '\x01') + "payload");
        CheckIncomplete(auxiliary);
        auto connection = GetConnection(primary);
        UNIT_ASSERT_VALUES_EQUAL(ReceiveBytes(connection.Socket, 5), "event");
        UNIT_ASSERT_VALUES_EQUAL(
            ReceiveBytes(connection.XdcSocket, 7),
            "payload");
        auto late = fixture.ConnectSocket();
        auto rejected = fixture.Server->Accept(
            fixture.Listener.Accept(),
            TDuration::Seconds(5));
        SendBytes(late, XdcHello(true, 'a'));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(rejected).GetError().GetCode(),
            E_REJECTED);
    }

    Y_UNIT_TEST(RejectsAuxiliaryWhenXdcIsDisabled)
    {
        TXdcFixture fixture(true, false);
        auto socket = fixture.ConnectSocket();
        auto accepted = fixture.Server->Accept(
            fixture.Listener.Accept(),
            TDuration::Seconds(5));
        SendBytes(socket, XdcHello(true, 'a'));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(accepted).GetError().GetCode(),
            E_ARGUMENT);
    }

    Y_UNIT_TEST(DisconnectOfEitherSocketFailsPendingPair)
    {
        for (bool closeMain: {false, true}) {
            TXdcFixture fixture(true, true);
            auto main = fixture.ConnectSocket();
            auto primary = fixture.Server->Accept(
                fixture.Listener.Accept(),
                TDuration::Seconds(30));
            SendBytes(main, XdcHello(false, 'a'));
            ReceiveHello(main);
            auto data = fixture.ConnectSocket();
            auto auxiliary = fixture.Server->Accept(
                fixture.Listener.Accept(),
                TDuration::Seconds(30));
            SendBytes(data, XdcHello(true, 'a'));
            ReceiveHello(data);
            (closeMain ? main : data).Reset();
            for (const auto& future: {primary, auxiliary}) {
                auto result = WaitHello(future);
                UNIT_ASSERT(HasError(result.GetError()));
                UNIT_ASSERT_VALUES_UNEQUAL(
                    result.GetError().GetCode(),
                    E_TIMEOUT);
            }
        }
    }

    Y_UNIT_TEST(DeadlineAndStopBoundWaitingForAuxiliarySocket)
    {
        for (bool stop: {false, true}) {
            TXdcFixture fixture(true, true);
            auto futures = fixture.Begin(
                stop ? TDuration::Seconds(30) : TDuration::MilliSeconds(100));
            TSocketPtr auxiliary;
            if (stop) {
                auxiliary = fixture.Listener.Accept();
                fixture.Server->RequestStop();
                fixture.Client->RequestStop();
            }
            bool timedOut = false;
            for (const auto& future: futures) {
                auto result = WaitHello(future);
                const auto code = result.GetError().GetCode();
                if (stop) {
                    UNIT_ASSERT_VALUES_EQUAL(code, E_REJECTED);
                } else {
                    // Closing the timed-out side may reach its peer before
                    // the peer's own deadline.
                    UNIT_ASSERT(HasError(result.GetError()));
                    timedOut |= code == E_TIMEOUT;
                }
            }
            UNIT_ASSERT(stop || timedOut);
        }
    }

    Y_UNIT_TEST(WaitingForAuxiliaryAcknowledgement)
    {
        enum class EAction
        {
            Timeout,
            Stop,
            Disconnect,
        };
        for (auto action:
             {EAction::Timeout, EAction::Stop, EAction::Disconnect})
        {
            TXdcFixture fixture(true, true);
            auto main = fixture.ConnectSocket();
            auto primary = fixture.Server->Accept(
                fixture.Listener.Accept(),
                action == EAction::Timeout ? TDuration::Seconds(3)
                                           : TDuration::Seconds(30));
            SendBytes(main, XdcHello(false, 'a'));
            ReceiveHello(main);
            SendBytes(main, TString(1, '\x01'));

            auto data = fixture.ConnectSocket();
            auto auxiliary = fixture.Server->Accept(
                fixture.Listener.Accept(),
                TDuration::Seconds(30));
            SendBytes(data, XdcHello(true, 'a'));
            ReceiveHello(data);
            UNIT_ASSERT(!primary.Wait(TDuration::MilliSeconds(30)));

            if (action == EAction::Stop) {
                fixture.Server->RequestStop();
            } else if (action == EAction::Disconnect) {
                main.Reset();
            }

            const auto mainError = WaitHello(primary).GetError();
            const auto auxiliaryError = WaitHello(auxiliary).GetError();
            UNIT_ASSERT(HasError(mainError));
            UNIT_ASSERT(HasError(auxiliaryError));
            if (action == EAction::Timeout) {
                UNIT_ASSERT_VALUES_EQUAL(mainError.GetCode(), E_TIMEOUT);
                UNIT_ASSERT_VALUES_EQUAL(auxiliaryError.GetCode(), E_TIMEOUT);
            } else if (action == EAction::Stop) {
                UNIT_ASSERT_VALUES_EQUAL(mainError.GetCode(), E_REJECTED);
                UNIT_ASSERT_VALUES_EQUAL(auxiliaryError.GetCode(), E_REJECTED);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(mainError.GetCode(), E_IO);
                UNIT_ASSERT_VALUES_EQUAL(
                    mainError.GetMessage(),
                    "IC V2 main socket closed while pairing");
                UNIT_ASSERT_VALUES_UNEQUAL(auxiliaryError.GetCode(), E_TIMEOUT);
            }
        }
    }

    Y_UNIT_TEST(RejectsClosedMainSocketWhenAuxiliaryIsReady)
    {
        TXdcFixture fixture(true, true);
        auto main = fixture.ConnectSocket();
        auto acceptedMain = fixture.Listener.Accept();
        const int mainFd = dup(*acceptedMain);
        UNIT_ASSERT_C(mainFd >= 0, errno);
        TSocketHolder mainMonitor(mainFd);
        auto primary = fixture.Server->Accept(
            std::move(acceptedMain),
            TDuration::Seconds(30));
        SendBytes(main, XdcHello(false, 'a'));
        ReceiveHello(main);
        SendBytes(main, TString(1, '\x01'));

        auto data = fixture.ConnectSocket();
        auto acceptedData = fixture.Listener.Accept();
        const int dataFd = dup(*acceptedData);
        UNIT_ASSERT_C(dataFd >= 0, errno);
        TSocketHolder dataMonitor(dataFd);
        auto auxiliary = fixture.Server->Accept(
            std::move(acceptedData),
            TDuration::Seconds(30));
        SendBytes(data, XdcHello(true, 'a'));
        ReceiveHello(data);

        auto barrierSockets = TFixture::MakeSockets();
        auto barrier = fixture.Server->Accept(
            std::move(barrierSockets[1]),
            TDuration::Seconds(30));
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::future<void> subscription;
        Y_DEFER
        {
            release.TrySetValue();
        };
        subscription = SubscribeBlocking(barrier, entered, release, nullptr);
        UNIT_ASSERT(subscription.wait_for(5s) == std::future_status::ready);
        subscription.get();
        SendBytes(barrierSockets[0], ClientHello);
        ReceiveHello(barrierSockets[0]);
        SendBytes(barrierSockets[0], TString(1, '\x01'));
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));

        SendBytes(data, TString(1, '\x01'));
        UNIT_ASSERT_VALUES_EQUAL(shutdown(*main, SHUT_WR), 0);
        pollfd ready[] = {
            {.fd = mainMonitor, .events = POLLRDHUP, .revents = 0},
            {.fd = dataMonitor, .events = POLLIN, .revents = 0}};
        for (auto& fd: ready) {
            UNIT_ASSERT_VALUES_EQUAL(poll(&fd, 1, 5000), 1);
        }
        UNIT_ASSERT(ready[0].revents & POLLRDHUP);
        UNIT_ASSERT(ready[1].revents & POLLIN);
        release.SetValue();

        const auto error = WaitHello(primary).GetError();
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), E_IO);
        UNIT_ASSERT_VALUES_EQUAL(
            error.GetMessage(),
            "IC V2 main socket closed while pairing");
        CheckIncomplete(auxiliary);
    }

    Y_UNIT_TEST(ConnectsXdcOverIpv6)
    {
        CheckXdcLocalAddress("::1", "::1");
    }

    Y_UNIT_TEST(BindsXdcToMainSocketLocalIpv4Address)
    {
        CheckXdcLocalAddress("127.0.0.1", "127.0.0.2");
    }

    Y_UNIT_TEST(ReportsXdcConnectFailure)
    {
        TXdcFixture fixture(true, true);
        auto socket = fixture.ConnectSocket();
        auto peer = fixture.Listener.Accept();
        auto connected =
            Connect(*fixture.Client, std::move(socket), TDuration::Seconds(5));
        auto hello = ReceiveHello(peer);
        fixture.Listener.Listener->Stop();
        SendBytes(peer, MakeHello(true, false, hello.GetPairId()));
        const auto error = WaitHello(connected).GetError();
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_UNEQUAL(error.GetCode(), E_TIMEOUT);
    }

    Y_UNIT_TEST(ConcurrentExchanges)
    {
        TFixture fixture;
        std::array<TAcceptHandshakeFuture, 8> outgoing;
        std::array<TAcceptHandshakeFuture, 8> incoming;
        for (size_t i = 0; i < outgoing.size(); ++i) {
            auto sockets = TFixture::MakeSockets();
            outgoing[i] = Connect(
                *fixture.Client,
                std::move(sockets[0]),
                TDuration::Seconds(5));
            incoming[i] = fixture.Server->Accept(
                std::move(sockets[1]),
                TDuration::Seconds(5));
        }
        std::array<THandshakeReady, 8> clients;
        std::array<THandshakeReady, 8> servers;
        for (size_t i = 0; i < outgoing.size(); ++i) {
            clients[i] = GetConnection(outgoing[i]);
            servers[i] = GetConnection(incoming[i]);
        }
        fixture.Client->Stop();
        fixture.Server->Stop();
        for (size_t i = 0; i < outgoing.size(); ++i) {
            const auto& client = clients[i];
            const auto& server = servers[i];
            CheckSocket(client.Socket);
            CheckSocket(server.Socket);
            SendBytes(client.Socket, "request");
            UNIT_ASSERT_VALUES_EQUAL(ReceiveBytes(server.Socket, 7), "request");
            SendBytes(server.Socket, "response");
            UNIT_ASSERT_VALUES_EQUAL(
                ReceiveBytes(client.Socket, 8),
                "response");
        }
    }

    Y_UNIT_TEST(ConcurrentSubmissionsWakeWorker)
    {
        TFixture fixture;
        std::array<std::array<TSocketPtr, 2>, 8> sockets;
        for (auto& pair: sockets) {
            pair = TFixture::MakeSockets();
        }

        std::array<std::future<std::array<TAcceptHandshakeFuture, 2>>, 8>
            submissions;
        for (size_t i = 0; i < submissions.size(); ++i) {
            submissions[i] = std::async(
                std::launch::async,
                [&, i]
                {
                    return std::array{
                        Connect(
                            *fixture.Client,
                            std::move(sockets[i][0]),
                            TDuration::Seconds(5)),
                        fixture.Server->Accept(
                            std::move(sockets[i][1]),
                            TDuration::Seconds(5))};
                });
        }
        for (auto& submission: submissions) {
            const auto futures = submission.get();
            const auto client = GetConnection(futures[0]);
            const auto server = GetConnection(futures[1]);
            SendBytes(client.Socket, "request");
            UNIT_ASSERT_VALUES_EQUAL(ReceiveBytes(server.Socket, 7), "request");
        }
    }

    Y_UNIT_TEST(FragmentedHelloAndCoalescedAckDoNotConsumeEventBytes)
    {
        TFixture fixture;
        auto sockets = TFixture::MakeSockets();
        auto accepted = fixture.Server->Accept(
            std::move(sockets[1]),
            TDuration::Seconds(5));
        for (char byte: ClientHello) {
            SendBytes(sockets[0], TStringBuf(&byte, 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            ReceiveBytes(sockets[0], ServerHello.size()),
            ServerHello);
        UNIT_ASSERT(!accepted.Wait(TDuration::MilliSeconds(30)));
        SendBytes(
            sockets[0],
            TString(
                "\x01"
                "event",
                6));
        auto connection = GetConnection(accepted);
        fixture.Server->Stop();
        UNIT_ASSERT_VALUES_EQUAL(ReceiveBytes(connection.Socket, 5), "event");
    }

    Y_UNIT_TEST(InitiatorValidatesReplyAndPreservesEventBytes)
    {
        TFixture fixture;
        auto sockets = TFixture::MakeSockets();
        auto connected = Connect(
            *fixture.Client,
            std::move(sockets[0]),
            TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            ReceiveBytes(sockets[1], ClientHello.size()),
            ClientHello);
        SendBytes(sockets[1], ServerHello + "event");
        auto connection = GetConnection(connected);
        fixture.Client->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            ReceiveBytes(sockets[1], 1),
            TString(1, '\x01'));
        UNIT_ASSERT_VALUES_EQUAL(ReceiveBytes(connection.Socket, 5), "event");
    }

    Y_UNIT_TEST(InitiatorRejectsInvalidReplies)
    {
        for (int invalid: {0, 1, 2}) {
            TFixture fixture(true, true);
            auto sockets = TFixture::MakeSockets();
            auto connected = Connect(
                *fixture.Client,
                std::move(sockets[0]),
                TDuration::Seconds(5));
            const auto hello = ReceiveHello(sockets[1]);
            TString reply;
            if (invalid == 0) {
                reply = MakeHello(true, false, hello.GetPairId(), 2);
            } else if (invalid == 1) {
                reply = MakeHello(true, true, hello.GetPairId());
            } else {
                TString wrongId = hello.GetPairId();
                wrongId[0] = static_cast<char>(wrongId[0] ^ 1);
                reply = MakeHello(true, false, wrongId);
            }
            SendBytes(sockets[1], reply);
            UNIT_ASSERT_VALUES_EQUAL(
                WaitHello(connected).GetError().GetCode(),
                E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(InitiatorRejectsInvalidAuxiliaryReplies)
    {
        for (int invalid: {0, 1, 2, 3}) {
            TXdcFixture fixture(true, true);
            auto socket = fixture.ConnectSocket();
            auto main = fixture.Listener.Accept();
            auto connected = Connect(
                *fixture.Client,
                std::move(socket),
                TDuration::Seconds(5));
            const auto hello = ReceiveHello(main);
            SendBytes(main, MakeHello(true, false, hello.GetPairId()));
            auto auxiliary = fixture.Listener.Accept();
            ReceiveHello(auxiliary);

            TString reply;
            if (invalid == 0) {
                reply = MakeHello(true, true, hello.GetPairId(), 2);
            } else if (invalid == 1) {
                reply = MakeHello(true, false, hello.GetPairId());
            } else if (invalid == 2) {
                reply = MakeHello(false, true);
            } else {
                TString wrongId = hello.GetPairId();
                wrongId[0] = static_cast<char>(wrongId[0] ^ 1);
                reply = MakeHello(true, true, wrongId);
            }
            SendBytes(auxiliary, reply);
            UNIT_ASSERT_VALUES_EQUAL(
                WaitHello(connected).GetError().GetCode(),
                E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(RejectsInvalidHeaderWithoutWaitingForBody)
    {
        TFixture fixture;
        // Bad magic, oversized and empty message sizes.
        for (const auto [offset, value]:
             std::array<std::pair<size_t, char>, 3>{{{0, 'X'}, {4, 5}, {5, 0}}})
        {
            auto sockets = TFixture::MakeSockets();
            auto accepted = fixture.Server->Accept(
                std::move(sockets[1]),
                TDuration::Seconds(5));
            auto header = ClientHello.substr(0, 6);
            header[offset] = value;
            SendBytes(sockets[0], header);
            auto result = WaitHello(accepted);
            UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_ARGUMENT);
            pollfd readable{.fd = *sockets[0], .events = POLLIN, .revents = 0};
            UNIT_ASSERT_VALUES_EQUAL(poll(&readable, 1, 5000), 1);
            char byte{};
            UNIT_ASSERT_VALUES_EQUAL(recv(*sockets[0], &byte, 1, 0), 0);
        }
    }

    Y_UNIT_TEST(RejectsMalformedHelloMessages)
    {
        const TVector<TString> messages{
            MakeHello(false, false, {}, 2),
            MakeHello(true, false, "short"),
            TString("ICV2\x00\x01\xff", 7),
        };
        TFixture fixture;
        for (const auto& message: messages) {
            auto sockets = TFixture::MakeSockets();
            auto accepted = fixture.Server->Accept(
                std::move(sockets[1]),
                TDuration::Seconds(5));
            SendBytes(sockets[0], message);
            UNIT_ASSERT_VALUES_EQUAL(
                WaitHello(accepted).GetError().GetCode(),
                E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(RejectsInvalidAcknowledgement)
    {
        TFixture fixture;
        auto sockets = TFixture::MakeSockets();
        auto accepted = fixture.Server->Accept(
            std::move(sockets[1]),
            TDuration::Seconds(5));
        SendBytes(sockets[0], ClientHello);
        ReceiveBytes(sockets[0], ServerHello.size());
        SendBytes(sockets[0], "x");
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(accepted).GetError().GetCode(),
            E_ARGUMENT);
    }

    Y_UNIT_TEST(DeadlineBoundsPartialHeaderBodyAndMissingAcknowledgement)
    {
        TFixture fixture;
        for (size_t prefix:
             {size_t(0), size_t(5), ClientHello.size() - 1, ClientHello.size()})
        {
            auto sockets = TFixture::MakeSockets();
            auto accepted = fixture.Server->Accept(
                std::move(sockets[1]),
                TDuration::MilliSeconds(100));
            SendBytes(sockets[0], ClientHello.substr(0, prefix));
            auto result = WaitHello(accepted);
            UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_TIMEOUT);
        }
        auto sockets = TFixture::MakeSockets();
        auto connected = Connect(
            *fixture.Client,
            std::move(sockets[0]),
            TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(connected).GetError().GetCode(),
            E_TIMEOUT);
    }

    Y_UNIT_TEST(ReportsEarlyDisconnect)
    {
        TFixture fixture;
        for (bool initiator: {false, true}) {
            auto sockets = TFixture::MakeSockets();
            sockets[1].Reset();
            auto pending = initiator ? Connect(
                                           *fixture.Client,
                                           std::move(sockets[0]),
                                           TDuration::Seconds(5))
                                     : fixture.Server->Accept(
                                           std::move(sockets[0]),
                                           TDuration::Seconds(5));
            auto result = WaitHello(pending);
            UNIT_ASSERT(HasError(result.GetError()));
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetError().GetCode(), E_TIMEOUT);
        }
    }

    Y_UNIT_TEST(StopCancelsPendingHelloAndWaitsForCallback)
    {
        TFixture fixture;
        auto sockets = TFixture::MakeSockets();
        auto pending = fixture.Server->Accept(
            std::move(sockets[1]),
            TDuration::Seconds(30));
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::future<void> subscription;
        std::future<void> first;
        std::future<void> second;
        Y_DEFER
        {
            release.TrySetValue();
        };
        subscription =
            SubscribeBlocking(pending, entered, release, fixture.Server.get());
        UNIT_ASSERT(subscription.wait_for(5s) == std::future_status::ready);
        subscription.get();
        fixture.Server->RequestStop();
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        first = std::async(std::launch::async, [&] { fixture.Server->Stop(); });
        second =
            std::async(std::launch::async, [&] { fixture.Server->Stop(); });
        const bool waits =
            first.wait_for(30ms) == std::future_status::timeout &&
            second.wait_for(30ms) == std::future_status::timeout;
        release.SetValue();
        first.get();
        second.get();
        UNIT_ASSERT(waits);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(pending).GetError().GetCode(),
            E_REJECTED);
        auto rejected = fixture.Server->Accept(
            std::move(sockets[0]),
            TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(rejected).GetError().GetCode(),
            E_REJECTED);
    }

    Y_UNIT_TEST(OwnerKeepsProviderAliveUntilStopReturns)
    {
        auto sockets = TFixture::MakeSockets();
        auto [provider, error] = CreateHandshakeProvider({});
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        std::weak_ptr<IHandshakeProvider> weak = provider;
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::future<void> subscription;
        std::future<void> stopped;
        Y_DEFER
        {
            release.TrySetValue();
            if (provider) {
                provider->Stop();
            }
        };

        provider->Start();
        auto pending =
            provider->Accept(std::move(sockets[1]), TDuration::Seconds(30));
        subscription = std::async(
            std::launch::async,
            [pending, provider, entered, release]() mutable
            {
                pending.Subscribe(
                    [provider, entered, release](
                        const TAcceptHandshakeFuture&) mutable
                    {
                        Y_UNUSED(provider);
                        entered.SetValue();
                        release.GetFuture().GetValueSync();
                    });
            });
        UNIT_ASSERT(subscription.wait_for(5s) == std::future_status::ready);
        subscription.get();
        sockets[0].Reset();
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));

        auto stopStarted = NThreading::NewPromise<void>();
        stopped = std::async(
            std::launch::async,
            [provider, stopStarted]() mutable
            {
                stopStarted.SetValue();
                provider->Stop();
            });
        UNIT_ASSERT(stopStarted.GetFuture().Wait(TDuration::Seconds(5)));
        provider.reset();
        UNIT_ASSERT(!weak.expired());
        UNIT_ASSERT(stopped.wait_for(30ms) == std::future_status::timeout);
        release.SetValue();
        stopped.get();
        UNIT_ASSERT(weak.expired());
    }

    Y_UNIT_TEST(DeadlineIncludesQueueTime)
    {
        TFixture fixture;
        auto sockets = TFixture::MakeSockets();
        auto first = fixture.Server->Accept(
            std::move(sockets[1]),
            TDuration::Seconds(5));
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        std::future<void> subscription;
        Y_DEFER
        {
            release.TrySetValue();
        };
        subscription = SubscribeBlocking(first, entered, release, nullptr);
        UNIT_ASSERT(subscription.wait_for(5s) == std::future_status::ready);
        subscription.get();
        sockets[0].Reset();
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        auto queuedSockets = TFixture::MakeSockets();
        auto queued = fixture.Server->Accept(
            std::move(queuedSockets[1]),
            TDuration::MilliSeconds(10));
        const bool waiting = !queued.Wait(TDuration::MilliSeconds(50));
        release.SetValue();
        UNIT_ASSERT(waiting);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(queued).GetError().GetCode(),
            E_TIMEOUT);
    }

    Y_UNIT_TEST(RejectsInvalidRequests)
    {
        TFixture fixture;
        auto sockets = TFixture::MakeSockets();
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(Connect(*fixture.Client, {}, TDuration::Seconds(5)))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(fixture.Client->Accept({}, TDuration::Seconds(5)))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        auto expired =
            fixture.Server->Accept(std::move(sockets[1]), TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            WaitHello(expired).GetError().GetCode(),
            E_TIMEOUT);
    }

    Y_UNIT_TEST(RejectsInvalidAndBlockingSockets)
    {
        TFixture fixture;
        for (bool outgoing: {false, true}) {
            auto invalid = MakeIntrusive<NInterconnect::TStreamSocket>(-1);
            auto result = outgoing ? Connect(
                                         *fixture.Client,
                                         std::move(invalid),
                                         TDuration::Seconds(5))
                                   : fixture.Server->Accept(
                                         std::move(invalid),
                                         TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(
                WaitHello(result).GetError().GetCode(),
                MAKE_SYSTEM_ERROR(EBADF));

            const int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            UNIT_ASSERT_C(fd >= 0, errno);
            auto blocking = MakeIntrusive<NInterconnect::TStreamSocket>(fd);
            result = outgoing ? Connect(
                                    *fixture.Client,
                                    std::move(blocking),
                                    TDuration::Seconds(5))
                              : fixture.Server->Accept(
                                    std::move(blocking),
                                    TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(
                WaitHello(result).GetError().GetCode(),
                E_ARGUMENT);
        }
    }
}

}   // namespace NCloud::NBlockStore::NIcV2
