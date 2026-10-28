#include "handshake.h"

#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <poll.h>
#include <sys/resource.h>

#include <future>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::IConnectorPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NIcV2::NTests;
using namespace std::chrono_literals;

const TString ClientHello("ICV2\x00\x01\x00\x00", 8);
const TString ServerHello = ClientHello;

TAcceptHandshakeResult WaitHello(const TAcceptHandshakeFuture& future)
{
    UNIT_ASSERT(future.Wait(TDuration::Seconds(5)));
    return future.GetValue();
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
            [](const THandshakeFuture& future) -> TAcceptHandshakeResult
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
        pollfd writable{*socket, POLLOUT, 0};
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
        pollfd readable{*socket, POLLIN, 0};
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

    explicit TFixture(
        bool checksumEvents = false,
        bool clientXdc = false,
        bool serverXdc = false)
    {
        auto [client, clientError] =
            CreateHandshakeProvider({checksumEvents, clientXdc});
        auto [server, serverError] =
            CreateHandshakeProvider({checksumEvents, serverXdc});
        UNIT_ASSERT_C(!HasError(clientError), FormatError(clientError));
        UNIT_ASSERT_C(!HasError(serverError), FormatError(serverError));
        Client = std::move(client);
        Server = std::move(server);
        Client->Start();
        Server->Start();
    }

    ~TFixture()
    {
        Client->Stop();
        Server->Stop();
    }

    std::array<TSocketPtr, 2> MakeSockets()
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

    TXdcFixture(bool clientXdc, bool serverXdc, bool checksums = false)
        : TFixture(checksums, clientXdc, serverXdc)
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
    auto hello = ClientHello;
    hello[7] = auxiliary ? 6 : 2;
    hello.append(16, token);
    return hello;
}

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2HandshakeTest)
{
    SIMPLE_UNIT_FORKED_TEST(ReturnsErrorWhenWakeupDescriptorsAreExhausted)
    {
        rlimit original;
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
        for (bool checksums: {false, true}) {
            for (bool clientXdc: {false, true}) {
                for (bool serverXdc: {false, true}) {
                    TXdcFixture fixture(clientXdc, serverXdc, checksums);
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
            const TString bytes(1, 'a' + i);
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
        ReceiveBytes(first, 24);
        for (auto hello: {XdcHello(true, 'b'), XdcHello(false, 'a')}) {
            auto socket = fixture.ConnectSocket();
            auto result = fixture.Server->Accept(
                fixture.Listener.Accept(),
                TDuration::Seconds(5));
            SendBytes(socket, hello);
            UNIT_ASSERT_VALUES_EQUAL(
                WaitHello(result).GetError().GetCode(),
                E_ARGUMENT);
        }
        auto second = fixture.ConnectSocket();
        auto auxiliary = fixture.Server->Accept(
            fixture.Listener.Accept(),
            TDuration::Seconds(5));
        SendBytes(second, XdcHello(true, 'a'));
        ReceiveBytes(second, 24);
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
            E_ARGUMENT);
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
            ReceiveBytes(main, 24);
            auto data = fixture.ConnectSocket();
            auto auxiliary = fixture.Server->Accept(
                fixture.Listener.Accept(),
                TDuration::Seconds(30));
            SendBytes(data, XdcHello(true, 'a'));
            ReceiveBytes(data, 24);
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
            auto auxiliary = fixture.Listener.Accept();
            if (stop) {
                fixture.Client->RequestStop();
                fixture.Server->RequestStop();
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
                    UNIT_ASSERT(code == E_TIMEOUT || code == E_IO);
                    timedOut |= code == E_TIMEOUT;
                }
                UNIT_ASSERT(
                    !std::get<THandshakeReady>(result.GetResult()).Socket);
                UNIT_ASSERT(
                    !std::get<THandshakeReady>(result.GetResult()).XdcSocket);
            }
            UNIT_ASSERT(stop || timedOut);
        }
    }

    Y_UNIT_TEST(ConcurrentExchangesWithBothChecksumModes)
    {
        for (bool checksums: {false, true}) {
            TFixture fixture(checksums);
            std::array<TAcceptHandshakeFuture, 8> outgoing;
            std::array<TAcceptHandshakeFuture, 8> incoming;
            for (size_t i = 0; i < outgoing.size(); ++i) {
                auto sockets = fixture.MakeSockets();
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
                UNIT_ASSERT_VALUES_EQUAL(
                    ReceiveBytes(server.Socket, 7),
                    "request");
                SendBytes(server.Socket, "response");
                UNIT_ASSERT_VALUES_EQUAL(
                    ReceiveBytes(client.Socket, 8),
                    "response");
            }
        }
    }

    Y_UNIT_TEST(FragmentedHelloAndCoalescedAckDoNotConsumeEventBytes)
    {
        TFixture fixture;
        auto sockets = fixture.MakeSockets();
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
        auto sockets = fixture.MakeSockets();
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

    Y_UNIT_TEST(RejectsInvalidHeaderWithoutWaitingForBody)
    {
        TFixture fixture;
        // Bad magic, version, checksum and unknown flags.
        for (const auto [offset, value]:
             std::array<std::pair<size_t, char>, 4>{
                 {{0, 'X'}, {5, 2}, {7, 1}, {7, 8}}})
        {
            auto sockets = fixture.MakeSockets();
            auto accepted = fixture.Server->Accept(
                std::move(sockets[1]),
                TDuration::Seconds(5));
            auto header = ClientHello;
            header[offset] = value;
            SendBytes(sockets[0], header);
            auto result = WaitHello(accepted);
            UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_ARGUMENT);
            UNIT_ASSERT(!std::get<THandshakeReady>(result.GetResult()).Socket);
            pollfd readable{*sockets[0], POLLIN, 0};
            UNIT_ASSERT_VALUES_EQUAL(poll(&readable, 1, 5000), 1);
            char byte;
            UNIT_ASSERT_VALUES_EQUAL(recv(*sockets[0], &byte, 1, 0), 0);
        }
    }

    Y_UNIT_TEST(RejectsInvalidAcknowledgement)
    {
        TFixture fixture;
        auto sockets = fixture.MakeSockets();
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
             {size_t(0), size_t(5), size_t(15), ClientHello.size()})
        {
            auto sockets = fixture.MakeSockets();
            auto accepted = fixture.Server->Accept(
                std::move(sockets[1]),
                TDuration::MilliSeconds(100));
            SendBytes(sockets[0], ClientHello.substr(0, prefix));
            auto result = WaitHello(accepted);
            UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_TIMEOUT);
            UNIT_ASSERT(!std::get<THandshakeReady>(result.GetResult()).Socket);
        }
        auto sockets = fixture.MakeSockets();
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
            auto sockets = fixture.MakeSockets();
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
            UNIT_ASSERT(!std::get<THandshakeReady>(result.GetResult()).Socket);
        }
    }

    Y_UNIT_TEST(StopCancelsPendingHelloAndWaitsForCallback)
    {
        TFixture fixture;
        auto sockets = fixture.MakeSockets();
        auto pending = fixture.Server->Accept(
            std::move(sockets[1]),
            TDuration::Seconds(30));
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        pending.Subscribe(
            [&](const TAcceptHandshakeFuture&)
            {
                fixture.Server->RequestStop();
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        fixture.Server->RequestStop();
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        auto first =
            std::async(std::launch::async, [&] { fixture.Server->Stop(); });
        auto second =
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

    Y_UNIT_TEST(DeadlineIncludesQueueTime)
    {
        TFixture fixture;
        auto sockets = fixture.MakeSockets();
        auto first = fixture.Server->Accept(
            std::move(sockets[1]),
            TDuration::Seconds(5));
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        first.Subscribe(
            [&](const TAcceptHandshakeFuture&)
            {
                entered.SetValue();
                release.GetFuture().GetValueSync();
            });
        sockets[0].Reset();
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        auto queuedSockets = fixture.MakeSockets();
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
        auto sockets = fixture.MakeSockets();
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
}

}   // namespace NCloud::NBlockStore::NIcV2
