#include <cloud/blockstore/libs/uring/transport/server.h>

#include <cloud/blockstore/libs/uring/transport/config.h>
#include <cloud/blockstore/libs/uring/transport/protocol.h>
#include <cloud/blockstore/libs/uring/transport/socket.h>

#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <thread>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace NCloud::NBlockStore::NUring {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEchoHandler: public IServerHandler
{
    IServerEndpointPtr Endpoint;
    std::atomic<int> Requests = 0;
    std::atomic<bool> Hold = false;
    std::atomic<size_t> ResponseSize = 0;   // 0: echo the request back
    TVector<void*> Held;
    TVector<std::shared_ptr<TString>> Buffers;
    TAdaptiveLock Lock;

    TCallContextBasePtr CreateCallContext() override
    {
        return MakeIntrusive<TCallContextBase>(ui64{0});
    }

    void HandleRequest(
        void* ctx,
        TCallContextBasePtr,
        TStringBuf proto,
        TSgList data) override
    {
        ++Requests;
        auto echo = std::make_shared<TString>();
        if (const size_t size = ResponseSize) {
            *echo = TString::Uninitialized(size);
        } else {
            for (const auto& ref: data) {
                echo->append(ref.Data(), ref.Size());
            }
        }
        with_lock (Lock) {
            Buffers.push_back(echo);
            if (Hold) {
                Held.push_back(ctx);
                return;
            }
        }
        TSgList out;
        if (!echo->empty()) {
            out.push_back(TBlockDataRef{echo->data(), echo->size()});
        }
        Endpoint->SendResponse(ctx, proto, std::move(out), echo);
    }
};

// Minimal client: speaks the wire protocol with blocking sockets.
struct TRawClient
{
    int MainFd = -1;
    int DataFd = -1;
    ui32 OobThreshold = 128 << 10;
    int RcvBuf = 0;
    TWelcome Welcome;

    void SendHello(ui16 port, EOobMode oobMode)
    {
        MainFd = Dial(port, RcvBuf);
        THello hello;
        hello.OobMode = ToWire(oobMode);
        hello.MaxProtoSize = 64 << 10;
        hello.MaxDataSize = 8 << 20;
        hello.MaxInflightBytes = 64 << 20;
        hello.OobThreshold = OobThreshold;
        WriteAll(MainFd, &hello, sizeof(hello));
    }

    void ReadWelcome()
    {
        ReadAll(MainFd, &Welcome, sizeof(Welcome));
    }

    void OpenData(ui16 port)
    {
        DataFd = Dial(port, RcvBuf);
        TDataHello dh;
        dh.DataToken = Welcome.DataToken;
        WriteAll(DataFd, &dh, sizeof(dh));
        TDataWelcome dw;
        ReadAll(DataFd, &dw, sizeof(dw));
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_OK), ui32(dw.Status));
    }

    void Connect(ui16 port, EOobMode oobMode, bool openData = true)
    {
        SendHello(port, oobMode);
        ReadWelcome();
        if (Welcome.Status == STATUS_OK && Welcome.OobMode == OOB_WIRE_REQUIRED &&
            openData)
        {
            OpenData(port);
        }
    }

    void Disconnect()
    {
        CloseSocket(MainFd);
        CloseSocket(DataFd);
        MainFd = DataFd = -1;
    }

    ~TRawClient()
    {
        Disconnect();
    }

    static int Dial(ui16 port, int rcvBuf = 0)
    {
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(fd >= 0);
        if (rcvBuf) {
            ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvBuf, sizeof(rcvBuf));
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        UNIT_ASSERT_C(
            ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0,
            LastSystemErrorText());
        return fd;
    }

    static void WriteAll(int fd, const void* data, size_t size)
    {
        const char* p = static_cast<const char*>(data);
        while (size) {
            const auto n = ::send(fd, p, size, MSG_NOSIGNAL);
            UNIT_ASSERT_C(n > 0, LastSystemErrorText());
            p += n;
            size -= n;
        }
    }

    static bool TryWriteAll(int fd, const void* data, size_t size)
    {
        const char* p = static_cast<const char*>(data);
        while (size) {
            const auto n = ::send(fd, p, size, MSG_NOSIGNAL);
            if (n <= 0) {
                return false;
            }
            p += n;
            size -= n;
        }
        return true;
    }

    static bool TryReadAll(int fd, void* data, size_t size)
    {
        char* p = static_cast<char*>(data);
        while (size) {
            const auto n = ::recv(fd, p, size, 0);
            if (n <= 0) {
                return false;
            }
            p += n;
            size -= n;
        }
        return true;
    }

    static void ReadAll(int fd, void* data, size_t size)
    {
        UNIT_ASSERT_C(TryReadAll(fd, data, size), "unexpected eof");
    }

    // Reads everything the server queued and then expects the end of the
    // stream; false if it is still open after a few idle seconds.
    static bool DrainToEof(int fd)
    {
        timeval tv{.tv_sec = 3, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        char buf[4096];
        for (;;) {
            const auto n = ::recv(fd, buf, sizeof(buf), 0);
            if (n == 0) {
                return true;
            }
            if (n < 0) {
                return LastSystemError() == ECONNRESET;
            }
        }
    }

    static bool WaitForEof(int fd)
    {
        char buf = 0;
        return !TryReadAll(fd, &buf, 1);
    }

    void SendRequestHeader(
        ui64 id,
        TStringBuf proto,
        size_t dataLen,
        bool oob)
    {
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.RequestId = id;
        h.ProtoLen = proto.size();
        h.DataLen = dataLen;
        h.Flags = dataLen ? (oob ? FLAG_OOB : FLAG_INLINE) : 0;
        WriteAll(MainFd, &h, sizeof(h));
        WriteAll(MainFd, proto.data(), proto.size());
    }

    void SendRequestData(ui64 id, TStringBuf data)
    {
        TDataHeader dh;
        dh.RequestId = id;
        dh.DataLen = data.size();
        WriteAll(DataFd, &dh, sizeof(dh));
        WriteAll(DataFd, data.data(), data.size());
    }

    void SendRequest(ui64 id, TStringBuf proto, TStringBuf data, bool oob)
    {
        SendRequestHeader(id, proto, data.size(), oob);
        if (!data.empty() && !oob) {
            WriteAll(MainFd, data.data(), data.size());
        }
        if (!data.empty() && oob) {
            SendRequestData(id, data);
        }
    }

    bool TrySendRequest(ui64 id, TStringBuf proto)
    {
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.RequestId = id;
        h.ProtoLen = proto.size();
        return TryWriteAll(MainFd, &h, sizeof(h)) &&
               TryWriteAll(MainFd, proto.data(), proto.size());
    }

    struct TResponse
    {
        TFrameHeader Header;
        TString Proto;
        TString Data;
    };

    // the frame off the main stream, leaving an out-of-band payload unread
    TResponse ReadFrameOnly()
    {
        TResponse r;
        ReadAll(MainFd, &r.Header, sizeof(r.Header));
        UNIT_ASSERT_VALUES_EQUAL(MAGIC, r.Header.Magic);
        r.Proto = TString::Uninitialized(r.Header.ProtoLen);
        ReadAll(MainFd, r.Proto.begin(), r.Proto.size());
        return r;
    }

    TResponse ReadResponse()
    {
        TResponse r = ReadFrameOnly();
        if (r.Header.DataLen) {
            r.Data = TString::Uninitialized(r.Header.DataLen);
            if (r.Header.Flags & FLAG_INLINE) {
                ReadAll(MainFd, r.Data.begin(), r.Data.size());
            } else {
                TDataHeader dh;
                ReadAll(DataFd, &dh, sizeof(dh));
                UNIT_ASSERT_VALUES_EQUAL(r.Header.RequestId, dh.RequestId);
                UNIT_ASSERT_VALUES_EQUAL(r.Header.DataLen, dh.DataLen);
                ReadAll(DataFd, r.Data.begin(), r.Data.size());
            }
        }
        return r;
    }
};

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();
    IServerPtr Server;
    std::shared_ptr<TEchoHandler> Handler = std::make_shared<TEchoHandler>();
    IServerEndpointPtr Endpoint;
    ui16 Port = 0;

    void StartServer(TServerConfig config = {})
    {
        Server = CreateServer(
            Logging,
            Monitoring,
            std::make_shared<TServerConfig>(config));
        Server->Start();
        auto [endpoint, error] = Server->StartEndpoint("127.0.0.1", 0, Handler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Endpoint = endpoint;
        Handler->Endpoint = endpoint;
        auto [port, perr] = GetEndpointPort(Endpoint);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));
        Port = port;
    }

    NMonitoring::TDynamicCounters::TCounterPtr ActiveConnections()
    {
        return Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "uring_server")
            ->GetCounter("ActiveConnections");
    }

    void WaitForRequests(int count)
    {
        while (Handler->Requests < count) {
            Sleep(TDuration::MilliSeconds(5));
        }
    }

    void* TakeHeld()
    {
        for (;;) {
            with_lock (Handler->Lock) {
                if (!Handler->Held.empty()) {
                    void* ctx = Handler->Held.back();
                    Handler->Held.pop_back();
                    return ctx;
                }
            }
            Sleep(TDuration::MilliSeconds(5));
        }
    }

    void TearDown(NUnitTest::TTestContext&) override
    {
        if (Server) {
            Server->Stop();
        }
        // a service that drops a request it was handed leaks it by contract,
        // so the fixture answers whatever the handler still holds
        TVector<void*> held;
        with_lock (Handler->Lock) {
            held.swap(Handler->Held);
        }
        for (void* ctx: held) {
            Endpoint->SendResponse(ctx, {}, {}, nullptr);
        }
    }
};

TString Pattern(size_t size, char seed)
{
    TString s = TString::Uninitialized(size);
    for (size_t i = 0; i < size; ++i) {
        s[i] = static_cast<char>(seed + i % 251);
    }
    return s;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUringServerTest)
{
    Y_UNIT_TEST_F(ShouldEchoInlineAndOob, TFixture)
    {
        StartServer();
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_OK), ui32(client.Welcome.Status));
        UNIT_ASSERT_VALUES_EQUAL(ui32(OOB_WIRE_REQUIRED), ui32(client.Welcome.OobMode));

        auto small = Pattern(1024, 1);
        client.SendRequest(1, "p1", small, false);
        auto r1 = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_RESPONSE), ui32(r1.Header.Type));
        UNIT_ASSERT_VALUES_EQUAL(1u, r1.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL("p1", r1.Proto);
        UNIT_ASSERT(small == r1.Data);

        auto big = Pattern(1 << 20, 2);
        client.SendRequest(2, "p2", big, true);
        auto r2 = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(2u, r2.Header.RequestId);
        UNIT_ASSERT(big == r2.Data);
    }

    Y_UNIT_TEST_F(ShouldServeManyConnections, TFixture)
    {
        StartServer();
        TVector<std::unique_ptr<TRawClient>> clients;
        for (int i = 0; i < 4; ++i) {
            auto c = std::make_unique<TRawClient>();
            c->Connect(Port, EOobMode::Auto);
            clients.push_back(std::move(c));
        }
        for (ui64 i = 0; i < clients.size(); ++i) {
            clients[i]->SendRequest(i + 1, "x", Pattern(4096, char(i)), false);
        }
        for (ui64 i = 0; i < clients.size(); ++i) {
            auto r = clients[i]->ReadResponse();
            UNIT_ASSERT_VALUES_EQUAL(i + 1, r.Header.RequestId);
            UNIT_ASSERT(Pattern(4096, char(i)) == r.Data);
        }
    }

    Y_UNIT_TEST_F(ShouldWorkWithOobDisabled, TFixture)
    {
        TServerConfig config;
        config.OobMode = EOobMode::Off;
        StartServer(config);
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        UNIT_ASSERT_VALUES_EQUAL(ui32(OOB_WIRE_OFF), ui32(client.Welcome.OobMode));
        auto payload = Pattern(1 << 20, 3);
        client.SendRequest(1, "p", payload, false);
        auto r = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_INLINE), ui32(r.Header.Flags));
        UNIT_ASSERT(payload == r.Data);
    }

    Y_UNIT_TEST_F(ShouldRejectIncompatibleOobMode, TFixture)
    {
        TServerConfig config;
        config.OobMode = EOobMode::Off;
        StartServer(config);
        TRawClient client;
        client.Connect(Port, EOobMode::Required, false);
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_CONFIG_MISMATCH), ui32(client.Welcome.Status));
    }

    Y_UNIT_TEST_F(ShouldRejectBadVersion, TFixture)
    {
        StartServer();
        int fd = TRawClient::Dial(Port);
        THello hello;
        hello.Version = PROTOCOL_VERSION + 10;
        TRawClient::WriteAll(fd, &hello, sizeof(hello));
        TWelcome welcome;
        TRawClient::ReadAll(fd, &welcome, sizeof(welcome));
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_VERSION_MISMATCH), ui32(welcome.Status));
        CloseSocket(fd);
    }

    Y_UNIT_TEST_F(ShouldRejectBadDataToken, TFixture)
    {
        StartServer();
        TRawClient client;
        client.Connect(Port, EOobMode::Auto, false);
        int fd = TRawClient::Dial(Port);
        TDataHello dh;
        dh.DataToken = client.Welcome.DataToken + 1;
        TRawClient::WriteAll(fd, &dh, sizeof(dh));
        TDataWelcome dw{};
        const bool got = TRawClient::TryReadAll(fd, &dw, sizeof(dw));
        UNIT_ASSERT(!got || dw.Status == STATUS_BAD_TOKEN);
        CloseSocket(fd);
    }

    Y_UNIT_TEST_F(ShouldReturnErrorFrame, TFixture)
    {
        StartServer();
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(5, "p", {}, false);
        // the response proves the handler took the request before Hold was
        // set: counting requests would not, since the count is bumped before
        // the flag is read
        auto first = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(5u, first.Header.RequestId);
        Handler->Hold = true;
        client.SendRequest(6, "p", {}, false);
        void* ctx = TakeHeld();
        Endpoint->SendError(ctx, E_REJECTED, "nope");
        auto r = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_ERROR), ui32(r.Header.Type));
        UNIT_ASSERT_VALUES_EQUAL(6u, r.Header.RequestId);
        auto error = ParseError(r.Proto);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("nope", error.GetMessage());
    }

    Y_UNIT_TEST_F(ShouldRejectOversizedFrame, TFixture)
    {
        TServerConfig config;
        config.MaxDataSize = 4096;
        StartServer(config);
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.RequestId = 1;
        h.Flags = FLAG_INLINE;
        h.DataLen = 8192;
        TRawClient::WriteAll(client.MainFd, &h, sizeof(h));
        char buf[8];
        UNIT_ASSERT(!TRawClient::TryReadAll(client.MainFd, buf, sizeof(buf)));
    }

    Y_UNIT_TEST_F(ShouldRejectOverInflightLimit, TFixture)
    {
        TServerConfig config;
        config.MaxInflightRequests = 2;
        StartServer(config);
        Handler->Hold = true;
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(1, "a", {}, false);
        client.SendRequest(2, "b", {}, false);
        WaitForRequests(2);
        client.SendRequest(3, "c", {}, false);
        auto r = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_ERROR), ui32(r.Header.Type));
        UNIT_ASSERT_VALUES_EQUAL(3u, r.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, ParseError(r.Proto).GetCode());
    }

    Y_UNIT_TEST_F(ShouldStopWithHeldRequests, TFixture)
    {
        StartServer();
        Handler->Hold = true;
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(1, "a", Pattern(4096, 1), false);
        WaitForRequests(1);
        Server->Stop();
        void* ctx = TakeHeld();
        // must not crash: the session is gone, the context is released here
        Endpoint->SendResponse(ctx, "a", {}, nullptr);
        Server.reset();
    }

    Y_UNIT_TEST_F(ShouldNegotiateOobThreshold, TFixture)
    {
        StartServer();
        TRawClient low;
        low.OobThreshold = 64 << 10;
        low.Connect(Port, EOobMode::Auto);
        UNIT_ASSERT_VALUES_EQUAL(64u << 10, low.Welcome.OobThreshold);

        TRawClient high;
        high.OobThreshold = 256 << 10;
        high.Connect(Port, EOobMode::Auto);
        UNIT_ASSERT_VALUES_EQUAL(128u << 10, high.Welcome.OobThreshold);

        for (auto* client: {&low, &high}) {
            client->SendRequest(7, "p", Pattern(1024, 4), false);
            auto r = client->ReadResponse();
            UNIT_ASSERT_VALUES_EQUAL(7u, r.Header.RequestId);
        }
    }

    Y_UNIT_TEST_F(ShouldCloseOnInlineAboveAgreedThreshold, TFixture)
    {
        StartServer();
        TRawClient client;
        client.OobThreshold = 8 << 10;
        client.Connect(Port, EOobMode::Auto);
        UNIT_ASSERT_VALUES_EQUAL(8u << 10, client.Welcome.OobThreshold);

        client.SendRequest(1, "p", Pattern(4096, 5), false);
        auto r = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(1u, r.Header.RequestId);

        // the agreed threshold is below the server's own, and an inline frame
        // at it is a protocol violation
        client.SendRequest(2, "p", Pattern(8 << 10, 6), false);
        UNIT_ASSERT(TRawClient::WaitForEof(client.MainFd));
    }

    Y_UNIT_TEST_F(ShouldKeepNegotiatedParamsPerPendingSession, TFixture)
    {
        StartServer();
        TRawClient low;
        TRawClient high;
        low.OobThreshold = 64 << 10;
        high.OobThreshold = 256 << 10;

        // both handshakes are open at the same time; the data sockets arrive
        // in the opposite order
        low.SendHello(Port, EOobMode::Required);
        high.SendHello(Port, EOobMode::Auto);
        low.ReadWelcome();
        high.ReadWelcome();
        UNIT_ASSERT_VALUES_EQUAL(64u << 10, low.Welcome.OobThreshold);
        UNIT_ASSERT_VALUES_EQUAL(128u << 10, high.Welcome.OobThreshold);
        high.OpenData(Port);
        low.OpenData(Port);

        high.SendRequest(1, "p", Pattern(64 << 10, 7), false);
        auto r = high.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(1u, r.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(64u << 10, r.Header.DataLen);

        low.SendRequest(2, "p", Pattern(64 << 10, 8), false);
        UNIT_ASSERT(TRawClient::WaitForEof(low.MainFd));
    }

    Y_UNIT_TEST_F(ShouldRejectMalformedHandshake, TFixture)
    {
        StartServer();

        int badMagic = TRawClient::Dial(Port);
        THello hello;
        hello.Magic = MAGIC + 1;
        TRawClient::WriteAll(badMagic, &hello, sizeof(hello));
        UNIT_ASSERT(TRawClient::WaitForEof(badMagic));
        CloseSocket(badMagic);

        int badRole = TRawClient::Dial(Port);
        hello = {};
        hello.Role = 7;
        TRawClient::WriteAll(badRole, &hello, sizeof(hello));
        UNIT_ASSERT(TRawClient::WaitForEof(badRole));
        CloseSocket(badRole);

        int badMode = TRawClient::Dial(Port);
        hello = {};
        hello.OobMode = 77;
        TRawClient::WriteAll(badMode, &hello, sizeof(hello));
        TWelcome welcome;
        TRawClient::ReadAll(badMode, &welcome, sizeof(welcome));
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_CONFIG_MISMATCH), ui32(welcome.Status));
        CloseSocket(badMode);

        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(1, "p", {}, false);
        UNIT_ASSERT_VALUES_EQUAL(1u, client.ReadResponse().Header.RequestId);
    }

    Y_UNIT_TEST_F(ShouldDropStalledHandshake, TFixture)
    {
        TServerConfig config;
        config.DataTokenTtl = TDuration::MilliSeconds(300);
        StartServer(config);

        int fd = TRawClient::Dial(Port);
        THello hello;
        TRawClient::WriteAll(fd, &hello, 8);
        UNIT_ASSERT(TRawClient::WaitForEof(fd));
        CloseSocket(fd);

        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(1, "p", {}, false);
        UNIT_ASSERT_VALUES_EQUAL(1u, client.ReadResponse().Header.RequestId);
    }

    Y_UNIT_TEST_F(ShouldExpireParkedMainSocket, TFixture)
    {
        TServerConfig config;
        config.DataTokenTtl = TDuration::MilliSeconds(300);
        StartServer(config);

        TRawClient client;
        client.Connect(Port, EOobMode::Auto, false);
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_OK), ui32(client.Welcome.Status));
        UNIT_ASSERT(TRawClient::WaitForEof(client.MainFd));
    }

    Y_UNIT_TEST_F(ShouldRejectSecondDataSocket, TFixture)
    {
        StartServer();
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);

        int fd = TRawClient::Dial(Port);
        TDataHello dh;
        dh.DataToken = client.Welcome.DataToken;
        TRawClient::WriteAll(fd, &dh, sizeof(dh));
        TDataWelcome dw{};
        const bool got = TRawClient::TryReadAll(fd, &dw, sizeof(dw));
        UNIT_ASSERT(!got || dw.Status == STATUS_BAD_TOKEN);
        CloseSocket(fd);

        auto payload = Pattern(1 << 20, 9);
        client.SendRequest(1, "p", payload, true);
        auto r = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(1u, r.Header.RequestId);
        UNIT_ASSERT(payload == r.Data);
    }

    Y_UNIT_TEST_F(ShouldStopWithHandshakesInFlight, TFixture)
    {
        StartServer();
        int silent = TRawClient::Dial(Port);
        int partial = TRawClient::Dial(Port);
        THello hello;
        TRawClient::WriteAll(partial, &hello, 8);

        TRawClient parked;
        parked.Connect(Port, EOobMode::Auto, false);
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_OK), ui32(parked.Welcome.Status));

        Server->Stop();
        UNIT_ASSERT(TRawClient::WaitForEof(silent));
        UNIT_ASSERT(TRawClient::WaitForEof(partial));
        UNIT_ASSERT(TRawClient::WaitForEof(parked.MainFd));
        CloseSocket(silent);
        CloseSocket(partial);
    }

    Y_UNIT_TEST_F(ShouldAnswerHeldRequestAfterPeerDisconnect, TFixture)
    {
        StartServer();
        Handler->Hold = true;
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(1, "a", Pattern(4096, 10), false);
        WaitForRequests(1);
        UNIT_ASSERT_VALUES_EQUAL(1, ActiveConnections()->Val());

        client.Disconnect();
        while (ActiveConnections()->Val() != 0) {
            Sleep(TDuration::MilliSeconds(5));
        }
        // the session is gone but the context is still the handler's
        Endpoint->SendResponse(TakeHeld(), "a", {}, nullptr);
    }

    Y_UNIT_TEST_F(ShouldAnswerWhilePeerDisconnects, TFixture)
    {
        StartServer();
        Handler->Hold = true;
        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        for (ui64 i = 1; i <= 8; ++i) {
            client.SendRequest(i, "a", Pattern(4096, char(i)), false);
        }
        WaitForRequests(8);

        TVector<void*> held;
        for (int i = 0; i < 8; ++i) {
            held.push_back(TakeHeld());
        }
        std::thread answerer([&] {
            for (void* ctx: held) {
                Endpoint->SendResponse(ctx, "a", {}, nullptr);
            }
        });
        client.Disconnect();
        answerer.join();
    }

    Y_UNIT_TEST_F(ShouldStopWithPrefixOnlyHandshakes, TFixture)
    {
        StartServer();
        TVector<int> fds;
        for (int i = 0; i < 8; ++i) {
            int fd = TRawClient::Dial(Port);
            THello hello;
            // the role byte says a main socket, so the server waits for the
            // rest of THello that never comes
            TRawClient::WriteAll(fd, &hello, sizeof(TDataHello));
            fds.push_back(fd);
        }
        Server->Stop();
        for (int fd: fds) {
            UNIT_ASSERT(TRawClient::WaitForEof(fd));
            CloseSocket(fd);
        }
    }

    Y_UNIT_TEST_F(ShouldAccountInflightBytesUntilTheKernelIsDone, TFixture)
    {
        TServerConfig config;
        config.MaxDataSize = 1 << 20;
        config.MaxInflightRequests = 2;
        // above what one request and its echo pin together, below what two of
        // them do, and still above a full queue of the largest request
        config.MaxInflightBytes = 2560 << 10;
        StartServer(config);

        TRawClient client;
        client.RcvBuf = 4096;
        client.Connect(Port, EOobMode::Auto);

        auto payload = Pattern(1 << 20, 11);
        client.SendRequest(1, "p", payload, true);
        // the response header arrives on the main stream, but its payload is
        // stuck on the data stream the client does not read
        auto first = client.ReadFrameOnly();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_RESPONSE), ui32(first.Header.Type));
        UNIT_ASSERT_VALUES_EQUAL(1u, first.Header.RequestId);

        client.SendRequest(2, "p", payload, true);
        auto second = client.ReadFrameOnly();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_ERROR), ui32(second.Header.Type));
        UNIT_ASSERT_VALUES_EQUAL(2u, second.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            ParseError(second.Proto).GetCode());
    }

    Y_UNIT_TEST_F(ShouldRejectDataSocketFromAnotherAddress, TFixture)
    {
        StartServer();
        TRawClient client;
        client.Connect(Port, EOobMode::Auto, false);
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_OK), ui32(client.Welcome.Status));

        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(fd >= 0);
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK + 1);
        UNIT_ASSERT_C(
            ::bind(fd, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0,
            LastSystemErrorText());
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(Port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        UNIT_ASSERT_C(
            ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0,
            LastSystemErrorText());

        TDataHello dh;
        dh.DataToken = client.Welcome.DataToken;
        TRawClient::WriteAll(fd, &dh, sizeof(dh));
        TDataWelcome dw{};
        TRawClient::ReadAll(fd, &dw, sizeof(dw));
        UNIT_ASSERT_VALUES_EQUAL(ui32(STATUS_BAD_TOKEN), ui32(dw.Status));
        CloseSocket(fd);

        // the token was not spent, so the client's own data socket still pairs
        client.OpenData(Port);
        auto payload = Pattern(1 << 20, 12);
        client.SendRequest(1, "p", payload, true);
        auto r = client.ReadResponse();
        UNIT_ASSERT_VALUES_EQUAL(1u, r.Header.RequestId);
        UNIT_ASSERT(payload == r.Data);
    }

    Y_UNIT_TEST_F(ShouldRejectWithoutClosingAFastReader, TFixture)
    {
        TServerConfig config;
        config.MaxDataSize = 256 << 10;
        config.MaxInflightRequests = 2;
        // exactly a full queue of the largest request, so nothing here is a
        // misconfiguration
        config.MaxInflightBytes = 2 * ((256 << 10) + (64 << 10));
        StartServer(config);
        Handler->ResponseSize = 256 << 10;

        TRawClient client;
        client.RcvBuf = 4096;
        client.Connect(Port, EOobMode::Auto);

        // both requests are admitted on their own size, which is what the
        // budget is for; their responses then double what is in flight, so it
        // overshoots the limit by far more than a reply costs and the session
        // has to survive saying no
        auto payload = Pattern(256 << 10, 14);
        client.SendRequestHeader(1, "p", payload.size(), true);
        client.SendRequestHeader(2, "p", payload.size(), true);
        client.SendRequestData(1, payload);
        client.SendRequestData(2, payload);
        size_t responses = 0;
        for (int i = 0; i < 2; ++i) {
            if (client.ReadFrameOnly().Header.Type == FRAME_RESPONSE) {
                ++responses;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(2u, responses);

        for (ui64 i = 3; i <= 5; ++i) {
            client.SendRequest(i, "p", payload, true);
            auto frame = client.ReadFrameOnly();
            UNIT_ASSERT_VALUES_EQUAL(i, frame.Header.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_ERROR), ui32(frame.Header.Type));
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                ParseError(frame.Proto).GetCode());
        }
    }

    Y_UNIT_TEST_F(ShouldReleaseRejectionBytes, TFixture)
    {
        TServerConfig config;
        // three unsent rejections fit this, and no more
        config.MaxProtoSize = 1024;
        config.MaxInflightRequests = 1;
        StartServer(config);
        Handler->Hold = true;

        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(1, "p", {}, false);
        WaitForRequests(1);

        // each rejection is read before the next request is sent, so the
        // budget frees up again; twenty is far past what it holds at once
        for (ui64 i = 2; i <= 21; ++i) {
            client.SendRequest(i, "p", {}, false);
            auto frame = client.ReadFrameOnly();
            UNIT_ASSERT_VALUES_EQUAL(i, frame.Header.RequestId);
            UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_ERROR), ui32(frame.Header.Type));
        }
    }

    Y_UNIT_TEST_F(ShouldCloseAPeerHoardingRejections, TFixture)
    {
        TServerConfig config;
        // three rejections fit the budget, the fourth does not
        config.MaxProtoSize = 1024;
        config.MaxInflightRequests = 1;
        StartServer(config);
        Handler->Hold = true;

        TRawClient client;
        client.Connect(Port, EOobMode::Auto);
        client.SendRequest(1, "p", {}, false);
        WaitForRequests(1);

        bool closed = false;
        for (ui64 i = 2; i <= 100 && !closed; ++i) {
            closed = !client.TrySendRequest(i, "p");
            if (!closed) {
                Sleep(TDuration::MilliSeconds(2));
            }
        }
        if (!closed) {
            UNIT_ASSERT_C(
                TRawClient::DrainToEof(client.MainFd),
                "the server kept queueing rejections");
        }
    }

    Y_UNIT_TEST_F(ShouldAccountResponseBytes, TFixture)
    {
        TServerConfig config;
        config.MaxDataSize = 1 << 20;
        config.MaxInflightRequests = 1;
        config.MaxInflightBytes = (1 << 20) + (64 << 10);
        StartServer(config);
        // the read shape: a tiny request, a large response
        Handler->ResponseSize = 1 << 20;

        TRawClient client;
        client.RcvBuf = 4096;
        client.Connect(Port, EOobMode::Auto);

        client.SendRequest(1, "p", {}, false);
        auto first = client.ReadFrameOnly();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_RESPONSE), ui32(first.Header.Type));
        UNIT_ASSERT_VALUES_EQUAL(1u, first.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(1u << 20, first.Header.DataLen);

        // the response is a megabyte the peer is not reading, so the next
        // request does not fit the limit even though the budget would hold it
        client.SendRequest(2, "p", Pattern(512 << 10, 13), true);
        auto second = client.ReadFrameOnly();
        UNIT_ASSERT_VALUES_EQUAL(ui32(FRAME_ERROR), ui32(second.Header.Type));
        UNIT_ASSERT_VALUES_EQUAL(2u, second.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            ParseError(second.Proto).GetCode());
    }
}

}   // namespace NCloud::NBlockStore::NUring
