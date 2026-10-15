#include <cloud/blockstore/libs/uring/transport/client.h>

#include <cloud/blockstore/libs/uring/transport/config.h>
#include <cloud/blockstore/libs/uring/transport/protocol.h>
#include <cloud/blockstore/libs/uring/transport/server.h>
#include <cloud/blockstore/libs/uring/transport/socket.h>

#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/scope.h>

#include <functional>
#include <thread>

#include <sys/socket.h>

namespace NCloud::NBlockStore::NUring {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString Pattern(size_t size, char seed)
{
    TString s = TString::Uninitialized(size);
    for (size_t i = 0; i < size; ++i) {
        s[i] = static_cast<char>(seed + i % 251);
    }
    return s;
}

struct TResult
{
    ui32 Status = 0;
    TString Proto;
    size_t DataBytes = 0;
};

struct TCollector: public IClientRequestHandler
{
    TAdaptiveLock Lock;
    TVector<TPromise<TResult>> Promises;
    // Close() spins while a guard is held, and this runs on the ring thread,
    // so a guard released after the handler hangs the response here
    bool CloseBuffers = false;

    TFuture<TResult> NewPromiseFuture()
    {
        with_lock (Lock) {
            Promises.push_back(NewPromise<TResult>());
            return Promises.back().GetFuture();
        }
    }

    void HandleResponse(
        TClientRequestPtr req,
        ui32 status,
        TStringBuf proto,
        size_t dataBytes) override
    {
        if (CloseBuffers) {
            req->SendData.Close();
            req->RecvData.Close();
        }
        with_lock (Lock) {
            for (auto& p: Promises) {
                if (!p.HasValue()) {
                    p.SetValue(TResult{status, TString(proto), dataBytes});
                    return;
                }
            }
        }
    }
};

// Echoes requests; can hold them to simulate a hung server.
struct TEchoHandler: public IServerHandler
{
    IServerEndpointPtr Endpoint;
    std::atomic<bool> Hold = false;
    std::atomic<int> Received = 0;
    TAdaptiveLock Lock;
    TVector<void*> Held;

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
        ++Received;
        if (Hold) {
            with_lock (Lock) {
                Held.push_back(ctx);
            }
            return;
        }
        auto echo = std::make_shared<TString>();
        for (const auto& ref: data) {
            echo->append(ref.Data(), ref.Size());
        }
        TSgList out;
        if (!echo->empty()) {
            out.push_back(TBlockDataRef{echo->data(), echo->size()});
        }
        Endpoint->SendResponse(ctx, proto, std::move(out), echo);
    }
};

struct TEvents: public IClientEndpointHandler
{
    TPromise<void> Connected = NewPromise();
    TPromise<void> Disconnected = NewPromise();
    TPromise<void> Unavailable = NewPromise();
    std::atomic<int> Connects = 0;

    void HandleConnected() override
    {
        ++Connects;
        Connected.TrySetValue();
    }

    void HandleDisconnected() override
    {
        Disconnected.TrySetValue();
    }

    void HandleUnavailable() override
    {
        Unavailable.TrySetValue();
    }
};

// Accepts connections and answers every hello with the same welcome.
struct TFakeServer
{
    int ListenFd = -1;
    ui16 Port = 0;
    std::thread Thread;

    explicit TFakeServer(TWelcome welcome)
    {
        auto [fd, error] = CreateListenSocket("127.0.0.1", 0, 16);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        ListenFd = fd;
        auto [port, perr] = GetLocalPort(ListenFd);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));
        Port = port;

        Thread = std::thread([this, welcome] {
            for (;;) {
                const int fd = ::accept(ListenFd, nullptr, nullptr);
                if (fd < 0) {
                    return;
                }
                THello hello;
                const auto n =
                    ::recv(fd, &hello, sizeof(hello), MSG_WAITALL);
                if (n == sizeof(hello)) {
                    ::send(fd, &welcome, sizeof(welcome), MSG_NOSIGNAL);
                }
                CloseSocket(fd);
            }
        });
    }

    ~TFakeServer()
    {
        // wakes the accept up with an error
        ::shutdown(ListenFd, SHUT_RDWR);
        Thread.join();
        CloseSocket(ListenFd);
    }
};

bool TryReadAll(int fd, void* data, size_t size)
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

bool TryWriteAll(int fd, const void* data, size_t size)
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

void WaitForEof(int fd)
{
    char c = 0;
    while (::recv(fd, &c, 1, 0) > 0) {
    }
}

// The id of the next request on the main stream; its payload, if any, must
// be inline.
ui64 ReadRequest(int mainFd)
{
    TFrameHeader h;
    if (!TryReadAll(mainFd, &h, sizeof(h))) {
        return 0;
    }
    const size_t rest = h.ProtoLen + (h.Flags & FLAG_INLINE ? h.DataLen : 0);
    TString buffer = TString::Uninitialized(rest);
    if (rest && !TryReadAll(mainFd, buffer.begin(), rest)) {
        return 0;
    }
    return h.RequestId;
}

void SendOobResponseHeader(int mainFd, ui64 id, ui32 dataLen)
{
    TFrameHeader h;
    h.Type = FRAME_RESPONSE;
    h.Flags = FLAG_OOB;
    h.RequestId = id;
    h.DataLen = dataLen;
    TryWriteAll(mainFd, &h, sizeof(h));
}

void SendDataHeader(int dataFd, ui64 id, ui32 dataLen)
{
    TDataHeader h;
    h.RequestId = id;
    h.DataLen = dataLen;
    TryWriteAll(dataFd, &h, sizeof(h));
}

// Serves one client: completes the out-of-band handshake, then hands both
// sockets to the script and closes them once it returns.
struct TScriptedPeer
{
    using TScript = std::function<void(int mainFd, int dataFd)>;

    int ListenFd = -1;
    ui16 Port = 0;
    std::thread Thread;

    explicit TScriptedPeer(TScript script)
    {
        auto [fd, error] = CreateListenSocket("127.0.0.1", 0, 16);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        ListenFd = fd;
        auto [port, perr] = GetLocalPort(ListenFd);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));
        Port = port;

        Thread = std::thread([this, script = std::move(script)] {
            const int mainFd = Accept();
            const int dataFd = mainFd >= 0 ? Handshake(mainFd) : -1;
            if (dataFd >= 0) {
                script(mainFd, dataFd);
            }
            CloseSocket(dataFd);
            CloseSocket(mainFd);
        });
    }

    ~TScriptedPeer()
    {
        ::shutdown(ListenFd, SHUT_RDWR);
        Thread.join();
        CloseSocket(ListenFd);
    }

    int Accept()
    {
        const int fd = ::accept(ListenFd, nullptr, nullptr);
        if (fd >= 0) {
            // a script never waits on the client for long
            timeval tv{.tv_sec = 10, .tv_usec = 0};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        }
        return fd;
    }

    int Handshake(int mainFd)
    {
        THello hello;
        if (!TryReadAll(mainFd, &hello, sizeof(hello))) {
            return -1;
        }
        TWelcome welcome;
        welcome.OobMode = OOB_WIRE_REQUIRED;
        welcome.DataToken = 1;
        welcome.MaxProtoSize = hello.MaxProtoSize;
        welcome.MaxDataSize = hello.MaxDataSize;
        welcome.MaxInflightBytes = hello.MaxInflightBytes;
        welcome.OobThreshold = hello.OobThreshold;
        if (!TryWriteAll(mainFd, &welcome, sizeof(welcome))) {
            return -1;
        }

        const int dataFd = Accept();
        TDataHello dataHello;
        TDataWelcome dataWelcome;
        if (dataFd < 0 ||
            !TryReadAll(dataFd, &dataHello, sizeof(dataHello)) ||
            !TryWriteAll(dataFd, &dataWelcome, sizeof(dataWelcome)))
        {
            CloseSocket(dataFd);
            return -1;
        }
        return dataFd;
    }
};

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    IMonitoringServicePtr Monitoring = CreateMonitoringServiceStub();

    IServerPtr Server;
    IServerEndpointPtr ServerEndpoint;
    std::shared_ptr<TEchoHandler> ServerHandler =
        std::make_shared<TEchoHandler>();
    ui16 Port = 0;

    IClientPtr Client;
    IClientEndpointPtr Endpoint;
    std::shared_ptr<TEvents> Events = std::make_shared<TEvents>();
    std::shared_ptr<TCollector> Collector = std::make_shared<TCollector>();

    void StartServer(TServerConfig config = {})
    {
        Server = CreateServer(
            Logging,
            Monitoring,
            std::make_shared<TServerConfig>(config));
        Server->Start();
        auto [endpoint, error] =
            Server->StartEndpoint("127.0.0.1", Port, ServerHandler);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        ServerEndpoint = endpoint;
        ServerHandler->Endpoint = endpoint;
        auto [port, perr] = GetEndpointPort(endpoint);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));
        Port = port;
    }

    void StopServer()
    {
        if (!Server) {
            return;
        }
        Server->Stop();
        // a ctx the handler still holds is freed by answering it after Stop
        TVector<void*> held;
        with_lock (ServerHandler->Lock) {
            held.swap(ServerHandler->Held);
        }
        for (void* ctx: held) {
            ServerEndpoint->SendResponse(ctx, {}, {}, nullptr);
        }
        Server.reset();
        ServerEndpoint.reset();
    }

    void StartClient(TClientConfig config = {})
    {
        Client = CreateClient(
            Logging,
            Monitoring,
            std::make_shared<TClientConfig>(config));
        Client->Start();
        auto [endpoint, error] =
            Client->StartEndpoint("127.0.0.1", Port, Events);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Endpoint = endpoint;
    }

    void TearDown(NUnitTest::TTestContext&) override
    {
        if (Endpoint) {
            Endpoint->Stop().GetValue(TDuration::Seconds(10));
        }
        if (Client) {
            Client->Stop();
        }
        StopServer();
    }

    TClientRequestPtr MakeRequest(
        TStringBuf proto,
        TGuardedSgList send = {},
        TGuardedSgList recv = {},
        TDuration timeout = {})
    {
        auto request = std::make_unique<TClientRequest>();
        request->Handler = Collector;
        request->Proto = TString(proto);
        request->SendData = std::move(send);
        request->RecvData = std::move(recv);
        request->Timeout = timeout;
        return request;
    }

    void WaitConnected()
    {
        Events->Connected.GetFuture().GetValue(TDuration::Seconds(10));
    }

    TVector<void*> TakeHeld(size_t count)
    {
        for (;;) {
            with_lock (ServerHandler->Lock) {
                if (ServerHandler->Held.size() >= count) {
                    TVector<void*> held;
                    held.swap(ServerHandler->Held);
                    return held;
                }
            }
            Sleep(TDuration::MilliSeconds(5));
        }
    }

    i64 ClientCounter(const TString& name)
    {
        return Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "uring_client")
            ->GetCounter(name)
            ->Val();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUringClientTest)
{
    Y_UNIT_TEST_F(ShouldRoundTripProtoOnly, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();

        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("ping"), nullptr);
        auto result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(0u, result.Status);
        UNIT_ASSERT_VALUES_EQUAL("ping", result.Proto);
    }

    Y_UNIT_TEST_F(ShouldRoundTripInlineAndOob, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();

        for (size_t size: {size_t(4096), size_t(1) << 20}) {
            auto payload = Pattern(size, 1);
            auto echoed = TString::Uninitialized(size);
            TGuardedSgList send({TBlockDataRef{payload.data(), payload.size()}});
            TGuardedSgList recv({TBlockDataRef{echoed.data(), echoed.size()}});

            auto future = Collector->NewPromiseFuture();
            Endpoint->SendRequest(MakeRequest("echo", send, recv), nullptr);
            auto result = future.GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(0u, result.Status);
            UNIT_ASSERT_VALUES_EQUAL(size, result.DataBytes);
            UNIT_ASSERT(payload == echoed);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, ClientCounter("StagingCopyBytes"));
    }

    Y_UNIT_TEST_F(ShouldWorkWithStagingSgIo, TFixture)
    {
        TServerConfig serverConfig;
        serverConfig.DirectSgIoEnabled = false;
        StartServer(serverConfig);
        TClientConfig clientConfig;
        clientConfig.DirectSgIoEnabled = false;
        StartClient(clientConfig);
        WaitConnected();

        auto payload = Pattern(1 << 20, 2);
        auto echoed = TString::Uninitialized(payload.size());
        TGuardedSgList send({TBlockDataRef{payload.data(), payload.size()}});
        TGuardedSgList recv({TBlockDataRef{echoed.data(), echoed.size()}});

        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("echo", send, recv), nullptr);
        auto result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(0u, result.Status);
        UNIT_ASSERT(payload == echoed);
        // one copy on the way out and one on the way in
        UNIT_ASSERT_VALUES_EQUAL(
            2 * payload.size(),
            ClientCounter("StagingCopyBytes"));
    }

    Y_UNIT_TEST_F(ShouldPipelineManyRequests, TFixture)
    {
        constexpr int count = 64;

        TServerConfig serverConfig;
        serverConfig.MaxInflightRequests = 128;
        serverConfig.MaxDataSize = 1 << 20;
        StartServer(serverConfig);
        TClientConfig clientConfig;
        clientConfig.MaxInflightRequests = 128;
        clientConfig.MaxDataSize = 1 << 20;
        StartClient(clientConfig);
        WaitConnected();

        TVector<TFuture<TResult>> futures;
        TVector<TString> payloads;
        TVector<TString> results;
        for (int i = 0; i < count; ++i) {
            payloads.push_back(Pattern(16 << 10, i));
            results.push_back(TString::Uninitialized(payloads.back().size()));
        }
        for (int i = 0; i < count; ++i) {
            futures.push_back(Collector->NewPromiseFuture());
            TGuardedSgList send(
                {TBlockDataRef{payloads[i].data(), payloads[i].size()}});
            TGuardedSgList recv(
                {TBlockDataRef{results[i].data(), results[i].size()}});
            Endpoint->SendRequest(MakeRequest("echo", send, recv), nullptr);
        }
        for (auto& f: futures) {
            UNIT_ASSERT_VALUES_EQUAL(
                0u,
                f.GetValue(TDuration::Seconds(20)).Status);
        }
    }

    Y_UNIT_TEST_F(ShouldFailBeforeConnected, TFixture)
    {
        Port = 1;   // nothing listens here
        StartClient();
        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("ping"), nullptr);
        auto result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, result.Status);
    }

    Y_UNIT_TEST_F(ShouldTimeOutAndReleaseGuard, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();
        ServerHandler->Hold = true;

        auto payload = Pattern(4096, 3);
        TGuardedSgList send({TBlockDataRef{payload.data(), payload.size()}});
        Collector->CloseBuffers = true;
        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(
            MakeRequest("slow", send, {}, TDuration::MilliSeconds(300)),
            nullptr);

        auto result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, result.Status);
    }

    Y_UNIT_TEST_F(LateOobResponseIsDiscarded, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();
        ServerHandler->Hold = true;

        const size_t size = 1 << 20;
        auto payload = Pattern(size, 4);
        const TString sentinel(size, 'S');
        TString echoed(size, 'S');
        TGuardedSgList send({TBlockDataRef{payload.data(), payload.size()}});
        TGuardedSgList recv({TBlockDataRef{echoed.data(), echoed.size()}});
        auto first = Collector->NewPromiseFuture();
        Endpoint->SendRequest(
            MakeRequest("slow", send, recv, TDuration::MilliSeconds(300)),
            nullptr);
        UNIT_ASSERT_VALUES_EQUAL(
            E_TIMEOUT,
            first.GetValue(TDuration::Seconds(10)).Status);
        recv.Close();

        // the server answers the timed-out request
        void* ctx = TakeHeld(1).front();
        auto late = std::make_shared<TString>(payload);
        ServerEndpoint->SendResponse(
            ctx,
            "slow",
            {TBlockDataRef{late->data(), late->size()}},
            late);

        // the data stream is FIFO, so this payload is read only after the
        // late one has been taken off it
        ServerHandler->Hold = false;
        auto fresh = Pattern(size, 8);
        auto freshEchoed = TString::Uninitialized(size);
        TGuardedSgList freshSend({TBlockDataRef{fresh.data(), fresh.size()}});
        TGuardedSgList freshRecv(
            {TBlockDataRef{freshEchoed.data(), freshEchoed.size()}});
        auto second = Collector->NewPromiseFuture();
        Endpoint->SendRequest(
            MakeRequest("echo", freshSend, freshRecv),
            nullptr);
        auto result = second.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(0u, result.Status);
        UNIT_ASSERT_VALUES_EQUAL(size, result.DataBytes);
        UNIT_ASSERT(fresh == freshEchoed);
        UNIT_ASSERT(echoed == sentinel);
    }

    Y_UNIT_TEST_F(ShouldCancelRequest, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();
        ServerHandler->Hold = true;

        auto future = Collector->NewPromiseFuture();
        const ui64 id = Endpoint->SendRequest(
            MakeRequest("slow", {}, {}, TDuration::Seconds(60)),
            nullptr);
        while (ServerHandler->Received == 0) {
            Sleep(TDuration::MilliSeconds(5));
        }
        Endpoint->CancelRequest(id);
        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            future.GetValue(TDuration::Seconds(10)).Status);
    }

    Y_UNIT_TEST_F(DisconnectDuringPayloadReleasesGuard, TFixture)
    {
        const ui32 size = 1 << 20;
        const auto payload = Pattern(size, 5);
        TScriptedPeer peer([&](int mainFd, int dataFd) {
            const ui64 id = ReadRequest(mainFd);
            SendOobResponseHeader(mainFd, id, size);
            SendDataHeader(dataFd, id, size);
            TryWriteAll(dataFd, payload.data(), size / 2);
            // the client is reading the payload when both sockets close
            Sleep(TDuration::MilliSeconds(100));
        });
        Port = peer.Port;
        StartClient();
        WaitConnected();

        auto request = Pattern(4096, 6);
        auto echoed = TString::Uninitialized(size);
        TGuardedSgList send({TBlockDataRef{request.data(), request.size()}});
        TGuardedSgList recv({TBlockDataRef{echoed.data(), echoed.size()}});
        Collector->CloseBuffers = true;
        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("echo", send, recv), nullptr);

        auto result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, result.Status);
        Events->Disconnected.GetFuture().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldReconnectAfterServerRestart, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();

        StopServer();
        Events->Disconnected.GetFuture().GetValue(TDuration::Seconds(10));

        ServerHandler = std::make_shared<TEchoHandler>();
        StartServer();   // same Port
        Endpoint->TryForceReconnect();

        const auto deadline = TInstant::Now() + TDuration::Seconds(20);
        while (Events->Connects < 2 && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT(Events->Connects >= 2);

        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("ping"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            future.GetValue(TDuration::Seconds(10)).Status);
    }

    Y_UNIT_TEST_F(ShouldReportUnavailable, TFixture)
    {
        Port = 1;
        TClientConfig config;
        config.MaxReconnectDelay = TDuration::MilliSeconds(200);
        StartClient(config);
        Events->Unavailable.GetFuture().GetValue(TDuration::Seconds(20));
    }

    Y_UNIT_TEST_F(InflightLimitRejects, TFixture)
    {
        StartServer();
        TClientConfig config;
        config.MaxInflightRequests = 2;
        StartClient(config);
        WaitConnected();
        ServerHandler->Hold = true;

        TVector<TFuture<TResult>> futures;
        for (int i = 0; i < 3; ++i) {
            futures.push_back(Collector->NewPromiseFuture());
            Endpoint->SendRequest(
                MakeRequest("slow", {}, {}, TDuration::Seconds(60)),
                nullptr);
        }

        // the collector fills its futures in completion order, so the
        // rejection may land in any of them; the two admitted requests
        // complete once the server lets them go
        for (void* ctx: TakeHeld(2)) {
            ServerEndpoint->SendResponse(ctx, "slow", {}, nullptr);
        }
        int rejected = 0;
        int succeeded = 0;
        for (auto& f: futures) {
            const ui32 status = f.GetValue(TDuration::Seconds(10)).Status;
            rejected += status == E_REJECTED;
            succeeded += status == S_OK;
        }
        UNIT_ASSERT_VALUES_EQUAL(1, rejected);
        UNIT_ASSERT_VALUES_EQUAL(2, succeeded);
    }

    Y_UNIT_TEST_F(StopFailsQueuedRequests, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();
        ServerHandler->Hold = true;

        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(
            MakeRequest("slow", {}, {}, TDuration::Seconds(60)),
            nullptr);
        auto stopped = Endpoint->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            future.GetValue(TDuration::Seconds(10)).Status);
        stopped.GetValue(TDuration::Seconds(10));
        Endpoint.reset();
    }

    Y_UNIT_TEST_F(ShouldAcceptRequestsFromManyThreads, TFixture)
    {
        constexpr int threads = 4;
        constexpr int perThread = 32;

        TServerConfig serverConfig;
        serverConfig.MaxInflightRequests = threads * perThread;
        serverConfig.MaxDataSize = 1 << 20;
        StartServer(serverConfig);
        TClientConfig clientConfig;
        clientConfig.MaxInflightRequests = threads * perThread;
        clientConfig.MaxDataSize = 1 << 20;
        StartClient(clientConfig);
        WaitConnected();

        TVector<TFuture<TResult>> futures;
        for (int i = 0; i < threads * perThread; ++i) {
            futures.push_back(Collector->NewPromiseFuture());
        }
        TVector<std::thread> workers;
        for (int t = 0; t < threads; ++t) {
            workers.emplace_back([&] {
                for (int i = 0; i < perThread; ++i) {
                    Endpoint->SendRequest(MakeRequest("ping"), nullptr);
                }
            });
        }
        for (auto& w: workers) {
            w.join();
        }
        for (auto& f: futures) {
            UNIT_ASSERT_VALUES_EQUAL(
                0u,
                f.GetValue(TDuration::Seconds(30)).Status);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsAfterClientStop, TFixture)
    {
        StartServer();
        StartClient();
        WaitConnected();
        Client->Stop();

        auto future = Collector->NewPromiseFuture();
        const ui64 id = Endpoint->SendRequest(MakeRequest("ping"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            future.GetValue(TDuration::Seconds(10)).Status);
        Endpoint->CancelRequest(id);
        Endpoint->TryForceReconnect();
        Endpoint->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldRejectRequestOverNegotiatedDataSize, TFixture)
    {
        TServerConfig serverConfig;
        serverConfig.MaxDataSize = 1 << 20;
        StartServer(serverConfig);
        StartClient();
        WaitConnected();

        auto payload = Pattern(2 << 20, 6);
        TGuardedSgList send({TBlockDataRef{payload.data(), payload.size()}});
        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("big", send), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            future.GetValue(TDuration::Seconds(10)).Status);
        send.Close();

        // only the request failed, the connection is still up
        auto next = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("ping"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            next.GetValue(TDuration::Seconds(10)).Status);
        UNIT_ASSERT_VALUES_EQUAL(1, Events->Connects.load());
    }

    Y_UNIT_TEST_F(ShouldReconnectOnBadWelcome, TFixture)
    {
        TWelcome welcome;
        welcome.OobMode = 7;
        welcome.MaxProtoSize = 64 << 10;
        welcome.MaxDataSize = 1 << 20;
        welcome.MaxInflightBytes = 256 << 20;
        welcome.OobThreshold = 128 << 10;
        TFakeServer fake(welcome);
        Port = fake.Port;

        TClientConfig config;
        config.MaxReconnectDelay = TDuration::MilliSeconds(200);
        StartClient(config);
        Events->Unavailable.GetFuture().GetValue(TDuration::Seconds(20));
        UNIT_ASSERT_VALUES_EQUAL(0, Events->Connects.load());
    }

    Y_UNIT_TEST_F(ShouldGiveUpOnHungHandshake, TFixture)
    {
        // the kernel completes connects into the backlog of a listener that
        // never accepts, so the welcome never comes
        auto [fd, error] = CreateListenSocket("127.0.0.1", 0, 16);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        Y_DEFER {
            CloseSocket(fd);
        };
        auto [port, perr] = GetLocalPort(fd);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));
        Port = port;

        TClientConfig config;
        config.MaxResponseDelay = TDuration::MilliSeconds(200);
        config.MaxReconnectDelay = TDuration::MilliSeconds(200);
        StartClient(config);
        Events->Unavailable.GetFuture().GetValue(TDuration::Seconds(20));
        UNIT_ASSERT_VALUES_EQUAL(0, Events->Connects.load());
    }

    Y_UNIT_TEST_F(ShouldDisconnectOnDuplicateResponse, TFixture)
    {
        const ui32 size = 1 << 20;
        const auto payload = Pattern(size, 7);
        TScriptedPeer peer([&](int mainFd, int dataFd) {
            const ui64 id = ReadRequest(mainFd);
            SendOobResponseHeader(mainFd, id, size);
            SendDataHeader(dataFd, id, size);
            TryWriteAll(dataFd, payload.data(), size / 2);
            // the client is reading the payload when the second response
            // for the same request comes, payload and all
            Sleep(TDuration::MilliSeconds(100));
            SendOobResponseHeader(mainFd, id, size);
            TryWriteAll(dataFd, payload.data() + size / 2, size - size / 2);
            SendDataHeader(dataFd, id, size);
            TryWriteAll(dataFd, payload.data(), size);
            WaitForEof(mainFd);
        });
        Port = peer.Port;
        StartClient();
        WaitConnected();

        auto echoed = TString::Uninitialized(size);
        TGuardedSgList recv({TBlockDataRef{echoed.data(), echoed.size()}});
        Collector->CloseBuffers = true;
        auto future = Collector->NewPromiseFuture();
        Endpoint->SendRequest(MakeRequest("dup", {}, recv), nullptr);

        auto result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, result.Status);
        Events->Disconnected.GetFuture().GetValue(TDuration::Seconds(10));
    }
}

}   // namespace NCloud::NBlockStore::NUring
