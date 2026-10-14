#include <cloud/blockstore/libs/uring/transport/connection.h>
#include <cloud/blockstore/libs/uring/transport/socket.h>

#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/network/socket.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace NCloud::NBlockStore::NUring {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTcpPair
{
    int Client = -1;
    int Server = -1;

    TTcpPair()
    {
        auto [listenFd, error] = CreateListenSocket("127.0.0.1", 0, 4);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto [port, perr] = GetLocalPort(listenFd);
        UNIT_ASSERT_C(!HasError(perr), FormatError(perr));

        Client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        UNIT_ASSERT(::connect(Client, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        Server = ::accept4(listenFd, nullptr, nullptr, SOCK_CLOEXEC);
        UNIT_ASSERT(Server >= 0);
        ::close(listenFd);
    }

    // Small enough to cut every large send into many partial ones, but not
    // below the loopback MSS: with SO_RCVBUF=4096 the advertised window never
    // reaches one 64K segment, TCP falls back to zero-window probes and moves
    // 32K per ~800ms (plain blocking sockets need 53s for 2MB that way).
    void SetSmallBuffers()
    {
        for (int fd: {Client, Server}) {
            int size = 1 << 16;
            ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
            ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
        }
    }
};

struct TReceived
{
    TFrameHeader Header;
    TString Proto;
    TString Data;
};

// Collects frames; for OOB frames allocates a buffer of DataLen bytes
// (or discards when DiscardNext is set).
struct TTestHandler: public IConnectionHandler
{
    TVector<TReceived> Frames;
    TVector<std::shared_ptr<TString>> Buffers;
    bool DiscardNext = false;
    TPromise<void> FrameArrived = NewPromise();
    TPromise<NProto::TError> Closed = NewPromise<NProto::TError>();
    TVector<NProto::TError> SinkResults;

    void OnFrame(const TFrameHeader& h, TStringBuf proto, TStringBuf data) override
    {
        Frames.push_back({h, TString(proto), TString(data)});
        Signal();
    }

    TDataSink OnOobFrame(const TFrameHeader& h, TStringBuf proto) override
    {
        auto buffer = std::make_shared<TString>(TString::Uninitialized(h.DataLen));
        Buffers.push_back(buffer);
        TReceived r{h, TString(proto), {}};
        const size_t index = Frames.size();
        Frames.push_back(std::move(r));

        TDataSink sink;
        if (!DiscardNext && h.DataLen) {
            sink.Target = {TBlockDataRef{buffer->data(), buffer->size()}};
        }
        DiscardNext = false;
        sink.OnDone = [this, buffer, index](const NProto::TError& e) {
            SinkResults.push_back(e);
            if (!HasError(e)) {
                Frames[index].Data = *buffer;
            }
            Signal();
        };
        return sink;
    }

    void OnConnectionClosed(const NProto::TError& error) override
    {
        Closed.TrySetValue(error);
    }

    void Signal()
    {
        FrameArrived.TrySetValue();
    }

    void Rearm()
    {
        FrameArrived = NewPromise();
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

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    std::unique_ptr<TRing> Ring;
    TTcpPair Main;
    TTcpPair Data;
    TTestHandler A;
    TTestHandler B;
    std::unique_ptr<TConnection> ConnA;
    std::unique_ptr<TConnection> ConnB;

    void SetUp(NUnitTest::TTestContext&) override
    {
        Ring = std::make_unique<TRing>(
            "URING.UT", 256, ESendZeroCopyMode::Auto, Logging->CreateLog("URING"));
        auto error = Ring->Start();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
    }

    template <typename F>
    auto OnRing(F f)
    {
        auto promise = NewPromise<decltype(f())>();
        Ring->Execute([=]() mutable { promise.SetValue(f()); });
        return promise.GetFuture().GetValue(TDuration::Seconds(5));
    }

    void TearDown(NUnitTest::TTestContext&) override
    {
        OnRing([&] {
            if (ConnA) { ConnA->Close(MakeError(E_REJECTED, "test end")); }
            if (ConnB) { ConnB->Close(MakeError(E_REJECTED, "test end")); }
            return 0;
        });
        if (ConnA) { A.Closed.GetFuture().GetValue(TDuration::Seconds(5)); }
        if (ConnB) { B.Closed.GetFuture().GetValue(TDuration::Seconds(5)); }
        OnRing([&] { ConnA.reset(); ConnB.reset(); return 0; });
        Ring->Stop();
    }

    TConnectionParams Params(bool oob, ui32 oobThreshold = 0, bool zc = false)
    {
        return TConnectionParams{
            .MaxProtoSize = 64 << 10,
            .MaxDataSize = 8 << 20,
            .SendWindowBytes = 1 << 20,
            .OobEnabled = oob,
            .OobThreshold = oobThreshold,
            .SendZeroCopy = zc,
            .SendZeroCopyThreshold = 0,
        };
    }

    void Connect(TConnectionParams params)
    {
        OnRing([&] {
            ConnA = std::make_unique<TConnection>(
                *Ring, params, Main.Client, params.OobEnabled ? Data.Client : -1, &A,
                Logging->CreateLog("A"));
            ConnB = std::make_unique<TConnection>(
                *Ring, params, Main.Server, params.OobEnabled ? Data.Server : -1, &B,
                Logging->CreateLog("B"));
            ConnA->Start();
            ConnB->Start();
            return 0;
        });
    }

    static TOutFramePtr MakeFrame(ui64 id, TStringBuf proto, TSgList data, std::function<void()> onDone = {})
    {
        auto frame = std::make_shared<TOutFrame>();
        frame->Head = TString::Uninitialized(sizeof(TFrameHeader) + proto.size());
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.RequestId = id;
        h.ProtoLen = proto.size();
        memcpy(frame->Head.begin(), &h, sizeof(h));
        memcpy(frame->Head.begin() + sizeof(h), proto.data(), proto.size());
        frame->Data = std::move(data);
        frame->OnDone = std::move(onDone);
        return frame;
    }

    void WaitFrame(TTestHandler& h, size_t count)
    {
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        for (;;) {
            size_t have = OnRing([&] {
                size_t ready = 0;
                for (auto& f: h.Frames) {
                    if (!(f.Header.Flags & FLAG_OOB) || f.Data.size() == f.Header.DataLen) {
                        ++ready;
                    }
                }
                return ready;
            });
            if (have >= count) {
                return;
            }
            UNIT_ASSERT_C(TInstant::Now() < deadline, "timed out waiting for frames");
            Sleep(TDuration::MilliSeconds(5));
        }
    }

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUringConnectionTest)
{
    Y_UNIT_TEST_F(ShouldPassProtoOnlyFrame, TFixture)
    {
        Connect(Params(false));
        OnRing([&] { ConnA->Send(MakeFrame(7, "ping", {})); return 0; });
        WaitFrame(B, 1);
        auto f = OnRing([&] { return B.Frames[0]; });
        UNIT_ASSERT_VALUES_EQUAL(7u, f.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL("ping", f.Proto);
        UNIT_ASSERT_VALUES_EQUAL(0u, ui32(f.Header.Flags));
        UNIT_ASSERT_VALUES_EQUAL(0u, f.Data.size());
    }

    Y_UNIT_TEST_F(ShouldPassInlineData, TFixture)
    {
        Connect(Params(true, 1 << 20));   // OOB on but threshold above payload
        auto payload = Pattern(4096, 1);
        OnRing([&] {
            ConnA->Send(MakeFrame(1, "write", {TBlockDataRef{payload.data(), payload.size()}}));
            return 0;
        });
        WaitFrame(B, 1);
        auto f = OnRing([&] { return B.Frames[0]; });
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_INLINE), ui32(f.Header.Flags));
        UNIT_ASSERT_VALUES_EQUAL(payload, f.Data);
    }

    Y_UNIT_TEST_F(ShouldPassOobData, TFixture)
    {
        Connect(Params(true, 0));
        auto payload = Pattern(1 << 20, 2);
        bool done = false;
        OnRing([&] {
            ConnA->Send(MakeFrame(1, "write", {TBlockDataRef{payload.data(), payload.size()}}, [&] { done = true; }));
            return 0;
        });
        WaitFrame(B, 1);
        auto f = OnRing([&] { return B.Frames[0]; });
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_OOB), ui32(f.Header.Flags));
        UNIT_ASSERT_VALUES_EQUAL(payload.size(), f.Header.DataLen);
        UNIT_ASSERT(payload == f.Data);
        UNIT_ASSERT(OnRing([&] { return done; }));
    }

    Y_UNIT_TEST_F(ShouldPassManyIovecsBothWays, TFixture)
    {
        Connect(Params(true, 100000));
        TVector<TString> parts;
        TSgList inlineList, oobList;
        TString expected;
        for (int i = 0; i < 200; ++i) {
            parts.push_back(Pattern(100, i));
            expected += parts.back();
        }
        for (auto& p: parts) {
            inlineList.push_back(TBlockDataRef{p.data(), p.size()});
        }
        TVector<TString> big;
        TString expectedBig;
        for (int i = 0; i < 200; ++i) {
            big.push_back(Pattern(1000, i + 7));
            expectedBig += big.back();
        }
        for (auto& p: big) {
            oobList.push_back(TBlockDataRef{p.data(), p.size()});
        }
        OnRing([&] {
            ConnA->Send(MakeFrame(1, "inline", inlineList));
            ConnA->Send(MakeFrame(2, "oob", oobList));
            return 0;
        });
        WaitFrame(B, 2);
        auto frames = OnRing([&] { return B.Frames; });
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_INLINE), ui32(frames[0].Header.Flags));
        UNIT_ASSERT(expected == frames[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_OOB), ui32(frames[1].Header.Flags));
        UNIT_ASSERT(expectedBig == frames[1].Data);
    }

    Y_UNIT_TEST_F(ShouldDiscardOobWithoutSink, TFixture)
    {
        Connect(Params(true, 0));
        auto p1 = Pattern(70000, 3);
        auto p2 = Pattern(300, 4);
        OnRing([&] {
            B.DiscardNext = true;
            ConnA->Send(MakeFrame(1, "a", {TBlockDataRef{p1.data(), p1.size()}}));
            ConnA->Send(MakeFrame(2, "b", {TBlockDataRef{p2.data(), p2.size()}}));
            return 0;
        });
        WaitFrame(B, 2);
        auto frames = OnRing([&] { return B.Frames; });
        UNIT_ASSERT_VALUES_EQUAL(2u, frames.size());
        UNIT_ASSERT(p2 == frames[1].Data);
        auto results = OnRing([&] { return B.SinkResults; });
        UNIT_ASSERT_VALUES_EQUAL(2u, results.size());
        UNIT_ASSERT(!HasError(results[0]));
    }

    Y_UNIT_TEST_F(ShouldSurviveTinySocketBuffers, TFixture)
    {
        Main.SetSmallBuffers();
        Data.SetSmallBuffers();
        Connect(Params(true, 0));
        auto payload = Pattern(2 << 20, 5);
        OnRing([&] {
            ConnA->Send(MakeFrame(1, "big", {TBlockDataRef{payload.data(), payload.size()}}));
            ConnB->Send(MakeFrame(2, "back", {TBlockDataRef{payload.data(), payload.size()}}));
            return 0;
        });
        WaitFrame(B, 1);
        WaitFrame(A, 1);
        UNIT_ASSERT(payload == OnRing([&] { return B.Frames[0].Data; }));
        UNIT_ASSERT(payload == OnRing([&] { return A.Frames[0].Data; }));
    }

    Y_UNIT_TEST_F(ShouldSendZeroCopyWhenSupported, TFixture)
    {
        Connect(Params(true, 0, true));
        auto payload = Pattern(1 << 20, 6);
        bool done = false;
        OnRing([&] {
            ConnA->Send(MakeFrame(1, "zc", {TBlockDataRef{payload.data(), payload.size()}}, [&] { done = true; }));
            return 0;
        });
        WaitFrame(B, 1);
        UNIT_ASSERT(payload == OnRing([&] { return B.Frames[0].Data; }));
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (!OnRing([&] { return done; }) && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(5));
        }
        UNIT_ASSERT(OnRing([&] { return done; }));
    }

    Y_UNIT_TEST_F(ShouldCloseOnBadMagic, TFixture)
    {
        OnRing([&] {
            ConnB = std::make_unique<TConnection>(
                *Ring, Params(false), Main.Server, -1, &B, Logging->CreateLog("B"));
            ConnB->Start();
            return 0;
        });
        const char garbage[32] = "this is not a frame header....";
        UNIT_ASSERT(::send(Main.Client, garbage, sizeof(garbage), MSG_NOSIGNAL) == sizeof(garbage));
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        ::close(Main.Client);
    }

    Y_UNIT_TEST_F(ShouldCloseOnPeerEof, TFixture)
    {
        OnRing([&] {
            ConnB = std::make_unique<TConnection>(
                *Ring, Params(false), Main.Server, -1, &B, Logging->CreateLog("B"));
            ConnB->Start();
            return 0;
        });
        ::close(Main.Client);
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST_F(DataFifoMismatchCloses, TFixture)
    {
        OnRing([&] {
            ConnB = std::make_unique<TConnection>(
                *Ring, Params(true, 0), Main.Server, Data.Server, &B, Logging->CreateLog("B"));
            ConnB->Start();
            return 0;
        });
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.Flags = FLAG_OOB;
        h.RequestId = 1;
        h.ProtoLen = 0;
        h.DataLen = 4;
        UNIT_ASSERT(::send(Main.Client, &h, sizeof(h), MSG_NOSIGNAL) == sizeof(h));
        TDataHeader d;
        d.RequestId = 2;   // not the FIFO head
        d.DataLen = 4;
        UNIT_ASSERT(::send(Data.Client, &d, sizeof(d), MSG_NOSIGNAL) == sizeof(d));
        UNIT_ASSERT(::send(Data.Client, "abcd", 4, MSG_NOSIGNAL) == 4);

        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        auto results = OnRing([&] { return B.SinkResults; });
        UNIT_ASSERT_VALUES_EQUAL(1u, results.size());
        UNIT_ASSERT(HasError(results[0]));
        ::close(Main.Client);
        ::close(Data.Client);
    }

    Y_UNIT_TEST_F(ShouldRejectOobFrameWhenOobDisabled, TFixture)
    {
        OnRing([&] {
            ConnB = std::make_unique<TConnection>(
                *Ring, Params(false), Main.Server, -1, &B, Logging->CreateLog("B"));
            ConnB->Start();
            return 0;
        });
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.Flags = FLAG_OOB;
        h.RequestId = 1;
        h.DataLen = 4;
        UNIT_ASSERT(::send(Main.Client, &h, sizeof(h), MSG_NOSIGNAL) == sizeof(h));
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        ::close(Main.Client);
    }

}

}   // namespace NCloud::NBlockStore::NUring
