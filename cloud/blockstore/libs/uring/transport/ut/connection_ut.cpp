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

#include <atomic>

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
        UNIT_ASSERT(
            ::connect(
                Client,
                reinterpret_cast<sockaddr*>(&addr),
                sizeof(addr)) == 0);
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
    // scatters the next sinks over refs of this size, as a client reading into
    // a caller's block list does
    size_t SinkPieceSize = 0;
    bool MismatchNextSink = false;
    TPromise<void> FrameArrived = NewPromise();
    TPromise<NProto::TError> Closed = NewPromise<NProto::TError>();
    TVector<NProto::TError> SinkResults;
    size_t ClosedCount = 0;

    void
    OnFrame(const TFrameHeader& h, TStringBuf proto, TStringBuf data) override
    {
        Frames.push_back({h, TString(proto), TString(data)});
        Signal();
    }

    TDataSink OnOobFrame(const TFrameHeader& h, TStringBuf proto) override
    {
        auto buffer =
            std::make_shared<TString>(TString::Uninitialized(h.DataLen));
        Buffers.push_back(buffer);
        TReceived r{h, TString(proto), {}};
        const size_t index = Frames.size();
        Frames.push_back(std::move(r));

        TDataSink sink;
        if (!DiscardNext && h.DataLen) {
            if (MismatchNextSink) {
                sink.Target = {
                    TBlockDataRef{buffer->data(), buffer->size() / 2}};
            } else if (SinkPieceSize) {
                for (size_t offset = 0; offset < buffer->size();
                     offset += SinkPieceSize)
                {
                    sink.Target.push_back(TBlockDataRef{
                        buffer->data() + offset,
                        Min(SinkPieceSize, buffer->size() - offset)});
                }
            } else {
                sink.Target = {TBlockDataRef{buffer->data(), buffer->size()}};
            }
        }
        DiscardNext = false;
        MismatchNextSink = false;
        sink.OnDone = [this, buffer, index](const NProto::TError& e)
        {
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
        ++ClosedCount;
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
            "URING.UT",
            256,
            ESendZeroCopyMode::Auto,
            Logging->CreateLog("URING"));
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
        OnRing(
            [&]
            {
                if (ConnA) {
                    ConnA->Close(MakeError(E_REJECTED, "test end"));
                }
                if (ConnB) {
                    ConnB->Close(MakeError(E_REJECTED, "test end"));
                }
                return 0;
            });
        if (ConnA) {
            A.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        }
        if (ConnB) {
            B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        }
        OnRing(
            [&]
            {
                ConnA.reset();
                ConnB.reset();
                return 0;
            });
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
        OnRing(
            [&]
            {
                ConnA = std::make_unique<TConnection>(
                    *Ring,
                    params,
                    Main.Client,
                    params.OobEnabled ? Data.Client : -1,
                    &A,
                    Logging->CreateLog("A"));
                ConnB = std::make_unique<TConnection>(
                    *Ring,
                    params,
                    Main.Server,
                    params.OobEnabled ? Data.Server : -1,
                    &B,
                    Logging->CreateLog("B"));
                ConnA->Start();
                ConnB->Start();
                return 0;
            });
    }

    static TOutFramePtr MakeFrame(
        ui64 id,
        TStringBuf proto,
        TSgList data,
        std::function<void()> onDone = {})
    {
        auto frame = std::make_shared<TOutFrame>();
        frame->Head =
            TString::Uninitialized(sizeof(TFrameHeader) + proto.size());
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
            size_t have = OnRing(
                [&]
                {
                    size_t ready = 0;
                    for (auto& f: h.Frames) {
                        if (!(f.Header.Flags & FLAG_OOB) ||
                            f.Data.size() == f.Header.DataLen)
                        {
                            ++ready;
                        }
                    }
                    return ready;
                });
            if (have >= count) {
                return;
            }
            UNIT_ASSERT_C(
                TInstant::Now() < deadline,
                "timed out waiting for frames");
            Sleep(TDuration::MilliSeconds(5));
        }
    }

    // Queues more than the sockets can take with nobody reading them, closes
    // the connection with sends still in flight and checks that every frame is
    // released exactly once.
    void CheckCloseWithQueuedSends(bool zc)
    {
        Main.SetSmallBuffers();
        Data.SetSmallBuffers();
        auto params = Params(true, 0, zc);
        OnRing(
            [&]
            {
                ConnA = std::make_unique<TConnection>(
                    *Ring,
                    params,
                    Main.Client,
                    Data.Client,
                    &A,
                    Logging->CreateLog("A"));
                ConnA->Start();
                return 0;
            });

        // the sockets take a few of these and leave the rest queued
        constexpr ui64 count = 8;
        auto payload = Pattern(128 << 10, 11);
        std::atomic<size_t> released = 0;
        OnRing(
            [&]
            {
                for (ui64 id = 1; id <= count; ++id) {
                    ConnA->Send(MakeFrame(
                        id,
                        "bye",
                        {TBlockDataRef{payload.data(), payload.size()}},
                        [&] { ++released; }));
                }
                return 0;
            });
        Sleep(TDuration::MilliSeconds(100));
        if (zc && Ring->SendZeroCopySupported()) {
            // the peer does not read, so none of these bytes can be
            // acknowledged, the kernel still holds every zero-copy page and no
            // frame may be reported as sent yet
            UNIT_ASSERT_VALUES_EQUAL(0u, released.load());
        }

        OnRing(
            [&]
            {
                ConnA->Close(
                    MakeError(E_REJECTED, "closing with sends queued"));
                return 0;
            });
        // the peer is still there and still not reading: closing must not
        // wait for it to drain the zero-copy pages it holds
        auto error = A.Closed.GetFuture().GetValue(TDuration::Seconds(10));
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(count, released.load());
        UNIT_ASSERT_VALUES_EQUAL(1u, OnRing([&] { return A.ClosedCount; }));
        ::close(Main.Server);
        ::close(Data.Server);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUringConnectionTest)
{
    Y_UNIT_TEST_F(ShouldPassProtoOnlyFrame, TFixture)
    {
        Connect(Params(false));
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(7, "ping", {}));
                return 0;
            });
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
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(
                    1,
                    "write",
                    {TBlockDataRef{payload.data(), payload.size()}}));
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
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(
                    1,
                    "write",
                    {TBlockDataRef{payload.data(), payload.size()}},
                    [&] { done = true; }));
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
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(1, "inline", inlineList));
                ConnA->Send(MakeFrame(2, "oob", oobList));
                return 0;
            });
        WaitFrame(B, 2);
        auto frames = OnRing([&] { return B.Frames; });
        UNIT_ASSERT_VALUES_EQUAL(
            ui32(FLAG_INLINE),
            ui32(frames[0].Header.Flags));
        UNIT_ASSERT(expected == frames[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_OOB), ui32(frames[1].Header.Flags));
        UNIT_ASSERT(expectedBig == frames[1].Data);
    }

    Y_UNIT_TEST_F(ShouldDiscardOobWithoutSink, TFixture)
    {
        Connect(Params(true, 0));
        auto p1 = Pattern(70000, 3);
        auto p2 = Pattern(300, 4);
        OnRing(
            [&]
            {
                B.DiscardNext = true;
                ConnA->Send(
                    MakeFrame(1, "a", {TBlockDataRef{p1.data(), p1.size()}}));
                ConnA->Send(
                    MakeFrame(2, "b", {TBlockDataRef{p2.data(), p2.size()}}));
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
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(
                    1,
                    "big",
                    {TBlockDataRef{payload.data(), payload.size()}}));
                ConnB->Send(MakeFrame(
                    2,
                    "back",
                    {TBlockDataRef{payload.data(), payload.size()}}));
                return 0;
            });
        WaitFrame(B, 1);
        WaitFrame(A, 1);
        UNIT_ASSERT(payload == OnRing([&] { return B.Frames[0].Data; }));
        UNIT_ASSERT(payload == OnRing([&] { return A.Frames[0].Data; }));
    }

    Y_UNIT_TEST_F(ShouldSendZeroCopyWhenSupported, TFixture)
    {
        if (!Ring->SendZeroCopySupported()) {
            Cerr << "IORING_OP_SENDMSG_ZC is not supported, skipping" << Endl;
            return;
        }
        Connect(Params(true, 0, true));
        auto payload = Pattern(1 << 20, 6);
        bool done = false;
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(
                    1,
                    "zc",
                    {TBlockDataRef{payload.data(), payload.size()}},
                    [&] { done = true; }));
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
        OnRing(
            [&]
            {
                ConnB = std::make_unique<TConnection>(
                    *Ring,
                    Params(false),
                    Main.Server,
                    -1,
                    &B,
                    Logging->CreateLog("B"));
                ConnB->Start();
                return 0;
            });
        const char garbage[32] = "this is not a frame header....";
        UNIT_ASSERT(
            ::send(Main.Client, garbage, sizeof(garbage), MSG_NOSIGNAL) ==
            sizeof(garbage));
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        ::close(Main.Client);
    }

    Y_UNIT_TEST_F(ShouldCloseOnPeerEof, TFixture)
    {
        OnRing(
            [&]
            {
                ConnB = std::make_unique<TConnection>(
                    *Ring,
                    Params(false),
                    Main.Server,
                    -1,
                    &B,
                    Logging->CreateLog("B"));
                ConnB->Start();
                return 0;
            });
        ::close(Main.Client);
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST_F(DataFifoMismatchCloses, TFixture)
    {
        OnRing(
            [&]
            {
                ConnB = std::make_unique<TConnection>(
                    *Ring,
                    Params(true, 0),
                    Main.Server,
                    Data.Server,
                    &B,
                    Logging->CreateLog("B"));
                ConnB->Start();
                return 0;
            });
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.Flags = FLAG_OOB;
        h.RequestId = 1;
        h.ProtoLen = 0;
        h.DataLen = 4;
        UNIT_ASSERT(
            ::send(Main.Client, &h, sizeof(h), MSG_NOSIGNAL) == sizeof(h));
        TDataHeader d;
        d.RequestId = 2;   // not the FIFO head
        d.DataLen = 4;
        UNIT_ASSERT(
            ::send(Data.Client, &d, sizeof(d), MSG_NOSIGNAL) == sizeof(d));
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
        OnRing(
            [&]
            {
                ConnB = std::make_unique<TConnection>(
                    *Ring,
                    Params(false),
                    Main.Server,
                    -1,
                    &B,
                    Logging->CreateLog("B"));
                ConnB->Start();
                return 0;
            });
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.Flags = FLAG_OOB;
        h.RequestId = 1;
        h.DataLen = 4;
        UNIT_ASSERT(
            ::send(Main.Client, &h, sizeof(h), MSG_NOSIGNAL) == sizeof(h));
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        ::close(Main.Client);
    }

    Y_UNIT_TEST_F(ShouldPickOobByThreshold, TFixture)
    {
        Connect(Params(true, 4096));
        auto below = Pattern(4095, 7);
        auto atThreshold = Pattern(4096, 8);
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(
                    1,
                    "below",
                    {TBlockDataRef{below.data(), below.size()}}));
                ConnA->Send(MakeFrame(
                    2,
                    "at",
                    {TBlockDataRef{atThreshold.data(), atThreshold.size()}}));
                return 0;
            });
        WaitFrame(B, 2);
        auto frames = OnRing([&] { return B.Frames; });
        UNIT_ASSERT_VALUES_EQUAL(2u, frames.size());
        UNIT_ASSERT_VALUES_EQUAL(
            ui32(FLAG_INLINE),
            ui32(frames[0].Header.Flags));
        UNIT_ASSERT(below == frames[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_OOB), ui32(frames[1].Header.Flags));
        UNIT_ASSERT(atThreshold == frames[1].Data);
    }

    Y_UNIT_TEST_F(ShouldParseFrameArrivingByteByByte, TFixture)
    {
        OnRing(
            [&]
            {
                ConnB = std::make_unique<TConnection>(
                    *Ring,
                    Params(false),
                    Main.Server,
                    -1,
                    &B,
                    Logging->CreateLog("B"));
                ConnB->Start();
                return 0;
            });
        auto payload = Pattern(100, 9);
        TFrameHeader h;
        h.Type = FRAME_RESPONSE;
        h.Flags = FLAG_INLINE;
        h.RequestId = 42;
        h.ProtoLen = 5;
        h.DataLen = payload.size();
        TString wire(reinterpret_cast<const char*>(&h), sizeof(h));
        wire += "proto";
        wire += payload;
        for (char c: wire) {
            UNIT_ASSERT(::send(Main.Client, &c, 1, MSG_NOSIGNAL) == 1);
        }
        WaitFrame(B, 1);
        auto f = OnRing([&] { return B.Frames[0]; });
        UNIT_ASSERT_VALUES_EQUAL(42u, f.Header.RequestId);
        UNIT_ASSERT_VALUES_EQUAL("proto", f.Proto);
        UNIT_ASSERT(payload == f.Data);
        ::close(Main.Client);
    }

    Y_UNIT_TEST_F(ShouldGrowRecvBufferForLargeInlineFrame, TFixture)
    {
        Connect(Params(false));   // OOB off, so the payload goes inline
        auto payload = Pattern(200000, 10);
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(
                    1,
                    "inline",
                    {TBlockDataRef{payload.data(), payload.size()}}));
                return 0;
            });
        WaitFrame(B, 1);
        auto f = OnRing([&] { return B.Frames[0]; });
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_INLINE), ui32(f.Header.Flags));
        UNIT_ASSERT(payload == f.Data);
    }

    // Many queued chunks and a socket too small for them: send completions
    // land in the middle of a chunk and have to carry over to the next ones.
    Y_UNIT_TEST_F(ShouldPipelineManyFrames, TFixture)
    {
        Main.SetSmallBuffers();
        Connect(Params(false));
        constexpr size_t count = 500;
        TVector<TString> payloads;
        TVector<TString> protos;
        for (size_t i = 0; i < count; ++i) {
            payloads.push_back(Pattern(i % 300 + 1, i));
            protos.push_back(Pattern(i % 17 + 1, i + 3));
        }
        OnRing(
            [&]
            {
                for (size_t i = 0; i < count; ++i) {
                    ConnA->Send(MakeFrame(
                        i + 1,
                        protos[i],
                        {TBlockDataRef{
                            payloads[i].data(),
                            payloads[i].size()}}));
                }
                return 0;
            });
        WaitFrame(B, count);
        auto frames = OnRing([&] { return B.Frames; });
        UNIT_ASSERT_VALUES_EQUAL(count, frames.size());
        for (size_t i = 0; i < count; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i + 1, frames[i].Header.RequestId);
            UNIT_ASSERT(protos[i] == frames[i].Proto);
            UNIT_ASSERT(payloads[i] == frames[i].Data);
        }
    }

    Y_UNIT_TEST_F(ShouldRunOnDoneWhenSendingWhileClosing, TFixture)
    {
        Connect(Params(false));
        size_t calls = 0;
        auto frame = MakeFrame(1, "late", {}, [&] { ++calls; });
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            OnRing(
                [&]
                {
                    ConnA->Close(MakeError(E_REJECTED, "closed before send"));
                    ConnA->Send(frame);
                    return calls;
                }));
        // the caller still holds the frame, and dropping it must not run
        // OnDone a second time
        frame.reset();
        UNIT_ASSERT_VALUES_EQUAL(1u, calls);
    }

    Y_UNIT_TEST_F(ShouldReleaseQueuedFramesOnClose, TFixture)
    {
        CheckCloseWithQueuedSends(false);
    }

    Y_UNIT_TEST_F(ShouldReleaseQueuedZeroCopyFramesOnClose, TFixture)
    {
        CheckCloseWithQueuedSends(true);
    }

    Y_UNIT_TEST_F(ShouldScatterOobPayloadIntoManyRefs, TFixture)
    {
        Connect(Params(true, 0));
        auto payload = Pattern(1 << 20, 12);
        OnRing(
            [&]
            {
                B.SinkPieceSize = 4096;   // 256 refs, four times MAX_IOV
                ConnA->Send(MakeFrame(
                    1,
                    "scatter",
                    {TBlockDataRef{payload.data(), payload.size()}}));
                return 0;
            });
        WaitFrame(B, 1);
        auto f = OnRing([&] { return B.Frames[0]; });
        UNIT_ASSERT_VALUES_EQUAL(ui32(FLAG_OOB), ui32(f.Header.Flags));
        UNIT_ASSERT(payload == f.Data);
    }

    Y_UNIT_TEST_F(ShouldCloseOnSinkSizeMismatch, TFixture)
    {
        Connect(Params(true, 0));
        auto payload = Pattern(8192, 13);
        OnRing(
            [&]
            {
                B.MismatchNextSink = true;
                ConnA->Send(MakeFrame(
                    1,
                    "bad",
                    {TBlockDataRef{payload.data(), payload.size()}}));
                return 0;
            });
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        auto results = OnRing([&] { return B.SinkResults; });
        UNIT_ASSERT_VALUES_EQUAL(1u, results.size());
        UNIT_ASSERT(HasError(results[0]));
    }

    // 24 bytes claiming a MaxDataSize inline payload must not grow the receive
    // buffer: a sender with a data stream sends those out of band
    Y_UNIT_TEST_F(ShouldRejectInlineFrameAboveOobThreshold, TFixture)
    {
        OnRing(
            [&]
            {
                ConnB = std::make_unique<TConnection>(
                    *Ring,
                    Params(true, 4096),
                    Main.Server,
                    Data.Server,
                    &B,
                    Logging->CreateLog("B"));
                ConnB->Start();
                return 0;
            });
        TFrameHeader h;
        h.Type = FRAME_REQUEST;
        h.Flags = FLAG_INLINE;
        h.RequestId = 1;
        h.DataLen = 8 << 20;
        UNIT_ASSERT(
            ::send(Main.Client, &h, sizeof(h), MSG_NOSIGNAL) == sizeof(h));
        auto error = B.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        ::close(Main.Client);
        ::close(Data.Client);
    }

    Y_UNIT_TEST_F(ShouldCloseWhenSendingOversizedPayload, TFixture)
    {
        auto params = Params(false);
        params.MaxDataSize = 4096;
        Connect(params);
        auto payload = Pattern(8192, 14);
        size_t released = 0;
        OnRing(
            [&]
            {
                ConnA->Send(MakeFrame(
                    1,
                    "big",
                    {TBlockDataRef{payload.data(), payload.size()}},
                    [&] { ++released; }));
                return 0;
            });
        auto error = A.Closed.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(ui32(E_ARGUMENT), error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1u, OnRing([&] { return released; }));
    }
}

}   // namespace NCloud::NBlockStore::NUring
