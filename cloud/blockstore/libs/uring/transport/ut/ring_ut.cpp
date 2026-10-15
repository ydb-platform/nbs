#include <cloud/blockstore/libs/uring/transport/ring.h>
#include <cloud/blockstore/libs/uring/transport/socket.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <thread>

namespace NCloud::NBlockStore::NUring {

using namespace NThreading;

namespace {

struct TFixture: public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    std::unique_ptr<TRing> Ring;

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

    void TearDown(NUnitTest::TTestContext&) override
    {
        Ring->Stop();
    }

    template <typename F>
    auto OnRing(F f)
    {
        auto promise = NewPromise<decltype(f())>();
        Ring->Execute([=]() mutable { promise.SetValue(f()); });
        return promise.GetFuture().GetValue(TDuration::Seconds(5));
    }
};

struct TTicker: public ITickable
{
    std::atomic<int> Ticks = 0;

    void OnTick(TInstant) override
    {
        ++Ticks;
    }
};

struct TRecorder: public IIoHandler
{
    struct TCqe
    {
        int Res;
        ui32 Flags;
    };

    TVector<TCqe> Cqes;
    TPromise<void> Done = NewPromise<void>();

    void OnIoComplete(TIoOp*, int res, ui32 flags) override
    {
        Cqes.push_back({res, flags});
        if (!(flags & IORING_CQE_F_MORE)) {
            Done.SetValue();
        }
    }
};

struct TTcpFixture: public TFixture
{
    int Client = -1;
    int Server = -1;

    void SetUp(NUnitTest::TTestContext& ctx) override
    {
        TFixture::SetUp(ctx);

        auto [listenFd, error] = CreateListenSocket("127.0.0.1", 0, 16);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto [port, portError] = GetLocalPort(listenFd);
        UNIT_ASSERT_C(!HasError(portError), FormatError(portError));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        Client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        UNIT_ASSERT(Client >= 0);

        TRecorder accepted, connected;
        TIoOp* ops[2] = {};
        OnRing(
            [&]
            {
                ops[0] = Ring->AllocOp(&accepted, 1);
                Ring->Accept(ops[0], listenFd);
                ops[1] = Ring->AllocOp(&connected, 2);
                Ring->Connect(
                    ops[1],
                    Client,
                    reinterpret_cast<sockaddr*>(&addr),
                    sizeof(addr));
                return 0;
            });
        accepted.Done.GetFuture().GetValue(TDuration::Seconds(5));
        connected.Done.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(0, connected.Cqes[0].Res);
        UNIT_ASSERT(accepted.Cqes[0].Res >= 0);
        Server = accepted.Cqes[0].Res;

        OnRing(
            [&]
            {
                Ring->FreeOp(ops[0]);
                Ring->FreeOp(ops[1]);
                return 0;
            });
        CloseSocket(listenFd);
    }

    void TearDown(NUnitTest::TTestContext& ctx) override
    {
        TFixture::TearDown(ctx);
        CloseSocket(Client);
        if (Server >= 0) {
            CloseSocket(Server);
        }
    }
};

struct TRemovingTicker: public ITickable
{
    TRing& Ring;
    ITickable* Victim;
    std::atomic<int> Ticks = 0;

    TRemovingTicker(TRing& ring, ITickable* victim)
        : Ring(ring)
        , Victim(victim)
    {}

    void OnTick(TInstant) override
    {
        ++Ticks;
        if (Victim) {
            Ring.RemoveTickable(Victim);
            Victim = nullptr;
        }
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TUringRingTest)
{
    Y_UNIT_TEST_F(ShouldExecuteCommandsOnRingThread, TFixture)
    {
        bool onRing = OnRing([&] { return Ring->IsRingThread(); });
        UNIT_ASSERT(onRing);
        UNIT_ASSERT(!Ring->IsRingThread());
    }

    Y_UNIT_TEST_F(ShouldTick, TFixture)
    {
        TTicker ticker;
        OnRing(
            [&]
            {
                Ring->AddTickable(&ticker);
                return 0;
            });
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        while (ticker.Ticks < 2 && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(ticker.Ticks >= 2);
        OnRing(
            [&]
            {
                Ring->RemoveTickable(&ticker);
                return 0;
            });
    }

    Y_UNIT_TEST_F(ShouldSendAndReceiveExactly, TFixture)
    {
        int fds[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        TString out = "hello, ring";
        TString in = TString::Uninitialized(out.size());
        auto recvDone = NewPromise<int>();
        auto sendDone = NewPromise<int>();
        std::unique_ptr<TExactIo> rx, tx;

        OnRing(
            [&]
            {
                rx = std::make_unique<TExactIo>(*Ring);
                tx = std::make_unique<TExactIo>(*Ring);
                rx->Recv(
                    fds[1],
                    in.begin(),
                    in.size(),
                    [=](int e) mutable { recvDone.SetValue(e); });
                tx->Send(
                    fds[0],
                    out.data(),
                    out.size(),
                    [=](int e) mutable { sendDone.SetValue(e); });
                return 0;
            });

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            sendDone.GetFuture().GetValue(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            recvDone.GetFuture().GetValue(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(out, in);

        OnRing(
            [&]
            {
                rx.reset();
                tx.reset();
                return 0;
            });
        ::close(fds[0]);
        ::close(fds[1]);
    }

    Y_UNIT_TEST_F(ShouldCancelPendingRecv, TFixture)
    {
        int fds[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        char buf[8];
        auto done = NewPromise<int>();
        std::unique_ptr<TExactIo> rx;

        OnRing(
            [&]
            {
                rx = std::make_unique<TExactIo>(*Ring);
                rx->Recv(
                    fds[1],
                    buf,
                    sizeof(buf),
                    [=](int e) mutable { done.SetValue(e); });
                return 0;
            });
        Sleep(TDuration::MilliSeconds(50));
        OnRing(
            [&]
            {
                rx->Cancel();
                return 0;
            });

        // a cancel that races an EOF may surface as ECONNRESET
        const int error = done.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(error == ECANCELED || error == ECONNRESET, error);
        OnRing(
            [&]
            {
                rx.reset();
                return 0;
            });
        ::close(fds[0]);
        ::close(fds[1]);
    }

    Y_UNIT_TEST_F(ShouldReportEofAsError, TFixture)
    {
        int fds[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        char buf[8];
        auto done = NewPromise<int>();
        std::unique_ptr<TExactIo> rx;
        OnRing(
            [&]
            {
                rx = std::make_unique<TExactIo>(*Ring);
                rx->Recv(
                    fds[1],
                    buf,
                    sizeof(buf),
                    [=](int e) mutable { done.SetValue(e); });
                return 0;
            });
        ::close(fds[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            ECONNRESET,
            done.GetFuture().GetValue(TDuration::Seconds(5)));
        OnRing(
            [&]
            {
                rx.reset();
                return 0;
            });
        ::close(fds[1]);
    }

    Y_UNIT_TEST_F(ShouldCancelRecvThatCompletedPartially, TFixture)
    {
        int a[2], b[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, a) == 0);
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, b) == 0);
        UNIT_ASSERT(::write(a[0], "12345678", 8) == 8);
        UNIT_ASSERT(::write(b[0], "abc", 3) == 3);

        char bufA[8], bufB[8];
        auto doneA = NewPromise<int>();
        auto doneB = NewPromise<int>();
        std::unique_ptr<TExactIo> rxA, rxB;

        // Both recvs complete inside one io_uring_enter, so the CQE of rxB
        // (3 of 8 bytes) is still undispatched when rxA's callback cancels it.
        OnRing(
            [&]
            {
                rxA = std::make_unique<TExactIo>(*Ring);
                rxB = std::make_unique<TExactIo>(*Ring);
                rxA->Recv(
                    a[1],
                    bufA,
                    sizeof(bufA),
                    [&, doneA](int e) mutable
                    {
                        rxB->Cancel();
                        doneA.SetValue(e);
                    });
                rxB->Recv(
                    b[1],
                    bufB,
                    sizeof(bufB),
                    [=](int e) mutable { doneB.SetValue(e); });
                return 0;
            });

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            doneA.GetFuture().GetValue(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(
            ECANCELED,
            doneB.GetFuture().GetValue(TDuration::Seconds(5)));
        OnRing(
            [&]
            {
                rxA.reset();
                rxB.reset();
                return 0;
            });
        for (int fd: {a[0], a[1], b[0], b[1]}) {
            ::close(fd);
        }
    }

    Y_UNIT_TEST(ShouldSubmitMoreOpsThanQueueSize)
    {
        auto logging = CreateLoggingService("console");
        TRing ring(
            "URING.UT",
            8,
            ESendZeroCopyMode::Off,
            logging->CreateLog("URING"));
        auto error = ring.Start();
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        int fds[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        constexpr size_t N = 100;
        char out[N], in[N];
        std::vector<std::unique_ptr<TExactIo>> rx, tx;
        std::atomic<size_t> remaining = 2 * N;
        std::atomic<int> failures = 0;
        auto allDone = NewPromise<void>();
        auto onDone = [&](int e)
        {
            failures += (e != 0);
            if (--remaining == 0) {
                allDone.SetValue();
            }
        };

        ring.Execute(
            [&]
            {
                for (size_t i = 0; i != N; ++i) {
                    out[i] = i;
                    rx.push_back(std::make_unique<TExactIo>(ring));
                    tx.push_back(std::make_unique<TExactIo>(ring));
                    rx[i]->Recv(fds[1], &in[i], 1, onDone);
                    tx[i]->Send(fds[0], &out[i], 1, onDone);
                }
            });

        allDone.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(0, failures.load());
        std::sort(in, in + N);
        UNIT_ASSERT(std::equal(in, in + N, out));

        auto cleaned = NewPromise<void>();
        ring.Execute(
            [&]
            {
                rx.clear();
                tx.clear();
                cleaned.SetValue();
            });
        cleaned.GetFuture().GetValue(TDuration::Seconds(5));
        ring.Stop();
        ::close(fds[0]);
        ::close(fds[1]);
    }

    Y_UNIT_TEST(ShouldFailStartAndBeDestroyable)
    {
        auto logging = CreateLoggingService("console");
        TRing ring(
            "URING.UT",
            0,
            ESendZeroCopyMode::Auto,
            logging->CreateLog("URING"));
        UNIT_ASSERT(HasError(ring.Start()));
        ring.Stop();
    }

    Y_UNIT_TEST_F(ShouldConnectAcceptAndExchangeMessages, TTcpFixture)
    {
        for (bool zeroCopy: {false, true}) {
            TRecorder sendRec, recvRec;
            const char payload[] = "ring payload";
            char in[sizeof(payload)] = {};
            TIoOp* ops[2] = {};

            OnRing(
                [&]
                {
                    ops[0] = Ring->AllocOp(&recvRec, 1);
                    ops[0]->Iov[0] = {in, sizeof(in)};
                    ops[0]->IovCount = 1;
                    Ring->RecvMsg(ops[0], Server, 0);

                    ops[1] = Ring->AllocOp(&sendRec, 2);
                    ops[1]->Iov[0] = {
                        const_cast<char*>(payload),
                        sizeof(payload)};
                    ops[1]->IovCount = 1;
                    ops[1]->ZeroCopy = zeroCopy;
                    Ring->SendMsg(ops[1], Client, 0);
                    return 0;
                });
            sendRec.Done.GetFuture().GetValue(TDuration::Seconds(5));
            recvRec.Done.GetFuture().GetValue(TDuration::Seconds(5));

            UNIT_ASSERT_VALUES_EQUAL(1, recvRec.Cqes.size());
            UNIT_ASSERT_VALUES_EQUAL(sizeof(payload), recvRec.Cqes[0].Res);
            UNIT_ASSERT_VALUES_EQUAL(payload, in);

            const bool zc = zeroCopy && Ring->SendZeroCopySupported();
            UNIT_ASSERT_VALUES_EQUAL(zc ? 2 : 1, sendRec.Cqes.size());
            UNIT_ASSERT_VALUES_EQUAL(sizeof(payload), sendRec.Cqes[0].Res);
            UNIT_ASSERT_VALUES_EQUAL(
                zc,
                bool(sendRec.Cqes[0].Flags & IORING_CQE_F_MORE));
            if (zc) {
                UNIT_ASSERT(sendRec.Cqes[1].Flags & IORING_CQE_F_NOTIF);
            }

            UNIT_ASSERT_VALUES_EQUAL(
                2,
                OnRing(
                    [&]
                    {
                        Ring->FreeOp(ops[0]);
                        Ring->FreeOp(ops[1]);
                        return Ring->InFlightOps();
                    }));
        }
    }

    Y_UNIT_TEST_F(ShouldReapZeroCopySendThatFailed, TTcpFixture)
    {
        CloseWithReset(Server);
        Server = -1;

        TRecorder rec;
        const char payload[] = "ring payload";
        TIoOp* op = nullptr;
        OnRing(
            [&]
            {
                op = Ring->AllocOp(&rec, 1);
                op->Iov[0] = {const_cast<char*>(payload), sizeof(payload)};
                op->IovCount = 1;
                op->ZeroCopy = true;
                Ring->SendMsg(op, Client, 0);
                return 0;
            });
        rec.Done.GetFuture().GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(rec.Cqes[0].Res < 0);

        TTicker ticker;
        OnRing(
            [&]
            {
                Ring->AddTickable(&ticker);
                return 0;
            });
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        while (ticker.Ticks < 2 && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(ticker.Ticks >= 2);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            OnRing(
                [&]
                {
                    Ring->RemoveTickable(&ticker);
                    Ring->FreeOp(op);
                    return Ring->InFlightOps();
                }));
    }

    Y_UNIT_TEST_F(ShouldNotTickTickableRemovedDuringTick, TFixture)
    {
        TTicker victim;
        TRemovingTicker remover(*Ring, &victim);

        OnRing(
            [&]
            {
                Ring->AddTickable(&remover);
                Ring->AddTickable(&victim);
                return 0;
            });
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        while (remover.Ticks < 3 && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(remover.Ticks >= 3);
        UNIT_ASSERT_VALUES_EQUAL(0, victim.Ticks.load());
        OnRing(
            [&]
            {
                Ring->RemoveTickable(&remover);
                return 0;
            });
    }

    Y_UNIT_TEST_F(ShouldSurviveThrowingCommandHandlerAndTicker, TFixture)
    {
        struct TThrowingTicker: public ITickable
        {
            std::atomic<int> Ticks = 0;

            void OnTick(TInstant) override
            {
                ++Ticks;
                ythrow yexception() << "tick";
            }
        };

        Ring->Execute([] { ythrow yexception() << "command"; });

        int fds[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        char buf[1];
        auto done = NewPromise<int>();
        std::unique_ptr<TExactIo> rx;
        OnRing(
            [&]
            {
                rx = std::make_unique<TExactIo>(*Ring);
                rx->Recv(
                    fds[1],
                    buf,
                    sizeof(buf),
                    [=](int) mutable
                    {
                        done.SetValue(0);
                        ythrow yexception() << "handler";
                    });
                return 0;
            });
        UNIT_ASSERT(::write(fds[0], "x", 1) == 1);
        done.GetFuture().GetValue(TDuration::Seconds(5));

        TThrowingTicker ticker;
        OnRing(
            [&]
            {
                Ring->AddTickable(&ticker);
                return 0;
            });
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        while (ticker.Ticks < 2 && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(ticker.Ticks >= 2);

        UNIT_ASSERT(OnRing(
            [&]
            {
                Ring->RemoveTickable(&ticker);
                rx.reset();
                return Ring->IsRingThread();
            }));
        ::close(fds[0]);
        ::close(fds[1]);
    }

    Y_UNIT_TEST_F(ShouldServeIoOfCommandQueuedDuringStop, TFixture)
    {
        int fds[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        UNIT_ASSERT(::write(fds[0], "x", 1) == 1);
        char buf[1];
        auto blocked = NewPromise<void>();
        auto gate = NewPromise<void>();
        auto done = NewPromise<int>();
        std::unique_ptr<TExactIo> rx;

        Ring->Execute(
            [=]() mutable
            {
                blocked.SetValue();
                gate.GetFuture().Wait();
            });
        blocked.GetFuture().GetValue(TDuration::Seconds(5));

        // the ring thread is stuck in the command above, so Stop() and the IO
        // command below are both queued when it resumes
        std::thread stopper([&] { Ring->Stop(); });
        Ring->Execute(
            [&]
            {
                rx = std::make_unique<TExactIo>(*Ring);
                rx->Recv(
                    fds[1],
                    buf,
                    sizeof(buf),
                    [=](int e) mutable { done.SetValue(e); });
            });
        gate.SetValue();

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            done.GetFuture().GetValue(TDuration::Seconds(5)));
        stopper.join();
        rx.reset();
        ::close(fds[0]);
        ::close(fds[1]);
    }

    Y_UNIT_TEST_F(ShouldRunCancelPostedWhileStopWaitsForUserOp, TFixture)
    {
        int fds[2];
        UNIT_ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        char buf[8];
        auto done = NewPromise<int>();
        std::unique_ptr<TExactIo> rx;

        // the peer never writes, so this recv is still in flight when Stop()
        // asks the loop to shut down
        OnRing(
            [&]
            {
                rx = std::make_unique<TExactIo>(*Ring);
                rx->Recv(
                    fds[1],
                    buf,
                    sizeof(buf),
                    [=](int e) mutable { done.SetValue(e); });
                return 0;
            });

        std::thread stopper([&] { Ring->Stop(); });
        // let the loop see StopRequested and go back to sleep before the
        // teardown cancel is posted
        Sleep(TDuration::MilliSeconds(200));
        Ring->Execute([&] { rx->Cancel(); });

        auto future = done.GetFuture();
        const bool ran = future.Wait(TDuration::Seconds(5));
        if (!ran) {
            // the loop is wedged with nothing armed on the eventfd: close the
            // peer so the recv completes, Stop() returns and the assertion
            // below reports the failure instead of hanging the suite
            ::close(fds[0]);
            fds[0] = -1;
            future.Wait(TDuration::Seconds(5));
        }
        stopper.join();
        UNIT_ASSERT_C(ran, "the cancel posted during Stop() never ran");
        UNIT_ASSERT_VALUES_EQUAL(ECANCELED, future.GetValue());

        rx.reset();
        if (fds[0] >= 0) {
            ::close(fds[0]);
        }
        ::close(fds[1]);
    }
}

}   // namespace NCloud::NBlockStore::NUring
