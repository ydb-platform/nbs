#include <cloud/filestore/libs/client/session.h>

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <atomic>
#include <latch>
#include <memory>
#include <thread>

namespace NCloud::NFileStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString FileSystemId = "fs1";
static const TString ClientId = "client1";
static const TString SessionId = "session1";

constexpr TDuration PingTimeout = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

struct TSessionSetup
{
    ILoggingServicePtr Logging;
    ITimerPtr Timer;
    std::shared_ptr<TTestScheduler> Scheduler;
    std::shared_ptr<TFileStoreTest> FileStore;
    ISessionPtr Session;

    TSessionSetup()
    {
        Logging = CreateLoggingService("console", { TLOG_RESOURCES });
        Timer = CreateWallClockTimer();
        Scheduler = std::make_shared<TTestScheduler>();

        FileStore = std::make_shared<TFileStoreTest>();

        FileStore->ReadDataHandler = [] (auto, auto) {
            NProto::TReadDataResponse response;
            return MakeFuture(response);
        };

        FileStore->WriteDataHandler = [] (auto, auto) {
            NProto::TWriteDataResponse response;
            return MakeFuture(response);
        };

        Session = CreateSession(
            Logging,
            Timer,
            Scheduler,
            FileStore,
            CreateSessionConfig());
    }

    ~TSessionSetup()
    {
        Stop();
    }

    void Start()
    {
        if (Scheduler) {
            Scheduler->Start();
        }

        if (Logging) {
            Logging->Start();
        }
    }

    void Stop()
    {
        if (Logging) {
            Logging->Stop();
        }

        if (Scheduler) {
            Scheduler->Stop();
        }
    }

    TFuture<NProto::TReadDataResponse> ReadData()
    {
        return Session->ReadData(
            MakeIntrusive<TCallContext>(FileSystemId),
            std::make_shared<NProto::TReadDataRequest>());
    }

    TFuture<NProto::TWriteDataResponse> WriteData()
    {
        return Session->WriteData(
            MakeIntrusive<TCallContext>(FileSystemId),
            std::make_shared<NProto::TWriteDataRequest>());
    }

    static TSessionConfigPtr CreateSessionConfig()
    {
        NProto::TSessionConfig proto;
        proto.SetFileSystemId(FileSystemId);
        proto.SetClientId(ClientId);
        proto.SetSessionPingTimeout(PingTimeout.MilliSeconds());

        return std::make_shared<TSessionConfig>(proto);
    }
};

TSessionSetup* GetOrCreateSession()
{
    return Singleton<TSessionSetup>();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define SESSION_BENCH(method, threadCount)                                     \
    Y_CPU_BENCHMARK(TSession##_##method##_##threadCount, iface)                \
    {                                                                          \
        auto iters = iface.Iterations();                                       \
                                                                               \
        auto* session = GetOrCreateSession();                                  \
                                                                               \
        std::atomic<ui32> counter;                                             \
        std::latch latch{threadCount + 1};                                     \
        TVector<std::thread> threads;                                          \
                                                                               \
        for (ui32 i = 0; i < threadCount; ++i) {                               \
            threads.emplace_back(                                              \
                [&]()                                                          \
                {                                                              \
                    latch.arrive_and_wait();                                   \
                                                                               \
                    for (size_t i = 0; i < iters; ++i) {                       \
                        auto future = session->method().Subscribe(             \
                            [&counter](auto)                                   \
                            {                                                  \
                                counter.fetch_add(                             \
                                    1,                                         \
                                    std::memory_order_relaxed);                \
                            });                                                \
                        future.Wait();                                         \
                    }                                                          \
                });                                                            \
        }                                                                      \
                                                                               \
        latch.arrive_and_wait();                                               \
        for (auto& t: threads) {                                               \
            t.join();                                                          \
        }                                                                      \
    }                                                                          \
// SESSION_BENCH

#define SESSION_BENCH_SET(method)                                              \
    SESSION_BENCH(method, 1)                                                   \
    SESSION_BENCH(method, 2)                                                   \
    SESSION_BENCH(method, 4)                                                   \
    SESSION_BENCH(method, 8)                                                   \
    SESSION_BENCH(method, 16)                                                  \
// SESSION_BENCH_SET

SESSION_BENCH_SET(ReadData)
SESSION_BENCH_SET(WriteData)

}   // namespace NCloud::NFileStore::NClient
