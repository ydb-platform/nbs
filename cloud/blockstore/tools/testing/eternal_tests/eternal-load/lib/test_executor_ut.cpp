#include "test_executor.h"

#include <library/cpp/logger/stream.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <atomic>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 FileSize = 1_MB;
constexpr ui32 RequestSize = 4_KB;

// An AIO context created by io_setup(n) has about 2 * max(n, 4 * ncpus) slots
// for requests that are in flight or whose completions are not reaped yet.
// Submitting more requests at once makes io_submit fail with EAGAIN.
constexpr ui32 RequestsPerIteration = 16384;
constexpr ui32 IterationCount = 4;

////////////////////////////////////////////////////////////////////////////////

struct TWriteWorker final: public ITestScenarioWorker
{
    const TVector<char> Buffer = TVector<char>(RequestSize, 'x');
    ui32 Iteration = 0;
    std::atomic<ui64> WriteCount = 0;

    void Run(
        double secondsSinceTestStart,
        ITestExecutorIOService& service) override
    {
        Y_UNUSED(secondsSinceTestStart);

        for (ui32 i = 0; i < RequestsPerIteration; ++i) {
            service.Write(
                Buffer.data(),
                RequestSize,
                (static_cast<ui64>(i) * RequestSize) % FileSize,
                [this] { ++WriteCount; });
        }

        if (++Iteration == IterationCount) {
            service.Stop();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteScenario final: public ITestScenario
{
    mutable TWriteWorker Worker;

    ui32 GetWorkerCount() const override
    {
        return 1;
    }

    ITestScenarioWorker& GetWorker(ui32 index) const override
    {
        Y_UNUSED(index);
        return Worker;
    }
};

////////////////////////////////////////////////////////////////////////////////

void ShouldResubmitRequestsWhenAioContextIsFull(bool runInCallbacks)
{
    TTempDir tempDir;
    auto scenario = std::make_shared<TWriteScenario>();

    auto executor = CreateTestExecutor({
        .TestScenarios = {{
            .TestScenario = scenario,
            .FilePath = (tempDir.Path() / "file").GetPath(),
            .FileSize = FileSize,
        }},
        .FileService = ETestExecutorFileService::AsyncIo,
        .Log = TLog(MakeHolder<TStreamLogBackend>(&Cerr)),
        .RunInCallbacks = runInCallbacks,
        // Buffered requests complete before io_submit returns, so the AIO
        // context fills up unless the poller thread keeps reaping completions
        .NoDirect = true,
    });

    UNIT_ASSERT(executor->Run());
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<ui64>(RequestsPerIteration) * IterationCount,
        scenario->Worker.WriteCount.load());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTestExecutorTest)
{
    // The poller thread submits the requests of all the iterations but the
    // first one and doesn't reap completions meanwhile, so io_submit is bound
    // to fail with EAGAIN
    Y_UNIT_TEST(ShouldResubmitRequestsWhenAioContextIsFullInCallbacks)
    {
        ShouldResubmitRequestsWhenAioContextIsFull(true);
    }

    Y_UNIT_TEST(ShouldResubmitRequestsWhenAioContextIsFull)
    {
        ShouldResubmitRequestsWhenAioContextIsFull(false);
    }
}

}   // namespace NCloud::NBlockStore::NTesting
