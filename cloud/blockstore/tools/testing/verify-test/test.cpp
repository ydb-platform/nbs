#include "test.h"

#include "options.h"
#include "test_executor.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/async.h>

#include <util/generic/map.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/info.h>
#include <util/thread/pool.h>

#include <atomic>
#include <latch>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTest final
    : public ITest
{
    struct TStopException
        : public yexception
    {};

private:
    const TOptionsPtr Options;
    const ILoggingServicePtr Logging;
    TLog Log;

    std::atomic<int> ExitCode;
    std::atomic_flag ShouldStop;

    TVector<TTestExecutorConfigPtr> ExecutorsConfigs;
    TVector<ITestExecutorPtr> Executors;

public:
    TTest(TOptionsPtr options, ILoggingServicePtr logging)
        : Options(std::move(options))
        , Logging(std::move(logging))
        , Log(Logging->CreateLog("VERIFY"))
    {}

    int Run() override;
    void Stop(int exitCode) override;

private:
    void InitExecutorsConfigs();
    void RunStage(const ETestExecutorType& type);
    void DropCaches();
    void CheckStopFlag();
};

////////////////////////////////////////////////////////////////////////////////

TStringBuilder BuildTestReport(
    const TTestExecutorConfig& config,
    const TTestExecutorReport& report)
{
    const auto dt = report.FinishTime - report.StartTime;

    return TStringBuilder()
        << "#StartOffset = " << config.StartOffset << " "
        << "#EndOffset = " << config.EndOffset << " "
        << "#TestPattern = " << config.TestPattern << " "
        << "#DirectIO = " << config.DirectIo << " "
        << "#StartTime = " << report.StartTime.ToString() << " "
        << "#FinishTime = " << report.FinishTime.ToString()
        << " [ " << FormatDuration(dt) << "]";
}

////////////////////////////////////////////////////////////////////////////////

int TTest::Run()
{
    try {
        STORAGE_INFO("Initializing executor configs...");
        InitExecutorsConfigs();

        if (!Options->CheckZero && !Options->ReadOnly) {
            STORAGE_INFO("Running write stage...");
            RunStage(ETestExecutorType::Write);

            STORAGE_INFO("Dropping caches...");
            DropCaches();

            STORAGE_INFO("Sleeping...");
            Sleep(TDuration::Seconds(30));
        }

        STORAGE_INFO("Running read stage...");
        RunStage(ETestExecutorType::Read);
    } catch (const TStopException& e) {
        // proceed to termination
        Y_UNUSED(e);
    }

    STORAGE_INFO("Terminating...");
    return ExitCode.load();
}

void TTest::Stop(int exitCode)
{
    if (ShouldStop.test_and_set()) {
        return;
    }

    ExitCode.store(exitCode);

    for (auto& executor: Executors) {
        executor->Stop();
    }
}

void TTest::InitExecutorsConfigs()
{
    CheckStopFlag();

    Y_ENSURE(Options->IoDepth > 0, "iodepth must be greater than 0");
    Y_ENSURE(Options->BlockSize > 0, "blocksize must be greater than 0");
    Y_ENSURE(
        Options->BlockSize % 512 == 0,
        "blocksize must be a multiple of 512"
    );
    Y_ENSURE(
        Options->FileSize >= Options->BlockSize * Options->IoDepth,
        "filesize must be greater than blocksize * iodepth"
    );

    ui64 executorBlocksCount = Options->FileSize /
        (Options->IoDepth * static_cast<ui64>(Options->BlockSize));
    ui64 executorBytesCount = executorBlocksCount * Options->BlockSize;

    Y_ENSURE(
        executorBytesCount % NSystemInfo::GetPageSize() == 0,
        "executor bytes count must be a multiple of page size"
    );

    for (ui16 i = 0; i < Options->IoDepth; i++) {
        ui64 startOffset = Options->Offset + i * executorBytesCount;
        ui64 endOffset = startOffset + executorBytesCount;
        auto testPattern = static_cast<ETestPattern>(
            i % static_cast<ui16>(ETestPattern::Max));
        bool directIO = i % 2;

        if (Options->CheckZero) {
            testPattern = ETestPattern::CheckZero;
            directIO = true;
        }

        auto executorConfig = std::make_shared<TTestExecutorConfig>(
            startOffset,
            endOffset,
            Options->Step,
            Options->BlockSize,
            testPattern,
            directIO
        );

        ExecutorsConfigs.push_back(std::move(executorConfig));
    }
}

void TTest::RunStage(const ETestExecutorType& type)
{
    using namespace NThreading;

    CheckStopFlag();

    auto threadPool = CreateThreadPool(Options->IoDepth);

    std::latch waitingForStart{Options->IoDepth};

    TMap<TTestExecutorConfig, TFuture<TTestExecutorReport>> configToReport;

    for (const auto& executorConfig: ExecutorsConfigs) {
        Executors.push_back(
            CreateTestExecutor(type, Options->FilePath, executorConfig));

        const auto report = Async([&, executor = Executors.back()]() mutable {
            return executor->Run(waitingForStart);
        }, *threadPool);

        configToReport[*executorConfig] = report;
    }

    for (const auto& [config, reportFuture]: configToReport) {
        const auto& report = reportFuture.GetValueSync();

        CheckStopFlag();

        Cout << BuildTestReport(config, report) << Endl;
    }

    Executors.clear();
}

void TTest::DropCaches()
{
    TFile file(Options->FilePath, EOpenModeFlag::RdWr);
    file.Flush();
}

void TTest::CheckStopFlag()
{
    if (ShouldStop.test()) {
        throw TStopException();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestPtr CreateTest(TOptionsPtr options, ILoggingServicePtr logging)
{
    return std::make_shared<TTest>(std::move(options), std::move(logging));
}

}   // namespace NCloud::NBlockStore
