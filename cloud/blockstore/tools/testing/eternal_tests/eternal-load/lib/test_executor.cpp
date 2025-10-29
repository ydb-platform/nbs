#include "test_executor.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <library/cpp/aio/aio.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/thread/lfstack.h>
#include <util/thread/pool.h>

#include <atomic>

namespace NCloud::NBlockStore::NTesting {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TTestExecutor: public ITestExecutor
{
private:
    class TWorkerService;

    const TInstant TestStartTimestamp;
    ITestScenarioPtr TestScenario;
    IFileIOServicePtr FileService;
    TFileHandle File;

    std::atomic_bool ShouldStop = false;
    std::atomic_bool Failed = false;
    TPromise<void> StopPromise = NewPromise();

    TVector<std::unique_ptr<TWorkerService>> WorkerServices;
    TLockFreeStack<TWorkerService*> ReadyWorkerServices;

    std::atomic_uint64_t BytesRead = 0;
    std::atomic_uint64_t BytesWritten = 0;
    ui64 PreviousBytesRead = 0;
    ui64 PreviousBytesWritten = 0;
    TInstant PreviousStatsTimestamp;

    const TLog Log;
    const bool RunInCallbacks = false;
    const bool PrintDebugStats = false;

    static constexpr TDuration PrintStatsInterval = TDuration::Seconds(5);

public:
    TTestExecutor(TTestExecutorSettings settings, IFileIOServicePtr service);
    bool Run() override;
    void Stop() override;
    void Fail(const TString& message);

private:
    void PrintStats();
};

////////////////////////////////////////////////////////////////////////////////

class TTestExecutor::TWorkerService: public ITestExecutorIOService
{
private:
    TTestExecutor& Executor;
    ITestScenarioWorker& Worker;
    TPromise<void> StopPromise = NewPromise();
    std::atomic_int PendingRequestCount = 0;
    int RequestCount = 0;

public:
    TWorkerService(TTestExecutor& executor, ITestScenarioWorker& worker);

    void Run();
    TFuture<void> GetFuture() const;

private:
    bool HandleRequest();

    void Read(
        void* buffer,
        ui32 count,
        ui64 offset,
        TCallback callback) override;

    void Write(
        const void* buffer,
        ui32 count,
        ui64 offset,
        TCallback callback) override;

    void Stop() override;

    void Fail(const TString& message) override;
};

////////////////////////////////////////////////////////////////////////////////

EOpenMode GetFileOpenMode(bool noDirect)
{
    constexpr auto CommonFileOpenMode =
        EOpenModeFlag::OpenAlways | EOpenModeFlag::RdWr;

    return noDirect ? CommonFileOpenMode
                    : EOpenModeFlag::DirectAligned | CommonFileOpenMode;
}

TTestExecutor::TTestExecutor(
        TTestExecutorSettings settings,
        IFileIOServicePtr service)
    : TestStartTimestamp(Now())
    , TestScenario(std::move(settings.TestScenario))
    , FileService(std::move(service))
    , File(settings.FilePath, GetFileOpenMode(settings.NoDirect))
    , PreviousStatsTimestamp(TestStartTimestamp)
    , Log(settings.Log)
    , RunInCallbacks(settings.RunInCallbacks)
    , PrintDebugStats(settings.PrintDebugStats)
{
    File.Resize(static_cast<i64>(settings.FileSize));

    for (ui32 i = 0; i < TestScenario->GetWorkerCount(); i++) {
        WorkerServices.push_back(
            std::make_unique<TWorkerService>(
                *this,
                TestScenario->GetWorker(i)));
    }
}

bool TTestExecutor::Run()
{
    if (!TestScenario->Init(File)) {
        return false;
    }

    STORAGE_INFO("Started TTestExecutor");

    FileService->Start();

    TVector<TFuture<void>> workerFutures;

    for (auto& worker: WorkerServices) {
        worker->Run();
        workerFutures.push_back(worker->GetFuture());
    }

    auto workersFuture = WaitAll(workerFutures);

    if (RunInCallbacks) {
        while (!workersFuture.Wait(PrintStatsInterval)) {
            PrintStats();
        }
    } else {
        TVector<TWorkerService*> workersToRun;
        while (!workersFuture.HasValue()) {
            workersToRun.clear();
            ReadyWorkerServices.DequeueAllSingleConsumer(&workersToRun);
            for (auto* worker: workersToRun) {
                worker->Run();
            }
            if (Now() - PreviousStatsTimestamp > PrintStatsInterval) {
                PrintStats();
            }
        }
    }

    FileService->Stop();
    File.Close();

    STORAGE_INFO("Stopped TTestExecutor");
    return !Failed.load();
}

void TTestExecutor::Stop()
{
    ShouldStop.store(true);
    if (StopPromise.TrySetValue()) {
        STORAGE_INFO("Stop has been requested");
    }
}

void TTestExecutor::Fail(const TString& message)
{
    Stop();
    Failed.store(true);
    STORAGE_ERROR(message);
}

void TTestExecutor::PrintStats()
{
    if (!PrintDebugStats) {
        return;
    }

    auto now = Now();
    auto currentBytesRead = BytesRead.load();
    auto currentBytesWritten = BytesWritten.load();

    auto elapsedSeconds = (now - PreviousStatsTimestamp).SecondsFloat();
    auto bytesRead = currentBytesRead - PreviousBytesRead;
    auto bytesWritten = currentBytesWritten - PreviousBytesWritten;

    STORAGE_DEBUG(
        "Read: " << bytesRead / elapsedSeconds / 1_MB << " MiB/s, "
        "Write: " << bytesWritten / elapsedSeconds / 1_MB << " MiB/s");

    PreviousStatsTimestamp = now;
    PreviousBytesRead = currentBytesRead;
    PreviousBytesWritten = currentBytesWritten;
}

////////////////////////////////////////////////////////////////////////////////

TTestExecutor::TWorkerService::TWorkerService(
        TTestExecutor& executor,
        ITestScenarioWorker& worker)
    : Executor(executor)
    , Worker(worker)
{}

void TTestExecutor::TWorkerService::Run()
{
    while (true) {
        if (Executor.ShouldStop) {
            StopPromise.SetValue();
            return;
        }

        Y_ABORT_UNLESS(
            PendingRequestCount == 0,
            "New iteration can be run only after requests from the previous "
            "iteration are handled");

        RequestCount = 0;
        PendingRequestCount = 1;

        auto testDuration = Now() - Executor.TestStartTimestamp;
        Worker.Run(testDuration.SecondsFloat(), *this);

        Y_ABORT_UNLESS(
            RequestCount > 0,
            "Test worker should make at least one request");

        if (!HandleRequest()) {
             // Run() will be called from somewhere else
            break;
        }
    }
}

TFuture<void> TTestExecutor::TWorkerService::GetFuture() const
{
    return StopPromise.GetFuture();
}

// Returns true if Run() method should be called immediately
bool TTestExecutor::TWorkerService::HandleRequest()
{
    auto prev = PendingRequestCount--;
    Y_ABORT_UNLESS(prev > 0, "There are no unhandled requests");

    if (prev > 1) {
        return false;
    }

    if (Executor.RunInCallbacks) {
        return true;
    }

    Executor.ReadyWorkerServices.Enqueue(this);
    return false;
}

void TTestExecutor::TWorkerService::Stop()
{
    Executor.Stop();
}

void TTestExecutor::TWorkerService::Fail(const TString& message)
{
    Executor.Fail(message);
}

void TTestExecutor::TWorkerService::Read(
    void* buffer,
    ui32 count,
    ui64 offset,
    TCallback callback)
{
    RequestCount++;
    PendingRequestCount++;

    Executor.FileService->AsyncRead(
        Executor.File,
        static_cast<i64>(offset),
        TArrayRef(static_cast<char*>(buffer), count),
        [this, count, callback = std::move(callback)](
            const NProto::TError& error,
            ui32 value)
        {
            if (HasError(error)) {
                Executor.Fail(
                    "Can't read from file: " + error.GetMessage());
            } else if (value < count) {
                Executor.Fail(
                    TStringBuilder() << "Read less than expected: " << value
                                     << " < " << count);
            } else {
                Executor.BytesRead += count;
                callback();
            }
            if (HandleRequest()) {
                Run();
            }
        });
}

void TTestExecutor::TWorkerService::Write(
    const void* buffer,
    ui32 count,
    ui64 offset,
    TCallback callback)
{
    RequestCount++;
    PendingRequestCount++;

    Executor.FileService->AsyncWrite(
        Executor.File,
        static_cast<i64>(offset),
        TArrayRef(static_cast<const char*>(buffer), count),
        [this, count, callback = std::move(callback)](
            const NProto::TError& error,
            ui32 value)
        {
            if (HasError(error)) {
                Executor.Fail(
                    "Can't write to file: " + error.GetMessage());
            } else if (value < count) {
                Executor.Fail(
                    TStringBuilder() << "Written less than expected: " << value
                                     << " < " << count);
            } else {
                Executor.BytesWritten += count;
                callback();
            }
            if (HandleRequest()) {
                Run();
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncIoFileService
    : public IFileIOService
    , private NAsyncIO::TAsyncIOService
{
public:
    using TAsyncIOService::TAsyncIOService;

    void Start() override
    {
        TAsyncIOService::Start();
    }

    void Stop() override
    {
        TAsyncIOService::Stop();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) override
    {
        auto future = TAsyncIOService::Read(
            file,
            buffer.data(),
            static_cast<ui32>(buffer.size()),
            static_cast<ui64>(offset));

        future.Subscribe([completion](const auto& f) {
            RunCompletion(f, completion);
        });
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) override
    {
        auto future = TAsyncIOService::Write(
            file,
            buffer.data(),
            static_cast<ui32>(buffer.size()),
            static_cast<ui64>(offset));

        future.Subscribe([completion](const auto& f) {
            RunCompletion(f, completion);
        });
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffers);
        Y_UNUSED(completion);
        Y_ABORT("Not used");
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffers);
        Y_UNUSED(completion);
        Y_ABORT("Not used");
    }

    static void RunCompletion(
        const NThreading::TFuture<ui32>& future,
        TFileIOCompletion* completion)
    {
        Y_ABORT_UNLESS(future.HasValue());

        try {
            auto value = future.GetValue();
            if (value >= 0) {
                completion->Func(completion, {}, value);
            } else {
                completion->Func(completion, MakeError(E_FAIL), value);
            }
        } catch (...) {
            completion->Func(
                completion,
                MakeError(E_FAIL, CurrentExceptionMessage()),
                0);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TThreadPoolFileService: public IFileIOService
{
private:
    TThreadPool ThreadPool;
    const ui32 ThreadCount = 0;

public:
    explicit TThreadPoolFileService(ui32 threadCount)
        : ThreadCount(threadCount)
    {}

    void Start() override
    {
        ThreadPool.Start(ThreadCount);
    }

    void Stop() override
    {
        ThreadPool.Stop();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) override
    {
        auto added = ThreadPool.AddFunc(
            [&file, buffer, offset, completion]()
            {
                auto value = file.Pread(
                    buffer.data(),
                    static_cast<ui32>(buffer.size()),
                    offset);

                RunCompletion(value, completion);
            });

        Y_ABORT_UNLESS(added, "Cannot add function to a thread pool");
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) override
    {
        auto added = ThreadPool.AddFunc(
            [&file, buffer, offset, completion]()
            {
                auto value = file.Pwrite(
                    buffer.data(),
                    static_cast<ui32>(buffer.size()),
                    offset);

                RunCompletion(value, completion);
            });

        Y_ABORT_UNLESS(added, "Cannot add function to a thread pool");
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffers);
        Y_UNUSED(completion);
        Y_ABORT("Not supported");
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffers);
        Y_UNUSED(completion);
        Y_ABORT("Not supported");
    }

    static void RunCompletion(i32 value, TFileIOCompletion* completion)
    {
        if (value >= 0) {
            completion->Func(completion, {}, static_cast<ui32>(value));
        } else {
            completion->Func(completion, MakeError(E_FAIL), value);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateUringFileService()
{
    auto factory = CreateIoUringServiceFactory({
        .SubmissionQueueEntries = 1024,
        .MaxKernelWorkersCount = 1,
        .ShareKernelWorkers = true,
        .ForceAsyncIO = true,
    });

    return factory->CreateFileIOService();
}

IFileIOServicePtr CreateFileService(
    ETestExecutorFileService fileService,
    ui32 threadCount)
{
    switch (fileService) {
        case ETestExecutorFileService::AsyncIo:
            return std::make_shared<TAsyncIoFileService>(0, threadCount);

        case ETestExecutorFileService::Sync:
            return std::make_unique<TThreadPoolFileService>(threadCount);

        case ETestExecutorFileService::IoUring:
            return CreateUringFileService();

        default:
            Y_ABORT("Not supported type - %d", fileService);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestExecutorPtr CreateTestExecutor(TTestExecutorSettings settings)
{
    auto fileService = CreateFileService(
        settings.FileService,
        settings.TestScenario->GetWorkerCount());

    return std::make_shared<TTestExecutor>(
        std::move(settings),
        std::move(fileService));
}

}   // namespace NCloud::NBlockStore::NTesting
