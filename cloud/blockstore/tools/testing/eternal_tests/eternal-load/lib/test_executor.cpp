#include "test_executor.h"

#include <atomic>

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <library/cpp/aio/aio.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/thread.h>
#include <util/thread/lfstack.h>
#include <util/thread/pool.h>

namespace NCloud::NBlockStore::NTesting {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TTestExecutor: public ITestExecutor
{
private:
    class TContext;

    const TInstant TestStartTimestamp;
    ITestScenarioPtr TestScenario;
    IFileIOServicePtr FileService;
    TFileHandle File;

    std::atomic_bool ShouldStop = false;
    std::atomic_bool Failed = false;
    TPromise<void> StopPromise = NewPromise();

    TVector<std::unique_ptr<TContext>> Contexts;
    TLockFreeStack<TContext*> ReadyContexts;

    std::atomic_uint64_t BytesRead = 0;
    std::atomic_uint64_t BytesWritten = 0;
    ui64 PreviousBytesRead = 0;
    ui64 PreviousBytesWritten = 0;
    TInstant PreviousStatsTimestamp;

    const TLog Log;
    const bool RunFromAnyThread = false;
    const bool PrintDebugInfo = false;

    static constexpr TDuration PrintStatsInterval = TDuration::Seconds(5);

public:
    TTestExecutor(TTestExecutorSettings settings, IFileIOServicePtr service);
    bool Run() override;
    void Stop() override;
    void Fail(TStringBuf message);

private:
    void RunAnyThread();
    void RunMainThread();
    void PrintStatistics();
};

////////////////////////////////////////////////////////////////////////////////

class TTestExecutor::TContext: public ITestThreadContext
{
private:
    TTestExecutor* TestExecutor = nullptr;
    ITestThread* TestThread = nullptr;
    TPromise<void> StopPromise = NewPromise();
    std::atomic_int UnhandledRequestCount = 0;
    int RequestCount = 0;

public:
    TContext(TTestExecutor* testExecutor, ITestThread* testThread);

    void Run();
    void Wait();

private:
    bool HandleRequest();

    void Stop() override;

    void Fail(TStringBuf message) override;

    void Read(void* buffer, ui32 count, ui64 offset, TCallback callback)
        override;

    void Write(const void* buffer, ui32 count, ui64 offset, TCallback callback)
        override;
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
    , RunFromAnyThread(settings.RunFromAnyThread)
    , PrintDebugInfo(settings.PrintDebugInfo)
{
    File.Resize(static_cast<i64>(settings.FileSize));

    for (ui32 i = 0; i < TestScenario->GetThreadCount(); i++) {
        Contexts.push_back(
            std::make_unique<TContext>(this, TestScenario->GetThread(i)));
    }
}

bool TTestExecutor::Run()
{
    if (!TestScenario->Init(File)) {
        return false;
    }

    STORAGE_INFO("Started TTestExecutor");

    FileService->Start();

    for (auto& context: Contexts) {
        context->Run();
    }

    if (RunFromAnyThread) {
        RunAnyThread();
    } else {
        RunMainThread();
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

void TTestExecutor::Fail(TStringBuf message)
{
    Stop();
    Failed.store(true);
    STORAGE_ERROR(message);
}

void TTestExecutor::RunAnyThread()
{
    auto stopFuture = StopPromise.GetFuture();

    if (PrintDebugInfo) {
        while (!stopFuture.Wait(PrintStatsInterval)) {
            PrintStatistics();
        }
    } else {
        stopFuture.Wait();
    }

    for (auto& context: Contexts) {
        context->Wait();
    }
}

void TTestExecutor::RunMainThread()
{
    TVector<TContext*> contextsToRun;

    while (!ShouldStop) {
        contextsToRun.clear();
        ReadyContexts.DequeueAllSingleConsumer(&contextsToRun);
        for (auto* context: contextsToRun) {
            context->Run();
        }

        if (PrintDebugInfo) {
            if (Now() - PreviousStatsTimestamp > PrintStatsInterval) {
                PrintStatistics();
            }
        }
    }

    while (contextsToRun.size() < Contexts.size()) {
        ReadyContexts.DequeueAllSingleConsumer(&contextsToRun);
    }
}

void TTestExecutor::PrintStatistics()
{
    auto now = Now();
    auto currentBytesRead = BytesRead.load();
    auto currentBytesWritten = BytesWritten.load();

    auto elapsedSeconds = (now - PreviousStatsTimestamp).SecondsFloat();
    auto bytesRead = currentBytesRead - PreviousBytesRead;
    auto bytesWritten = currentBytesWritten - PreviousBytesWritten;

    STORAGE_INFO(
        "Read: " << bytesRead / elapsedSeconds / 1_MB << " MiB/s, "
        "Write: " << bytesWritten / elapsedSeconds / 1_MB << " MiB/s");

    PreviousStatsTimestamp = now;
    PreviousBytesRead = currentBytesRead;
    PreviousBytesWritten = currentBytesWritten;
}

////////////////////////////////////////////////////////////////////////////////

TTestExecutor::TContext::TContext(
        TTestExecutor* testExecutor,
        ITestThread* testThread)
    : TestExecutor(testExecutor)
    , TestThread(testThread)
{}

void TTestExecutor::TContext::Run()
{
    while (!TestExecutor->ShouldStop) {
        Y_ABORT_UNLESS(
            UnhandledRequestCount == 0,
            "New iteration can be run only after requests from the previous "
            "iteration are handled");

        RequestCount = 0;
        UnhandledRequestCount = 1;

        auto testDuration = Now() - TestExecutor->TestStartTimestamp;
        TestThread->Run(testDuration.SecondsFloat(), this);

        Y_ABORT_UNLESS(
            RequestCount > 0,
            "Test thread should make at least one request");

        if (!HandleRequest()) {
            // Run() will be called from somewhere else
            return;
        }
    }
    StopPromise.SetValue();
}

void TTestExecutor::TContext::Wait()
{
    StopPromise.GetFuture().Wait();
}

bool TTestExecutor::TContext::HandleRequest()
{
    auto prev = UnhandledRequestCount--;
    Y_ABORT_UNLESS(prev > 0, "There are no unhandled requests");

    if (prev > 1) {
        return false;
    }

    if (TestExecutor->RunFromAnyThread) {
        return true;
    }

    TestExecutor->ReadyContexts.Enqueue(this);
    return false;
}

void TTestExecutor::TContext::Stop()
{
    TestExecutor->Stop();
}

void TTestExecutor::TContext::Fail(TStringBuf message)
{
    TestExecutor->Fail(message);
}

void TTestExecutor::TContext::Read(
    void* buffer,
    ui32 count,
    ui64 offset,
    TCallback callback)
{
    RequestCount++;
    UnhandledRequestCount++;

    TestExecutor->FileService->AsyncRead(
        TestExecutor->File,
        static_cast<i64>(offset),
        TArrayRef(static_cast<char*>(buffer), count),
        [this, count, callback = std::move(callback)](
            const NProto::TError& error,
            ui32 value)
        {
            if (HasError(error)) {
                TestExecutor->Fail(
                    "Can't read from file: " + error.GetMessage());
            } else if (value < count) {
                TestExecutor->Fail(
                    TStringBuilder() << "Read less than expected: " << value
                                     << " < " << count);
            } else {
                TestExecutor->BytesRead += count;
                callback();
            }
            if (HandleRequest()) {
                Run();
            }
        });
}

void TTestExecutor::TContext::Write(
    const void* buffer,
    ui32 count,
    ui64 offset,
    TCallback callback)
{
    RequestCount++;
    UnhandledRequestCount++;

    TestExecutor->FileService->AsyncWrite(
        TestExecutor->File,
        static_cast<i64>(offset),
        TArrayRef(static_cast<const char*>(buffer), count),
        [this, count, callback = std::move(callback)](
            const NProto::TError& error,
            ui32 value)
        {
            if (HasError(error)) {
                TestExecutor->Fail(
                    "Can't write to file: " + error.GetMessage());
            } else if (value < count) {
                TestExecutor->Fail(
                    TStringBuilder() << "Written less than expected: " << value
                                     << " < " << count);
            } else {
                TestExecutor->BytesWritten += count;
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
        settings.TestScenario->GetThreadCount());

    return std::make_shared<TTestExecutor>(
        std::move(settings),
        std::move(fileService));
}

}   // namespace NCloud::NBlockStore::NTesting
