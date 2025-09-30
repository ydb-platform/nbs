#include "test_executor.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/io_uring/service.h>

#include <library/cpp/aio/aio.h>
#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/info.h>
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

    const TLog Log;
    const bool RunInCallbacks = false;

public:
    TTestExecutor(TTestExecutorSettings settings, IFileIOServicePtr service);
    bool Run() override;
    void Stop() override;
    void Fail(const TString& message);
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
    , Log(settings.Log)
    , RunInCallbacks(settings.RunInCallbacks)
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
    if (!TestScenario->Initialize(File)) {
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
        workersFuture.Wait();
    } else {
        TVector<TWorkerService*> workersToRun;
        while (!workersFuture.HasValue()) {
            workersToRun.clear();
            ReadyWorkerServices.DequeueAllSingleConsumer(&workersToRun);
            for (auto* worker: workersToRun) {
                worker->Run();
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

constexpr ui64 DIRECT_IO_ALIGNMENT = 512; // bytes

////////////////////////////////////////////////////////////////////////////////

std::pair<i64, i64> ExtendedEuclideanAlgorithm(ui64 a, ui64 b)
{
    if (a == 0) {
        return {0, 1};
    }
    auto [x1, y1] = ExtendedEuclideanAlgorithm(b % a, a);
    return {y1 - (b / a) * x1, x1};
}

////////////////////////////////////////////////////////////////////////////////

ui64 CalculateInverse(ui64 step, ui64 len)
{
    auto [x, _] = ExtendedEuclideanAlgorithm(step, len);
    x = (x + len) % len;
    return x;
}

////////////////////////////////////////////////////////////////////////////////

struct TRange {
    TRangeConfig& Config;
    ui64 Size;
    std::shared_ptr<char[]> Buf;
    ui64 StepInversion;

    TRange(TRangeConfig& config, ui64 size)
        : Config{config}
        , Size{size}
        , Buf{static_cast<char*>(std::aligned_alloc(NSystemInfo::GetPageSize(), Size)), std::free}
        , StepInversion{CalculateInverse(Config.GetStep(), Config.GetRequestCount())}
    {
        memset(Buf.get(), '1', Size);

        Y_ABORT_UNLESS(
            size % Config.GetWriteParts() == 0,
            "invalid write parts number"
        );
        Y_ABORT_UNLESS(
            size / Config.GetWriteParts() >= sizeof(TBlockData),
            "blockdata doesn't fit write part"
        );
        Y_ABORT_UNLESS(
            (size / Config.GetWriteParts()) % DIRECT_IO_ALIGNMENT == 0,
            "write parts has invalid alignment"
        );
    }

    char* Data(ui64 offset = 0)
    {
        return Buf.get() + offset;
    }

    ui64 DataSize()
    {
        return Size;
    }

    std::pair<ui64, ui64> NextWrite()
    {
        ui64 blockIdx = Config.GetStartOffset() + Config.GetLastBlockIdx() * Config.GetRequestBlockCount();
        ui64 iteration = Config.GetNumberToWrite();

        Config.SetLastBlockIdx((Config.GetLastBlockIdx() + Config.GetStep()) % Config.GetRequestCount());
        Config.SetNumberToWrite(Config.GetNumberToWrite() + 1);
        return {blockIdx, iteration};
    }

    std::pair<ui64, TMaybe<ui64>> RandomRead()
    {
        // Idea of this code is to find request number (x) which is written in random block (r).
        // To do this we need to solve equation `startBlockIdx + x * step = r [mod %requestCount]` which is equal
        // to equation `x = (r - startBlockIdx) * inverted_step [mod %requestCount]`.
        ui64 requestCount = Config.GetRequestCount();

        ui64 randomBlock = RandomNumber(requestCount);
        ui64 tmp = (randomBlock - Config.GetStartBlockIdx() + requestCount) % requestCount;
        ui64 x = (tmp * StepInversion) % requestCount;

        TMaybe<ui64> expected = Nothing();
        if (Config.GetNumberToWrite() > x) {
            ui64 fullCycles = (Config.GetNumberToWrite() - x - 1) / requestCount;
            expected = x + fullCycles * requestCount;
        }

        ui64 requestBlockIdx = Config.GetStartOffset() + randomBlock * Config.GetRequestBlockCount();
        return {requestBlockIdx, expected};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAlignedBlockTestScenario: public ITestScenario
{
private:
    using IService = ITestExecutorIOService;

    const TInstant TestStartTimestamp;
    IConfigHolderPtr ConfigHolder;
    TDuration SlowRequestThreshold;
    std::optional<double> PhaseDuration;
    ui32 WriteRate;

    TVector<TRange> Ranges;
    TVector<std::unique_ptr<ITestScenarioWorker>> Workers;

    TLog Log;

    TAtomic WriteRequestsCompleted = 0;

private:
    void DoRequest(
        ui16 rangeIdx,
        double secondsSinceTestStart,
        IService& service);

    void DoWriteRequest(ui16 rangeIdx, IService& service);
    void DoReadRequest(ui16 rangeIdx, IService& service);

    void OnResponse(
        TInstant startTs,
        ui16 rangeIdx,
        TStringBuf reqType,
        IService& service);

    struct TWorker: public ITestScenarioWorker
    {
        TAlignedBlockTestScenario* Scenario;
        ui32 Index;

        TWorker(TAlignedBlockTestScenario* scenario, ui32 index)
            : Scenario(scenario)
            , Index(index)
        {}

        void Run(double secondsSinceTestStart, IService& service) final
        {
            Scenario->DoRequest(Index, secondsSinceTestStart, service);
        }
    };

public:
    TAlignedBlockTestScenario(IConfigHolderPtr configHolder, const TLog& log)
        : TestStartTimestamp(Now())
        , ConfigHolder(configHolder)
        , Log(log)
    {
        auto& config = ConfigHolder->GetConfig();
        for (ui16 i = 0; i < config.GetIoDepth(); ++i) {
            auto& rangeConfig = *config.MutableRanges(i);
            Ranges.emplace_back(
                rangeConfig,
                rangeConfig.GetRequestBlockCount() * config.GetBlockSize());
            Workers.push_back(std::make_unique<TWorker>(this, i));
        }

        SlowRequestThreshold = TDuration::Parse(config.GetSlowRequestThreshold());

        if (config.HasAlternatingPhase()) {
            PhaseDuration =
                TDuration::Parse(config.GetAlternatingPhase()).SecondsFloat();

            Y_ENSURE(
                PhaseDuration > 0,
                "Alternating phase duration should be a positive non-zero value");
        }

        WriteRate = config.GetWriteRate();
    }

    ui32 GetWorkerCount() const final
    {
        return static_cast<ui32>(Workers.size());
    }

    ITestScenarioWorker& GetWorker(ui32 index) const final
    {
        return *Workers[index];
    }
};

////////////////////////////////////////////////////////////////////////////////

void TAlignedBlockTestScenario::DoRequest(
    ui16 rangeIdx,
    double secondsSinceTestStart,
    IService& service)
{
    auto writeRate = WriteRate;
    if (PhaseDuration) {
        auto iter = secondsSinceTestStart / PhaseDuration.value();
        if (static_cast<ui64>(iter) % 2 == 1) {
            writeRate = 100 - writeRate;
        }
    }

    if (RandomNumber(100u) >= writeRate) {
        DoReadRequest(rangeIdx, service);
    } else {
        DoWriteRequest(rangeIdx, service);
    }
}

void TAlignedBlockTestScenario::OnResponse(
    TInstant startTs,
    ui16 rangeIdx,
    TStringBuf reqType,
    IService& service)
{
    if (reqType == "write") {
        const i64 maxRequestCount =
            ConfigHolder->GetConfig().GetMaxWriteRequestCount();
        if (maxRequestCount &&
            AtomicIncrement(WriteRequestsCompleted) >= maxRequestCount)
        {
            service.Stop();
        }
    }

    const auto now = Now();
    const auto d = now - startTs;
    if (d > SlowRequestThreshold) {
        STORAGE_WARN("Slow " << reqType << " request: "
                    << "range=" << rangeIdx << ", duration=" << d);
    }
}

void TAlignedBlockTestScenario::DoReadRequest(
    ui16 rangeIdx,
    IService& service)
{
    auto& range = Ranges[rangeIdx];
    // https://stackoverflow.com/questions/46114214/lambda-implicit-capture-fails-with-variable-declared-from-structured-binding
    ui64 blockIdx;
    TMaybe<ui64> expected;
    std::tie(blockIdx, expected) = range.RandomRead();

    ui64 blockSize = ConfigHolder->GetConfig().GetBlockSize();

    const auto startTs = Now();

    auto readHandler =
        [this, startTs, blockIdx, rangeIdx, expected, &service]() mutable
    {
        OnResponse(startTs, rangeIdx, "read", service);

        if (!expected) {
            return;
        }

        auto& range = Ranges[rangeIdx];

        ui64 partSize = range.DataSize() / range.Config.GetWriteParts();
        for (ui64 part = 0; part < range.Config.GetWriteParts(); ++part) {
            TBlockData blockData;
            memcpy(&blockData, range.Data(part * partSize), sizeof(blockData));

            if (blockData.RequestNumber != *expected ||
                blockData.PartNumber != part)
            {
                service.Fail(
                    TStringBuilder()
                    << "[" << rangeIdx << "] Wrong data in block "
                    << blockIdx
                    << " expected RequestNumber " << expected
                    << " actual TBlockData " << blockData);
                return;
            }
        }
    };

    service.Read(
        range.Data(),
        range.DataSize(),
        blockIdx * blockSize,
        readHandler);
}

void TAlignedBlockTestScenario::DoWriteRequest(
    ui16 rangeIdx,
    IService& service)
{
    auto& range = Ranges[rangeIdx];

    const auto startTs = Now();
    auto [blockIdx, iteration] = range.NextWrite();
    TBlockData blockData {
        .RequestNumber = iteration,
        .BlockIndex = blockIdx,
        .RangeIdx = rangeIdx,
        .RequestTimestamp = startTs.MicroSeconds(),
        .TestTimestamp = TestStartTimestamp.MicroSeconds(),
        .TestId = ConfigHolder->GetConfig().GetTestId(),
        .Checksum = 0
    };

    ui64 blockSize = ConfigHolder->GetConfig().GetBlockSize();
    ui64 partSize = range.DataSize() / range.Config.GetWriteParts();
    for (ui32 part = 0; part < range.Config.GetWriteParts(); ++part) {
        blockData.PartNumber = part;
        blockData.Checksum = 0;
        blockData.Checksum = Crc32c(&blockData, sizeof(blockData));
        ui64 partOffset = part * partSize;
        memcpy(range.Data(partOffset), &blockData, sizeof(blockData));
        service.Write(
            range.Data(partOffset),
            partSize,
            blockIdx * blockSize + partOffset,
            [this, startTs, rangeIdx, &service]() mutable
            {
                OnResponse(startTs, rangeIdx, "write", service);
            });
    }
}

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

ITestScenarioPtr CreateAlignedBlockTestScenario(
    IConfigHolderPtr configHolder,
    const TLog& log)
{
    return ITestScenarioPtr(
        new TAlignedBlockTestScenario(std::move(configHolder), log));
}

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
