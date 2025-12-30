#include "test_runner.h"

#include "buffer_pool.h"
#include "request_generator.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/validation/validation.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/condvar.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <atomic>

namespace NCloud::NBlockStore::NLoadTest {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCompletedRequest
{
    EBlockStoreRequest RequestType;
    TBlockRange64 BlockRange;
    NProto::TError Error;
    TDuration Elapsed;

    TCompletedRequest(
            EBlockStoreRequest requestType,
            const TBlockRange64& blockRange,
            const NProto::TError& error,
            TDuration elapsed)
        : RequestType(requestType)
        , BlockRange(blockRange)
        , Error(error)
        , Elapsed(elapsed)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TRequestsCompletionQueue
{
private:
    TDeque<std::unique_ptr<TCompletedRequest>> Items;
    TMutex Lock;

public:
    void Enqueue(std::unique_ptr<TCompletedRequest> request)
    {
        with_lock (Lock) {
            Items.emplace_back(std::move(request));
        }
    }

    std::unique_ptr<TCompletedRequest> Dequeue()
    {
        with_lock (Lock) {
            std::unique_ptr<TCompletedRequest> ptr;
            if (Items) {
                ptr = std::move(Items.front());
                Items.pop_front();
            }
            return ptr;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestRunner final
    : public ITestRunner
    , public ISimpleThread
    , public std::enable_shared_from_this<TTestRunner>
{
private:
    const TString TestName;
    TLog Log;
    NProto::TVolume Volume;
    TString LoggingTag;
    IRequestGeneratorPtr Requests;
    TString CheckpointId;
    ui32 MaxIoDepth;

    TInstant StartTs;

    ISessionPtr Session;
    TVector<ui32> SuccessOnError;
    IBlockDigestCalculatorPtr DigestCalculator;
    std::atomic<bool>& ShouldStop;

    ui64 RequestsSent = 0;
    ui64 RequestsCompleted = 0;
    ui32 CurrentIoDepth = 0;

    TInstant LastReportTs;
    ui64 LastRequestsCompleted = 0;

    TRequestsCompletionQueue CompletionQueue;
    TAutoEvent Event;

    TPromise<TTestResultsPtr> Response = NewPromise<TTestResultsPtr>();
    TTestResultsPtr TestResults = std::make_unique<TTestResults>();

    IAllocator* Allocator = BufferPool();  // TDefaultAllocator::Instance()

public:
    TTestRunner(
            TString testName,
            ILoggingServicePtr loggingService,
            ISessionPtr session,
            NProto::TVolume volume,
            TString loggingTag,
            IRequestGeneratorPtr requests,
            TString checkpointId,
            ui32 maxIoDepth,
            TVector<ui32> successOnError,
            IBlockDigestCalculatorPtr digestCalculator,
            std::atomic<bool>& shouldStop)
        : TestName(std::move(testName))
        , Log(loggingService->CreateLog(requests->Describe()))
        , Volume(std::move(volume))
        , LoggingTag(std::move(loggingTag))
        , Requests(std::move(requests))
        , CheckpointId(std::move(checkpointId))
        , MaxIoDepth(maxIoDepth)
        , Session(std::move(session))
        , SuccessOnError(std::move(successOnError))
        , DigestCalculator(std::move(digestCalculator))
        , ShouldStop(shouldStop)
    {
    }

    TFuture<TTestResultsPtr> Run() override
    {
        Start();
        return Response;
    }

    void* ThreadProc() override;

private:
    bool SendNextRequest();
    void SendReadRequest(const TBlockRange64& range);
    void SendWriteRequest(const TBlockRange64& range);
    void SendZeroRequest(const TBlockRange64& range);

    bool StopRequested() const;
    bool CheckSendRequestCondition() const;
    bool CheckExitCondition() const;

    static TSgList BuildSgList(const IAllocator::TBlock& block);

    void SignalCompletion(
        EBlockStoreRequest requestType,
        const TBlockRange64& range,
        const NProto::TError& error,
        TDuration elapsed);

    void ProcessCompletedRequests();

    void ReportProgress();
};

////////////////////////////////////////////////////////////////////////////////

void* TTestRunner::ThreadProc()
{
    SetCurrentThreadName(TestName.c_str());
    StartTs = Now();
    LastReportTs = Now();

    while (!CheckExitCondition()) {
        while (CheckSendRequestCondition() && SendNextRequest()) {
            ++RequestsSent;
        }

        if (!CheckExitCondition()) {
            Event.WaitD(Requests->Peek());
            ProcessCompletedRequests();
        }

        ReportProgress();
    }

    Response.SetValue(std::move(TestResults));
    return nullptr;
}

bool TTestRunner::SendNextRequest()
{
    if (MaxIoDepth && CurrentIoDepth >= MaxIoDepth) {
        return false;
    }

    TRequest request;
    if (!Requests->Next(&request)) {
        return false;
    }

    ++CurrentIoDepth;

    switch (request.RequestType) {
        case EBlockStoreRequest::ReadBlocks:
            SendReadRequest(request.BlockRange);
            break;

        case EBlockStoreRequest::WriteBlocks:
            SendWriteRequest(request.BlockRange);
            break;

        case EBlockStoreRequest::ZeroBlocks:
            SendZeroRequest(request.BlockRange);
            break;

        default:
            Y_ABORT_UNLESS(TStringBuilder()
                << "unexpected request type: "
                << GetBlockStoreRequestName(request.RequestType));
    }

    return true;
}

void TTestRunner::SendReadRequest(const TBlockRange64& range)
{
    STORAGE_DEBUG(LoggingTag
        << "ReadBlocks request: ("
        << range.Start << ", " << range.Size() << ")");

    auto buffer = Allocator->Allocate(Volume.GetBlockSize() * range.Size());
    auto guardedSgList = TGuardedSgList(BuildSgList(buffer));

    auto started = TInstant::Now();
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(range.Start);
    request->SetBlocksCount(range.Size());
    request->SetCheckpointId(CheckpointId);
    request->BlockSize = Volume.GetBlockSize();
    request->Sglist = guardedSgList;

    auto future = Session->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    future.Subscribe(
        [=, this, p=shared_from_this()] (const auto& f) mutable {
            guardedSgList.Close();
            Allocator->Release(buffer);

            const auto& response = f.GetValue();
            const auto& error = response.GetError();
            if (FAILED(error.GetCode())) {
                STORAGE_ERROR(LoggingTag
                    << "ReadBlocks request failed with error: "
                    << FormatError(error));
            }

            p->SignalCompletion(
                EBlockStoreRequest::ReadBlocks,
                range,
                error,
                TInstant::Now() - started);
        });
}

void TTestRunner::SendWriteRequest(const TBlockRange64& range)
{
    STORAGE_DEBUG(LoggingTag
        << "WriteBlocks request: ("
        << range.Start << ", " << range.Size() << ")");

    Y_DEBUG_ABORT_UNLESS(Volume.GetBlockSize() >= sizeof(RequestsSent));
    auto buffer = Allocator->Allocate(Volume.GetBlockSize() * range.Size());
    auto guardedSgList = TGuardedSgList(BuildSgList(buffer));

    for (ui32 b = 0; b < range.Size(); ++b) {
        auto data = static_cast<char*>(buffer.Data) + b * Volume.GetBlockSize();
        auto reqNumber = RequestsSent + 1;
        memcpy(data, &reqNumber, sizeof(reqNumber));
        memset(
            data + sizeof(reqNumber),
            1 + RandomNumber<ui8>(Max<ui8>()),
            Volume.GetBlockSize() - sizeof(reqNumber)
        );

        if (DigestCalculator) {
            // return value ignored
            DigestCalculator->Calculate(
                range.Start + b,
                {data, Volume.GetBlockSize()}
            );
        }
    }

    auto started = TInstant::Now();
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(range.Start);
    request->BlocksCount = range.Size();
    request->BlockSize = Volume.GetBlockSize();
    request->Sglist = guardedSgList;

    auto future = Session->WriteBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    future.Subscribe(
        [=, this, p=shared_from_this()] (const auto& f) mutable {
            guardedSgList.Close();
            Allocator->Release(buffer);

            const auto& response = f.GetValue();
            const auto& error = response.GetError();
            if (FAILED(error.GetCode())) {
                STORAGE_ERROR(LoggingTag
                    << "WriteBlocks request failed with error: "
                    << FormatError(error));
            }

            p->SignalCompletion(
                EBlockStoreRequest::WriteBlocks,
                range,
                error,
                TInstant::Now() - started);
        });
}

void TTestRunner::SendZeroRequest(const TBlockRange64& range)
{
    STORAGE_DEBUG(LoggingTag
        << "ZeroBlocks request: ("
        << range.Start << ", " << range.Size() << ")");

    auto started = TInstant::Now();
    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(range.Start);
    request->SetBlocksCount(range.Size());

    if (DigestCalculator) {
        TVector<char> data(Volume.GetBlockSize());
        for (ui32 b = 0; b < range.Size(); ++b) {
            // return value ignored
            DigestCalculator->Calculate(
                range.Start + b,
                {data.begin(), data.size()}
            );
        }
    }

    auto future = Session->ZeroBlocks(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    future.Subscribe(
        [=, this, p=shared_from_this()] (const auto& f) {
            const auto& response = f.GetValue();
            const auto& error = response.GetError();
            if (FAILED(error.GetCode())) {
                STORAGE_ERROR(LoggingTag
                    << "ZeroBlocks request failed with error: "
                    << FormatError(error));
            }

            p->SignalCompletion(
                EBlockStoreRequest::ZeroBlocks,
                range,
                error,
                TInstant::Now() - started);
        });
}

bool TTestRunner::StopRequested() const
{
    return ShouldStop.load(std::memory_order_acquire)
        || TestResults->Status != NProto::TEST_STATUS_OK;
}

bool TTestRunner::CheckExitCondition() const
{
    return (RequestsSent == RequestsCompleted
        && (StopRequested() || !Requests->HasMoreRequests()));
}

bool TTestRunner::CheckSendRequestCondition() const
{
    return !StopRequested() && Requests->HasMoreRequests();
}

TSgList TTestRunner::BuildSgList(const IAllocator::TBlock& block)
{
    return {{ (char*)block.Data, block.Len }};
}

void TTestRunner::SignalCompletion(
    EBlockStoreRequest requestType,
    const TBlockRange64& range,
    const NProto::TError& error,
    TDuration elapsed)
{
    CompletionQueue.Enqueue(
        std::make_unique<TCompletedRequest>(requestType, range, error, elapsed));

    Event.Signal();
}

void TTestRunner::ProcessCompletedRequests()
{
    while (auto request = CompletionQueue.Dequeue()) {
        Requests->Complete(request->BlockRange);

        Y_ABORT_UNLESS(CurrentIoDepth > 0);
        --CurrentIoDepth;

        if (FAILED(request->Error.GetCode())) {
            const auto status = FindPtr(SuccessOnError, request->Error.GetCode())
                ? NProto::TEST_STATUS_EXPECTED_ERROR
                : NProto::TEST_STATUS_FAILURE;

            if (TestResults->Status != status && status == NProto::TEST_STATUS_FAILURE) {
                STORAGE_ERROR(LoggingTag
                    << "Request failed with error: "
                    << FormatError(request->Error));
            }

            TestResults->Status = status;
        }

        ++RequestsCompleted;

        ++TestResults->RequestsCompleted;

        switch (request->RequestType) {
            case EBlockStoreRequest::ReadBlocks:
                TestResults->BlocksRead += request->BlockRange.Size();
                TestResults->ReadHist.RecordValue(request->Elapsed);
                break;

            case EBlockStoreRequest::WriteBlocks:
                TestResults->BlocksWritten += request->BlockRange.Size();
                TestResults->WriteHist.RecordValue(request->Elapsed);
                break;

            case EBlockStoreRequest::ZeroBlocks:
                TestResults->BlocksZeroed += request->BlockRange.Size();
                TestResults->ZeroHist.RecordValue(request->Elapsed);
                break;

            default:
                Y_ABORT_UNLESS(TStringBuilder()
                    << "unexpected request type: "
                    << GetBlockStoreRequestName(request->RequestType));
        }
    }
}

void TTestRunner::ReportProgress()
{
    const auto reportInterval = TDuration::Seconds(5);

    auto now = Now();
    if (now - LastReportTs > reportInterval) {
        const auto timePassed = now - LastReportTs;
        const auto requestsCompleted = RequestsCompleted - LastRequestsCompleted;

        STORAGE_INFO(LoggingTag
            << "Current IOPS: " << (requestsCompleted / timePassed.Seconds()));

        LastReportTs = now;
        LastRequestsCompleted = RequestsCompleted;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestRunnerPtr CreateTestRunner(
    TString testName,
    ILoggingServicePtr loggingService,
    ISessionPtr session,
    NProto::TVolume volume,
    TString loggingTag,
    IRequestGeneratorPtr requests,
    TString checkpointId,
    ui32 maxIoDepth,
    TVector<ui32> successOnError,
    IBlockDigestCalculatorPtr digestCalculator,
    std::atomic<bool>& shouldStop)
{
    return std::make_shared<TTestRunner>(
        std::move(testName),
        std::move(loggingService),
        std::move(session),
        std::move(volume),
        std::move(loggingTag),
        std::move(requests),
        std::move(checkpointId),
        maxIoDepth,
        std::move(successOnError),
        std::move(digestCalculator),
        shouldStop);
}

}   // namespace NCloud::NBlockStore::NLoadTest
