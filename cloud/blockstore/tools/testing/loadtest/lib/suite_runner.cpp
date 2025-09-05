#include "suite_runner.h"

#include "helpers.h"
#include "request_generator.h"

#include <cloud/blockstore/libs/storage/model/public.h>
#include <cloud/blockstore/libs/validation/validation.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TCachingDigestCalculator final
    : IBlockDigestCalculator
{
    IBlockDigestCalculatorPtr Parent;
    TTestContext::TBlockChecksums& Checksums;

    TCachingDigestCalculator(
            IBlockDigestCalculatorPtr parent,
            TTestContext::TBlockChecksums& checksums)
        : Parent(std::move(parent))
        , Checksums(checksums)
    {
    }

    ui64 Calculate(ui64 blockIndex, const TStringBuf block) const override
    {
        const auto checksum = Parent->Calculate(blockIndex, block);
        if (blockIndex != NStorage::InvalidBlockIndex) {
            Checksums[blockIndex] = checksum;
        }
        return checksum;
    }
};

////////////////////////////////////////////////////////////////////////////////

TSuiteRunner::TSuiteRunner(
        TAppContext& appContext,
        ILoggingServicePtr logging,
        const TVector<ui32>& successOnError,
        const TString& checkpointId,
        const TString& testName,
        TTestContext& testContext)
    : TestName(testName)
    , AppContext(appContext)
    , Logging(std::move(logging))
    , SuccessOnError(successOnError)
    , CheckpointId(checkpointId)
    , LoggingTag(MakeLoggingTag(testName))
    , TestContext(testContext)
    , StartTime(Now())
{}

void TSuiteRunner::StartSubtest(const NProto::TRangeTest& range)
{
    auto runner = CreateTestRunner(
        TestName,
        Logging,
        TestContext.Session,
        TestContext.Volume,
        LoggingTag,
        CreateArtificialRequestGenerator(Logging, range),
        CheckpointId,
        range.GetIoDepth(),
        SuccessOnError,
        TestContext.DigestCalculator
            ? std::make_shared<TCachingDigestCalculator>(
                TestContext.DigestCalculator,
                TestContext.BlockChecksums
            ) : nullptr,
        TestContext.ShouldStop);

    RegisterSubtest(std::move(runner));
}

void TSuiteRunner::StartReplay(
    const TString& profileLogPath,
    ui32 maxIoDepth,
    bool fullSpeed,
    const TString& diskId,
    const TString& startTime,
    const TString& endTime,
    ui64 maxRequestsInMemory)
{
    auto runner = CreateTestRunner(
        TestName,
        Logging,
        TestContext.Session,
        TestContext.Volume,
        LoggingTag,
        CreateRealRequestGenerator(
            Logging,
            profileLogPath,
            diskId ? diskId : TestContext.Volume.GetDiskId(),
            startTime,
            endTime,
            fullSpeed,
            maxRequestsInMemory,
            TestContext.ShouldStop
        ),
        CheckpointId,
        maxIoDepth,
        SuccessOnError,
        nullptr,
        TestContext.ShouldStop);

    RegisterSubtest(std::move(runner));
}

void TSuiteRunner::RegisterSubtest(ITestRunnerPtr runner)
{
    runner->Run().Subscribe([&] (const auto& future) mutable {
        const auto& testResults = future.GetValue();

        with_lock (TestContext.WaitMutex) {
            ++CompletedSubtests;

            if (Results.Status == NProto::TEST_STATUS_OK) {
                Results.Status = testResults->Status;
            }

            Results.RequestsCompleted += testResults->RequestsCompleted;

            Results.BlocksRead += testResults->BlocksRead;
            Results.BlocksWritten += testResults->BlocksWritten;
            Results.BlocksZeroed += testResults->BlocksZeroed;

            Results.ReadHist.Add(testResults->ReadHist);
            Results.WriteHist.Add(testResults->WriteHist);
            Results.ZeroHist.Add(testResults->ZeroHist);

            switch (testResults->Status) {
                case NProto::TEST_STATUS_FAILURE:
                    AppContext.FailedTests.fetch_add(1);
                    break;
                case NProto::TEST_STATUS_EXPECTED_ERROR:
                    StopTest(TestContext);
                    break;
                default:
                    break;
            }

            TestContext.WaitCondVar.Signal();
            TestContext.Finished.store(true, std::memory_order_release);
        }
    });

    Subtests.push_back(std::move(runner));
}

void TSuiteRunner::Wait(ui64 duration)
{
    TInstant expectedEnd = TInstant::Max();
    if (duration) {
        expectedEnd = StartTime + TDuration::Seconds(duration);
    }

    with_lock (TestContext.WaitMutex) {
        while (CompletedSubtests < Subtests.size()) {
            TestContext.WaitCondVar.WaitD(
                TestContext.WaitMutex, expectedEnd);
            if (TInstant::Now() > expectedEnd
                    && !AppContext.ShouldStop.load(std::memory_order_acquire))
            {
                StopTest(TestContext);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NLoadTest
