#pragma once

#include "app_context.h"
#include "test_runner.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TSuiteRunner
{
private:
    const TString TestName;
    TAppContext& AppContext;
    ILoggingServicePtr Logging;
    const TVector<ui32>& SuccessOnError;
    const TString& CheckpointId;
    TString LoggingTag;
    TTestContext& TestContext;

    TInstant StartTime;
    TTestResults Results;
    TVector<ITestRunnerPtr> Subtests;
    size_t CompletedSubtests = 0;

public:
    TSuiteRunner(
        TAppContext& appContext,
        ILoggingServicePtr logging,
        const TVector<ui32>& successOnError,
        const TString& checkpointId,
        const TString& testName,
        TTestContext& testContext);

public:
    TInstant GetStartTime() const
    {
        return StartTime;
    }

    const TTestResults& GetResults() const
    {
        return Results;
    }

    void StartSubtest(const NProto::TRangeTest& range);
    void StartReplay(
        const TString& profileLogPath,
        ui32 maxIoDepth,
        bool fullSpeed,
        const TString& diskId,
        const TString& startTime,
        const TString& endTime,
        const ui64 maxRequestsInMemory);
    void Wait(ui64 duration);

private:
    void RegisterSubtest(ITestRunnerPtr runner);
};

}   // namespace NCloud::NBlockStore::NLoadTest
