#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/validation/public.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>

#include <cloud/blockstore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TTestResults
{
    NProto::ETestStatus Status = NProto::TEST_STATUS_OK;

    size_t RequestsCompleted = 0;

    size_t BlocksRead = 0;
    size_t BlocksWritten = 0;
    size_t BlocksZeroed = 0;

    TLatencyHistogram ReadHist;
    TLatencyHistogram WriteHist;
    TLatencyHistogram ZeroHist;
};

////////////////////////////////////////////////////////////////////////////////

struct ITestRunner
{
    virtual ~ITestRunner() = default;

    virtual NThreading::TFuture<TTestResultsPtr> Run() = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITestRunnerPtr CreateTestRunner(
    TString testName,
    ILoggingServicePtr loggingService,
    NClient::ISessionPtr session,
    NProto::TVolume volume,
    TString loggingTag,
    IRequestGeneratorPtr requestGenerator,
    TString checkpointId,
    ui32 maxIoDepth,
    TVector<ui32> successOnError,
    IBlockDigestCalculatorPtr digestCalculator,
    std::atomic<bool>& shouldStop);

}   // namespace NCloud::NBlockStore::NLoadTest
