#pragma once

#include "public.h"

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/types.h>

#include <functional>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

/**
* Runs a test scenario until Stop is requested.
*
* A test scenario is represented by a collection of concurrently executed
* workers.
*
* Each worker is executed repeatedly by calling ITestScenarioWorker::Run.
* It should initiate one or several read or write requests via the provided
* service object. Next call of ITestScenarioWorker::Run is made when all
* the requests are completed and handled.
*/
struct ITestExecutor
{
    virtual ~ITestExecutor() = default;

    // The function returns when the test is finished (true) or failed (false)
    virtual bool Run() = 0;

    // Requests the test to stop and returns immediately
    virtual void Stop() = 0;
};

using ITestExecutorPtr = std::shared_ptr<ITestExecutor>;

////////////////////////////////////////////////////////////////////////////////

struct ITestExecutorIOService
{
    using TCallback = std::function<void()>;

    virtual ~ITestExecutorIOService() = default;

    // Initiates an asynchronous read operation and calls the callback when
    // the operation is completed successfully.
    // If it fails, the test is stopped and the error is reported.
    virtual void Read(
        void* buffer,
        ui32 count,
        ui64 offset,
        TCallback callback) = 0;

    // Initiates an asynchronous write operation and calls the callback when
    // the operation is completed successfully.
    // If it fails, the test is stopped and the error is reported.
    virtual void Write(
        const void* buffer,
        ui32 count,
        ui64 offset,
        TCallback callback) = 0;

    // Gracefully stop the scenario
    virtual void Stop() = 0;

    // Stop and write the error message
    virtual void Fail(TStringBuf message) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Workers are run concurrently - Run method and callbacks passed to
// ITestExecutorIOService::Read and Write can be called from any thread.
struct ITestScenarioWorker
{
    virtual ~ITestScenarioWorker() = default;

    virtual void Run(
        double secondsSinceTestStart,
        ITestExecutorIOService& service) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ITestScenario
{
    virtual ~ITestScenario() = default;

    virtual bool Initialize(TFileHandle& fileHandle)
    {
        Y_UNUSED(fileHandle);
        return true;
    }

    virtual ui32 GetWorkerCount() const = 0;

    virtual ITestScenarioWorker& GetWorker(ui32 index) const = 0;
};

using ITestScenarioPtr = std::shared_ptr<ITestScenario>;

////////////////////////////////////////////////////////////////////////////////

struct TTestExecutorSettings
{
    ITestScenarioPtr TestScenario;
    TString FilePath;
    ui64 FileSize = 0;
    TLog Log;
};

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateAlignedBlockTestScenario(
    IConfigHolderPtr configHolder,
    const TLog& log);

ITestExecutorPtr CreateTestExecutor(
    IConfigHolderPtr configHolder,
    const TLog& log);

}   // namespace NCloud::NBlockStore::NTesting
