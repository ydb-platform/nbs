#pragma once

#include "public.h"

#include <functional>

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

/**
* Runs a test scenario until Stop is requested.
*
* A test scenario is represented by a collection of concurrently executed
* logical threads.
*
* Each thread is executed repeatedly by calling ITestThread::Run.
* It should initiate one or several read or write requests via the provided
* context object.
* Next call of ITestThread::Run is made when all the requests are handled.
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

struct ITestThreadContext
{
    using TCallback = std::function<void()>;

    virtual ~ITestThreadContext() = default;

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

    virtual void Stop() = 0;

    virtual void Fail(TStringBuf message) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// It doesn't necessary correspond to a real thread and instead is considered
// as a logical thread. The Run method can be called from any thread.
struct ITestScenarioThread
{
    virtual ~ITestScenarioThread() = default;

    virtual void Run(
        double secondsSinceTestStart,
        ITestThreadContext& context) = 0;
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

    virtual ui32 GetThreadCount() const = 0;

    virtual ITestScenarioThread& GetThread(ui32 index) const = 0;
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
