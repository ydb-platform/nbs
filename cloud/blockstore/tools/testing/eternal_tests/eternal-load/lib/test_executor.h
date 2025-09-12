#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/future/core/future.h>

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

    virtual bool Run() = 0;

    virtual void Stop() = 0;
};

using ITestExecutorPtr = std::shared_ptr<ITestExecutor>;

////////////////////////////////////////////////////////////////////////////////

struct ITestThreadContext
{
    using TCallback = std::function<void()>;

    virtual ~ITestThreadContext() = default;

    virtual void Stop() = 0;

    virtual void Fail(TStringBuf message) = 0;

    virtual void Read(
        void* buffer,
        ui32 count,
        ui64 offset,
        TCallback callback) = 0;

    virtual void Write(
        const void* buffer,
        ui32 count,
        ui64 offset,
        TCallback callback) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// It doesn't necessary correspond to a real thread and instead is considered
// as a logical thread. The Run method can be called from any thread.
struct ITestThread
{
    virtual ~ITestThread() = default;

    virtual void Run(
        double secondsSinceTestStart,
        ITestThreadContext* context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ITestScenario
{
    virtual ~ITestScenario() = default;

    virtual ui32 GetThreadCount() const = 0;

    virtual ITestThread* GetThread(ui32 index) const = 0;

    virtual bool Init(TFileHandle& handle)
    {
        Y_UNUSED(handle);
        return true;
    }
};

using ITestScenarioPtr = std::shared_ptr<ITestScenario>;

////////////////////////////////////////////////////////////////////////////////

enum class ETestExecutorFileService
{
    AsyncIo,
    IoUring,
    Sync
};

////////////////////////////////////////////////////////////////////////////////

struct TTestExecutorSettings
{
    ITestScenarioPtr TestScenario;
    ETestExecutorFileService FileService;
    TString FilePath;
    ui64 FileSize = 0;
    TLog Log;
    bool RunFromAnyThread = false;
    bool NoDirect = false;
    bool PrintDebugInfo = false;
};

////////////////////////////////////////////////////////////////////////////////

ITestExecutorPtr CreateTestExecutor(TTestExecutorSettings settings);

}   // namespace NCloud::NBlockStore::NTesting
