#include "loop.h"
#include "fs.h"
#include "vfs.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>

#include <cloud/storage/core/libs/common/affinity.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/executor_counters.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NCloud::NFileStore::NVFSVhost {

using namespace NCloud::NFileStore::NClient;
using namespace NCloud::NFileStore::NVFS;
using namespace NCloud::NProto;

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : public TIntrusiveListItem<TRequest>
    , public TAtomicRefCount<TRequest>
{
    const TVfsRequestPtr VfsRequest;
    std::atomic_flag Completed = 0;

    TRequest(TVfsRequestPtr vfsRequest)
        : VfsRequest(std::move(vfsRequest))
    {}
};

using TRequestPtr = TIntrusivePtr<TRequest>;

////////////////////////////////////////////////////////////////////////////////

class TVfsLoop final
    : public std::enable_shared_from_this<TVfsLoop>
    , public IFileSystemLoop
{
private:
    const TString SocketPath;
    const IFileSystemPtr FileSystem;

    TLog Log;
    IVfsDevicePtr VfsDevice;

    TAdaptiveLock RequestsLock;
    TIntrusiveList<TRequest> RequestsInFlight;

    std::atomic_flag Stopped = false;

public:
    TVfsLoop(TString socketPath,
            IFileSystemPtr fileSystem,
            ILoggingServicePtr logging)
        : SocketPath(std::move(socketPath))
        , FileSystem(std::move(fileSystem))
        , Log(logging->CreateLog("NFS_VHOST"))
    {}

    void SetVfsDevice(IVfsDevicePtr vfsDevice)
    {
        Y_ABORT_UNLESS(!VfsDevice);
        VfsDevice = std::move(vfsDevice);
    }

    TFuture<NProto::TError> StartAsync() override
    {
        Y_ABORT_UNLESS(VfsDevice);

        STORAGE_TRACE("Starting device at " << SocketPath);
        NProto::TError error = InitDevice();
        if (HasError(error)) {
            return MakeFuture(error);
        }

        STORAGE_TRACE("Starting filesystem at " << SocketPath);
        return FileSystem->Start();
    }

    TFuture<NProto::TError> StopAsync() override
    {
        if (Stopped.test_and_set()) {
            return MakeFuture(MakeError(S_ALREADY));
        }

        STORAGE_TRACE("Stopping filesystem at " << SocketPath);
        auto self = shared_from_this();
        return FileSystem->Stop().Apply([=] (const auto& future) {
            Y_UNUSED(future);
            auto cancelError = MakeError(E_IO, "endpoint is stopping");
            return self->DoStop(true, cancelError);
        });
    }

    NThreading::TFuture<void> SuspendAsync() override
    {
        if (Stopped.test_and_set()) {
            return MakeFuture();
        }

        // Should not call filesystem::stop()
        STORAGE_TRACE("Suspending filesystem at " << SocketPath);
        auto cancelError = MakeError(E_CANCELLED, "endpoint is suspending");
        return DoStop(false, cancelError).IgnoreResult();
    }

    NThreading::TFuture<NProto::TError> AlterAsync(
        bool readOnly,
        ui64 mountSeqNumber) override
    {
        return FileSystem->Alter(readOnly, mountSeqNumber);
    }

    void ProcessRequest(TVfsRequestPtr vfsRequest)
    {
        auto request = RegisterRequest(std::move(vfsRequest));
        if (!request) {
            return;
        }

        auto self = weak_from_this();
        auto future = FileSystem->Process(request->VfsRequest);
        future.Apply([weak = std::move(self), req = std::move(request)] (const auto& future) {
            if (auto self = weak.lock()) {
                try {
                    self->CompleteRequest(*req, future.GetValue());
                    self->UnregisterRequest(*req);
                } catch (...) {
                    // FIXME: should not happen
                    Y_ABORT();
                }
            }
        });
    }

private:
    NProto::TError InitDevice()
    {
        TFsPath(SocketPath).DeleteIfExists();

        bool started = VfsDevice->Start();
        if (!started) {
            NProto::TError error;
            error.SetCode(E_FAIL);
            error.SetMessage(TStringBuilder()
                << "could not register fs device "
                << SocketPath.Quote());

            return error;
        }

        int mode = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;
        auto err = Chmod(SocketPath.c_str(), mode);

        if (err != 0) {
            NProto::TError error;
            error.SetCode(MAKE_SYSTEM_ERROR(err));
            error.SetMessage(TStringBuilder()
                << "failed to chmod socket "
                << SocketPath.Quote()
                <<": " << LastSystemError());

            return error;
        }

        return {};
    }

    TFuture<NProto::TError> DoStop(bool deleteSocket, NProto::TError error)
    {
        auto future = VfsDevice->Stop();

        with_lock (RequestsLock) {
            STORAGE_INFO("stop endpoint " << SocketPath.Quote()
                << " with " << RequestsInFlight.Size() << " inflight requests");

            RequestsInFlight.ForEach([&] (TRequest* request) {
                CompleteRequest(*request, error);
                request->Unlink();
            });
        }

        if (deleteSocket) {
            future = future.Apply([socketPath = SocketPath] (const auto& f) {
                TFsPath(socketPath).DeleteIfExists();
                return f.GetValue();
            });
        }

        return future;
    }

    TRequestPtr RegisterRequest(TVfsRequestPtr vfsRequest)
    {
        auto request = MakeIntrusive<TRequest>(
            std::move(vfsRequest));

        with_lock (RequestsLock) {
            if (!Stopped.test()) {
                RequestsInFlight.PushBack(request.Get());
                return request;
            }
        }

        auto error = MakeError(E_CANCELLED, "vhost endpoint was stopped");
        CompleteRequest(*request, error);
        return nullptr;
    }

    void CompleteRequest(TRequest& request, const NProto::TError& error)
    {
        if (request.Completed.test_and_set()) {
            return;
        }

        auto statsError = error;
        auto vfsResult = GetResult(statsError);
        request.VfsRequest->Complete(vfsResult);
    }

    void UnregisterRequest(TRequest& request)
    {
        with_lock (RequestsLock) {
            request.Unlink();
        }
    }

    TVfsRequest::EResult GetResult(NProto::TError& error)
    {
        if (!HasError(error)) {
            return TVfsRequest::SUCCESS;
        }

        bool cancelError =
            error.GetCode() == E_CANCELLED ||
            GetErrorKind(error) == EErrorKind::ErrorRetriable;

        bool stopEndpoint = Stopped.test();
        if (stopEndpoint && cancelError) {
            auto flags = error.GetFlags();
            SetProtoFlag(flags, NProto::EF_SILENT);
            error.SetFlags(flags);

            return TVfsRequest::CANCELLED;
        }

        return TVfsRequest::IOERR;
    }
};

using TVfsLoopPtr = std::shared_ptr<TVfsLoop>;

////////////////////////////////////////////////////////////////////////////////

class TExecutor final
    : public ISimpleThread
{
private:
    const TString Name;
    const TAffinity Affinity;
    const IVfsQueuePtr VfsQueue;

    std::atomic<ui64> QueuesCount;

    TLog Log;

    // TODO: TExecutorCounters::TExecutorScope ExecutorScope;

public:
    TExecutor(
            TString name,
            const TAffinity& affinity,
            IVfsQueuePtr VfsQueue,
            ILoggingServicePtr logging)
        : Name(std::move(name))
        , Affinity(affinity)
        , VfsQueue(std::move(VfsQueue))
        , Log(logging->CreateLog("NFS_VHOST"))
    {}

    void Shutdown()
    {
        VfsQueue->Stop();
        Join();
    }

    void AddLoop(TString socketPath, const TVfsLoopPtr& loop)
    {
        auto vhostDevice = VfsQueue->CreateDevice(
            socketPath,
            loop.get());

        loop->SetVfsDevice(std::move(vhostDevice));

        QueuesCount.fetch_add(1);
    }

    void RemoveLoop()
    {
        ui64 prev = QueuesCount.fetch_sub(1);
        Y_ABORT_UNLESS(prev > 0);
    }

    ui32 GetRequestQueueCount() const
    {
        return QueuesCount.load();
    }

private:
    void* ThreadProc() override
    {
        TAffinityGuard affinityGuard(Affinity);
        ::NCloud::SetCurrentThreadName(Name);

        while (true) {
            int res = RunRequestQueue();
            if (res != -EAGAIN) {
                if (res < 0) {
                    ReportVfsQueueRunningError();
                    STORAGE_ERROR(
                        "Failed to run vhost request queue. Return code: " << -res);
                }
                break;
            }

            while (auto req = VfsQueue->DequeueRequest()) {
                ProcessRequest(std::move(req));
            }
        }

        return nullptr;
    }

    int RunRequestQueue()
    {
        // TODO: auto activity = ExecutorScope.StartWait();
        return VfsQueue->Run();
    }

    void ProcessRequest(TVfsRequestPtr vhostRequest)
    {
        // TODO: auto activity = ExecutorScope.StartExecute();
        auto* loop = reinterpret_cast<TVfsLoop*>(vhostRequest->Cookie);
        loop->ProcessRequest(std::move(vhostRequest));
    }
};

using TExecutorPtr = std::shared_ptr<TExecutor>;

////////////////////////////////////////////////////////////////////////////////

class TVfsLoopFactory
    : public IFileSystemLoopFactory
{
private:
    const TLoopFactoryConfig Config;
    const IVfsQueueFactoryPtr VfsQueueFactory;
    const IFileSystemFactoryPtr FileSystemFactory;
    const ILoggingServicePtr Logging;
    const ISchedulerPtr Scheduler;
    const IRequestStatsRegistryPtr RequestStats;
    const IProfileLogPtr ProfileLog;

    TVector<TExecutorPtr> Executors;

public:
    TVfsLoopFactory(
            const TLoopFactoryConfig& config,
            IVfsQueueFactoryPtr vfsQueueFactory,
            IFileSystemFactoryPtr fileSystemFactory,
            ILoggingServicePtr logging,
            ISchedulerPtr scheduler,
            IRequestStatsRegistryPtr requestStats,
            IProfileLogPtr profileLog)
        : Config(config)
        , VfsQueueFactory(std::move(vfsQueueFactory))
        , FileSystemFactory(std::move(fileSystemFactory))
        , Logging(std::move(logging))
        , Scheduler(std::move(scheduler))
        , RequestStats(std::move(requestStats))
        , ProfileLog(std::move(profileLog))
    {
        InitExecutors();
    }

    ~TVfsLoopFactory()
    {
        for (auto& executor: Executors) {
            executor->Shutdown();
        }
    }

    IFileSystemLoopPtr Create(
        TVFSConfigPtr config,
        NClient::ISessionPtr session)
    {
        auto* executor = PickExecutor();
        Y_ABORT_UNLESS(executor);

        auto fileSystem = FileSystemFactory->Create(
            config,
            std::move(session),
            Scheduler,
            RequestStats,
            ProfileLog);

        auto loop = std::make_shared<TVfsLoop>(
            config->GetSocketPath(),
            std::move(fileSystem),
            Logging);

        // FIXME: subscribe for RemoveLoop
        executor->AddLoop(config->GetSocketPath(), loop);

        return loop;
    }

private:
    void InitExecutors()
    {
        Executors.resize(Config.ThreadsCount);
        for (size_t i = 0; i < Config.ThreadsCount; ++i) {
            auto vhostQueue = VfsQueueFactory->CreateQueue();
            auto executor = std::make_unique<TExecutor>(
                TStringBuilder() << "VfsLoop" << i,
                Config.Affinity,
                std::move(vhostQueue),
                Logging);

            executor->Start();
            Executors[i] = std::move(executor);
        }
    }

    TExecutor* PickExecutor() const
    {
        TExecutor* result = nullptr;
        for (const auto& executor: Executors) {
            if (result == nullptr ||
                executor->GetRequestQueueCount() < result->GetRequestQueueCount())
            {
                result = executor.get();
            }
        }

        return result;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileSystemLoopFactoryPtr CreateVfsLoopFactory(
    const TLoopFactoryConfig& config,
    IVfsQueueFactoryPtr vfsQueueFactory,
    IFileSystemFactoryPtr fileSystemFactory,
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    IRequestStatsRegistryPtr requestStats,
    IProfileLogPtr profileLog)
{
    return std::make_shared<TVfsLoopFactory>(
        config,
        std::move(vfsQueueFactory),
        std::move(fileSystemFactory),
        std::move(logging),
        std::move(scheduler),
        std::move(requestStats),
        std::move(profileLog));
}

}   // namespace NCloud::NFileStore::NVFSVhost
