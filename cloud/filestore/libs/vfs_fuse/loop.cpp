#include "loop.h"

#include "config.h"
#include "fs.h"
#include "fuse.h"
#include "handle_ops_queue.h"
#include "log.h"
#include "util/system/file_lock.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/convert.h>
#include <cloud/filestore/libs/vfs/loop.h>
#include <cloud/filestore/libs/vfs/probes.h>
#include <cloud/filestore/libs/vfs/protos/session.pb.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/atomic/bool.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/fs.h>
#include <util/system/info.h>
#include <util/system/rwlock.h>
#include <util/system/thread.h>
#include <util/thread/factory.h>

#include <cerrno>
#include <pthread.h>

namespace NCloud::NFileStore::NFuse {

LWTRACE_USING(FILESTORE_VFS_PROVIDER);

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;
using namespace NCloud::NFileStore::NVFS;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf HandleOpsQueueFileName = "handle_ops_queue";
static constexpr TStringBuf WriteBackCacheFileName = "write_back_cache";

NProto::TError CreateAndLockFile(
    const TString& dir,
    const TStringBuf& fileName,
    THolder<TFileLock>& fileLock)
{
    if (!NFs::MakeDirectoryRecursive(dir)) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to create directories, path: " << dir);
    }

    auto filePath = TFsPath(dir) / fileName;
    filePath.Touch();
    fileLock = MakeHolder<TFileLock>(filePath);
    if (!fileLock->TryAcquire()) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to lock file, path: %s " << filePath);
    }
    return {};
}

NProto::TError UnlockAndDeleteFile(
    const TString& dir,
    THolder<TFileLock>& fileLock)
{
    fileLock->Release();

    try {
        NFs::RemoveRecursive(dir);
    } catch (const TSystemError& err) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to remove dir"
                             << ", reason: " << err.AsStrBuf());
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TCompletionQueue final
    : public ICompletionQueue
    , public IIncompleteRequestProvider
{
private:
    const TString FileSystemId;
    const IRequestStatsPtr RequestStats;
    TLog Log;
    const NProto::EStorageMediaKind RequestMediaKind;

    enum fuse_cancelation_code CancelCode{};
    NAtomic::TBool ShouldStop = false;
    TAtomicCounter CompletingCount = {0};

    TMutex RequestsLock;
    THashMap<fuse_req_t, TCallContextPtr> Requests;

    TPromise<void> StopPromise = NewPromise<void>();

public:
    TCompletionQueue(
            TString fileSystemId,
            IRequestStatsPtr stats,
            TLog& log,
            NProto::EStorageMediaKind requestMediaKind)
        : FileSystemId(std::move(fileSystemId))
        , RequestStats(std::move(stats))
        , Log(log)
        , RequestMediaKind(requestMediaKind)
    {
    }

    TMaybe<enum fuse_cancelation_code> Enqueue(
        fuse_req_t req,
        TCallContextPtr context)
    {
        TGuard g{RequestsLock};

        if (ShouldStop) {
            return CancelCode;
        }

        Requests[req] = std::move(context);
        return Nothing();
    }

    int Complete(fuse_req_t req, TCompletionCallback cb) noexcept override
    {
        with_lock (RequestsLock) {
            if (!Requests.erase(req)) {
                return 0;
            }
            CompletingCount.Inc();
        }

        int ret = cb(req);
        auto completingCount = CompletingCount.Dec();
        bool noCompleting = completingCount == 0;

        if (ShouldStop) {
            STORAGE_INFO("[f:%s] Complete: completing left: %ld",
                FileSystemId.c_str(),
                completingCount);

            if (noCompleting) {
                bool noInflight = false;
                ui32 requestsSize = 0;
                with_lock (RequestsLock) {
                    noInflight = Requests.empty();
                    requestsSize = Requests.size();
                    // double-checking needed because inflight count and completing
                    // count should be checked together atomically
                    completingCount = CompletingCount.Val();
                    noCompleting = completingCount == 0;
                }

                STORAGE_INFO("[f:%s] Complete: completing left: %ld"
                    ", requests left: %u",
                    FileSystemId.c_str(),
                    completingCount,
                    requestsSize);

                if (noInflight && noCompleting) {
                    StopPromise.TrySetValue();
                }
            }
        }

        return ret;
    }

    TFuture<void> StopAsync(enum fuse_cancelation_code code)
    {
        CancelCode = code;
        ShouldStop = true;

        bool canStop = false;
        ui32 requestsSize = 0;
        i64 completingCount = 0;

        with_lock (RequestsLock) {
            requestsSize = Requests.size();
            completingCount = CompletingCount.Val();
            canStop = Requests.empty() && completingCount == 0;

            // cancel signal is needed for the ops that may be indefinitely
            // retried by our vfs layer - e.g. AcquireLock
            for (auto& request: Requests) {
                request.second->CancellationCode = CancelCode;
                request.second->Cancelled = true;
            }
        }

        STORAGE_INFO(
            "[f:%s] StopAsync: completing left: %ld, requests left: %u, "
            "fuse cancellation code: %u",
            FileSystemId.c_str(),
            completingCount,
            requestsSize,
            code);

        if (canStop) {
            StopPromise.TrySetValue();
        }

        return StopPromise;
    }

    void Accept(IIncompleteRequestCollector& collector) override
    {
        auto now = GetCycleCount();

        TGuard g{RequestsLock};
        for (auto&& [_, context]: Requests) {
            if (const auto time = context->CalcRequestTime(now); time) {
                collector.Collect(
                    TIncompleteRequest(
                        RequestMediaKind,
                        context->RequestType,
                        time.ExecutionTime,
                        time.TotalTime));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TArgs
{
private:
    fuse_args Args = FUSE_ARGS_INIT(0, nullptr);

public:
    TArgs(const TVFSConfig& config, ui32 threadsCount)
    {
        AddArg(""); // fuse_opt_parse starts with 1

        if (config.GetDebug()) {
            AddArg("-odebug");
        }

#if defined(FUSE_VIRTIO)
        if (auto path = config.GetSocketPath()) {
            AddArg("--socket-path=" + path);
        }


        // HIPRIO + number of requests queues
        ui32 backendQueues = Max(2u, config.GetVhostQueuesCount());
        AddArg("--num-backend-queues=" + ToString(backendQueues));
        AddArg("--num-frontend-queues=" + ToString(threadsCount));
#else
        Y_UNUSED(threadsCount);
        if (config.GetReadOnly()) {
            AddArg("-oro");
        }
#endif
    }

    ~TArgs()
    {
        fuse_opt_free_args(&Args);
    }

    void AddArg(const TString& arg)
    {
        fuse_opt_add_arg(&Args, arg.c_str());
    }

    operator fuse_args* ()
    {
        return &Args;
    }
};

#if defined(FUSE_VIRTIO)

////////////////////////////////////////////////////////////////////////////////
// virtio-fs version

class TSession
{
private:
    TArgs Args;

    fuse_session* Session = nullptr;

public:
    TSession(
            const TVFSConfig& config,
            ui32 threadsCount,
            const fuse_lowlevel_ops& ops,
            const TString& state,
            void* context)
        : Args(config, threadsCount)
    {
        Session = fuse_session_new(Args, &ops, sizeof(ops), context);
        Y_ENSURE(Session, "fuse_session_new() failed");

        TryLoadSessionState(state);

        int error = fuse_session_mount(Session);
        Y_ENSURE(!error, "fuse_session_mount() failed");
    }

    void TryLoadSessionState(const TString& state)
    {
        NProto::TVfsSessionState proto;
        if (!state || !proto.ParseFromString(state)) {
            return;
        }

        // FIXME: sanity check
        Y_ABORT_UNLESS(proto.GetProtoMajor());
        Y_ABORT_UNLESS(proto.GetBufferSize());

        fuse_session_params params = {
            proto.GetProtoMajor(),
            proto.GetProtoMinor(),
            proto.GetCapable(),
            proto.GetWant(),
            proto.GetBufferSize(),
        };

        fuse_session_setparams(Session, &params);
    }

    void Exit()
    {
        if (Session) {
            fuse_session_exit(Session);
        }
    }

    void Unmount()
    {
        if (Session) {
            fuse_session_unmount(Session);
            fuse_session_destroy(Session);
            Session = nullptr;
        }
    }

    operator fuse_session* ()
    {
        return Session;
    }

    TString Dump() const
    {
        Y_ABORT_UNLESS(Session);

        fuse_session_params params;
        fuse_session_getparams(Session, &params);

        NProto::TVfsSessionState session;
        session.SetProtoMajor(params.proto_major);
        session.SetProtoMinor(params.proto_minor);
        session.SetCapable(params.capable);
        session.SetWant(params.want);
        session.SetBufferSize(params.bufsize);

        TString result;
        Y_PROTOBUF_SUPPRESS_NODISCARD session.SerializeToString(&result);

        return result;
    }

    void Suspend()
    {
        fuse_session_suspend(Session);
    }
};

#else

////////////////////////////////////////////////////////////////////////////////
// regular FUSE version

class TSession
{
private:
    TArgs Args;
    TString MountPath;

    fuse_chan* Channel = nullptr;
    fuse_session* Session = nullptr;

public:
    TSession(
            const TVFSConfig& config,
            ui32 threadsCount,
            const fuse_lowlevel_ops& ops,
            const TString& state,
            void* context)
        : Args(config, threadsCount)
        , MountPath(config.GetMountPath())
    {
        Y_UNUSED(state);

        Channel = fuse_mount(MountPath.c_str(), Args);
        Y_ENSURE(Channel, "fuse_mount() failed");

        Session = fuse_lowlevel_new(Args, &ops, sizeof(ops), context);
        Y_ENSURE(Session, "fuse_lowlevel_new() failed");

        fuse_session_add_chan(Session, Channel);
    }

    void Exit()
    {
        fuse_session_exit(*this);
    }

    void Unmount()
    {
        if (Session) {
            fuse_session_remove_chan(Channel);
            fuse_session_destroy(Session);
            fuse_unmount(MountPath.c_str(), Channel);
            Session = nullptr;
        }
    }

    operator fuse_session* ()
    {
        return Session;
    }

    TString Dump() const
    {
        return {};
    }

    void Suspend()
    {
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

class TFuseLoopThread final
    : public ISimpleThread
{
private:
    TSession& Session;
    const ui32 FuseLoopIndex = 0;
    const ui32 QueueIndex = 0;
    bool InterruptSignaled = false;
    TLog Log;

    pthread_t ThreadId = 0;

public:
    TFuseLoopThread(
        TSession& session,
        ui32 fuseLoopIndex,
        ui32 queueIndex,
        TLog log)
        : Session(session)
        , FuseLoopIndex(fuseLoopIndex)
        , QueueIndex(queueIndex)
        , Log(std::move(log))
    {}

    void Start()
    {
        ISimpleThread::Start();
    }

    void SignalInterrupt()
    {
        if (InterruptSignaled) {
            return;
        }

        if (auto threadId = AtomicGet(ThreadId)) {
            // session loop may get stuck on sem_wait/read.
            // Interrupt it by sending the thread a signal.
            pthread_kill(threadId, SIGUSR1);
        }

        InterruptSignaled = true;
    }

    void StopThread()
    {
        STORAGE_INFO("stopping FUSE loop " << FuseLoopIndex << "." << QueueIndex);

        SignalInterrupt();
        Join();

        STORAGE_INFO("stopped FUSE loop "  << FuseLoopIndex << "." << QueueIndex);
    }

private:
    void* ThreadProc() override
    {
        ::NCloud::SetCurrentThreadName(
            "FUSE" + ToString(FuseLoopIndex) + "." + ToString(QueueIndex),
            4);

        AtomicSet(ThreadId, pthread_self());
        fuse_session_loop(Session, QueueIndex);

        return nullptr;
    }
};

class TFuseLoop final
{
private:
    TLog Log;
    TSession Session;

    TVector<std::unique_ptr<TFuseLoopThread>> QueueThreads;

public:
    TFuseLoop(
            TLog log,
            const TVFSConfig& config,
            ui32 threadsCount,
            const fuse_lowlevel_ops& ops,
            const TString& state,
            void* context)
        : Log(std::move(log))
        , Session(config, threadsCount, ops, state, context)
        , QueueThreads(threadsCount)
    {
        static std::atomic<ui64> NexFuseLoopIndex = 0;
        ui64 fuseLoopIndex = NexFuseLoopIndex++;

        for (ui32 queueIndex = 0; queueIndex < QueueThreads.size(); queueIndex++) {
            QueueThreads[queueIndex] = std::make_unique<TFuseLoopThread>(
                Session,
                fuseLoopIndex,
                queueIndex,
                Log);
        }
    }

    void Start()
    {
        for (auto& thread: QueueThreads) {
            thread->Start();
        }
    }

    void Stop()
    {
        STORAGE_INFO("stopping FUSE loop");

        Session.Exit();

        for (auto& thread: QueueThreads) {
            thread->SignalInterrupt();
        }

        for (auto& thread: QueueThreads) {
            thread->StopThread();
        }

        STORAGE_INFO("stopped FUSE loop");
    }

    void Suspend()
    {
        Session.Suspend();
    }

    void Unmount()
    {
        Stop();

        STORAGE_INFO("unmounting FUSE session");
        Session.Unmount();
    }

    const TSession& GetSession() const
    {
        return Session;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileSystemLoop final
    : public IFileSystemLoop
    , public std::enable_shared_from_this<TFileSystemLoop>
{
private:
    const TVFSConfigPtr Config;
    const ILoggingServicePtr Logging;
    const IRequestStatsRegistryPtr StatsRegistry;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const IProfileLogPtr ProfileLog;
    const ISessionPtr Session;

    TLog Log;

    TString SessionState;
    TString SessionId;
    std::unique_ptr<TFuseLoop> FuseLoop;
    NProto::EStorageMediaKind StorageMediaKind = NProto::STORAGE_MEDIA_DEFAULT;

    std::shared_ptr<TCompletionQueue> CompletionQueue;
    IRequestStatsPtr RequestStats;
    IFileSystemPtr FileSystem;
    TFileSystemConfigPtr FileSystemConfig;

    THolder<TFileLock> HandleOpsQueueFileLock;
    THolder<TFileLock> WriteBackCacheFileLock;

    bool HandleOpsQueueInitialized = false;
    bool WriteBackCacheInitialized = false;

public:
    TFileSystemLoop(
            TVFSConfigPtr config,
            ILoggingServicePtr logging,
            IRequestStatsRegistryPtr statsRegistry,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            IProfileLogPtr profileLog,
            ISessionPtr session)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , StatsRegistry(std::move(statsRegistry))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , ProfileLog(std::move(profileLog))
        , Session(std::move(session))
    {
        Log = Logging->CreateLog("NFS_FUSE");
    }

    TFuture<NProto::TError> StartAsync() override
    {
        RequestStats = StatsRegistry->GetFileSystemStats(
            Config->GetFileSystemId(),
            Config->GetClientId(),
            "", // folderId, empty until the session is created
            ""); // cloudId

        auto callContext = MakeIntrusive<TCallContext>(
            Config->GetFileSystemId(),
            CreateRequestId());
        callContext->RequestType = EFileStoreRequest::CreateSession;
        RequestStats->RequestStarted(Log, *callContext);

        auto weakPtr = weak_from_this();
        return Session->CreateSession(
            Config->GetReadOnly(),
            Config->GetMountSeqNumber()).Apply([=] (const auto& future) {
                if (auto p = weakPtr.lock()) {
                    const auto& response = future.GetValue();
                    p->RequestStats->RequestCompleted(
                        p->Log,
                        *callContext,
                        response.GetError());
                    // After the session is established, we update existing
                    // TFileStats with newly known cloudId and folderId
                    p->StatsRegistry->UpdateCloudAndFolder(
                        p->Config->GetFileSystemId(),
                        p->Config->GetClientId(),
                        response.GetFileStore().GetCloudId(),
                        response.GetFileStore().GetFolderId());

                    return p->StartWithSessionState(future);
                }
                return MakeError(E_INVALID_STATE, "Driver is destroyed");
            });
    }

    TFuture<void> StopAsync() override
    {
        if (!FuseLoop) {
            return MakeFuture();
        }

        auto w = weak_from_this();
        auto s = NewPromise<void>();

        auto onStop = [w = std::move(w), s] (const TFuture<void>& f) mutable {
            f.GetValue();

            auto p = w.lock();
            if (!p) {
                return;
            }

            p->FuseLoop->Unmount();
            p->FuseLoop = nullptr;

            auto callContext = MakeIntrusive<TCallContext>(
                p->Config->GetFileSystemId(),
                CreateRequestId());
            callContext->RequestType = EFileStoreRequest::DestroySession;
            p->RequestStats->RequestStarted(p->Log, *callContext);

            p->Session->DestroySession()
                .Subscribe([
                    w = std::move(w),
                    s = std::move(s),
                    callContext = std::move(callContext)
                ] (const auto& f) mutable {
                    auto p = w.lock();
                    if (!p) {
                        s.SetValue();
                        return;
                    }

                    const auto& response = f.GetValue();
                    p->RequestStats->RequestCompleted(
                        p->Log,
                        *callContext,
                        response.GetError());

                    p->StatsRegistry->Unregister(
                        p->Config->GetFileSystemId(),
                        p->Config->GetClientId());

                    // We need to cleanup HandleOpsQueue file and directories
                    if (p->HandleOpsQueueInitialized) {
                        auto error = UnlockAndDeleteFile(
                            TFsPath(p->Config->GetHandleOpsQueuePath()) /
                                p->Config->GetFileSystemId() / p->SessionId,
                            p->HandleOpsQueueFileLock);
                        if (HasError(error)) {
                            ReportHandleOpsQueueCreatingOrDeletingError(
                                error.GetMessage());
                        }
                    }

                    // We need to cleanup WriteBackCache file and directories
                    if (p->WriteBackCacheInitialized) {
                        auto error = UnlockAndDeleteFile(
                            TFsPath(p->Config->GetWriteBackCachePath()) /
                                p->Config->GetFileSystemId() / p->SessionId,
                            p->WriteBackCacheFileLock);
                        if (HasError(error)) {
                            ReportWriteBackCacheCreatingOrDeletingError(
                                error.GetMessage());
                        }
                    }

                    s.SetValue();
                });
        };

        CompletionQueue->StopAsync(FUSE_ERROR).Subscribe(
            [onStop = std::move(onStop)] (TFuture<void> f) mutable {
                // this callback may be called from the same thread where the
                // returned future is set => we shouldn't call onStop inside
                // this callback directly to avoid a deadlock caused by the
                // Join call which is done by SessionThread->Unmount()
                SystemThreadFactory()->Run(
                    [onStop = std::move(onStop), f = std::move(f)] () mutable {
                        onStop(f);
                    });
            });

        return s;
    }

    TFuture<void> SuspendAsync() override
    {
        if (!FuseLoop) {
            return MakeFuture();
        }

        auto w = weak_from_this();
        auto s = NewPromise<void>();
        auto onStop = [w = std::move(w), s] (const auto& f) mutable {
            f.GetValue();

            auto p = w.lock();
            if (!p) {
                s.SetValue();
                return;
            }

            // just stop loop, leave connection
            p->FuseLoop->Stop();
            p->FuseLoop->Suspend();
            p->FuseLoop = nullptr;

            p->StatsRegistry->Unregister(
                p->Config->GetFileSystemId(),
                p->Config->GetClientId());

            s.SetValue();
        };

        CompletionQueue->StopAsync(FUSE_SUSPEND).Subscribe(
            [onStop = std::move(onStop)] (TFuture<void> f) mutable {
                // this callback may be called from the same thread where the
                // returned future is set => we shouldn't call onStop inside
                // this callback directly to avoid a deadlock caused by the
                // Join call which is done by SessionThread->StopThread()
                SystemThreadFactory()->Run(
                    [onStop = std::move(onStop), f = std::move(f)] () mutable {
                        onStop(f);
                    });
            });

        return s;
    }

    TFuture<NProto::TError> AlterAsync(
        bool isReadonly,
        ui64 mountSeqNumber) override
    {
        auto callContext = MakeIntrusive<TCallContext>(
            Config->GetFileSystemId(),
            CreateRequestId());
        callContext->RequestType = EFileStoreRequest::CreateSession;
        RequestStats->RequestStarted(Log, *callContext);

        auto weakPtr = weak_from_this();
        return Session->AlterSession(isReadonly, mountSeqNumber)
            .Apply([=] (const auto& future) {
                if (auto p = weakPtr.lock()) {
                    NProto::TError error;
                    try {
                        const auto& response = future.GetValue();
                        p->RequestStats->RequestCompleted(
                            p->Log,
                            *callContext,
                            response.GetError());
                        if (HasError(response)) {
                            STORAGE_ERROR("[f:%s][c:%s] failed to create session: %s",
                                p->Config->GetFileSystemId().Quote().c_str(),
                                p->Config->GetClientId().Quote().c_str(),
                                FormatError(response.GetError()).c_str());

                            return response.GetError();
                        }
                    } catch (const TServiceError& e) {
                        error = MakeError(e.GetCode(), TString(e.GetMessage()));

                        STORAGE_ERROR("[f:%s][c:%s] failed to alter: %s",
                            p->Config->GetFileSystemId().Quote().c_str(),
                            p->Config->GetClientId().Quote().c_str(),
                            FormatError(error).c_str());
                    } catch (...) {
                        error = MakeError(E_FAIL, CurrentExceptionMessage());
                        STORAGE_ERROR("[f:%s][c:%s] failed to alter: %s",
                            p->Config->GetFileSystemId().Quote().c_str(),
                            p->Config->GetClientId().Quote().c_str(),
                            FormatError(error).c_str());
                    }
                    return error;
                }
                return MakeError(E_INVALID_STATE, "Driver is destroyed");
            });
    }

private:
    NProto::TError StartWithSessionState(
        const TFuture<NProto::TCreateSessionResponse>& future)
    {
        NProto::TError error;

        try {
            auto response = future.GetValue();
            if (HasError(response)) {
                STORAGE_ERROR("[f:%s][c:%s] failed to create session: %s",
                    Config->GetFileSystemId().Quote().c_str(),
                    Config->GetClientId().Quote().c_str(),
                    FormatError(response.GetError()).c_str());

                return response.GetError();
            }

            const auto& filestore = response.GetFileStore();
            ui64 rawMedia = filestore.GetStorageMediaKind();
            if (!NProto::EStorageMediaKind_IsValid(rawMedia)) {
                STORAGE_WARN("[f:%s][c:%s] got unsupported media kind %lu",
                    Config->GetFileSystemId().Quote().c_str(),
                    Config->GetClientId().Quote().c_str(),
                    rawMedia);
            }

            switch (rawMedia) {
                case NProto::STORAGE_MEDIA_SSD:
                    StorageMediaKind = NProto::STORAGE_MEDIA_SSD;
                    break;
                default:
                    StorageMediaKind = NProto::STORAGE_MEDIA_HDD;
                    break;
            }
            StatsRegistry->SetFileSystemMediaKind(
                Config->GetFileSystemId(),
                Config->GetClientId(),
                StorageMediaKind);
            StatsRegistry->RegisterUserStats(
                Config->GetFileSystemId(),
                Config->GetClientId(),
                filestore.GetCloudId(),
                filestore.GetFolderId());

            CompletionQueue = std::make_shared<TCompletionQueue>(
                Config->GetFileSystemId(),
                RequestStats,
                Log,
                StorageMediaKind);
            FileSystemConfig = MakeFileSystemConfig(filestore);

            SessionId = response.GetSession().GetSessionId();

            THandleOpsQueuePtr handleOpsQueue;
            if (FileSystemConfig->GetAsyncDestroyHandleEnabled()) {
                if (Config->GetHandleOpsQueuePath()) {
                    auto path = TFsPath(Config->GetHandleOpsQueuePath()) /
                        FileSystemConfig->GetFileSystemId() /
                        SessionId;

                    auto error = CreateAndLockFile(
                        path,
                        HandleOpsQueueFileName,
                        HandleOpsQueueFileLock);

                    if (HasError(error)) {
                        ReportHandleOpsQueueCreatingOrDeletingError(
                            error.GetMessage());
                        return error;
                    }

                    handleOpsQueue = CreateHandleOpsQueue(
                        path / HandleOpsQueueFileName,
                        Config->GetHandleOpsQueueSize());
                    HandleOpsQueueInitialized = true;
                } else {
                    TString msg = "Error initializing HandleOpsQueue: "
                        "HandleOpsQueuePath is not set";
                    STORAGE_WARN("[f:%s][c:%s] %s",
                        Config->GetFileSystemId().Quote().c_str(),
                        Config->GetClientId().Quote().c_str(),
                        msg.c_str()
                    );
                    ReportHandleOpsQueueCreatingOrDeletingError(msg);
                }
            }

            TWriteBackCache writeBackCache;
            if (FileSystemConfig->GetServerWriteBackCacheEnabled()) {
                if (Config->GetWriteBackCachePath()) {
                    auto path = TFsPath(Config->GetWriteBackCachePath()) /
                        FileSystemConfig->GetFileSystemId() /
                        SessionId;

                    auto error = CreateAndLockFile(
                        path,
                        WriteBackCacheFileName,
                        WriteBackCacheFileLock);

                    if (HasError(error)) {
                        ReportWriteBackCacheCreatingOrDeletingError(
                            error.GetMessage());
                        return error;
                    }

                    writeBackCache = TWriteBackCache(
                        Session,
                        Scheduler,
                        Timer,
                        path / WriteBackCacheFileName,
                        Config->GetWriteBackCacheCapacity(),
                        Config->GetWriteBackCacheAutomaticFlushPeriod());
                    WriteBackCacheInitialized = true;
                } else {
                    TString msg =
                        "Error initializing WriteBackCache: "
                        "WriteBackCachePath is not set";
                    STORAGE_WARN(
                        "[f:%s][c:%s] %s",
                        Config->GetFileSystemId().Quote().c_str(),
                        Config->GetClientId().Quote().c_str(),
                        msg.c_str());
                    ReportWriteBackCacheCreatingOrDeletingError(msg);
                }
            }

            FileSystem = CreateFileSystem(
                Logging,
                ProfileLog,
                Scheduler,
                Timer,
                FileSystemConfig,
                Session,
                RequestStats,
                CompletionQueue,
                std::move(handleOpsQueue),
                std::move(writeBackCache));

            RequestStats->RegisterIncompleteRequestProvider(CompletionQueue);

            SessionState = response.GetSession().GetSessionState();
            fuse_lowlevel_ops ops = {};
            InitOps(ops);

            STORAGE_INFO("[f:%s][c:%s] starting %s session",
                Config->GetFileSystemId().Quote().c_str(),
                Config->GetClientId().Quote().c_str(),
                SessionState.empty() ? "new" : "existing");

            TStringStream filestoreConfigDump;
            FileSystemConfig->Dump(filestoreConfigDump);
            STORAGE_INFO(
                "[f:%s][c:%s] new session filestore config: %s",
                Config->GetFileSystemId().Quote().c_str(),
                Config->GetClientId().Quote().c_str(),
                filestoreConfigDump.Str().Quote().c_str());

            ui32 fuseLoopThreadCount = FileSystemConfig->GetMaxFuseLoopThreads();
            if (Config->GetVhostQueuesCount()) {
                fuseLoopThreadCount = Min(fuseLoopThreadCount, Config->GetVhostQueuesCount());
            }

            FuseLoop = std::make_unique<TFuseLoop>(
                Log,
                *Config,
                fuseLoopThreadCount,
                ops,
                SessionState,
                this);

            FuseLoop->Start();
        } catch (const TServiceError& e) {
            error = MakeError(e.GetCode(), TString(e.GetMessage()));

            STORAGE_ERROR("[f:%s][c:%s] failed to start: %s",
                Config->GetFileSystemId().Quote().c_str(),
                Config->GetClientId().Quote().c_str(),
                FormatError(error).c_str());
        } catch (...) {
            error = MakeError(E_FAIL, CurrentExceptionMessage());
            STORAGE_ERROR("[f:%s][c:%s] failed to start: %s",
                Config->GetFileSystemId().Quote().c_str(),
                Config->GetClientId().Quote().c_str(),
                FormatError(error).c_str());
        }

        return error;
    }

    TFileSystemConfigPtr MakeFileSystemConfig(const NProto::TFileStore& filestore)
    {
        NProto::TFileSystemConfig config;
        config.SetFileSystemId(filestore.GetFileSystemId());
        config.SetBlockSize(filestore.GetBlockSize());

        if (auto pages = Config->GetMaxWritePages()) {
            config.SetMaxBufferSize(pages * NSystemInfo::GetPageSize());
        }

        const auto& features = filestore.GetFeatures();
        if (features.GetPreferredBlockSize()) {
            config.SetPreferredBlockSize(features.GetPreferredBlockSize());
        } else {
            config.SetPreferredBlockSize(filestore.GetBlockSize());
        }
        if (features.GetEntryTimeout()) {
            config.SetEntryTimeout(features.GetEntryTimeout());
        }
        if (features.GetNegativeEntryTimeout()) {
            config.SetNegativeEntryTimeout(features.GetNegativeEntryTimeout());
        }
        if (features.GetAttrTimeout()) {
            config.SetAttrTimeout(features.GetAttrTimeout());
        }
        config.SetAsyncDestroyHandleEnabled(
            features.GetAsyncDestroyHandleEnabled());
        config.SetAsyncHandleOperationPeriod(
            features.GetAsyncHandleOperationPeriod());

        config.SetDirectIoEnabled(features.GetDirectIoEnabled());
        config.SetDirectIoAlign(features.GetDirectIoAlign());

        config.SetGuestWriteBackCacheEnabled(
            features.GetGuestWriteBackCacheEnabled());

        config.SetServerWriteBackCacheEnabled(
            features.GetServerWriteBackCacheEnabled());

        config.SetZeroCopyEnabled(features.GetZeroCopyEnabled());

        config.SetGuestPageCacheDisabled(features.GetGuestPageCacheDisabled());
        config.SetExtendedAttributesDisabled(
            features.GetExtendedAttributesDisabled());

        config.SetGuestKeepCacheAllowed(features.GetGuestKeepCacheAllowed());
        config.SetMaxBackground(features.GetMaxBackground());
        config.SetMaxFuseLoopThreads(features.GetMaxFuseLoopThreads());

        return std::make_shared<TFileSystemConfig>(config);
    }

    void Init(fuse_conn_info* conn)
    {
        // Cap the number of scatter-gather segments per request to virtqueue
        // size, taking FUSE own headers into account (see FUSE_HEADER_OVERHEAD
        // in linux:fs/fuse/virtio_fs.c)
        const size_t maxPages = Config->GetMaxWritePages();

        // libfuse doesn't allow to configure max_pages directly but infers it
        // from fuse_conn_info.max_write, dividing it by page size.
        // see CLOUD-75329 for details
        const size_t maxWrite = NSystemInfo::GetPageSize() * maxPages;
        if (maxWrite < conn->max_write) {
            STORAGE_DEBUG("[f:%s][c:%s] setting max write pages %u -> %lu",
                Config->GetFileSystemId().Quote().c_str(),
                Config->GetClientId().Quote().c_str(),
                conn->max_write,
                maxWrite);
            conn->max_write = maxWrite;
        }

        // Max async tasks allowed by fuse. Default FUSE limit is 12.
        // Hard limit on the kernel side is around 64k.
        conn->max_background = FileSystemConfig->GetMaxBackground()
                                   ? FileSystemConfig->GetMaxBackground()
                                   : Config->GetMaxBackground();

        // in case of newly mount we should drop any prev state
        // e.g. left from a crash or smth, paranoid mode
        ResetSessionState(FuseLoop->GetSession().Dump());

        if (FileSystemConfig->GetGuestWriteBackCacheEnabled()) {
            conn->want |= FUSE_CAP_WRITEBACK_CACHE;
        }

        FileSystem->Init();
    }

    void Destroy()
    {
        STORAGE_INFO("[f:%s][c:%s] got destroy request",
            Config->GetFileSystemId().Quote().c_str(),
            Config->GetClientId().Quote().c_str());
        // in case of unmount we should cleanup everything
        ResetSessionState({});
    }

    void ResetSessionState(const TString& state)
    {
        STORAGE_INFO("[f:%s][c:%s][l:%lu] resetting session state",
            Config->GetFileSystemId().Quote().c_str(),
            Config->GetClientId().Quote().c_str(),
            state.size());

        SessionState = state;

        auto callContext = MakeIntrusive<TCallContext>(
            Config->GetFileSystemId(),
            CreateRequestId());
        callContext->RequestType = EFileStoreRequest::ResetSession;
        auto request = std::make_shared<NProto::TResetSessionRequest>();
        request->SetSessionState(state);

        auto response = Session->ResetSession(
            std::move(callContext),
            std::move(request));

        // TODO: CRIT EVENT, though no way to interrupt mount
        auto result = response.GetValueSync();
        STORAGE_INFO("[f:%s][c:%s] session reset completed: %s",
            Config->GetFileSystemId().Quote().c_str(),
            Config->GetClientId().Quote().c_str(),
            FormatError(result.GetError()).c_str());

        FileSystem->Reset();
    }

    template <typename Method, typename... Args>
    static void Call(
        Method&& m,
        const char* name,
        EFileStoreRequest requestType,
        ui32 requestSize,
        fuse_req_t req,
        Args&&... args) noexcept
    {
        auto* pThis = static_cast<TFileSystemLoop*>(fuse_req_userdata(req));
        auto& Log = pThis->Log;

        auto callContext = MakeIntrusive<TCallContext>(
            pThis->Config->GetFileSystemId(),
            fuse_req_unique(req));
        callContext->RequestType = requestType;
        callContext->RequestSize = requestSize;

        if (auto cancelCode = pThis->CompletionQueue->Enqueue(req, callContext)) {
            STORAGE_DEBUG("driver is stopping, cancel request");
            callContext->CancellationCode = *cancelCode;
            CancelRequest(
                pThis->Log,
                *pThis->RequestStats,
                *callContext,
                req);
            return;
        }

        FILESTORE_TRACK(
            RequestReceived,
            callContext,
            name,
            callContext->FileSystemId,
            pThis->StorageMediaKind,
            callContext->RequestSize);
        pThis->RequestStats->RequestStarted(Log, *callContext);

        try {
            auto* fs = pThis->FileSystem.get();
            (fs->*m)(callContext, req, std::forward<Args>(args)...);
        } catch (const TServiceError& e) {
            STORAGE_ERROR("unexpected error: "
                << FormatResultCode(e.GetCode()) << " " << e.GetMessage());
            ReplyError(
                pThis->Log,
                *pThis->RequestStats,
                *callContext,
                MakeError(e.GetCode(), TString(e.GetMessage())),
                req,
                ErrnoFromError(e.GetCode()));
        } catch (...) {
            STORAGE_ERROR("unexpected error: " << CurrentExceptionMessage());
            ReplyError(
                pThis->Log,
                *pThis->RequestStats,
                *callContext,
                MakeError(E_IO, CurrentExceptionMessage()),
                req,
                EIO);
        }
    }

    static void InitOps(fuse_lowlevel_ops& ops)
    {
#define CALL(m, requestType, requestSize, req, ...)                            \
    TFileSystemLoop::Call(                                                     \
        &IFileSystem::m,                                                       \
        #m,                                                                    \
        requestType,                                                           \
        requestSize,                                                           \
        req,                                                                   \
        __VA_ARGS__)                                                           \
// CALL

        //
        // Initialization
        //

        ops.init = [] (void* userdata, fuse_conn_info* conn) {
            static_cast<TFileSystemLoop*>(userdata)->Init(conn);
        };
        ops.destroy = [] (void* userdata) {
            static_cast<TFileSystemLoop*>(userdata)->Destroy();
        };

        //
        // Filesystem information
        //

        ops.statfs = [] (fuse_req_t req, fuse_ino_t ino) {
            CALL(StatFs, EFileStoreRequest::StatFileStore, 0, req, ino);
        };

        //
        // Nodes
        //

        ops.lookup = [] (fuse_req_t req, fuse_ino_t parent, const char* name) {
            CALL(Lookup, EFileStoreRequest::GetNodeAttr, 0, req, parent, name);
        };
        ops.forget = [] (fuse_req_t req, fuse_ino_t ino, unsigned long nlookup) {
            CALL(Forget, EFileStoreRequest::MAX, 0, req, ino, nlookup);
        };
        ops.forget_multi = [] (fuse_req_t req, size_t count, fuse_forget_data* forgets) {
            CALL(ForgetMulti, EFileStoreRequest::MAX, 0, req, count, forgets);
        };
        ops.mkdir = [] (fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode) {
            CALL(MkDir, EFileStoreRequest::CreateNode, 0, req, parent, name, mode);
        };
        ops.rmdir = [] (fuse_req_t req, fuse_ino_t parent, const char* name) {
            CALL(RmDir, EFileStoreRequest::UnlinkNode, 0, req, parent, name);
        };
        ops.mknod = [] (fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode, dev_t rdev) {
            CALL(MkNode, EFileStoreRequest::CreateNode, 0, req, parent, name, mode, rdev);
        };
        ops.unlink = [] (fuse_req_t req, fuse_ino_t parent, const char* name) {
            CALL(Unlink, EFileStoreRequest::UnlinkNode, 0, req, parent, name);
        };
#if defined(FUSE_VIRTIO)
        ops.rename = [] (fuse_req_t req, fuse_ino_t parent, const char* name, fuse_ino_t newparent, const char* newname, uint32_t flags) {
            CALL(Rename, EFileStoreRequest::RenameNode, 0, req, parent, name, newparent, newname, flags);
        };
#else
        ops.rename = [] (fuse_req_t req, fuse_ino_t parent, const char* name, fuse_ino_t newparent, const char* newname) {
            CALL(Rename, EFileStoreRequest::RenameNode, 0, req, parent, name, newparent, newname, 0);
        };
#endif
        ops.symlink = [] (fuse_req_t req, const char* link, fuse_ino_t parent, const char* name) {
            CALL(SymLink, EFileStoreRequest::CreateNode, 0, req, link, parent, name);
        };
        ops.link = [] (fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char* newname) {
            CALL(Link, EFileStoreRequest::CreateNode, 0, req, ino, newparent, newname);
        };
        ops.readlink = [] (fuse_req_t req, fuse_ino_t ino) {
            CALL(ReadLink, EFileStoreRequest::ReadLink, 0, req, ino);
        };

        //
        // Node attributes
        //

        ops.setattr = [] (fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set, fuse_file_info* fi) {
            CALL(SetAttr, EFileStoreRequest::SetNodeAttr, 0, req, ino, attr, to_set, fi);
        };
        ops.getattr = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
            CALL(GetAttr, EFileStoreRequest::GetNodeAttr, 0, req, ino, fi);
        };
        ops.access = [] (fuse_req_t req, fuse_ino_t ino, int mask) {
            CALL(Access, EFileStoreRequest::AccessNode, 0, req, ino, mask);
        };

        //
        // Extended node attributes
        //

        ops.setxattr = [] (fuse_req_t req, fuse_ino_t ino, const char* name, const char* value, size_t size, int flags) {
            CALL(SetXAttr, EFileStoreRequest::SetNodeXAttr, 0, req, ino, name, TString{value, size}, flags);
        };
        ops.getxattr = [] (fuse_req_t req, fuse_ino_t ino, const char* name, size_t size) {
            CALL(GetXAttr, EFileStoreRequest::GetNodeXAttr, 0, req, ino, name, size);
        };
        ops.listxattr = [] (fuse_req_t req, fuse_ino_t ino, size_t size) {
            CALL(ListXAttr, EFileStoreRequest::ListNodeXAttr, 0, req, ino, size);
        };
        ops.removexattr = [] (fuse_req_t req, fuse_ino_t ino, const char* name) {
            CALL(RemoveXAttr, EFileStoreRequest::RemoveNodeXAttr, 0, req, ino, name);
        };

        //
        // Directory listing
        //

        ops.opendir = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
            CALL(OpenDir, EFileStoreRequest::MAX, 0, req, ino, fi);
        };
#if defined(FUSE_VIRTIO)
        ops.readdirplus = [] (fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset, fuse_file_info* fi) {
            CALL(ReadDir, EFileStoreRequest::ListNodes, 0, req, ino, size, offset, fi);
        };
#else
        ops.readdir = [] (fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset, fuse_file_info* fi) {
            CALL(ReadDir, EFileStoreRequest::ListNodes, 0, req, ino, size, offset, fi);
        };
#endif
        ops.releasedir = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
            CALL(ReleaseDir, EFileStoreRequest::MAX, 0, req, ino, fi);
        };

        //
        // Read  write files
        //

        ops.create = [] (fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode, fuse_file_info* fi) {
            CALL(Create, EFileStoreRequest::CreateHandle, 0, req, parent, name, mode, fi);
        };
        ops.open = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
            CALL(Open, EFileStoreRequest::CreateHandle, 0, req, ino, fi);
        };
        ops.read = [] (fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset, fuse_file_info* fi) {
            CALL(Read, EFileStoreRequest::ReadData, size, req, ino, size, offset, fi);
        };
        ops.write = [] (fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size, off_t offset, fuse_file_info* fi) {
            CALL(Write, EFileStoreRequest::WriteData, size, req, ino, TStringBuf{buf, size}, offset, fi);
        };
        ops.write_buf = [] (fuse_req_t req, fuse_ino_t ino, fuse_bufvec* bufv, off_t offset, fuse_file_info* fi) {
            CALL(WriteBuf, EFileStoreRequest::WriteData, fuse_buf_size(bufv), req, ino, bufv, offset, fi);
        };
        ops.fallocate = [] (fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length, fuse_file_info* fi) {
            CALL(FAllocate, EFileStoreRequest::AllocateData, length, req, ino, mode, offset, length, fi);
        };
        ops.flush = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
            CALL(Flush, EFileStoreRequest::MAX, 0, req, ino, fi);
        };
        ops.fsync = [] (fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi) {
            CALL(FSync, EFileStoreRequest::MAX, 0, req, ino, datasync, fi);
        };
        ops.fsyncdir = [] (fuse_req_t req, fuse_ino_t ino, int datasync, fuse_file_info* fi) {
            CALL(FSyncDir, EFileStoreRequest::MAX, 0, req, ino, datasync, fi);
        };
        ops.release = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
            CALL(Release, EFileStoreRequest::DestroyHandle, 0, req, ino, fi);
        };

        //
        // Locking
        //

        ops.getlk = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, struct flock* lock) {
            CALL(GetLock, EFileStoreRequest::TestLock, lock->l_len, req, ino, fi, lock);
        };
        ops.setlk = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, struct flock* lock, int sleep) {
            CALL(SetLock, GetLockRequestType(lock), lock->l_len, req, ino, fi, lock, sleep != 0);
        };
        ops.flock = [] (fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi, int op) {
            CALL(FLock, GetLockRequestType(op), 0, req, ino, fi, op);
        };
#undef CALL
    }

private:
    static EFileStoreRequest GetLockRequestType(int op)
    {
        return (op & LOCK_UN) ? EFileStoreRequest::ReleaseLock : EFileStoreRequest::AcquireLock;
    }

    static EFileStoreRequest GetLockRequestType(struct flock* lock)
    {
        return (lock->l_type == F_UNLCK) ? EFileStoreRequest::ReleaseLock : EFileStoreRequest::AcquireLock;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemLoopFactory
    : public IFileSystemLoopFactory
{
    const ILoggingServicePtr Logging;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IRequestStatsRegistryPtr RequestStats;
    const IProfileLogPtr ProfileLog;

    TFileSystemLoopFactory(
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IRequestStatsRegistryPtr requestStats,
            IProfileLogPtr profileLog)
        : Logging(std::move(logging))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , RequestStats(std::move(requestStats))
        , ProfileLog(std::move(profileLog))
    {}

    IFileSystemLoopPtr Create(
        TVFSConfigPtr config,
        ISessionPtr session) override
    {
        return CreateFuseLoop(
            std::move(config),
            Logging,
            RequestStats,
            Scheduler,
            Timer,
            ProfileLog,
            std::move(session));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileSystemLoopPtr CreateFuseLoop(
    TVFSConfigPtr config,
    ILoggingServicePtr logging,
    IRequestStatsRegistryPtr requestStats,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    IProfileLogPtr profileLog,
    ISessionPtr session)
{
    return std::make_shared<TFileSystemLoop>(
        std::move(config),
        std::move(logging),
        std::move(requestStats),
        std::move(scheduler),
        std::move(timer),
        std::move(profileLog),
        std::move(session));
}

////////////////////////////////////////////////////////////////////////////////

IFileSystemLoopFactoryPtr CreateFuseLoopFactory(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRequestStatsRegistryPtr requestStats,
    IProfileLogPtr profileLog)
{
    struct TInitializer {
        TInitializer(const ILoggingServicePtr& logging)
        {
            InitLog(logging);
        }
    };

    static const TInitializer initializer(logging);

    return std::make_shared<TFileSystemLoopFactory>(
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(requestStats),
        std::move(profileLog));
}

}   // namespace NCloud::NFileStore::NFuse
