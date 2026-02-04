#include "fs.h"

#include "log.h"
#include "loop.h"
#include "node_cache.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/filesystem_counters.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>
#include <cloud/filestore/libs/vfs/protos/session.pb.h>
#include <cloud/filestore/libs/vhost/client.h>
#include <cloud/filestore/libs/vhost/request.h>
#include <cloud/filestore/libs/vhost/server.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_ring_buffer.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/contrib/virtiofsd/fuse.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/atomic/bool.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/random/random.h>

#include <atomic>
#include <fstream>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;

using namespace NCloud::NFileStore::NClient;
using namespace NCloud::NFileStore::NVFS;
using namespace NCloud::NFileStore::NVhost;

using namespace NCloud::NStorage::NUserStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr TDuration ExceptionWaitTimeout = TDuration::Seconds(1);
constexpr ui64 WriteBackCacheCapacity = 1024 * 1024 + 1024;
constexpr TStringBuf MetricsComponent = "fs_ut";

static const TString FileSystemId = "fs1";
static const TString SessionId = CreateGuidAsString();

static const TTempDir TempDir;

TString CreateBuffer(size_t len, char fill = 0)
{
    return TString(len, fill);
}

TString GenerateValidateData(ui32 size, ui32 seed = 0)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

template <class F>
bool WaitForCondition(TDuration timeout, F&& predicate)
{
    TSpinWait sw;
    auto deadline = TInstant::Now() + timeout;
    while (!predicate()) {
        if (TInstant::Now() > deadline) {
            return false;
        }
        sw.Sleep();
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<TFuseVirtioClient> Fuse;

    const ILoggingServicePtr Logging;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const NMonitoring::TDynamicCountersPtr Counters;
    const IFsCountersProviderPtr FsCountersProvider;
    const IRequestStatsRegistryPtr StatsRegistry;
    const IModuleStatsRegistryPtr ModuleStatsRegistry;

    ISessionPtr Session;
    std::shared_ptr<TFileStoreTest> Service;

    IFileSystemLoopPtr Loop;

    TString SocketPath;

    TPromise<void> StopTriggered = NewPromise<void>();

    TString DirectoryHandlesStoragePath;

    TBootstrap(
            ITimerPtr timer = CreateWallClockTimer(),
            ISchedulerPtr scheduler = CreateScheduler(),
            const NProto::TFileStoreFeatures& featuresConfig = {},
            ui32 handleOpsQueueSize = 1000,
            ui32 writeBackCacheAutomaticFlushPeriodMs = 1000,
            ui64 writeBackCacheCapacity = WriteBackCacheCapacity)
        : Logging(CreateLoggingService("console", { TLOG_RESOURCES }))
        , Scheduler{std::move(scheduler)}
        , Timer{std::move(timer)}
        , Counters{MakeIntrusive<NMonitoring::TDynamicCounters>()}
        , FsCountersProvider{CreateFsCountersProvider(
            TString{MetricsComponent},
            Counters)}
        , StatsRegistry{CreateRequestStatsRegistry(
            TString{MetricsComponent},
            std::make_shared<TDiagnosticsConfig>(),
            Counters,
            Timer,
            CreateUserCounterSupplierStub(),
            FsCountersProvider)}
        , ModuleStatsRegistry{CreateModuleStatsRegistry(FsCountersProvider)}
    {
        signal(SIGUSR1, SIG_IGN);   // see fuse/driver for details

        SocketPath = (TFsPath(GetSystemTempDir()) / Sprintf("vhost.socket_%lu", RandomNumber<ui64>())).GetPath();

        InitLog(Logging);
        NVhost::InitLog(Logging);
        NVhost::StartServer();

        Fuse = std::make_shared<TFuseVirtioClient>(SocketPath, WaitTimeout);

        Service = std::make_shared<TFileStoreTest>();
        Service->CreateSessionHandler =
            [featuresConfig](auto callContext, auto request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->GetRestoreClientSession());
            NProto::TCreateSessionResponse result;
            result.MutableSession()->SetSessionId(SessionId);
            result.MutableFileStore()->SetBlockSize(4096);
            result.MutableFileStore()->MutableFeatures()->CopyFrom(
                featuresConfig);
            result.MutableFileStore()->SetFileSystemId(FileSystemId);
            return MakeFuture(result);
        };

        Service->ResetSessionHandler = [] (auto, auto) {
            return MakeFuture(NProto::TResetSessionResponse{});
        };

        Service->DestroySessionHandler = [] (auto, auto) {
            return MakeFuture(NProto::TDestroySessionResponse());
        };

        Service->PingSessionHandler = [] (auto, auto) {
            return MakeFuture(NProto::TPingSessionResponse());
        };

        auto sessionConfig = std::make_shared<TSessionConfig>(NProto::TSessionConfig{});
        Session = CreateSession(
            Logging,
            Timer,
            Scheduler,
            Service,
            std::move(sessionConfig));

        NProto::TVFSConfig proto;
        proto.SetDebug(true);
        proto.SetSocketPath(SocketPath);
        proto.SetFileSystemId(FileSystemId);
        if (featuresConfig.GetAsyncDestroyHandleEnabled()) {
            proto.SetHandleOpsQueuePath(TempDir.Path() / "HandleOpsQueue");
            proto.SetHandleOpsQueueSize(handleOpsQueueSize);
        }

        if (featuresConfig.GetDirectoryHandlesStorageEnabled()) {
            proto.SetDirectoryHandlesStoragePath(TempDir.Path() / "DirectoryHandles");
            DirectoryHandlesStoragePath = proto.GetDirectoryHandlesStoragePath();
        }

        // WriteBackCache should be configured even if it is disabled
        proto.SetWriteBackCachePath(TempDir.Path() / "WriteBackCache");
        // minimum possible capacity
        proto.SetWriteBackCacheCapacity(writeBackCacheCapacity);
        proto.SetWriteBackCacheAutomaticFlushPeriod(
            writeBackCacheAutomaticFlushPeriodMs);

        auto config = std::make_shared<TVFSConfig>(std::move(proto));
        Loop = NFuse::CreateFuseLoop(
            config,
            Logging,
            StatsRegistry,
            ModuleStatsRegistry,
            FsCountersProvider,
            Scheduler,
            Timer,
            CreateProfileLogStub(),
            Session);
    }

    NMonitoring::TDynamicCountersPtr GetDirectoryHandlesCounters() const
    {
        return Counters
            ->FindSubgroup("component", TString{MetricsComponent} + "_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", FileSystemId)
            ->FindSubgroup("client", "")
            ->FindSubgroup("cloud", "")
            ->FindSubgroup("folder", "")
            ->FindSubgroup("module", "DirectoryHandles");
    }

    ~TBootstrap()
    {
        Stop();
    }

    NProto::TError Start(bool sendInitRequest=true)
    {
        auto response = StartAsync();
        UNIT_ASSERT_NO_EXCEPTION(response.Wait(WaitTimeout));
        auto error = response.GetValueSync();

        if (HasError(error)) {
            return error;
        }

        if (sendInitRequest) {
            auto init = Fuse->SendRequest<TInitRequest>();
            UNIT_ASSERT_NO_EXCEPTION(init.GetValueSync());
        }
        return error;
    }

    TFuture<NProto::TError> StartAsync()
    {
        if (Scheduler) {
            Scheduler->Start();
        }

        auto future = Loop->StartAsync();
        return future.Apply(
            [=](const auto& f)
            {
                const auto& error = f.GetValue();
                if (HasError(error)) {
                    return MakeFuture<NProto::TError>(error);
                }

                Fuse->Init();
                return MakeFuture<NProto::TError>();
            });
    }

    void Stop()
    {
        auto stop = StopAsync();
        StopTriggered.TrySetValue();
        stop.Wait();
        Fuse->DeInit();
        Loop = nullptr;
        std::remove(SocketPath.c_str());
    }

    TFuture<void> StopAsync()
    {
        auto f = MakeFuture();
        if (Loop) {
            f = Loop->StopAsync();
        }

        if (!Scheduler) {
            return f;
        }

        auto p = NewPromise<void>();
        f.Subscribe([=] (auto f) mutable {
            f.GetValue();
            Scheduler->Stop();
            p.SetValue();
        });
        return p;
    }

    void InterruptNextRequest()
    {
        auto interrupt = Fuse->SendRequest<TInterruptRequest>(Fuse->GetLastRequestId() + 2);
        UNIT_ASSERT_NO_EXCEPTION(interrupt.GetValueSync());
    };

    static TBootstrap CreateWithHandlesStorage() {
        NProto::TFileStoreFeatures features;
        features.SetDirectoryHandlesStorageEnabled(true);
        return TBootstrap(CreateWallClockTimer(), CreateScheduler(), features);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileSystemTest)
{
    Y_UNIT_TEST(ShouldHandleInitRequest)
    {
        TBootstrap bootstrap;
        bootstrap.Start(false);

        auto init = bootstrap.Fuse->SendRequest<TInitRequest>();
        UNIT_ASSERT_NO_EXCEPTION(init.GetValue(WaitTimeout));
    }

    void CheckWriteRequestWithFSyncQueue(bool isFSyncQueueDisabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetFSyncQueueDisabled(isFSyncQueueDisabled);

        TBootstrap bootstrap(CreateWallClockTimer(), CreateScheduler(), features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result;
            result.SetHandle(handleId);
            result.MutableNodeAttr()->SetId(nodeId);
            return MakeFuture(result);
        };

        bootstrap.Service->WriteDataHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        std::atomic<int> fsyncCalledWithDataSync = 0;
        std::atomic<int> fsyncCalledWithoutDataSync = 0;

        bootstrap.Service->FsyncHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            if (request->GetDataSync()) {
                fsyncCalledWithDataSync++;
            } else {
                fsyncCalledWithoutDataSync++;
            }

            return MakeFuture(NProto::TFsyncResponse());
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto handle = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_VALUES_EQUAL(handle.GetValue(WaitTimeout), handleId);

        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(
            nodeId, handleId, 0, CreateBuffer(4096, 'a'));
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));

        auto fsync = bootstrap.Fuse->SendRequest<TFsyncRequest>(
            nodeId, handleId, false /* no data sync */);
        UNIT_ASSERT_NO_EXCEPTION(fsync.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, fsyncCalledWithoutDataSync.load());
        UNIT_ASSERT_VALUES_EQUAL(0, fsyncCalledWithDataSync.load());

        fsync = bootstrap.Fuse->SendRequest<TFsyncRequest>(
            nodeId, handleId, true /* data sync */);
        UNIT_ASSERT_NO_EXCEPTION(fsync.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, fsyncCalledWithoutDataSync.load());
        UNIT_ASSERT_VALUES_EQUAL(1, fsyncCalledWithDataSync.load());
    }

    Y_UNIT_TEST(ShouldHandleWriteRequestWithFSyncQueue)
    {
        CheckWriteRequestWithFSyncQueue(false);
    }

    Y_UNIT_TEST(ShouldHandleWriteRequestWithoutFSyncQueue)
    {
        CheckWriteRequestWithFSyncQueue(true);
    }

    void CheckCreateOpenHandleRequest(bool isCreate, bool isWriteBackCacheEnabled)
    {
        NProto::TFileStoreFeatures features;
        if (isWriteBackCacheEnabled) {
            features.SetGuestWriteBackCacheEnabled(true);
        }

        auto timer = CreateWallClockTimer();
        TBootstrap bootstrap(
            timer,
            CreateScheduler(timer),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        bootstrap.Service->CreateHandleHandler =
            [&](auto callContext, auto request)
        {
            if (isWriteBackCacheEnabled) {
                UNIT_ASSERT(HasFlag(
                    request->GetFlags(),
                    NProto::TCreateHandleRequest::E_READ));
            } else {
                UNIT_ASSERT(!HasFlag(
                    request->GetFlags(),
                    NProto::TCreateHandleRequest::E_READ));
            }
            UNIT_ASSERT(HasFlag(
                request->GetFlags(),
                NProto::TCreateHandleRequest::E_WRITE));
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result;
            result.SetHandle(handleId);
            result.MutableNodeAttr()->SetId(nodeId);
            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto req = std::make_shared<TCreateHandleRequest>("/file1", RootNodeId);
        req->In->Body.flags |= O_WRONLY;
        auto handle = bootstrap.Fuse->SendRequest<TCreateHandleRequest>(req);
        UNIT_ASSERT_VALUES_EQUAL(handle.GetValue(WaitTimeout), handleId);

        if (!isCreate) {
            auto req = std::make_shared<TOpenHandleRequest>(handleId);
            req->In->Body.flags |= O_WRONLY;
            auto handle = bootstrap.Fuse->SendRequest<TOpenHandleRequest>(req);
            UNIT_ASSERT_VALUES_EQUAL(handle.GetValue(WaitTimeout), handleId);
        }
    }

    Y_UNIT_TEST(ShouldHandleCreateHandleRequestWithGuestWriteBackCacheEnabled)
    {
        CheckCreateOpenHandleRequest(
            true,   // Create Handle
            false   // no guest writeback cache
        );
        CheckCreateOpenHandleRequest(
            true,   // Create Handle
            true    // guest writeback cache
        );
    }

    Y_UNIT_TEST(ShouldHandleOpenHandleRequestWithGuestWriteBackCacheEnabled)
    {
        CheckCreateOpenHandleRequest(
            false,   // Open Handle
            false    // no guest writeback cache
        );
        CheckCreateOpenHandleRequest(
            false,   // Open Handle
            true     // guest writeback cache
        );
    }

    Y_UNIT_TEST(ShouldPassSessionId)
    {
        TBootstrap bootstrap;

        const TString sessionId = CreateGuidAsString();
        bootstrap.Service->CreateSessionHandler = [&] (auto, auto) {
            NProto::TCreateSessionResponse result;
            result.MutableSession()->SetSessionId(sessionId);
            return MakeFuture(result);
        };

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result;
            if (GetSessionId(*request) != sessionId) {
                result = TErrorResponse(E_ARGUMENT, "invalid session id passed");
            } else {
                result.SetHandle(handleId);
                result.MutableNodeAttr()->SetId(nodeId);
            }
            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto handle = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_VALUES_EQUAL(handle.GetValue(WaitTimeout), handleId);
    }

    Y_UNIT_TEST(ShouldRecoverSession)
    {
        TBootstrap bootstrap;

        const TSet<int> expected = {100500, 100501};
        int handle = 100500;

        const TString sessionId = CreateGuidAsString();

        std::atomic_bool recovered = false;
        bootstrap.Service->CreateSessionHandler =
            [sessionId, &recovered](auto callContext, auto request)
        {
            Y_UNUSED(callContext);

            NProto::TCreateSessionResponse result;
            if (auto session = GetSessionId(*request)) {
                if (session != sessionId) {
                    NProto::TCreateSessionResponse result =
                        TErrorResponse(E_ARGUMENT, "invalid session");
                    return MakeFuture(result);
                }

                recovered = true;
            }

            result.MutableSession()->SetSessionId(sessionId);
            return MakeFuture(result);
        };

        bool called = false;
        auto promise = NewPromise<NProto::TCreateHandleResponse>();
        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result;
            if (!called) {
                called = true;
                return promise.GetFuture();
            } else if (GetSessionId(*request) != sessionId) {
                result = TErrorResponse(E_ARGUMENT, "");
            } else {
                result.SetHandle(handle++);
                result.MutableNodeAttr()->SetId(100500);
            }

            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto future1 = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT(!future1.HasValue());

        auto future2 = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file2", RootNodeId);
        UNIT_ASSERT(!future2.HasValue());

        NProto::TCreateHandleResponse result;
        result = TErrorResponse(E_FS_INVALID_SESSION, "invalid session");
        promise.SetValue(result);

        UNIT_ASSERT(IsIn(expected, future1.GetValue(WaitTimeout)));
        UNIT_ASSERT(IsIn(expected, future2.GetValue(WaitTimeout)));
        UNIT_ASSERT(recovered);
    }

    Y_UNIT_TEST(ShouldFailRequestIfFailedToRecoverSession)
    {
        TBootstrap bootstrap;

        const TString sessionId = CreateGuidAsString();
        std::atomic<bool> failSession = false;

        bootstrap.Service->SetHandlerCreateSession([&] (auto, auto) {
            NProto::TCreateSessionResponse result;
            if (failSession) {
                result = TErrorResponse(E_FS_INVALID_SESSION, "invalid session");
            } else {
                result.MutableSession()->SetSessionId(sessionId);
            }
            return MakeFuture(result);
        });

        bootstrap.Service->SetHandlerCreateHandle([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result;
            result = TErrorResponse(E_FS_INVALID_SESSION, "");
            return MakeFuture(result);
        });

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        failSession = true;
        auto future = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_EXCEPTION(future.GetValue(WaitTimeout), yexception);
    }

    Y_UNIT_TEST(ShouldNoFailRequestIfSessionIsAbleToRecover)
    {
        TBootstrap bootstrap;

        const int handle = 100500;
        const TString sessionId1 = CreateGuidAsString();
        const TString sessionId2 = CreateGuidAsString();

        const TString* sessionId = &sessionId1;
        bootstrap.Service->CreateSessionHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(callContext);

            NProto::TCreateSessionResponse result;
            if (auto session = GetSessionId(*request)) {
                if (session != *sessionId) {
                    result = TErrorResponse(E_FS_INVALID_SESSION, "invalid session");
                }
            }

            result.MutableSession()->SetSessionId(*sessionId);
            sessionId = &sessionId2;

            return MakeFuture(result);
        };

        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result;
            if (GetSessionId(*request) != *sessionId) {
                result = TErrorResponse(E_FS_INVALID_SESSION, "");
            } else {
                result.SetHandle(handle);
                result.MutableNodeAttr()->SetId(100500);
            }

            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto future = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(future.GetValue(WaitTimeout), handle);
    }

    Y_UNIT_TEST(ShouldPingSession)
    {
        auto scheduler = std::make_shared<TTestScheduler>();
        TBootstrap bootstrap(CreateWallClockTimer(), scheduler);

        const TString sessionId = CreateGuidAsString();
        bootstrap.Service->CreateSessionHandler = [&] (auto, auto) {
            NProto::TCreateSessionResponse result;
            result.MutableSession()->SetSessionId(sessionId);

            return MakeFuture(result);
        };

        bool called = false;
        bootstrap.Service->PingSessionHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(callContext);

            if (GetSessionId(*request) == sessionId) {
                called = true;
            }

            return MakeFuture(NProto::TPingSessionResponse());
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        scheduler->RunAllScheduledTasks();

        UNIT_ASSERT(called);
    }

    Y_UNIT_TEST(ShouldHandleReadDir)
    {
        TBootstrap bootstrap;
        bootstrap.Service->ListNodesHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TListNodesResponse result;
            result.AddNames()->assign("1.txt");

            auto* node = result.AddNodes();
            node->SetId(10);
            node->SetType(NProto::E_REGULAR_NODE);

            return MakeFuture(result);
        };

        std::atomic<int> fsyncDirCalledWithDataSync = 0;
        std::atomic<int> fsyncDirCalledWithoutDataSync = 0;

        bootstrap.Service->FsyncDirHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            if (request->GetDataSync()) {
                fsyncDirCalledWithDataSync++;
            } else {
                fsyncDirCalledWithoutDataSync++;
            }

            return MakeFuture(NProto::TFsyncDirResponse());
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        const ui64 nodeId = 123;

        auto handle = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
        UNIT_ASSERT(handle.Wait(WaitTimeout));
        auto handleId = handle.GetValue();

        // read dir consists of sequantial reading until empty resposne
        auto read = bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        auto size = read.GetValue();
        UNIT_ASSERT(size > 0);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId, size);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        size = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size, 0);

        auto fsyncdir = bootstrap.Fuse->SendRequest<TFsyncDirRequest>(
            nodeId, handleId, false /* no data sync */);
        UNIT_ASSERT_NO_EXCEPTION(fsyncdir.GetValue(WaitTimeout));
        UNIT_ASSERT_EQUAL(1, fsyncDirCalledWithoutDataSync.load());
        UNIT_ASSERT_EQUAL(0, fsyncDirCalledWithDataSync.load());

        fsyncdir = bootstrap.Fuse->SendRequest<TFsyncDirRequest>(
            nodeId, handleId, true /* data sync */);
        UNIT_ASSERT_NO_EXCEPTION(fsyncdir.GetValue(WaitTimeout));
        UNIT_ASSERT_EQUAL(1, fsyncDirCalledWithoutDataSync.load());
        UNIT_ASSERT_EQUAL(1, fsyncDirCalledWithDataSync.load());

        auto close = bootstrap.Fuse->SendRequest<TReleaseDirRequest>(nodeId, handleId);
        UNIT_ASSERT_NO_EXCEPTION(close.GetValue(WaitTimeout));
    }

    Y_UNIT_TEST(ShouldHandleReadDirInvalidHandles)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        const ui64 nodeId = 123;

        auto read = bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, 100500);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT(read.HasException());

        auto close = bootstrap.Fuse->SendRequest<TReleaseDirRequest>(nodeId, 100500);
        UNIT_ASSERT_NO_EXCEPTION(close.GetValue(WaitTimeout));
    }

    Y_UNIT_TEST(ShouldHandleReadDirPaging)
    {
        TBootstrap bootstrap;

        std::atomic<ui32> numCalls = 0;
        bootstrap.Service->ListNodesHandler =
            [&](auto callContext, auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            static ui64 id = 1;

            NProto::TListNodesResponse result;
            result.AddNames()->assign(ToString(id) + ".txt");

            auto* node = result.AddNodes();
            node->SetId(id++);
            node->SetType(NProto::E_REGULAR_NODE);

            if (!numCalls) {
                result.SetCookie("cookie");
            } else {
                Y_ABORT_UNLESS(request->GetCookie());
            }

            ++numCalls;
            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        const ui64 nodeId = 123;

        auto handle = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
        UNIT_ASSERT(handle.Wait(WaitTimeout));
        auto handleId = handle.GetValue();

        // read dir consists of sequantial reading until empty resposne
        auto read =
            bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 1);

        auto size1 = read.GetValue();
        UNIT_ASSERT(size1 > 0);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            size1 / 2);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 1);

        auto size2 = read.GetValue();
        UNIT_ASSERT(size2 > 0);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            size1);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 2);

        auto size3 = read.GetValue();
        UNIT_ASSERT(size3 > 0);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            size1 + size2);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 2);

        auto size4 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size4, 0);

        auto close =
            bootstrap.Fuse->SendRequest<TReleaseDirRequest>(nodeId, handleId);
        UNIT_ASSERT_NO_EXCEPTION(close.GetValue(WaitTimeout));
    }

    Y_UNIT_TEST(ShouldHandleReadDirLargeDataWithHandlesStoragePaging)
    {
        auto bootstrap = TBootstrap::CreateWithHandlesStorage();

        std::atomic<ui32> numCalls = 0;
        bootstrap.Service->ListNodesHandler =
            [&](auto callContext, auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            ++numCalls;

            NProto::TListNodesResponse result;

            if (!request->GetCookie()) {
                for (ui32 i = 1; i <= 20000; ++i) {
                    result.AddNames()->assign(
                        "first_chunk_file_" + ToString(i) + ".txt");
                    auto* node = result.AddNodes();
                    node->SetId(100 + i);
                    node->SetType(NProto::E_REGULAR_NODE);
                }
                result.SetCookie("has_more_data");
            } else {
                UNIT_ASSERT_VALUES_EQUAL("has_more_data", request->GetCookie());
                for (ui32 i = 20001; i <= 25100; ++i) {
                    result.AddNames()->assign(
                        "second_chunk_file_" + ToString(i) + ".txt");
                    auto* node = result.AddNodes();
                    node->SetId(100 + i);
                    node->SetType(NProto::E_REGULAR_NODE);
                }
            }

            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        const ui64 nodeId = 123;

        auto handle = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
        UNIT_ASSERT(handle.Wait(WaitTimeout));
        auto handleId = handle.GetValue();

        auto read =
            bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 1);

        auto size1 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size1, 4096);

        read =
            bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId, 0);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 2);

        auto size2 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size2, 4096);

        auto partialOffset = size1 / 2;
        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            partialOffset);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 2);

        auto size3 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size3, 4096);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            size1);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 2);

        auto size4 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size4, 4096);

        read =
            bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId, 0);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 3);

        auto size5 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size5, 4096);

        bootstrap.ModuleStatsRegistry->UpdateStats(true);

        auto moduleCounters = bootstrap.GetDirectoryHandlesCounters();

        auto maxCacheSize = moduleCounters->FindCounter("MaxCacheSize");
        UNIT_ASSERT(maxCacheSize);
        UNIT_ASSERT_GT(maxCacheSize->Val(), 4096);

        auto maxChunkCount = moduleCounters->FindCounter("MaxChunkCount");
        UNIT_ASSERT(maxChunkCount);
        UNIT_ASSERT_VALUES_EQUAL(2, maxChunkCount->Val());

        auto largeOffset = size1 + 3700000;   // Go beyond the first chunk
        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            largeOffset);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 4);

        auto size6 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size6, 4096);

        bootstrap.ModuleStatsRegistry->UpdateStats(true);

        maxChunkCount = moduleCounters->FindCounter("MaxChunkCount");
        UNIT_ASSERT(maxChunkCount);
        UNIT_ASSERT_VALUES_EQUAL(3, maxChunkCount->Val());

        auto firstChunkOffset = size1 / 3;
        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            firstChunkOffset);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 4);

        auto size7 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size7, 4096);

        auto secondChunkOffset = largeOffset + 200;
        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId,
            secondChunkOffset);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 4);

        auto size8 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size8, 4096);

        // Open second directory handle for the same nodeId
        auto handle2 = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
        UNIT_ASSERT(handle2.Wait(WaitTimeout));
        auto handleId2 = handle2.GetValue();

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId2);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 5);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
            nodeId,
            handleId2,
            size1);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 5);

        auto size9 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size9, 4096);

        bootstrap.ModuleStatsRegistry->UpdateStats(true);

        UNIT_ASSERT(moduleCounters);

        maxCacheSize = moduleCounters->FindCounter("MaxCacheSize");
        UNIT_ASSERT(maxCacheSize);
        UNIT_ASSERT_GT(maxCacheSize->Val(), largeOffset);

        maxChunkCount = moduleCounters->FindCounter("MaxChunkCount");
        UNIT_ASSERT(maxChunkCount);
        UNIT_ASSERT_VALUES_EQUAL(5, maxChunkCount->Val());

        auto close2 =
            bootstrap.Fuse->SendRequest<TReleaseDirRequest>(nodeId, handleId2);
        UNIT_ASSERT_NO_EXCEPTION(close2.GetValue(WaitTimeout));

        auto close =
            bootstrap.Fuse->SendRequest<TReleaseDirRequest>(nodeId, handleId);
        UNIT_ASSERT_NO_EXCEPTION(close.GetValue(WaitTimeout));
    }

    Y_UNIT_TEST(ShouldLoadDirectoryHandlesStorageWithoutErrors)
    {
        const TString sessionId = CreateGuidAsString();
        NProto::TFileStoreFeatures features;
        features.SetDirectoryHandlesStorageEnabled(true);

        auto createSessionHandler = [&](auto, auto)
        {
            NProto::TCreateSessionResponse result;
            result.MutableSession()->SetSessionId(sessionId);
            result.MutableFileStore()->MutableFeatures()->CopyFrom(features);
            result.MutableFileStore()->SetFileSystemId(FileSystemId);
            return MakeFuture(result);
        };

        std::atomic<ui32> numCalls1 = 0;
        std::atomic<ui32> numCalls2 = 0;

        TFsPath tmpPathForCache(
            TFsPath(GetSystemTempDir()) / "directory_handles_storage.dump");

        TString pathToCache;

        {
            auto bootstrap = TBootstrap::CreateWithHandlesStorage();

            bootstrap.Service->CreateSessionHandler = createSessionHandler;

            bootstrap.Service->ListNodesHandler =
                [&](auto callContext, auto request)
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    FileSystemId,
                    callContext->FileSystemId);

                NProto::TListNodesResponse result;

                if (!request->GetCookie()) {
                    for (ui32 i = 1; i <= 20000; ++i) {
                        result.AddNames()->assign(
                            "first_chunk_file_" + ToString(i) + ".txt");
                        auto* node = result.AddNodes();
                        node->SetId(100 + i);
                        node->SetType(NProto::E_REGULAR_NODE);
                    }
                    ++numCalls1;
                    result.SetCookie("has_more_data");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        "has_more_data",
                        request->GetCookie());
                    for (ui32 i = 20001; i <= 25100; ++i) {
                        result.AddNames()->assign(
                            "second_chunk_file_" + ToString(i) + ".txt");
                        auto* node = result.AddNodes();
                        node->SetId(100 + i);
                        node->SetType(NProto::E_REGULAR_NODE);
                    }
                    ++numCalls2;
                }

                return MakeFuture(result);
            };

            bootstrap.Start();
            Y_DEFER
            {
                bootstrap.Stop();
            };

            pathToCache = TFsPath(bootstrap.DirectoryHandlesStoragePath) /
                          FileSystemId / sessionId /
                          "directory_handles_storage";

            const ui64 nodeId = 123;

            auto handle = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
            UNIT_ASSERT(handle.Wait(WaitTimeout));
            auto handleId = handle.GetValue();

            auto read =
                bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
            UNIT_ASSERT(read.Wait(WaitTimeout));
            UNIT_ASSERT_VALUES_EQUAL(numCalls1.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(numCalls2.load(), 0);

            auto largeOffset = 3700000;   // Go beyond the first chunk
            read = bootstrap.Fuse->SendRequest<TReadDirRequest>(
                nodeId,
                handleId,
                largeOffset);
            UNIT_ASSERT(read.Wait(WaitTimeout));
            UNIT_ASSERT_VALUES_EQUAL(numCalls1.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(numCalls2.load(), 1);

            TFsPath(pathToCache).CopyTo(tmpPathForCache.GetPath(), true);
        }

        {
            auto bootstrap = TBootstrap::CreateWithHandlesStorage();

            tmpPathForCache.CopyTo(pathToCache, true);

            bootstrap.Service->CreateSessionHandler = createSessionHandler;

            bootstrap.Start();
            bootstrap.Stop();
        }
    }

    Y_UNIT_TEST(ShouldHandleForgetRequestsForUnknownNodes)
    {
        TBootstrap bootstrap;

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        const ui64 nodeId = 123;
        const ui64 refCount = 10;

        auto forget = bootstrap.Fuse->SendRequest<TForgetRequest>(nodeId, refCount);
        UNIT_ASSERT_NO_EXCEPTION(forget.GetValue(WaitTimeout));
    }

    Y_UNIT_TEST(ShouldResetSessionStateUponInitAndDestroy)
    {
        TBootstrap bootstrap;

        TMutex stateMutex;
        TString state;
        std::atomic<ui32> resets = 0;
        bootstrap.Service->ResetSessionHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            ++resets;
            with_lock(stateMutex) {
                state = request->GetSessionState();
            }

            return MakeFuture(NProto::TResetSessionResponse{});
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        UNIT_ASSERT_VALUES_EQUAL(resets.load(), 1);
        with_lock(stateMutex) {
            UNIT_ASSERT(state);
        }

        auto destroy = bootstrap.Fuse->SendRequest<TDestroyRequest>();
        UNIT_ASSERT_NO_EXCEPTION(destroy.GetValue(WaitTimeout));

        UNIT_ASSERT_VALUES_EQUAL(resets.load(), 2);
        with_lock(stateMutex) {
            UNIT_ASSERT(!state);
        }
    }

    Y_UNIT_TEST(ShouldReinitSessionWithoutInitRequest)
    {
        TBootstrap bootstrap;

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            Y_ABORT_UNLESS(request->GetNodeId() != nodeId);

            NProto::TCreateHandleResponse response;
            response.SetHandle(handleId);
            response.MutableNodeAttr()->SetId(nodeId);

            return MakeFuture(response);
        };

        bootstrap.Service->CreateSessionHandler = [&] (auto, auto) {
            NProto::TVfsSessionState state;
            state.SetProtoMajor(7);
            state.SetProtoMinor(33);
            state.SetBufferSize(256 * 1024);

            NProto::TCreateSessionResponse response;
            response.MutableSession()->SetSessionId(CreateGuidAsString());
            Y_ABORT_UNLESS(state.SerializeToString(response.MutableSession()->MutableSessionState()));

            return MakeFuture(response);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto handle = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_VALUES_EQUAL(handle.GetValue(WaitTimeout), handleId);
    }

    Y_UNIT_TEST(ShouldWaitToAcquireLock)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        std::atomic<ui32> called = 0;
        bootstrap.Service->SetHandlerAcquireLock([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            if (++called < 3) {
                NProto::TAcquireLockResponse response = TErrorResponse(E_FS_WOULDBLOCK, "xxx");
                return MakeFuture(response);
            }

            NProto::TAcquireLockResponse response;
            return MakeFuture(response);
        });

        auto result = bootstrap.Fuse->SendRequest<TAcquireLockRequest>(0, F_RDLCK);
        UNIT_ASSERT_NO_EXCEPTION(result.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(called.load(), 3);
    }

    Y_UNIT_TEST(ShouldNotWaitToAcquireLock)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        std::atomic<ui32> called = 0;
        bootstrap.Service->SetHandlerAcquireLock([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            ++called;
            NProto::TAcquireLockResponse response = TErrorResponse(E_FS_WOULDBLOCK, "xxx");
            return MakeFuture(response);
        });

        auto result = bootstrap.Fuse->SendRequest<TAcquireLockRequest>(0, F_RDLCK, false);
        UNIT_ASSERT(result.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(called.load(), 1);
        UNIT_ASSERT(result.HasException());

        bootstrap.Service->SetHandlerAcquireLock([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            ++called;
            NProto::TAcquireLockResponse response = TErrorResponse(E_FS_BADHANDLE, "xxx");
            return MakeFuture(response);
        });

        result = bootstrap.Fuse->SendRequest<TAcquireLockRequest>(0, F_RDLCK);
        UNIT_ASSERT(result.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(called.load(), 2);
        UNIT_ASSERT(result.HasException());
    }

    Y_UNIT_TEST(ShouldTestLock)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        static constexpr pid_t DefaultPid = 123;

        bootstrap.Service->SetHandlerTestLock([&] (auto callContext, auto /* request */) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TTestLockResponse response;
            return MakeFuture(response);
        });

        auto result = bootstrap.Fuse->SendRequest<TTestLockRequest>(0, F_RDLCK, 0, 100);
        UNIT_ASSERT(result.Wait(WaitTimeout));
        auto lk = result.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(lk.type, F_UNLCK);

        bootstrap.Service->SetHandlerTestLock([&] (auto callContext, auto /* request */) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TTestLockResponse response;
            return MakeFuture(response);
        });

        bootstrap.Service->SetHandlerTestLock([&] (auto callContext, auto /* request */) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TTestLockResponse response = TErrorResponse(E_FS_WOULDBLOCK, "");
            response.SetOwner(100);
            response.SetOffset(100500);
            response.SetLength(500100);
            response.SetLockType(NProto::E_EXCLUSIVE);
            response.SetPid(DefaultPid);

            return MakeFuture(response);
        });

        result = bootstrap.Fuse->SendRequest<TTestLockRequest>(0, F_RDLCK, 0, 100);
        UNIT_ASSERT(result.Wait(WaitTimeout));
        lk = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(lk.type, F_WRLCK);
        UNIT_ASSERT_VALUES_EQUAL(lk.start, 100500);
        UNIT_ASSERT_VALUES_EQUAL(lk.end, 100500 + 500100 - 1);
        UNIT_ASSERT_VALUES_EQUAL(lk.pid, DefaultPid);

        bootstrap.Service->SetHandlerTestLock([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TTestLockResponse response = TErrorResponse(E_FS_BADHANDLE, "");
            return MakeFuture(response);
        });

        result = bootstrap.Fuse->SendRequest<TTestLockRequest>(0, F_RDLCK, 0, 100);
        UNIT_ASSERT(result.Wait(WaitTimeout));
        UNIT_ASSERT_EXCEPTION(result.GetValue(), yexception);
    }

    Y_UNIT_TEST(ShouldNotFailOnSuspendWithRequestsInFlight)
    {
        NAtomic::TBool sessionDestroyed = false;

        TBootstrap bootstrap;
        bootstrap.Service->DestroySessionHandler = [&sessionDestroyed] (auto, auto) {
            sessionDestroyed = true;
            return MakeFuture(NProto::TDestroySessionResponse());
        };

        auto response = NewPromise<NProto::TListNodesResponse>();
        bootstrap.Service->ListNodesHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            return response.GetFuture();
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        const ui64 nodeId = 123;

        auto handle = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
        UNIT_ASSERT(handle.Wait(WaitTimeout));
        auto handleId = handle.GetValue();

        auto read =
            bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
        UNIT_ASSERT(!read.Wait(TDuration::Seconds(1)));

        auto suspend = bootstrap.Loop->SuspendAsync();
        UNIT_ASSERT(!suspend.Wait(TDuration::Seconds(1)));

        response.SetValue(NProto::TListNodesResponse{});
        UNIT_ASSERT(suspend.Wait(WaitTimeout));
        UNIT_ASSERT_NO_EXCEPTION(read.GetValueSync());
    }

    Y_UNIT_TEST(ShouldNotFailOnStopWithRequestsInFlight)
    {
        NAtomic::TBool sessionDestroyed = false;

        TBootstrap bootstrap;
        bootstrap.Service->DestroySessionHandler = [&sessionDestroyed] (auto, auto) {
            sessionDestroyed = true;
            return MakeFuture(NProto::TDestroySessionResponse());
        };

        auto response = NewPromise<NProto::TListNodesResponse>();
        bootstrap.Service->ListNodesHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            return response.GetFuture();
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        const ui64 nodeId = 123;

        auto handle = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
        UNIT_ASSERT(handle.Wait(WaitTimeout));
        auto handleId = handle.GetValue();

        auto read =
            bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
        UNIT_ASSERT(!read.Wait(TDuration::Seconds(1)));

        auto stop = bootstrap.StopAsync();
        UNIT_ASSERT(!stop.Wait(TDuration::Seconds(1)));

        auto read2 =
            bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            read2.GetValueSync(),
            yexception,
            "Unknown error -4");

        response.SetValue(NProto::TListNodesResponse{});
        UNIT_ASSERT(stop.Wait(WaitTimeout));
        UNIT_ASSERT_NO_EXCEPTION(read.GetValueSync());
    }

    Y_UNIT_TEST(ShouldNotAbortOnInvalidServerLookup)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        bootstrap.Service->SetHandlerGetNodeAttr([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TGetNodeAttrResponse response;
            // response.MutableNode()->SetId(123);
            return MakeFuture(response);
        });

        auto lookup = bootstrap.Fuse->SendRequest<TLookupRequest>("test", RootNodeId);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            lookup.GetValue(WaitTimeout),
            yexception,
            "Unknown error -5");
    }

    Y_UNIT_TEST(ShouldCacheXAttrValueOnGet)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        std::atomic<int> callCount = 0;
        bootstrap.Service->SetHandlerGetNodeXAttr([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            ++callCount;
            NProto::TGetNodeXAttrResponse response;
            response.SetValue("value");
            response.SetVersion(1);
            return MakeFuture(response);
        });

        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_STRINGS_EQUAL("value", xattr.GetValue());
        }
        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_STRINGS_EQUAL("value", xattr.GetValue());
            UNIT_ASSERT_EQUAL(1, callCount);
        }
    }

    Y_UNIT_TEST(ShouldCacheXAttrValueOnSet)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        int callCount = 0;
        bootstrap.Service->SetHandlerGetNodeXAttr([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            ++callCount;
            NProto::TGetNodeXAttrResponse response;
            return MakeFuture(response);
        });
        bootstrap.Service->SetHandlerSetNodeXAttr([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TSetNodeXAttrResponse response;
            response.SetVersion(1);
            return MakeFuture(response);
        });

        {
            auto set = bootstrap.Fuse->SendRequest<TSetXAttrValueRequest>("name", "value", RootNodeId);
            UNIT_ASSERT(set.Wait(WaitTimeout));
        }
        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", RootNodeId);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_STRINGS_EQUAL("value", xattr.GetValue());
            UNIT_ASSERT_EQUAL(0, callCount);
        }
    }

    Y_UNIT_TEST(ShouldSkipCacheValueAfterTimeout)
    {
        std::shared_ptr<TTestTimer> timer = std::make_shared<TTestTimer>();
        TBootstrap bootstrap{timer};
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        std::atomic<int> callCount = 0;
        bootstrap.Service->SetHandlerGetNodeXAttr([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            ++callCount;
            NProto::TGetNodeXAttrResponse response;
            response.SetValue("value");
            response.SetVersion(callCount);
            return MakeFuture(response);
        });

        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_STRINGS_EQUAL("value", xattr.GetValue());
        }

        timer->AdvanceTime(TDuration::Hours(1));

        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_STRINGS_EQUAL("value", xattr.GetValue());
            UNIT_ASSERT_EQUAL(2, callCount);
        }
    }

    Y_UNIT_TEST(ShouldNotCacheXAttrWhenErrorHappens)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        std::atomic<int> callCount = 0;
        bootstrap.Service->SetHandlerGetNodeXAttr([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TGetNodeXAttrResponse response;
            if (callCount.load() == 0) {
                response.MutableError()->SetCode(MAKE_FILESTORE_ERROR(NProto::E_FS_XATTR2BIG));
            } else {
                response.SetValue("value");
                response.SetVersion(callCount.load());
            }
            ++callCount;
            return MakeFuture(response);
        });

        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_EXCEPTION(xattr.GetValue(), yexception);
        }
        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_STRINGS_EQUAL("value", xattr.GetValue());
            UNIT_ASSERT_EQUAL(2, callCount.load());
        }
    }

    Y_UNIT_TEST(ShouldCacheXAttrAbsence)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        std::atomic<int> callCount = 0;
        bootstrap.Service->SetHandlerGetNodeXAttr([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TGetNodeXAttrResponse response;
            response.MutableError()->SetCode(MAKE_FILESTORE_ERROR(NProto::E_FS_NOXATTR));
            ++callCount;
            return MakeFuture(response);
        });

        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                xattr.GetValue(),
                yexception,
                "-61"); // NODATA error code
        }
        {
            auto xattr = bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>("name", 6);
            UNIT_ASSERT(xattr.Wait(WaitTimeout));
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                xattr.GetValue(),
                yexception,
                "-61"); // NODATA error code
            UNIT_ASSERT_EQUAL(1, callCount.load());
        }
    }

    Y_UNIT_TEST(SuspendShouldNotDestroySession)
    {
        TBootstrap bootstrap;
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        std::atomic<bool> sessionDestroyed = false;
        bootstrap.Service->SetHandlerDestroySession([&] (auto, auto) {
            sessionDestroyed = true;
            NProto::TDestroySessionResponse response;
            return MakeFuture(response);
        });

        bootstrap.Service->SetHandlerResetSession([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            sessionDestroyed = true;
            NProto::TResetSessionResponse response;
            return MakeFuture(response);
        });

        auto future = bootstrap.Loop->SuspendAsync();
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        UNIT_ASSERT(!sessionDestroyed);

        // prevent stack use after scope on shutdown
        bootstrap.Service->SetHandlerResetSession([&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            return MakeFuture(NProto::TResetSessionResponse{});
        });

        bootstrap.Service->SetHandlerDestroySession([] (auto, auto) {
            return MakeFuture(NProto::TDestroySessionResponse());
        });
    }

    Y_UNIT_TEST(StopShouldDestroySession)
    {
        bool sessionReset = false;
        bool sessionDestroyed = false;

        TBootstrap bootstrap;
        bootstrap.Service->ResetSessionHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            sessionReset = true;
            NProto::TResetSessionResponse response;
            return MakeFuture(response);
        };
        bootstrap.Service->DestroySessionHandler = [&] (auto, auto) {
            sessionDestroyed = true;
            NProto::TDestroySessionResponse response;
            return MakeFuture(response);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto future = bootstrap.Loop->StopAsync();
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        UNIT_ASSERT(sessionReset);
        UNIT_ASSERT(sessionDestroyed);

        // prevent stack use after scope on shutdown
        bootstrap.Service->ResetSessionHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            return MakeFuture(NProto::TResetSessionResponse{});
        };

        bootstrap.Service->DestroySessionHandler = [] (auto, auto) {
            return MakeFuture(NProto::TDestroySessionResponse());
        };
    }

    Y_UNIT_TEST(ShouldHitErrorMetricOnFailure)
    {
        TBootstrap bootstrap;

        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result = TErrorResponse(E_FS_EXIST, "");
            return MakeFuture(result);
        };

        bootstrap.Start();

        auto future = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_EXCEPTION(future.GetValueSync(), yexception);
        bootstrap.Stop(); // wait till all requests are done writing their stats

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", "fs_ut")
            ->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Errors")->GetAtomic());
        UNIT_ASSERT_EQUAL(0, counters->GetCounter("Errors/Fatal")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldNotCrashWhenStoppedBeforeFileStoreResponse)
    {
        return; // NBS-4767
        TBootstrap bootstrap;

        auto create = NewPromise<NProto::TCreateHandleResponse>();
        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            return create.GetFuture();
        };

        bootstrap.Start();

        auto future = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_NO_EXCEPTION(future.Wait(WaitTimeout));
        bootstrap.Stop();

        create.SetValue(TErrorResponse(E_FS_EXIST, ""));
        UNIT_ASSERT_EXCEPTION(future.GetValue(), yexception);
    }

    Y_UNIT_TEST(ShouldHandleReadRequest)
    {
        TBootstrap bootstrap;

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        const ui64 size = 789;

        bootstrap.Service->ReadDataHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(request->GetHandle(), handleId);

            NProto::TReadDataResponse result;
            result.MutableBuffer()->assign(TString(request->GetLength(), 'a'));

            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto read = bootstrap.Fuse->SendRequest<TReadRequest>(
            nodeId, handleId, 0, size);

        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(read.GetValue(), size);

        read = bootstrap.Fuse->SendRequest<TReadRequest>(
            nodeId, handleId, 0, 10_MB);

        UNIT_ASSERT_EXCEPTION(read.GetValueSync(), yexception);
    }

    Y_UNIT_TEST(ShouldUpdateFilesystemMetricsWithInFlight)
    {
        TBootstrap bootstrap;

        auto execute = NewPromise<void>();
        auto requestPromise = NewPromise<NProto::TCreateHandleResponse>();
        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            execute.SetValue();
            return requestPromise.GetFuture();
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto future = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);

        UNIT_ASSERT(execute.GetFuture().Wait(WaitTimeout));
        bootstrap.StatsRegistry->UpdateStats(false);

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", "fs_ut_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", FileSystemId)
            ->FindSubgroup("client", "")
            ->FindSubgroup("cloud", "")
            ->FindSubgroup("folder", "")
            ->FindSubgroup("request", "CreateHandle");

        UNIT_ASSERT_VALUES_EQUAL(1, counters->GetCounter("InProgress")->GetAtomic());

        requestPromise.SetValue(TErrorResponse(E_FS_EXIST, ""));
        UNIT_ASSERT_EXCEPTION(future.GetValueSync(), yexception);
    }

    Y_UNIT_TEST(SendInterruptRequestBeforeOriginal)
    {
        TBootstrap bootstrap;
        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        bootstrap.Service->CreateHandleHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(request);
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);

            NProto::TCreateHandleResponse result;
            result.SetHandle(handleId);
            result.MutableNodeAttr()->SetId(nodeId);
            return MakeFuture(result);
        };

        bootstrap.Start();
        bootstrap.InterruptNextRequest();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto create = bootstrap.Fuse->SendRequest<TCreateHandleRequest>("/file1", RootNodeId);
        UNIT_ASSERT_EQUAL(create.GetValueSync(), handleId); // no interrupted
    }

    Y_UNIT_TEST(ShouldNotTriggerFatalErrorForCancelledRequests)
    {
        TBootstrap bootstrap;
        auto promise = NewPromise<NProto::TAcquireLockResponse>();
        auto handlerCalled = NewPromise<void>();

        bootstrap.Service->AcquireLockHandler = [&] (auto callContext, auto request) {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            handlerCalled.TrySetValue();
            return promise.GetFuture();
        };

        bootstrap.StopTriggered.GetFuture().Subscribe([&] (const auto&) {
            promise.SetValue(
                TErrorResponse(E_FS_WOULDBLOCK, "waiting"));
        });

        bootstrap.Start();

        auto future =
            bootstrap.Fuse->SendRequest<TAcquireLockRequest>(0, F_RDLCK);

        handlerCalled.GetFuture().Wait();

        bootstrap.Stop(); // wait till all requests are done writing their stats

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", "fs_ut")
            ->FindSubgroup("request", "AcquireLock");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Errors")->GetAtomic());
        UNIT_ASSERT_EQUAL(0, counters->GetCounter("Errors/Fatal")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldNotTriggerFatalErrorsForNewRequestsDuringFuseStop)
    {
        TBootstrap bootstrap;
        auto promise = NewPromise<NProto::TAcquireLockResponse>();
        auto handlerCalled = NewPromise<void>();

        bootstrap.Service->AcquireLockHandler = [&] (auto, auto) {
            handlerCalled.TrySetValue();
            return promise.GetFuture();
        };

        bootstrap.Service->CreateHandleHandler = [&] (auto , auto) {
            UNIT_ASSERT_C(false, "Handler should not be called");
            return MakeFuture(NProto::TCreateHandleResponse{});
        };

        bootstrap.StopTriggered.GetFuture().Subscribe([&] (const auto&) {
            // Make synchronous call. Since Stop is triggered we expect
            // that request will be cancelled and future will contain exception
            auto future = bootstrap.Fuse->SendRequest<TCreateHandleRequest>(
                    "/file1",
                    RootNodeId);
            UNIT_ASSERT_EXCEPTION(future.GetValueSync(), yexception);

            // Let completion queue complete all pending requests.
            promise.SetValue(NProto::TAcquireLockResponse{});
        });

        bootstrap.Start();

        auto future =
            bootstrap.Fuse->SendRequest<TAcquireLockRequest>(0, F_RDLCK);

        // Wait for lock request to reach lock handler in service.
        // Now StopAsync in bootstrap cannot complete immediately.
        // All new requests (CreateHandle) should be rejected immediately.
        handlerCalled.GetFuture().Wait();

        bootstrap.Stop();

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", "fs_ut")
            ->FindSubgroup("request", "CreateHandle");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Errors")->GetAtomic());
        UNIT_ASSERT_EQUAL(0, counters->GetCounter("Errors/Fatal")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldProcessDestroyHandleRequestsAsynchronously)
    {
        NProto::TFileStoreFeatures features;
        features.SetAsyncDestroyHandleEnabled(true);
        auto scheduler = std::make_shared<TTestScheduler>();
        TBootstrap bootstrap(CreateWallClockTimer(), scheduler, features);

        const ui64 handle1 = 2;
        const ui64 nodeId1 = 10;
        const ui64 handle2 = 5;
        const ui64 nodeId2 = 11;
        std::atomic_bool releaseFinished = false;
        std::atomic_uint handlerCalled = 0;
        auto counters = bootstrap.Counters->FindSubgroup("component", "fs_ut")
                            ->FindSubgroup("request", "DestroyHandle");
        auto responsePromise1 = NewPromise<NProto::TDestroyHandleResponse>();
        auto responsePromise2 = NewPromise<NProto::TDestroyHandleResponse>();
        bootstrap.Service->SetHandlerDestroyHandle(
            [&,
             responsePromise1,
             responsePromise2](auto callContext, auto request) mutable
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    1,
                    AtomicGet(counters->GetCounter("InProgress")->GetAtomic()));

                UNIT_ASSERT_VALUES_EQUAL(
                    FileSystemId,
                    callContext->FileSystemId);

                if (++handlerCalled == 1) {
                    UNIT_ASSERT(releaseFinished);
                    UNIT_ASSERT_VALUES_EQUAL(handle1, request->GetHandle());
                    UNIT_ASSERT_VALUES_EQUAL(nodeId1, request->GetNodeId());
                    return responsePromise1;
                }
                if (handlerCalled == 2) {
                    UNIT_ASSERT(releaseFinished);
                    UNIT_ASSERT_VALUES_EQUAL(handle2, request->GetHandle());
                    UNIT_ASSERT_VALUES_EQUAL(nodeId2, request->GetNodeId());
                    return responsePromise2;
                }

                UNIT_ASSERT_C(
                    false,
                    "Handler should not be called more than two times");
                return NewPromise<NProto::TDestroyHandleResponse>();
            });

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto future =
            bootstrap.Fuse->SendRequest<TReleaseRequest>(nodeId1, handle1);
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        future = bootstrap.Fuse->SendRequest<TReleaseRequest>(nodeId2, handle2);
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        releaseFinished = true;

        scheduler->RunAllScheduledTasks();
        responsePromise1.SetValue(NProto::TDestroyHandleResponse{});

        scheduler->RunAllScheduledTasks();
        responsePromise2.SetValue(NProto::TDestroyHandleResponse{});

        UNIT_ASSERT_VALUES_EQUAL(2U, handlerCalled.load());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AtomicGet(counters->GetCounter("InProgress")->GetAtomic()));
    }

    Y_UNIT_TEST(ShouldRetryDestroyIfNotSuccessDuringAsyncProcessing)
    {
        NProto::TFileStoreFeatures features;
        features.SetAsyncDestroyHandleEnabled(true);
        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 handle = 2;
        const ui64 nodeId = 10;
        std::atomic_uint handlerCalled = 0;
        auto destroyFinished = NewPromise<void>();
        bootstrap.Service->SetHandlerDestroyHandle(
            [&, destroyFinished](auto callContext, auto request) mutable
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    FileSystemId,
                    callContext->FileSystemId);
                UNIT_ASSERT_VALUES_EQUAL(handle, request->GetHandle());
                UNIT_ASSERT_VALUES_EQUAL(nodeId, request->GetNodeId());
                if (++handlerCalled > 3) {
                    destroyFinished.TrySetValue();
                    return MakeFuture(NProto::TDestroyHandleResponse{});
                }

                NProto::TDestroyHandleResponse response = TErrorResponse(
                    E_REJECTED, "xxx");
                return MakeFuture(response);
            });

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto future =
            bootstrap.Fuse->SendRequest<TReleaseRequest>(nodeId, handle);
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));

        destroyFinished.GetFuture().Wait(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(4U, handlerCalled.load());
    }

    Y_UNIT_TEST(ShouldNotRetryDestroyHandleAndRaiseCritEvent)
    {
        NProto::TFileStoreFeatures features;
        features.SetAsyncDestroyHandleEnabled(true);
        auto scheduler = std::make_shared<TTestScheduler>();
        TBootstrap bootstrap(CreateWallClockTimer(), scheduler, features);

        const ui64 handle = 2;
        const ui64 nodeId = 10;
        auto responsePromise = NewPromise<NProto::TDestroyHandleResponse>();
        bootstrap.Service->SetHandlerDestroyHandle(
            [&, responsePromise](auto callContext, auto request)
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    FileSystemId,
                    callContext->FileSystemId);
                UNIT_ASSERT_VALUES_EQUAL(handle, request->GetHandle());
                UNIT_ASSERT_VALUES_EQUAL(nodeId, request->GetNodeId());

                return responsePromise;
            });

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto future =
            bootstrap.Fuse->SendRequest<TReleaseRequest>(nodeId, handle);
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));

        scheduler->RunAllScheduledTasks();
        NProto::TDestroyHandleResponse response =
            TErrorResponse(E_FS_NOENT, "xxx");
        responsePromise.SetValue(std::move(response));

        auto errorCounter =
            bootstrap.Counters->GetSubgroup("component", "fs_ut")
                ->GetCounter(
                    "AppCriticalEvents/AsyncDestroyHandleFailed",
                    true);

        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*errorCounter));
    }

    Y_UNIT_TEST(ShouldPostponeDestroyHandleRequestIfHandleOpsQueueOverflows)
    {
        NProto::TFileStoreFeatures features;
        features.SetAsyncDestroyHandleEnabled(true);
        auto scheduler = std::make_shared<TTestScheduler>();
        TBootstrap bootstrap(CreateWallClockTimer(), scheduler, features, 20);

        const ui64 handle1 = 2;
        const ui64 nodeId1 = 10;
        const ui64 handle2 = 5;
        const ui64 nodeId2 = 11;
        std::atomic_uint handlerCalled = 0;
        auto responsePromise = NewPromise<NProto::TDestroyHandleResponse>();
        bootstrap.Service->SetHandlerDestroyHandle(
            [&, responsePromise](auto callContext, auto request) mutable
            {
                Y_UNUSED(callContext);
                Y_UNUSED(request);

                if (++handlerCalled == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(handle1, request->GetHandle());
                    UNIT_ASSERT_VALUES_EQUAL(nodeId1, request->GetNodeId());
                    return responsePromise;
                }

                if (handlerCalled == 2) {
                    UNIT_ASSERT_VALUES_EQUAL(handle2, request->GetHandle());
                    UNIT_ASSERT_VALUES_EQUAL(nodeId2, request->GetNodeId());
                }

                return NewPromise<NProto::TDestroyHandleResponse>();
            });

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", "fs_ut")
            ->FindSubgroup("request", "DestroyHandle");
        auto future =
            bootstrap.Fuse->SendRequest<TReleaseRequest>(nodeId1, handle1);
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        UNIT_ASSERT_EQUAL(
            0,
            AtomicGet(counters->GetCounter("InProgress")->GetAtomic()));

        future =
            bootstrap.Fuse->SendRequest<TReleaseRequest>(nodeId2, handle2);

        // Second request should wait until the first request is processed.
        UNIT_ASSERT_EXCEPTION(
            future.GetValue(ExceptionWaitTimeout),
            yexception);
        UNIT_ASSERT_EQUAL(
            1,
            AtomicGet(counters->GetCounter("InProgress")->GetAtomic()));

        // Process first request.
        scheduler->RunAllScheduledTasks();
        responsePromise.SetValue(NProto::TDestroyHandleResponse{});
        UNIT_ASSERT_EQUAL(1, handlerCalled);

        // After the first request is processed, the second request should be
        // completed and added to the HandleOpsQueue.
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        UNIT_ASSERT_EQUAL(
            0,
            AtomicGet(counters->GetCounter("InProgress")->GetAtomic()));

        // Check that second request was added to the queue and processed later.
        scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_EQUAL(2, handlerCalled);
    }

    // We want to ensure that the same file cannot be reused for FileRingBuffers
    // in different FileSystemLoop instances at the same time,
    // because it may lead to file corruption.
    Y_UNIT_TEST(ShouldFailStartWithSameSessionIdIfAsyncDestroyEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetAsyncDestroyHandleEnabled(true);
        TBootstrap bootstrap1(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);
        TBootstrap bootstrap2(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        auto error = bootstrap1.Start();
        Y_DEFER {
            bootstrap1.Stop();
        };
        UNIT_ASSERT(!HasError(error));

        error = bootstrap2.Start();
        UNIT_ASSERT(HasError(error));

        auto handleOpsQueueError =
            bootstrap2.Counters->GetSubgroup("component", "fs_ut")
                ->GetCounter(
                    "AppCriticalEvents/HandleOpsQueueCreatingOrDeletingError",
                    true);

        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*handleOpsQueueError));
    }

    Y_UNIT_TEST(ShouldReadAndWriteWithServerWriteBackCacheEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        const ui64 size = 789;

        std::atomic<int> readDataCalled = 0;

        bootstrap.Service->ReadDataHandler = [&] (auto callContext, auto request) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(request->GetHandle(), handleId);

            readDataCalled++;

            NProto::TReadDataResponse result;
            result.MutableBuffer()->assign(TString(request->GetLength(), 'a'));
            return MakeFuture(result);
        };

        std::atomic<int> writeDataCalled = 0;

        bootstrap.Service->WriteDataHandler = [&] (auto callContext, auto) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            writeDataCalled++;
            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        std::atomic<int> fsyncCalled = 0;

        bootstrap.Service->FsyncHandler = [&] (auto callContext, auto) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            fsyncCalled++;
            return MakeFuture(NProto::TFsyncResponse());
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        // should read empty data
        auto read = bootstrap.Fuse->SendRequest<TReadRequest>(
            nodeId, handleId, 0, size);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(read.GetValue(), size);
        UNIT_ASSERT_VALUES_EQUAL(1, readDataCalled.load());

        // write request without O_DIRECT should go to write cache
        auto reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        // should not write (flush) data from cache immediately
        UNIT_ASSERT_VALUES_EQUAL(0, writeDataCalled.load());

        // should read data from cache
        read = bootstrap.Fuse->SendRequest<TReadRequest>(
            nodeId, handleId, 0, size);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(read.GetValue(), size);
        // ReadData should not be called since last time
        UNIT_ASSERT_VALUES_EQUAL(1, readDataCalled.load());

        auto fsync = bootstrap.Fuse->SendRequest<TFsyncRequest>(
            nodeId, handleId, false /* datasync */);
        UNIT_ASSERT_NO_EXCEPTION(fsync.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, fsyncCalled.load());
        // cache should be flushed
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());

        read = bootstrap.Fuse->SendRequest<TReadRequest>(
            nodeId, handleId, 0, size);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(read.GetValue(), size);
        // should read data from underlying session after flush
        UNIT_ASSERT_VALUES_EQUAL(2, readDataCalled.load());

        // subsequent fsync does nothing
        fsync = bootstrap.Fuse->SendRequest<TFsyncRequest>(
            nodeId, handleId, false /* datasync */);
        UNIT_ASSERT_NO_EXCEPTION(fsync.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(2, fsyncCalled.load());
        // no new writes (flushes) are expected
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());

        // write request without O_DIRECT should go to write cache
        reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());

        // call fsync with |datasync == true| - should work the same way it did
        // before
        fsync = bootstrap.Fuse->SendRequest<TFsyncRequest>(
            nodeId, handleId, true /* datasync */);
        UNIT_ASSERT_NO_EXCEPTION(fsync.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(3, fsyncCalled.load());
        // cache should be flushed
        UNIT_ASSERT_VALUES_EQUAL(2, writeDataCalled.load());

        // write request without O_DIRECT should go to write cache
        reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(2, writeDataCalled.load());

        // same checks for Flush
        auto flush = bootstrap.Fuse->SendRequest<TFlushRequest>(
            nodeId, handleId);
        UNIT_ASSERT_NO_EXCEPTION(flush.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(3, writeDataCalled.load());
    }

    Y_UNIT_TEST(ShouldFsyncDirWithServerWriteBackCacheEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;

        std::atomic<int> writeDataCalled = 0;

        bootstrap.Service->WriteDataHandler = [&] (auto callContext, auto) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            writeDataCalled++;
            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        std::atomic<int> fsyncDirCalled = 0;

        bootstrap.Service->FsyncDirHandler = [&] (auto callContext, auto) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            fsyncDirCalled++;
            return MakeFuture(NProto::TFsyncDirResponse());
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        // write request without O_DIRECT should go to write cache
        auto reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        // should not write (flush) data from cache immediately
        UNIT_ASSERT_VALUES_EQUAL(0, writeDataCalled.load());

        auto dirHandle = bootstrap.Fuse->SendRequest<TOpenDirRequest>(nodeId);
        UNIT_ASSERT(dirHandle.Wait(WaitTimeout));
        auto dirHandleId = dirHandle.GetValue();

        auto fsyncDir = bootstrap.Fuse->SendRequest<TFsyncDirRequest>(
            nodeId, dirHandleId, false /* datasync */);
        UNIT_ASSERT_NO_EXCEPTION(fsyncDir.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, fsyncDirCalled.load());
        // cache should be flushed
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());

        // write request without O_DIRECT should go to write cache
        reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        // should not write (flush) data from cache immediately
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());

        // call FsyncDir with |datasync == true| - should work the same way it
        // did before
        fsyncDir = bootstrap.Fuse->SendRequest<TFsyncDirRequest>(
            nodeId, dirHandleId, true /* datasync */);
        UNIT_ASSERT_NO_EXCEPTION(fsyncDir.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(2, fsyncDirCalled.load());
        // cache should be flushed
        UNIT_ASSERT_VALUES_EQUAL(2, writeDataCalled.load());
    }

    // TODO: create and use metrics for write cache https://github.com/ydb-platform/nbs/issues/4154
    Y_UNIT_TEST(
        ShouldNotUseServerWriteBackCacheForDirectHandles)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        // disable automatic flush to make sure that write cache is not flushed
        const ui32 automaticFlushPeriodMs = 0;
        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features,
            1000,
            automaticFlushPeriodMs);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;

        std::atomic<int> writeDataCalled = 0;

        bootstrap.Service->WriteDataHandler = [&](auto callContext, auto)
        {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            writeDataCalled++;
            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER
        {
            bootstrap.Stop();
        };

        // write request with O_DIRECT should not go to write cache
        auto reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_DIRECT;
        reqWrite->In->Body.flags |= O_WRONLY;
        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());

        // write request without O_DIRECT should go to write cache
        reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());
    }

    Y_UNIT_TEST(ShouldFlushWithServerWriteBackCacheEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;

        std::atomic<int> writeDataCalled = 0;

        bootstrap.Service->WriteDataHandler = [&] (auto callContext, auto) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            writeDataCalled++;
            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto writeReq = std::make_shared<TWriteRequest>(nodeId, handleId, 0, CreateBuffer(4096, 'a'));
        writeReq->In->Body.flags |= O_WRONLY;
        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(writeReq);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        // should not write (flush) data from cache immediately
        UNIT_ASSERT_VALUES_EQUAL(0, writeDataCalled.load());

        // same checks for Flush
        auto flush = bootstrap.Fuse->SendRequest<TFlushRequest>(
            nodeId, handleId);
        UNIT_ASSERT_NO_EXCEPTION(flush.GetValue(WaitTimeout));
        // cache should be flushed
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());
    }

    Y_UNIT_TEST(ShouldReleaseWithServerWriteBackCacheEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;

        std::atomic<int> writeDataCalled = 0;

        bootstrap.Service->WriteDataHandler = [&] (auto callContext, auto) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            writeDataCalled++;
            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        std::atomic<int> destroyHandleCalled = 0;

        bootstrap.Service->SetHandlerDestroyHandle(
            [&](auto callContext, auto)
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    FileSystemId,
                    callContext->FileSystemId);

                destroyHandleCalled++;

                auto promise = NewPromise<NProto::TDestroyHandleResponse>();
                promise.SetValue({});
                return promise.GetFuture();
            });

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        // write request without O_DIRECT should go to write cache
        auto reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));
        // should not write (flush) data from cache immediately
        UNIT_ASSERT_VALUES_EQUAL(0, writeDataCalled.load());

        auto future =
            bootstrap.Fuse->SendRequest<TReleaseRequest>(nodeId, handleId);
        UNIT_ASSERT_NO_EXCEPTION(future.GetValue(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(1, destroyHandleCalled.load());
        // cache should be flushed
        UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());
    }

    Y_UNIT_TEST(ShouldFlushAutomaticallyWithServerWriteBackCacheEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        ui32 automaticFlushPeriodMs = 1;
        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features,
            0,
            automaticFlushPeriodMs);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;

        std::atomic<int> writeDataCalled = 0;

        bootstrap.Service->WriteDataHandler = [&] (auto callContext, auto) {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            writeDataCalled++;
            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        // write request without O_DIRECT should go to write cache
        auto reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
        UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));

        while (true) {
            bootstrap.Timer->Sleep(TDuration::MilliSeconds(1));

            if (writeDataCalled.load() == 0) {
                continue;
            }

            UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled.load());
            break;
        }
    }

    // We want to ensure that the same file cannot be reused for FileRingBuffers
    // in different FileSystemLoop instances at the same time,
    // because it may lead to file corruption.
    Y_UNIT_TEST(ShouldFailStartWithSameSessionIdIfWriteBackCacheEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);
        TBootstrap bootstrap1(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);
        TBootstrap bootstrap2(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        auto error = bootstrap1.Start();
        Y_DEFER {
            bootstrap1.Stop();
        };
        UNIT_ASSERT(!HasError(error));

        error = bootstrap2.Start();
        UNIT_ASSERT(HasError(error));

        auto writeBackCacheError =
            bootstrap2.Counters->GetSubgroup("component", "fs_ut")
                ->GetCounter(
                    "AppCriticalEvents/"
                    "WriteBackCacheCreatingOrDeletingError",
                    true);

        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*writeBackCacheError));
    }

    Y_UNIT_TEST(ShouldNotCrashWhileStoppingWhenForgetRequestIsInFlight)
    {
        TBootstrap bootstrap(CreateWallClockTimer());

        bootstrap.Start();

        const ui64 nodeId = 123;
        const ui64 refCount = 10;

        auto future = bootstrap.Fuse->SendRequest<TForgetRequest>(
            nodeId,
            refCount);
        bootstrap.Stop();

        // The test is designed to verify the absence of a double-free in a
        // specific scenario. However, it cannot reliably assert success or
        // failure because both execution paths are possible
        try {
            future.Wait(WaitTimeout);
            // Rare case: the request can finish before StopAsync runs
        } catch (...) {
            // Exception happens in two cases:
            // * Most commonly: a request is scheduled, dequeued from the
            // virtio queue and then cancelled
            // * Very rarely: request scheduling is deferred and the FUSE loop
            // has already been destroyed by the time the request reaches the
            // virtio queue, causing a wait timeout in |TFuseVirtioClient|
        }
    }

    Y_UNIT_TEST(ShouldRaiseCritEventWhenErrorWasSentToGuest)
    {
        TBootstrap bootstrap;

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        const ui64 size = 789;

        bootstrap.Service->ReadDataHandler = [&](auto, auto)
        {
            return MakeFuture(NProto::TReadDataResponse(TErrorResponse(E_IO)));
        };

        bootstrap.Service->WriteDataHandler = [&](auto, auto)
        {
            return MakeFuture(NProto::TWriteDataResponse(TErrorResponse(E_IO)));
        };

        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        auto read = bootstrap.Fuse->SendRequest<TReadRequest>(
            nodeId, handleId, 0, size);

        UNIT_ASSERT_EXCEPTION(read.GetValueSync(), yexception);

        auto errorCounter =
            bootstrap.Counters->GetSubgroup("component", "fs_ut")
                ->GetCounter("AppCriticalEvents/ErrorWasSentToTheGuest", true);

        UNIT_ASSERT_VALUES_EQUAL(1, errorCounter->Val());

        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(
            nodeId, handleId, 0, CreateBuffer(4096, 'a'));

        UNIT_ASSERT_EXCEPTION(write.GetValue(WaitTimeout), yexception);

        UNIT_ASSERT_VALUES_EQUAL(2, errorCounter->Val());
    }

    Y_UNIT_TEST(ShouldSupportWriteBackCacheCapacity4GiB)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features,
            1000,
            1000,
            4_GB + 100500); // writeBackCacheCapacity

        // Narrowing ui64 to ui32 will result in verification failure because
        // 100500 is less than the minimal allowed cache capacity
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
    }

    void TestShouldSupportZeroCopyWriteByWriteBackCache(
        bool zeroCopyWriteEnabled)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);
        features.SetZeroCopyWriteEnabled(zeroCopyWriteEnabled);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;

        std::atomic<int> writeDataCalled = 0;

        bootstrap.Service->WriteDataHandler = [&](auto, const auto& request)
        {
            UNIT_ASSERT_EQUAL_C(
                zeroCopyWriteEnabled,
                !request->GetIovecs().empty(),
                "Requests generated by TWriteBackCache should use iovecs if "
                "and only if ZeroCopyWriteEnabled feature is on");
            writeDataCalled++;
            NProto::TWriteDataResponse result;
            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER
        {
            bootstrap.Stop();
        };

        // write request without O_DIRECT should go to write cache
        auto reqWrite = std::make_shared<TWriteRequest>(
            nodeId,
            handleId,
            0,
            CreateBuffer(4096, 'a'));
        reqWrite->In->Body.flags |= O_WRONLY;
        auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);

        while (writeDataCalled.load() == 0) {
            bootstrap.Timer->Sleep(TDuration::MilliSeconds(1));
        }
    }

    Y_UNIT_TEST(ShouldSupportZeroCopyWriteByWriteBackCache)
    {
        TestShouldSupportZeroCopyWriteByWriteBackCache(false);
        TestShouldSupportZeroCopyWriteByWriteBackCache(true);
    }

    Y_UNIT_TEST(ShouldFlushAllRequestsBeforeSessionIsDestroyed)
    {
        // The idea is to fill WriteBackCache with requests and to stop session.
        // It should flush both cached and pending requests before session is
        // destroyed. To ensure that there are pending requests, we write more
        // data than the cache capacity (~1_MB) and temporarily prevent write
        // requests from completion.

        // Note: due to the test framework/client limitations, the number of
        // pending requests should not exceed 128 and the size of a message
        // should not exceed 8192 bytes

        constexpr ui64 NodeCount = 8;
        // ThreadPool limitation
        constexpr i64 MaxPendingRequestCount = 4;
        constexpr i64 MaxRequestCount = 1000;
        // Queue buffer limitation
        constexpr ui64 MinByteCount = 7000;
        constexpr ui64 MaxByteCount = 7500;
        constexpr ui64 MaxOffset = 128_KB;
        constexpr TDuration Timeout = TDuration::Seconds(15);

        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        // Disable automatic flush
        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features,
            /* handleOpsQueueSize= */ 1000,
            /* writeBackCacheAutomaticFlushPeriodMs= */ 0,
            WriteBackCacheCapacity);

        auto writeDataPromise = NewPromise();
        std::atomic<int> writeDataCalledCount = 0;

        bootstrap.Service->WriteDataHandler = [&](auto, auto)
        {
            writeDataCalledCount++;
            // The same future cannot be shared between responses
            auto promise = NewPromise<NProto::TWriteDataResponse>();
            writeDataPromise.GetFuture().Subscribe(
                [promise](const auto&) mutable { promise.SetValue({}); });
            return promise;
        };

        bootstrap.Start();
        Y_DEFER
        {
            bootstrap.Stop();
        };

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", "fs_ut_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", FileSystemId)
            ->FindSubgroup("client", "")
            ->FindSubgroup("cloud", "")
            ->FindSubgroup("folder", "")
            ->FindSubgroup("request", "WriteData");

        auto requestCountSensor = counters->GetCounter("Count");
        auto requestInProgressSensor = counters->GetCounter("InProgress");
        i64 requestCount = 0;

        while (requestInProgressSensor->Val() < MaxPendingRequestCount) {
            UNIT_ASSERT_GT(MaxRequestCount, requestCount);

            ui64 nodeId = RandomNumber(NodeCount) + 123;
            ui64 handleId = nodeId + 456;
            ui64 offset = RandomNumber(MaxOffset);
            ui64 byteCount =
                RandomNumber(MaxByteCount - MinByteCount + 1) + MinByteCount;

            auto reqWrite = std::make_shared<TWriteRequest>(
                nodeId,
                handleId,
                offset,
                CreateBuffer(byteCount, 'a'));

            reqWrite->In->Body.flags |= O_WRONLY;
            bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);

            requestCount++;

            UNIT_ASSERT(WaitForCondition(
                WaitTimeout,
                [&]()
                {
                    const bool requestIsProcessed =
                        requestCount == requestInProgressSensor->Val() +
                                            requestCountSensor->Val();

                    // A pending request will eventually become completed if the
                    // cache is not full. We don't want to count these requests
                    // as pending and will wait instead.

                    // Cache fullness can be detected by non-zero WriteData
                    // attempts from Flush that is triggered when there is no
                    // space to store the request

                    const bool nonZeroPendingIsExpected =
                        requestInProgressSensor->Val() == 0 ||
                        writeDataCalledCount > 0;

                    return requestIsProcessed && nonZeroPendingIsExpected;
                }));
        }

        auto path = TempDir.Path() / "WriteBackCache" / FileSystemId /
                    SessionId / "write_back_cache";

        UNIT_ASSERT(path.Exists());

        auto stopFuture = bootstrap.StopAsync();

        UNIT_ASSERT_VALUES_EQUAL(
            MaxPendingRequestCount,
            requestInProgressSensor->Val());

        // Enable progression of WriteData requests - this will enable flushing
        writeDataPromise.SetValue();
        UNIT_ASSERT(stopFuture.Wait(Timeout));

        UNIT_ASSERT_VALUES_EQUAL(0, requestInProgressSensor->Val());
        UNIT_ASSERT_VALUES_EQUAL(requestCount, requestCountSensor->Val());
        UNIT_ASSERT(!path.Exists());
    }

    Y_UNIT_TEST(ShouldHandleZeroCopyReadRequest)
    {
        NProto::TFileStoreFeatures features;
        features.SetZeroCopyReadEnabled(true);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        const ui64 size = 30;
        const auto data = GenerateValidateData(size, 2);

        bootstrap.Service->ReadDataHandler = [&](auto callContext, auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(request->GetHandle(), handleId);
            auto& iovecs = request->GetIovecs();
            UNIT_ASSERT_EQUAL(1, iovecs.size());
            UNIT_ASSERT_EQUAL(request->GetLength(), iovecs[0].GetLength());

            NProto::TReadDataResponse result;
            memcpy(
                reinterpret_cast<void*>(iovecs[0].GetBase()),
                data.data(),
                iovecs[0].GetLength());
            result.SetLength(iovecs[0].GetLength());

            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER
        {
            bootstrap.Stop();
        };

        auto request =
            std::make_shared<TReadRequest>(nodeId, handleId, 0, size);
        auto read = bootstrap.Fuse->SendRequest<TReadRequest>(request);

        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(read.GetValue(), size);
        UNIT_ASSERT_VALUES_EQUAL(
            data,
            TString(reinterpret_cast<char*>(&request->Out->Body), size));
    }

    Y_UNIT_TEST(ShouldHandleZeroCopyReadRequestFallbackToBuffer)
    {
        NProto::TFileStoreFeatures features;
        features.SetZeroCopyReadEnabled(true);

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        const ui64 size = 105;
        const auto data = GenerateValidateData(size, 1);

        bootstrap.Service->ReadDataHandler = [&](auto callContext, auto request)
        {
            UNIT_ASSERT_VALUES_EQUAL(FileSystemId, callContext->FileSystemId);
            UNIT_ASSERT_VALUES_EQUAL(request->GetHandle(), handleId);
            auto& iovecs = request->GetIovecs();
            UNIT_ASSERT_EQUAL(1, iovecs.size());
            UNIT_ASSERT_EQUAL(request->GetLength(), iovecs[0].GetLength());

            NProto::TReadDataResponse result;
            result.MutableBuffer()->assign(data);

            return MakeFuture(result);
        };

        bootstrap.Start();
        Y_DEFER
        {
            bootstrap.Stop();
        };

        auto request =
            std::make_shared<TReadRequest>(nodeId, handleId, 0, size);
        auto read = bootstrap.Fuse->SendRequest<TReadRequest>(request);

        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(read.GetValue(), size);
        UNIT_ASSERT_VALUES_EQUAL(
            data,
            TString(reinterpret_cast<char*>(&request->Out->Body), size));
    }

    Y_UNIT_TEST(ShouldRestoreAndDrainCacheAfterSessionRestart)
    {
        const TString sessionId = CreateGuidAsString();

        std::atomic<int> writeDataCalled = 0;
        std::atomic<int> writeDataCalled2 = 0;

        const ui64 nodeId = 123;
        const ui64 handleId = 456;

        auto createBootstrap = [&](bool serverWriteBackCacheEnabled,
                                   std::atomic<int>& counter)
        {
            NProto::TFileStoreFeatures features;
            features.SetServerWriteBackCacheEnabled(
                serverWriteBackCacheEnabled);

            TBootstrap bootstrap(
                CreateWallClockTimer(),
                CreateScheduler(),
                features);

            bootstrap.Service->CreateSessionHandler =
                [features, &sessionId](auto, auto)
            {
                NProto::TCreateSessionResponse result;
                result.MutableSession()->SetSessionId(sessionId);
                result.MutableFileStore()->SetBlockSize(4096);
                result.MutableFileStore()->MutableFeatures()->CopyFrom(
                    features);
                result.MutableFileStore()->SetFileSystemId(FileSystemId);
                return MakeFuture(result);
            };

            bootstrap.Service->WriteDataHandler = [&counter](auto, const auto&)
            {
                counter++;
                NProto::TWriteDataResponse result;
                return MakeFuture(result);
            };

            return bootstrap;
        };

        {
            auto bootstrap = createBootstrap(true, writeDataCalled);

            bootstrap.Start();
            Y_DEFER
            {
                bootstrap.Stop();
            };

            auto reqWrite = std::make_shared<TWriteRequest>(
                nodeId,
                handleId,
                0,
                CreateBuffer(4096, 'a'));
            reqWrite->In->Body.flags |= O_WRONLY;
            auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
            UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));

            auto suspend = bootstrap.Loop->SuspendAsync();
            UNIT_ASSERT(suspend.Wait(WaitTimeout));
        }

        // Since write-back cache was enabled, the actual write didn't happen
        // and the request is stored in the persistent queue
        UNIT_ASSERT_VALUES_EQUAL(0, writeDataCalled.load());

        auto path = TempDir.Path() / "WriteBackCache" / FileSystemId /
                    sessionId / "write_back_cache";

        {
            TFileRingBuffer ringBuffer(path, WriteBackCacheCapacity);
            UNIT_ASSERT(!ringBuffer.Empty());
        }

        {
            auto bootstrap = createBootstrap(false, writeDataCalled2);

            bootstrap.Start();
            Y_DEFER
            {
                bootstrap.Stop();
            };

            UNIT_ASSERT_VALUES_EQUAL(0, writeDataCalled2.load());

            auto flush =
                bootstrap.Fuse->SendRequest<TFlushRequest>(nodeId, handleId);
            UNIT_ASSERT_NO_EXCEPTION(flush.GetValue(WaitTimeout));

            // cache should be flushed
            UNIT_ASSERT_VALUES_EQUAL(1, writeDataCalled2.load());

            auto reqWrite = std::make_shared<TWriteRequest>(
                nodeId,
                handleId,
                0,
                CreateBuffer(4096, 'a'));
            reqWrite->In->Body.flags |= O_WRONLY;
            auto write = bootstrap.Fuse->SendRequest<TWriteRequest>(reqWrite);
            UNIT_ASSERT_NO_EXCEPTION(write.GetValue(WaitTimeout));

            // Cache is drained and disabled - new requests go directly
            // to the session
            UNIT_ASSERT_VALUES_EQUAL(2, writeDataCalled2.load());

            auto suspend = bootstrap.Loop->SuspendAsync();
            UNIT_ASSERT(suspend.Wait(WaitTimeout));
        }

        {
            TFileRingBuffer ringBuffer(path, WriteBackCacheCapacity);
            UNIT_ASSERT(ringBuffer.Empty());
        }

        {
            auto bootstrap = createBootstrap(false, writeDataCalled2);

            bootstrap.Start();
            Y_DEFER
            {
                bootstrap.Stop();
            };
        }

        UNIT_ASSERT(!path.Exists());
    }

    Y_UNIT_TEST(ShouldUpdateFileSizeWhenUsingWriteBackCache)
    {
        NProto::TFileStoreFeatures features;
        features.SetServerWriteBackCacheEnabled(true);

        TMutex mutex;

        TBootstrap bootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features);

        // Set these variables to different before each test case in order to
        // prevent caching node attributes
        std::atomic<ui64> nodeId = 0;

        auto getNodeName = [&]() {
            return Sprintf("file_%lu", nodeId.load());
        };

        std::atomic<ui64> size = 0;

        std::atomic<int> writeDataCalled = 0;
        std::atomic<int> createHandleCalled = 0;
        std::atomic<int> getNodeAttrCalled = 0;
        std::atomic<int> listNodesCalled = 0;
        std::atomic<int> resolvePathCalled = 0;

        // Emulate std::atomic<ui64>::fetch_max - atomically calculate max
        auto fetchMax = [&](ui64 arg)
        {
            ui64 cur = size.load();
            while (cur < arg && !size.compare_exchange_strong(cur, arg)) {
            }
        };

        // The following filestore service requests return node attributes:
        // - CreateHandle
        // - GetNodeAttr
        // - ListNodes
        // - ResolvePath

        auto setAttr = [&](NProto::TNodeAttr* nodeAttr)
        {
            nodeAttr->SetId(nodeId);
            nodeAttr->SetType(NProto::E_REGULAR_NODE);
            nodeAttr->SetSize(size);
        };

        auto proceedServiceOperations = NewPromise();

        auto getProceedServiceOperations = [&]()
        {
            with_lock (mutex) {
                return proceedServiceOperations;
            }
        };

        auto delayedFuture = [&](auto result)
        {
            auto promise = NewPromise<decltype(result)>();
            getProceedServiceOperations().GetFuture().Subscribe(
                [promise, result = std::move(result)](const auto&) mutable
                { promise.SetValue(result); });
            return promise.GetFuture();
        };

        bootstrap.Service->CreateHandleHandler = [&](auto, auto)
        {
            createHandleCalled++;
            NProto::TCreateHandleResponse result;
            result.SetHandle(nodeId + 100);
            setAttr(result.MutableNodeAttr());
            return delayedFuture(result);
        };

        bootstrap.Service->GetNodeAttrHandler = [&](auto, auto)
        {
            getNodeAttrCalled++;
            NProto::TGetNodeAttrResponse result;
            setAttr(result.MutableNode());
            return delayedFuture(result);
        };

        bootstrap.Service->ListNodesHandler = [&](auto, auto)
        {
            listNodesCalled++;
            NProto::TListNodesResponse result;
            result.AddNames()->assign(getNodeName());
            setAttr(result.AddNodes());
            return delayedFuture(result);
        };

        bootstrap.Service->ResolvePathHandler = [&](auto, auto)
        {
            resolvePathCalled++;
            NProto::TResolvePathResponse result;
            result.AddNodeIds(nodeId);
            setAttr(result.MutableAttr());
            return delayedFuture(result);
        };

        // We should also check the amount of bytes returned by ReadData
        // that is affected by the node size

        bootstrap.Service->ReadDataHandler = [&](auto, const auto& rq)
        {
            ui64 end = Min(size.load(), rq->GetOffset() + rq->GetLength());
            ui64 byteCount = end > rq->GetOffset() ? end - rq->GetOffset() : 0;

            NProto::TReadDataResponse result;
            result.MutableBuffer()->assign(TString(byteCount, 'a'));
            return delayedFuture(result);
        };

        // The following Fuse requests call the mentioned above filestore
        // service requests:
        // - FUSE_CREATE -> CreateHandle (creates new node, expected size = 0)
        // - FUSE_OPEN -> CreateHandle (the returned attributes are not used)
        // - FUSE_GETATTR -> GetNodeAttr
        // - FUSE_LOOKUP -> GetNodeAttr
        // - FUSE_READDIR -> ListNodes (the returned attributes are not used)
        // - FUSE_READDIRPLUS -> ListNodes
        // Filestore service request ResolvePath seems to be never called

        // Note: TFileSystem implementation may implicitly cache node attributes
        // returned by the filestore service. We want to ensure that the
        // correct values are cached so it worth to call FUSE_CREATE, FUSE_OPEN
        // and FUSE_READDIR despite the fact that they don't directly return
        // attributes in their responses

        auto getSizeFromGetAttr = [&]()
        {
            auto rq = std::make_shared<TGetAttrRequest>(nodeId);
            return bootstrap.Fuse->SendRequest(rq).Apply(
                [rq](const auto&) { return rq->Out->Body.attr.size; });
        };

        auto getSizeFromOpenHandle = [&]()
        {
            auto rq = std::make_shared<TOpenHandleRequest>(nodeId);
            // The handler for FUSE_OPEN internally calls Session->CreateHandle
            // that returns node attributes that may be cached and returned by
            // the subsequent FUSE_GETATTR call
            return bootstrap.Fuse->SendRequest(rq).Apply(
                [&](const auto&) { return getSizeFromGetAttr(); });
        };

        auto getSizeFromLookup = [&]()
        {
            auto rq = std::make_shared<TLookupRequest>(getNodeName(), nodeId);
            return bootstrap.Fuse->SendRequest(rq).Apply(
                [rq](const auto&) { return rq->Out->Body.attr.size; });
        };

        auto getSizeFromDirectoryRead = [&]()
        {
            auto rq1 = std::make_shared<TOpenDirRequest>(nodeId + 1);
            auto rq2 = std::make_shared<TReadDirRequest>(nodeId + 1, 0);
            auto rq3 = std::make_shared<TReleaseDirRequest>(nodeId + 1, 0);

            return bootstrap.Fuse->SendRequest(rq1)
                .Apply(
                    [&, rq1, rq2](const auto&)
                    {
                        rq2->In->Body.fh = rq1->Out->Body.fh;
                        return bootstrap.Fuse->SendRequest(rq2);
                    })
                .Apply(
                    [&, rq1, rq3](const auto&)
                    {
                        rq3->In->Body.fh = rq1->Out->Body.fh;
                        return bootstrap.Fuse->SendRequest(rq3);
                    })
                .Apply(
                    [&, rq2](const auto&)
                    {
                        auto buf = TStringBuf(
                            reinterpret_cast<const char*>(rq2->Out->Data()),
                            rq2->Out->Header.len - sizeof(rq2->Out->Header));

                        // Skip "." and ".." entries
                        UNIT_ASSERT_LE(
                            sizeof(fuse_direntplus) + 320,
                            buf.size());
                        buf = buf.Skip(320);

                        const auto* de =
                            reinterpret_cast<const fuse_direntplus*>(
                                buf.data());

                        return de->entry_out.attr.size;
                    });
        };

        auto getSizeFromDirectRead = [&]()
        {
            auto rq = std::make_shared<TReadRequest>(
                nodeId,
                nodeId + 100,
                0,
                1000);
            rq->In->Body.flags |= O_DIRECT;
            return bootstrap.Fuse->SendRequest<TReadRequest>(rq);
        };

        auto getSizeFromCachedRead = [&]()
        {
            auto rq = std::make_shared<TReadRequest>(
                nodeId,
                nodeId + 100,
                0,
                1000);
            return bootstrap.Fuse->SendRequest<TReadRequest>(rq);
        };

        // The following filestore service operations affect node size:
        // - AllocateData
        // - SetNodeAttr (also returns new attributes)
        // - TruncateData - seems to be unused
        // - WriteData

        bootstrap.Service->AllocateDataHandler = [&](auto, const auto& rq)
        {
            fetchMax(rq->GetOffset() + rq->GetLength());
            return MakeFuture(NProto::TAllocateDataResponse());
        };

        bootstrap.Service->SetNodeAttrHandler = [&](auto, const auto& rq)
        {
            // Assume that F_SET_ATTR_SIZE is set
            size = rq->GetUpdate().GetSize();
            NProto::TSetNodeAttrResponse result;
            result.MutableNode()->SetId(nodeId);
            result.MutableNode()->SetSize(size);
            return MakeFuture(std::move(result));
        };

        bootstrap.Service->WriteDataHandler = [&](auto, const auto& rq)
        {
            writeDataCalled++;
            fetchMax(
                rq->GetOffset() + rq->GetBuffer().size() -
                rq->GetBufferOffset());
            return MakeFuture(NProto::TWriteDataResponse());
        };

        // We verify reported node size using various request combinations

        bootstrap.Start();
        Y_DEFER
        {
            bootstrap.Stop();
        };

        auto write = [&](ui64 offset, ui64 size, bool direct)
        {
            auto rq = std::make_shared<TWriteRequest>(
                nodeId,
                nodeId + 100,
                offset,
                CreateBuffer(size, 'a'));
            rq->In->Body.flags |= O_WRONLY;
            if (direct) {
                rq->In->Body.flags |= O_DIRECT;
            }
            auto resp = bootstrap.Fuse->SendRequest<TWriteRequest>(rq);
            return resp.GetValue(WaitTimeout) == size;
        };

        auto flush = [&]()
        {
            auto rq = std::make_shared<TFlushRequest>(
                nodeId,
                nodeId + 100);
            auto resp = bootstrap.Fuse->SendRequest<TFlushRequest>(rq);
            resp.Wait(WaitTimeout);
        };

        auto setSize = [&](ui64 size)
        {
            auto rq = std::make_shared<TSetAttrRequest>(nodeId, nodeId + 100);
            rq->In->Body.valid = FUSE_SET_ATTR_SIZE;
            rq->In->Body.size = size;
            UNIT_ASSERT(bootstrap.Fuse->SendRequest(rq).Wait(WaitTimeout));
            return rq->Out->Body.attr.size;
        };

        Y_UNUSED(setSize);

        auto iter = 1;

        auto nextIter = [&]()
        {
            ++iter;
            size = iter * 10;
            nodeId = iter * 5;
            return size.load();
        };

        // Scenario 1:
        // - Request and wait for WriteData with O_DIRECT flag
        // - Get node size
        auto testScenario1 = [&](const TString& testName, auto getSizeFunc)
        {
            auto size = nextIter();
            auto counter = writeDataCalled.load();
            getProceedServiceOperations().TrySetValue();
            UNIT_ASSERT_C(write(size - 5, 9, true), "Scenario 1 " + testName);
            UNIT_ASSERT_VALUES_EQUAL_C(
                counter + 1,
                writeDataCalled.load(),
                "Scenario 1 " + testName);
            UNIT_ASSERT_VALUES_EQUAL_C(
                size + 4,
                getSizeFunc().GetValue(WaitTimeout),
                "Scenario 1 " + testName);
        };

        // Scenario 2:
        // - Request and wait for WriteData without O_DIRECT flag
        // - Get node size
        auto testScenario2 = [&](const TString& testName, auto getSizeFunc)
        {
            auto size = nextIter();
            auto counter = writeDataCalled.load();
            getProceedServiceOperations().TrySetValue();
            UNIT_ASSERT_C(write(size - 5, 9, false), "Scenario 2 " + testName);
            UNIT_ASSERT_VALUES_EQUAL_C(
                counter,
                writeDataCalled.load(),
                "Scenario 2 " + testName);
            UNIT_ASSERT_VALUES_EQUAL_C(
                size + 4,
                getSizeFunc().GetValue(WaitTimeout),
                "Scenario 2 " + testName);
        };

        // Scenario 3:
        // - Request and wait for WriteData without O_DIRECT flag
        // - Initiate a request that returns node size
        // - Request and wait for Flush
        // - Proceed the request that returns node size
        auto testScenario3 = [&](const TString& testName, auto getSizeFunc)
        {
            auto size = nextIter();
            auto counter = writeDataCalled.load();
            with_lock(mutex) {
                proceedServiceOperations = NewPromise();
            }
            UNIT_ASSERT_C(write(size - 5, 9, false), "Scenario 3 " + testName);
            UNIT_ASSERT_VALUES_EQUAL_C(
                counter,
                writeDataCalled.load(),
                "Scenario 3 " + testName);

            auto sizeFuture = getSizeFunc();
            UNIT_ASSERT_C(!sizeFuture.HasValue(), "Scenario 3 " + testName);

            flush();
            UNIT_ASSERT_VALUES_EQUAL_C(
                counter + 1,
                writeDataCalled.load(),
                "Scenario 3 " + testName);

            getProceedServiceOperations().SetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                size + 4,
                sizeFuture.GetValue(WaitTimeout),
                "Scenario 3 " + testName);
        };

        auto test = [&](const TString& testName, auto getSizeFunc)
        {
            testScenario1(testName, getSizeFunc);
            testScenario2(testName, getSizeFunc);
            testScenario3(testName, getSizeFunc);
        };

        test("OpenHandle", getSizeFromOpenHandle);
        test("GetAttr", getSizeFromGetAttr);
        test("Lookup", getSizeFromLookup);
        test("DirectoryRead", getSizeFromDirectoryRead);
        test("CachedRead", getSizeFromCachedRead);
        test("DirectRead", getSizeFromDirectRead);
    }
}

}   // namespace NCloud::NFileStore::NFuse
