#include "fs.h"

#include "log.h"
#include "loop.h"
#include "node_cache.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
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
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/libs/virtiofsd/fuse.h>

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

static const TString FileSystemId = "fs1";
static const TString SessionId = CreateGuidAsString();

static const TTempDir TempDir;

TString CreateBuffer(size_t len, char fill = 0)
{
    return TString(len, fill);
}

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<TFuseVirtioClient> Fuse;

    const ILoggingServicePtr Logging;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const NMonitoring::TDynamicCountersPtr Counters;
    const IRequestStatsRegistryPtr StatsRegistry;

    ISessionPtr Session;
    std::shared_ptr<TFileStoreTest> Service;

    IFileSystemLoopPtr Loop;

    TString SocketPath;

    TPromise<void> StopTriggered = NewPromise<void>();

    TBootstrap(
            ITimerPtr timer = CreateWallClockTimer(),
            ISchedulerPtr scheduler = CreateScheduler(),
            const NProto::TFileStoreFeatures& featuresConfig = {},
            ui32 handleOpsQueueSize = 1000,
            ui32 writeBackCacheAutomaticFlushPeriodMs = 1000)
        : Logging(CreateLoggingService("console", { TLOG_RESOURCES }))
        , Scheduler{std::move(scheduler)}
        , Timer{std::move(timer)}
        , Counters{MakeIntrusive<NMonitoring::TDynamicCounters>()}
        , StatsRegistry{CreateRequestStatsRegistry(
            "fs_ut",
            std::make_shared<TDiagnosticsConfig>(),
            Counters,
            Timer,
            CreateUserCounterSupplierStub())}
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
        if (featuresConfig.GetServerWriteBackCacheEnabled()) {
            proto.SetWriteBackCachePath(TempDir.Path() / "WriteBackCache");
            // minimum possible capacity
            proto.SetWriteBackCacheCapacity(1024*1024 + 1024);
            proto.SetWriteBackCacheAutomaticFlushPeriod(
                writeBackCacheAutomaticFlushPeriodMs);
        }

        auto config = std::make_shared<TVFSConfig>(std::move(proto));
        Loop = NFuse::CreateFuseLoop(
            config,
            Logging,
            StatsRegistry,
            Scheduler,
            Timer,
            CreateProfileLogStub(),
            Session);
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

    Y_UNIT_TEST(ShouldHandleWriteRequest)
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
        bootstrap.Service->ListNodesHandler = [&] (auto callContext, auto request) {
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
        auto read = bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 1);

        auto size1 = read.GetValue();
        UNIT_ASSERT(size1 > 0);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId, size1);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 2);

        auto size2 = read.GetValue();
        UNIT_ASSERT(size2 > 0);

        read = bootstrap.Fuse->SendRequest<TReadDirRequest>(nodeId, handleId, size1 + size2);
        UNIT_ASSERT(read.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(numCalls.load(), 2);

        auto size3 = read.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(size3, 0);

        auto close = bootstrap.Fuse->SendRequest<TReleaseDirRequest>(nodeId, handleId);
        UNIT_ASSERT_NO_EXCEPTION(close.GetValue(WaitTimeout));
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
        UNIT_ASSERT_EXCEPTION(future.GetValue(WaitTimeout), yexception);
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
        auto scheduler = std::make_shared<TTestScheduler>();
        TBootstrap bootstrap(CreateWallClockTimer(), scheduler);

        bootstrap.Start();

        const ui64 nodeId = 123;
        const ui64 refCount = 10;

        auto future = bootstrap.Fuse->SendRequest<TForgetRequest>(nodeId, refCount);
        bootstrap.Stop();

        scheduler->RunAllScheduledTasks();

        UNIT_ASSERT_EXCEPTION(future.GetValue(WaitTimeout), yexception);
    }

    Y_UNIT_TEST(ShouldRaiseCritEventWhenErrorWasSentToGuest)
    {
        TBootstrap bootstrap;

        const ui64 nodeId = 123;
        const ui64 handleId = 456;
        const ui64 size = 789;

        bootstrap.Service->ReadDataHandler = [&](auto, auto)
        {
            NProto::TReadDataResponse result = TErrorResponse(E_IO);
            return MakeFuture(result);
        };

        bootstrap.Service->WriteDataHandler = [&](auto, auto)
        {
            NProto::TWriteDataResponse result = TErrorResponse(E_IO);
            return MakeFuture(result);
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
}

}   // namespace NCloud::NFileStore::NFuse
