#pragma once

#include "directory_handle_storage.h"
#include "log.h"
#include "loop.h"
#include "persistent_state_manager.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/filesystem_counters.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/service/filesystem_event.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>
#include <cloud/filestore/libs/vhost/client.h>
#include <cloud/filestore/libs/vhost/request.h>
#include <cloud/filestore/libs/vhost/server.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/random/random.h>

#include <csignal>
#include <cstdio>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr ui64 WriteBackCacheStateFileSize = 1_MB + 4_KB;
constexpr TStringBuf MetricsComponent = "fs_ut";

inline const TString FileSystemId = "fs1";
inline const TString SessionId = CreateGuidAsString();

inline const TTempDir TempDir;

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<NVhost::TFuseVirtioClient> Fuse;

    const ILoggingServicePtr Logging;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const NMonitoring::TDynamicCountersPtr Counters;
    const IFsCountersProviderPtr FsCountersProvider;
    const IRequestStatsRegistryPtr StatsRegistry;
    const IModuleStatsRegistryPtr ModuleStatsRegistry;

    NClient::ISessionPtr Session;
    std::shared_ptr<TFileStoreTest> Service;

    NVFS::IFileSystemLoopPtr Loop;

    TString SocketPath;

    NThreading::TPromise<void> StopTriggered =
        NThreading::NewPromise<void>();

    TString DirectoryHandleStoragePath;

    TBootstrap(
            ITimerPtr timer = CreateWallClockTimer(),
            ISchedulerPtr scheduler = CreateScheduler(),
            const NProto::TFileStoreFeatures& featuresConfig = {},
            ui32 handleOpsQueueStateFileSize = 1000,
            ui32 writeBackCacheAutomaticFlushPeriodMs = 1000,
            ui64 writeBackCacheStateFileSize = WriteBackCacheStateFileSize,
            ui64 directoryHandlesInitialDataSize = 0,
            ui64 directoryHandlesMaxDataAreaStepSize = 0,
            IFileMapMemoryLimiterPtr fileMapMemoryLimiter =
                CreateFileMapMemoryLimiterStub(),
            IPersistentStateManagerPtr persistentStateManager = nullptr,
            IMultiFileSystemEventHandlerPtr multiFileSystemEventHandler =
                nullptr)
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
            NCloud::NStorage::NUserStats::CreateUserCounterSupplierStub(),
            FsCountersProvider)}
        , ModuleStatsRegistry{CreateModuleStatsRegistry(
            Timer,
            FsCountersProvider,
            Counters->GetSubgroup("component", TString{MetricsComponent}))}
    {
        signal(SIGUSR1, SIG_IGN);   // see fuse/driver for details

        SocketPath = (TFsPath(GetSystemTempDir()) /
                      Sprintf("vhost.socket_%lu", RandomNumber<ui64>()))
                         .GetPath();

        InitLog(Logging);
        NVhost::InitLog(Logging);
        NVhost::StartServer();

        Fuse = std::make_shared<NVhost::TFuseVirtioClient>(
            SocketPath,
            WaitTimeout);

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
            return NThreading::MakeFuture(result);
        };

        Service->ResetSessionHandler = [] (auto, auto) {
            return NThreading::MakeFuture(NProto::TResetSessionResponse{});
        };

        Service->DestroySessionHandler = [] (auto, auto) {
            return NThreading::MakeFuture(NProto::TDestroySessionResponse());
        };

        Service->PingSessionHandler = [] (auto, auto) {
            return NThreading::MakeFuture(NProto::TPingSessionResponse());
        };

        auto sessionConfig = std::make_shared<NClient::TSessionConfig>(
            NProto::TSessionConfig{});
        Session = NClient::CreateSession(
            Logging,
            Timer,
            Scheduler,
            Service,
            std::move(sessionConfig));

        NProto::TVFSConfig proto;
        proto.SetDebug(true);
        proto.SetSocketPath(SocketPath);
        proto.SetFileSystemId(FileSystemId);
        // Keep the path configured to allow restoring
        // an existing HandleOpsQueue.
        proto.SetHandleOpsQueuePath(TempDir.Path() / "HandleOpsQueue");
        proto.SetHandleOpsQueueSize(handleOpsQueueStateFileSize);

        proto.SetDirectoryHandlesStoragePath(
            TempDir.Path() / "DirectoryHandles");
        DirectoryHandleStoragePath = proto.GetDirectoryHandlesStoragePath();
        if (featuresConfig.GetDirectoryHandlesStorageEnabled()) {
            if (directoryHandlesInitialDataSize) {
                proto.SetDirectoryHandlesInitialDataSize(
                    directoryHandlesInitialDataSize);
            }
            if (directoryHandlesMaxDataAreaStepSize) {
                proto.SetDirectoryHandlesMaxDataAreaStepSize(
                    directoryHandlesMaxDataAreaStepSize);
            }
        }

        // WriteBackCache should be configured even if it is disabled
        proto.SetWriteBackCachePath(TempDir.Path() / "WriteBackCache");
        proto.SetWriteBackCacheStateFileSize(writeBackCacheStateFileSize);
        proto.SetWriteBackCacheAutomaticFlushPeriod(
            writeBackCacheAutomaticFlushPeriodMs);

        auto config = std::make_shared<NVFS::TVFSConfig>(std::move(proto));
        if (!persistentStateManager) {
            persistentStateManager = CreatePersistentStateManager({
                .HandleOpsQueueBasePath = config->GetHandleOpsQueuePath(),
                .WriteBackCacheBasePath = config->GetWriteBackCachePath(),
                .DirectoryHandlesStorageBasePath =
                    config->GetDirectoryHandlesStoragePath(),
            });
        }

        Loop = NFuse::CreateFuseLoop(
            config,
            Logging,
            StatsRegistry,
            ModuleStatsRegistry,
            FsCountersProvider,
            Scheduler,
            Timer,
            CreateProfileLogStub(),
            Session,
            std::move(fileMapMemoryLimiter),
            std::move(persistentStateManager),
            std::move(multiFileSystemEventHandler));
    }

    NMonitoring::TDynamicCountersPtr GetFileSystemStatsCounters() const
    {
        return Counters
            ->FindSubgroup("component", TString{MetricsComponent} + "_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", FileSystemId)
            ->FindSubgroup("client", "")
            ->FindSubgroup("cloud", "")
            ->FindSubgroup("folder", "");
    }

    NMonitoring::TDynamicCountersPtr GetDirectoryHandleCounters() const
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

    NMonitoring::TDynamicCountersPtr GetHandleOpsQueueCounters() const
    {
        return Counters
            ->FindSubgroup("component", TString{MetricsComponent} + "_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", FileSystemId)
            ->FindSubgroup("client", "")
            ->FindSubgroup("cloud", "")
            ->FindSubgroup("folder", "")
            ->FindSubgroup("module", "HandleOpsQueue");
    }

    NMonitoring::TDynamicCountersPtr GetAggregateHandleOpsQueueCounters() const
    {
        return Counters
            ->FindSubgroup("component", TString{MetricsComponent})
            ->FindSubgroup("module", "HandleOpsQueue");
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
            auto init = Fuse->SendRequest<NVhost::TInitRequest>();
            UNIT_ASSERT_NO_EXCEPTION(init.GetValueSync());
        }
        return error;
    }

    NThreading::TFuture<NProto::TError> StartAsync()
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
                    return NThreading::MakeFuture<NProto::TError>(error);
                }

                Fuse->Init();
                return NThreading::MakeFuture<NProto::TError>();
            });
    }

    void Stop()
    {
        auto stop = StopAsync();
        StopTriggered.TrySetValue();

        // Scheduled tasks are not run automatically when TTestScheduler is used
        auto testScheduler = dynamic_cast<TTestScheduler*>(Scheduler.get());
        if (testScheduler) {
            auto now = TInstant::Now();
            while (!stop.HasValue() && !stop.HasException()) {
                UNIT_ASSERT(TInstant::Now() - now < WaitTimeout);
                testScheduler->RunAllScheduledTasks();
            }
        } else {
            UNIT_ASSERT(stop.Wait(WaitTimeout));
        }

        if (Scheduler) {
            // It is not allowed to stop Scheduler in the subscriber of
            // Loop->StopAsync() because the callback may be called from
            // a Scheduler thread
            Scheduler->Stop();
        }

        Fuse->DeInit();
        Loop = nullptr;
        std::remove(SocketPath.c_str());
    }

    NThreading::TFuture<void> StopAsync()
    {
        return Loop ? Loop->StopAsync() : NThreading::MakeFuture();
    }

    void InterruptNextRequest()
    {
        auto interrupt = Fuse->SendRequest<NVhost::TInterruptRequest>(
            Fuse->GetLastRequestId() + 2);
        UNIT_ASSERT_NO_EXCEPTION(interrupt.GetValueSync());
    };

    static TBootstrap CreateWithHandleStorage(
        ui64 directoryHandlesInitialDataSize = 0,
        ui64 directoryHandlesMaxDataAreaStepSize = 0,
        IFileMapMemoryLimiterPtr fileMapMemoryLimiter =
            CreateFileMapMemoryLimiterStub())
    {
        NProto::TFileStoreFeatures features;
        features.SetDirectoryHandlesStorageEnabled(true);
        return TBootstrap(
            CreateWallClockTimer(),
            CreateScheduler(),
            features,
            1000,
            1000,
            WriteBackCacheStateFileSize,
            directoryHandlesInitialDataSize,
            directoryHandlesMaxDataAreaStepSize,
            std::move(fileMapMemoryLimiter));
    }
};

inline NProto::TNodeAttr MakeNodeAttr(ui64 nodeId, ui32 nodeType)
{
    NProto::TNodeAttr attrs;
    attrs.SetId(nodeId);
    attrs.SetType(nodeType);
    attrs.SetMode(0644);
    return attrs;
}

}   // namespace NCloud::NFileStore::NFuse
