#include "public.h"

#include "fs.h"
#include "loop.h"
#include "vfs_vhost.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>
#include <cloud/filestore/libs/vhost/client.h>
#include <cloud/filestore/libs/vhost/request.h>
#include <cloud/filestore/libs/vhost/server.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/random/random.h>

namespace NCloud::NFileStore::NVFSVhost {

using namespace NThreading;

using namespace NCloud::NFileStore::NClient;
using namespace NCloud::NFileStore::NVFS;
using namespace NCloud::NFileStore::NVhost;

using namespace NCloud::NStorage::NUserStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FileSystemId = "fs";
constexpr TDuration WaitTimeout = TDuration::Seconds(5);

/*
TString CreateBuffer(size_t len, char fill = 0)
{
    return TString(len, fill);
}
*/

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemTest final
    : public IFileSystem
{
    std::function<TFuture<NProto::TError>()> StartHandler;
    std::function<TFuture<NProto::TError>()> StopHandler;
    std::function<TFuture<NProto::TError>(bool, ui64)> AlterHandler;
    std::function<TFuture<NProto::TError>(TVfsRequestPtr)> ProcessHandler;

    TFuture<NProto::TError> Start() override
    {
        if (StartHandler) {
            return StartHandler();
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Stop() override
    {
        if (StopHandler) {
            return StopHandler();
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Alter(
        bool readOnly,
        ui64 mountSeqNumber) override
    {
        if (AlterHandler) {
            return AlterHandler(readOnly, mountSeqNumber);
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Process(
        TVfsRequestPtr request) override
    {
        if (ProcessHandler) {
            return ProcessHandler(request);
        }

        return MakeFuture<NProto::TError>();
    }
};

struct TFileSystemTestFactory
    : public IFileSystemFactory
{
    std::shared_ptr<TFileSystemTest> FileSystem;

    TFileSystemTestFactory(std::shared_ptr<TFileSystemTest> fileSystem)
        : FileSystem(std::move(fileSystem))
    {}

    IFileSystemPtr Create(
        NVFS::TVFSConfigPtr,
        NClient::ISessionPtr,
        ISchedulerPtr,
        IRequestStatsRegistryPtr,
        IProfileLogPtr) override
    {
        return FileSystem;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<TFuseVirtioClient> VirtioClient;

    const ILoggingServicePtr Logging;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const NMonitoring::TDynamicCountersPtr Counters;
    const IRequestStatsRegistryPtr StatsRegistry;

    ISessionPtr Session;
    std::shared_ptr<TFileStoreTest> Service;
    std::shared_ptr<TFileSystemTest> FileSystem;

    IFileSystemLoopFactoryPtr LoopFactory;
    IFileSystemLoopPtr Loop;

    TString SocketPath;

    TBootstrap(
            ITimerPtr timer = CreateWallClockTimer(),
            ISchedulerPtr scheduler = CreateScheduler())
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
        // see fuse/driver for details
        signal(SIGUSR1, SIG_IGN);

        SocketPath = (
            TFsPath(GetSystemTempDir()) /
            Sprintf("vhost.socket_%lu", RandomNumber<ui64>())).GetPath();

        InitLog(Logging);
        NVhost::InitLog(Logging);
        NVhost::StartServer();

        VirtioClient = std::make_shared<TFuseVirtioClient>(
            SocketPath,
            WaitTimeout);

        Service = std::make_shared<TFileStoreTest>();
        Service->CreateSessionHandler = [=] (auto callContext, auto request) {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->GetRestoreClientSession());
            NProto::TCreateSessionResponse result;
            result.MutableSession()->SetSessionId(CreateGuidAsString());
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
        proto.SetFileSystemId(TString(FileSystemId));

        FileSystem = std::make_shared<TFileSystemTest>();

        LoopFactory = CreateVfsLoopFactory(
            TLoopFactoryConfig{1, {}},
            CreateVhostQueueFactory(Logging),
            std::make_shared<TFileSystemTestFactory>(FileSystem),
            Logging,
            Scheduler,
            StatsRegistry,
            CreateProfileLogStub());

        auto config = std::make_shared<TVFSConfig>(std::move(proto));
        Loop = LoopFactory->Create(
            std::move(config),
            Session);

        auto error = Loop->StartAsync().GetValue(WaitTimeout);
        UNIT_ASSERT(!HasError(error));

        VirtioClient->Init();
    }

    ~TBootstrap()
    {
        Loop->StopAsync().Wait(WaitTimeout);
        VirtioClient->DeInit();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVfsVhostLoopTest)
{
    Y_UNIT_TEST(ShouldHandleInitRequest)
    {
        TBootstrap bootstrap;

        bootstrap.FileSystem->ProcessHandler = [=] (TVfsRequestPtr) mutable
        {
            return MakeFuture<NProto::TError>();
        };

        auto init = bootstrap.VirtioClient->SendRequest<TInitRequest>();
        UNIT_ASSERT_NO_EXCEPTION(init.GetValue(WaitTimeout));
    }
}

}   // namespace NCloud::NFileStore::NFuse
