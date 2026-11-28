#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/filesystem_counters.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service_local/config.h>
#include <cloud/filestore/libs/service_local/service.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>
#include <cloud/filestore/libs/vfs_fuse/loop.h>
#include <cloud/filestore/libs/vfs_fuse/persistent_state_manager.h>
#include <cloud/filestore/libs/vhost/server.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/file_backed_containers/file_map_memory_limiter.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>

#include <pthread.h>

#include <csignal>
#include <filesystem>

namespace {

using namespace NCloud;
using namespace NCloud::NFileStore;

void CheckError(const NCloud::NProto::TError& error)
{
    Y_ENSURE(!HasError(error), FormatError(error));
}

// Own the components in shutdown order, including partially completed startup.
class TServer
{
private:
    ILoggingServicePtr Logging = CreateLoggingService("console", {TLOG_NOTICE});
    ITimerPtr Timer = CreateWallClockTimer();
    ISchedulerPtr Scheduler = CreateScheduler();
    ITaskQueuePtr Threads = CreateThreadPool("local-vfs", 8);
    IFileIOServicePtr FileIO = CreateAIOService();
    IFileStoreServicePtr Service;
    NVFS::IFileSystemLoopPtr Loop;
    std::filesystem::path SocketPath;
    bool LoggingStarted = false;
    bool SchedulerStarted = false;
    bool ThreadsStarted = false;
    bool FileIOStarted = false;
    bool VhostStarted = false;
    bool LoopStarted = false;

public:
    void Start(
        const std::filesystem::path& socket,
        const std::filesystem::path& shared,
        const std::filesystem::path& state)
    {
        Y_ENSURE(
            std::filesystem::is_directory(shared),
            "shared directory is not a directory");
        for (const auto* name:
             {"root", "sessions", "handles", "cache", "directories"})
        {
            std::filesystem::create_directories(state / name);
        }
        const auto root = state / "root" / "nfs_shared";
        if (std::filesystem::is_symlink(root)) {
            Y_ENSURE(
                std::filesystem::canonical(root) == shared,
                "socket state belongs to another shared directory");
        } else {
            std::filesystem::create_directory_symlink(shared, root);
        }

        NFileStore::NProto::TLocalServiceConfig local;
        local.SetRootPath((state / "root").string());
        local.SetStatePath((state / "sessions").string());
        local.SetPathPrefix("nfs_");
        local.SetOpenNodeByHandleEnabled(false);
        Service = CreateLocalFileStore(
            std::make_shared<TLocalFileStoreConfig>(local),
            Timer,
            Scheduler,
            Logging,
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            FileIO,
            Threads,
            CreateProfileLogStub());

        Logging->Start();
        LoggingStarted = true;
        Scheduler->Start();
        SchedulerStarted = true;
        Threads->Start();
        ThreadsStarted = true;
        FileIO->Start();
        FileIOStarted = true;
        Service->Start();
        NVhost::InitLog(Logging);
        NVhost::StartServer();
        VhostStarted = true;

        auto request =
            std::make_shared<NFileStore::NProto::TCreateFileStoreRequest>();
        request->SetFileSystemId("shared");
        request->SetCloudId("test");
        request->SetFolderId("test");
        request->SetBlockSize(4096);
        request->SetBlocksCount(26214400);
        CheckError(
            Service->CreateFileStore(MakeIntrusive<TCallContext>(), request)
                .GetValueSync()
                .GetError());

        NFileStore::NProto::TSessionConfig sessionConfig;
        sessionConfig.SetFileSystemId("shared");
        sessionConfig.SetClientId("virtiofs-shared");
        auto session = NClient::CreateSession(
            Logging,
            Timer,
            Scheduler,
            Service,
            std::make_shared<NClient::TSessionConfig>(sessionConfig));

        NFileStore::NProto::TVFSConfig vfs;
        vfs.SetFileSystemId("shared");
        vfs.SetClientId("virtiofs-shared");
        vfs.SetSocketPath(socket.string());
        vfs.SetHandleOpsQueuePath((state / "handles").string());
        vfs.SetWriteBackCachePath((state / "cache").string());
        vfs.SetDirectoryHandlesStoragePath((state / "directories").string());
        vfs.SetDirectoryHandlesInitialDataSize(1048576);
        auto persistentState = NFuse::CreatePersistentStateManager({
            .HandleOpsQueueBasePath = vfs.GetHandleOpsQueuePath(),
            .WriteBackCacheBasePath = vfs.GetWriteBackCachePath(),
            .DirectoryHandlesStorageBasePath =
                vfs.GetDirectoryHandlesStoragePath(),
        });
        Loop = NFuse::CreateFuseLoop(
            std::make_shared<NVFS::TVFSConfig>(vfs),
            Logging,
            CreateRequestStatsRegistryStub(),
            CreateModuleStatsRegistryStub(),
            CreateFsCountersProviderStub(),
            Scheduler,
            Timer,
            CreateProfileLogStub(),
            session,
            CreateFileMapMemoryLimiterStub(),
            persistentState,
            nullptr);
        CheckError(Loop->StartAsync().GetValueSync());
        LoopStarted = true;
        SocketPath = socket;
    }

    ~TServer()
    {
        // Suspend preserves sessions and handles for a subsequent QEMU
        // reconnect. Never destroy the filesystem: its root points into the
        // caller's tree.
        if (LoopStarted) {
            try {
                Loop->SuspendAsync().GetValueSync();
            } catch (...) {
                Cerr << "Failed to suspend endpoint: "
                     << CurrentExceptionMessage() << Endl;
            }
        }
        Loop.reset();
        if (LoopStarted) {
            std::error_code error;
            std::filesystem::remove(SocketPath, error);
            std::filesystem::remove(SocketPath.string() + ".pid", error);
        }
        if (Service) {
            Service->Stop();
        }
        if (FileIOStarted) {
            FileIO->Stop();
        }
        if (ThreadsStarted) {
            Threads->Stop();
        }
        if (VhostStarted) {
            NVhost::StopServer();
        }
        if (SchedulerStarted) {
            Scheduler->Stop();
        }
        if (LoggingStarted) {
            Logging->Stop();
        }
    }
};

}   // namespace

int main(int argc, char** argv)
{
    try {
        TString socketPath;
        TString sharedDir;
        TString stateDir;
        TString sandbox;
        NLastGetopt::TOpts opts;
        opts.AddHelpOption();
        opts.AddLongOption("socket-path", "Vhost-user socket")
            .RequiredArgument()
            .Required()
            .StoreResult(&socketPath);
        opts.AddLongOption("shared-dir", "Host directory to export")
            .RequiredArgument()
            .Required()
            .StoreResult(&sharedDir);
        opts.AddLongOption(
                "state-dir",
                "Persistent state (default: SOCKET.state)")
            .RequiredArgument()
            .StoreResult(&stateDir);
        opts.AddLongOption(
                "sandbox",
                "Compatibility option; only none is supported")
            .RequiredArgument()
            .DefaultValue("none")
            .StoreResult(&sandbox);
        opts.SetFreeArgsNum(0);
        NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);
        Y_ENSURE(sandbox == "none", "only --sandbox none is supported");

        // Block termination before starting workers; sigwait handles it in
        // main.
        sigset_t signals;
        sigemptyset(&signals);
        sigaddset(&signals, SIGTERM);
        sigaddset(&signals, SIGINT);
        Y_ENSURE(pthread_sigmask(SIG_BLOCK, &signals, nullptr) == 0);
        signal(SIGUSR1, SIG_IGN);   // Used by the FUSE driver during shutdown.
        const auto socket = std::filesystem::absolute(socketPath.c_str());
        const auto shared = std::filesystem::canonical(sharedDir.c_str());
        const auto state =
            stateDir.empty() ? std::filesystem::path(socket.string() + ".state")
                             : std::filesystem::absolute(stateDir.c_str());
        TServer server;
        server.Start(socket, shared, state);
        int receivedSignal = 0;
        Y_ENSURE(sigwait(&signals, &receivedSignal) == 0);
        return 0;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
