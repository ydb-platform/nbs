#include "external_vhost_server.h"
#include "external_endpoint_stats.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>

#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/condvar.h>
#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <chrono>

#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TChild
{
    pid_t Pid = 0;
    TFileHandle StdOut;
    TFileHandle StdErr;

public:
    TChild() = default;

    TChild(pid_t pid, TFileHandle stdOut, TFileHandle stdErr)
        : Pid {pid}
        , StdOut {std::move(stdOut)}
        , StdErr {std::move(stdErr)}
    {}

    TChild(const TChild&) = delete;
    TChild& operator = (const TChild&) = delete;

    TChild(TChild&& rhs) noexcept
    {
        Swap(rhs);
    }

    TChild& operator = (TChild&& rhs) noexcept
    {
        Swap(rhs);

        return *this;
    }

    void Swap(TChild& rhs) noexcept
    {
        std::swap(Pid, rhs.Pid);
        StdOut.Swap(rhs.StdOut);
        StdErr.Swap(rhs.StdErr);
    }

    int Kill(int sig) noexcept
    {
        return ::kill(Pid, sig);
    }

    void Terminate() noexcept
    {
        Kill(SIGTERM);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPipe
{
    TFileHandle R;
    TFileHandle W;

    TPipe()
    {
        int fds[2] {};
        Y_ENSURE(!::pipe2(fds, O_CLOEXEC | O_NONBLOCK));
        TFileHandle r {fds[0]};
        TFileHandle w {fds[1]};

        r.Swap(R);
        w.Swap(W);
    }

    void LinkTo(int fd)
    {
        R.Close();

        TFileHandle h {fd};
        Y_SCOPE_EXIT(&h) { h.Release(); };

        h.LinkTo(W);
    }
};

////////////////////////////////////////////////////////////////////////////////

TChild SpawnChild(
    const TString& binaryPath,
    TVector<TString> args)
{
    TPipe stdOut;
    TPipe stdErr;

    pid_t childPid = ::fork();

    if (childPid == -1) {
        int err = errno;
        char buf[64] {};
        ythrow TServiceError {MAKE_SYSTEM_ERROR(err)}
            << "fork error: " << ::strerror_r(err, buf, sizeof(buf));
    }

    if (childPid) {
        return TChild {childPid, std::move(stdOut.R), std::move(stdErr.R)};
    }

    // child process

    stdOut.LinkTo(STDOUT_FILENO);
    stdErr.LinkTo(STDERR_FILENO);

    // Following "const_cast"s are safe:
    // http://pubs.opengroup.org/onlinepubs/9699919799/functions/exec.html

    TVector<char*> qargs;
    qargs.reserve(args.size() + 2);

    qargs.push_back(const_cast<char*>(binaryPath.data()));
    for (auto& arg: args) {
        qargs.push_back(const_cast<char*>(arg.data()));
    }
    qargs.emplace_back();

    ::execvp(binaryPath.c_str(), qargs.data());

    int err = errno;
    char buf[64] {};
    Y_FAIL("Process was not created: %s", ::strerror_r(err, buf, sizeof(buf)));
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError WaitPid(pid_t pid)
{
    int status = 0;

    if (::waitpid(pid, &status, 0) == -1) {
        int err = errno;
        return MakeError(
            MAKE_SYSTEM_ERROR(err),
            TStringBuilder() << "waitpid: " << ::strerror(err));
    }

    if (WIFSIGNALED(status)) {
        const char* message = WCOREDUMP(status)
            ? "core dump"
            : "terminated by a signal";

        return MakeError(MAKE_SYSTEM_ERROR(WTERMSIG(status)), message);
    }

    if (WIFEXITED(status) && WEXITSTATUS(status)) {
        return MakeError(MAKE_SYSTEM_ERROR(WEXITSTATUS(status)));
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TEndpointProcess final
    : public TThrRefBase
{
private:
    const ILoggingServicePtr Logging;
    const TString ClientId;

    TLog Log;

    TEndpointStats Stats;

    TChild Process;

    std::atomic_bool ShouldStop = false;

public:
    TEndpointProcess(
            TString clientId,
            ILoggingServicePtr logging,
            TEndpointStats stats,
            TChild process)
        : Logging {std::move(logging)}
        , ClientId {std::move(clientId)}
        , Log {Logging->CreateLog("BLOCKSTORE_EXTERNAL_ENDPOINT")}
        , Stats {std::move(stats)}
        , Process {std::move(process)}
    {}

    ~TEndpointProcess()
    {
        if (!ShouldStop) {
            Process.Terminate();
        }
    }

    void Start(TContExecutor* e)
    {
        e->Create<TEndpointProcess, &TEndpointProcess::ReadStats>(this, "stats");
        e->Create<TEndpointProcess, &TEndpointProcess::ReadStdErr>(this, "err");
    }

    void Stop()
    {
        ShouldStop = true;
        Process.Kill(SIGINT);
    }

    NProto::TError Wait()
    {
        return WaitPid(Process.Pid);
    }

private:
    void ReadStats(TCont* c)
    {
        TIntrusivePtr<TEndpointProcess> holder {this};

        try {
            ReadStatsImpl(c);
        } catch (...) {
            const auto logPriority = ShouldStop
                ? TLOG_INFO
                : TLOG_ERR;

            STORAGE_LOG(logPriority, "[" << ClientId << "] Read stats error: "
                << CurrentExceptionMessage());
        }
    }

    void ReadStatsImpl(TCont* c)
    {
        TContIO io {Process.StdOut, c};

        for (;;) {
            c->SleepT(1s);

            if (ShouldStop) {
                break;
            }

            Process.Kill(SIGUSR1);

            NJson::TJsonValue stats;
            NJson::ReadJsonTree(io.ReadLine(), &stats, true);

            STORAGE_DEBUG("[" << ClientId << "] " << stats);

            Stats.Update(stats);
        }
    }

    void ReadStdErr(TCont* c)
    {
        TIntrusivePtr<TEndpointProcess> holder {this};

        try {
            ReadStdErrImpl(c);
        } catch (...) {
            const auto logPriority = ShouldStop
                ? TLOG_INFO
                : TLOG_ERR;

            STORAGE_LOG(logPriority, "[" << ClientId << "] Read stderr error: "
                << CurrentExceptionMessage());
        }
    }

    void ReadStdErrImpl(TCont* c)
    {
        char buf[1024] {};
        TString incompleteLine;

        for (;;) {
            const auto r = NCoro::ReadI(c, Process.StdErr, buf, sizeof(buf));
            const size_t len = r.Processed();

            if (!len) {
                break;
            }

            TStringBuf s {buf, len};
            for (;;) {
                size_t i = s.find('\n');
                if (i == s.npos) {
                    incompleteLine += s;
                    break;
                }

                incompleteLine += s.substr(0, i);

                WriteLog(incompleteLine);

                incompleteLine.clear();
                s.remove_prefix(i + 1);
            }
        }

        if (!incompleteLine.empty()) {
            WriteLog(incompleteLine);
        }
    }

    void WriteLog(TStringBuf line)
    {
        NJson::TJsonValue logRec;
        if (!NJson::ReadJsonTree(line, &logRec, false)) {
            STORAGE_ERROR("[" << ClientId << "] bad json: " << line);
            return;
        }

        long long logPriority = TLOG_INFO;
        logRec["priority"].GetInteger(&logPriority);

        STORAGE_LOG(logPriority, "[" << ClientId << "] "
            << logRec["message"].GetString());
    }
};

////////////////////////////////////////////////////////////////////////////////

void AddToCGroups(pid_t pid, const TVector<TString>& cgroups)
{
    const auto flags =
          EOpenModeFlag::OpenExisting
        | EOpenModeFlag::WrOnly
        | EOpenModeFlag::ForAppend;

    const TString line = ToString(pid) + '\n';

    for (auto& cgroup: cgroups) {
        TFile file {TFsPath {cgroup} / "cgroup.procs", flags};

        file.Write(line.data(), line.size());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TEndpoint final
    : public TThrRefBase
    , public ISimpleThread  // XXX: pidfd_open is not implemented for Linux 4.14
{                           // so we are forced to use a thread for waitpid.
private:
    const ILoggingServicePtr Logging;
    const TExecutorPtr Executor;
    const TString ClientId;
    const TString BinaryPath;
    const TVector<TString> Args;
    const TVector<TString> Cgroups;

    TLog Log;

    TEndpointStats Stats;

    TMutex Mutex;

    TIntrusivePtr<TEndpointProcess> Process;
    std::atomic_bool ShouldStop = false;

    TPromise<NProto::TError> StopPromise = NewPromise<NProto::TError>();

public:
    TEndpoint(
            TString clientId,
            ILoggingServicePtr logging,
            TExecutorPtr executor,
            TEndpointStats stats,
            TString binaryPath,
            TVector<TString> args,
            TVector<TString> cgroups)
        : Logging {std::move(logging)}
        , Executor {std::move(executor)}
        , ClientId {std::move(clientId)}
        , BinaryPath {std::move(binaryPath)}
        , Args {std::move(args)}
        , Cgroups {std::move(cgroups)}
        , Log {Logging->CreateLog("BLOCKSTORE_EXTERNAL_ENDPOINT")}
        , Stats {std::move(stats)}
    {}

    ~TEndpoint()
    {
        Y_VERIFY_DEBUG(ShouldStop);
    }

    void Run()
    {
        Process = StartProcess();

        ISimpleThread::Start();
    }

    TFuture<NProto::TError> Stop()
    {
        with_lock (Mutex) {
            Y_VERIFY_DEBUG(Process);

            ShouldStop = true;
            Process->Stop();
        }

        return StopPromise;
    }

    void* ThreadProc() override
    {
        TIntrusivePtr<TEndpoint> holder {this};

        Detach();   // don't call Join in the same thread

        ::NCloud::SetCurrentThreadName("waitEP");

        NProto::TError error;

        for (;;) {
            error = Process->Wait();

            STORAGE_INFO("[" << ClientId << "] Child stopped: "
                << FormatError(error));

            if (ShouldStop) {
                break;
            }

            // TODO: limiter

            auto process = RestartProcess();
            if (!process) {
                break;
            }

            with_lock (Mutex) {
                if (ShouldStop) {
                    process->Stop();
                }

                Process = std::move(process);
            }
        }

        StopPromise.SetValue(error);

        return nullptr;
    }

    TIntrusivePtr<TEndpointProcess> StartProcess()
    {
        auto process = SpawnChild(BinaryPath, Args);
        Y_SCOPE_EXIT(&process) {
            if (process.Pid) {
                process.Terminate();
            }
        };

        AddToCGroups(process.Pid, Cgroups);

        auto ep = MakeIntrusive<TEndpointProcess>(
            ClientId,
            Logging,
            Stats,
            std::move(process));

        Executor->ExecuteSimple([=] {
            ep->Start(Executor->GetContExecutor());
        });

        return ep;
    }

    TIntrusivePtr<TEndpointProcess> RestartProcess()
    {
        STORAGE_WARN("[" << ClientId << "] Restart external endpoint");

        try {
            return StartProcess();
        } catch (...) {
            STORAGE_ERROR("[" << ClientId << "] Can't restart external endpoint: "
                << CurrentExceptionMessage());
        }

        return nullptr;
    }
};

using TEndpointPtr = TIntrusivePtr<TEndpoint>;

////////////////////////////////////////////////////////////////////////////////

class TExternalVhostEndpointListener final
    : public IEndpointListener
{
private:
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const TExecutorPtr Executor;
    const TString BinaryPath;
    const TString LocalAgentId;
    const IEndpointListenerPtr FallbackListener;

    TLog Log;

    THashMap<TString, TEndpointPtr> Endpoints;

public:
    TExternalVhostEndpointListener(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            TString binaryPath,
            TString localAgentId,
            IEndpointListenerPtr fallbackListener)
        : Logging {std::move(logging)}
        , ServerStats {std::move(serverStats)}
        , Executor {std::move(executor)}
        , BinaryPath {std::move(binaryPath)}
        , LocalAgentId {std::move(localAgentId)}
        , FallbackListener {std::move(fallbackListener)}
        , Log {Logging->CreateLog("BLOCKSTORE_SERVER")}
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        if (!TryStartExternalEndpoint(request, volume)) {
            return FallbackListener->StartEndpoint(
                request,
                volume,
                std::move(session));
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override
    {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return FallbackListener->StopEndpoint(socketPath);
        }

        auto ep = std::move(it->second);

        auto future = ep->Stop();

        Endpoints.erase(it);

        return future;
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        // TODO: NBS-4151
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return {};
    }

private:
    bool TryStartExternalEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume)
    {
        if (volume.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD_LOCAL
                || request.GetVolumeMountMode() != NProto::VOLUME_MOUNT_LOCAL
                || request.GetIpcType() != NProto::IPC_VHOST)
        {
            return false;
        }

        if (!ValidateLocation(volume)) {
            return false;
        }

        try {
            StartExternalEndpoint(request, volume);

            return true;

        } catch (...) {
            STORAGE_ERROR("Can't run external endpoint: "
                << CurrentExceptionMessage());
        }

        return false;
    }

    void StartExternalEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume)
    {
        const auto& socketPath = request.GetUnixSocketPath();
        const auto& deviceName = request.GetDeviceName()
            ? request.GetDeviceName()
            : volume.GetDiskId();

        TVector<TString> args {
            "-i", deviceName,
            "-s", socketPath,
            "-q", ToString(request.GetVhostQueuesCount())
        };

        for (const auto& device: volume.GetDevices()) {
            const ui64 size = device.GetBlockCount() * volume.GetBlockSize();
            args.insert(args.end(), {
                "-b", TStringBuilder() << device.GetDeviceName() << ":" << size
            });
        }

        if (request.GetVolumeAccessMode() == NProto::VOLUME_ACCESS_READ_ONLY) {
            args.emplace_back("-r");
        }

        TVector<TString> cgroups(
            request.GetClientCGroups().begin(),
            request.GetClientCGroups().end()
        );

        const auto& clientId = request.GetClientId()
            ? request.GetClientId()
            : request.GetInstanceId();

        auto ep = MakeIntrusive<TEndpoint>(
            clientId,
            Logging,
            Executor,
            TEndpointStats {
                .ClientId = clientId,
                .DiskId = request.GetDiskId(),
                .ServerStats = ServerStats
            },
            BinaryPath,
            std::move(args),
            std::move(cgroups)
        );

        ep->Run();

        Endpoints[socketPath] = std::move(ep);
    }

    bool ValidateLocation(const NProto::TVolume& volume) const
    {
        if (LocalAgentId.empty()) {
            return true;
        }

        for (const auto& device: volume.GetDevices()) {
            if (device.GetAgentId() != LocalAgentId) {
                STORAGE_WARN("Device "
                    << device.GetDeviceUUID().Quote()
                    << " with non local agent id: "
                    << device.GetAgentId().Quote());

                return false;
            }
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString binaryPath,
    TString localAgentId,
    IEndpointListenerPtr fallbackListener)
{
    return std::make_shared<TExternalVhostEndpointListener>(
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        std::move(binaryPath),
        std::move(localAgentId),
        std::move(fallbackListener));
}

}   // namespace NCloud::NBlockStore::NServer
