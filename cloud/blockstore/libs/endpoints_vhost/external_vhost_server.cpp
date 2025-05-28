#include "external_vhost_server.h"

#include "cont_io_with_timeout.h"
#include "external_endpoint_stats.h"

#include <cloud/blockstore/libs/common/device_path.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/encryption/model/utils.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/vhost-server/options.h>
#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/coroutine/engine/condvar.h>
#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/condvar.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <thread>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

// When the process starts, there may be a delay until it can process the
// signals. Therefore, it is possible that the child process will miss our first
// SIGUSR1 signal. When requesting the first block of statistics, it is
// necessary to make a reasonable number of repetitions.
constexpr ui64 ReadFirstStatRetryCount = 10;

// It doesn't make sense to make the delay less than
// COMPLETION_STATS_WAIT_DURATION.
constexpr auto StatReadDuration = TDuration::Seconds(1);

// Backoff delays for external-vhost server restart.
constexpr auto RestartMinDelay = TDuration::MilliSeconds(100);
constexpr auto RestartMaxDelay = TDuration::Seconds(30);
constexpr auto RestartWasTooLongAgo = TDuration::Seconds(60);

enum class EEndpointType
{
    Local,
    Rdma,
    Fallback
};

const char* GetEndpointTypeName(EEndpointType state)
{
    static const char* names[] = {
        "LOCAL",
        "RDMA",
        "FALLBACK",
    };

    if ((size_t)state < Y_ARRAY_SIZE(names)) {
        return names[(size_t)state];
    }
    return "UNDEFINED";
}

TString ReadFromFile(const TString& fileName)
{
    try {
        return TFileInput(fileName).ReadAll();
    } catch (...) {
        return {};
    }
}

TString ParseDiskIdFromCmdLine(const TString& cmdLine)
{
    // Prepare argv from string with zero-separated params.
    // Flags AllowUnknownCharOptions_ and AllowUnknownCharOptions_ force parser
    // to ignore all of the following parameters if it encounters an unknown
    // parameter. Therefore, we discard all the parameters that are in front of
    // the --disk-id.
    std::vector<const char*> argv;
    size_t diskIdOffset = cmdLine.find("--disk-id");
    if (diskIdOffset == TString::npos || diskIdOffset == 0) {
        return {};
    }

    argv.push_back("dummy");
    argv.push_back(&cmdLine[diskIdOffset]);
    for (size_t i = diskIdOffset + 1; i < cmdLine.size(); ++i) {
        if (cmdLine[i] != 0 && cmdLine[i - 1] == 0) {
            argv.push_back(&cmdLine[i]);
            break;
        }
    }
    argv.push_back(nullptr);

    // Parse command line to get disk-id.
    TString diskId;
    NLastGetopt::TOpts opts;
    opts.AddLongOption("disk-id").StoreResult(&diskId);

    opts.AllowUnknownCharOptions_ = true;
    opts.AllowUnknownLongOptions_ = true;

    NLastGetopt::TOptsParseResult parsedOpts(&opts, argv.size(), argv.data());
    return diskId;
}

TString FindDiskIdForRunningProcess(int pid)
{
    TFsPath proc("/proc");
    TString processCmd = ReadFromFile(proc / ToString(pid) / "cmdline");
    if (!processCmd) {
        return {};
    }
    try {
        return ParseDiskIdFromCmdLine(processCmd);
    } catch (...) {
        return {};
    }
}

bool PrefetchBinaryToCache(const TString& binaryPath)
{
    try {
        TFile binary(
            binaryPath,
            EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly |
                EOpenModeFlag::Seq);

        binary.PrefetchCache(0, 0, true);
        return true;
    } catch (...) {
        // Just ignore
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TChild
{
    pid_t Pid = 0;
    TFileHandle StdOut;
    TFileHandle StdErr;

public:
    explicit TChild(pid_t pid)
        : Pid{pid}
    {}

    TChild(pid_t pid, TFileHandle stdOut, TFileHandle stdErr)
        : Pid{pid}
        , StdOut{std::move(stdOut)}
        , StdErr{std::move(stdErr)}
    {}

    TChild(const TChild&) = delete;
    TChild& operator=(const TChild&) = delete;

    TChild(TChild&& rhs) noexcept
    {
        Swap(rhs);
    }

    TChild& operator=(TChild&& rhs) noexcept
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

    int SendSignal(int sig) const noexcept
    {
        return ::kill(Pid, sig);
    }

    void Terminate() const noexcept
    {
        SendSignal(SIGTERM);
    }

    void Kill() const noexcept
    {
        SendSignal(SIGKILL);
    }

    NProto::TError Wait() const
    {
        int status = 0;

        if (::waitpid(Pid, &status, 0) == -1) {
            int err = errno;
            return MakeError(
                MAKE_SYSTEM_ERROR(err),
                TStringBuilder() << "waitpid: " << ::strerror(err));
        }

        if (WIFSIGNALED(status)) {
            const char* message =
                WCOREDUMP(status) ? "core dump" : "terminated by a signal";

            return MakeError(MAKE_SYSTEM_ERROR(WTERMSIG(status)), message);
        }

        if (WIFEXITED(status) && WEXITSTATUS(status)) {
            return MakeError(MAKE_SYSTEM_ERROR(WEXITSTATUS(status)));
        }

        return {};
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

        h.LinkTo(W);
        h.Release();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChild SpawnChild(
    const TString& binaryPath,
    TVector<TString> args)
{
    TPipe stdOut;
    TPipe stdErr;

    // Make allocations before the fork() call. In rare cases, the tcmalloc can
    // deadlock on allocation because other threads have locked mutexes that
    // would never be unlocked since the threads that placed the locks are not
    // duplicated in the child.
    TVector<char*> qargs;
    qargs.reserve(args.size() + 2);

    pid_t childPid = ::fork();

    // WARNING: Don't make heap allocations here!
    {
        if (childPid == -1) {
            int err = errno;
            char buf[64]{};
            ythrow TServiceError{MAKE_SYSTEM_ERROR(err)}
                << "fork error: " << ::strerror_r(err, buf, sizeof(buf));
        }

        if (childPid) {
            // Parent process.
            return TChild{childPid, std::move(stdOut.R), std::move(stdErr.R)};
        }

        // child process

        stdOut.LinkTo(STDOUT_FILENO);
        stdErr.LinkTo(STDERR_FILENO);

        // Last chance to figure out what's going on.
        // freopen((TString("/tmp/out.") + ToString(::getpid())).c_str(), "w",
        // stdout); freopen((TString("/tmp/err.") +
        // ToString(::getpid())).c_str(), "w", stderr);

        // Following "const_cast"s are safe:
        // http://pubs.opengroup.org/onlinepubs/9699919799/functions/exec.html
        qargs.push_back(const_cast<char*>(binaryPath.data()));
        for (auto& arg: args) {
            qargs.push_back(const_cast<char*>(arg.data()));
        }
        qargs.emplace_back();

        ::execvp(binaryPath.c_str(), qargs.data());
    }

    int err = errno;
    char buf[64] {};
    Y_ABORT("Process was not created: %s", ::strerror_r(err, buf, sizeof(buf)));
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

    ~TEndpointProcess() override
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
        Process.SendSignal(SIGINT);
    }

    NProto::TError Wait()
    {
        return Process.Wait();
    }

private:
    void ReadStats(TCont* c)
    {
        TIntrusivePtr<TEndpointProcess> holder{this};
        // Since it is not guaranteed that the vhost-server will receive first
        // signal to generate statistics, we need to read the response with a
        // timeout.
        TContIOWithTimeout io{Process.StdOut, c, StatReadDuration * 2};

        bool firstRead = true;
        for (ui64 i = 0; !ShouldStop; ++i) {
            const auto startReadStatAt = TInstant::Now();

            try {
                ReadStatsImpl(io);
                firstRead = false;
            } catch (...) {
                const bool shouldContinue = HandleReadStatsError(firstRead, i);
                if (!shouldContinue) {
                    break;
                }
            }

            auto elapsedTime = TInstant::Now() - startReadStatAt;
            if (elapsedTime < StatReadDuration) {
                c->SleepT(StatReadDuration - elapsedTime);
            }
        }
    }

    bool HandleReadStatsError(bool firstRead, ui64 i)
    {
        if (ShouldStop) {
            STORAGE_INFO(
                "[" << ClientId
                    << "] Read stats error: " << CurrentExceptionMessage());
            return false;
        }

        if (firstRead) {
            if (i < ReadFirstStatRetryCount) {
                STORAGE_WARN(
                    "[" << ClientId << "] Retrying read first stat #" << i + 1
                        << ". Error: " << CurrentExceptionMessage());
                return true;
            }
            STORAGE_ERROR(
                "[" << ClientId << "] Stop retrying read first stat. Error: "
                    << CurrentExceptionMessage());
            return false;
        }

        STORAGE_ERROR(
            "[" << ClientId << "] Stop read stat. On #" << i
                << ". Error: " << CurrentExceptionMessage());
        return false;
    }

    void ReadStatsImpl(TContIOWithTimeout& io)
    {
        Process.SendSignal(SIGUSR1);

        NJson::TJsonValue stats;
        NJson::ReadJsonTree(io.ReadLine(), &stats, true);

        STORAGE_DEBUG("[" << ClientId << "] " << stats);

        Stats.Update(stats);
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

        STORAGE_LOG(
            logPriority,
            "vhost-server[" << Process.Pid << "] [" << ClientId << "] "
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
    : public IExternalEndpoint
    , public std::enable_shared_from_this<TEndpoint>
{
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
    TInstant LastRestartAt;
    TBackoffDelayProvider RestartBackoff{RestartMinDelay, RestartMaxDelay};

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

    ~TEndpoint() override
    {
        Y_DEBUG_ABORT_UNLESS(ShouldStop);
    }

    void PrepareToStart() override
    {
        const auto startAt = TInstant::Now();
        const bool succ = PrefetchBinaryToCache(BinaryPath);
        const auto logPriority = succ ? TLOG_INFO : TLOG_ERR;

        STORAGE_LOG(
            logPriority,
            "Prefetch binary " << (succ ? "success" : "failed") << ". It took "
                               << (TInstant::Now() - startAt).ToString());
    }

    void Start() override
    {
        Process = StartProcess();
        std::thread(&TEndpoint::ThreadProc, shared_from_this()).detach();
    }

    TFuture<NProto::TError> Stop() override
    {
        with_lock (Mutex) {
            Y_DEBUG_ABORT_UNLESS(Process);

            ShouldStop = true;
            Process->Stop();
        }

        return StopPromise;
    }

private:
    // XXX: pidfd_open is not implemented for Linux 4.14
    // so we are forced to use a thread for waitpid.
    void ThreadProc()
    {
        ::NCloud::SetCurrentThreadName("waitEP");

        NProto::TError error;

        for (;;) {
            error = Process->Wait();

            STORAGE_INFO("[" << ClientId << "] Child stopped: "
                << FormatError(error));

            if (ShouldStop) {
                break;
            }

            ReportExternalEndpointUnexpectedExit(TStringBuilder()
                << "External endpoint for a disk " << Stats.DiskId.Quote()
                << " and a client " << Stats.ClientId.Quote()
                << " unexpectedly stopped: " << FormatError(error));

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

        // We must call "SetValue()" on coroutine thread, since it will trigger
        // future handlers synchronously.
        Executor->ExecuteSimple([promise = this->StopPromise, error]() mutable
                                { promise.SetValue(error); });
    }

    TIntrusivePtr<TEndpointProcess> StartProcess()
    {
        auto process = SpawnChild(BinaryPath, Args);

        STORAGE_INFO(
            "[" << ClientId
                << "] Endpoint process has been started, PID:" << process.Pid);

        Y_SCOPE_EXIT(&process) {
            if (process.Pid) {
                process.Terminate();
            }
        };

        try {
            AddToCGroups(process.Pid, Cgroups);
        } catch (...) {
            ShouldStop = true;
            throw;
        }

        auto ep = MakeIntrusive<TEndpointProcess>(
            ClientId,
            Logging,
            Stats,
            std::move(process));
        process = TChild{0};

        Executor->ExecuteSimple([=, this] {
            ep->Start(Executor->GetContExecutor());
        });

        return ep;
    }

    TIntrusivePtr<TEndpointProcess> RestartProcess()
    {
        if (TInstant::Now() - LastRestartAt > RestartWasTooLongAgo) {
            // The last restart happened a long time ago, restart immediately.
            RestartBackoff.Reset();
        } else {
            auto delay = RestartBackoff.GetDelayAndIncrease();
            STORAGE_WARN(
                "[" << ClientId << "] Will restart external endpoint after "
                    << delay.ToString());
            Sleep(delay);
        }

        if (ShouldStop) {
            return nullptr;
        }

        STORAGE_WARN("[" << ClientId << "] Restart external endpoint");

        try {
            LastRestartAt = TInstant::Now();
            return StartProcess();
        } catch (...) {
            STORAGE_ERROR("[" << ClientId << "] Can't restart external endpoint: "
                << CurrentExceptionMessage());
        }

        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TExternalVhostEndpointListener final
    : public std::enable_shared_from_this<TExternalVhostEndpointListener>
    , public IEndpointListener
{
private:
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const TExecutorPtr Executor;
    const TString LocalAgentId;
    const ui32 SocketAccessMode;
    const bool IsAlignedDataEnabled;
    const IEndpointListenerPtr FallbackListener;
    const TExternalEndpointFactory EndpointFactory;
    const TDuration VhostServerTimeoutAfterParentExit;

    TLog Log;

    THashMap<TString, IExternalEndpointPtr> Endpoints;
    TMap<i32, TString> OldEndpoints;

public:
    TExternalVhostEndpointListener(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            TString localAgentId,
            ui32 socketAccessMode,
            TDuration vhostServerTimeoutAfterParentExit,
            bool isAlignedDataEnabled,
            IEndpointListenerPtr fallbackListener,
            TExternalEndpointFactory endpointFactory)
        : Logging {std::move(logging)}
        , ServerStats {std::move(serverStats)}
        , Executor {std::move(executor)}
        , LocalAgentId {std::move(localAgentId)}
        , SocketAccessMode {socketAccessMode}
        , IsAlignedDataEnabled(isAlignedDataEnabled)
        , FallbackListener {std::move(fallbackListener)}
        , EndpointFactory {std::move(endpointFactory)}
        , VhostServerTimeoutAfterParentExit{vhostServerTimeoutAfterParentExit}
        , Log {Logging->CreateLog("BLOCKSTORE_SERVER")}
    {
        FindRunningEndpoints();
    }

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        const auto& socketPath = request.GetUnixSocketPath();

        if (Endpoints.contains(socketPath)) {
            return MakeFuture(MakeError(E_ARGUMENT, TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " has already been started"));
        }

        if (!TryStartExternalEndpoint(request, volume)) {
            Endpoints.emplace(socketPath, nullptr);

            STORAGE_INFO("starting endpoint "
                << request.GetUnixSocketPath() << ", epType=FALLBACK");

            return FallbackListener->StartEndpoint(
                request,
                volume,
                std::move(session));
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        const auto& socketPath = request.GetUnixSocketPath();

        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return MakeFuture(MakeError(E_NOT_FOUND, TStringBuilder()
                << "endpoint " << socketPath.Quote() << " not found"));
        }

        if (!it->second && !CanStartExternalEndpoint(request, volume)) {
            return FallbackListener->AlterEndpoint(request, volume, std::move(session));
        }

        return TrySwitchEndpoint(request, volume, session);
    }

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override
    {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return MakeFuture(MakeError(S_ALREADY, TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " has already been stopped"));
        }

        auto ep = std::move(it->second);
        Endpoints.erase(it);

        if (ep) {
            return ep->Stop();
        }

        return FallbackListener->StopEndpoint(socketPath);
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return MakeError(E_NOT_FOUND, TStringBuilder()
                << "endpoint " << socketPath.Quote() << " not found");
        }

        if (!it->second) {
            return FallbackListener->RefreshEndpoint(socketPath, volume);
        }

        // TODO: NBS-4151

        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        const auto& socketPath = request.GetUnixSocketPath();
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return MakeFuture(MakeError(E_NOT_FOUND, TStringBuilder()
                << "endpoint " << socketPath.Quote() << " not found"));
        }

        bool isExternalEndpoint = it->second != nullptr;
        if (isExternalEndpoint != CanStartExternalEndpoint(request, volume)) {
            return TrySwitchEndpoint(request, volume, session);
        }

        return MakeFuture<NProto::TError>();
    }

    NProto::TError CancelEndpointInFlightRequests(
        const TString& socketPath) override
    {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return MakeError(
                S_FALSE,
                TStringBuilder()
                    << "endpoint " << socketPath.Quote() << "is not found");
        }

        if (!it->second) {
            return FallbackListener->CancelEndpointInFlightRequests(socketPath);
        }

        return MakeError(
            E_NOT_IMPLEMENTED,
            "Can't cancel in-flight requests for external vhost endpoint");
    }

private:
    NThreading::TFuture<NProto::TError> TrySwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session)
    {
        auto epType = GetEndpointType(request, volume);

        STORAGE_INFO(TStringBuilder()
            << "switching endpoint " << request.GetUnixSocketPath()
            << ", epType=" << GetEndpointTypeName(epType)
            << ", external=" << CanStartExternalEndpoint(request, volume));

        auto self = weak_from_this();

        return StopEndpoint(request.GetUnixSocketPath())
            .Apply([=] (const auto& future) {
                if (future.HasException() || HasError(future.GetValue())) {
                    return future;
                }

                if (auto p = self.lock()) {
                    return p->StartEndpoint(request, volume, session);
                }

                return MakeFuture(MakeError(E_REJECTED, "Cancelled"));
            });
    }

    bool IsLocalMode(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume) const
    {
        return IsDiskRegistryLocalMediaKind(volume.GetStorageMediaKind()) &&
               request.GetVolumeMountMode() == NProto::VOLUME_MOUNT_LOCAL &&
               request.GetIpcType() == NProto::IPC_VHOST;
    }

    bool IsFastPathMode(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume) const
    {
        return volume.GetIsFastPathEnabled()
            && request.GetVolumeMountMode() == NProto::VOLUME_MOUNT_LOCAL
            && request.GetIpcType() == NProto::IPC_VHOST;
    }

    bool CanStartExternalEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume) const
    {
        auto epType = GetEndpointType(request, volume);
        switch (epType) {
        case EEndpointType::Local:
            return true;
        case EEndpointType::Rdma:
            return volume.GetMigrations().empty();
        case EEndpointType::Fallback:
            return false;
        }
    }

    EEndpointType GetEndpointType(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume) const
    {
        if (IsLocalMode(request, volume) && IsOnLocalAgent(volume)) {
            return EEndpointType::Local;
        }

        if (IsFastPathMode(request, volume) && IsOnRdmaAgent(volume)) {
            return EEndpointType::Rdma;
        }

        return EEndpointType::Fallback;
    }

    bool TryStartExternalEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume)
    {
        auto epType = GetEndpointType(request, volume);
        if (epType == EEndpointType::Local && !ValidateLocalLocation(volume)) {
            return false;
        }

        if (!CanStartExternalEndpoint(request, volume)) {
            return false;
        }

        try {
            StartExternalEndpoint(epType, request, volume);

            return true;

        } catch (...) {
            STORAGE_ERROR("Can't run external endpoint: "
                << CurrentExceptionMessage());
        }

        return false;
    }

    TString GetDeviceBackend(EEndpointType epType)
    {
        switch (epType) {
        case EEndpointType::Local:
            return "aio";
        case EEndpointType::Rdma:
            return "rdma";
        case EEndpointType::Fallback:
            return "fallback";
        }
    }

    TString GetDevicePath(EEndpointType epType, const NProto::TDevice& device)
    {
        switch (epType) {
        case EEndpointType::Local:
            return device.GetDeviceName();
        case EEndpointType::Rdma: {
            DevicePath path(
                "rdma",
                device.GetRdmaEndpoint().GetHost(),
                device.GetRdmaEndpoint().GetPort(),
                device.GetDeviceUUID());
            return path.Serialize();
        }
        case EEndpointType::Fallback:
            return "";
        }
    }

    void StartExternalEndpoint(
        EEndpointType epType,
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume)
    {
        STORAGE_INFO("starting endpoint "
            << request.GetUnixSocketPath() << ", epType="
            << GetEndpointTypeName(epType));

        const auto& socketPath = request.GetUnixSocketPath();
        const auto& deviceName = request.GetDeviceName()
            ? request.GetDeviceName()
            : volume.GetDiskId();
        const auto& clientId = request.GetClientId()
            ? request.GetClientId()
            : request.GetInstanceId();

        if (volume.GetStorageMediaKind() == NProto::STORAGE_MEDIA_SSD_LOCAL &&
            volume.GetBlockSize() != DefaultLocalSSDBlockSize)
        {
            ReportStartExternalEndpointError(
                "Local disks should have block size of 512 bytes.");
        }

        TVector<TString> args {
            "--disk-id", request.GetDiskId(),
            "--block-size", ToString(volume.GetBlockSize()),
            "--serial", deviceName,
            "--socket-path", socketPath,
            "--socket-access-mode", ToString(SocketAccessMode),
            "-q", ToString(request.GetVhostQueuesCount()),
            "--wait-after-parent-exit",
            ToString(VhostServerTimeoutAfterParentExit.Seconds()).c_str()
        };

        if (epType == EEndpointType::Rdma) {
            args.emplace_back("--client-id");
            args.emplace_back(clientId);

            args.emplace_back("--device-backend");
            args.emplace_back(GetDeviceBackend(epType));

            if (IsAlignedDataEnabled) {
                args.emplace_back("--rdma-aligned-data");
            }
        }

        for (const auto& device: volume.GetDevices()) {
            const ui64 size = device.GetBlockCount() * volume.GetBlockSize();

            auto offset = device.GetPhysicalOffset();
            if (epType == EEndpointType::Rdma) {
                offset = 0;
            }

            args.insert(args.end(), {
                "--device", TStringBuilder()
                    << GetDevicePath(epType, device) << ":"
                    << size << ":"
                    << offset
                });
        }

        if (request.GetVolumeAccessMode() == NProto::VOLUME_ACCESS_READ_ONLY) {
            args.emplace_back("--read-only");
        }

        const auto& encryptionSpec = request.GetEncryptionSpec();
        if (encryptionSpec.GetMode() != NProto::NO_ENCRYPTION) {
            args.emplace_back("--encryption-mode");
            args.emplace_back(EncryptionModeToString(encryptionSpec.GetMode()));

            const auto& keyPath = encryptionSpec.GetKeyPath();
            if (keyPath.HasFilePath()) {
                args.emplace_back("--encryption-key-path");
                args.emplace_back(keyPath.GetFilePath());
            } else if (keyPath.HasKeyringId()) {
                args.emplace_back("--encryption-keyring-id");
                args.emplace_back(ToString(keyPath.GetKeyringId()));
            } else {
                ythrow yexception()
                    << "EncryptionSpec should has FilePath or KeyringId "
                    << encryptionSpec.AsJSON();
            }
        }

        TVector<TString> cgroups(
            request.GetClientCGroups().begin(),
            request.GetClientCGroups().end()
        );


        auto ep = EndpointFactory(
            clientId,
            request.GetDiskId(),
            std::move(args),
            std::move(cgroups)
        );

        ep->PrepareToStart();
        ShutdownOldEndpoint(request.GetDiskId());
        ep->Start();

        Endpoints[socketPath] = std::move(ep);
    }

    bool IsOnLocalAgent(const NProto::TVolume& volume) const
    {
        return LocalAgentId.empty() || AllOf(
            volume.GetDevices(),
            [&] (const auto& d) {
                return d.GetAgentId() == LocalAgentId;
            });
    }

    bool IsOnRdmaAgent(const NProto::TVolume& volume) const
    {
        return AllOf(
            volume.GetDevices(),
            [&] (const auto& d) {
                return d.GetRdmaEndpoint().GetHost() == d.GetAgentId()
                    && d.GetAgentId() != "";
            });
    }

    bool ValidateLocalLocation(const NProto::TVolume& volume) const
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

    void FindRunningEndpoints()
    {
        TFsPath proc("/proc");
        TVector<TFsPath> allProcesses;
        proc.List(allProcesses);
        for (const auto& process: allProcesses) {
            if (!process.IsDirectory()) {
                continue;
            }

            i32 pid = 0;
            if (!TryFromString<i32>(process.Basename(), pid)) {
                continue;
            }

            auto processMark = ReadFromFile(process / "comm");
            if (!processMark.StartsWith("vhost-")) {
                continue;
            }

            const auto diskId = FindDiskIdForRunningProcess(pid);
            if (diskId) {
                OldEndpoints[pid] = diskId;
                STORAGE_INFO(
                    "Find running external-vhost-server PID:"
                    << pid << " for disk-id: " << diskId.Quote());
            }
        }
    }

    void ShutdownOldEndpoint(const TString& diskId)
    {
        for (auto it = OldEndpoints.begin(); it != OldEndpoints.end();) {
            i32 pid = it->first;
            const auto& oldDiskId = it->second;

            if (oldDiskId != diskId) {
                ++it;
                continue;
            }

            // There is a time interval between the search for old processes
            // and their termination. We are checking here that another
            // process has not been started under the same pid.
            if (oldDiskId == FindDiskIdForRunningProcess(pid)) {
                TChild child(pid);

                STORAGE_WARN(
                    "Send SIGKILL to external-vhost-server with PID:"
                    << pid << " for disk-id: " << diskId.Quote());
                child.Kill();
                child.Wait();
            }
            it = OldEndpoints.erase(it);
        }
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
    ui32 socketAccessMode,
    TDuration vhostServerTimeoutAfterParentExit,
    bool isAlignedDataEnabled,
    IEndpointListenerPtr fallbackListener)
{
    auto defaultFactory = [=] (
        const TString& clientId,
        const TString& diskId,
        TVector<TString> args,
        TVector<TString> cgroups)
    {
        return std::make_shared<TEndpoint>(
            clientId,
            logging,
            executor,
            TEndpointStats {
                .ClientId = clientId,
                .DiskId = diskId,
                .ServerStats = serverStats
            },
            binaryPath,
            std::move(args),
            std::move(cgroups)
        );
    };

    return std::make_shared<TExternalVhostEndpointListener>(
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        std::move(localAgentId),
        socketAccessMode,
        vhostServerTimeoutAfterParentExit,
        isAlignedDataEnabled,
        std::move(fallbackListener),
        std::move(defaultFactory));
}

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString localAgentId,
    ui32 socketAccessMode,
    TDuration vhostServerTimeoutAfterParentExit,
    bool isAlignedDataEnabled,
    IEndpointListenerPtr fallbackListener,
    TExternalEndpointFactory factory)
{
    return std::make_shared<TExternalVhostEndpointListener>(
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        std::move(localAgentId),
        socketAccessMode,
        vhostServerTimeoutAfterParentExit,
        isAlignedDataEnabled,
        std::move(fallbackListener),
        std::move(factory));
}

}   // namespace NCloud::NBlockStore::NServer
