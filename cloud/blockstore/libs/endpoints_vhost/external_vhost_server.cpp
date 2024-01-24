#include "external_vhost_server.h"
#include "external_endpoint_stats.h"

#include <cloud/blockstore/libs/common/device_path.h>
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
#include <util/generic/hash.h>
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
    Y_ABORT("Process was not created: %s", ::strerror_r(err, buf, sizeof(buf)));
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
    : public IExternalEndpoint
    , public std::enable_shared_from_this<TEndpoint>
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
        Y_DEBUG_ABORT_UNLESS(ShouldStop);
    }

    void Start() override
    {
        Process = StartProcess();

        ISimpleThread::Start();
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
    void* ThreadProc() override
    {
        auto holder = shared_from_this();

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
    const IEndpointListenerPtr FallbackListener;
    const TExternalEndpointFactory EndpointFactory;

    TLog Log;

    THashMap<TString, IExternalEndpointPtr> Endpoints;

public:
    TExternalVhostEndpointListener(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            TString localAgentId,
            IEndpointListenerPtr fallbackListener,
            TExternalEndpointFactory endpointFactory)
        : Logging {std::move(logging)}
        , ServerStats {std::move(serverStats)}
        , Executor {std::move(executor)}
        , LocalAgentId {std::move(localAgentId)}
        , FallbackListener {std::move(fallbackListener)}
        , EndpointFactory {std::move(endpointFactory)}
        , Log {Logging->CreateLog("BLOCKSTORE_SERVER")}
    {}

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
        return volume.GetStorageMediaKind() == NProto::STORAGE_MEDIA_SSD_LOCAL
            && request.GetVolumeMountMode() == NProto::VOLUME_MOUNT_LOCAL
            && request.GetIpcType() == NProto::IPC_VHOST;
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

        TVector<TString> args {
            "--serial", deviceName,
            "--socket-path", socketPath,
            "-q", ToString(request.GetVhostQueuesCount())
        };

        if (epType == EEndpointType::Rdma) {
            args.emplace_back("--client-id");
            args.emplace_back(clientId);

            args.emplace_back("--disk-id");
            args.emplace_back(request.GetDiskId());

            args.emplace_back("--device-backend");
            args.emplace_back(GetDeviceBackend(epType));

            args.emplace_back("--block-size");
            args.emplace_back(ToString(volume.GetBlockSize()));
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
        std::move(fallbackListener),
        std::move(defaultFactory));
}

IEndpointListenerPtr CreateExternalVhostEndpointListener(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    TString localAgentId,
    IEndpointListenerPtr fallbackListener,
    TExternalEndpointFactory factory)
{
    return std::make_shared<TExternalVhostEndpointListener>(
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        std::move(localAgentId),
        std::move(fallbackListener),
        std::move(factory));
}

}   // namespace NCloud::NBlockStore::NServer
