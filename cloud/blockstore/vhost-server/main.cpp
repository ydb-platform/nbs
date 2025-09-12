#include "backend_aio.h"
#include "backend_io_uring.h"
#include "backend_null.h"
#include "backend_rdma.h"
#include "critical_event.h"
#include "server.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryptor.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/json_writer.h>

#include <util/datetime/base.h>
#include <util/system/thread.h>

#include <pthread.h>
#include <signal.h>
#include <sys/prctl.h>

namespace NCloud::NBlockStore::NVHostServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxHandle = 1024;

timespec ToTimeSpec(TDuration t)
{
    return timespec{
        .tv_sec = static_cast<int>(t.Seconds()),
        .tv_nsec = t.MicroSecondsOfSecond() * 1000};
}

class TCerrJsonLogBackend
    : public TLogBackend
{
    ELogPriority VerboseLevel;
    bool PipeClosed = false;

public:
    explicit TCerrJsonLogBackend(ELogPriority verboseLevel)
        : VerboseLevel {verboseLevel}
    {}

    ELogPriority FiltrationLevel() const override
    {
        return VerboseLevel;
    }

    void WriteData(const TLogRecord& rec) override
    {
        if (PipeClosed) {
            return;
        }

        TStringStream ss;
        NJsonWriter::TBuf buf {NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss};
        buf.BeginObject();
        buf.WriteKey("priority");
        buf.WriteInt(rec.Priority);
        buf.WriteKey("message");
        buf.WriteString(TStringBuf(rec.Data, rec.Len));
        buf.EndObject();
        ss << '\n';

        try {
            Cerr << ss.Str();
            Cerr.Flush();
        } catch (const TSystemError& e) {
            // Ignore broken pipe
            PipeClosed = true;
        }
    }

    void ReopenLog() override
    {}
};

struct TDefaultLoggingService
    : public NCloud::ILoggingService
{
    const ELogPriority LogPriority;

    TDefaultLoggingService(ELogPriority logPriority)
        : LogPriority {logPriority}
    {}

    TLog CreateLog(const TString& component) override
    {
        Y_UNUSED(component);

        return TLog {MakeHolder<TCerrJsonLogBackend>(LogPriority)};
    }

    void Start() override
    {
        // nothing to do
    }

    void Stop() override
    {
        // nothing to do
    }
};

NCloud::ILoggingServicePtr CreateLogService(const TOptions& options)
{
    auto logLevel = NCloud::GetLogLevel(options.VerboseLevel).GetRef();
    if (options.LogType == "console") {
        return NCloud::CreateLoggingService(
            "console",
            NCloud::TLogSettings{.FiltrationLevel = logLevel});
    }

    return std::make_shared<TDefaultLoggingService>(logLevel);
}

IEncryptorPtr CreateEncryptor(
    const TOptions& options,
    NCloud::ILoggingServicePtr logging)
{
    auto encryptionSpec = options.GetEncryptionSpec();
    if (encryptionSpec.GetMode() == NProto::NO_ENCRYPTION) {
        return {};
    }

    auto Log = logging->CreateLog("SERVER");

    STORAGE_INFO("Encryption. Get key " << encryptionSpec.AsJSON());

    auto keyProvider = CreateDefaultEncryptionKeyProvider();
    auto keyFuture =
        keyProvider->GetKey(options.GetEncryptionSpec(), options.DiskId);
    auto keyOrError = std::move(keyFuture).ExtractValue();
    if (HasError(keyOrError)) {
        Y_ABORT(
            "Error getting encryption key: %s",
            ToString(keyOrError.GetError()).c_str());
    }
    auto key = keyOrError.ExtractResult();
    STORAGE_ERROR("Got encryption key with hash " << key.GetHash());
    return CreateAesXtsEncryptor(std::move(key));
}

IBackendPtr CreateBackend(
    const TOptions& options,
    NCloud::ILoggingServicePtr logging)
{
    auto encryptor = CreateEncryptor(options, logging);

    if (options.DeviceBackend == "aio") {
        return CreateAioBackend(std::move(encryptor), std::move(logging));
    }

    if (options.DeviceBackend == "rdma") {
        return CreateRdmaBackend(std::move(logging));
    }

    if (options.DeviceBackend == "io_uring") {
        return CreateIoUringBackend(std::move(encryptor), std::move(logging));
    }

    if (options.DeviceBackend == "null") {
        return CreateNullBackend(std::move(logging));
    }

    Y_ABORT(
        "Failed to create %s device backend",
        options.DeviceBackend.c_str());
}

void CloseAllFileHandlesExceptSTD()
{
    for (int h = 0; h < MaxHandle; ++h) {
        if (h == STDIN_FILENO || h == STDOUT_FILENO || h == STDERR_FILENO) {
            continue;
        }
        ::close(h);
    }
}

void EscapeFromParentProcessGroup()
{
    ::setpgid(0, 0);
}

void SetProcessMark(const TString& diskId)
{
    TString id = "vhost-" + diskId;
    TThread::SetCurrentThreadName(id.c_str());
}

bool IsParentProcessAlive(__pid_t parentPid)
{
    return kill(parentPid, 0) == 0;
}

}   // namespace

} // namespace NCloud::NBlockStore::NVHostServer

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NBlockStore::NVHostServer;

int main(int argc, char** argv)
{
    TOptions options;

    try {
        options.Parse(argc, argv);
        if (!options.BlockstoreServicePid) {
            options.BlockstoreServicePid = getppid();
        }
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    CloseAllFileHandlesExceptSTD();
    EscapeFromParentProcessGroup();
    SetProcessMark(options.DiskId);

    // Attention! We set the SIG_BLOCK mask before creating backends so that the
    // forked threads inherit this mask.
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    auto logService = CreateLogService(options);
    auto backend = CreateBackend(options, logService);
    auto server = CreateServer(logService, backend);
    auto Log = logService->CreateLog("SERVER");
    SetCriticalEventsLog(Log);

    server->Start(options);

    TSimpleStats prevStats;
    TInstant ts = Now();

    // Translate parent process exit to SIGUSR2
    ::prctl(PR_SET_PDEATHSIG, SIGUSR2);

    // Tune the signal to block on waiting for "stop server", "dump", "parent
    // exit" signals.
    sigaddset(&sigset, SIGUSR2);
    sigaddset(&sigset, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    TInstant deathTimerStartedAt;
    // Wait for signal to stop the server (Ctrl+C) or dump statistics.
    const bool isParentAlive = IsParentProcessAlive(options.BlockstoreServicePid);
    if (!isParentAlive) {
        STORAGE_INFO("Parent process exit immediately.");
        deathTimerStartedAt = TInstant::Now();
    }
    for (bool running = true, parentExit = !isParentAlive; running;) {
        int sig = 0;
        if (parentExit) {
            TDuration delayAfterParentExit = deathTimerStartedAt +
                                             options.WaitAfterParentExit -
                                             TInstant::Now();
            if (!delayAfterParentExit) {
                break;
            }
            // Wait for signal with timeout.
            STORAGE_INFO("Wait for timeout " << delayAfterParentExit);
            timespec timeout = ToTimeSpec(delayAfterParentExit);
            siginfo_t info;
            memset(&info, 0, sizeof(info));
            sig = ::sigtimedwait(&sigset, &info, &timeout);

        } else {
            // Wait for signal without timeout.
            ::sigwait(&sigset, &sig);
        }
        switch (sig) {
            case SIGUSR1: {
                auto completeStats = server->GetStats(prevStats);
                auto now = Now();
                try {
                    DumpStats(
                        completeStats,
                        prevStats,
                        now - ts,
                        Cout,
                        GetCyclesPerMillisecond());
                } catch (const TSystemError& e) {
                    STORAGE_INFO("DumpStats error: " << e.AsStrBuf());
                }
                ts = now;
            } break;
            case SIGINT: {
                STORAGE_INFO("Exit. SIGINT");
                running = false;
            } break;
            case SIGUSR2: {
                STORAGE_INFO("Parent process exit.");
                if (!deathTimerStartedAt) {
                    deathTimerStartedAt = TInstant::Now();
                }
                parentExit = true;
            } break;
            case SIGPIPE: {
                STORAGE_INFO("Pipe to parent process broken.");
                if (!deathTimerStartedAt) {
                    deathTimerStartedAt = TInstant::Now();
                }
                parentExit = true;
            } break;
            case -1: {
                STORAGE_WARN("Exit. Timeout after parent process exit has expired.");
                running = false;
            } break;
            default: {
                STORAGE_ERROR("Exit. Unexpected signal: " << sig);
                running = false;
            }
        }
    }

    server->Stop();

    return 0;
}
