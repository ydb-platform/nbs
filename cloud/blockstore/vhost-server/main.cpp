#include "backend_aio.h"
#include "backend_null.h"
#include "backend_rdma.h"
#include "server.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/json_writer.h>

#include <util/datetime/base.h>

#include <pthread.h>
#include <signal.h>

using namespace NCloud::NBlockStore::NVHostServer;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCerrJsonLogBackend
    : public TLogBackend
{
    ELogPriority VerboseLevel;

    explicit TCerrJsonLogBackend(ELogPriority verboseLevel)
        : VerboseLevel {verboseLevel}
    {}

    ELogPriority FiltrationLevel() const override
    {
        return VerboseLevel;
    }

    void WriteData(const TLogRecord& rec) override
    {
        TStringStream ss;
        NJsonWriter::TBuf buf {NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss};
        buf.BeginObject();
        buf.WriteKey("priority");
        buf.WriteInt(rec.Priority);
        buf.WriteKey("message");
        buf.WriteString(TStringBuf(rec.Data, rec.Len));
        buf.EndObject();
        ss << '\n';

        Cerr << ss.Str();
        Cerr.Flush();
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

IBackendPtr CreateBackend(
    const TOptions& options,
    NCloud::ILoggingServicePtr logging)
{
    if (options.DeviceBackend == "aio") {
        return CreateAioBackend(logging);
    } else if (options.DeviceBackend == "rdma") {
        return CreateRdmaBackend(logging);
    } else if (options.DeviceBackend == "null") {
        return CreateNullBackend(logging);
    }

    Y_ABORT(
        "Failed to create %s device backend",
        options.DeviceBackend.c_str());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    TOptions options;

    try {
        options.Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    // tune the signal to block on waiting for "stop server" command
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    auto logService = CreateLogService(options);
    auto backend = CreateBackend(options, logService);
    auto server = CreateServer(logService, backend);

    server->Start(options);

    TSimpleStats prevStats;
    TInstant ts = Now();

    // wait for signal to stop the server (Ctrl+C)
    int sig;
    while (!sigwait(&sigset, &sig) && sig != SIGINT) {
        auto stats = server->GetStats(prevStats);
        auto now = Now();
        DumpStats(
            stats,
            prevStats,
            now - ts,
            Cout,
            GetCyclesPerMillisecond());

        ts = now;
    }

    server->Stop();

    return 0;
}
