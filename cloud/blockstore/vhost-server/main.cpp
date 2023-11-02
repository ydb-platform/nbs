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

struct TLoggingService
    : public NCloud::ILoggingService
{
    const ELogPriority LogPriority;

    TLoggingService(ELogPriority logPriority)
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

    auto server = CreateServer(std::make_shared<TLoggingService>(
        NCloud::GetLogLevel(options.VerboseLevel).GetRef()
    ));

    server->Start(options);

    TSimpleStats prevStats;
    TInstant ts = Now();

    // wait for signal to stop the server (Ctrl+C)
    int sig;
    while (!sigwait(&sigset, &sig) && sig != SIGINT) {
        auto stats = server->GetStats();
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
