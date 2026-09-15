#include "command.h"

#include <cloud/fastshard/bootstrap/core.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/system/event.h>

#include <cstring>

namespace NCloud::NFastShard::NClient {

using namespace NLastGetopt;

using silk::FiberScheduler;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::MilliSeconds(100);

// Slack on top of --request-timeout before Run gives up on the fiber: the
// storage node may legitimately spend the whole timeout on the request and
// the reply still has to travel back.
constexpr TDuration DeadlineMargin = TDuration::Seconds(1);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TCommand::TFiberParams
{
    TCommand* Command;
};

////////////////////////////////////////////////////////////////////////////////

TCommand::TCommand(IStorageNodePtr client)
    : Client(std::move(client))
{
    Opts.AddHelpOption('h');
    Opts.SetFreeArgsNum(0);

    Opts.AddLongOption("host", "storage node host")
        .RequiredArgument("STR")
        .DefaultValue("localhost")
        .StoreResult(&Host);

    Opts.AddLongOption("port", "storage node port")
        .RequiredArgument("NUM")
        .StoreResult(&Port);

    Opts.AddLongOption("client-id", "client identifier sent in request headers")
        .RequiredArgument("STR")
        .StoreResult(&ClientId);

    Opts.AddLongOption(
            "request-timeout",
            "request timeout in milliseconds sent in request headers; the "
            "command also gives up on the request shortly after it elapses")
        .RequiredArgument("NUM")
        .StoreResult(&RequestTimeoutMs);

    Opts.AddLongOption("proto")
        .Help(
            "read the request from input as protobuf text and print the "
            "response as protobuf text")
        .NoArgument()
        .SetFlag(&Proto);

    Opts.AddLongOption("input", "input file name (or stdin if not specified)")
        .RequiredArgument("STR")
        .StoreResult(&InputFile);

    Opts.AddLongOption(
            "output",
            "output file name (or stdout if not specified)")
        .RequiredArgument("STR")
        .StoreResult(&OutputFile);

    Opts.AddLongOption("verbose", "enable silk debug logging")
        .NoArgument()
        .SetFlag(&Verbose);

    Opts.AddLongOption("timing", "print connect and round trip times to stderr")
        .NoArgument()
        .SetFlag(&Timing);
}

void TCommand::ParseOpts(int argc, const char* argv[])
{
    // argv[0] is the command name; the parser takes argv[0] as the program
    // name for usage, so substitute "<binary> <command>".
    ProgramName = TString::Join(GetProgramName(), " ", argv[0]);
    TVector<const char*> args(argv, argv + argc);
    args[0] = ProgramName.c_str();

    TOptsParseResultException parseResult(
        &Opts,
        static_cast<int>(args.size()),
        args.data());

    if (!Client && !Port) {
        ythrow TUsageException() << "--port is required";
    }
    CheckOpts();
}

bool TCommand::Run()
{
    if (Verbose) {
        EnableDebugLogging();
    }

    const int r = FiberScheduler::run(
        &TCommand::FiberMain,
        TFiberParams{.Command = this},
        nullptr /* future */);
    if (r) {
        ythrow yexception() << "failed to start fiber: " << ::strerror(r);
    }

    const TInstant deadline =
        RequestTimeoutMs
            ? TInstant::Now() + TDuration::MilliSeconds(RequestTimeoutMs) +
                  DeadlineMargin
            : TInstant::Max();

    while (!Done.WaitT(WaitTimeout)) {
        if (StopRequested.load()) {
            Stopped = true;
            Cerr << "command was stopped" << Endl;
            return false;
        }
        if (TInstant::Now() > deadline) {
            Stopped = true;
            Cerr << "request timed out" << Endl;
            return false;
        }
    }

    PrintTiming();

    if (Error) {
        ythrow yexception() << Error;
    }
    return Result;
}

void TCommand::PrintTiming() const
{
    if (!Timing || !Called) {
        return;
    }

    auto ms = [](TDuration d)
    {
        return Sprintf("%.3f ms", d.MicroSeconds() / 1000.0);
    };

    // The connection is opened lazily inside the call, so the round trip
    // proper is the call minus the time the client spent connecting.
    // Without Metrics (a preset Client) only the whole call is known.
    if (!Metrics) {
        Cerr << "round trip " << ms(CallTime) << Endl;
        return;
    }

    const auto connect = TDuration::MicroSeconds(Metrics->ConnectTimeUs.load());
    const auto roundTrip =
        CallTime > connect ? CallTime - connect : TDuration::Zero();
    Cerr << "connect " << ms(connect) << ", round trip " << ms(roundTrip)
         << Endl;
}

void TCommand::Shutdown()
{
    StopRequested.store(true);
}

int TCommand::FiberMain(TFiberParams* params) noexcept
{
    static_assert(sizeof(TFiberParams) <= silk::FIBER_PARAMETERS_SIZE);

    auto& command = *params->Command;
    try {
        if (!command.Client) {
            command.Metrics = std::make_shared<TStorageNodeClientMetrics>();
            command.Client = CreateStorageNodeClient(
                command.Host,
                command.Port,
                command.Metrics);
        }
        command.Result = command.DoExecute();
        command.GetOutputStream().Flush();
    } catch (...) {
        command.Error = CurrentExceptionMessage();
    }
    command.Done.Signal();
    return 0;
}

void TCommand::SetInputStream(std::unique_ptr<IInputStream> is)
{
    InputStream = std::move(is);
}

void TCommand::SetOutputStream(std::shared_ptr<IOutputStream> os)
{
    OutputStream = std::move(os);
}

IInputStream& TCommand::GetInputStream()
{
    if (!InputStream && InputFile) {
        InputStream = std::make_unique<TFileInput>(InputFile);
    }
    return InputStream ? *InputStream : Cin;
}

IOutputStream& TCommand::GetOutputStream()
{
    if (!OutputStream && OutputFile) {
        OutputStream = std::make_shared<TFileOutput>(OutputFile);
    }
    return OutputStream ? *OutputStream : Cout;
}

void TCommand::PrepareHeaders(
    NCloud::NProto::TDeviceRequestHeaders& headers) const
{
    if (ClientId) {
        headers.SetClientId(ClientId);
    }
    if (RequestTimeoutMs) {
        headers.SetRequestTimeout(RequestTimeoutMs);
    }
}

}   // namespace NCloud::NFastShard::NClient
