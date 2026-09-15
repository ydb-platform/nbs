#include "command.h"

#include <cloud/filestore/libs/storage/fastshard/bootstrap/core.h>
#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/event.h>

#include <cstring>

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

using namespace NLastGetopt;

using silk::FiberScheduler;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::MilliSeconds(100);

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
            "request timeout in milliseconds sent in request headers")
        .RequiredArgument("NUM")
        .StoreResult(&RequestTimeoutMs);

    Opts.AddLongOption("proto")
        .Help("read the request from input as protobuf text and print the "
              "response as protobuf text")
        .NoArgument()
        .SetFlag(&Proto);

    Opts.AddLongOption("input", "input file name (or stdin if not specified)")
        .RequiredArgument("STR")
        .StoreResult(&InputFile);

    Opts.AddLongOption("output", "output file name (or stdout if not specified)")
        .RequiredArgument("STR")
        .StoreResult(&OutputFile);

    Opts.AddLongOption("verbose", "enable silk debug logging")
        .NoArgument()
        .SetFlag(&Verbose);
}

bool TCommand::Run(int argc, const char* argv[])
{
    ParseResult = std::make_unique<TOptsParseResultException>(&Opts, argc, argv);

    if (!Client && !Port) {
        ythrow TUsageException() << "--port is required";
    }
    CheckOpts();

    if (Verbose) {
        EnableDebugLogging();
    }

    //
    // The outcome is reported through Done rather than a FiberFuture:
    // the main thread has to wake up periodically to notice Shutdown,
    // and the timed future wait is fiber-only.
    //

    const int r = FiberScheduler::run(
        &TCommand::FiberMain,
        TFiberParams{.Command = this},
        nullptr /* future */);
    if (r) {
        ythrow yexception() << "failed to start fiber: " << ::strerror(r);
    }

    while (!Done.WaitT(WaitTimeout)) {
        if (StopRequested.load()) {
            Stopped = true;
            Cerr << "command was stopped" << Endl;
            return false;
        }
    }

    if (Error) {
        ythrow yexception() << Error;
    }
    return Result;
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
            command.Client = CreateStorageNodeClient(command.Host, command.Port);
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

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
