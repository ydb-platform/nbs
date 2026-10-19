#include "app.h"

#include "device_manager.h"
#include "options.h"

#include <cloud/fastshard/journal/server/builder.h>
#include <cloud/fastshard/journal/server/config.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/network/address.h>
#include <util/string/builder.h>
#include <util/system/progname.h>

#include <signal.h>

#include <cstdio>
#include <new>

namespace NCloud::NFastShard::NJdServer {

using namespace NJournalled;

namespace {

////////////////////////////////////////////////////////////////////////////////

// The signals that stop the server. They are blocked in every thread and
// picked up by the main thread with sigwait, so no handler runs.
sigset_t MakeStopSignals()
{
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    return signals;
}

TNetworkAddress MakeListenAddress(const TOptions& options)
{
    if (options.Host) {
        return TNetworkAddress(options.Host, options.Port);
    }
    return TNetworkAddress(options.Port);
}

////////////////////////////////////////////////////////////////////////////////

int Serve(const TOptions& options)
{
    auto logging = CreateLoggingService(
        "console",
        {.FiltrationLevel = options.Verbose ? TLOG_DEBUG : TLOG_INFO});
    logging->Start();
    Y_DEFER
    {
        logging->Stop();
    };

    auto Log = logging->CreateLog("JDSERVER");

    const ui64 blockCount = options.GetDeviceSize() / options.BlockSize;

    STORAGE_INFO(
        "Opening the device " << options.DevicePath.Quote() << ": "
        << FormatByteSize(options.GetDeviceSize()) << " = journal metadata "
        << FormatByteSize(options.GetLogMetaSize()) << " + journal data "
        << FormatByteSize(options.GetLogDataSize()) << " + data "
        << FormatByteSize(options.DataSize) << ", block size "
        << options.BlockSize);

    auto fileIO = options.AioThreads == 1
                      ? CreateAIOService()
                      : CreateThreadedAIOService(options.AioThreads);

    // The first executor runs the device and the listener; with more than
    // one, the connections are spread over all of them
    TVector<TExecutorPtr> executors;
    for (ui32 i = 0; i != options.Executors; ++i) {
        TString name = "JD";
        if (i) {
            name += ToString(i);
        }
        executors.push_back(TExecutor::Create(std::move(name)));
    }
    auto executor = executors.front();

    auto deviceManager = CreateFileDeviceManager(
        fileIO,
        options.DeviceUUID,
        options.DevicePath,
        blockCount,
        options.BlockSize);

    auto server = TServerBuilder(
        logging,
        executor,
        std::move(deviceManager),
        TServerConfig{
            .ListenAddress = MakeListenAddress(options),
            .JournalEnabled = !options.NoJournal,
            .RestoreConcurrency = 1,
            .ConnectionExecutors =
                options.Executors > 1 ? executors : TVector<TExecutorPtr>{},
        },
        {TJournalledDeviceConfig{
            .DeviceUUID = options.DeviceUUID,
            .BlocksCount = blockCount,
            .BlockSize = options.BlockSize,
            .LogMetaSize = options.GetLogMetaSize(),
            .LogDataSize = options.GetLogDataSize(),
        }}).Build();

    {
        fileIO->Start();
        Y_DEFER
        {
            fileIO->Stop();
        };

        for (const auto& e: executors) {
            e->Start();
        }
        Y_DEFER
        {
            for (const auto& e: executors) {
                e->Stop();
            }
        };

        server->Start();
        Y_DEFER
        {
            server->Stop();
        };

        STORAGE_INFO(
            "Serving the device " << options.DeviceUUID.Quote() << " on port "
            << options.Port << (options.NoJournal ? " without a journal" : "")
            << " with " << options.Executors << " executor(s) and "
            << options.AioThreads << " AIO thread(s), press Ctrl+C to stop");

        //
        // Wait for a stop signal; a second one is left to the default handler
        // and kills the process
        //

        const auto stopSignals = MakeStopSignals();
        int signum = 0;
        sigwait(&stopSignals, &signum);
        pthread_sigmask(SIG_UNBLOCK, &stopSignals, nullptr);

        STORAGE_INFO("Stopping on signal " << signum << "...");
    }

    STORAGE_INFO("Stopped");

    return 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int Run(int argc, const char* argv[])
{
    TOptions options;
    try {
        options.Parse(argc, argv);
    } catch (const NLastGetopt::TException&) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }

    try {
        return Serve(options);
    } catch (...) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }
}

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // a client closing the connection must surface as a send error, not
    // kill the process
    signal(SIGPIPE, SIG_IGN);

    // the threads started later inherit the mask, so the stop signals reach
    // the main thread only
    const auto stopSignals = MakeStopSignals();
    pthread_sigmask(SIG_BLOCK, &stopSignals, nullptr);
}

}   // namespace NCloud::NFastShard::NJdServer
