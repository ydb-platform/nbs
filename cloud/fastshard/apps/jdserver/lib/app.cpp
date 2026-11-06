#include "app.h"

#include "device_manager.h"
#include "options.h"

#include <cloud/fastshard/libs/journal/impl/file_device.h>
#include <cloud/fastshard/libs/journal/server/builder.h>
#include <cloud/fastshard/libs/journal/server/config.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/executor.h>

#include <library/cpp/coroutine/engine/impl.h>

#include <library/cpp/coroutine/engine/network.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/scope.h>
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
        "Opening the device "
        << options.DevicePath.Quote() << ": "
        << FormatByteSize(options.GetDeviceSize()) << " = journal metadata "
        << FormatByteSize(options.GetLogMetaSize()) << " + journal data "
        << FormatByteSize(options.GetLogDataSize()) << " + data "
        << FormatByteSize(options.DataSize) << ", block size "
        << options.BlockSize);

    std::shared_ptr<IExternalCompletionFileIOService> externalAio;
    IFileIOServicePtr fileIO;
    if (options.AioOnExecutor) {
        externalAio = CreateAIOServiceExternalCompletion();
        fileIO = externalAio;
    } else {
        fileIO = options.AioThreads == 1
                     ? CreateAIOService()
                     : CreateThreadedAIOService(options.AioThreads);
    }

    //
    // Create the executors: the first one runs the device and the listener,
    // with more than one the connections are spread over all of them
    //

    TVector<TExecutorPtr> executors;
    for (ui32 i = 0; i != options.Executors; ++i) {
        TString name = "JD";
        if (i) {
            name += ToString(i);
        }
        executors.push_back(TExecutor::Create(std::move(name)));
    }
    auto executor = executors.front();

    auto device = CreateFileDevice(
        fileIO,
        options.DevicePath,
        blockCount,
        options.BlockSize);

    auto deviceManager =
        CreateSingleDeviceManager(std::move(device), options.DeviceUUID);

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
            if (externalAio) {
                // wakes and ends the completion coroutine below
                externalAio->Stop();
            }
            for (const auto& e: executors) {
                e->Stop();
            }
        };

        //
        // PROTOTYPE: reap the AIO completions on the first executor - the
        // kernel wakes the executor's poller through the eventfd, so the
        // completion thread and its hand-off are out of the path
        //

        if (externalAio) {
            const bool spin = options.AioSpin;
            executor->ExecuteSimple(
                [externalAio, spin]
                {
                    TCont* cont = RunningCont();
                    const int fd = externalAio->GetCompletionFd();
                    for (;;) {
                        if (NCoro::PollI(cont, fd, CONT_POLL_READ) != 0) {
                            break;
                        }
                        ui64 count = 0;
                        if (::read(fd, &count, sizeof(count)) < 0) {
                            continue;
                        }
                        externalAio->ProcessCompletions();

                        if (!spin) {
                            continue;
                        }

                        //
                        // Busy-poll while writes are in flight: a submit
                        // kicks the fd, so this runs right after it and
                        // sees the completion without a wake-up. Bounded,
                        // so a slow device falls back to the wait above
                        //

                        for (ui32 i = 0; i < 20000 && externalAio->InFlight();
                             ++i)
                        {
                            if (externalAio->ProcessCompletions()) {
                                break;
                            }
                            sched_yield();
                        }
                    }
                });
        }

        server->Start();
        Y_DEFER
        {
            server->Stop();
        };

        STORAGE_INFO(
            "Serving the device "
            << options.DeviceUUID.Quote() << " on port " << options.Port
            << (options.NoJournal ? " without a journal" : "") << " with "
            << options.Executors << " executor(s) and " << options.AioThreads
            << " AIO thread(s), press Ctrl+C to stop");

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
