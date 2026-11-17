#include "bootstrap.h"

#include <cloud/fastshard/apps/jdserver/lib/device_manager.h>
#include <cloud/fastshard/apps/jdserver/lib/options.h>
#include <cloud/fastshard/libs/journal/impl/file_device.h>
#include <cloud/fastshard/libs/journal/server/builder.h>
#include <cloud/fastshard/libs/journal/server/config.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/network/address.h>
#include <util/string/builder.h>

namespace NCloud::NFastShard::NJdServer {

using namespace NJournalled;

namespace {

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress MakeListenAddress(const TOptions& options)
{
    if (options.Host) {
        return TNetworkAddress(options.Host, options.Port);
    }
    return TNetworkAddress(options.Port);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TOptionsPtr options)
    : Options(std::move(options))
{}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Init()
{
    const auto& options = *Options;

    Logging = CreateLoggingService(
        "console",
        {.FiltrationLevel = options.Verbose ? TLOG_DEBUG : TLOG_INFO});
    Log = Logging->CreateLog("JDSERVER");

    const ui64 blockCount = options.GetDeviceSize() / options.BlockSize;

    STORAGE_INFO(
        "Opening the device "
        << options.DevicePath.Quote() << ": "
        << FormatByteSize(options.GetDeviceSize()) << " = journal metadata "
        << FormatByteSize(options.GetLogMetaSize()) << " + journal data "
        << FormatByteSize(options.GetLogDataSize()) << " + data "
        << FormatByteSize(options.DataSize) << ", block size "
        << options.BlockSize);

    FileIO = options.AioThreads == 1
                 ? CreateAIOService()
                 : CreateThreadedAIOService(options.AioThreads);

    Executor = TExecutor::Create("JD");

    auto device = CreateFileDevice(
        FileIO,
        options.DevicePath,
        blockCount,
        options.BlockSize);

    auto deviceManager =
        CreateSingleDeviceManager(std::move(device), options.DeviceUUID);

    Server = TServerBuilder(
        Logging,
        Executor,
        std::move(deviceManager),
        TServerConfig{
            .ListenAddress = MakeListenAddress(options),
            .JournalEnabled = !options.NoJournal,
            .RestoreConcurrency = 1,
        },
        {TJournalledDeviceConfig{
            .DeviceUUID = options.DeviceUUID,
            .BlocksCount = blockCount,
            .BlockSize = options.BlockSize,
            .LogMetaSize = options.GetLogMetaSize(),
            .LogDataSize = options.GetLogDataSize(),
        }}).Build();
}

void TBootstrap::Start()
{
    StartComponent(Logging);
    StartComponent(FileIO);
    StartComponent(Executor);
    StartComponent(Server);
}

void TBootstrap::Stop()
{
    //
    // Stop in the reverse order. Logging was started first, so it is
    // stopped last and the other components can still log while they stop
    //

    while (!Started.empty()) {
        Started.back()->Stop();
        Started.pop_back();
    }

    STORAGE_INFO("Stopped");
}

void TBootstrap::StartComponent(IStartablePtr component)
{
    component->Start();
    Started.push_back(std::move(component));
}

}   // namespace NCloud::NFastShard::NJdServer
