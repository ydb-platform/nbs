#include "builder.h"

#include "server.h"
#include "service.h"

#include <cloud/fastshard/journal/impl/journal.h>
#include <cloud/fastshard/journal/impl/journalled_device_v1.h>
#include <cloud/fastshard/journal/impl/journalled_device_v2.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IJournalledDevicePtr> CreateJournalledDevice(
    TLog& Log,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IDeviceManagerPtr deviceManager,
    bool journalEnabled,
    const TJournalledDeviceConfig& config)
{
    const ui32 blockSize = config.BlockSize;
    const ui64 blockCount = config.BlocksCount;
    const auto& uuid = config.DeviceUUID;

    if (!blockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
    }

    if (!journalEnabled) {
        // No journal: the whole device holds the data, writes go straight to it.
        return CreateJournalledDeviceV1(
            deviceManager->CreateDevice(
                uuid,
                {.FirstPageNo = 0, .PageCount = blockCount},
                blockSize));
    }

    // The device is split into three parts: the journal metadata, the journal
    // data and the data itself.
    const ui64 logMetaSize = config.LogMetaSize;
    const ui64 logDataSize = config.LogDataSize;

    if (logMetaSize % blockSize || logDataSize % blockSize) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal parts " << logMetaSize << " and "
                << logDataSize << " bytes are not multiples of the block size "
                << blockSize);
    }

    const ui64 logMetaBlockCount = logMetaSize / blockSize;
    const ui64 logDataBlockCount = logDataSize / blockSize;

    if (logMetaBlockCount + logDataBlockCount >= blockCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal parts " << logMetaBlockCount << " and "
                << logDataBlockCount << " blocks leave no room on the device "
                << "of " << blockCount << " blocks");
    }

    const ui64 dataBlockCount =
        blockCount - logMetaBlockCount - logDataBlockCount;

    auto createAdapter = [&](ui64 firstBlockIndex, ui64 regionBlockCount)
    {
        return deviceManager->CreateDevice(
            uuid,
            {.FirstPageNo = firstBlockIndex, .PageCount = regionBlockCount},
            blockSize);
    };

    auto logMetaDevice = createAdapter(0, logMetaBlockCount);
    auto logDataDevice = createAdapter(logMetaBlockCount, logDataBlockCount);
    auto dataStore = createAdapter(
        logMetaBlockCount + logDataBlockCount,
        dataBlockCount);

    // The factory validates the journal layout.
    auto [journalFactory, error] = CreateJournalFactory(
        logging,
        executor,
        std::move(logMetaDevice),
        std::move(logDataDevice),
        TJournalConfig{
            .LogMetaPageCount = logMetaBlockCount,
            .LogDataPageCount = logDataBlockCount,
            .DataPageCount = dataBlockCount,
            .PageSize = blockSize,
        });

    if (HasError(error)) {
        return error;
    }

    auto journalledDevice = CreateJournalledDeviceV2(
        std::move(logging),
        std::move(executor),
        std::move(journalFactory),
        std::move(dataStore),
        uuid);

    STORAGE_DEBUG(
        "Journalled device "
        << uuid.Quote() << " created: journal "
        << "metadata " << FormatByteSize(logMetaBlockCount * blockSize)
        << ", journal data "
        << FormatByteSize(logDataBlockCount * blockSize) << ", data "
        << FormatByteSize(dataBlockCount * blockSize) << ", block size "
        << blockSize);

    return journalledDevice;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TServerBuilder::TServerBuilder(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IDeviceManagerPtr deviceManager,
    const TNetworkAddress& listenAddress,
    bool journalEnabled,
    TVector<TJournalledDeviceConfig> deviceConfigs)
    : Logging(std::move(logging))
    , Executor(std::move(executor))
    , DeviceManager(std::move(deviceManager))
    , ListenAddress(listenAddress)
    , JournalEnabled(journalEnabled)
    , DeviceConfigs(std::move(deviceConfigs))
{}

IStartablePtr TServerBuilder::Build()
{
    auto Log = Logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE");

    TVector<TJournalledDeviceSpec> devices;

    for (const auto& config: DeviceConfigs) {
        const auto& uuid = config.DeviceUUID;

        if (!JournalEnabled && (config.LogMetaSize || config.LogDataSize)) {
            ReportJournalledDeviceCreationError(
                TStringBuilder()
                << "the journal is disabled, but the device " << uuid.Quote()
                << " has the journal parts " << config.LogMetaSize << " and "
                << config.LogDataSize << " bytes");
            continue;
        }

        auto [device, error] = CreateJournalledDevice(
            Log,
            Logging,
            Executor,
            DeviceManager,
            JournalEnabled,
            config);

        if (HasError(error)) {
            ReportJournalledDeviceCreationError(
                TStringBuilder() << "unable to create device " << uuid.Quote()
                                 << ": " << FormatError(error));
            continue;
        }

        devices.emplace_back(std::move(device), config);
    }

    if (devices.empty()) {
        STORAGE_THROW_SERVICE_ERROR(MakeError(
            E_NOT_FOUND,
            TStringBuilder() << "none of the " << DeviceConfigs.size()
                             << " journalled devices could be created"));
    }

    return CreateServer(
        ListenAddress,
        std::move(Logging),
        std::move(Executor),
        CreateService(std::move(DeviceManager), std::move(devices)));
}

}   // namespace NCloud::NJournalled
