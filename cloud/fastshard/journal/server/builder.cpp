#include "builder.h"

#include "server.h"
#include "service.h"

#include <cloud/fastshard/journal/impl/device_page_store.h>
#include <cloud/fastshard/journal/impl/journal.h>
#include <cloud/fastshard/journal/impl/journalled_device_v1.h>
#include <cloud/fastshard/journal/impl/journalled_device_v2.h>
#include <cloud/fastshard/journal/impl/key_buffer_store.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceV1Factory final
    : public IJournalledDeviceFactory
{
private:
    const IDeviceManagerPtr DeviceManager;

public:
    explicit TJournalledDeviceV1Factory(IDeviceManagerPtr deviceManager)
        : DeviceManager(std::move(deviceManager))
    {}

    TResultOrError<IJournalledDevicePtr> Create(
        const TJournalledDeviceConfig& config) final;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IJournalledDevicePtr> TJournalledDeviceV1Factory::Create(
    const TJournalledDeviceConfig& config)
{
    const auto& uuid = config.DeviceUUID;

    if (!config.BlockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
    }

    if (config.LogMetaSize || config.LogDataSize) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal is disabled, but the device " << uuid.Quote()
                << " has the journal parts " << config.LogMetaSize << " and "
                << config.LogDataSize << " bytes");
    }

    // No journal: the whole device holds the data, writes go straight to it.
    return CreateJournalledDeviceV1(
        DeviceManager->CreateDevice(
            uuid,
            {.FirstPageNo = 0, .PageCount = config.BlocksCount},
            config.BlockSize));
}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceV2Factory final
    : public IJournalledDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const TExecutorPtr Executor;
    const IDeviceManagerPtr DeviceManager;

    TLog Log;

public:
    TJournalledDeviceV2Factory(
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        IDeviceManagerPtr deviceManager)
        : Logging(std::move(logging))
        , Executor(std::move(executor))
        , DeviceManager(std::move(deviceManager))
        , Log(Logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE"))
    {}

    TResultOrError<IJournalledDevicePtr> Create(
        const TJournalledDeviceConfig& config) final;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IJournalledDevicePtr> TJournalledDeviceV2Factory::Create(
    const TJournalledDeviceConfig& config)
{
    const ui32 blockSize = config.BlockSize;
    const ui64 blockCount = config.BlocksCount;
    const auto& uuid = config.DeviceUUID;

    if (!blockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
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

    // The key buffer store needs a couple of pages for its superblock and at
    // least one for the entries.
    constexpr ui64 MinLogMetaBlockCount = 3;

    if (logMetaBlockCount < MinLogMetaBlockCount || logDataBlockCount < 1 ||
        logMetaBlockCount + logDataBlockCount >= blockCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal parts " << logMetaBlockCount << " and "
                << logDataBlockCount << " blocks leave no room on the device "
                << "of " << blockCount << " blocks");
    }

    const ui64 dataBlockCount =
        blockCount - logMetaBlockCount - logDataBlockCount;

    if (logMetaBlockCount > logDataBlockCount ||
        logDataBlockCount > dataBlockCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal metadata (" << logMetaBlockCount
                << " blocks) must not exceed the journal data ("
                << logDataBlockCount << " blocks), which must not exceed "
                << "the data (" << dataBlockCount << " blocks)");
    }

    auto createAdapter = [&](ui64 firstBlockIndex, ui64 regionBlockCount)
    {
        return DeviceManager->CreateDevice(
            uuid,
            {.FirstPageNo = firstBlockIndex, .PageCount = regionBlockCount},
            blockSize);
    };

    auto logMetaStore = CreateDeviceKeyBufferStore(
        Logging,
        createAdapter(0, logMetaBlockCount),
        logMetaBlockCount,
        blockSize);

    auto logDataStore = CreateDevicePageStore(
        createAdapter(logMetaBlockCount, logDataBlockCount),
        logDataBlockCount,
        blockSize);

    auto dataStore = createAdapter(
        logMetaBlockCount + logDataBlockCount,
        dataBlockCount);

    auto journal = CreateJournal(
        Logging,
        Executor,
        std::move(logMetaStore),
        std::move(logDataStore),
        dataBlockCount);

    auto journalledDevice = CreateJournalledDeviceV2(
        Logging,
        Executor,
        std::move(journal),
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
    TServerConfig serverConfig,
    TVector<TJournalledDeviceConfig> deviceConfigs)
    : Logging(std::move(logging))
    , Executor(std::move(executor))
    , DeviceManager(std::move(deviceManager))
    , ServerConfig(std::move(serverConfig))
    , DeviceConfigs(std::move(deviceConfigs))
{}

IStartablePtr TServerBuilder::Build()
{
    IJournalledDeviceFactoryPtr factory;

    if (ServerConfig.JournalEnabled) {
        factory = std::make_shared<TJournalledDeviceV2Factory>(
            Logging,
            Executor,
            DeviceManager);
    } else {
        factory = std::make_shared<TJournalledDeviceV1Factory>(DeviceManager);
    }

    auto service = CreateService(
        Executor,
        std::move(DeviceManager),
        std::move(factory),
        std::move(DeviceConfigs),
        ServerConfig.RestoreConcurrency);

    return CreateServer(
        ServerConfig.ListenAddress,
        std::move(Logging),
        std::move(Executor),
        std::move(service),
        std::move(ServerConfig.ConnectionExecutors));
}

}   // namespace NCloud::NJournalled
