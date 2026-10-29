#include "builder.h"

#include "server.h"
#include "service.h"

#include <cloud/fastshard/libs/journal/impl/device_page_store.h>
#include <cloud/fastshard/libs/journal/impl/journal.h>
#include <cloud/fastshard/libs/journal/impl/journalled_device_v1.h>
#include <cloud/fastshard/libs/journal/impl/journalled_device_v2.h>
#include <cloud/fastshard/libs/journal/impl/key_buffer_store.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

// The device is partitioned into three parts that follow each other:
// the journal metadata, the journal data and the data itself.
struct TDeviceLayout
{
    ui32 BlockSize = 0;
    ui64 LogMetaBlockCount = 0;
    ui64 LogDataBlockCount = 0;
    ui64 DataBlockCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TDeviceLayout> PartitionDevice(
    const TJournalledDeviceConfig& config)
{
    const ui32 blockSize = config.BlockSize;
    const ui64 blockCount = config.BlocksCount;

    if (!blockSize) {
        return MakeError(E_ARGUMENT, "the device block size is zero");
    }

    const ui64 logMetaSize = config.LogMetaSize;
    const ui64 logDataSize = config.LogDataSize;

    if (logMetaSize % blockSize || logDataSize % blockSize) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal parts " << logMetaSize << " and " << logDataSize
                << " bytes are not multiples of the block size " << blockSize);
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

    return TDeviceLayout{
        .BlockSize = blockSize,
        .LogMetaBlockCount = logMetaBlockCount,
        .LogDataBlockCount = logDataBlockCount,
        .DataBlockCount = blockCount - logMetaBlockCount - logDataBlockCount,
    };
}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceV1Factory final: public IJournalledDeviceFactory
{
private:
    const ILoggingServicePtr Logging;
    const IDeviceManagerPtr DeviceManager;

    TLog Log;

public:
    TJournalledDeviceV1Factory(
        ILoggingServicePtr logging,
        IDeviceManagerPtr deviceManager)
        : Logging(std::move(logging))
        , DeviceManager(std::move(deviceManager))
        , Log(Logging->CreateLog("BLOCKSTORE_JOURNALLED_DEVICE"))
    {}

    TResultOrError<IJournalledDevicePtr> Create(
        const TJournalledDeviceConfig& config) final;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IJournalledDevicePtr> TJournalledDeviceV1Factory::Create(
    const TJournalledDeviceConfig& config)
{
    auto [layout, error] = PartitionDevice(config);
    if (HasError(error)) {
        return error;
    }

    //
    // Skip the journal parts: without the journal the writes go straight to
    // the data, which stays where a journalled device would keep it
    //

    const ui64 logBlockCount =
        layout.LogMetaBlockCount + layout.LogDataBlockCount;

    auto device = CreateJournalledDeviceV1(DeviceManager->CreateDevice(
        config.DeviceUUID,
        {.FirstPageNo = logBlockCount, .PageCount = layout.DataBlockCount},
        layout.BlockSize));

    STORAGE_INFO(
        "Journalled device "
        << config.DeviceUUID.Quote()
        << " created without the journal: skipped journal parts "
        << FormatByteSize(logBlockCount * layout.BlockSize) << ", data "
        << FormatByteSize(layout.DataBlockCount * layout.BlockSize)
        << ", block size " << layout.BlockSize);

    return device;
}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceV2Factory final: public IJournalledDeviceFactory
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
    const auto& uuid = config.DeviceUUID;

    auto [layout, error] = PartitionDevice(config);
    if (HasError(error)) {
        return error;
    }

    const ui32 blockSize = layout.BlockSize;
    const ui64 logMetaBlockCount = layout.LogMetaBlockCount;
    const ui64 logDataBlockCount = layout.LogDataBlockCount;
    const ui64 dataBlockCount = layout.DataBlockCount;

    //
    // Require a few journal metadata blocks: the key buffer store needs a
    // couple of pages for its superblock and at least one for the entries
    //

    constexpr ui64 MinLogMetaBlockCount = 3;

    if (logMetaBlockCount < MinLogMetaBlockCount) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the journal metadata (" << logMetaBlockCount
                             << " blocks) is smaller than the minimum of "
                             << MinLogMetaBlockCount << " blocks");
    }

    if (logMetaBlockCount >= logDataBlockCount ||
        logDataBlockCount >= dataBlockCount)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the journal metadata (" << logMetaBlockCount
                << " blocks) must be smaller than the journal data ("
                << logDataBlockCount << " blocks), which must be smaller "
                << "than the data (" << dataBlockCount << " blocks)");
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
        blockSize,
        CalculateMinRecordBufferSize());

    auto logDataStore = CreateDevicePageStore(
        createAdapter(logMetaBlockCount, logDataBlockCount),
        logDataBlockCount,
        blockSize);

    auto dataStore =
        createAdapter(logMetaBlockCount + logDataBlockCount, dataBlockCount);

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

    STORAGE_INFO(
        "Journalled device "
        << uuid.Quote() << " created: journal "
        << "metadata " << FormatByteSize(logMetaBlockCount * blockSize)
        << ", journal data " << FormatByteSize(logDataBlockCount * blockSize)
        << ", data " << FormatByteSize(dataBlockCount * blockSize)
        << ", block size " << blockSize);

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
        factory = std::make_shared<TJournalledDeviceV1Factory>(
            Logging,
            DeviceManager);
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
        std::move(service));
}

}   // namespace NCloud::NJournalled
