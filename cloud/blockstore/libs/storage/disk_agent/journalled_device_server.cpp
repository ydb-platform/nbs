#include "journalled_device_server.h"

#include "journalled_device_manager.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/fastshard/libs/journal/server/builder.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NJournalled;

namespace {

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress CreateNetworkAddress(TStringBuf s)
{
    TStringBuf hostRef;
    TStringBuf portRef;
    s.RSplit(':', hostRef, portRef);

    return {
        hostRef ? TString(hostRef).c_str() : nullptr,
        FromString<ui16>(portRef)};
}

////////////////////////////////////////////////////////////////////////////////

class TJournalledDeviceServerProvider final
    : public IJournalledDeviceServerProvider
{
private:
    const TDiskAgentConfigConstPtr AgentConfig;
    const ILoggingServicePtr Logging;

    TLog Log;
    TExecutorPtr Executor;
    IStartablePtr Server;

public:
    TJournalledDeviceServerProvider(
            TDiskAgentConfigConstPtr agentConfig,
            ILoggingServicePtr logging)
        : AgentConfig(std::move(agentConfig))
        , Logging(std::move(logging))
    {}

    void Start() override;
    void Stop() override;

    NProto::TError StartServer(
        TVector<NProto::TDeviceConfig> devices,
        THashMap<TString, NProto::TJournalConfig> journalConfigs,
        TDeviceClientPtr deviceClient,
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& diskAgentActorId) override;

private:
    NProto::TError DoStartServer(
        TVector<NProto::TDeviceConfig> devices,
        THashMap<TString, NProto::TJournalConfig> journalConfigs,
        TDeviceClientPtr deviceClient,
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& diskAgentActorId);
};

////////////////////////////////////////////////////////////////////////////////

void TJournalledDeviceServerProvider::Start()
{
    // The disk agent actor starts the server later, when the devices are ready
}

void TJournalledDeviceServerProvider::Stop()
{
    //
    // Stop the server first: it stops the journalled devices, and they need
    // the executor to finish their requests
    //

    if (auto server = std::exchange(Server, nullptr)) {
        server->Stop();
    }

    if (auto executor = std::exchange(Executor, nullptr)) {
        executor->Stop();
    }
}

NProto::TError TJournalledDeviceServerProvider::StartServer(
    TVector<NProto::TDeviceConfig> devices,
    THashMap<TString, NProto::TJournalConfig> journalConfigs,
    TDeviceClientPtr deviceClient,
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& diskAgentActorId)
{
    if (journalConfigs.empty()) {
        return {};
    }

    try {
        return DoStartServer(
            std::move(devices),
            std::move(journalConfigs),
            std::move(deviceClient),
            actorSystem,
            diskAgentActorId);
    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), TString(e.GetMessage()));
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }
}

NProto::TError TJournalledDeviceServerProvider::DoStartServer(
    TVector<NProto::TDeviceConfig> devices,
    THashMap<TString, NProto::TJournalConfig> journalConfigs,
    TDeviceClientPtr deviceClient,
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& diskAgentActorId)
{
    auto address = AgentConfig->GetJournalledDeviceTcpServerListenAddress();
    if (address.empty()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the listen address is not configured, but there are "
                << journalConfigs.size() << " journalled devices");
    }

    TVector<TJournalledDeviceConfig> configs;
    for (const auto& config: devices) {
        auto it = journalConfigs.find(config.GetDeviceUUID());
        if (it != journalConfigs.end()) {
            const auto& journalConfig = it->second;
            configs.emplace_back(
                TJournalledDeviceConfig{
                    .DeviceUUID = config.GetDeviceUUID(),
                    .BlocksCount = config.GetBlocksCount(),
                    .BlockSize = config.GetBlockSize(),
                    .LogMetaSize = journalConfig.GetLogMetaSize(),
                    .LogDataSize = journalConfig.GetLogDataSize()});
            journalConfigs.erase(it);
        }
    }

    if (!journalConfigs.empty()) {
        TStringBuilder missing;
        for (const auto& [id, _]: journalConfigs) {
            missing << (missing.empty() ? "" : ", ") << id.Quote();
        }

        ReportJournalledDeviceCreationError(
            TStringBuilder()
            << "Journalled devices not found among the disk agent devices: "
            << missing);
    }

    if (configs.empty()) {
        return MakeError(
            E_NOT_FOUND,
            "none of the journalled devices is among the disk agent devices");
    }

    auto deviceManager = CreateDeviceManager(
        CreateWallClockTimer(),
        std::move(deviceClient),
        actorSystem,
        diskAgentActorId);

    const ui32 restoreConcurrency =
        Max<ui32>(AgentConfig->GetJournalRestoreConcurrency(), 1);

    //
    // Create the log here and not in the constructor: the daemon creates the
    // provider before the logging service is ready
    //

    Log = Logging->CreateLog("BLOCKSTORE_DISK_AGENT");

    STORAGE_INFO(
        "Starting journalled device TCP server on "
        << address.Quote() << ", restoring up to " << restoreConcurrency
        << " journals at once...");

    //
    // Keep the executor and the server in the fields before the start. If
    // the start fails in the middle, Stop still stops what is running
    //

    Executor = TExecutor::Create("JD");

    Server = NJournalled::TServerBuilder(
        Logging,
        Executor,
        std::move(deviceManager),
        NJournalled::TServerConfig{
            .ListenAddress = CreateNetworkAddress(address),
            .JournalEnabled = AgentConfig->GetJournalEnabled(),
            .RestoreConcurrency = restoreConcurrency,
        },
        std::move(configs)).Build();

    Executor->Start();
    Server->Start();

    STORAGE_INFO(
        "Journalled device TCP server started on " << address.Quote());

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IJournalledDeviceServerProviderPtr CreateJournalledDeviceServerProvider(
    TDiskAgentConfigConstPtr agentConfig,
    ILoggingServicePtr logging)
{
    return std::make_shared<TJournalledDeviceServerProvider>(
        std::move(agentConfig),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore::NStorage
