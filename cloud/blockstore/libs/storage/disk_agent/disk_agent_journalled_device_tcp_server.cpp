#include "disk_agent_actor.h"

#include "journalled_device_manager.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/fastshard/journal/server/builder.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NJournalled;
using namespace NKikimr;
using namespace NThreading;

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError TDiskAgentActor::StartJournalledDeviceTcpServer(
    const NActors::TActorContext& ctx,
    const THashMap<TString, NProto::TJournalConfig>& journalledDevices)
{
    if (journalledDevices.empty()) {
        return {};
    }

    auto address = AgentConfig->GetJournalledDeviceTcpServerListenAddress();
    if (address.empty()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "the listen address is not configured, but there are "
                << journalledDevices.size() << " journalled devices");
    }

    if (!State) {
        return MakeError(
            E_INVALID_STATE,
            "the disk agent state is not initialized");
    }

    auto journalConfigs = journalledDevices;

    TVector<TJournalledDeviceConfig> configs;
    for (const auto& config: State->GetDevices()) {
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

        ReportDiskAgentJournalledDeviceCreationError(
            "Journalled devices not found among the disk agent devices",
            {{"devices", missing}});
    }

    if (configs.empty()) {
        return MakeError(
            E_NOT_FOUND,
            "none of the journalled devices is among the disk agent devices");
    }

    Executor = TExecutor::Create("JD");

    auto deviceManager = CreateDeviceManager(
        CreateWallClockTimer(),
        State->GetDeviceClient(),
        TActivationContext::ActorSystem(),
        ctx.SelfID);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Starting journalled device TCP server on "
            << address.Quote() << ", restoring up to "
            << AgentConfig->GetJournalRestoreConcurrency()
            << " journals at once...");

    try {
        const auto listenAddress = CreateNetworkAddress(address);

        JournalledDeviceTcpServer = NJournalled::TServerBuilder(
            Logging,
            Executor,
            deviceManager,
            listenAddress,
            AgentConfig->GetJournalEnabled(),
            AgentConfig->GetJournalRestoreConcurrency(),
            std::move(configs)).Build();

        Executor->Start();

        JournalledDeviceTcpServer->Start();

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Journalled device TCP server started on " << address.Quote());

        return {};

    } catch (const TServiceError& e) {
        return MakeError(e.GetCode(), TString(e.GetMessage()));
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
