#include "storage_local.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service_local/compound_storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageProvider final
    : public IStorageProvider
{
    const TString LocalAgentId;
    const IStorageProviderPtr Upstream;
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;

    TLog Log;

public:
    TStorageProvider(
            TString localAgentId,
            IStorageProviderPtr upstream,
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats)
        : LocalAgentId(std::move(localAgentId))
        , Upstream(std::move(upstream))
        , Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
    {
        Log = Logging->CreateLog("BLOCKSTORE_LOCAL_STORAGE");
    }

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        if (volume.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD_LOCAL) {
            return MakeFuture<IStoragePtr>(nullptr);
        }

        STORAGE_INFO("Create storage for volume " << volume.GetDiskId());

        if (!volume.DevicesSize()) {
            STORAGE_ERROR("Empty device list");
            return MakeFuture<IStoragePtr>(nullptr);
        }

        const auto& devices = volume.GetDevices();
        const ui32 blockSize = volume.GetBlockSize();

        TVector<TFuture<IStoragePtr>> futures;
        futures.reserve(devices.size());

        TVector<ui64> offsets;
        offsets.reserve(devices.size());

        ui64 offset = 0;
        for (const auto& device: devices) {
            offset += device.GetBlockCount();
            offsets.push_back(offset);

            if (device.GetAgentId() != LocalAgentId) {
                STORAGE_ERROR("Device " << device.GetDeviceUUID().Quote()
                    << " with non local agent id: " << device.GetAgentId().Quote());
                return MakeFuture<IStoragePtr>(nullptr);
            }

            NProto::TVolume chunk;
            chunk.SetDiskId(device.GetDeviceName());
            chunk.SetBlockSize(blockSize);
            chunk.SetBlocksCount(device.GetBlockCount());

            futures.push_back(Upstream->CreateStorage(chunk, clientId, accessMode));
        }

        IServerStatsPtr serverStats = ServerStats;

        return WaitAll(futures).Apply([=] (const auto& future) mutable {
            Y_UNUSED(future);

            TVector<IStoragePtr> storages;
            storages.reserve(futures.size());

            for (auto& x: futures) {
                storages.push_back(x.GetValue());
            }

            return CreateCompoundStorage(
                std::move(storages),
                std::move(offsets),
                volume.GetBlockSize(),
                volume.GetDiskId(),
                std::move(clientId),
                serverStats);
        });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateLocalStorageProvider(
    TString localAgentId,
    IStorageProviderPtr upstream,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats)
{
    return std::make_shared<TStorageProvider>(
        std::move(localAgentId),
        std::move(upstream),
        std::move(logging),
        std::move(serverStats)
    );
}

}   // namespace NCloud::NBlockStore::NServer
