#include "serving_host_observer.h"

#include <cloud/blockstore/libs/diagnostics/volume_stats.h>

#include <util/system/spinlock.h>

#include <atomic>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

std::atomic<ui64> LastConnectionId = 0;

////////////////////////////////////////////////////////////////////////////////

class TServingCellHostObserver final
    : public IServingCellHostObserver
{
private:
    const IVolumeStatsPtr VolumeStats;
    const TString CellId;
    const TString ClientId;
    const ui64 ConnectionId = ++LastConnectionId;

    TAdaptiveLock Lock;
    TString DiskId;   // empty until attached
    TString Fqdn;

public:
    TServingCellHostObserver(
            IVolumeStatsPtr volumeStats,
            TString cellId,
            TString clientId)
        : VolumeStats(std::move(volumeStats))
        , CellId(std::move(cellId))
        , ClientId(std::move(clientId))
    {}

    void OnTabletHostChanged(TString fqdn) noexcept override
    {
        Y_UNUSED(fqdn);
    }

    void OnServingHostChanged(TString fqdn) noexcept override
    {
        with_lock (Lock) {
            Fqdn = std::move(fqdn);
            SetServingCellHostLocked(Fqdn);
        }
    }

    void Attach(const TString& diskId) override
    {
        with_lock (Lock) {
            DiskId = diskId;
            SetServingCellHostLocked(Fqdn);
        }
    }

    void Detach() override
    {
        with_lock (Lock) {
            SetServingCellHostLocked({});
            DiskId.clear();
        }
    }

private:
    void SetServingCellHostLocked(const TString& fqdn)
    {
        if (DiskId) {
            VolumeStats->SetServingCellHost(
                DiskId,
                ClientId,
                ConnectionId,
                CellId,
                fqdn);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServingCellHostObserverPtr CreateServingCellHostObserver(
    IVolumeStatsPtr volumeStats,
    TString cellId,
    TString clientId)
{
    return std::make_shared<TServingCellHostObserver>(
        std::move(volumeStats),
        std::move(cellId),
        std::move(clientId));
}

}   // namespace NCloud::NBlockStore::NCells
