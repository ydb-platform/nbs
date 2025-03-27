#include "part_mirror_state.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TMirrorPartitionState::TMirrorPartitionState(
        TStorageConfigPtr config,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TMigrations migrations,
        TVector<TDevices> replicaDevices)
    : Config(std::move(config))
    , PartConfig(std::move(partConfig))
    , RWClientId(std::move(rwClientId))
    , Migrations(std::move(migrations))
{
    ReplicaInfos.push_back({PartConfig->Fork(PartConfig->GetDevices()), {}});
    for (auto& devices: replicaDevices) {
        ReplicaInfos.push_back({PartConfig->Fork(std::move(devices)), {}});
    }

    ui32 freshDeviceCount = 0;
    for (auto& replicaInfo: ReplicaInfos) {
        freshDeviceCount += replicaInfo.Config->GetFreshDeviceIds().size();
    }

    if (freshDeviceCount != PartConfig->GetFreshDeviceIds().size()) {
        ReportFreshDeviceNotFoundInConfig(TStringBuilder()
            << "Fresh device count mismatch: " << freshDeviceCount
            << " != " << PartConfig->GetFreshDeviceIds().size()
            << " for disk " << PartConfig->GetName());
    }
}

NProto::TError TMirrorPartitionState::Validate()
{
    for (auto& replicaInfo: ReplicaInfos) {
        const auto mainDeviceCount =
            ReplicaInfos.front().Config->GetDevices().size();
        const auto replicaDeviceCount =
            replicaInfo.Config->GetDevices().size();

        if (mainDeviceCount != replicaDeviceCount) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "bad replica device count, main config: " << mainDeviceCount
                << ", replica: " << replicaDeviceCount);
        }
    }

    return {};
}

void TMirrorPartitionState::PrepareMigrationConfig()
{
    if (MigrationConfigPrepared) {
        return;
    }

    if (PrepareMigrationConfigForFreshDevices() ||
        PrepareMigrationConfigForWarningDevices())
    {
        MigrationConfigPrepared = true;
    }
}

bool TMirrorPartitionState::PrepareMigrationConfigForWarningDevices()
{
    if (Migrations.empty()) {
        return false;
    }

    for (auto& replica: ReplicaInfos) {
        const auto& devices = replica.Config->GetDevices();
        const auto& freshDeviceIds = replica.Config->GetFreshDeviceIds();

        for (const auto& d: devices) {
            if (freshDeviceIds.contains(d.GetDeviceUUID())) {
                STORAGE_CHECK_PRECONDITION(false);
                // we should not migrate from fresh devices since they
                // don't contain all the needed data yet
                continue;
            }

            auto mit = FindIf(
                Migrations,
                [&](const NProto::TDeviceMigration& m)
                { return m.GetSourceDeviceId() == d.GetDeviceUUID(); });

            if (mit != Migrations.end()) {
                auto* rm = replica.Migrations.Add();
                rm->SetSourceDeviceId(d.GetDeviceUUID());
                *rm->MutableTargetDevice() = mit->GetTargetDevice();

                // Migrating one device at a time.

                // TODO: Migrate all devices at once.
                // MigrationIndex needs to be restored from volume database for
                // mirrored disks to make it happen. Most probably ReplicaId
                // will need to be stored alongside MigrationIndex.
                // Might be useful to store ReplicationIndex in a separate
                // field. Currently it's stored in MigrationIndex (it will be
                // easy to fix since nothing critical depends on
                // ReplicationIndex
                // - currently it's used only to display ReplicationProgress via
                // the MigrationProgress sensor.

                return true;
            }
        }
    }

    ReportMigrationSourceNotFound();
    // TODO: log details
    return false;
}

bool TMirrorPartitionState::PrepareMigrationConfigForFreshDevices()
{
    auto* replicaInfo = FindIfPtr(ReplicaInfos, [] (const auto& info) {
        return !info.Config->GetFreshDeviceIds().empty();
    });

    if (!replicaInfo) {
        // nothing to replicate
        return false;
    }

    const auto& freshDevices = replicaInfo->Config->GetFreshDeviceIds();
    auto& devices = replicaInfo->Config->AccessDevices();

    // initializing (copying data via migration) one device at a time
    int deviceIdx = 0;
    while (deviceIdx < devices.size()) {
        if (freshDevices.contains(devices[deviceIdx].GetDeviceUUID())) {
            break;
        }

        ++deviceIdx;
    }

    if (deviceIdx == devices.size()) {
        STORAGE_CHECK_PRECONDITION(deviceIdx != devices.size());
        return false;
    }

    // we need to find corresponding good device from some other replica
    for (auto& anotherReplica: ReplicaInfos) {
        const auto& anotherFreshDevices =
            anotherReplica.Config->GetFreshDeviceIds();
        auto& anotherDevices = anotherReplica.Config->AccessDevices();
        auto& anotherDevice = anotherDevices[deviceIdx];
        const auto& uuid = anotherDevice.GetDeviceUUID();

        if (!anotherFreshDevices.contains(uuid)) {
            // we found a good device, lets build our migration config
            auto targetDevice = devices[deviceIdx];
            devices[deviceIdx] = anotherDevice;
            auto& migration = *replicaInfo->Migrations.Add();
            migration.SetSourceDeviceId(uuid);
            *migration.MutableTargetDevice() = std::move(targetDevice);

            // we need to replace anotherDevice with a dummy device
            // since now our migration actor will be responsible for
            // write request replication to this device
            anotherDevice.SetDeviceUUID({});

            return true;
        }
    }

    ReportMigrationSourceNotFound();
    return false;
}

NProto::TError TMirrorPartitionState::NextReadReplica(
    const TBlockRange64 readRange,
    ui32& replicaIndex)
{
    replicaIndex = 0;
    for (ui32 i = 0; i < ReplicaActors.size(); ++i) {
        replicaIndex = ReadReplicaIndex++ % ReplicaActors.size();
        const auto& replicaInfo = ReplicaInfos[replicaIndex];
        if (replicaInfo.Config->DevicesReadyForReading(readRange)) {
            return {};
        }
    }

    return MakeError(E_INVALID_STATE, TStringBuilder() << "range "
        << DescribeRange(readRange) << " targets only fresh/dummy devices");
}

auto TMirrorPartitionState::SplitRangeByDeviceBorders(
    const TBlockRange64 readRange) const -> TVector<TBlockRange64>
{
    return PartConfig->SplitBlockRangeByDevicesBorder(readRange);
}

ui32 TMirrorPartitionState::GetBlockSize() const
{
    return PartConfig->GetBlockSize();
}

ui64 TMirrorPartitionState::GetBlockCount() const
{
    return PartConfig->GetBlockCount();
}

}   // namespace NCloud::NBlockStore::NStorage
