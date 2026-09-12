#include "group_factory.h"

#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group_quorum.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStorageGroupFactory: IStorageGroupFactory
{
    IStorageGroupPtr MakeStorageGroup(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 generation) override
    {
        TVector<TStorageDevice> devices;
        const auto& sg = config.GetStorageGroups(0);
        for (const auto& d: sg.GetDevices()) {
            devices.push_back({
                .Node = CreateStorageNodeClient(d.GetHost(), d.GetPort()),
                .DeviceUUID = d.GetDeviceId(),
            });
        }

        TStorageGroupConfig groupConfig;
        // TODO(#5894): set client id
        groupConfig.AcquireGeneration = generation;
        if (config.GetRetryTotalTimeoutMs()) {
            groupConfig.RetryPolicy.TotalTimeout =
                TDuration::MilliSeconds(config.GetRetryTotalTimeoutMs());
        }
        if (config.GetRetryBackoffIncrementMs()) {
            groupConfig.RetryPolicy.BackoffIncrement =
                TDuration::MilliSeconds(config.GetRetryBackoffIncrementMs());
        }

        groupConfig.JournalRestoreEnabled = config.GetJournalRestoreEnabled();

        if (sg.GetType() == NProtoPrivate::TStorageGroup::E_SG_QUORUM_MIRROR) {
            return CreateQuorumMirroredStorageGroup(
                std::move(groupConfig),
                std::move(devices),
                CreateFiberTimer());
        }

        return CreateNaiveMirroredStorageGroup(
            std::move(groupConfig),
            std::move(devices),
            CreateFiberTimer());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupFactoryPtr CreateStorageGroupFactory()
{
    return std::make_shared<TStorageGroupFactory>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
