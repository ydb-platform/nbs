#include "group_factory.h"

#include <cloud/fastshard/libs/sn/client/client.h>
#include <cloud/fastshard/libs/storage_group/storage_group_quorum.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::CreateFiberTimer;
using NCloud::NFastShard::CreateNaiveMirroredStorageGroup;
using NCloud::NFastShard::CreateQuorumMirroredStorageGroup;
using NCloud::NFastShard::CreateStorageNodeClient;
using NCloud::NFastShard::IStorageGroupPtr;
using NCloud::NFastShard::TStorageDevice;
using NCloud::NFastShard::TStorageGroupConfig;

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateStorageGroup(
    const TString& fileSystemId,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    // Using only one storage group for now.
    const auto& sg = config.GetStorageGroups(0);

    TVector<TStorageDevice> devices;
    for (const auto& d: sg.GetDevices()) {
        devices.push_back({
            .Node = CreateStorageNodeClient(d.GetHost(), d.GetPort()),
            .DeviceUUID = d.GetDeviceId(),
        });
    }

    TStorageGroupConfig groupConfig;
    groupConfig.ClientId = fileSystemId;
    if (config.GetRetryTotalTimeoutMs()) {
        groupConfig.RetryPolicy.TotalTimeout =
            TDuration::MilliSeconds(config.GetRetryTotalTimeoutMs());
    }
    if (config.GetRetryBackoffIncrementMs()) {
        groupConfig.RetryPolicy.BackoffIncrement =
            TDuration::MilliSeconds(config.GetRetryBackoffIncrementMs());
    }

    if (config.GetPageSize()) {
        groupConfig.PageSize = config.GetPageSize();
    }

    if (sg.GetType() == NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR) {
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

}   // namespace NCloud::NFileStore::NStorage::NFastShard
