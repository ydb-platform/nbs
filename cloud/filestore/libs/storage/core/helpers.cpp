#include "helpers.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <ydb/core/protos/filestore_config.pb.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStorePerformanceProfile& performanceProfile)
{
    performanceProfile.SetThrottlingEnabled(
        config.GetPerformanceProfileThrottlingEnabled());
    performanceProfile.SetMaxReadBandwidth(
        config.GetPerformanceProfileMaxReadBandwidth());
    performanceProfile.SetMaxReadIops(
        config.GetPerformanceProfileMaxReadIops());
    performanceProfile.SetMaxWriteBandwidth(
        config.GetPerformanceProfileMaxWriteBandwidth());
    performanceProfile.SetMaxWriteIops(
        config.GetPerformanceProfileMaxWriteIops());
    performanceProfile.SetBoostTime(
        config.GetPerformanceProfileBoostTime());
    performanceProfile.SetBoostRefillTime(
        config.GetPerformanceProfileBoostRefillTime());
    performanceProfile.SetBoostPercentage(
        config.GetPerformanceProfileBoostPercentage());
    performanceProfile.SetBurstPercentage(
        config.GetPerformanceProfileBurstPercentage());
    performanceProfile.SetDefaultPostponedRequestWeight(
        config.GetPerformanceProfileDefaultPostponedRequestWeight());
    performanceProfile.SetMaxPostponedWeight(
        config.GetPerformanceProfileMaxPostponedWeight());
    performanceProfile.SetMaxWriteCostMultiplier(
        config.GetPerformanceProfileMaxWriteCostMultiplier());
    performanceProfile.SetMaxPostponedTime(
        config.GetPerformanceProfileMaxPostponedTime());
    performanceProfile.SetMaxPostponedCount(
        config.GetPerformanceProfileMaxPostponedCount());
}

void Convert(
    const NProto::TFileStorePerformanceProfile& performanceProfile,
    NKikimrFileStore::TConfig& config)
{
    config.SetPerformanceProfileThrottlingEnabled(
        performanceProfile.GetThrottlingEnabled());
    config.SetPerformanceProfileMaxReadBandwidth(
        performanceProfile.GetMaxReadBandwidth());
    config.SetPerformanceProfileMaxReadIops(
        performanceProfile.GetMaxReadIops());
    config.SetPerformanceProfileMaxWriteBandwidth(
        performanceProfile.GetMaxWriteBandwidth());
    config.SetPerformanceProfileMaxWriteIops(
        performanceProfile.GetMaxWriteIops());
    config.SetPerformanceProfileBoostTime(
        performanceProfile.GetBoostTime());
    config.SetPerformanceProfileBoostRefillTime(
        performanceProfile.GetBoostRefillTime());
    config.SetPerformanceProfileBoostPercentage(
        performanceProfile.GetBoostPercentage());
    config.SetPerformanceProfileBurstPercentage(
        performanceProfile.GetBurstPercentage());
    config.SetPerformanceProfileDefaultPostponedRequestWeight(
        performanceProfile.GetDefaultPostponedRequestWeight());
    config.SetPerformanceProfileMaxPostponedWeight(
        performanceProfile.GetMaxPostponedWeight());
    config.SetPerformanceProfileMaxWriteCostMultiplier(
        performanceProfile.GetMaxWriteCostMultiplier());
    config.SetPerformanceProfileMaxPostponedTime(
        performanceProfile.GetMaxPostponedTime());
    config.SetPerformanceProfileMaxPostponedCount(
        performanceProfile.GetMaxPostponedCount());
}


template <>
ui64 CalculateByteCount<NProto::TWriteDataRequest>(
    const NProto::TWriteDataRequest& request)
{
    if (!request.GetBuffer().empty()) {
        return request.GetBuffer().size();
    }

    ui64 byteCount = 0;
    for (const auto& iovec: request.GetIovecs()) {
        byteCount += iovec.GetLength();
    }

    return byteCount;
}

}   // namespace NCloud::NFileStore::NStorage
