#include "helpers.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool Consume(TStringBuf& buffer, ui64 len, TString* chunk)
{
    if (buffer.Size() < len) {
        buffer.Clear();
        return false;
    }

    chunk->assign(buffer.Data(), len);
    buffer.Skip(len);
    return true;
}

}   // namespace

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

void Convert(
    NProtoPrivate::TListNodesInternalResponse& internalResponse,
    NProto::TListNodesResponse& response)
{
    if (HasError(internalResponse.GetError())) {
        *response.MutableError() = std::move(*internalResponse.MutableError());
        return;
    }

    response.SetCookie(internalResponse.GetCookie());
    *response.MutableHeaders() = std::move(*internalResponse.MutableHeaders());

    const ui32 nameCount = internalResponse.NameSizesSize();
    const ui32 externalRefCount = internalResponse.ExternalRefIndicesSize();
    response.MutableNames()->Reserve(nameCount);
    response.MutableNodes()->Reserve(nameCount);

    TStringBuf nameBuffer(internalResponse.GetNameBuffer());
    TStringBuf extRefBuffer(internalResponse.GetExternalRefBuffer());

    if (externalRefCount != internalResponse.ShardIdSizesSize()
            || externalRefCount != internalResponse.ShardNodeNameSizesSize())
    {
        *response.MutableError() = MakeError(E_INVALID_STATE, TStringBuilder()
            << "inconsistent external ref/id/nodename counts");
        return;
    }

    bool success = true;
    ui32 j = 0;
    ui32 i = 0;
    while (i < nameCount && success) {
        const ui32 nextExternalNodeRefIdx =
            j < externalRefCount
            ? internalResponse.GetExternalRefIndices(j) : Max<ui32>();

        success &= Consume(
            nameBuffer,
            internalResponse.GetNameSizes(i),
            response.AddNames());

        auto* node = response.AddNodes();
        if (i == nextExternalNodeRefIdx) {
            success &= Consume(
                extRefBuffer,
                internalResponse.GetShardIdSizes(j),
                node->MutableShardFileSystemId());
            success &= Consume(
                extRefBuffer,
                internalResponse.GetShardNodeNameSizes(j),
                node->MutableShardNodeName());
            ++j;
        } else if (i - j < internalResponse.NodesSize()) {
            *node = std::move(*internalResponse.MutableNodes(i - j));
        } else {
            success = false;
        }

        ++i;
    }

    if (!success) {
        *response.MutableError() = MakeError(E_INVALID_STATE, TStringBuilder()
            << "failed to parse response at name " << i);
    }
}

void Store(
    TStringBuf name,
    TStringBuf shardId,
    TStringBuf shardNodeName,
    ui32 i,
    NProtoPrivate::TListNodesInternalResponse& internalResponse)
{
    auto& nameBuffer = *internalResponse.MutableNameBuffer();
    auto& nameSizes = *internalResponse.MutableNameSizes();
    nameBuffer.append(name);
    nameSizes.Add(name.size());

    if (shardId) {
        internalResponse.AddExternalRefIndices(i);

        auto& extRefBuffer = *internalResponse.MutableExternalRefBuffer();
        auto& idSizes = *internalResponse.MutableShardIdSizes();
        auto& nodeNameSizes = *internalResponse.MutableShardNodeNameSizes();

        extRefBuffer.append(shardId);
        idSizes.Add(shardId.size());
        extRefBuffer.append(shardNodeName);
        nodeNameSizes.Add(shardNodeName.size());
    }
}

}   // namespace NCloud::NFileStore::NStorage
