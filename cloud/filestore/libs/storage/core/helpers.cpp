#include "helpers.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool Consume(TStringBuf& input, ui64 len, TString* chunk)
{
    if (input.Size() < len) {
        input.Clear();
        return false;
    }

    chunk->assign(input.Data(), len);
    input.Skip(len);
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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

struct TListNodesInternalResponseBuilder::TImpl
{
    google::protobuf::RepeatedField<ui32>& NameSizes;
    google::protobuf::RepeatedField<ui32>& ExternalRefIndices;
    google::protobuf::RepeatedField<ui32>& ShardIdSizes;
    google::protobuf::RepeatedField<ui32>& ShardNodeNameSizes;
    char* NameBuffer;
    ui64 NameBufferSize;
    char* ExternalRefBuffer;
    ui64 ExternalRefBufferSize;
    ui32 Index = 0;

    TImpl(
            NProtoPrivate::TListNodesInternalResponse& response,
            ui64 nameBufferSize,
            ui64 externalRefBufferSize,
            ui64 nameCount,
            ui64 externalRefCount)
        : NameSizes(*response.MutableNameSizes())
        , ExternalRefIndices(*response.MutableExternalRefIndices())
        , ShardIdSizes(*response.MutableShardIdSizes())
        , ShardNodeNameSizes(*response.MutableShardNodeNameSizes())
        , NameBufferSize(nameBufferSize)
        , ExternalRefBufferSize(externalRefBufferSize)
    {
        auto& nameBuffer = *response.MutableNameBuffer();
        nameBuffer.ReserveAndResize(nameBufferSize);
        NameBuffer = nameBuffer.begin();
        auto& externalRefBuffer = *response.MutableExternalRefBuffer();
        externalRefBuffer.ReserveAndResize(externalRefBufferSize);
        ExternalRefBuffer = externalRefBuffer.begin();

        NameSizes.Reserve(nameCount);
        ExternalRefIndices.Reserve(externalRefCount);
        ShardIdSizes.Reserve(externalRefCount);
        ShardNodeNameSizes.Reserve(externalRefCount);
    }

    bool AddNodeRef(
        const TString& name,
        const TString& shardId,
        const TString& shardNodeName)
    {
        if (name.size() > NameBufferSize) {
            return false;
        }

        const ui64 externalRefSize = shardId.size() + shardNodeName.size();
        if (shardId && externalRefSize > ExternalRefBufferSize) {
            return false;
        }

        memcpy(NameBuffer, name.data(), name.size());
        NameBuffer += name.size();
        NameBufferSize -= name.size();
        NameSizes.Add(name.size());

        if (shardId) {
            ExternalRefIndices.Add(Index);

            memcpy(
                ExternalRefBuffer,
                shardId.data(),
                shardId.size());
            ExternalRefBuffer += shardId.size();
            ExternalRefBufferSize -= shardId.size();
            ShardIdSizes.Add(shardId.size());
            memcpy(
                ExternalRefBuffer,
                shardNodeName.data(),
                shardNodeName.size());
            ExternalRefBuffer += shardNodeName.size();
            ExternalRefBufferSize -= shardNodeName.size();
            ShardNodeNameSizes.Add(shardNodeName.size());
        }

        ++Index;
        return true;
    }

    ui32 GetIndex() const
    {
        return Index;
    }
};

TListNodesInternalResponseBuilder::TListNodesInternalResponseBuilder(
        NProtoPrivate::TListNodesInternalResponse& response,
        ui64 nameBufferSize,
        ui64 externalRefBufferSize,
        ui64 nameCount,
        ui64 externalRefCount)
    : Impl(new TImpl(
        response,
        nameBufferSize,
        externalRefBufferSize,
        nameCount,
        externalRefCount))
{}

TListNodesInternalResponseBuilder::~TListNodesInternalResponseBuilder()
{}

bool TListNodesInternalResponseBuilder::AddNodeRef(
    const TString& name,
    const TString& shardId,
    const TString& shardNodeName)
{
    return Impl->AddNodeRef(name, shardId, shardNodeName);
}

ui32 TListNodesInternalResponseBuilder::GetIndex() const
{
    return Impl->GetIndex();
}

}   // namespace NCloud::NFileStore::NStorage
