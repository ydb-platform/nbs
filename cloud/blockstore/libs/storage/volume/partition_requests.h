#pragma once

#include "partition_info.h"
#include "tracing.h"
#include "volume_events_private.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/volume/model/merge.h>
#include <cloud/blockstore/libs/storage/volume/model/stripe.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/event.h>

#include <library/cpp/lwtrace/all.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
struct TPartitionRequest
{
    NActors::TActorId ActorId;
    ui64 TabletId = 0;
    ui32 PartitionId = Max<ui32>();
    TBlockRange64 BlockRange;
    std::unique_ptr<typename TMethod::TRequest> Event;
};

template <typename TMethod>
bool ToPartitionRequests(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const typename TMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TMethod>>* requests,
    TBlockRange64* blockRange);

template <typename TMethod>
ui32 InitPartitionRequest(
    const TPartitionInfoList& partitions,
    const ui32 originalBlocksCount,
    const ui32 blocksPerStripe,
    const ui32 i,
    const typename TMethod::TRequest::ProtoRecordType& proto,
    TPartitionRequest<TMethod>& request)
{
    const auto blockRange = TBlockRange64::WithLength(
        proto.GetStartIndex(),
        originalBlocksCount
    );
    const auto stripeInfo = ConvertToRelativeBlockRange(
        blocksPerStripe,
        blockRange,
        partitions.size(),
        i
    );
    const auto& partition = partitions[stripeInfo.PartitionId];

    request.ActorId = partition.Owner,
    request.TabletId = partition.TabletId;
    request.PartitionId = stripeInfo.PartitionId;
    request.BlockRange = stripeInfo.BlockRange;

    request.Event = std::make_unique<typename TMethod::TRequest>();
    request.Event->Record.MutableHeaders()->CopyFrom(proto.GetHeaders());
    request.Event->Record.SetDiskId(proto.GetDiskId());
    request.Event->Record.SetStartIndex(stripeInfo.BlockRange.Start);

    return stripeInfo.BlockRange.Size();
}

template <typename TMethod>
ui32 InitIORequest(
    const TPartitionInfoList& partitions,
    const ui32 originalBlocksCount,
    const ui32 blocksPerStripe,
    const ui32 i,
    const typename TMethod::TRequest::ProtoRecordType& proto,
    TPartitionRequest<TMethod>& request)
{
    const auto blocksCount = InitPartitionRequest<TMethod>(
        partitions,
        originalBlocksCount,
        blocksPerStripe,
        i,
        proto,
        request
    );

    request.Event->Record.SetFlags(proto.GetFlags());

    return blocksCount;
}

template <>
bool ToPartitionRequests<TEvService::TWriteBlocksMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvService::TWriteBlocksMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvService::TWriteBlocksMethod>>* requests,
    TBlockRange64* blockRange)
{
    auto& proto = ev->Get()->Record;
    auto& buffers = *proto.MutableBlocks()->MutableBuffers();

    ui32 bufferSize = 0;
    for (const auto& buffer: buffers) {
        Y_ABORT_UNLESS(bufferSize == 0 || bufferSize == buffer.Size());
        bufferSize = buffer.Size();
    }

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    for (ui32 i = 0; i < requests->size(); ++i) {
        auto& request = (*requests)[i];
        const auto blocksCount = InitIORequest<TEvService::TWriteBlocksMethod>(
            partitions,
            blockRange->Size(),
            blocksPerStripe,
            i,
            proto,
            request
        );
        request.Event->Record.SetSessionId(proto.GetSessionId());

        for (ui32 j = 0; j < blocksCount; ++j) {
            const auto index = RelativeToGlobalIndex(
                blocksPerStripe,
                request.BlockRange.Start + j,
                partitions.size(),
                request.PartitionId
            );

            if (bufferSize == blockSize) {
                *request.Event->Record.MutableBlocks()->AddBuffers() =
                    std::move(buffers[index - proto.GetStartIndex()]);
            } else {
                ui32 relativeBlockIndex = index - proto.GetStartIndex();
                ui32 relativeByteIndex = relativeBlockIndex * blockSize;
                ui32 bufferIndex = relativeByteIndex / bufferSize;
                ui32 offsetInBuffer = relativeByteIndex - bufferIndex * bufferSize;

                auto& block = *request.Event->Record.MutableBlocks()->AddBuffers();
                block.resize(blockSize);
                memcpy(
                    block.begin(),
                    buffers[bufferIndex].begin() + offsetInBuffer,
                    blockSize);
            }
        }
    }

    return true;
}

template <>
bool ToPartitionRequests<TEvService::TReadBlocksMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvService::TReadBlocksMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvService::TReadBlocksMethod>>* requests,
    TBlockRange64* blockRange)
{
    const auto& proto = ev->Get()->Record;

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    for (ui32 i = 0; i < requests->size(); ++i) {
        const auto blocksCount = InitIORequest<TEvService::TReadBlocksMethod>(
            partitions,
            proto.GetBlocksCount(),
            blocksPerStripe,
            i,
            proto,
            (*requests)[i]
        );
        (*requests)[i].Event->Record.SetBlocksCount(blocksCount);
        (*requests)[i].Event->Record.SetCheckpointId(proto.GetCheckpointId());
        (*requests)[i].Event->Record.SetSessionId(proto.GetSessionId());
    }

    return true;
}

template <>
bool ToPartitionRequests<TEvService::TZeroBlocksMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvService::TZeroBlocksMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvService::TZeroBlocksMethod>>* requests,
    TBlockRange64* blockRange)
{
    Y_UNUSED(blockSize);

    const auto& proto = ev->Get()->Record;

    *blockRange =  BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    for (ui32 i = 0; i < requests->size(); ++i) {
        auto& request = (*requests)[i];
        const auto blocksCount = InitIORequest<TEvService::TZeroBlocksMethod>(
            partitions,
            proto.GetBlocksCount(),
            blocksPerStripe,
            i,
            proto,
            request
        );
        request.Event->Record.SetBlocksCount(blocksCount);
        request.Event->Record.SetSessionId(proto.GetSessionId());
    }

    return true;
}

template <typename TMethod>
ui32 InitLocalRequest(
    const TPartitionInfoList& partitions,
    const ui32 blocksPerStripe,
    const ui32 i,
    const typename TMethod::TRequest::ProtoRecordType& proto,
    const TSgList& blockDatas,
    TPartitionRequest<TMethod>& request)
{
    const auto blocksCount = InitIORequest<TMethod>(
        partitions,
        GetBlocksCount(proto),
        blocksPerStripe,
        i,
        proto,
        request
    );

    TSgList sglist;
    for (ui32 j = 0; j < blocksCount; ++j) {
        const auto index = RelativeToGlobalIndex(
            blocksPerStripe,
            request.BlockRange.Start + j,
            partitions.size(),
            request.PartitionId
        );
        sglist.push_back(blockDatas[index - proto.GetStartIndex()]);
    }

    request.Event->Record.Sglist = proto.Sglist.Create(sglist);
    request.Event->Record.SetSessionId(proto.GetSessionId());

    return blocksCount;
}

template <>
bool ToPartitionRequests<TEvService::TWriteBlocksLocalMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvService::TWriteBlocksLocalMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvService::TWriteBlocksLocalMethod>>* requests,
    TBlockRange64* blockRange)
{
    const auto& proto = ev->Get()->Record;

    auto guard = proto.Sglist.Acquire();
    if (!guard) {
        return false;
    }

    const auto& blockDatas = guard.Get();

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    Y_ABORT_UNLESS(
        blockDatas.size() == blockRange->Size(),
        "BlocksCount != sglist.size()");

    for (ui32 i = 0; i < requests->size(); ++i) {
        auto& request = (*requests)[i];
        const auto blocksCount = InitLocalRequest<TEvService::TWriteBlocksLocalMethod>(
            partitions,
            blocksPerStripe,
            i,
            proto,
            blockDatas,
            request
        );

        request.Event->Record.BlocksCount = blocksCount;
        request.Event->Record.BlockSize = blockSize;
    }

    return true;
}

template <>
bool ToPartitionRequests<TEvService::TReadBlocksLocalMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvService::TReadBlocksLocalMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvService::TReadBlocksLocalMethod>>* requests,
    TBlockRange64* blockRange)
{
    const auto& proto = ev->Get()->Record;

    auto guard = proto.Sglist.Acquire();
    if (!guard) {
        return false;
    }

    const auto& blockDatas = guard.Get();

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    for (ui32 i = 0; i < requests->size(); ++i) {
        auto& request = (*requests)[i];
        const auto blocksCount = InitLocalRequest<TEvService::TReadBlocksLocalMethod>(
            partitions,
            blocksPerStripe,
            i,
            proto,
            blockDatas,
            request
        );

        request.Event->Record.SetBlocksCount(blocksCount);
        request.Event->Record.SetCheckpointId(proto.GetCheckpointId());
        request.Event->Record.BlockSize = blockSize;
    }

    return true;
}

template <>
bool ToPartitionRequests<TEvVolume::TDescribeBlocksMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TDescribeBlocksMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TDescribeBlocksMethod>>* requests,
    TBlockRange64* blockRange)
{
    const auto& proto = ev->Get()->Record;

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    for (ui32 i = 0; i < requests->size(); ++i) {
        const auto blocksCount = InitIORequest<TEvVolume::TDescribeBlocksMethod>(
            partitions,
            proto.GetBlocksCount(),
            blocksPerStripe,
            i,
            proto,
            (*requests)[i]
        );
        (*requests)[i].Event->Record.SetBlocksCount(blocksCount);
        (*requests)[i].Event->Record.SetCheckpointId(proto.GetCheckpointId());
    }

    return true;
}

template <>
bool ToPartitionRequests<TEvService::TGetChangedBlocksMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvService::TGetChangedBlocksMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvService::TGetChangedBlocksMethod>>* requests,
    TBlockRange64* blockRange)
{
    const auto& proto = ev->Get()->Record;

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    for (ui32 i = 0; i < requests->size(); ++i) {
        const auto blocksCount = InitPartitionRequest<TEvService::TGetChangedBlocksMethod>(
            partitions,
            proto.GetBlocksCount(),
            blocksPerStripe,
            i,
            proto,
            (*requests)[i]
        );
        (*requests)[i].Event->Record.SetBlocksCount(blocksCount);
        (*requests)[i].Event->Record.SetLowCheckpointId(proto.GetLowCheckpointId());
        (*requests)[i].Event->Record.SetHighCheckpointId(proto.GetHighCheckpointId());
        (*requests)[i].Event->Record.SetIgnoreBaseDisk(proto.GetIgnoreBaseDisk());
    }

    return true;
}

template <>
bool ToPartitionRequests<TEvVolume::TGetPartitionInfoMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TGetPartitionInfoMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TGetPartitionInfoMethod>>* requests,
    TBlockRange64* blockRange)
{
    Y_UNUSED(blockSize);
    Y_UNUSED(blocksPerStripe);
    Y_UNUSED(blockRange);

    requests->resize(1);

    const auto partitionId = ev->Get()->Record.GetPartitionId();
    const auto& partition = partitions[partitionId];
    (*requests)[0].ActorId = partition.Owner;
    (*requests)[0].TabletId = partition.TabletId;
    (*requests)[0].PartitionId = partitionId;
    (*requests)[0].Event = std::make_unique<TEvVolume::TEvGetPartitionInfoRequest>();
    (*requests)[0].Event->Record = std::move(ev->Get()->Record);

    return true;
}

template <>
bool ToPartitionRequests<TEvVolume::TCompactRangeMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TCompactRangeMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TCompactRangeMethod>>* requests,
    TBlockRange64* blockRange)
{
    const auto& proto = ev->Get()->Record;

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    for (ui32 i = 0; i < requests->size(); ++i) {
        const auto blocksCount = InitIORequest<TEvVolume::TCompactRangeMethod>(
            partitions,
            proto.GetBlocksCount(),
            blocksPerStripe,
            i,
            proto,
            (*requests)[i]
        );
        (*requests)[i].Event->Record.SetBlocksCount(blocksCount);
        (*requests)[i].Event->Record.SetOperationId(ev->Get()->Record.GetOperationId());
    }

    return true;
}

template <typename TMethod>
bool ToPartitionRequestsSimple(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const typename TMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TMethod>>* requests,
    TBlockRange64* blockRange)
{
    Y_UNUSED(blockSize);
    Y_UNUSED(blocksPerStripe);
    Y_UNUSED(blockRange);

    requests->resize(partitions.size());

    for (ui32 i = 0; i < partitions.size(); ++i) {
        (*requests)[i].ActorId = partitions[i].Owner;
        (*requests)[i].TabletId = partitions[i].TabletId;
        (*requests)[i].PartitionId = i;
        (*requests)[i].Event = std::make_unique<typename TMethod::TRequest>();
        (*requests)[i].Event->Record = ev->Get()->Record;
    }

    return true;
}

template <>
bool ToPartitionRequests<TEvVolume::TGetCompactionStatusMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TGetCompactionStatusMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TGetCompactionStatusMethod>>* requests,
    TBlockRange64* blockRange)
{
    return ToPartitionRequestsSimple(
        partitions,
        blockSize,
        blocksPerStripe,
        ev,
        requests,
        blockRange);
}

template <>
bool ToPartitionRequests<TEvVolume::TRebuildMetadataMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TRebuildMetadataMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TRebuildMetadataMethod>>* requests,
    TBlockRange64* blockRange)
{
    return ToPartitionRequestsSimple(
        partitions,
        blockSize,
        blocksPerStripe,
        ev,
        requests,
        blockRange);
}

template <>
bool ToPartitionRequests<TEvVolume::TGetRebuildMetadataStatusMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TGetRebuildMetadataStatusMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TGetRebuildMetadataStatusMethod>>* requests,
    TBlockRange64* blockRange)
{
    return ToPartitionRequestsSimple(
        partitions,
        blockSize,
        blocksPerStripe,
        ev,
        requests,
        blockRange);
}

template <>
bool ToPartitionRequests<TEvVolume::TScanDiskMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TScanDiskMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TScanDiskMethod>>* requests,
    TBlockRange64* blockRange)
{
    return ToPartitionRequestsSimple(
        partitions,
        blockSize,
        blocksPerStripe,
        ev,
        requests,
        blockRange);
}

template <>
bool ToPartitionRequests<TEvVolume::TGetScanDiskStatusMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TGetScanDiskStatusMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TGetScanDiskStatusMethod>>* requests,
    TBlockRange64* blockRange)
{
    return ToPartitionRequestsSimple(
        partitions,
        blockSize,
        blocksPerStripe,
        ev,
        requests,
        blockRange);
}

template <>
bool ToPartitionRequests<TEvVolume::TGetUsedBlocksMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TGetUsedBlocksMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TGetUsedBlocksMethod>>* requests,
    TBlockRange64* blockRange)
{
    Y_UNUSED(blockSize);
    Y_UNUSED(blocksPerStripe);
    Y_UNUSED(ev);
    Y_UNUSED(requests);
    Y_UNUSED(blockRange);

    Y_ABORT_UNLESS(!partitions.empty());
    STORAGE_VERIFY_C(
        false,
        NCloud::TWellKnownEntityTypes::DISK,
        partitions[0].PartitionConfig.GetDiskId(),
        TStringBuilder() << "ToPartitionRequests not implemented for "
                            "TEvVolume::TGetUsedBlocksMethod");

    return false;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TPartitionRequestActor final
    : public NActors::TActorBootstrapped<TPartitionRequestActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NActors::TActorId VolumeActorId;
    const ui64 VolumeRequestId;
    const TBlockRange64 OriginalRange;
    const ui32 BlocksPerStripe;
    const ui32 BlockSize;
    const ui32 PartitionsCount;
    TVector<TPartitionRequest<TMethod>> PartitionRequests;
    const TRequestTraceInfo TraceInfo;
    ui32 Responses = 0;
    typename TMethod::TResponse::ProtoRecordType Record;

    using TBase = NActors::TActorBootstrapped<TPartitionRequestActor<TMethod>>;

    TVector<TCallContextPtr> ChildCallContexts;

public:
    TPartitionRequestActor(
        TRequestInfoPtr requestInfo,
        NActors::TActorId volumeActorId,
        ui64 volumeRequestId,
        TBlockRange64 originalRange,
        ui32 blocksPerStripe,
        ui32 blockSize,
        ui32 partitionsCount,
        TVector<TPartitionRequest<TMethod>> partitionRequests,
        TRequestTraceInfo traceInfo);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void Prepare(const NActors::TActorContext& ctx);

    void HandlePartitionResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename T>
    static void MergeCommonFields(const T& src, T& dst)
    {
        if (src.GetError().GetCode() > dst.GetError().GetCode()) {
            dst.MutableError()->CopyFrom(src.GetError());
        }
    }

    void Merge(
        NProto::TReadBlocksResponse& src,
        ui32 requestNo,
        NProto::TReadBlocksResponse& dst)
    {
        auto& buffers = *dst.MutableBlocks()->MutableBuffers();
        buffers.Reserve(OriginalRange.Size());
        while (static_cast<ui32>(buffers.size()) < OriginalRange.Size()) {
            buffers.Add();
        }

        auto& srcBuffers = *src.MutableBlocks()->MutableBuffers();
        for (ui32 i = 0; i < static_cast<ui32>(srcBuffers.size()); ++i) {
            const auto index = RelativeToGlobalIndex(
                BlocksPerStripe,
                PartitionRequests[requestNo].BlockRange.Start + i,
                PartitionsCount,
                PartitionRequests[requestNo].PartitionId
            );

            buffers[index - OriginalRange.Start] = std::move(srcBuffers[i]);
        }
    }

    void Merge(
        NProto::TCompactRangeResponse& src,
        ui32 requestNo,
        NProto::TCompactRangeResponse& dst)
    {
        Y_UNUSED(requestNo);

        dst.SetOperationId(src.GetOperationId());
    }

    void Merge(
        NProto::TGetCompactionStatusResponse& src,
        ui32 requestNo,
        NProto::TGetCompactionStatusResponse& dst)
    {
        Y_UNUSED(requestNo);

        dst.SetProgress(src.GetProgress() + dst.GetProgress());
        dst.SetTotal(src.GetTotal() + dst.GetTotal());
        dst.SetIsCompleted(src.GetIsCompleted() && dst.GetIsCompleted());
    }

    void Merge(
        NProto::TGetChangedBlocksResponse& src,
        ui32 requestNo,
        NProto::TGetChangedBlocksResponse& dst)
    {
        MergeStripedBitMask(
            OriginalRange,
            PartitionRequests[requestNo].BlockRange,
            BlocksPerStripe,
            PartitionsCount,
            PartitionRequests[requestNo].PartitionId,
            src.GetMask(),
            *dst.MutableMask()
        );
    }

    void Merge(
        NProto::TRebuildMetadataResponse& src,
        ui32 requestNo,
        NProto::TRebuildMetadataResponse& dst)
    {
        Y_UNUSED(requestNo);
        if (FAILED(src.GetError().GetCode()) ||
            src.GetError().GetCode() != S_OK)
        {
            *dst.MutableError() = std::move(*src.MutableError());
        }
    }

    void Merge(
        NProto::TGetRebuildMetadataStatusResponse& src,
        ui32 requestNo,
        NProto::TGetRebuildMetadataStatusResponse& dst)
    {
        Y_UNUSED(requestNo);

        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
        } else {
            const auto& s = src.GetProgress();
            const auto& d = dst.GetProgress();
            bool isCompleted = Responses ? d.GetIsCompleted() : true;

            dst.MutableProgress()->SetProcessed(
                s.GetProcessed() + d.GetProcessed());
            dst.MutableProgress()->SetTotal(
                s.GetTotal() + d.GetTotal());
            dst.MutableProgress()->SetIsCompleted(
                s.GetIsCompleted() && isCompleted);
        }
    }

    void Merge(
        NProto::TScanDiskResponse& src,
        ui32 requestNo,
        NProto::TScanDiskResponse& dst)
    {
        Y_UNUSED(requestNo);
        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
        }
    }

    void Merge(
        NProto::TGetScanDiskStatusResponse& src,
        ui32 requestNo,
        NProto::TGetScanDiskStatusResponse& dst)
    {
        Y_UNUSED(requestNo);

        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
        } else {
            const auto& srcProgress = src.GetProgress();
            const auto& dstProgress = dst.GetProgress();

            dst.MutableProgress()->SetProcessed(
                srcProgress.GetProcessed() + dstProgress.GetProcessed());
            dst.MutableProgress()->SetTotal(
                srcProgress.GetTotal() + dstProgress.GetTotal());

            const bool dstIsCompleted = Responses
                ? dstProgress.GetIsCompleted()
                : true;
            dst.MutableProgress()->SetIsCompleted(
                srcProgress.GetIsCompleted() && dstIsCompleted);

            const auto& srcBrokenBlobs = srcProgress.GetBrokenBlobs();
            for (int i = 0; i < srcBrokenBlobs.size(); ++i) {
                auto& dstBrokenBlob = *dst.MutableProgress()->AddBrokenBlobs();
                dstBrokenBlob.SetRawX1(srcBrokenBlobs.at(i).GetRawX1());
                dstBrokenBlob.SetRawX2(srcBrokenBlobs.at(i).GetRawX2());
                dstBrokenBlob.SetRawX3(srcBrokenBlobs.at(i).GetRawX3());
            }
        }
    }

    void Merge(
        NProto::TDescribeBlocksResponse& src,
        ui32 requestNo,
        NProto::TDescribeBlocksResponse& dst)
    {
        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
            return;
        }

        MergeDescribeBlocksResponse(
            src,
            dst,
            BlocksPerStripe,
            BlockSize,
            PartitionsCount,
            PartitionRequests[requestNo].PartitionId);
    }

    template <typename T>
    void Merge(
        T& src,
        ui32 requestNo,
        T& dst)
    {
        Y_UNUSED(requestNo);
        MergeCommonFields(src, dst);
    }

    void NotifyCompleted(const NActors::TActorContext& ctx);

    void ForkTraces(TCallContextPtr callContext)
    {
        auto& cc = RequestInfo->CallContext;
        if (!cc->LWOrbit.Fork(callContext->LWOrbit)) {
            LWTRACK(ForkFailed, cc->LWOrbit, TMethod::Name, cc->RequestId);
        }

        ChildCallContexts.push_back(callContext);
    }

    void JoinTraces(ui32 cookie)
    {
        if (cookie < ChildCallContexts.size()) {
            if (ChildCallContexts[cookie]) {
                auto& cc = RequestInfo->CallContext;
                cc->LWOrbit.Join(ChildCallContexts[cookie]->LWOrbit);
                ChildCallContexts[cookie].Reset();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TPartitionRequestActor<TMethod>::TPartitionRequestActor(
        TRequestInfoPtr requestInfo,
        NActors::TActorId volumeActorId,
        ui64 volumeRequestId,
        TBlockRange64 originalRange,
        ui32 blocksPerStripe,
        ui32 blockSize,
        ui32 partitionsCount,
        TVector<TPartitionRequest<TMethod>> partitionRequests,
        TRequestTraceInfo traceInfo)
    : RequestInfo(std::move(requestInfo))
    , VolumeActorId(volumeActorId)
    , VolumeRequestId(volumeRequestId)
    , OriginalRange(originalRange)
    , BlocksPerStripe(blocksPerStripe)
    , BlockSize(blockSize)
    , PartitionsCount(partitionsCount)
    , PartitionRequests(std::move(partitionRequests))
    , TraceInfo(std::move(traceInfo))
    , ChildCallContexts(Reserve(PartitionRequests.size()))
{
    Y_DEBUG_ABORT_UNLESS(PartitionsCount >= PartitionRequests.size());
}

template <typename TMethod>
void TPartitionRequestActor<TMethod>::Bootstrap(const NActors::TActorContext& ctx)
{
    Prepare(ctx);


    ui32 requestNo = 0;
    for (auto& partitionRequest: PartitionRequests) {
        const auto selfId = TBase::SelfId();

        ForkTraces(partitionRequest.Event->CallContext);

        auto event = std::make_unique<NActors::IEventHandle>(
            partitionRequest.ActorId,
            selfId,
            partitionRequest.Event.release(),
            NActors::IEventHandle::FlagForwardOnNondelivery,
            requestNo++,
            &selfId);

        ctx.Send(event.release());
    }

    TBase::Become(&TBase::TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TPartitionRequestActor<TMethod>::Prepare(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
}

template <>
void TPartitionRequestActor<TEvVolume::TGetCompactionStatusMethod>::Prepare(
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Record.SetIsCompleted(true);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TPartitionRequestActor<TMethod>::NotifyCompleted(const NActors::TActorContext& ctx)
{
    if constexpr (RequiresReadWriteAccess<TMethod>) {
        using TEvent = TEvVolumePrivate::TEvMultipartitionWriteOrZeroCompleted;
        auto ev = std::make_unique<TEvent>(
            VolumeRequestId,
            Record.GetError().GetCode());

        NCloud::Send(
            ctx,
            VolumeActorId,
            std::move(ev)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TPartitionRequestActor<TMethod>::HandlePartitionResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui32 requestNo = ev->Cookie;

    if (FAILED(msg->GetStatus())) {
        Record.MutableError()->CopyFrom(msg->GetError());
    } else {
        Merge(msg->Record, requestNo, Record);
    }

    JoinTraces(requestNo);

    if (++Responses == PartitionRequests.size() || FAILED(msg->GetStatus())) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            Record
        );

        if (TraceInfo.IsTraced) {
            TraceInfo.TraceSerializer->BuildTraceInfo(
                *response->Record.MutableTrace(),
                RequestInfo->CallContext->LWOrbit,
                TraceInfo.ReceiveTime,
                GetCycleCount());
        }

        LWTRACK(
            ResponseSent_Volume,
            RequestInfo->CallContext->LWOrbit,
            TMethod::Name,
            RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        NotifyCompleted(ctx);

        TBase::Die(ctx);
    }
}

template <typename TMethod>
void TPartitionRequestActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    *Record.MutableError() = MakeError(
        E_REJECTED,
        "failed to deliver request to some partitions"
    );

    auto response = std::make_unique<typename TMethod::TResponse>(
        Record
    );

    LWTRACK(
        ResponseSent_Volume,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NotifyCompleted(ctx);

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
STFUNC(TPartitionRequestActor<TMethod>::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TMethod::TResponse, HandlePartitionResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
