#include "multi_partition_requests.h"

#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/model/stripe.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

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

    request.ActorId = partition.GetTopActorId(),
    request.TabletId = partition.TabletId;
    request.PartitionId = stripeInfo.PartitionId;
    request.BlockRange = stripeInfo.BlockRange;

    request.Event = std::make_unique<typename TMethod::TRequest>();
    request.Event->Record.MutableHeaders()->CopyFrom(proto.GetHeaders());
    request.Event->Record.SetDiskId(proto.GetDiskId());
    request.Event->Record.SetStartIndex(stripeInfo.BlockRange.Start);
    if constexpr (IsExactlyWriteMethod<TMethod>) {
        if (i < proto.ChecksumsSize()) {
            const ui32 blockSize = partition.PartitionConfig.GetBlockSize();
            if (proto.GetChecksums(i).GetByteCount() ==
                stripeInfo.BlockRange.Size() * blockSize)
            {
                *request.Event->Record.AddChecksums() = proto.GetChecksums(i);
            } else {
                ReportChecksumCalculationError(
                    TStringBuilder()
                        << "Incorrectly calculated checksum for block range",
                    {{"range", stripeInfo.BlockRange},
                     {"range length=", stripeInfo.BlockRange.Size()},
                     {"checksum length",
                      proto.GetChecksums(i).GetByteCount() / blockSize},
                     {"diskId", partition.PartitionConfig.GetDiskId()}});
            }
        }
    }

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
        (*requests)[i].ActorId = partitions[i].GetTopActorId();
        (*requests)[i].TabletId = partitions[i].TabletId;
        (*requests)[i].PartitionId = i;
        (*requests)[i].Event = std::make_unique<typename TMethod::TRequest>();
        (*requests)[i].Event->Record = ev->Get()->Record;
    }

    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

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
        Y_ABORT_UNLESS(bufferSize == 0 || bufferSize == buffer.size());
        bufferSize = buffer.size();
    }

    *blockRange = BuildRequestBlockRange(*ev->Get(), blockSize);

    requests->resize(CalculateRequestCount(
        blocksPerStripe,
        *blockRange,
        partitions.size()
    ));

    if (requests->size() == 1) {
        CombineChecksumsInPlace(*proto.MutableChecksums());
    }

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
    auto& proto = ev->Get()->Record;

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

    if (requests->size() == 1) {
        CombineChecksumsInPlace(*proto.MutableChecksums());
    }

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
        request.Event->Record.ShouldReportFailedRangesOnFailure =
            ev->Get()->Record.ShouldReportFailedRangesOnFailure;
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
    (*requests)[0].ActorId = partition.GetTopActorId();
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

template <>
bool ToPartitionRequests<TEvVolume::TCheckRangeMethod>(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const TEvVolume::TCheckRangeMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TEvVolume::TCheckRangeMethod>>* requests,
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

}   // namespace NCloud::NBlockStore::NStorage
