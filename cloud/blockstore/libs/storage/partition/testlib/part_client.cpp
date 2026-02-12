#include "part_client.h"


namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize)
{
    return TString(size, fill);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPartitionClient::TPartitionClient(
        NActors::TTestActorRuntime& runtime,
        ui32 nodeIdx,
        ui64 tabletId)
    : Runtime(runtime)
    , NodeIdx(nodeIdx)
    , TabletId(tabletId)
    , Sender(runtime.AllocateEdgeActor(NodeIdx))
{
    PipeClient = Runtime.ConnectToPipe(
        TabletId,
        Sender,
        NodeIdx,
        NKikimr::GetPipeConfigWithRetries());
}

void TPartitionClient::RebootTablet()
{
    TVector<ui64> tablets = {TabletId};
    auto guard =
        NKikimr::CreateTabletScheduledEventsGuard(tablets, Runtime, Sender);

    NKikimr::RebootTablet(Runtime, TabletId, Sender);

    // sooner or later after reset pipe will reconnect
    // but we do not want to wait
    PipeClient = Runtime.ConnectToPipe(
        TabletId,
        Sender,
        NodeIdx,
        NKikimr::GetPipeConfigWithRetries());
}

void TPartitionClient::KillTablet()
{
    SendToPipe(std::make_unique<TEvents::TEvPoisonPill>());
    Runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::Seconds(1));
}

std::unique_ptr<TEvPartition::TEvStatPartitionRequest>
TPartitionClient::CreateStatPartitionRequest()
{
    return std::make_unique<TEvPartition::TEvStatPartitionRequest>();
}

std::unique_ptr<TEvService::TEvWriteBlocksRequest>
TPartitionClient::CreateWriteBlocksRequest(
    ui32 blockIndex,
    TString blockContent)
{
    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    request->Record.SetStartIndex(blockIndex);
    *request->Record.MutableBlocks()->MutableBuffers()->Add() = blockContent;
    return request;
}

std::unique_ptr<TEvService::TEvWriteBlocksRequest>
TPartitionClient::CreateWriteBlocksRequest(
    const TBlockRange32& writeRange,
    char fill)
{
    auto blockContent = GetBlockContent(fill);

    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    request->Record.SetStartIndex(writeRange.Start);

    auto& buffers = *request->Record.MutableBlocks()->MutableBuffers();
    for (ui32 i = 0; i < writeRange.Size(); ++i) {
        *buffers.Add() = blockContent;
    }

    return request;
}

std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
TPartitionClient::CreateWriteBlocksLocalRequest(
    const TBlockRange32& writeRange,
    TStringBuf blockContent)
{
    TSgList sglist;
    sglist.resize(
        writeRange.Size(),
        {blockContent.data(), blockContent.size()});

    auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
    request->Record.SetStartIndex(writeRange.Start);
    request->Record.Sglist = TGuardedSgList(std::move(sglist));
    request->Record.BlocksCount = writeRange.Size();
    request->Record.BlockSize = blockContent.size() / writeRange.Size();
    return request;
}

std::unique_ptr<TEvService::TEvWriteBlocksRequest>
TPartitionClient::CreateWriteBlocksRequest(ui32 blockIndex, char fill)
{
    return CreateWriteBlocksRequest(blockIndex, GetBlockContent(fill));
}

std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
TPartitionClient::CreateWriteBlocksLocalRequest(
    ui32 blockIndex,
    TStringBuf blockContent)
{
    return CreateWriteBlocksLocalRequest(
        TBlockRange32::MakeClosedInterval(blockIndex, blockIndex),
        std::move(blockContent));
}

std::unique_ptr<TEvService::TEvZeroBlocksRequest>
TPartitionClient::CreateZeroBlocksRequest(ui32 blockIndex)
{
    auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
    request->Record.SetStartIndex(blockIndex);
    request->Record.SetBlocksCount(1);
    return request;
}

std::unique_ptr<TEvService::TEvZeroBlocksRequest>
TPartitionClient::CreateZeroBlocksRequest(const TBlockRange32& writeRange)
{
    auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
    request->Record.SetStartIndex(writeRange.Start);
    request->Record.SetBlocksCount(writeRange.Size());
    return request;
}

std::unique_ptr<TEvService::TEvReadBlocksRequest>
TPartitionClient::CreateReadBlocksRequest(
    const TBlockRange32& range,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    request->Record.SetStartIndex(range.Start);
    request->Record.SetBlocksCount(range.Size());
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvService::TEvReadBlocksRequest>
TPartitionClient::CreateReadBlocksRequest(
    ui32 blockIndex,
    const TString& checkpointId)
{
    return CreateReadBlocksRequest(
        TBlockRange32::WithLength(blockIndex, 1),
        checkpointId);
}

std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
TPartitionClient::CreateReadBlocksLocalRequest(
    ui32 blockIndex,
    TStringBuf buffer,
    const TString& checkpointId)
{
    return CreateReadBlocksLocalRequest(
        blockIndex,
        TGuardedSgList{{TBlockDataRef(buffer.data(), buffer.size())}},
        checkpointId);
}

std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
TPartitionClient::CreateReadBlocksLocalRequest(
    ui32 blockIndex,
    const TGuardedSgList& sglist,
    const TString& checkpointId)
{
    return CreateReadBlocksLocalRequest(
        TBlockRange32::MakeClosedInterval(blockIndex, blockIndex),
        sglist,
        checkpointId);
}

std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
TPartitionClient::CreateReadBlocksLocalRequest(
    const TBlockRange32& readRange,
    const TSgList& sglist,
    const TString& checkpointId)
{
    return CreateReadBlocksLocalRequest(
        readRange,
        TGuardedSgList(sglist),
        checkpointId);
}

std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
TPartitionClient::CreateReadBlocksLocalRequest(
    const TBlockRange32& readRange,
    const TGuardedSgList& sglist,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    request->Record.SetCheckpointId(checkpointId);
    request->Record.SetStartIndex(readRange.Start);
    request->Record.SetBlocksCount(readRange.Size());

    request->Record.Sglist = sglist;
    request->Record.BlockSize = DefaultBlockSize;
    return request;
}

std::unique_ptr<TEvService::TEvCreateCheckpointRequest>
TPartitionClient::CreateCreateCheckpointRequest(const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvService::TEvCreateCheckpointRequest>
TPartitionClient::CreateCreateCheckpointRequest(
    const TString& checkpointId,
    const TString& idempotenceId,
    bool withoutData)
{
    auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
    request->Record.SetCheckpointId(checkpointId);
    request->Record.SetCheckpointType(
        withoutData ? NProto::ECheckpointType::WITHOUT_DATA
                    : NProto::ECheckpointType::NORMAL);
    request->Record.MutableHeaders()->SetIdempotenceId(idempotenceId);
    return request;
}

std::unique_ptr<TEvService::TEvDeleteCheckpointRequest>
TPartitionClient::CreateDeleteCheckpointRequest(const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvService::TEvDeleteCheckpointRequest>
TPartitionClient::CreateDeleteCheckpointRequest(
    const TString& checkpointId,
    const TString& idempotenceId)
{
    auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
    request->Record.SetCheckpointId(checkpointId);
    request->Record.MutableHeaders()->SetIdempotenceId(idempotenceId);
    return request;
}

std::unique_ptr<TEvVolume::TEvDeleteCheckpointDataRequest>
TPartitionClient::CreateDeleteCheckpointDataRequest(const TString& checkpointId)
{
    auto request =
        std::make_unique<TEvVolume::TEvDeleteCheckpointDataRequest>();
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvService::TEvGetChangedBlocksRequest>
TPartitionClient::CreateGetChangedBlocksRequest(
    const TBlockRange32& range,
    const TString& lowCheckpointId,
    const TString& highCheckpointId,
    const bool ignoreBaseDisk)
{
    auto request = std::make_unique<TEvService::TEvGetChangedBlocksRequest>();
    request->Record.SetStartIndex(range.Start);
    request->Record.SetBlocksCount(range.Size());
    request->Record.SetLowCheckpointId(lowCheckpointId);
    request->Record.SetHighCheckpointId(highCheckpointId);
    request->Record.SetIgnoreBaseDisk(ignoreBaseDisk);
    return request;
}

std::unique_ptr<TEvPartition::TEvWaitReadyRequest>
TPartitionClient::CreateWaitReadyRequest()
{
    return std::make_unique<TEvPartition::TEvWaitReadyRequest>();
}

std::unique_ptr<TEvPartitionPrivate::TEvFlushRequest>
TPartitionClient::CreateFlushRequest()
{
    return std::make_unique<TEvPartitionPrivate::TEvFlushRequest>();
}

std::unique_ptr<TEvPartitionCommonPrivate::TEvTrimFreshLogRequest>
TPartitionClient::CreateTrimFreshLogRequest()
{
    return std::make_unique<
        TEvPartitionCommonPrivate::TEvTrimFreshLogRequest>();
}

std::unique_ptr<TEvPartitionPrivate::TEvMetadataRebuildBlockCountRequest>
TPartitionClient::CreateMetadataRebuildBlockCountRequest(
    TPartialBlobId blobId,
    ui32 count,
    TPartialBlobId lastBlobId)
{
    return std::make_unique<
        TEvPartitionPrivate::TEvMetadataRebuildBlockCountRequest>(
        blobId,
        count,
        lastBlobId,
        TBlockCountRebuildState());
}

std::unique_ptr<TEvPartitionPrivate::TEvScanDiskBatchRequest>
TPartitionClient::CreateScanDiskBatchRequest(
    TPartialBlobId blobId,
    ui32 count,
    TPartialBlobId lastBlobId)
{
    return std::make_unique<TEvPartitionPrivate::TEvScanDiskBatchRequest>(
        blobId,
        count,
        lastBlobId);
}

std::unique_ptr<TEvPartitionPrivate::TEvCleanupRequest>
TPartitionClient::CreateCleanupRequest()
{
    return std::make_unique<TEvPartitionPrivate::TEvCleanupRequest>();
}

std::unique_ptr<TEvTablet::TEvGetCounters>
TPartitionClient::CreateGetCountersRequest()
{
    auto request = std::make_unique<TEvTablet::TEvGetCounters>();
    return request;
}

void TPartitionClient::SendGetCountersRequest()
{
    auto request = CreateGetCountersRequest();
    SendToPipe(std::move(request));
}

std::unique_ptr<TEvTablet::TEvGetCountersResponse>
TPartitionClient::RecvGetCountersResponse()
{
    return RecvResponse<TEvTablet::TEvGetCountersResponse>();
}

std::unique_ptr<TEvTablet::TEvGetCountersResponse>
TPartitionClient::GetCounters()
{
    auto request = CreateGetCountersRequest();
    SendToPipe(std::move(request));

    auto response = RecvResponse<TEvTablet::TEvGetCountersResponse>();
    return response;
}

std::unique_ptr<TEvPartitionPrivate::TEvCollectGarbageRequest>
TPartitionClient::CreateCollectGarbageRequest()
{
    return std::make_unique<TEvPartitionPrivate::TEvCollectGarbageRequest>();
}

std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest>
TPartitionClient::CreateDescribeBlocksRequest(
    ui32 startIndex,
    ui32 blockCount,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();
    request->Record.SetStartIndex(startIndex);
    request->Record.SetBlocksCount(blockCount);
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest>
TPartitionClient::CreateDescribeBlocksRequest(
    const TBlockRange32& range,
    const TString& checkpointId)
{
    return CreateDescribeBlocksRequest(range.Start, range.Size(), checkpointId);
}

std::unique_ptr<TEvVolume::TEvGetUsedBlocksRequest>
TPartitionClient::CreateGetUsedBlocksRequest()
{
    return std::make_unique<TEvVolume::TEvGetUsedBlocksRequest>();
}

std::unique_ptr<TEvPartitionCommonPrivate::TEvReadBlobRequest>
TPartitionClient::CreateReadBlobRequest(
    const NKikimr::TLogoBlobID& blobId,
    const ui32 bSGroupId,
    const TVector<ui16>& blobOffsets,
    TSgList sglist)
{
    auto request =
        std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
            blobId,
            MakeBlobStorageProxyID(bSGroupId),
            blobOffsets,
            TGuardedSgList(std::move(sglist)),
            bSGroupId,
            false,             // async
            TInstant::Max(),   // deadline
            false              // shouldCalculateChecksums
        );
    return request;
}

std::unique_ptr<TEvVolume::TEvCompactRangeRequest>
TPartitionClient::CreateCompactRangeRequest(ui32 blockIndex, ui32 blockCount)
{
    auto request = std::make_unique<TEvVolume::TEvCompactRangeRequest>();
    request->Record.SetStartIndex(blockIndex);
    request->Record.SetBlocksCount(blockCount);
    return request;
}

std::unique_ptr<TEvVolume::TEvGetCompactionStatusRequest>
TPartitionClient::CreateGetCompactionStatusRequest(const TString& operationId)
{
    auto request = std::make_unique<TEvVolume::TEvGetCompactionStatusRequest>();
    request->Record.SetOperationId(operationId);
    return request;
}

std::unique_ptr<TEvPartition::TEvDrainRequest>
TPartitionClient::CreateDrainRequest()
{
    return std::make_unique<TEvPartition::TEvDrainRequest>();
}

std::unique_ptr<NMon::TEvRemoteHttpInfo> TPartitionClient::CreateRemoteHttpInfo(
    const TString& params,
    HTTP_METHOD method)
{
    return std::make_unique<NMon::TEvRemoteHttpInfo>(params, method);
}

std::unique_ptr<NMon::TEvRemoteHttpInfo> TPartitionClient::CreateRemoteHttpInfo(
    const TString& params)
{
    return std::make_unique<NMon::TEvRemoteHttpInfo>(params);
}

void TPartitionClient::SendRemoteHttpInfo(
    const TString& params,
    HTTP_METHOD method)
{
    auto request = CreateRemoteHttpInfo(params, method);
    SendToPipe(std::move(request));
}

void TPartitionClient::SendRemoteHttpInfo(const TString& params)
{
    auto request = CreateRemoteHttpInfo(params);
    SendToPipe(std::move(request));
}

std::unique_ptr<NMon::TEvRemoteHttpInfoRes>
TPartitionClient::RecvCreateRemoteHttpInfoRes()
{
    return RecvResponse<NMon::TEvRemoteHttpInfoRes>();
}

std::unique_ptr<NMon::TEvRemoteHttpInfoRes> TPartitionClient::RemoteHttpInfo(
    const TString& params,
    HTTP_METHOD method)
{
    auto request = CreateRemoteHttpInfo(params, method);
    SendToPipe(std::move(request));

    auto response = RecvResponse<NMon::TEvRemoteHttpInfoRes>();
    return response;
}

std::unique_ptr<NMon::TEvRemoteHttpInfoRes> TPartitionClient::RemoteHttpInfo(
    const TString& params)
{
    return RemoteHttpInfo(params, HTTP_METHOD::HTTP_METHOD_GET);
}

std::unique_ptr<TEvVolume::TEvRebuildMetadataRequest>
TPartitionClient::CreateRebuildMetadataRequest(
    NProto::ERebuildMetadataType type,
    ui32 batchSize)
{
    auto request = std::make_unique<TEvVolume::TEvRebuildMetadataRequest>();
    request->Record.SetMetadataType(type);
    request->Record.SetBatchSize(batchSize);
    return request;
}

std::unique_ptr<TEvVolume::TEvGetRebuildMetadataStatusRequest>
TPartitionClient::CreateGetRebuildMetadataStatusRequest()
{
    auto request =
        std::make_unique<TEvVolume::TEvGetRebuildMetadataStatusRequest>();
    return request;
}

std::unique_ptr<TEvVolume::TEvScanDiskRequest>
TPartitionClient::CreateScanDiskRequest(ui32 blobsPerBatch)
{
    auto request = std::make_unique<TEvVolume::TEvScanDiskRequest>();
    request->Record.SetBatchSize(blobsPerBatch);
    return request;
}

std::unique_ptr<TEvVolume::TEvGetScanDiskStatusRequest>
TPartitionClient::CreateGetScanDiskStatusRequest()
{
    return std::make_unique<TEvVolume::TEvGetScanDiskStatusRequest>();
}

std::unique_ptr<TEvVolume::TEvCheckRangeRequest>
TPartitionClient::CreateCheckRangeRequest(
    TString id,
    ui32 startIndex,
    ui32 size)
{
    auto request = std::make_unique<TEvVolume::TEvCheckRangeRequest>();
    request->Record.SetDiskId(id);
    request->Record.SetStartIndex(startIndex);
    request->Record.SetBlocksCount(size);
    return request;
}

std::unique_ptr<TEvVolume::TEvGetPartitionInfoRequest>
TPartitionClient::CreateGetPartitionInfoRequest()
{
    auto request = std::make_unique<TEvVolume::TEvGetPartitionInfoRequest>();
    return request;
}



}   // namespace NCloud::NBlockStore::NStorage::NPartition
