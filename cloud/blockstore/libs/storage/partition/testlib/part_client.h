#pragma once

#include "contrib/ydb/library/actors/interconnect/types.h"

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/mon.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TPartitionClient
{
    static constexpr TDuration WaitTimeout = TDuration::Seconds(5);

private:
    NActors::TTestActorRuntime& Runtime;
    ui32 NodeIdx = 0;
    ui64 TabletId;

    const TActorId Sender;
    TActorId PipeClient;

public:
    TPartitionClient(
        NActors::TTestActorRuntime& runtime,
        ui32 nodeIdx = 0,
        ui64 tabletId = TestTabletId);

    void RebootTablet();

    void KillTablet();

    template <typename TRequest>
    void SendToPipe(std::unique_ptr<TRequest> request, ui64 cookie = 0)
    {
        Runtime
            .SendToPipe(PipeClient, Sender, request.release(), NodeIdx, cookie);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT_C(handle, TypeName<TResponse>() << " is expected");
        return std::unique_ptr<TResponse>(
            handle->Release<TResponse>().Release());
    }

    std::unique_ptr<TEvPartition::TEvStatPartitionRequest>
    CreateStatPartitionRequest();

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        TString blockContent);

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        const TBlockRange32& writeRange,
        char fill = 0);

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
    CreateWriteBlocksLocalRequest(
        const TBlockRange32& writeRange,
        TStringBuf blockContent);

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        char fill = 0);

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
    CreateWriteBlocksLocalRequest(ui32 blockIndex, TStringBuf blockContent);

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        ui32 blockIndex);

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        const TBlockRange32& writeRange);

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        const TBlockRange32& range,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        ui32 blockIndex,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
    CreateReadBlocksLocalRequest(
        ui32 blockIndex,
        TStringBuf buffer,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
    CreateReadBlocksLocalRequest(
        ui32 blockIndex,
        const TGuardedSgList& sglist,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
    CreateReadBlocksLocalRequest(
        const TBlockRange32& readRange,
        const TSgList& sglist,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
    CreateReadBlocksLocalRequest(
        const TBlockRange32& readRange,
        const TGuardedSgList& sglist,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvCreateCheckpointRequest>
    CreateCreateCheckpointRequest(const TString& checkpointId);

    std::unique_ptr<TEvService::TEvCreateCheckpointRequest>
    CreateCreateCheckpointRequest(
        const TString& checkpointId,
        const TString& idempotenceId,
        bool withoutData = false);

    std::unique_ptr<TEvService::TEvDeleteCheckpointRequest>
    CreateDeleteCheckpointRequest(const TString& checkpointId);

    std::unique_ptr<TEvService::TEvDeleteCheckpointRequest>
    CreateDeleteCheckpointRequest(
        const TString& checkpointId,
        const TString& idempotenceId);

    std::unique_ptr<TEvVolume::TEvDeleteCheckpointDataRequest>
    CreateDeleteCheckpointDataRequest(const TString& checkpointId);

    std::unique_ptr<TEvService::TEvGetChangedBlocksRequest>
    CreateGetChangedBlocksRequest(
        const TBlockRange32& range,
        const TString& lowCheckpointId,
        const TString& highCheckpointId,
        const bool ignoreBaseDisk);

    std::unique_ptr<TEvPartition::TEvWaitReadyRequest> CreateWaitReadyRequest();

    std::unique_ptr<TEvPartitionPrivate::TEvFlushRequest> CreateFlushRequest();

    std::unique_ptr<TEvPartitionCommonPrivate::TEvTrimFreshLogRequest>
    CreateTrimFreshLogRequest();

    template <typename... TArgs>
    std::unique_ptr<TEvPartitionPrivate::TEvCompactionRequest>
    CreateCompactionRequest(TArgs&&... args)
    {
        return std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(
            std::forward<TArgs>(args)...);
    }

    std::unique_ptr<TEvPartitionPrivate::TEvMetadataRebuildBlockCountRequest>
    CreateMetadataRebuildBlockCountRequest(
        TPartialBlobId blobId,
        ui32 count,
        TPartialBlobId lastBlobId);

    std::unique_ptr<TEvPartitionPrivate::TEvScanDiskBatchRequest>
    CreateScanDiskBatchRequest(
        TPartialBlobId blobId,
        ui32 count,
        TPartialBlobId lastBlobId);

    std::unique_ptr<TEvPartitionPrivate::TEvCleanupRequest>
    CreateCleanupRequest();

    std::unique_ptr<NKikimr::TEvTablet::TEvGetCounters>
    CreateGetCountersRequest();

    void SendGetCountersRequest();

    std::unique_ptr<NKikimr::TEvTablet::TEvGetCountersResponse>
    RecvGetCountersResponse();

    std::unique_ptr<NKikimr::TEvTablet::TEvGetCountersResponse> GetCounters();

    std::unique_ptr<TEvPartitionPrivate::TEvCollectGarbageRequest>
    CreateCollectGarbageRequest();

    std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest>
    CreateDescribeBlocksRequest(
        ui32 startIndex,
        ui32 blockCount,
        const TString& checkpointId = "");

    std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest>
    CreateDescribeBlocksRequest(
        const TBlockRange32& range,
        const TString& checkpointId = "");

    std::unique_ptr<TEvVolume::TEvGetUsedBlocksRequest>
    CreateGetUsedBlocksRequest();

    std::unique_ptr<TEvPartitionCommonPrivate::TEvReadBlobRequest>
    CreateReadBlobRequest(
        const NKikimr::TLogoBlobID& blobId,
        const ui32 bSGroupId,
        const TVector<ui16>& blobOffsets,
        TSgList sglist);

    std::unique_ptr<TEvVolume::TEvCompactRangeRequest>
    CreateCompactRangeRequest(ui32 blockIndex, ui32 blockCount);

    std::unique_ptr<TEvVolume::TEvGetCompactionStatusRequest>
    CreateGetCompactionStatusRequest(const TString& operationId);

    std::unique_ptr<TEvPartition::TEvDrainRequest> CreateDrainRequest();

    std::unique_ptr<NActors::NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method);

    std::unique_ptr<NActors::NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params);

    void SendRemoteHttpInfo(const TString& params, HTTP_METHOD method);

    void SendRemoteHttpInfo(const TString& params);

    std::unique_ptr<NActors::NMon::TEvRemoteHttpInfoRes>
    RecvCreateRemoteHttpInfoRes();

    std::unique_ptr<NActors::NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method);

    std::unique_ptr<NActors::NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params);

    std::unique_ptr<TEvVolume::TEvRebuildMetadataRequest>
    CreateRebuildMetadataRequest(
        NProto::ERebuildMetadataType type,
        ui32 batchSize);

    std::unique_ptr<TEvVolume::TEvGetRebuildMetadataStatusRequest>
    CreateGetRebuildMetadataStatusRequest();

    std::unique_ptr<TEvVolume::TEvScanDiskRequest> CreateScanDiskRequest(
        ui32 blobsPerBatch);

    std::unique_ptr<TEvVolume::TEvGetScanDiskStatusRequest>
    CreateGetScanDiskStatusRequest();

    std::unique_ptr<TEvVolume::TEvCheckRangeRequest>
    CreateCheckRangeRequest(TString id, ui32 startIndex, ui32 size);

    std::unique_ptr<TEvVolume::TEvGetPartitionInfoRequest>
    CreateGetPartitionInfoRequest();

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                    \
    void Send##name##Request(Args&&... args)                                       \
    {                                                                              \
        auto request = Create##name##Request(std::forward<Args>(args)...);         \
        SendToPipe(std::move(request));                                            \
    }                                                                              \
                                                                                   \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()                \
    {                                                                              \
        return RecvResponse<ns::TEv##name##Response>();                            \
    }                                                                              \
                                                                                   \
    template <typename... Args>                                                    \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)                  \
    {                                                                              \
        auto request = Create##name##Request(std::forward<Args>(args)...);         \
        SendToPipe(std::move(request));                                            \
                                                                                   \
        auto response = RecvResponse<ns::TEv##name##Response>();                   \
        UNIT_ASSERT_C(                                                             \
            SUCCEEDED(response->GetStatus()),                                      \
            response->GetErrorReason());                                           \
        return response;                                                           \
    }                                                                              \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvPartition)
    BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvPartitionPrivate)
    BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvPartitionCommonPrivate)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(BLOCKSTORE_DECLARE_METHOD, TEvService)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(BLOCKSTORE_DECLARE_METHOD, TEvVolume)

    #undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
