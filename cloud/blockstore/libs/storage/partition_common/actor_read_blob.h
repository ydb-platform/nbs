#pragma once

#include "blob_compressor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/long_running_operation_companion.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString GetBrokenDataMarkerBlock(ui32 blockSize);

std::optional<ui32> GetRepairBlockChecksum(
    EStorageAccessMode storageAccessMode,
    ui32 blockSize);

////////////////////////////////////////////////////////////////////////////////

class TReadBlobActor final
    : public NActors::TActorBootstrapped<TReadBlobActor>
    , public TLongRunningOperationCompanion
{
public:
    using TRequest = TEvPartitionCommonPrivate::TEvReadBlobRequest;
    using TResponse = TEvPartitionCommonPrivate::TEvReadBlobResponse;

private:
    const TRequestInfoPtr RequestInfo;

    const NActors::TActorId PartitionActorId;
    const ui64 PartitionTabletId;
    const ui32 BlockSize;
    const bool ShouldCalculateChecksums;
    const EStorageAccessMode StorageAccessMode;
    const std::unique_ptr<TRequest> Request;

    const ui64 BSGroupOperationId = 0;

    const bool PassTraceIdToBlobstorage = false;

    TVector<TBlockRange32> QueryBlockRanges;

    TInstant RequestSent;
    TInstant ResponseReceived;

    bool DeadlineSeen = false;

public:
    TReadBlobActor(
        TRequestInfoPtr requestInfo,
        const NActors::TActorId& partitionActorId,
        const NActors::TActorId& volumeActorId,
        ui64 partitionTabletId,
        ui32 blockSize,
        bool shouldCalculateChecksums,
        const EStorageAccessMode storageAccessMode,
        std::unique_ptr<TRequest> request,
        TDuration longRunningThreshold,
        ui64 bsGroupOperationId,
        bool passTraceIdToBlobstorage);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    TResultOrError<TReadPlan> GetQueries() const;

    void SendGetRequest(const NActors::TActorContext& ctx);
    void NotifyCompleted(
        const NActors::TActorContext& ctx,
        const TResponse& response);

    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        std::unique_ptr<TResponse> response);

    void ReplyError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error,
        EErrorSource errorSource,
        const NKikimr::TEvBlobStorage::TEvGetResult* response);

    void ReplyError(
        const NActors::TActorContext& ctx,
        const NKikimr::TEvBlobStorage::TEvGetResult& response,
        const TString& description,
        EErrorSource errorSource);

    struct TDataThatCanBeProcessed
    {
        TSgList Sglist;
        TVector<ui16> BlockOffsets;
        TVector<NKikimr::TEvBlobStorage::TEvGetResult::TResponse> Responses;
    };

    bool FillBrokenDataAndFilterItOut(
        const NKikimr::TEvBlobStorage::TEvGetResult* msg,
        const TSgList& sglist,
        const NActors::TActorContext& ctx,
        TDataThatCanBeProcessed* dataThatCanBeProcessed) const;

    TResultOrError<TVector<ui32>> DecompressReadResultToSglist(
        std::span<NKikimr::TEvBlobStorage::TEvGetResult::TResponse> responses,
        std::span<const TBlockDataRef> sglist,
        std::span<const ui16> blockOffsets);

    TResultOrError<TVector<ui32>> CopyReadResultToSglist(
        std::span<NKikimr::TEvBlobStorage::TEvGetResult::TResponse> responses,
        std::span<const TBlockDataRef> sglist,
        std::span<const ui16> blockOffsets);

    TVector<ui32> FillBlockChecksumsForRecoveredBlocksIfNeeded(
        std::span<const ui16> blockOffsets,
        std::span<const ui16> blockOffsetsWithoutRecovered,
        TVector<ui32> blockChecksumsWithoutRecovered);

private:
    STFUNC(StateWork);

    void HandleGetResult(
        const NKikimr::TEvBlobStorage::TEvGetResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivered(
        const NActors::TEvents::TEvUndelivered::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NKikimr::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
