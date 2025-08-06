#pragma once

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/long_running_operation_companion.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

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

    ui64 BlopOperationId = 0;

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
        ui64 blopOperationId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendGetRequest(const NActors::TActorContext& ctx);
    void NotifyCompleted(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        std::unique_ptr<TResponse> response);

    void ReplyError(
        const NActors::TActorContext& ctx,
        const NKikimr::TEvBlobStorage::TEvGetResult& response,
        const TString& description);

private:
    STFUNC(StateWork);

    void HandleGetResult(
        const NKikimr::TEvBlobStorage::TEvGetResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivered(
        const NActors::TEvents::TEvUndelivered::TPtr &ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NKikimr::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
