#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/get_device_for_range_companion.h>

#include <cloud/storage/core/libs/actors/poison_pill_helper.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TVolumeAsPartitionActor final
    : public NActors::TActorBootstrapped<TVolumeAsPartitionActor>
    , public IPoisonPillHelperOwner
{
    using TBase = NActors::TActorBootstrapped<TVolumeAsPartitionActor>;

public:
    enum class EState
    {
        Describing,
        Ready,
        Error,
    };

private:
    struct TRequestCtx
    {
        const NActors::TActorId OriginalSender;
        const ui64 OriginalCookie = 0;

        const TBlockRange64 BlockRange;
    };

    const TString OriginalDiskId;
    const ui32 OriginalBlockSize;

    const TString DiskId;

    EState State = EState::Describing;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    ui64 BlockCount = 0;
    ui32 BlockSize = 0;

    TRequestsInProgress<EAllowedRequests::WriteOnly, ui64, TRequestCtx>
        RequestsInProgress;

    TGetDeviceForRangeCompanion GetDeviceForRangeCompanion{
        TGetDeviceForRangeCompanion::EAllowedOperation::None};
    TPoisonPillHelper PoisonPillHelper;

public:
    TVolumeAsPartitionActor(
        TString originalDiskId,
        ui32 originalBlockSize,
        TString diskId);

    ~TVolumeAsPartitionActor() override;

    virtual void Bootstrap(const NActors::TActorContext& ctx);

    // IPoisonPillHelperOwner implementation
    void Die(const NActors::TActorContext& ctx) override
    {
        TBase::Die(ctx);
    }

private:
    bool CheckRange(TBlockRange64 range) const;

    template <typename TEvent>
    void ForwardWriteRequest(
        const TEvent& ev,
        const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocks(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWriteBlocksLocal(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleZeroBlocks(
        const TEvService::TEvZeroBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksUndelivery(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWriteBlocksLocalUndelivery(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleZeroBlocksUndelivery(
            const TEvService::TEvZeroBlocksRequest::TPtr& ev,
            const NActors::TActorContext& ctx);

    void HandleWriteBlocksResponse(
        const TEvService::TEvWriteBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWriteBlocksLocalResponse(
        const TEvService::TEvWriteBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleZeroBlocksResponse(
        const TEvService::TEvZeroBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
};

}   // namespace NCloud::NBlockStore::NStorage
