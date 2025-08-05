#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/get_device_for_range_companion.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TVolumeAsPartitionActor final
    : public NActors::TActorBootstrapped<TVolumeAsPartitionActor>
{
    using TBase = NActors::TActorBootstrapped<TVolumeAsPartitionActor>;

public:
    enum class EState
    {
        Describing,
        Ready,
        Error,
        Zombie,
    };

    enum class EReplyType
    {
        Ordinary,
        Local,
    };

private:
    struct TRequestCtx
    {
        const NActors::TActorId OriginalSender;
        const ui64 OriginalCookie = 0;
        const TBlockRange64 BlockRange;
        const EReplyType ReplyType = EReplyType::Ordinary;
    };

    const TChildLogTitle LogTitle;
    const ui32 OriginalBlockSize;
    const TString DiskId;

    EState State = EState::Describing;
    ui64 BlockCount = 0;
    ui32 BlockSize = 0;
    TRequestsInProgress<EAllowedRequests::WriteOnly, ui64, TRequestCtx>
        RequestsInProgress;
    TGetDeviceForRangeCompanion GetDeviceForRangeCompanion{
        TGetDeviceForRangeCompanion::EAllowedOperation::None};

    TRequestInfoPtr Poisoner;

public:
    TVolumeAsPartitionActor(
        TChildLogTitle logTitle,
        ui32 originalBlockSize,
        TString diskId);

    ~TVolumeAsPartitionActor() override;

    virtual void Bootstrap(const NActors::TActorContext& ctx);

private:
    bool CheckRange(TBlockRange64 range) const;

    template <typename TEvent>
    void ForwardRequestToFollower(
        const TEvent& ev,
        const NActors::TActorContext& ctx,
        EReplyType replyType);

    template <typename TMethod>
    void ForwardResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ReplyUndelivery(const NActors::TActorContext& ctx, ui64 cookie);

    template <typename TMethod>
    void ReplyInvalidState(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ReplyInvalidRange(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        EReplyType replyType);

    void DoWriteBlocks(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx,
        EReplyType replyType);

    void ReplyAndDie(const NActors::TActorContext& ctx);

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

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
};

///////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
