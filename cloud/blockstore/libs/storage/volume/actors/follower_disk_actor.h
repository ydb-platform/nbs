#pragma once

#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_migration_common_actor.h>
#include <cloud/blockstore/libs/storage/volume/model/follower_disk.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

struct TFollowerDiskActorParams
{
    const NProto::EStorageMediaKind LeaderMediaKind =
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    const TString LeaderDiskId;
    const ui64 LeaderBlockCount = 0;
    const ui32 LeaderBlockSize = 0;

    NActors::TActorId LeaderVolumeActorId;
    NActors::TActorId LeaderPartitionActorId;
    bool TakePartitionOwnership = false;
    TString ClientId;
    bool LeaderOutdated = false;

    TFollowerDiskInfo FollowerDiskInfo;
};

///////////////////////////////////////////////////////////////////////////////

//   DataTransfer (migration in progress)
//        |
//        |                          /----------------------\
//        v                          v                      |
//    DataReady ->  [Add "outdated" tag to leader]          |
//                              |        |       Error      |
//        /---------------------/        \------------------/
//        |
//        |         OK               / ---------------------\
//        v                          v                      |
//   OutdatedTagSet    ->    [Switch session]               |
//                              |        |       Error      |
//        ----------------------/        \------------------/
//        |         OK
//        |                                   / ----------------------\
//        v                                   v                       |
//   SessionSwitched  ->  [Propagate follower state (CopyCompleted)]  |
//                              |        |            Error           |
//        ----------------------/        \----------------------------/
//        |         OK
//        |         OK               / ---------------------\
//        v                          v                      |
//   FollowerPropagated  ->  [Destroy leader volume]        |
//                              |        |      Error       |
//        ----------------------/        \------------------/
//        |         OK
//        v
//   Actor destroyed since it created by leader

class TFollowerDiskActor final
    : public TNonreplicatedPartitionMigrationCommonActor
    , public IMigrationOwner
{
public:
    enum class EState
    {
        DataTransfer,
        DataReady,
        SessionSwitched,
        OutdatedTagSet,
        FollowerPropagated,
    };

    enum EFollowerWakeupReason
    {
        RETRY_SWITCH_SESSION_NOTIFY =
            TNonreplicatedPartitionMigrationCommonActor::WR_REASON_COUNT,
        SWITCH_SESSION_DONE_NOTIFY,
        SWITCH_SESSION_ERROR_NOTIFY,
        DESTROY_LEADER_VOLUME_NOTIFY,
    };

private:
    const TChildLogTitle LogTitle;
    const NProto::EStorageMediaKind LeaderMediaKind =
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    const ui64 LeaderBlockCount = 0;
    const ui32 LeaderBlockSize = 0;
    const NActors::TActorId LeaderVolumeActorId;
    const NActors::TActorId LeaderPartitionActorId;
    const bool TakePartitionOwnership = false;
    NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    TString ClientId;

    EState State = EState::DataTransfer;
    TFollowerDiskInfo FollowerDiskInfo;
    NActors::TActorId FollowerPartitionActorId;

    TBackoffDelayProvider Backoff{
        TDuration::Seconds(1),
        TDuration::Seconds(30)};

public:
    TFollowerDiskActor(
        const TLogTitle& parentLogTitle,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        TFollowerDiskActorParams params);

    ~TFollowerDiskActor() override;

    // IMigrationOwner implementation
    void OnBootstrap(const NActors::TActorContext& ctx) override;
    bool OnMessage(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) override;
    void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) override;
    void OnMigrationFinished(const NActors::TActorContext& ctx) override;
    void OnMigrationError(const NActors::TActorContext& ctx) override;

private:
    void PersistFollowerState(
        const NActors::TActorContext& ctx,
        const TFollowerDiskInfo& newDiskInfo);

    void TransferLeadership(const NActors::TActorContext& ctx);
    void SwitchSession(const NActors::TActorContext& ctx);
    void PropagateFollowerState(const NActors::TActorContext& ctx);
    void DestroyLeaderVolume(const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ForwardRequestToLeaderPartition(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ForwardRequestToFollowerPartition(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    bool HandleReadWriteZeroBlocks(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRdmaUnavailable(
        const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRWClientIdChanged(
        const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetChangedBlocks(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateFollowerStateResponse(
        const TEvVolumePrivate::TEvUpdateFollowerStateResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSwitchSessionResponse(
        const NProto::TError& error,
        const NActors::TActorContext& ctx);

    void HandlePropagateFollowerStateResponse(
        const TEvVolumePrivate::TEvLinkOnFollowerCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDestroyLeaderVolumeResponse(
        const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
