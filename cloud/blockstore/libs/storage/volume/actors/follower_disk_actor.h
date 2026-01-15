#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_migration_common_actor.h>
#include <cloud/blockstore/libs/storage/volume/model/follower_disk.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

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

    TFollowerDiskInfo FollowerDiskInfo;
};

///////////////////////////////////////////////////////////////////////////////

//   DataTransferring (migration in progress)
//        |
//        |                          /-------------------------------\
//        v                          v                               |
//   DataTransferred -> [Propagate follower state (DataTransferred)] |
//                              |        |                           |
//                     OK       |        |  On other Error repeat    |
//            /-----------------/        |---------------------------/
//            |                          |
//            |                          |  On NotFoundSchemeShardError
//            |                          v
//            |                    Link error state
//            v
//   LeadershipTransferred -> [Persist LeadershipTransferred on leader]
//                                      |
//                        OK            |
//            /-------------------------/
//            |
//            |
//            v
//  LeadershipTransferredAndPersisted

class TFollowerDiskActor final
    : public TNonreplicatedPartitionMigrationCommonActor
    , public IMigrationOwner
{
public:
    enum class EState
    {
        DataTransferring,
        DataTransferred,
        LeadershipTransferred,
        LeadershipTransferredAndPersisted,
        Error,
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

    TString ClientId;

    EState State = EState::DataTransferring;
    TFollowerDiskInfo FollowerDiskInfo;
    NActors::TActorId FollowerPartitionActorId;

public:
    TFollowerDiskActor(
        const TLogTitle& parentLogTitle,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
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

    // Return true if need to start data transfer.
    bool ApplyLinkState(const NActors::TActorContext& ctx);
    void AdvanceState(const NActors::TActorContext& ctx, EState newState);
    void PropagateLeadershipToFollower(const NActors::TActorContext& ctx);
    void PersistLeadershipTransferred(const NActors::TActorContext& ctx);
    void RebootLeaderVolume(const NActors::TActorContext& ctx, TDuration delay);

    template <typename TMethod>
    void ForwardRequestToLeaderPartition(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ForwardRequestToFollowerPartition(
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

    void HandlePropagateLeadershipToFollowerResponse(
        const TEvVolumePrivate::TEvLinkOnFollowerDataTransferred::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
