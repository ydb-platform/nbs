#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/migration_request_actor.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/mirror_request_actor.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandleWriteOrZeroCompleted(
    const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto * msg = ev->Get();
    const auto counter = msg->RequestId;
    if (!WriteAndZeroRequestsInProgress.RemoveRequest(counter)) {
        Y_DEBUG_ABORT_UNLESS(0);
    }

    if (msg->FollowerGotNonRetriableError) {
        OnMigrationNonRetriableError(ctx);
    }

    DrainActorCompanion.ProcessDrainRequests(ctx);
    ScheduleRangeMigration(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TNonreplicatedPartitionMigrationCommonActor::MirrorRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!DstActorId) {
        // TODO(drbasic) use WriteAndZeroRequestsInProgress
        ForwardRequestWithNondeliveryTracking(
            ctx,
            SrcActorId,
            *ev);

        return;
    }

    auto replyError = [&ctx, &ev] (ui32 errorCode, TString errorMessage)
    {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeError(errorCode, std::move(errorMessage)));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    auto* msg = ev->Get();

    const auto range = BuildRequestBlockRange(*msg, BlockSize);

    auto checkOverlapsWithMigration = [&replyError, range](
                                          TBlockRange64 migrationRange) -> bool
    {
        if (range.Overlaps(migrationRange)) {
            replyError(
                E_REJECTED,
                TStringBuilder()
                    << "Request " << TMethod::Name << DescribeRange(range)
                    << " intersects with currently migrated range "
                    << DescribeRange(migrationRange));
            return true;
        }
        return false;
    };

    // Check overlapping with inflight migrations.
    TDisjointRangeSetIterator inProgressIterator{MigrationsInProgress};
    while (inProgressIterator.HasNext()) {
        const auto migrationRange = inProgressIterator.Next();
        if (checkOverlapsWithMigration(migrationRange)) {
            return;
        }
    }

    // While at least one migration is in progress, we are not slowing down user requests.
    if (MigrationsInProgress.Empty()) {
        // Check overlapping with the range that will be migrated next.
        // We need to ensure priority for the migration process, otherwise if
        // the client continuously writes to one block, the migration progress
        // will stall on this block.
        auto nextRange = GetNextMigrationRange();
        if (nextRange && checkOverlapsWithMigration(*nextRange)) {
            return;
        }
    }

    if (TargetMigrationIsLagging) {
        STORAGE_VERIFY(ActorOwner, TWellKnownEntityTypes::DISK, DiskId);

        // TMirrorRequestActor is used here just as a simple request actor.
        NCloud::Register<TMirrorRequestActor<TMethod>>(
            ctx,
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            TVector<NActors::TActorId>{SrcActorId},
            std::move(msg->Record),
            DiskId,
            SelfId(),   // parentActorId
            WriteAndZeroRequestsInProgress.AddWriteRequest(range));
    } else {
        NCloud::Register<TMigrationRequestActor<TMethod>>(
            ctx,
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
            ActorOwner ? SrcActorId : TActorId{},
            DstActorId,
            std::move(msg->Record),
            DiskId,
            SelfId(),   // parentActorId
            WriteAndZeroRequestsInProgress.AddWriteRequest(range));
    }

    if constexpr (IsExactlyWriteMethod<TMethod>) {
        NonZeroRangesMap.MarkChanged(range);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TWriteBlocksMethod>(ev, ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TZeroBlocksMethod>(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
