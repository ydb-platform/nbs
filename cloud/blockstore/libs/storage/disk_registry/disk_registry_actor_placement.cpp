#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCreatePlacementGroup(
    const TEvService::TEvCreatePlacementGroupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateDisk);

    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received CreatePlacementGroup request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TCreatePlacementGroup>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetGroupId(),
        msg->Record.GetPlacementStrategy(),
        msg->Record.GetPlacementPartitionCount());
}

bool TDiskRegistryActor::PrepareCreatePlacementGroup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCreatePlacementGroup& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteCreatePlacementGroup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TCreatePlacementGroup& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->CreatePlacementGroup(
        db,
        args.GroupId,
        args.PlacementStrategy,
        args.PlacementPartitionCount);
}

void TDiskRegistryActor::CompleteCreatePlacementGroup(
    const TActorContext& ctx,
    TTxDiskRegistry::TCreatePlacementGroup& args)
{
    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s CreatePlacementGroup error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str());
    }

    auto response =
        std::make_unique<TEvService::TEvCreatePlacementGroupResponse>(
            std::move(args.Error));

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleDestroyPlacementGroup(
    const TEvService::TEvDestroyPlacementGroupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateDisk);

    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received DestroyPlacementGroup request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TDestroyPlacementGroup>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetGroupId());
}

bool TDiskRegistryActor::PrepareDestroyPlacementGroup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDestroyPlacementGroup& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteDestroyPlacementGroup(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TDestroyPlacementGroup& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.Error =
        State->DestroyPlacementGroup(db, args.GroupId, args.AffectedDisks);
}

void TDiskRegistryActor::CompleteDestroyPlacementGroup(
    const TActorContext& ctx,
    TTxDiskRegistry::TDestroyPlacementGroup& args)
{
    TWaitDependentAndReply* delayedReply = nullptr;

    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s DestroyPlacementGroup error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str());
    } else {
        if (args.AffectedDisks) {
            delayedReply = UpdateVolumeConfigsWaiters.StartPrincipalTask();
            UpdateVolumeConfigs(
                ctx,
                args.AffectedDisks,
                delayedReply->GetPrincipalTaskId());
        }
    }

    auto response =
        std::make_unique<TEvService::TEvDestroyPlacementGroupResponse>(
            std::move(args.Error));

    if (delayedReply) {
        delayedReply->ArmReply(*args.RequestInfo, std::move(response));
    } else {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleAlterPlacementGroupMembership(
    const TEvService::TEvAlterPlacementGroupMembershipRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateDisk);

    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received AlterPlacementGroupMembership request: %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str(),
        TransactionTimeTracker.GetInflightInfo(GetCycleCount()).c_str());

    ExecuteTx<TAlterPlacementGroupMembership>(
        ctx,
        std::move(requestInfo),
        msg->Record.GetGroupId(),
        msg->Record.GetPlacementPartitionIndex(),
        msg->Record.GetConfigVersion(),
        TVector<TString>(
            msg->Record.GetDisksToAdd().begin(),
            msg->Record.GetDisksToAdd().end()),
        TVector<TString>(
            msg->Record.GetDisksToRemove().begin(),
            msg->Record.GetDisksToRemove().end()));
}

bool TDiskRegistryActor::PrepareAlterPlacementGroupMembership(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAlterPlacementGroupMembership& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteAlterPlacementGroupMembership(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TAlterPlacementGroupMembership& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    args.FailedToAdd = args.DiskIdsToAdd;
    args.Error = State->AlterPlacementGroupMembership(
        db,
        args.GroupId,
        args.PlacementPartitionIndex,
        args.ConfigVersion,
        args.FailedToAdd,
        args.DiskIdsToRemove);
}

void TDiskRegistryActor::CompleteAlterPlacementGroupMembership(
    const TActorContext& ctx,
    TTxDiskRegistry::TAlterPlacementGroupMembership& args)
{
    TWaitDependentAndReply* delayedReply = nullptr;

    if (HasError(args.Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "%s AlterPlacementGroupMembership error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(args.Error).c_str());
    } else {
        TVector<TString> affectedDisks(args.DiskIdsToAdd);
        affectedDisks.insert(
            affectedDisks.end(),
            args.DiskIdsToRemove.begin(),
            args.DiskIdsToRemove.end());
        if (affectedDisks) {
            delayedReply = UpdateVolumeConfigsWaiters.StartPrincipalTask();
            UpdateVolumeConfigs(
                ctx,
                affectedDisks,
                delayedReply->GetPrincipalTaskId());
        }
    }

    auto response =
        std::make_unique<TEvService::TEvAlterPlacementGroupMembershipResponse>(
            std::move(args.Error));
    for (auto& diskId: args.FailedToAdd) {
        *response->Record.AddDisksImpossibleToAdd() = std::move(diskId);
    }

    if (delayedReply) {
        delayedReply->ArmReply(*args.RequestInfo, std::move(response));
    } else {
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleListPlacementGroups(
    const TEvService::TEvListPlacementGroupsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateDisk);

    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received ListPlacementGroups request: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str());

    auto response =
        std::make_unique<TEvService::TEvListPlacementGroupsResponse>();
    for (const auto& x: State->GetPlacementGroups()) {
        *response->Record.AddGroupIds() = x.first;
    }

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleDescribePlacementGroup(
    const TEvService::TEvDescribePlacementGroupRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(AllocateDisk);

    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received DescribePlacementGroup request: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.ShortDebugString().c_str());

    auto response =
        std::make_unique<TEvService::TEvDescribePlacementGroupResponse>();
    if (const auto* g = State->FindPlacementGroup(msg->Record.GetGroupId())) {
        auto* group = response->Record.MutableGroup();
        group->SetGroupId(msg->Record.GetGroupId());
        group->SetPlacementStrategy(g->GetPlacementStrategy());
        group->SetPlacementPartitionCount(g->GetPlacementPartitionCount());
        THashSet<TStringBuf> racks;
        for (const auto& disk: g->GetDisks()) {
            *group->AddDiskIds() = disk.GetDiskId();
            for (const auto& rack: disk.GetDeviceRacks()) {
                if (racks.insert(rack).second) {
                    *group->AddRacks() = rack;
                }
            }
        }
        group->SetConfigVersion(g->GetConfigVersion());
    } else {
        *response->Record.MutableError() = MakeError(
            E_NOT_FOUND,
            Sprintf("no such group: %s", msg->Record.GetGroupId().c_str()));
    }

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
