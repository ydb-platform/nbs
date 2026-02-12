#include "write_blob_companion_client.h"

#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

void TIOCompanionClient::UpdateWriteThroughput(
    const TInstant& now,
    const NKikimr::NMetrics::TChannel& channel,
    const NKikimr::NMetrics::TGroupId& group,
    ui64 value)
{
    Owner.UpdateWriteThroughput(now, channel, group, value);
}

void TIOCompanionClient::UpdateNetworkStat(
    const TInstant& now,
    ui64 value)
{
    Owner.UpdateNetworkStat(now, value);
}

void TIOCompanionClient::ScheduleYellowStateUpdate(
    const NActors::TActorContext& ctx)
{
    Owner.ScheduleYellowStateUpdate(ctx);
}

void TIOCompanionClient::UpdateYellowState(
    const NActors::TActorContext& ctx)
{
    Owner.UpdateYellowState(ctx);
}

void TIOCompanionClient::ReassignChannelsIfNeeded(
    const NActors::TActorContext& ctx)
{
    Owner.ReassignChannelsIfNeeded(ctx);
}

void TIOCompanionClient::UpdateChannelPermissions(
    const NActors::TActorContext& ctx,
    ui32 channel,
    EChannelPermissions permissions)
{
    Owner.UpdateChannelPermissions(ctx, channel, permissions);
}

void TIOCompanionClient::RegisterSuccess(TInstant now, ui32 groupId)
{
    Owner.State->RegisterSuccess(now, groupId);
}

void TIOCompanionClient::RegisterDowntime(TInstant now, ui32 groupId)
{
    Owner.State->RegisterDowntime(now, groupId);
}

TPartitionDiskCounters& TIOCompanionClient::GetPartCounters()
{
    return *Owner.PartCounters;
}

// IMortalActor implements

void TIOCompanionClient::Poison(const NActors::TActorContext& ctx)
{
    Owner.Suicide(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
