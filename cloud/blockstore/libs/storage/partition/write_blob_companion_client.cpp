#include "write_blob_companion_client.h"

#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

void TWriteBlobCompanionClient::UpdateWriteThroughput(
    const TInstant& now,
    const NKikimr::NMetrics::TChannel& channel,
    const NKikimr::NMetrics::TGroupId& group,
    ui64 value)
{
    Owner.UpdateWriteThroughput(now, channel, group, value);
}

void TWriteBlobCompanionClient::UpdateNetworkStat(
    const TInstant& now,
    ui64 value)
{
    Owner.UpdateNetworkStat(now, value);
}

void TWriteBlobCompanionClient::ScheduleYellowStateUpdate(
    const NActors::TActorContext& ctx)
{
    Owner.ScheduleYellowStateUpdate(ctx);
}

void TWriteBlobCompanionClient::UpdateYellowState(
    const NActors::TActorContext& ctx)
{
    Owner.UpdateYellowState(ctx);
}

void TWriteBlobCompanionClient::ReassignChannelsIfNeeded(
    const NActors::TActorContext& ctx)
{
    Owner.ReassignChannelsIfNeeded(ctx);
}

void TWriteBlobCompanionClient::UpdateChannelPermissions(
    const NActors::TActorContext& ctx,
    ui32 channel,
    EChannelPermissions permissions)
{
    Owner.UpdateChannelPermissions(ctx, channel, permissions);
}

void TWriteBlobCompanionClient::RegisterSuccess(TInstant now, ui32 groupId)
{
    Owner.State->RegisterSuccess(now, groupId);
}

void TWriteBlobCompanionClient::RegisterDowntime(TInstant now, ui32 groupId)
{
    Owner.State->RegisterDowntime(now, groupId);
}

void TWriteBlobCompanionClient::ProcessIOQueue(
    const NActors::TActorContext& ctx,
    ui32 channel)
{
    Owner.ProcessIOQueue(ctx, channel);
}

TPartitionDiskCounters& TWriteBlobCompanionClient::GetPartCounters()
{
    return *Owner.PartCounters;
}

// IMortalActor implements

void TWriteBlobCompanionClient::Poison(const NActors::TActorContext& ctx)
{
    Owner.Suicide(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
