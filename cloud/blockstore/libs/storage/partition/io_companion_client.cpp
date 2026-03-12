#include "io_companion_client.h"

#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

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

// IMortalActor implements

void TIOCompanionClient::Poison(const NActors::TActorContext& ctx)
{
    Owner.Suicide(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
