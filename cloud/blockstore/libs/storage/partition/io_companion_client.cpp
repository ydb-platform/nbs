#include "io_companion_client.h"

#include "part_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

void TIOCompanionClient::ProcessStorageStatusFlags(
    const NActors::TActorContext& ctx,
    NKikimr::TStorageStatusFlags flags,
    ui32 channel,
    ui32 generation,
    double approximateFreeSpaceShare)
{
    Owner.ProcessStorageStatusFlags(
        ctx,
        flags,
        channel,
        generation,
        approximateFreeSpaceShare,
        true);   // notify FreshBlocksWriter
}

// IMortalActor implements

void TIOCompanionClient::Poison(const NActors::TActorContext& ctx)
{
    Owner.Suicide(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
