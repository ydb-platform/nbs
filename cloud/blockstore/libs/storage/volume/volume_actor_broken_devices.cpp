#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleBrokenDeviceNotification(
    const TEvNonreplPartitionPrivate::TEvBrokenDeviceNotification::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Device %s is broken (broken at %s)",
        LogTitle.GetWithTime().c_str(),
        msg->DeviceUUID.Quote().c_str(),
        msg->BrokenTs.ToString().c_str());

    // TODO: save the broken device to the database
}

void TVolumeActor::HandleDeviceRecoveredNotification(
    const TEvNonreplPartitionPrivate::TEvDeviceRecoveredNotification::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Device %s has recovered",
        LogTitle.GetWithTime().c_str(),
        msg->DeviceUUID.Quote().c_str());

    // TODO: remove the device from the list of broken ones in the database
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
