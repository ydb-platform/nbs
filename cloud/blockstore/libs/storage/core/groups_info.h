#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <contrib/ydb/core/base/blobstorage.h>

// TODO:_ do we really need separate file for this?
// TODO:_ naming

namespace NCloud::NBlockStore::NStorage::NGroupsInfo {

////////////////////////////////////////////////////////////////////////////////

// struct TPartitionGroupsInfo {
//     // struct TChannelInfo {
//     //     ui32 Channel;
//     //     TVector<ui32> GroupIds;
//     // };
//
//     const ui64 VolumeTabletId;  // TODO:_ Does it belong here?
//     const ui64 PartitionTabletId;  // TODO:_ Already have it in keys of TVolumeGroupsInfo
//     // TVector<TChannelInfo> ChannelInfos;
//     TVector<NKikimr::TTabletChannelInfo> ChannelInfos;  // TODO:_ maybe we should make our own struct with channel info.
//
//     TPartitionGroupsInfo(
//             ui64 volumeTabletId,
//             ui64 partitionTabletId,
//             TVector<NKikimr::TTabletChannelInfo> channelInfos)
//         : VolumeTabletId(volumeTabletId)
//         , PartitionTabletId(partitionTabletId)
//         , ChannelInfos(std::move(channelInfos))
//     {}
// };

using TVolumeGroupsInfo = THashMap<ui64, TVector<NKikimr::TTabletChannelInfo>>;

} // namespace NCloud::NBlockStore::NStorage::NGroupsInfo
