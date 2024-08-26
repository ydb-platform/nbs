#include "tablet_state_impl.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

ui64 TIndexTabletState::GetTabletChannelCount() const
{
    return TabletStorageInfo.ChannelsSize();
}

ui64 TIndexTabletState::GetConfigChannelCount() const
{
    return FileSystem.ExplicitChannelProfilesSize();
}

void TIndexTabletState::UpdateChannelStats(
    ui32 channel,
    bool writable,
    bool toMove,
    double freeSpaceShare)
{
    Impl->Channels.UpdateChannelStats(
        channel,
        writable,
        toMove,
        freeSpaceShare);
}

TVector<ui32> TIndexTabletState::GetChannels(EChannelDataKind kind) const
{
    return Impl->Channels.GetChannels(kind);
}

TVector<ui32> TIndexTabletState::GetUnwritableChannels() const
{
    return Impl->Channels.GetUnwritableChannels();
}

TVector<ui32> TIndexTabletState::GetChannelsToMove(ui32 percentageThreshold) const
{
    return Impl->Channels.GetChannelsToMove(percentageThreshold);
}

TVector<NCloud::NStorage::TChannelMonInfo>
TIndexTabletState::MakeChannelMonInfos() const
{
    return Impl->Channels.MakeChannelMonInfos();
}

TChannelsStats TIndexTabletState::CalculateChannelsStats() const
{
    return Impl->Channels.CalculateChannelsStats();
}

void TIndexTabletState::LoadChannels()
{
    Y_ABORT_UNLESS(Impl->Channels.Empty());

    // This value never decreases during tablet lifetime since
    // tabletChannelCount and configChannelCount are only allowed to
    // increase on tablet restart and updateConfig request correspondingly.
    // However we have to take minimum of these values due to
    // possibility of their mismatch.
    const ui32 channelCount = Min(
        GetTabletChannelCount(),
        GetConfigChannelCount());

    for (ui32 channel = 0; channel < channelCount; ++channel) {
        const auto& profile = FileSystem.GetExplicitChannelProfiles(channel);

        Impl->Channels.AddChannel(
            channel,
            static_cast<EChannelDataKind>(profile.GetDataKind()),
            profile.GetPoolKind());
    }
}

void TIndexTabletState::UpdateChannels()
{
    const ui32 oldChannelCount = Impl->Channels.Size();
    const ui32 newChannelCount = Min(
        GetTabletChannelCount(),
        GetConfigChannelCount());

    Y_ABORT_UNLESS(oldChannelCount <= newChannelCount);

    if (oldChannelCount == newChannelCount) {
        // Nothing to update.
        return;
    }

    for (ui32 channel = oldChannelCount; channel < newChannelCount; ++channel) {
        const auto& profile = FileSystem.GetExplicitChannelProfiles(channel);

        Impl->Channels.AddChannel(
            channel,
            static_cast<EChannelDataKind>(profile.GetDataKind()),
            profile.GetPoolKind());
    }
}

}   // namespace NCloud::NFileStore::NStorage
