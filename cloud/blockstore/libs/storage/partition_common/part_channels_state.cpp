#include "part_channels_state.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/tablet/model/channels.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

//////////////////////////////////////////////////////////////////////////////////

double CalculateChannelSpaceScore(
    const TChannelState& ch,
    const TFreeSpaceConfig& fsc,
    const EChannelPermissions permissions)
{
    if (!ch.Permissions.HasFlags(permissions)) {
        return 1;
    }

    if (ch.ApproximateFreeSpaceShare != 0) {
        return 1 - Normalize(
            ch.ApproximateFreeSpaceShare,
            fsc.ChannelMinFreeSpace,
            fsc.ChannelFreeSpaceThreshold
        );
    }

    return 0;
}

double CalculateDiskSpaceScore(
    double systemChannelSpaceScoreSum,
    double dataChannelSpaceScoreSum,
    ui32 dataChannelCount,
    double freshChannelSpaceScoreSum,
    ui32 freshChannelCount)
{
    return 1 / (1 - Min(0.99,
                        systemChannelSpaceScoreSum +
                            dataChannelSpaceScoreSum / dataChannelCount +
                            (freshChannelCount
                                 ? freshChannelSpaceScoreSum / freshChannelCount
                                 : 0)));
}

}   // namespace

//////////////////////////////////////////////////////////////////////////////////

TPartitionChannelsState::TPartitionChannelsState(
        NProto::TPartitionConfig config,
        const TFreeSpaceConfig& freeSpaceConfig,
        ui32 maxIORequestsInFlight,
        ui32 reassignChannelsPercentageThreshold,
        ui32 reassignFreshChannelsPercentageThreshold,
        ui32 reassignMixedChannelsPercentageThreshold,
        bool reassignSystemChannelsImmediately,
        ui32 channelCount)
    : Config(std::move(config))
    , FreeSpaceConfig(freeSpaceConfig)
    , ChannelCount(channelCount)
    , MaxIORequestsInFlight(maxIORequestsInFlight)
    , ReassignChannelsPercentageThreshold(reassignChannelsPercentageThreshold)
    , ReassignFreshChannelsPercentageThreshold(
          reassignFreshChannelsPercentageThreshold)
    , ReassignMixedChannelsPercentageThreshold(
          reassignMixedChannelsPercentageThreshold)
    , ReassignSystemChannelsImmediately(reassignSystemChannelsImmediately)
{
    InitChannels();
}

EChannelDataKind TPartitionChannelsState::GetChannelDataKind(ui32 channel) const
{
    // FIXME(NBS-2088): use Y_ABORT_UNLESS
    Y_DEBUG_ABORT_UNLESS(channel < ChannelCount);
    if (channel >= ChannelCount) {
        return EChannelDataKind::Merged;
    }

    auto kind = Config.GetExplicitChannelProfiles(channel).GetDataKind();
    return static_cast<EChannelDataKind>(kind);
}

TVector<ui32> TPartitionChannelsState::GetChannelsByKind(
    std::function<bool(EChannelDataKind)> predicate) const
{
    TVector<ui32> result(Reserve(ChannelCount));
    for (ui32 ch = 0; ch < ChannelCount; ++ch) {
        if (predicate(GetChannelDataKind(ch))) {
            result.push_back(ch);
        }
    }
    return result;
}

bool TPartitionChannelsState::UpdatePermissions(
    ui32 channel,
    EChannelPermissions permissions)
{
    auto& channelState = GetChannel(channel);
    if (channelState.Permissions != permissions) {
        channelState.Permissions = permissions;

        return UpdateChannelFreeSpaceScore(channelState, channel);
    }

    return false;
}

bool TPartitionChannelsState::CheckPermissions(
    ui32 channel,
    EChannelPermissions permissions) const
{
    const auto* ch = GetChannel(channel);
    return ch ? ch->Permissions.HasFlags(permissions) : true;
}

double TPartitionChannelsState::GetFreeSpaceShare(ui32 channel) const
{
    const auto* ch = GetChannel(channel);
    return ch ? ch->ApproximateFreeSpaceShare : 0;
}

bool TPartitionChannelsState::UpdateChannelFreeSpaceShare(
    ui32 channel,
    double share)
{
    if (share) {
        auto& channelState = GetChannel(channel);
        const auto prevShare = channelState.ApproximateFreeSpaceShare;
        const auto threshold = FreeSpaceConfig.ChannelFreeSpaceThreshold;
        channelState.ApproximateFreeSpaceShare = share;
        if (share < threshold && (!prevShare || prevShare >= threshold)) {
            ++AlmostFullChannelCount;
        } else if (share >= threshold && prevShare && prevShare < threshold) {
            Y_DEBUG_ABORT_UNLESS(AlmostFullChannelCount);
            --AlmostFullChannelCount;
        }

        return UpdateChannelFreeSpaceScore(channelState, channel);
    }

    return false;
}

bool TPartitionChannelsState::CheckChannelFreeSpaceShare(ui32 channel) const
{
    const auto& fsc = FreeSpaceConfig;
    const auto* ch = GetChannel(channel);

    if (!ch) {
        return true;
    }

    return NCloud::CheckChannelFreeSpaceShare(
        ch->ApproximateFreeSpaceShare,
        fsc.ChannelMinFreeSpace,
        fsc.ChannelFreeSpaceThreshold);
}

bool TPartitionChannelsState::IsCompactionAllowed() const
{
    return IsWriteAllowed(EChannelPermission::SystemWritesAllowed);
}

bool TPartitionChannelsState::IsWriteAllowed(
    EChannelPermissions permissions) const
{
    bool allSystemChannelsWritable = true;
    bool anyDataChannelWritable = false;
    bool anyFreshChannelWritable = FreshChannelCount == 0;

    for (ui32 ch = 0; ch < ChannelCount; ++ch) {
        switch (GetChannelDataKind(ch)) {
            case EChannelDataKind::System:
            case EChannelDataKind::Log:
            case EChannelDataKind::Index: {
                if (!CheckPermissions(ch, permissions)) {
                    allSystemChannelsWritable = false;
                }
                break;
            }

            case EChannelDataKind::Mixed:
            case EChannelDataKind::Merged: {
                if (CheckPermissions(ch, permissions)) {
                    anyDataChannelWritable = true;
                }
                break;
            }

            case EChannelDataKind::Fresh: {
                if (CheckPermissions(ch, permissions)) {
                    anyFreshChannelWritable = true;
                }
                break;
            }

            default: {
                Y_DEBUG_ABORT_UNLESS(0);
            }
        }
    }

    return allSystemChannelsWritable && anyDataChannelWritable &&
           anyFreshChannelWritable;
}

void TPartitionChannelsState::RegisterReassignRequestFromBlobStorage(
    ui32 channel)
{
    GetChannel(channel).ReassignRequestedByBlobStorage = true;
}

TVector<ui32> TPartitionChannelsState::GetChannelsToReassign() const
{
    const auto permissions = EChannelPermission::UserWritesAllowed |
                             EChannelPermission::SystemWritesAllowed;

    TVector<ui32> channels;
    TVector<ui32> freshChannels;
    TVector<ui32> mixedChannels;
    TVector<ui32> systemChannels;

    for (ui32 ch = 0; ch < ChannelCount; ++ch) {
        const auto* channelState = GetChannel(ch);
        if ((channelState && channelState->ReassignRequestedByBlobStorage) ||
            !CheckPermissions(ch, permissions))
        {
            channels.push_back(ch);
            switch (GetChannelDataKind(ch)) {
                case EChannelDataKind::Log:
                case EChannelDataKind::Index:
                case EChannelDataKind::System: {
                    systemChannels.push_back(ch);
                    break;
                }

                case EChannelDataKind::Mixed: {
                    mixedChannels.push_back(ch);
                    break;
                }

                case EChannelDataKind::Fresh: {
                    freshChannels.push_back(ch);
                    break;
                }

                case EChannelDataKind::Merged: {
                    break;
                }

                default: {
                    Y_DEBUG_ABORT_UNLESS(0);
                }
            }
        }
    }

    const auto threshold = ReassignChannelsPercentageThreshold * ChannelCount;
    if (!IsWriteAllowed(permissions) || channels.size() * 100 >= threshold) {
        return channels;
    }

    channels.clear();
    if (ReassignMixedChannelsPercentageThreshold < 100 &&
        !mixedChannels.empty())
    {
        const auto threshold =
            ReassignMixedChannelsPercentageThreshold * MixedChannels.size();
        if (mixedChannels.size() * 100 >= threshold) {
            channels.insert(
                channels.end(),
                mixedChannels.begin(),
                mixedChannels.end());
        }
    }

    if (ReassignSystemChannelsImmediately && !systemChannels.empty()) {
        channels.insert(
            channels.end(),
            systemChannels.begin(),
            systemChannels.end());
    }

    if (ReassignFreshChannelsPercentageThreshold < 100 &&
        !freshChannels.empty())
    {
        const auto threshold =
            ReassignFreshChannelsPercentageThreshold * FreshChannels.size();
        if (freshChannels.size() * 100 >= threshold) {
            channels.insert(
                channels.end(),
                freshChannels.begin(),
                freshChannels.end());
        }
    }

    return channels;
}

ui32 TPartitionChannelsState::GetAlmostFullChannelCount() const
{
    return AlmostFullChannelCount;
}

void TPartitionChannelsState::EnqueueIORequest(
    ui32 channel,
    NActors::IActorPtr requestActor,
    ui64 bsGroupOperationId,
    ui32 group,
    TBSGroupOperationTimeTracker::EOperationType operationType,
    ui32 blockSize)
{
    auto& ch = GetChannel(channel);
    ch.IORequests.emplace_back(
        std::move(requestActor),
        bsGroupOperationId,
        group,
        operationType,
        blockSize);
    ++ch.IORequestsQueued;
}

std::optional<TQueuedRequest> TPartitionChannelsState::DequeueIORequest(
    ui32 channel)
{
    auto& ch = GetChannel(channel);
    if (ch.IORequestsQueued && ch.IORequestsInFlight < MaxIORequestsInFlight) {
        TQueuedRequest req = std::move(ch.IORequests.front());
        ch.IORequests.pop_front();
        --ch.IORequestsQueued;
        ++ch.IORequestsInFlight;
        return req;
    }

    return std::nullopt;
}

void TPartitionChannelsState::CompleteIORequest(ui32 channel)
{
    auto& ch = GetChannel(channel);
    --ch.IORequestsInFlight;
}

ui32 TPartitionChannelsState::GetIORequestsInFlight() const
{
    ui32 count = 0;
    for (const auto& ch: Channels) {
        count += ch.IORequestsInFlight;
    }
    return count;
}

ui32 TPartitionChannelsState::GetIORequestsQueued() const
{
    ui32 count = 0;
    for (const auto& ch: Channels) {
        count += ch.IORequestsQueued;
    }
    return count;
}

TPartialBlobId TPartitionChannelsState::GenerateBlobId(
    EChannelDataKind kind,
    EChannelPermissions permissions,
    ui64 commitId,
    ui32 blobSize,
    ui32 blobIndex)
{
    ui32 channel = 0;
    if (blobSize) {
        channel = PickNextChannel(kind, permissions);
        Y_ABORT_UNLESS(channel);
    }

    ui64 generation, step;
    std::tie(generation, step) = ParseCommitId(commitId);

    return TPartialBlobId(
        generation,
        step,
        channel,
        blobSize,
        blobIndex,
        0);   // partId - should always be zero
}

ui32 TPartitionChannelsState::PickNextChannel(
    EChannelDataKind kind,
    EChannelPermissions permissions)
{
    Y_ABORT_UNLESS(
        kind == EChannelDataKind::Fresh || kind == EChannelDataKind::Mixed ||
        kind == EChannelDataKind::Merged);

    const auto& channels = kind == EChannelDataKind::Fresh   ? FreshChannels
                           : kind == EChannelDataKind::Mixed ? MixedChannels
                                                             : MergedChannels;

    auto& selector = kind == EChannelDataKind::Fresh   ? FreshChannelSelector
                     : kind == EChannelDataKind::Mixed ? MixedChannelSelector
                                                       : MergedChannelSelector;

    ++selector;

    ui32 bestChannel = Max<ui32>();
    double bestSpaceShare = 0;
    for (ui32 i = 0; i < channels.size(); ++i) {
        const auto channel = channels[selector % channels.size()];

        if (CheckPermissions(channel, permissions)) {
            if (CheckChannelFreeSpaceShare(channel)) {
                return channel;
            }

            const auto spaceShare =
                GetChannel(channel).ApproximateFreeSpaceShare;
            if (spaceShare > bestSpaceShare) {
                bestSpaceShare = spaceShare;
                bestChannel = channel;
            }
        }

        ++selector;
    }

    if (bestChannel != Max<ui32>()) {
        // all channels are close to full, but bestChannel has more free space
        // than the others
        return bestChannel;
    }

    if (kind == EChannelDataKind::Mixed && HaveSeparateMixedChannels) {
        // not all hope is gone at this point - we can still try to write our
        // mixed blob to one of the channels intended for merged blobs
        return PickNextChannel(EChannelDataKind::Merged, permissions);
    }

    return channels.front();
}

void TPartitionChannelsState::InitChannels()
{
    for (ui32 ch = 0; ch < ChannelCount; ++ch) {
        switch (GetChannelDataKind(ch)) {
            case EChannelDataKind::Mixed: {
                MixedChannels.push_back(ch);
                ++DataChannelCount;
                break;
            }
            case EChannelDataKind::Merged: {
                MergedChannels.push_back(ch);
                ++DataChannelCount;
                break;
            }
            case EChannelDataKind::Fresh: {
                FreshChannels.push_back(ch);
                ++FreshChannelCount;
                break;
            }
            default: {
                break;
            }
        }
    }

    if (MixedChannels) {
        HaveSeparateMixedChannels = true;
    } else {
        MixedChannels = MergedChannels;
    }
}

TChannelState& TPartitionChannelsState::GetChannel(ui32 channel)
{
    if (channel >= Channels.size()) {
        Channels.resize(channel + 1);
    }
    return Channels[channel];
}

const TChannelState* TPartitionChannelsState::GetChannel(ui32 channel) const
{
    if (channel < Channels.size()) {
        return &Channels[channel];
    }
    return nullptr;
}

bool TPartitionChannelsState::UpdateChannelFreeSpaceScore(
    TChannelState& channelState,
    ui32 channel)
{
    const auto kind = GetChannelDataKind(channel);

    EChannelPermissions requiredPermissions =
        kind == EChannelDataKind::Mixed || kind == EChannelDataKind::Merged
            ? EChannelPermission::UserWritesAllowed
            : EChannelPermission::SystemWritesAllowed;

    double& scoreSum = [this, kind]() -> auto&
    {
        switch (kind) {
            case EChannelDataKind::Fresh:
                return FreshChannelSpaceScoreSum;
            case EChannelDataKind::Mixed:
            case EChannelDataKind::Merged:
                return DataChannelSpaceScoreSum;
            default:
                return SystemChannelSpaceScoreSum;
        }
    }();

    scoreSum -= channelState.FreeSpaceScore;
    channelState.FreeSpaceScore = CalculateChannelSpaceScore(
        channelState,
        FreeSpaceConfig,
        requiredPermissions);
    scoreSum += channelState.FreeSpaceScore;

    const auto diskSpaceScore = CalculateDiskSpaceScore(
        SystemChannelSpaceScoreSum,
        DataChannelSpaceScoreSum,
        DataChannelCount,
        FreshChannelSpaceScoreSum,
        FreshChannelCount);

    if (diskSpaceScore != BackpressureDiskSpaceScore) {
        BackpressureDiskSpaceScore = diskSpaceScore;
        return true;
    }

    return false;
}

}   // namespace NCloud::NBlockStore::NStorage
