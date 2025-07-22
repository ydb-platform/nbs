#include "part2_state.h"

#include "part2_counters.h"

#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/partition2/model/alloc.h>
#include <cloud/blockstore/libs/storage/partition2/model/rebase_logic.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/tablet/model/channels.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/algorithm.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NMonitoringUtils;

using TJsonValue = NJson::TJsonValue;

namespace {

////////////////////////////////////////////////////////////////////////////////

TJsonValue ToJson(const TOperationState& op)
{
    TJsonValue json;
    json["Status"] = ToString(op.GetStatus());
    const auto duration = TInstant::Now() - op.GetTimestamp();
    json["Duration"] = duration.MicroSeconds();
    return json;
}

////////////////////////////////////////////////////////////////////////////////

double BPFeature(const TBackpressureFeatureConfig& c, double x)
{
    auto nx = Normalize(x, c.InputThreshold, c.InputLimit);
    return (1 - nx) + nx * c.MaxValue;
}

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
    return 1 / (1 - Min(0.99, systemChannelSpaceScoreSum
            + dataChannelSpaceScoreSum / dataChannelCount
            + (freshChannelCount ?
            freshChannelSpaceScoreSum / freshChannelCount : 0)));
}

////////////////////////////////////////////////////////////////////////////////

void ApplyUpdate(const TVector<TDeletedBlock>& deletedBlocks, TBlock& block)
{
    auto deletion = UpperBound(
        deletedBlocks.begin(),
        deletedBlocks.end(),
        TDeletedBlock(block.BlockIndex, block.MinCommitId)
    );

    if (deletion != deletedBlocks.end()
            && deletion->BlockIndex == block.BlockIndex
            && deletion->CommitId < block.MaxCommitId)
    {
        block.MaxCommitId = deletion->CommitId;
    }
}

bool CheckBlob(
    TPartialBlobId blobId,
    const TVector<TBlock>& blocks,
    ui32 blockSize)
{
    auto expectedDataBlockCount = blobId.BlobSize() / blockSize;
    ui32 actualDataBlockCount = 0;
    for (const auto& block: blocks) {
        if (!block.Zeroed) {
            ++actualDataBlockCount;
        }
    }
    return expectedDataBlockCount == actualDataBlockCount;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TOperationState::Dump(IOutputStream& out) const
{
    out << "State: ";
    switch (Status) {
        case EOperationStatus::Idle:
            out << "Idle";
            break;

        case EOperationStatus::Enqueued:
            out << "Enqueued";
            break;

        case EOperationStatus::Delayed:
            out << "Delayed";
            break;

        case EOperationStatus::Started:
            out << "Started";
            break;
    }

    auto delay = TInstant::Now() - Timestamp;
    if (delay < TDuration::Minutes(30)) {
        out << " for " << delay;
    }

    out << ", Total: " << Count;
}

////////////////////////////////////////////////////////////////////////////////

TPartitionState::TPartitionState(
        NProto::TPartitionMeta meta,
        ui64 tabletId,
        ui32 generation,
        ui32 channelCount,
        ui32 maxBlobSize,
        ui32 maxRangesPerBlob,
        EOptimizationMode optimizationMode,
        ICompactionPolicyPtr compactionPolicy,
        TBackpressureFeaturesConfig bpConfig,
        TFreeSpaceConfig freeSpaceConfig,
        TIndexCachingConfig indexCachingConfig,
        ui32 maxIORequestsInFlight,
        ui32 reassignChannelsPercentageThreshold,
        ui32 reassignMixedChannelsPercentageThreshold,
        ui32 lastStep)
    : Meta(std::move(meta))
    , TabletId(tabletId)
    , Generation(generation)
    , MaxBlobSize(maxBlobSize)
    , MaxRangesPerBlob(maxRangesPerBlob)
    , CompactionPolicy(compactionPolicy)
    , BPConfig(bpConfig)
    , FreeSpaceConfig(freeSpaceConfig)
    , IndexCachingConfig(indexCachingConfig)
    , Config(*Meta.MutableConfig())
    , LastStep(lastStep)
    , MaxIORequestsInFlight(maxIORequestsInFlight)
    , ReassignChannelsPercentageThreshold(reassignChannelsPercentageThreshold)
    , ReassignMixedChannelsPercentageThreshold(reassignMixedChannelsPercentageThreshold)
    , ChannelCount(channelCount)
    , FreshBlocks(GetAllocatorByTag(EAllocatorTag::FreshBlockMap))
    , Blobs(
        ceil(double(Config.GetBlocksCount()) / Config.GetZoneBlockCount()),
        Config.GetZoneBlockCount(),
        IndexCachingConfig.BlockListCacheSizeShare * Config.GetBlocksCount(),
        GetMaxBlocksInBlob(),
        optimizationMode)
    , CompactionMap(GetMaxBlocksInBlob(), std::move(compactionPolicy))
    , Stats(*Meta.MutableStats())
{
    Y_ABORT_UNLESS(Config.GetZoneBlockCount());
    InitChannels();
}

////////////////////////////////////////////////////////////////////////////////
// Config

bool TPartitionState::CheckBlockRange(const TBlockRange64& range) const
{
    Y_DEBUG_ABORT_UNLESS(Config.GetBlocksCount() <= Max<ui32>());
    auto validRange = TBlockRange64::WithLength(0, Config.GetBlocksCount());
    return validRange.Contains(range);
}

////////////////////////////////////////////////////////////////////////////////
// Channels

void TPartitionState::InitChannels()
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

TVector<ui32> TPartitionState::GetChannelsByKind(
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

EChannelDataKind TPartitionState::GetChannelDataKind(ui32 channel) const
{
    // FIXME(NBS-2088): use Y_ABORT_UNLESS
    Y_DEBUG_ABORT_UNLESS(channel < ChannelCount);
    if (channel >= ChannelCount) {
        return EChannelDataKind::Merged;
    }

    auto kind = Config.GetExplicitChannelProfiles(channel).GetDataKind();
    return static_cast<EChannelDataKind>(kind);
}

TChannelState& TPartitionState::GetChannel(ui32 channel)
{
    if (channel >= Channels.size()) {
        Channels.resize(channel + 1);
    }
    return Channels[channel];
}

const TChannelState* TPartitionState::GetChannel(ui32 channel) const
{
    if (channel < Channels.size()) {
        return &Channels[channel];
    }
    return nullptr;
}

bool TPartitionState::UpdatePermissions(ui32 channel, EChannelPermissions permissions)
{
    auto& channelState = GetChannel(channel);
    if (channelState.Permissions != permissions) {
        channelState.Permissions = permissions;

        return UpdateChannelFreeSpaceScore(channelState, channel);
    }

    return false;
}

bool TPartitionState::CheckPermissions(ui32 channel, EChannelPermissions permissions) const
{
    const auto* ch = GetChannel(channel);
    return ch ? ch->Permissions.HasFlags(permissions) : true;
}

bool TPartitionState::UpdateChannelFreeSpaceShare(ui32 channel, double share)
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

bool TPartitionState::UpdateChannelFreeSpaceScore(
    TChannelState& channelState,
    ui32 channel)
{
    const auto kind = GetChannelDataKind(channel);

    EChannelPermissions requiredPermissions =
        kind == EChannelDataKind::Mixed || kind == EChannelDataKind::Merged
        ? EChannelPermission::UserWritesAllowed
        : EChannelPermission::SystemWritesAllowed;

    double& scoreSum = [this, kind]() -> auto& {
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
        requiredPermissions
    );
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

bool TPartitionState::CheckChannelFreeSpaceShare(ui32 channel) const
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

bool TPartitionState::IsCompactionAllowed() const
{
    return IsWriteAllowed(EChannelPermission::SystemWritesAllowed);
}

bool TPartitionState::IsWriteAllowed(EChannelPermissions permissions) const
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

void TPartitionState::RegisterReassignRequestFromBlobStorage(ui32 channel)
{
    GetChannel(channel).ReassignRequestedByBlobStorage = true;
}

TVector<ui32> TPartitionState::GetChannelsToReassign() const
{
    const auto permissions = EChannelPermission::UserWritesAllowed |
                             EChannelPermission::SystemWritesAllowed;

    TVector<ui32> channels;
    TVector<ui32> mixedChannels;

    for (ui32 ch = 0; ch < ChannelCount; ++ch) {
        const auto* channelState = GetChannel(ch);
        if (channelState && channelState->ReassignRequestedByBlobStorage ||
            !CheckPermissions(ch, permissions))
        {
            channels.push_back(ch);
            if (GetChannelDataKind(ch) == EChannelDataKind::Mixed) {
                mixedChannels.push_back(ch);
            }
        }
    }

    const auto threshold = ReassignChannelsPercentageThreshold * ChannelCount;
    if (!IsWriteAllowed(permissions) || channels.size() * 100 >= threshold) {
        return channels;
    }

    if (ReassignMixedChannelsPercentageThreshold < 100 &&
        !mixedChannels.empty())
    {
        const auto threshold =
            ReassignChannelsPercentageThreshold * MixedChannels.size();
        if (mixedChannels.size() * 100 >= threshold) {
            return mixedChannels;
        }
    }

    return {};
}

TBackpressureReport TPartitionState::CalculateCurrentBackpressure() const
{
    const auto& freshFeature = BPConfig.FreshByteCountFeatureConfig;
    const auto& compactionFeature = BPConfig.CompactionScoreFeatureConfig;
    const auto& cleanupFeature = BPConfig.CleanupQueueBytesFeatureConfig;

    return {
        BPFeature(freshFeature, GetFreshBlockCount() * GetBlockSize()),
        CompactionPolicy->BackpressureEnabled()
            ? BPFeature(compactionFeature, GetLegacyCompactionScore())
            : 0,
        BackpressureDiskSpaceScore,
        BPFeature(cleanupFeature, GetPendingUpdates() * GetBlockSize()),
    };
}

ui32 TPartitionState::GetAlmostFullChannelCount() const
{
    return AlmostFullChannelCount;
}

void TPartitionState::EnqueueIORequest(ui32 channel, IActorPtr requestActor)
{
    auto& ch = GetChannel(channel);
    ch.IORequests.emplace_back(std::move(requestActor));
    ++ch.IORequestsQueued;
}

IActorPtr TPartitionState::DequeueIORequest(ui32 channel)
{
    auto& ch = GetChannel(channel);
    if (ch.IORequestsQueued && ch.IORequestsInFlight < MaxIORequestsInFlight) {
        IActorPtr requestActor = std::move(ch.IORequests.front());
        ch.IORequests.pop_front();
        --ch.IORequestsQueued;
        ++ch.IORequestsInFlight;
        return requestActor;
    }

    return {};
}

void TPartitionState::CompleteIORequest(ui32 channel)
{
    auto& ch = GetChannel(channel);
    --ch.IORequestsInFlight;
}

ui32 TPartitionState::GetIORequestsInFlight() const
{
    ui32 count = 0;
    for (const auto& ch: Channels) {
        count += ch.IORequestsInFlight;
    }
    return count;
}

ui32 TPartitionState::GetIORequestsQueued() const
{
    ui32 count = 0;
    for (const auto& ch: Channels) {
        count += ch.IORequestsQueued;
    }
    return count;
}

ui32 TPartitionState::PickNextChannel(EChannelDataKind kind, EChannelPermissions permissions)
{
    Y_ABORT_UNLESS(kind == EChannelDataKind::Fresh ||
        kind == EChannelDataKind::Mixed ||
        kind == EChannelDataKind::Merged);

    const auto& channels =
        kind == EChannelDataKind::Fresh ? FreshChannels
        : kind == EChannelDataKind::Mixed ? MixedChannels
        : MergedChannels;

    auto& selector =
        kind == EChannelDataKind::Fresh ? FreshChannelSelector
        : kind == EChannelDataKind::Mixed ? MixedChannelSelector
        : MergedChannelSelector;

    ++selector;
    const auto last = selector;

    ui32 bestChannel = Max<ui32>();
    double bestSpaceShare = 0;
    while (selector < last + channels.size()) {
        const auto channel = channels[selector % channels.size()];

        if (CheckPermissions(channel, permissions)) {
            if (CheckChannelFreeSpaceShare(channel)) {
                return channel;
            }

            const auto spaceShare = GetChannel(channel).ApproximateFreeSpaceShare;
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

TPartialBlobId TPartitionState::GenerateBlobId(
    EChannelDataKind kind,
    EChannelPermissions permissions,
    ui64 commitId,
    ui32 blobSize,
    ui32 blobIndex)
{
    ui32 channel = PickNextChannel(kind, permissions);
    Y_ABORT_UNLESS(channel);

    ui64 generation, step;
    std::tie(generation, step) = ParseCommitId(commitId);
    Y_ABORT_UNLESS(generation == Generation);

    return TPartialBlobId(
        generation,
        step,
        channel,
        blobSize,
        blobIndex,
        0); // partId - should always be zero
}

////////////////////////////////////////////////////////////////////////////////
// Fresh blocks

void TPartitionState::InitFreshBlocks(const TVector<TOwningFreshBlock>& freshBlocks)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(freshBlocks.begin(), freshBlocks.end(),
        [](const auto& lhs, const auto& rhs) {
            return std::make_pair(lhs.Meta.BlockIndex, lhs.Meta.MinCommitId)
                <  std::make_pair(rhs.Meta.BlockIndex, rhs.Meta.MinCommitId);
        }
    ));

    for (const auto& freshBlock: freshBlocks) {
        const auto& meta = freshBlock.Meta;

        if (meta.MinCommitId <= GetLastFlushCommitId()) {
            // the block has already been flushed
            continue;
        }

        bool added = FreshBlocks.AddBlock(
            meta.BlockIndex,
            freshBlock.Content,
            meta.MinCommitId,
            meta.MaxCommitId);

        Y_ABORT_UNLESS(added, "Duplicate block detected: %u @%lu",
            meta.BlockIndex,
            meta.MinCommitId);

        FreshBlocks.AddDeletedBlock(meta.BlockIndex, meta.MinCommitId);
    }
}

void TPartitionState::WriteFreshBlock(
    const TBlock& block,
    TBlockDataRef blockContent)
{
    bool added = FreshBlocks.AddBlock(
        block.BlockIndex,
        blockContent.AsStringBuf(),
        block.MinCommitId,
        block.MaxCommitId);

    Y_ABORT_UNLESS(added, "Duplicate block detected: %u @%lu",
        block.BlockIndex,
        block.MinCommitId);

    FreshBlocks.AddDeletedBlock(block.BlockIndex, block.MinCommitId);
}

bool TPartitionState::DeleteFreshBlock(ui32 blockIndex, ui64 commitId)
{
    return FreshBlocks.RemoveBlock(blockIndex, commitId);
}

const TFreshBlock* TPartitionState::FindFreshBlock(
    ui32 blockIndex,
    ui64 commitId) const
{
    return FreshBlocks.FindBlock(blockIndex, commitId);
}

void TPartitionState::FindFreshBlocks(
    ui64 commitId,
    const TBlockRange32& blockRange,
    IFreshBlockVisitor& visitor) const
{
    FindFreshBlocks(FreshBlocks.FindBlocks(blockRange, commitId), visitor);
}

void TPartitionState::FindFreshBlocks(
    const TBlockRange32& blockRange,
    IFreshBlockVisitor& visitor) const
{
    FindFreshBlocks(FreshBlocks.FindBlocks(blockRange), visitor);
}

void TPartitionState::FindFreshBlocks(IFreshBlockVisitor& visitor) const
{
    FindFreshBlocks(FreshBlocks.FindBlocks(), visitor);
}

void TPartitionState::FindFreshBlocks(
    const TVector<TFreshBlock>& blocks,
    IFreshBlockVisitor& visitor) const
{
    for (const auto& block: blocks) {
        Y_ABORT_UNLESS(block.Content.size() == Config.GetBlockSize());
        visitor.Visit(block.Meta, block.Content);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fresh block updates

void TPartitionState::SetFreshBlockUpdates(TFreshBlockUpdates updates)
{
    FreshBlockUpdates = std::move(updates);
}

void TPartitionState::ApplyFreshBlockUpdates()
{
    for (const auto update: FreshBlockUpdates) {
        for (const ui32 blockIndex: xrange(update.BlockRange)) {
            FreshBlocks.AddDeletedBlock(blockIndex, update.CommitId);
        }
    }
}

size_t TPartitionState::GetFreshBlockUpdateCount() const
{
    return FreshBlockUpdates.size();
}

void TPartitionState::AddFreshBlockUpdate(
    TPartitionDatabase& db,
    TFreshBlockUpdate update)
{
    if (!FreshBlockUpdates.empty()) {
        Y_ABORT_UNLESS(FreshBlockUpdates.back().CommitId <= update.CommitId);
    }

    FreshBlockUpdates.push_back(update);
    db.AddFreshBlockUpdate(update);

    for (const ui32 blockIndex: xrange(update.BlockRange)) {
        FreshBlocks.AddDeletedBlock(blockIndex, update.CommitId);
    }
}

void TPartitionState::TrimFreshBlockUpdates(TPartitionDatabase& db)
{
    auto first = FreshBlockUpdates.begin();
    auto last = std::upper_bound(
        FreshBlockUpdates.begin(),
        FreshBlockUpdates.end(),
        GetLastFlushCommitId(),
        [](ui64 commitId, TFreshBlockUpdate rhs) {
            return commitId < rhs.CommitId;
        });

    db.TrimFreshBlockUpdates(first, last);

    for (; first != last; ++first) {
        FreshBlockUpdates.pop_front();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Blob updates by fresh

void TPartitionState::SetBlobUpdatesByFresh(
    TBlobUpdatesByFresh blobUpdatesByFresh)
{
    BlobUpdatesByFresh = std::move(blobUpdatesByFresh);
}

void TPartitionState::ApplyBlobUpdatesByFresh()
{
    auto it = BlobUpdatesByFresh.begin();
    while (it != BlobUpdatesByFresh.end()) {
        LastDeletionId = Max(LastDeletionId, it->DeletionId);

        if (it->CommitId <= GetLastFlushCommitId()) {
            // The blocks have been flushed already but the tablet had restarted
            // before the blobs were trimmed. Thus we don't need this blobUpdate
            BlobUpdatesByFresh.erase(it++);
            continue;
        }

        Blobs.MarkBlocksDeleted(*it);
        ++it;
    }
}

void TPartitionState::AddBlobUpdateByFresh(TBlobUpdate blobUpdate)
{
    Blobs.MarkBlocksDeleted(blobUpdate);
    const bool inserted = BlobUpdatesByFresh.insert(blobUpdate).second;
    Y_ABORT_UNLESS(inserted);
}

void TPartitionState::MoveBlobUpdatesByFreshToDb(TPartitionDatabase& db)
{
    auto it = BlobUpdatesByFresh.begin();
    while (it != BlobUpdatesByFresh.end()) {
        if (it->CommitId > GetLastFlushCommitId()) {
            // The blocks have not been flushed yet
            ++it;
            continue;
        }

        const auto zoneRange = Blobs.ToZoneRange(it->BlockRange);

        if (zoneRange.first != zoneRange.second) {
            db.WriteGlobalBlobUpdate(
                it->DeletionId,
                it->CommitId,
                it->BlockRange
            );
        } else {
            db.WriteZoneBlobUpdate(
                zoneRange.first,
                it->DeletionId,
                it->CommitId,
                it->BlockRange
            );
        }

        BlobUpdatesByFresh.erase(it++);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Merged blobs

void TPartitionState::WriteBlob(
    TPartitionDatabase& db,
    const TPartialBlobId& blobId,
    TVector<TBlock>& blocks)
{
    Y_DEBUG_ABORT_UNLESS(CheckBlob(blobId, blocks, GetBlockSize()));

    auto blockRanges = BuildRanges(blocks, MaxRangesPerBlob);

    Blobs.ApplyUpdates(
        TBlockRange32::MakeClosedInterval(
            blocks.front().BlockIndex,
            blocks.back().BlockIndex),
        blocks);

    // rebase blocks
    TSet<ui64> checkpointIds;

    const auto blockCounts = RebaseBlocks(
        Checkpoints,
        [&] (ui32 blockIndex) {
            return FreshBlocks.HasBlockAt(blockIndex)
                || FreshBlocksInFlight.HasBlocksAt(blockIndex);
        },
        GetLastCommitId(),
        blocks,
        checkpointIds
    );

    Checkpoints.SetCheckpointBlockCount(
        Checkpoints.GetCheckpointBlockCount() + blockCounts.CheckpointBlocks
    );

    if (blockCounts.LiveBlocks) {
        NProto::TBlobMeta2 blobMeta;
        blobMeta.SetBlockCount(blocks.size());
        blobMeta.SetCheckpointBlockCount(blockCounts.CheckpointBlocks);
        for (const auto& blockRange: blockRanges) {
            blobMeta.AddStartIndices(blockRange.Start);
            blobMeta.AddEndIndices(blockRange.End);
        }

        auto addedBlobInfo = Blobs.AddBlob(
            blobId,
            blockRanges,
            blocks.size(),
            blockCounts.CheckpointBlocks
        );

        Y_ABORT_UNLESS(addedBlobInfo.Added, "Duplicate blob detected: %s",
            DumpBlobIds(TabletId, blobId).data());

        if (!IsDeletionMarker(blobId)) {
            auto addedToGarbageQueue = GarbageQueue.AddNewBlob(blobId);
            Y_ABORT_UNLESS(addedToGarbageQueue);
        }

        if (Blobs.IsGlobalZone(addedBlobInfo.ZoneId)) {
            db.WriteGlobalBlob(blobId, blobMeta);
        } else {
            db.WriteZoneBlob(addedBlobInfo.ZoneId, blobId, blobMeta);
        }

        auto blockList = BuildBlockList(blocks);
        db.WriteBlockList(blobId, blockList);
        Blobs.AddBlockList(addedBlobInfo.ZoneId, blobId, blockList, blocks.size());

        if (blockCounts.GarbageBlocks && !IsDeletionMarker(blobId)) {
            if (Blobs.IsGlobalZone(addedBlobInfo.ZoneId)) {
                db.WriteGlobalBlobGarbage({blobId, blockCounts.GarbageBlocks});
            } else {
                db.WriteZoneBlobGarbage(
                    addedBlobInfo.ZoneId,
                    {blobId, blockCounts.GarbageBlocks}
                );
            }
            Blobs.AddGarbage(addedBlobInfo.ZoneId, blobId, blockCounts.GarbageBlocks);
        }

        for (ui64 commitId: checkpointIds) {
            db.WriteCheckpointBlob(commitId, blobId);
        }

        ui16 blobOffset = 0;
        for (const auto& block: blocks) {
            if (block.MinCommitId != block.MaxCommitId) {
                if (block.Zeroed) {
                    Blobs.WriteOrUpdateMixedBlock({block, {blobId, ZeroBlobOffset}});
                } else {
                    Blobs.WriteOrUpdateMixedBlock({block, {blobId, blobOffset}});
                }
            }
            if (!block.Zeroed) {
                ++blobOffset;
            }
        }
    } else if (!IsDeletionMarker(blobId)) {
        // it seems that blob has been overwritten while writing
        // XXX do we need to add it to garbage queue? it should not have gotten a keep
        // flag
        bool added = GarbageQueue.AddGarbageBlob(blobId);
        Y_ABORT_UNLESS(added);

        db.WriteGarbageBlob(blobId);
    }
}

bool TPartitionState::UpdateBlob(
    TPartitionDatabase& db,
    const TPartialBlobId& blobId,
    bool fastPathAllowed,
    TVector<TBlock>& blocks)
{
    const auto blobInfo = Blobs.AccessBlob(
        blocks.front().BlockIndex / Config.GetZoneBlockCount(),
        blobId
    );
    auto* blob = blobInfo.Blob;
    if (!blob) {
        return false;
    }

    const ui32 updateCount = Blobs.ApplyUpdates(
        TBlockRange32::MakeClosedInterval(
            blob->BlockRanges.front().Start,
            blob->BlockRanges.back().End),
        blocks);

    if (fastPathAllowed && updateCount == 0) {
        return true;
    }

    // rebase blocks
    TSet<ui64> checkpointIds;

    const auto blockCounts = RebaseBlocks(
        Checkpoints,
        [&] (ui32 blockIndex) {
            return FreshBlocks.HasBlockAt(blockIndex)
                || FreshBlocksInFlight.HasBlocksAt(blockIndex);
        },
        GetLastCommitId(),
        blocks,
        checkpointIds
    );

    Checkpoints.SetCheckpointBlockCount(
        Checkpoints.GetCheckpointBlockCount()
            + blockCounts.CheckpointBlocks
            - blob->CheckpointBlockCount
    );

    blob->CheckpointBlockCount = blockCounts.CheckpointBlocks;

    if (blockCounts.LiveBlocks) {
        // some blocks are still accessible by the clients
        auto blockList = BuildBlockList(blocks);
        db.WriteBlockList(blobId, blockList);
        Blobs.AddBlockList(blobInfo.ZoneId, blobId, blockList, blocks.size());

        if (blockCounts.GarbageBlocks && !IsDeletionMarker(blobId)) {
            if (Blobs.IsGlobalZone(blobInfo.ZoneId)) {
                db.WriteGlobalBlobGarbage({blobId, blockCounts.GarbageBlocks});
            } else {
                db.WriteZoneBlobGarbage(
                    blobInfo.ZoneId,
                    {blobId, blockCounts.GarbageBlocks}
                );
            }
            Blobs.AddGarbage(blobInfo.ZoneId, blobId, blockCounts.GarbageBlocks);
        }

        for (ui64 commitId: checkpointIds) {
            db.WriteCheckpointBlob(commitId, blobId);
        }
    } else {
        // it is safe now to delete blob from index
        bool removed = Blobs.RemoveBlob(blobInfo.ZoneId, blobId);
        Y_ABORT_UNLESS(removed);

        if (!IsDeletionMarker(blobId)) {
            bool added = GarbageQueue.AddGarbageBlob(blobId);
            Y_ABORT_UNLESS(added);
            if (Blobs.IsGlobalZone(blobInfo.ZoneId)) {
                db.DeleteGlobalBlobGarbage(blobId);
            } else {
                db.DeleteZoneBlobGarbage(blobInfo.ZoneId, blobId);
            }
            db.WriteGarbageBlob(blobId);
        }

        if (Blobs.IsGlobalZone(blobInfo.ZoneId)) {
            db.DeleteGlobalBlob(blobId);
        } else {
            db.DeleteZoneBlob(blobInfo.ZoneId, blobId);
        }
        db.DeleteBlockList(blobId);
    }

    return true;
}

void TPartitionState::ExtractBlobsFromChunkToCleanup(size_t limit)
{
    const size_t cap = Min(limit, PendingChunkToCleanup.size());

    PendingBlobsToCleanup.clear();
    PendingBlobsToCleanup.reserve(cap);

    auto it = PendingChunkToCleanup.rbegin();
    while (PendingBlobsToCleanup.size() < cap) {
        PendingBlobsToCleanup.push_back(*it);
        ++it;
    }

    PendingChunkToCleanup.resize(PendingChunkToCleanup.size() - cap);
}

TVector<TBlobUpdate> TPartitionState::FinishDirtyBlobCleanup(
    TPartitionDatabase& db)
{
    PendingBlobsToCleanup.clear();

    if (!PendingChunkToCleanup.empty()) {
        // we haven't cleaned up the chunk yet
        return {};
    }

    auto deletionsInfo = Blobs.CleanupDirtyRanges();

    for (const auto& blobUpdate: deletionsInfo.BlobUpdates) {
        auto it = BlobUpdatesByFresh.find(blobUpdate);
        if (it != BlobUpdatesByFresh.end()) {
            BlobUpdatesByFresh.erase(it);
            continue;
        }

        if (Blobs.IsGlobalZone(deletionsInfo.ZoneId)) {
            db.DeleteGlobalBlobUpdate(blobUpdate.DeletionId);
        } else {
            db.DeleteZoneBlobUpdate(deletionsInfo.ZoneId, blobUpdate.DeletionId);
        }
    }

    return std::move(deletionsInfo.BlobUpdates);
}

void TPartitionState::ExtractCheckpointBlobsToCleanup(size_t limit)
{
    Y_ABORT_UNLESS(!CleanupCheckpointCommitId);

    // TODO: store zone ids with checkpoint blobs
    PendingCheckpointBlobsToCleanup = CheckpointsToDelete.ExtractBlobsToCleanup(
        limit,
        &CleanupCheckpointCommitId);
}

void TPartitionState::FinishCheckpointBlobCleanup(TPartitionDatabase& db)
{
    if (!CleanupCheckpointCommitId) {
        // we haven't cleaned up all the checkpoint blobs yet
        return;
    }

    const auto blobIds = CheckpointsToDelete.DeleteNextCheckpoint();

    for (const auto& blobId: blobIds) {
        db.DeleteCheckpointBlob(CleanupCheckpointCommitId, blobId);
    }

    db.DeleteCheckpoint(CleanupCheckpointCommitId);

    CleanupCheckpointCommitId = 0;
}

bool TPartitionState::DeleteBlob(
    TPartitionDatabase& db,
    const TPartialBlobId& blobId)
{
    // TODO: zoneHint
    const auto blobInfo = Blobs.FindBlob(0, blobId);
    const auto* blob = blobInfo.Blob;
    if (!blob) {
        return false;
    }

    bool removed = Blobs.RemoveBlob(blobInfo.ZoneId, blobId);
    Y_ABORT_UNLESS(removed);

    if (!IsDeletionMarker(blobId)) {
        bool added = GarbageQueue.AddGarbageBlob(blobId);
        Y_ABORT_UNLESS(added);
        if (Blobs.IsGlobalZone(blobInfo.ZoneId)) {
            db.DeleteGlobalBlobGarbage(blobId);
        } else {
            db.DeleteZoneBlobGarbage(blobInfo.ZoneId, blobId);
        }
        db.WriteGarbageBlob(blobId);
    }

    if (Blobs.IsGlobalZone(blobInfo.ZoneId)) {
        db.DeleteGlobalBlob(blobId);
    } else {
        db.DeleteZoneBlob(blobInfo.ZoneId, blobId);
    }
    db.DeleteBlockList(blobId);

    return true;
}

bool TPartitionState::FindBlockList(
    TPartitionDatabase& db,
    ui32 zoneHint,
    const TPartialBlobId& blobId,
    TMaybe<TBlockList>& blocks) const
{
    if (const auto* p = Blobs.FindBlockList(zoneHint, blobId)) {
        blocks = *p;
        return true;
    }

    if (db.ReadBlockList(blobId, blocks)) {
        Y_ABORT_UNLESS(blocks);

        bool added = Blobs.AddBlockList(
            zoneHint,
            blobId,
            *blocks,
            blocks->CountBlocks()
        );
        Y_ABORT_UNLESS(added);
        return true;
    }

    return false;
}

bool TPartitionState::UpdateIndexStructures(
    TPartitionDatabase& db,
    TInstant now,
    const TBlockRange32& blockRange,
    TVector<TBlockRange64>* convertedToMixedIndex,
    TVector<TBlockRange64>* convertedToRangeMap)
{
    const auto totalUsage = Blobs.UpdateAndGetTotalUsageScore(now);
    if (totalUsage == 0) {
        return true;
    }

    const auto avgUsage = totalUsage / Blobs.GetZoneCount();
    const auto hi = IndexCachingConfig.ConvertToMixedIndexFactor * avgUsage;
    const auto lo = IndexCachingConfig.ConvertToRangeMapFactor * avgUsage;

    const auto zoneRange = Blobs.ToZoneRange(blockRange);

    bool ready = true;

    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        const auto zoneUsage = Blobs.UpdateAndGetZoneUsageScore(z, now);

        // TODO: convert only if average blob density is low for this zone
        // there's no point in a hashtable if this zone contains big dense blobs
        if (zoneUsage >= hi && !Blobs.IsMixedIndex(z)) {
            Blobs.SetPruneBlockListCache(false);

            auto zoneBlobs = Blobs.FindZoneBlobs(z);

            for (const auto* blob: zoneBlobs) {
                // initializing zone block lists before index conversion
                TMaybe<TBlockList> blockList;
                if (!FindBlockList(db, z, blob->BlobId, blockList)) {
                    ready = false;
                }
            }

            if (ready) {
                if (convertedToMixedIndex) {
                    convertedToMixedIndex->push_back(
                        ConvertRangeSafe(Blobs.ToBlockRange(z))
                    );
                }
                Blobs.ConvertToMixedIndex(z, Checkpoints.GetCommitIds());
            }
        } else if (zoneUsage < lo && Blobs.IsMixedIndex(z)) {
            if (convertedToRangeMap) {
                convertedToRangeMap->push_back(
                    ConvertRangeSafe(Blobs.ToBlockRange(z))
                );
            }
            Blobs.ConvertToRangeMap(z);
        }
    }

    if (ready) {
        Blobs.SetPruneBlockListCache(true);
    }

    return ready;
}

void TPartitionState::UpdateIndex(
    const TVector<TPartitionDatabase::TBlobMeta>& blobs,
    const TVector<TBlobUpdate>& blobUpdates,
    const TVector<TPartitionDatabase::TBlobGarbage>& blobGarbage)
{
    UpdateIndex(GlobalZoneId, blobs, blobUpdates, blobGarbage);
}

void TPartitionState::UpdateIndex(
    ui32 z,
    const TVector<TPartitionDatabase::TBlobMeta>& blobs,
    const TVector<TBlobUpdate>& blobUpdates,
    const TVector<TPartitionDatabase::TBlobGarbage>& blobGarbage)
{
    for (const auto& item: blobs) {
        Y_ABORT_UNLESS(item.BlobMeta.StartIndicesSize()
            == item.BlobMeta.EndIndicesSize());

        TBlockRanges blockRanges;
        blockRanges.reserve(item.BlobMeta.StartIndicesSize());
        for (ui32 i = 0; i < item.BlobMeta.StartIndicesSize(); ++i) {
            blockRanges.push_back(TBlockRange32::MakeClosedInterval(
                item.BlobMeta.GetStartIndices(i),
                item.BlobMeta.GetEndIndices(i)));
        }

        auto addedBlobInfo = Blobs.AddBlob(
            item.BlobId,
            blockRanges,
            item.BlobMeta.GetBlockCount(),
            item.BlobMeta.GetCheckpointBlockCount()
        );

        Y_ABORT_UNLESS(addedBlobInfo.Added, "Duplicate blob detected: %s",
            DumpBlobIds(TabletId, item.BlobId).data());
    }

    const auto& checkpointIds = Checkpoints.GetCommitIds();
    for (const ui64 checkpointId: checkpointIds) {
        if (Blobs.IsGlobalZone(z)) {
            Blobs.OnCheckpoint(checkpointId);
        } else {
            Blobs.OnCheckpoint(z, checkpointId);
        }
    }

    for (const auto& item: blobUpdates) {
        Blobs.MarkBlocksDeleted(item);
        if (item.DeletionId > LastDeletionId) {
            LastDeletionId = item.DeletionId;
        }
    }

    for (const auto& item: blobGarbage) {
        bool added = Blobs.AddGarbage(z, item.BlobId, item.BlockCount);
        Y_ABORT_UNLESS(added, "Missing blob detected: %s",
            DumpBlobIds(TabletId, item.BlobId).data());
    }
}

bool TPartitionState::InitIndex(
    TPartitionDatabase& db,
    const TBlockRange32& blockRange)
{
    const auto zoneRange = Blobs.ToZoneRange(blockRange);
    bool ready = true;

    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        if (!Blobs.IsZoneInitialized(z)) {
            TVector<TPartitionDatabase::TBlobMeta> blobs;
            TVector<TBlobUpdate> blobUpdates;
            TVector<TPartitionDatabase::TBlobGarbage> blobGarbage;

            ready &= db.ReadZoneBlobs(z, blobs);
            ready &= db.ReadZoneBlobUpdates(z, blobUpdates);
            ready &= db.ReadZoneBlobGarbage(z, blobGarbage);

            if (!ready) {
                continue;
            }

            Blobs.InitializeZone(z);
            UpdateIndex(z, blobs, blobUpdates, blobGarbage);
        }
    }

    return ready;
}

bool TPartitionState::IsIndexInitialized(const TBlockRange32& blockRange)
{
    const auto zoneRange = Blobs.ToZoneRange(blockRange);

    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        if (!Blobs.IsZoneInitialized(z)) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Merged blocks

void TPartitionState::MarkMergedBlocksDeleted(
    TPartitionDatabase& db,
    const TBlockRange32& blockRange,
    ui64 commitId)
{
    auto deletionId = ++LastDeletionId;
    auto zoneId = Blobs.MarkBlocksDeleted({
        blockRange,
        commitId,
        deletionId
    });

    if (Blobs.IsGlobalZone(zoneId)) {
        db.WriteGlobalBlobUpdate(
            deletionId,
            commitId,
            blockRange
        );
    } else {
        db.WriteZoneBlobUpdate(
            zoneId,
            deletionId,
            commitId,
            blockRange
        );
    }
}

bool TPartitionState::FindMergedBlocks(
    TPartitionDatabase& db,
    ui64 commitId,
    const TBlockRange32& blockRange,
    IMergedBlockVisitor& visitor) const
{
    const auto deletedBlocks = Blobs.FindDeletedBlocks(blockRange, commitId);

    THashSet<TPartialBlobId, TPartialBlobIdHash> blobIds;
    bool ready = true;
    for (const auto* blob: Blobs.FindBlobs(blockRange)) {
        TMaybe<TBlockList> blockList;
        const bool found = FindBlockList(
            db,
            blockRange.Start / Config.GetZoneBlockCount(),
            blob->BlobId,
            blockList
        );

        if (!found) {
            ready = false;
            continue;
        }

        if (ready) {
            blobIds.insert(blob->BlobId);

            ui16 blobOffset = 0;
            for (auto& block: blockList->GetBlocks()) {
                if (blockRange.Contains(block.BlockIndex)) {
                    ApplyUpdate(deletedBlocks, block);

                    if (commitId == InvalidCommitId
                            || block.MinCommitId <= commitId
                            && block.MaxCommitId > commitId)
                    {
                        if (block.Zeroed) {
                            visitor.Visit(block, blob->BlobId, ZeroBlobOffset);
                        } else {
                            visitor.Visit(block, blob->BlobId, blobOffset);
                        }
                    }
                }

                if (!block.Zeroed) {
                    ++blobOffset;
                }
            }
        }
    }

    if (ready) {
        TVector<TBlockAndLocation> mixedBlocks;
        if (commitId == InvalidCommitId) {
            mixedBlocks = Blobs.FindAllMixedBlocks(blockRange);
        } else {
            mixedBlocks = Blobs.FindMixedBlocks(blockRange, commitId);
        }

        for (auto& x: mixedBlocks) {
            if (blobIds.contains(x.Location.BlobId)) {
                continue;
            }

            if (x.Block.MinCommitId == InvalidCommitId) {
                x.Block.MinCommitId = GetLastCommitId();
            }

            visitor.Visit(x.Block, x.Location.BlobId, x.Location.BlobOffset);
        }
    }

    return ready;
}

bool TPartitionState::FindMergedBlocks(
    TPartitionDatabase& db,
    const TBlockRange32& blockRange,
    IMergedBlockVisitor& visitor) const
{
    return FindMergedBlocks(db, InvalidCommitId, blockRange, visitor);
}

bool TPartitionState::FindMergedBlocks(
    TPartitionDatabase& db,
    const TGarbageInfo& garbageInfo,
    IMergedBlockVisitor& visitor) const
{
    bool ready = true;
    for (const auto& x: garbageInfo.BlobCounters) {
        const auto& blobId = x.first;
        TMaybe<TBlockList> blockList;
        if (!FindBlockList(db, garbageInfo.ZoneId, blobId, blockList)) {
            ready = false;
            continue;
        }

        if (ready) {
            ui16 blobOffset = 0;
            for (const auto& block: blockList->GetBlocks()) {
                if (block.Zeroed) {
                    visitor.Visit(block, blobId, ZeroBlobOffset);
                } else {
                    visitor.Visit(block, blobId, blobOffset);
                    ++blobOffset;
                }
            }
        }
    }

    return ready;
}

bool TPartitionState::ContainsMixedZones(const TBlockRange32& range) const
{
    auto zoneRange = Blobs.ToZoneRange(range);

    for (ui32 z = zoneRange.first; z <= zoneRange.second; ++z) {
        if (Blobs.IsMixedIndex(z)) {
            return true;
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoints

void TPartitionState::InitCheckpoints(
    const TVector<NProto::TCheckpointMeta>& checkpoints,
    const TVector<TVector<TPartialBlobId>>& deletedCheckpointBlobIds)
{
    auto it = deletedCheckpointBlobIds.begin();
    for (const auto& meta: checkpoints) {
        if (meta.GetDateDeleted()) {
            Y_ABORT_UNLESS(it != deletedCheckpointBlobIds.end());
            CheckpointsToDelete.Put(meta.GetCommitId(), *it);
            ++it;
        } else {
            bool added = Checkpoints.Add(meta);
            Y_ABORT_UNLESS(added, "Duplicate checkpoint detected: %s",
                meta.GetCheckpointId().Quote().data());
        }
    }

    Checkpoints.SetCheckpointBlockCount(Stats.GetCheckpointBlocksCount());
}

void TPartitionState::WriteCheckpoint(const NProto::TCheckpointMeta& meta)
{
    bool added = Checkpoints.Add(meta);
    Y_ABORT_UNLESS(added, "Duplicate checkpoint detected: %s",
        meta.GetCheckpointId().Quote().data());

    Blobs.OnCheckpoint(meta.GetCommitId());
}

bool TPartitionState::MarkCheckpointDeleted(
    TPartitionDatabase& db,
    TInstant now,
    ui64 commitId,
    const TString& checkpointId,
    TVector<TPartialBlobId> blobIds)
{
    if (auto* meta = Checkpoints.Find(checkpointId)) {
        auto newMeta = *meta;
        newMeta.SetDateDeleted(now.MicroSeconds());
        db.WriteCheckpoint(newMeta);

        Y_ABORT_UNLESS(Checkpoints.Remove(checkpointId));
        if (blobIds) {
            CheckpointsToDelete.Put(commitId, std::move(blobIds));
        }

        Blobs.OnCheckpointDeletion(commitId);

        return true;
    }

    return false;
}

void TPartitionState::SubtractCheckpointBlocks(ui32 erasedCheckpointBlocks)
{
    Checkpoints.SetCheckpointBlockCount(
        Checkpoints.GetCheckpointBlockCount() - erasedCheckpointBlocks
    );
}

////////////////////////////////////////////////////////////////////////////////
// Compaction

void TPartitionState::InitCompactionMap(
    const TVector<TCompactionCounter>& compactionMap)
{
    CompactionMap.Update(compactionMap, nullptr);
}

void TPartitionState::UpdateCompactionMap(
    TPartitionDatabase& db,
    const TVector<TBlock>& blocks)
{
    ui32 prevBlockIndex = 0;
    ui32 blockCount = 0;

    auto flush = [&] () {
        if (blockCount) {
            auto stat = CompactionMap.Get(prevBlockIndex);
            TCompactionMap::UpdateCompactionCounter(
                stat.BlobCount + 1,
                &stat.BlobCount
            );
            TCompactionMap::UpdateCompactionCounter(
                stat.BlockCount + blockCount,
                &stat.BlockCount
            );

            CompactionMap.Update(
                prevBlockIndex,
                stat.BlobCount,
                stat.BlockCount,
                Min(static_cast<ui32>(stat.BlockCount), GetMaxBlocksInBlob()),
                false
            );
            db.WriteCompactionMap(prevBlockIndex, stat.BlobCount, stat.BlockCount);
            blockCount = 0;
        }
    };

    for (size_t i = 0; i < blocks.size(); ++i) {
        ui32 blockIndex = CompactionMap.GetRangeStart(blocks[i].BlockIndex);
        Y_DEBUG_ABORT_UNLESS(prevBlockIndex <= blockIndex);

        if (i == 0 || prevBlockIndex != blockIndex) {
            flush();

            prevBlockIndex = blockIndex;
        }

        ++blockCount;
    }

    flush();
}

void TPartitionState::ResetCompactionMap(
    TPartitionDatabase& db,
    const TVector<TBlock>& blocks,
    const ui32 blobsSkipped,
    const ui32 blocksSkipped)
{
    ui32 prevBlockIndex = 0;
    ui32 blockCount = 0;

    auto flush = [&] () {
        if (blockCount) {
            CompactionMap.Update(
                prevBlockIndex,
                1 + blobsSkipped,
                blockCount + blocksSkipped,
                blockCount + blocksSkipped,
                true
            );
            db.WriteCompactionMap(
                prevBlockIndex,
                1 + blobsSkipped,
                blockCount + blocksSkipped
            );

            blockCount = 0;
        }
    };

    for (size_t i = 0; i < blocks.size(); ++i) {
        ui32 blockIndex = CompactionMap.GetRangeStart(blocks[i].BlockIndex);
        Y_DEBUG_ABORT_UNLESS(prevBlockIndex <= blockIndex);

        if (i == 0 || prevBlockIndex != blockIndex) {
            flush();

            prevBlockIndex = blockIndex;
        }

        ++blockCount;
    }

    flush();
}

////////////////////////////////////////////////////////////////////////////////
// Compaction

EOperationStatus TPartitionState::GetCompactionStatus(
    ECompactionType type) const
{
    const auto& state = type == ECompactionType::Forced ?
        ForcedCompactionState.State :
        CompactionState;
    return state.GetStatus();
}

void TPartitionState::SetCompactionStatus(
    ECompactionType type,
    EOperationStatus status)
{
    auto& state =  type == ECompactionType::Forced ?
        ForcedCompactionState.State :
        CompactionState;
    state.SetStatus(status);
}

////////////////////////////////////////////////////////////////////////////////
// Stats

void TPartitionState::WriteStats(TPartitionDatabase& db)
{
    Stats.SetFreshBlocksCount(GetFreshBlockCount());
    Stats.SetMixedBlobsCount(GetMixedBlobCount());
    Stats.SetMixedBlocksCount(GetMixedBlockCount());
    Stats.SetMergedBlobsCount(GetMergedBlobCount());
    Stats.SetMergedBlocksCount(GetMergedBlockCount());
    Stats.SetGarbageBlocksCount(GetGarbageBlockCount());
    Stats.SetUsedBlocksCount(GetUsedBlockCount());
    // TODO(NBS-2364): calculate logical used bytes count for partition2
    Stats.SetLogicalUsedBlocksCount(GetUsedBlockCount());
    Stats.SetCheckpointBlocksCount(Checkpoints.GetCheckpointBlockCount());

    db.WriteMeta(Meta);
}

void TPartitionState::DumpHtml(IOutputStream& out) const
{
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "LastCommitId"; }
                    TABLED() { DumpCommitId(out, GetLastCommitId()); }
                }
                TABLER() {
                    TABLED() { out << "FreshBlocks"; }
                    TABLED() {
                        out << "Total: " << GetFreshBlockCount()
                            << ", InFlight: " << GetFreshBlockCountInFlight()
                            << ", Queued: " << GetFreshBlocksQueued();
                    }
                }
                TABLER() {
                    TABLED() { out << "MergedBlocks"; }
                    TABLED() {
                        out << "Total: " << GetBlockCount()
                            << ", Stored: " << GetMergedBlockCount()
                            << ", Garbage: " << GetGarbageBlockCount();
                    }
                }
                TABLER() {
                    TABLED() { out << "Checkpoints"; }
                    TABLED() {
                        out << "Blocks: "
                            << Checkpoints.GetCheckpointBlockCount();
                    }
                }
                TABLER() {
                    TABLED() { out << "MergedBlobs"; }
                    TABLED() {
                        out << "GlobalBlobs: " << Blobs.GetGlobalBlobCount()
                            << ", ZoneBlobs: " << Blobs.GetZoneBlobCount()
                            << ", Ranges: " << Blobs.GetRangeCount()
                            << ", Lists: " << Blobs.GetBlockListCount();
                    }
                }
                TABLER() {
                    const auto rangeZoneCount =
                        Blobs.GetZoneCount() - Blobs.GetMixedZoneCount();
                    TABLED() { out << "Zones"; }
                    TABLED() {
                        out << "MixedZones: " << Blobs.GetMixedZoneCount()
                            << ", RangeZones: " << rangeZoneCount
                            << ", ZoneBlockCount: "
                            << Config.GetZoneBlockCount();
                    }
                }
                TABLER() {
                    TABLED() { out << "Flush"; }
                    TABLED() { FlushState.Dump(out); }
                }
                TABLER() {
                    TABLED() { out << "Compaction"; }
                    TABLED() { CompactionState.Dump(out); }
                }
                TABLER() {
                    TABLED() { out << "Cleanup"; }
                    TABLED() { CleanupState.Dump(out); }
                }
                TABLER() {
                    TABLED() { out << "Dirty Blobs (Zone)"; }
                    TABLED() {
                        out << PendingChunkToCleanup.size()
                            << " (" << PendingCleanupZoneId << ")";
                    }
                }
                TABLER() {
                    TABLED() { out << "Pending Updates"; }
                    TABLED() { out << Blobs.GetPendingUpdates(); }
                }
                TABLER() {
                    TABLED() { out << "CollectGarbage"; }
                    TABLED() { CollectGarbageState.Dump(out); }
                }
            }
        }
        TAG(TH3) { out << "Bytes Allocated"; }
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                ui64 allBytes = 0;
                for (ui32 i = 0; i < static_cast<ui32>(EAllocatorTag::Max); i++) {
                    EAllocatorTag tag = static_cast<EAllocatorTag>(i);
                    const auto& bytes =
                        GetAllocatorByTag(tag)->GetBytesAllocated();
                    TABLER() {
                        TABLED() { out << tag; }
                        TABLED() { out << FormatByteSize(bytes); }
                    }
                    allBytes += bytes;
                }
                TABLER() {
                    TABLED() { out << "Summary"; }
                    TABLED() { out << FormatByteSize(allBytes); }
                }
            }
        }
    }
}

TJsonValue TPartitionState::AsJson() const
{
    TJsonValue json;

    {
        TJsonValue state;
        state["LastCommitId"] = GetLastCommitId();
        state["FreshBlocksTotal"] = GetFreshBlockCount();
        state["FreshBlocksInFlight"] = GetFreshBlockCountInFlight();
        state["FreshBlocksQueued"] = GetFreshBlocksQueued();
        state["FlushState"] = ToJson(FlushState);
        state["Compaction"] = ToJson(CompactionState);
        state["Cleanup"] = ToJson(CleanupState);
        state["CollectGarbage"] = ToJson(CollectGarbageState);

        json["State"] = std::move(state);
    }
    json["Checkpoints"] = Checkpoints.AsJson();

    {
        TJsonValue stats;
        try {
            NProtobufJson::Proto2Json(Stats, stats);
            json["Stats"] = std::move(stats);
        } catch (...) {}
    }
    {
        TJsonValue config;
        try {
            NProtobufJson::Proto2Json(Config, config);
            json["Config"] = std::move(config);
        } catch (...) {}
    }

    return json;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
