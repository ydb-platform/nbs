#include "part_state.h"

#include <cloud/storage/core/libs/tablet/model/channels.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using TJsonValue = NJson::TJsonValue;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T SafeIncrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(counter < Max<T>() - value);
    return counter + value;
}

template <typename T>
T SafeDecrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(counter >= value);
    return counter - value;
}

TJsonValue ToJson(const TOperationState& op)
{
    TJsonValue json;
    json["Status"] = ToString(op.Status);
    const auto duration = TInstant::Now() - op.Timestamp;
    json["Duration"] = duration.MicroSeconds();
    return json;
}

void DumpOperationState(IOutputStream& out, const TOperationState& op)
{
    out << ToString(op.Status);

    if (op.Timestamp != TInstant::Zero()) {
        out << " for " << TInstant::Now() - op.Timestamp;
    }
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPartitionState::TPartitionState(
        NProto::TPartitionMeta meta,
        ui32 generation,
        ICompactionPolicyPtr compactionPolicy,
        ui32 compactionScoreHistorySize,
        ui32 cleanupScoreHistorySize,
        const TBackpressureFeaturesConfig& bpConfig,
        const TFreeSpaceConfig& freeSpaceConfig,
        ui32 maxIORequestsInFlight,
        ui32 reassignChannelsPercentageThreshold,
        ui32 reassignMixedChannelsPercentageThreshold,
        ui32 lastCommitId,
        ui32 channelCount,
        ui32 mixedIndexCacheSize,
        ui64 allocationUnit,
        ui32 maxBlobsPerUnit,
        ui32 maxBlobsPerRange,
        ui32 compactionRangeCountPerRun)
    : Meta(std::move(meta))
    , Generation(generation)
    , CompactionPolicy(compactionPolicy)
    , BPConfig(bpConfig)
    , FreeSpaceConfig(freeSpaceConfig)
    , Config(*Meta.MutableConfig())
    , ChannelCount(channelCount)
    , MaxIORequestsInFlight(maxIORequestsInFlight)
    , ReassignChannelsPercentageThreshold(reassignChannelsPercentageThreshold)
    , ReassignMixedChannelsPercentageThreshold(reassignMixedChannelsPercentageThreshold)
    , LastCommitId(lastCommitId)
    , MixedIndexCache(mixedIndexCacheSize, &MixedIndexCacheAllocator)
    , CompactionMap(GetMaxBlocksInBlob(), std::move(compactionPolicy))
    , CompactionScoreHistory(compactionScoreHistorySize)
    , UsedBlocks(Config.GetBlocksCount())
    , LogicalUsedBlocks(Config.GetBlocksCount())
    , MaxBlobsPerDisk(Max(Config.GetBlocksCount() * Config.GetBlockSize()
        / allocationUnit, 1ul) * maxBlobsPerUnit)
    , MaxBlobsPerRange(maxBlobsPerRange)
    , CompactionRangeCountPerRun(compactionRangeCountPerRun)
    , CleanupQueue(GetBlockSize())
    , CleanupScoreHistory(cleanupScoreHistorySize)
{
    InitChannels();
}

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

bool TPartitionState::CheckBlockRange(const TBlockRange64& range) const
{
    Y_DEBUG_ABORT_UNLESS(Config.GetBlocksCount() <= Max<ui32>());
    const auto validRange =
        TBlockRange64::WithLength(0, Config.GetBlocksCount());
    return validRange.Contains(range);
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

double TPartitionState::GetFreeSpaceShare(ui32 channel) const
{
    const auto* ch = GetChannel(channel);
    return ch ? ch->ApproximateFreeSpaceShare : 0;
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
            ReassignMixedChannelsPercentageThreshold * MixedChannels.size();
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

    const auto freshByteCount =
        GetUntrimmedFreshBlobByteCount() + GetUnflushedFreshBlobByteCount() +
        GetStats().GetFreshBlocksCount() * GetBlockSize();

    return {
        BPFeature(freshFeature, freshByteCount),
        CompactionPolicy->BackpressureEnabled()
            ? BPFeature(compactionFeature, GetLegacyCompactionScore())
            : 0,
        BackpressureDiskSpaceScore,
        Checkpoints.IsEmpty()
            ? BPFeature(cleanupFeature, CleanupQueue.GetQueueBytes())
            : 0,
    };
}

ui32 TPartitionState::GetAlmostFullChannelCount() const
{
    return AlmostFullChannelCount;
}

void TPartitionState::EnqueueIORequest(
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

std::optional<TQueuedRequest> TPartitionState::DequeueIORequest(ui32 channel)
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

    ui32 bestChannel = Max<ui32>();
    double bestSpaceShare = 0;
    for (ui32 i = 0; i < channels.size(); ++i) {
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
        0); // partId - should always be zero
}

ui64 TPartitionState::GetCleanupCommitId() const
{
    ui64 commitId = GetLastCommitId();

    // should not cleanup after any barrier
    commitId = Min(commitId, CleanupQueue.GetMinCommitId() - 1);

    // should not cleanup after any checkpoint
    commitId = Min(commitId, Checkpoints.GetMinCommitId() - 1);

    return commitId;
}

ui64 TPartitionState::CalculateCheckpointBytes() const
{
    const auto* lastCheckpoint = Checkpoints.GetLast();
    if (!lastCheckpoint) {
        return 0;
    }

    const auto& lastStats = lastCheckpoint->Stats;
    ui64 blocksCount = GetUnflushedFreshBlocksCount();
    blocksCount += lastStats.GetMixedBlocksCount();
    blocksCount += lastStats.GetMergedBlocksCount();
    return blocksCount * GetBlockSize();
}

ui64 TPartitionState::GetCollectCommitId() const
{
    ui64 commitId = GetLastCommitId();

    // should not collect after any barrier
    commitId = Min(commitId, GarbageQueue.GetMinCommitId() - 1);

    return commitId;
}

bool TPartitionState::OverlapsUnconfirmedBlobs(
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange) const
{
    return Overlaps(UnconfirmedBlobs, lowCommitId, highCommitId, blockRange);
}

bool TPartitionState::OverlapsConfirmedBlobs(
    ui64 lowCommitId,
    ui64 highCommitId,
    const TBlockRange32& blockRange) const
{
    return Overlaps(ConfirmedBlobs, lowCommitId, highCommitId, blockRange);
}

void TPartitionState::InitUnconfirmedBlobs(
    TCommitIdToBlobsToConfirm blobs)
{
    UnconfirmedBlobs = std::move(blobs);
    UnconfirmedBlobCount = 0;

    for (const auto& [commitId, blobs]: UnconfirmedBlobs) {
        UnconfirmedBlobCount += blobs.size();
        CommitQueue.AcquireBarrier(commitId);
        GarbageQueue.AcquireBarrier(commitId);
    }
}

void TPartitionState::WriteUnconfirmedBlob(
    TPartitionDatabase& db,
    ui64 commitId,
    const TBlobToConfirm& blob)
{
    auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
    db.WriteUnconfirmedBlob(blobId, blob);
    UnconfirmedBlobs[commitId].push_back(blob);
    UnconfirmedBlobCount++;
}

void TPartitionState::ConfirmedBlobsAdded(
    TPartitionDatabase& db,
    ui64 commitId)
{
    auto it = ConfirmedBlobs.find(commitId);
    if (it == ConfirmedBlobs.end()) {
        return;
    }

    auto& blobs = it->second;
    const auto blobCount = blobs.size();

    for (const auto& blob: blobs) {
        auto blobId = MakePartialBlobId(commitId, blob.UniqueId);
        db.DeleteUnconfirmedBlob(blobId);
    }

    ConfirmedBlobs.erase(it);
    Y_DEBUG_ABORT_UNLESS(ConfirmedBlobCount >= blobCount);
    ConfirmedBlobCount -= blobCount;

    GarbageQueue.ReleaseBarrier(commitId);
    CommitQueue.ReleaseBarrier(commitId);
}

void TPartitionState::BlobsConfirmed(
    ui64 commitId,
    TVector<TBlobToConfirm> blobs)
{
    auto it = UnconfirmedBlobs.find(commitId);
    Y_DEBUG_ABORT_UNLESS(it != UnconfirmedBlobs.end());

    auto& dstBlobs = it->second;
    const auto blobCount = dstBlobs.size();
    Y_DEBUG_ABORT_UNLESS(blobs.empty() || blobCount == blobs.size());
    for (ui32 i = 0; i < Min(blobCount, blobs.size()); ++i) {
        const auto blockRange = dstBlobs[i].BlockRange;
        Y_DEBUG_ABORT_UNLESS(dstBlobs[i].UniqueId == blobs[i].UniqueId);
        Y_DEBUG_ABORT_UNLESS(blockRange.Start == blobs[i].BlockRange.Start);
        Y_DEBUG_ABORT_UNLESS(blockRange.End == blobs[i].BlockRange.End);
        Y_DEBUG_ABORT_UNLESS(blockRange.Size() == blobs[i].Checksums.size());
        if (dstBlobs[i].UniqueId == blobs[i].UniqueId) {
            dstBlobs[i].Checksums = std::move(blobs[i]).Checksums;
        }
    }

    ConfirmedBlobs[commitId] = std::move(dstBlobs);
    ConfirmedBlobCount += blobCount;

    UnconfirmedBlobs.erase(it);
    Y_DEBUG_ABORT_UNLESS(UnconfirmedBlobCount >= blobCount);
    UnconfirmedBlobCount -= blobCount;
}

void TPartitionState::ConfirmBlobs(
    TPartitionDatabase& db,
    const TVector<TPartialBlobId>& unrecoverableBlobs)
{
    for (auto blobId: unrecoverableBlobs) {
        db.DeleteUnconfirmedBlob(blobId);

        auto& blobs = UnconfirmedBlobs[blobId.CommitId()];
        EraseIf(blobs, [&] (const auto& blob) {
            return blob.UniqueId == blobId.UniqueId();
        });

        if (blobs.empty()) {
            UnconfirmedBlobs.erase(blobId.CommitId());

            GarbageQueue.ReleaseBarrier(blobId.CommitId());
            CommitQueue.ReleaseBarrier(blobId.CommitId());
        }
    }

    ConfirmedBlobs = std::move(UnconfirmedBlobs);
    ConfirmedBlobCount = UnconfirmedBlobCount;

    UnconfirmedBlobs.clear();
    UnconfirmedBlobCount = 0;
}

#define BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER(name)                           \
    ui64 TPartitionState::Increment##name(size_t value)                        \
    {                                                                          \
        auto& stats = AccessStats();                                           \
        ui64 counter = SafeIncrement(stats.Get##name(), value);                \
        stats.Set##name(counter);                                              \
        return counter;                                                        \
    }                                                                          \
                                                                               \
    ui64 TPartitionState::Decrement##name(size_t value)                        \
    {                                                                          \
        auto& stats = AccessStats();                                           \
        ui64 counter = SafeDecrement(stats.Get##name(), value);                \
        stats.Set##name(counter);                                              \
        return counter;                                                        \
    }                                                                          \
// BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER

BLOCKSTORE_PARTITION_PROTO_COUNTERS(BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER)

#undef BLOCKSTORE_PARTITION_IMPLEMENT_COUNTER

void TPartitionState::AddFreshBlob(TFreshBlobMeta freshBlobMeta)
{
    Y_ABORT_UNLESS(freshBlobMeta.CommitId > LastTrimFreshLogToCommitId);
    const bool inserted = UntrimmedFreshBlobs.insert(freshBlobMeta).second;
    Y_ABORT_UNLESS(inserted);
    UntrimmedFreshBlobByteCount += freshBlobMeta.BlobSize;
}

void TPartitionState::TrimFreshBlobs(ui64 commitId)
{
    auto& blobs = UntrimmedFreshBlobs;

    while (blobs && blobs.begin()->CommitId <= commitId)
    {
        Y_ABORT_UNLESS(UntrimmedFreshBlobByteCount >= blobs.begin()->BlobSize);
        UntrimmedFreshBlobByteCount -= blobs.begin()->BlobSize;
        blobs.erase(blobs.begin());
    }
}

ui32 TPartitionState::IncrementUnflushedFreshBlocksFromDbCount(size_t value)
{
    auto& stats = AccessStats();
    ui64 counter = SafeIncrement(stats.GetFreshBlocksCount(), value);
    stats.SetFreshBlocksCount(counter);
    return counter;
}

ui32 TPartitionState::DecrementUnflushedFreshBlocksFromDbCount(size_t value)
{
    auto& stats = AccessStats();
    ui64 counter = SafeDecrement(stats.GetFreshBlocksCount(), value);
    stats.SetFreshBlocksCount(counter);
    return counter;
}

ui32 TPartitionState::IncrementUnflushedFreshBlocksFromChannelCount(size_t value)
{
    UnflushedFreshBlocksFromChannelCount = SafeIncrement(
        UnflushedFreshBlocksFromChannelCount,
        value);

    return UnflushedFreshBlocksFromChannelCount;
}

ui32 TPartitionState::DecrementUnflushedFreshBlocksFromChannelCount(size_t value)
{
    UnflushedFreshBlocksFromChannelCount = SafeDecrement(
        UnflushedFreshBlocksFromChannelCount,
        value);

    return UnflushedFreshBlocksFromChannelCount;
}

ui32 TPartitionState::IncrementFreshBlocksInFlight(size_t value)
{
    FreshBlocksInFlight = SafeIncrement(FreshBlocksInFlight, value);
    return FreshBlocksInFlight;
}

ui32 TPartitionState::DecrementFreshBlocksInFlight(size_t value)
{
    FreshBlocksInFlight = SafeDecrement(FreshBlocksInFlight, value);
    return FreshBlocksInFlight;
}

void TPartitionState::InitFreshBlocks(const TVector<TOwningFreshBlock>& freshBlocks)
{
    for (const auto& freshBlock: freshBlocks) {
        const auto& meta = freshBlock.Meta;

        bool added = Blocks.AddBlock(
            meta.BlockIndex,
            meta.CommitId,
            meta.IsStoredInDb,
            freshBlock.Content);

        Y_ABORT_UNLESS(added, "Duplicate block detected: %u @%lu",
            meta.BlockIndex,
            meta.CommitId);
    }
}

void TPartitionState::FindFreshBlocks(
    IFreshBlocksIndexVisitor& visitor,
    const TBlockRange32& readRange,
    ui64 maxCommitId)
{
    Blocks.FindBlocks(visitor, readRange, maxCommitId);
}

void TPartitionState::WriteFreshBlocks(
    TPartitionDatabase& db,
    const TBlockRange32& writeRange,
    ui64 commitId,
    TSgList sglist)
{
    Y_ABORT_UNLESS(writeRange.Size() == sglist.size());

    WriteFreshBlocksImpl(
        db,
        writeRange,
        commitId,
        [&](ui32 index) { return sglist[index]; }
    );
}

void TPartitionState::WriteFreshBlocks(
    const TBlockRange32& writeRange,
    ui64 commitId,
    TSgList sglist)
{
    Y_ABORT_UNLESS(writeRange.Size() == sglist.size());

    WriteFreshBlocksImpl(
        writeRange,
        commitId,
        [&](ui32 index) { return sglist[index]; }
    );
}


void TPartitionState::ZeroFreshBlocks(
    TPartitionDatabase& db,
    const TBlockRange32& zeroRange,
    ui64 commitId)
{
    WriteFreshBlocksImpl(
        db,
        zeroRange,
        commitId,
        [](ui32) { return TBlockDataRef(); }
    );
}

void TPartitionState::ZeroFreshBlocks(
    const TBlockRange32& zeroRange,
    ui64 commitId)
{
    WriteFreshBlocksImpl(
        zeroRange,
        commitId,
        [](ui32) { return TBlockDataRef(); }
    );
}

void TPartitionState::DeleteFreshBlock(
    TPartitionDatabase& db,
    ui32 blockIndex,
    ui64 commitId)
{
    bool removed = Blocks.RemoveBlock(
        blockIndex,
        commitId,
        true);  // isStoredInDb

    Y_ABORT_UNLESS(removed);

    db.DeleteFreshBlock(blockIndex, commitId);
    DecrementUnflushedFreshBlocksFromDbCount(1);
}

void TPartitionState::DeleteFreshBlock(
    ui32 blockIndex,
    ui64 commitId)
{
    bool removed = Blocks.RemoveBlock(
        blockIndex,
        commitId,
        false);  // isStoredInDb

    Y_ABORT_UNLESS(removed);

    DecrementUnflushedFreshBlocksFromChannelCount(1);
}

////////////////////////////////////////////////////////////////////////////////
// Mixed blocks

void TPartitionState::WriteMixedBlock(
    TPartitionDatabase& db,
    TMixedBlock block)
{
    const ui32 rangeIdx = CompactionMap.GetRangeIndex(block.BlockIndex);
    MixedIndexCache.InsertBlockIfHot(rangeIdx, block);
    db.WriteMixedBlock(block);
}

void TPartitionState::WriteMixedBlocks(
    TPartitionDatabase& db,
    const TPartialBlobId& blobId,
    const TVector<ui32>& blockIndices)
{
    const ui64 commitId = blobId.CommitId();
    ui16 blobOffset = 0;

    for (const ui32 blockIndex: blockIndices) {
        const ui32 rangeIdx = CompactionMap.GetRangeIndex(blockIndex);
        MixedIndexCache.InsertBlockIfHot(rangeIdx, {
            blobId,
            commitId,
            blockIndex,
            blobOffset
        });
        ++blobOffset;
    }

    db.WriteMixedBlocks(blobId, blockIndices);
}

void TPartitionState::DeleteMixedBlock(
    TPartitionDatabase& db,
    ui32 blockIndex,
    ui64 commitId)
{
    const ui32 rangeIdx = CompactionMap.GetRangeIndex(blockIndex);
    MixedIndexCache.EraseBlockIfHot(rangeIdx, { blockIndex, commitId });
    db.DeleteMixedBlock(blockIndex, commitId);
}

bool TPartitionState::FindMixedBlocksForCompaction(
    TPartitionDatabase& db,
    IBlocksIndexVisitor& visitor,
    ui32 rangeIdx)
{
    if (MixedIndexCache.VisitBlocksIfHot(rangeIdx, visitor)) {
        // Compaction range is hot: no need to query db.
        return true;
    }

    auto cacheInserter = MixedIndexCache.GetInserterForRange(rangeIdx);

    struct TVisitorAndCacheInserter final
        : public IBlocksIndexVisitor
    {
        IBlocksIndexVisitor& Visitor;
        TMixedIndexCache::TInserterPtr CacheInserter;

        TVisitorAndCacheInserter(
                IBlocksIndexVisitor& visitor,
                TMixedIndexCache::TInserterPtr cacheInserter)
            : Visitor(visitor)
            , CacheInserter(std::move(cacheInserter))
        {}

        bool Visit(
            ui32 blockIndex,
            ui64 commitId,
            const TPartialBlobId& blobId,
            ui16 blobOffset) override
        {
            bool ok = Visitor.Visit(blockIndex, commitId, blobId, blobOffset);
            Y_ABORT_UNLESS(ok);

            CacheInserter->Insert({blobId, commitId, blockIndex, blobOffset});
            return true;
        }

    } visitorAndCacheInserter(visitor, std::move(cacheInserter));

    return db.FindMixedBlocks(
        visitorAndCacheInserter,
        CompactionMap.GetBlockRange(rangeIdx),
        true);  // precharge
}

void TPartitionState::RaiseRangeTemperature(ui32 rangeIdx)
{
    MixedIndexCache.RaiseRangeTemperature(rangeIdx);
}

ui64 TPartitionState::GetMixedIndexCacheMemSize() const
{
    return MixedIndexCacheAllocator.GetBytesAllocated();
}

////////////////////////////////////////////////////////////////////////////////
// Compaction

TOperationState& TPartitionState::GetCompactionState(ECompactionType type)
{
    return type == ECompactionType::Forced ?
        ForcedCompactionState.State :
        CompactionState;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionState::SetUsedBlocks(
    TPartitionDatabase& db,
    const TBlockRange32& range,
    ui32 skipCount)
{
    auto blockCount = GetUsedBlocks().Set(range.Start, range.End + 1) - skipCount;
    ui32 logicalBlockCount = 0;

    if (GetBaseDiskId()) {
        logicalBlockCount = GetLogicalUsedBlocks().Set(range.Start, range.End + 1) - skipCount;
    } else {
        logicalBlockCount = blockCount;
    }

    IncrementUsedBlocksCount(blockCount);
    IncrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        WriteUsedBlocksToDB(db, range.Start, range.End + 1);
    }
}

void TPartitionState::SetUsedBlocks(
    TPartitionDatabase& db,
    const TVector<ui32>& blocks)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(blocks.begin(), blocks.end()));

    ui32 blockCount = 0;
    ui32 logicalBlockCount = 0;

    for (const ui32 b: blocks) {
        ui64 count = GetUsedBlocks().Set(b, b + 1);

        blockCount += count;

        if (GetBaseDiskId()) {
            logicalBlockCount += GetLogicalUsedBlocks().Set(b, b + 1);
        } else {
            logicalBlockCount += count;
        }
    }

    IncrementUsedBlocksCount(blockCount);
    IncrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        auto first = blocks.begin();
        auto last = std::next(first);

        while (last != blocks.end()) {
            if (*last != *std::prev(last) + 1) {
                WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
                first = last;
            }
            ++last;
        }

        if (first != blocks.end()) {
            WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
        }
    }
}

void TPartitionState::UnsetUsedBlocks(
    TPartitionDatabase& db,
    const TBlockRange32& range)
{
    ui32 blockCount = GetUsedBlocks().Unset(range.Start, range.End + 1);
    ui32 logicalBlockCount = 0;

    if (GetBaseDiskId()) {
        logicalBlockCount = GetLogicalUsedBlocks().Unset(range.Start, range.End + 1);
    } else {
        logicalBlockCount = blockCount;
    }

    DecrementUsedBlocksCount(blockCount);
    DecrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        WriteUsedBlocksToDB(db, range.Start, range.End + 1);
    }
}

void TPartitionState::UnsetUsedBlocks(
    TPartitionDatabase& db,
    const TVector<ui32>& blocks)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(blocks.begin(), blocks.end()));

    ui32 blockCount = 0;
    ui32 logicalBlockCount = 0;

    for (const ui32 b: blocks) {
        ui64 count = GetUsedBlocks().Unset(b, b + 1);

        blockCount += count;

        if (GetBaseDiskId()) {
            logicalBlockCount += GetLogicalUsedBlocks().Unset(b, b + 1);
        } else {
            logicalBlockCount += count;
        }
    }

    DecrementUsedBlocksCount(blockCount);
    DecrementLogicalUsedBlocksCount(logicalBlockCount);

    if (blockCount || logicalBlockCount) {
        auto first = blocks.begin();
        auto last = std::next(first);

        while (last != blocks.end()) {
            if (*last != *std::prev(last) + 1) {
                WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
                first = last;
            }
            ++last;
        }

        if (first != blocks.end()) {
            WriteUsedBlocksToDB(db, *first, *std::prev(last) + 1);
        }
    }
}

void TPartitionState::WriteUsedBlocksToDB(
    TPartitionDatabase& db,
    ui32 begin,
    ui32 end)
{
    auto serializer = GetUsedBlocks().RangeSerializer(begin, end);
    TCompressedBitmap::TSerializedChunk sc;
    while (serializer.Next(&sc)) {
        db.WriteUsedBlocks(sc);
    }

    if (GetBaseDiskId()) {
        auto serializerLogical = GetLogicalUsedBlocks().RangeSerializer(begin, end);
        while (serializerLogical.Next(&sc)) {
            db.WriteLogicalUsedBlocks(sc);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionState::IncrementUnflushedFreshBlobCount(ui32 value)
{
    UnflushedFreshBlobCount = SafeIncrement(UnflushedFreshBlobCount, value);
}

void TPartitionState::DecrementUnflushedFreshBlobCount(ui32 value)
{
    UnflushedFreshBlobCount = SafeDecrement(UnflushedFreshBlobCount, value);
}

void TPartitionState::IncrementUnflushedFreshBlobByteCount(ui64 value)
{
    UnflushedFreshBlobByteCount = SafeIncrement(UnflushedFreshBlobByteCount, value);
}

void TPartitionState::DecrementUnflushedFreshBlobByteCount(ui64 value)
{
    UnflushedFreshBlobByteCount = SafeDecrement(UnflushedFreshBlobByteCount, value);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionState::RegisterDowntime(TInstant now, ui32 groupId)
{
    GroupId2Downtimes[groupId].PushBack(now, EDowntimeStateChange::DOWN);
}

void TPartitionState::RegisterSuccess(TInstant now, ui32 groupId)
{
    auto it = GroupId2Downtimes.find(groupId);
    if (it != GroupId2Downtimes.end()) {
        it->second.PushBack(now, EDowntimeStateChange::UP);
        if (!it->second.HasRecentState(now, EDowntimeStateChange::DOWN)) {
            GroupId2Downtimes.erase(it);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionState::DumpHtml(IOutputStream& out) const
{
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "LastCommitId"; }
                    TABLED() { out << GetLastCommitId(); }
                }
                TABLER() {
                    TABLED() { out << "FreshBlocks"; }
                    TABLED() {
                        out << "Total: " << GetUnflushedFreshBlocksCount()
                            << ", FromDb: " << GetStats().GetFreshBlocksCount()
                            << ", FromChannel: "
                            << UnflushedFreshBlocksFromChannelCount
                            << ", InFlight: " << GetFreshBlocksInFlight()
                            << ", Queued: " << GetFreshBlocksQueued()
                            << ", UntrimmedBytes: "
                            << GetUntrimmedFreshBlobByteCount();
                    }
                }
                TABLER() {
                    TABLED() { out << "Flush"; }
                    TABLED() { DumpOperationState(out, FlushState); }
                }
                TABLER() {
                    TABLED() { out << "Compaction"; }
                    TABLED() { DumpOperationState(out, CompactionState); }
                }
                TABLER() {
                    TABLED() { out << "CompactionDelay"; }
                    TABLED() { out << CompactionDelay; }
                }
                TABLER() {
                    TABLED() { out << "Cleanup"; }
                    TABLED() { DumpOperationState(out, CleanupState); }
                }
                TABLER() {
                    TABLED() { out << "CleanupDelay"; }
                    TABLED() { out << CleanupDelay; }
                }
                TABLER() {
                    TABLED() { out << "CollectGarbage"; }
                    TABLED() { DumpOperationState(out, CollectGarbageState); }
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
        state["FreshBlocksTotal"] = GetUnflushedFreshBlocksCount();
        state["FreshBlocksFromDb"] = GetStats().GetFreshBlocksCount();
        state["FreshBlocksFromChannel"] = UnflushedFreshBlocksFromChannelCount;
        state["FreshBlocksInFlight"] = GetFreshBlocksInFlight();
        state["FreshBlocksQueued"] = GetFreshBlocksQueued();
        state["FreshBlobUntrimmedBytes"] = GetUntrimmedFreshBlobByteCount();
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
            NProtobufJson::Proto2Json(GetStats(), stats);
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

}   // namespace NCloud::NBlockStore::NStorage::NPartition
