#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TTabletStorageInfo SerializeTabletStorageInfo(const auto& info)
{
    NProto::TTabletStorageInfo proto;

    proto.SetTabletId(info.TabletID);
    proto.SetVersion(info.Version);

    for (const auto& srcChannel: info.Channels) {
        auto& dstChannel = *proto.MutableChannels()->Add();
        dstChannel.SetStoragePool(srcChannel.StoragePool);

        for (const auto& srcEntry: srcChannel.History) {
            auto& dstEntry = *dstChannel.MutableHistory()->Add();
            dstEntry.SetFromGeneration(srcEntry.FromGeneration);
            dstEntry.SetGroupId(srcEntry.GroupID);
        }
    }

    return proto;
}

NProto::TError ValidateTabletStorageInfoUpdate(
    const TString& LogTag,
    const NProto::TTabletStorageInfo& oldInfo,
    const NProto::TTabletStorageInfo& newInfo)
{
    const ui32 oldInfoVersion = oldInfo.GetVersion();
    const ui32 newInfoVersion = newInfo.GetVersion();

    if (oldInfoVersion > newInfoVersion) {
        return MakeError(E_FAIL, TStringBuilder()
            << "version mismatch (old: " << oldInfoVersion
            << ", new: " << newInfoVersion << ")");
    }

    if (oldInfoVersion == newInfoVersion) {
        google::protobuf::util::MessageDifferencer differencer;

        TString diff;
        differencer.ReportDifferencesToString(&diff);
        if (differencer.Compare(oldInfo, newInfo)) {
            return MakeError(S_ALREADY, "nothing to update");
        }

        return MakeError(E_FAIL, TStringBuilder()
            << "content has changed without version increment, diff: " << diff);
    }

    TABLET_VERIFY_C(oldInfoVersion < newInfoVersion,
        TStringBuilder() << "config version mismatch: old "
            << oldInfoVersion << " , new: " << newInfoVersion);

    const ui32 oldChannelCount = oldInfo.ChannelsSize();
    const ui32 newChannelCount = newInfo.ChannelsSize();;

    if (oldChannelCount > newChannelCount) {
        return MakeError(E_FAIL, TStringBuilder()
            << "channel count has been decreased (old: " << oldChannelCount
            << ", new: " << newChannelCount << ")");
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_LoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TLoadState& args)
{
    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading tablet state data");

    TIndexTabletDatabase db(tx.DB);

    std::initializer_list<bool> results = {
        db.ReadFileSystem(args.FileSystem),
        db.ReadFileSystemStats(args.FileSystemStats),
        db.ReadTabletStorageInfo(args.TabletStorageInfo),
        db.ReadNode(RootNodeId, 0, args.RootNode),
        db.ReadSessions(args.Sessions),
        db.ReadSessionHandles(args.Handles),
        db.ReadSessionLocks(args.Locks),
        db.ReadSessionDupCacheEntries(args.DupCache),
        db.ReadFreshBytes(args.FreshBytes),
        db.ReadFreshBlocks(args.FreshBlocks),
        db.ReadNewBlobs(args.NewBlobs),
        db.ReadGarbageBlobs(args.GarbageBlobs),
        db.ReadCheckpoints(args.Checkpoints),
        db.ReadTruncateQueue(args.TruncateQueue),
        db.ReadStorageConfig(args.StorageConfig),
        db.ReadSessionHistoryEntries(args.SessionHistory)
    };

    bool ready = std::accumulate(
        results.begin(),
        results.end(),
        true,
        std::logical_and<>()
    );

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading tablet state data "
            << (ready ? "finished" : "restarted"));

    return ready;
}

void TIndexTabletActor::ExecuteTx_LoadState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TLoadState& args)
{
    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Preparing tablet state");

    TIndexTabletDatabase db(tx.DB);

    if (!args.RootNode) {
        args.RootNode.ConstructInPlace();
        args.RootNode->Attrs = CreateDirectoryAttrs(0777, 0, 0);
        db.WriteNode(RootNodeId, 0, args.RootNode->Attrs);
    }

    const auto& oldTabletStorageInfo = args.TabletStorageInfo;
    const auto newTabletStorageInfo = SerializeTabletStorageInfo(*Info());

    if (!oldTabletStorageInfo.GetTabletId()) {
        // First TxLoadState on tablet creation
        LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
            LogTag << " Initializing tablet storage info");

        TABLET_VERIFY(newTabletStorageInfo.GetTabletId());
        args.TabletStorageInfo.CopyFrom(newTabletStorageInfo);
        db.WriteTabletStorageInfo(newTabletStorageInfo);
        return;
    }

    const auto error = ValidateTabletStorageInfoUpdate(
        LogTag,
        oldTabletStorageInfo,
        newTabletStorageInfo);

    if (HasError(error)) {
        ReportInvalidTabletStorageInfo();
        args.Error = error;
        return;
    }

    if (error.GetCode() != S_ALREADY) {
        LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
            LogTag << " Updating tablet storage info");

        args.TabletStorageInfo.CopyFrom(newTabletStorageInfo);
        db.WriteTabletStorageInfo(newTabletStorageInfo);
    }

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Completed preparing tablet state");
}

void TIndexTabletActor::CompleteTx_LoadState(
    const TActorContext& ctx,
    TTxIndexTablet::TLoadState& args)
{
    if (args.StorageConfig.Defined()) {
        Config = std::make_shared<TStorageConfig>(*Config);
        Config->Merge(*args.StorageConfig.Get());
        LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
            LogTag << " Merge StorageConfig with config from tablet database");
    }

    if (HasError(args.Error)) {
        LOG_ERROR_S(ctx, TFileStoreComponents::TABLET,
            LogTag
            << "Switching tablet to BROKEN state due to the failed TxLoadState: "
            << FormatError(args.Error));

        BecomeAux(ctx, STATE_BROKEN);

        // allow pipes to connect
        SignalTabletActive(ctx);

        // resend pending WaitReady requests
        while (WaitReadyRequests) {
            ctx.Send(WaitReadyRequests.front().release());
            WaitReadyRequests.pop_front();
        }

        return;
    }

    BecomeAux(ctx, STATE_WORK);
    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Activating tablet");

    // allow pipes to connect
    SignalTabletActive(ctx);

    // resend pending WaitReady requests
    while (WaitReadyRequests) {
        ctx.Send(WaitReadyRequests.front().release());
        WaitReadyRequests.pop_front();
    }

    TThrottlerConfig config;
    Convert(args.FileSystem.GetPerformanceProfile(), config);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Initializing tablet state");

    LoadState(
        Executor()->Generation(),
        *Config,
        args.FileSystem,
        args.FileSystemStats,
        args.TabletStorageInfo,
        config);
    UpdateLogTag();

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading tablet sessions");
    auto idleSessionDeadline = ctx.Now() + Config->GetIdleSessionTimeout();
    LoadSessions(
        idleSessionDeadline,
        args.Sessions,
        args.Handles,
        args.Locks,
        args.DupCache,
        args.SessionHistory);

    if (!Config->GetEnableCollectGarbageAtStart()) {
        SetStartupGcExecuted();
    }

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Enqueueing truncate operations: "
            << args.TruncateQueue.size());
    for (const auto& entry: args.TruncateQueue) {
        EnqueueTruncateOp(
            entry.GetNodeId(),
            TByteRange(entry.GetOffset(), entry.GetLength(), GetBlockSize()));
    }

    // checkpoints should be loaded before data
    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading tablet checkpoints: "
            << args.Checkpoints.size());
    LoadCheckpoints(args.Checkpoints);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading fresh bytes: "
            << args.FreshBytes.size());
    LoadFreshBytes(args.FreshBytes);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading fresh blocks: "
            << args.FreshBlocks.size());
    LoadFreshBlocks(args.FreshBlocks);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading garbage blobs: "
            << args.GarbageBlobs.size());
    LoadGarbage(args.NewBlobs, args.GarbageBlobs);

    const ui32 loadedRangesPerIteration = 1024 * 1024;
    LoadCompactionMapQueue.push_back({
        0,
        loadedRangesPerIteration,
        false});
    LoadNextCompactionMapChunkIfNeeded(ctx);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Scheduling startup events");
    ScheduleCleanupSessions(ctx);
    RestartCheckpointDestruction(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);
    EnqueueTruncateIfNeeded(ctx);

    RegisterFileStore(ctx);
    RegisterStatCounters();
    ResetThrottlingPolicy();

    CompleteStateLoad();

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Load state completed");
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleLoadCompactionMapChunk(
    const TEvIndexTabletPrivate::TEvLoadCompactionMapChunkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s LoadCompactionMapChunk started (first range: #%u, count: %u)",
        LogTag.c_str(),
        msg->FirstRangeId,
        msg->RangeCount);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    Y_UNUSED(requestInfo);

    ExecuteTx<TLoadCompactionMapChunk>(
        ctx,
        std::move(requestInfo),
        msg->FirstRangeId,
        msg->RangeCount);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::LoadNextCompactionMapChunkIfNeeded(
    const TActorContext& ctx)
{
    if (!LoadCompactionMapChunkInProgress && LoadCompactionMapQueue) {
        ctx.Send(
            SelfId(),
            new TEvIndexTabletPrivate::TEvLoadCompactionMapChunkRequest(
                LoadCompactionMapQueue.front())
        );

        LoadCompactionMapChunkInProgress = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleLoadCompactionMapChunkCompleted(
    const TEvIndexTabletPrivate::TEvLoadCompactionMapChunkCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s LoadCompactionMapChunk completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    TABLET_VERIFY(!!LoadCompactionMapQueue);
    auto req = LoadCompactionMapQueue.front();

    if (req.OutOfOrder) {
        LoadedOutOfOrderRangeIds.insert(msg->FirstRangeId);
    } else {
        MaxLoadedInOrderRangeId =
            Max(MaxLoadedInOrderRangeId, msg->LastRangeId);

        if (msg->LastRangeId == 0) {
            CompactionStateLoaded = true;

            EnqueueFlushIfNeeded(ctx);
            EnqueueBlobIndexOpIfNeeded(ctx);

            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s Compaction state loaded, MaxLoadedInOrderRangeId: %u",
                LogTag.c_str(),
                MaxLoadedInOrderRangeId);
        } else {
            const ui32 loadedRangesPerIteration = 1024 * 1024;
            LoadCompactionMapQueue.push_back({
                msg->LastRangeId + 1,
                loadedRangesPerIteration,
                false});

            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s Compaction map chunk loaded, LastRangeId: %u",
                LogTag.c_str(),
                msg->LastRangeId);
        }
    }

    LoadCompactionMapQueue.pop_front();
    LoadCompactionMapChunkInProgress = false;

    if (LoadCompactionMapQueue) {
        ctx.Send(
            SelfId(),
            new TEvIndexTabletPrivate::TEvLoadCompactionMapChunkRequest(
                LoadCompactionMapQueue.front())
        );
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_LoadCompactionMapChunk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TLoadCompactionMapChunk& args)
{
    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading compaction map chunk "
            << args.FirstRangeId << ", " << args.RangeCount);

    TIndexTabletDatabase db(tx.DB);

    bool ready = db.ReadCompactionMap(
        args.CompactionMap,
        args.FirstRangeId,
        args.RangeCount);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Loading compaction map chunk "
            << (ready ? "finished" : "restarted"));

    return ready;
}

void TIndexTabletActor::ExecuteTx_LoadCompactionMapChunk(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TLoadCompactionMapChunk& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TIndexTabletActor::CompleteTx_LoadCompactionMapChunk(
    const TActorContext& ctx,
    TTxIndexTablet::TLoadCompactionMapChunk& args)
{
    LoadCompactionMap(args.CompactionMap);
    for (const auto& x: args.CompactionMap) {
        args.LastRangeId = Max(args.LastRangeId, x.RangeId);
    }

    using TNotification =
        TEvIndexTabletPrivate::TEvLoadCompactionMapChunkCompleted;
    auto notification = std::make_unique<TNotification>(
        args.FirstRangeId,
        args.LastRangeId);
    NCloud::Send(ctx, SelfId(), std::move(notification));

    if (args.RequestInfo->Sender != ctx.SelfID) {
        using TResponse =
            TEvIndexTabletPrivate::TEvLoadCompactionMapChunkResponse;
        auto response = std::make_unique<TResponse>();
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    }
}

}   // namespace NCloud::NFileStore::NStorage
