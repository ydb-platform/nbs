#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/diag_down_graph.h>
#include <cloud/blockstore/libs/diagnostics/hostname.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/tenant.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

#include <ydb/core/base/appdata.h>

#include <ranges>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NKikimr;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

void DumpDownGroups(
    IOutputStream& out,
    TInstant now,
    const TPartitionState& state,
    const TTabletStorageInfo& storage,
    const TDiagnosticsConfig& config)
{
    HTML(out)
    {
        TABLE_SORTABLE_CLASS("table table-bordered")
        {
            TABLEHEAD()
            {
                TABLER()
                {
                    TABLEH() { out << "Group"; }
                    TABLEH() { out << "Downtime"; }

                }
            }

            auto addGroupRow = [&](
                const ui32 groupId,
                const TDowntimeHistory& history)
            {
                TABLER() {
                    TABLEH()
                    {
                        auto groupIdFinder =
                            [groupId](const TTabletChannelInfo& channelInfo)
                        {
                            const auto* entry = channelInfo.LatestEntry();
                            if (!entry) {
                                return false;
                            }
                            return entry->GroupID == groupId;
                        };
                        auto matchedInfos = storage.Channels |
                                            std::views::filter(groupIdFinder);
                        if (matchedInfos.empty()) {
                            out << groupId;
                        } else {
                            for (const TTabletChannelInfo& channelInfo:
                                 matchedInfos)
                            {
                                out << groupId << "&nbsp;<a href='"
                                    << GetMonitoringYDBGroupUrl(
                                           config,
                                           groupId,
                                           channelInfo.StoragePool)
                                    << "'>Graphs&nbsp;"
                                    << "(Channel=" << channelInfo.Channel
                                    << ")</a><br/>";
                            }
                        }
                    }
                    TABLEH() {
                        TSvgWithDownGraph svg(out);
                        for (const auto& [time, state]: history) {
                            svg.AddEvent(
                                time,
                                state == EDowntimeStateChange::DOWN);
                        }
                    }
                }
            };

            for (const auto& [groupId, history]: state.GetGroupId2Downtimes()) {
                addGroupRow(groupId, history.RecentEvents(now));
            }
        }
    }
}

void DumpChannels(
    IOutputStream& out,
    const TPartitionState& state,
    const TTabletStorageInfo& storage,
    const TDiagnosticsConfig& config,
    ui64 hiveTabletId)
{
    HTML(out) {
        DIV() {
            out << "<p><a href='app?TabletID=" << hiveTabletId
                << "&page=Groups"
                << "&tablet_id=" << storage.TabletID
                << "'>Channel history</a></p>";
        }

        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                for (const auto& channel: storage.Channels) {
                    TABLER() {
                        TABLED() { out << "Channel: " << channel.Channel; }
                        TABLED() { out << "StoragePool: " << channel.StoragePool; }

                        if (auto latestEntry = channel.LatestEntry()) {
                            TABLED() { out << "Id: " << latestEntry->GroupID; }
                            TABLED() { out << "Gen: " << latestEntry->FromGeneration; }
                            const auto& cps =
                                state.GetConfig().GetExplicitChannelProfiles();
                            if (cps.size()) {
                                // we need this check for legacy volumes
                                // see NBS-752
                                if (channel.Channel < static_cast<ui32>(cps.size())) {
                                    const auto dataKind = static_cast<EChannelDataKind>(
                                        cps[channel.Channel].GetDataKind());
                                    const auto& poolKind =
                                        cps[channel.Channel].GetPoolKind();
                                    TABLED() { out << "PoolKind: " << poolKind; }
                                    TABLED() { out << "DataKind: " << dataKind; }
                                } else {
                                    // we need to output 2 cells, otherwise table
                                    // markup will be a bit broken
                                    TABLED() { out << "Ghost"; }
                                    TABLED() { out << "Channel"; }
                                }
                            }
                            TABLED() {
                                TStringBuf label;
                                TStringBuf color;
                                if (state.CheckPermissions(channel.Channel, EChannelPermission::SystemWritesAllowed)) {
                                    if (state.CheckPermissions(channel.Channel, EChannelPermission::UserWritesAllowed)) {
                                        color = "green";
                                        label = "Writable";
                                    } else {
                                        color = "yellow";
                                        label = "SystemWritable";
                                    }
                                } else {
                                    if (state.CheckPermissions(channel.Channel, EChannelPermission::UserWritesAllowed)) {
                                        color = "pink";
                                        label = "WeirdState";
                                    } else {
                                        color = "orange";
                                        label = "Readonly";
                                    }
                                }

                                SPAN_CLASS_STYLE("label", TStringBuilder() << "background-color: " << color) {
                                    out << label;
                                }
                            }
                            TABLED() {
                                out << "<a href='"
                                    << "../actors/blobstorageproxies/blobstorageproxy"
                                    << latestEntry->GroupID
                                    << "'>Status</a>";
                            }
                            TABLED() {
                                out << "<a href='"
                                    << GetMonitoringYDBGroupUrl(
                                           config,
                                           latestEntry->GroupID,
                                           channel.StoragePool)
                                    << "'>Graphs</a>";
                            }
                            TABLED() {
                                BuildReassignChannelButton(
                                    out,
                                    hiveTabletId,
                                    storage.TabletID,
                                    channel.Channel);
                            }
                        }
                    }
                }
            }
        }
    }
}

void DumpCheckpoints(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    ui32 freshBlocksCount,
    ui32 blockSize,
    const TVector<TCheckpoint>& checkpoints,
    const THashMap<TString, ui64>& checkpointId2CommitId)
{
    Y_UNUSED(storage);

    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "CheckpointId"; }
                    TABLED() { out << "CommitId"; }
                    TABLED() { out << "IdempotenceId"; }
                    TABLED() { out << "Time"; }
                    TABLED() { out << "Size"; }
                    TABLED() { out << "DataDeleted"; }
                }
            }
            TABLEBODY() {
                for (const auto& mapping: checkpointId2CommitId) {
                    const auto& checkpointId = mapping.first;
                    const auto& commitId = mapping.second;

                    const auto* checkpoint = FindIfPtr(
                        checkpoints,
                        [&](const auto& ckp) {
                            return ckp.CheckpointId == checkpointId;
                    });

                    TABLER() {
                        TABLED() { out << checkpointId; }
                        TABLED() { out << commitId; }
                        TABLED() { out << (checkpoint ? checkpoint->IdempotenceId : ""); }
                        TABLED() { out << FormatTimestamp(checkpoint ? checkpoint->DateCreated : TInstant::Zero()); }
                        TABLED() {
                            ui64 byteSize = 0;
                            if (checkpoint) {
                                auto blocksCount = freshBlocksCount;
                                blocksCount += checkpoint->Stats.GetMixedBlocksCount();
                                blocksCount += checkpoint->Stats.GetMergedBlocksCount();
                                byteSize = static_cast<ui64>(blocksCount) * blockSize;
                            }

                            out << FormatByteSize(byteSize);
                        }
                        TABLED() { out << (checkpoint ? "" : "true"); }
                    }
                }
            }
        }
    }
}

void DumpCleanupQueue(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    const TCleanupQueue& cleanupQueue)
{
    HTML(out) {
        TAG(TH3) { out << "CleanupQueueItems"; }

        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "CommitId"; }
                    TABLED() { out << "BlobId"; }
                    TABLED() { out << "Deleted"; }
                }
            }
            TABLEBODY() {
                for (const auto& item: cleanupQueue.GetItems()) {
                    TABLER() {
                        TABLED() { DumpCommitId(out, item.BlobId.CommitId()); }
                        TABLED_CLASS("view") {
                            DumpBlobId(out, storage, item.BlobId);
                        }
                        TABLED() { DumpCommitId(out, item.CommitId); }
                    }
                }
            }
        }

        TAG(TH3) { out << "CleanupQueueBarriers"; }

        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "CommitId"; }
                }
            }
            TABLEBODY() {
                TVector<ui64> commitIds;
                cleanupQueue.GetCommitIds(commitIds);

                for (const auto commitId: commitIds) {
                    TABLER() {
                        TABLED() { DumpCommitId(out, commitId); }
                    }
                }
            }
        }

        TAG(TH3) { out << "CleanupQueueCounters"; }

        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Name"; }
                    TABLED() { out << "Value"; }
                }
            }
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "Count"; }
                    TABLED() { out << cleanupQueue.GetCount(); }
                }
                TABLER() {
                    TABLED() { out << "QueueBytes"; }
                    TABLED() { out << cleanupQueue.GetQueueBytes(); }
                }
            }
        }
    }
}

void DumpProgress(IOutputStream& out, ui64 progress, ui64 total)
{
    HTML(out) {
        DIV_CLASS("progress") {
            ui32 percents = (progress * 100 / total);
            out << "<div class='progress-bar' role='progressbar' aria-valuemin='0'"
                << " style='width: " << percents << "%'"
                << " aria-valuenow='" << progress
                << "' aria-valuemax='" << total << "'>"
                << percents << "%</div>";
        }
        out << progress << " of " << total;
    }
}

void DumpCompactionInfo(IOutputStream& out, const TForcedCompactionState& state)
{
    if (state.IsRunning && (state.OperationId == "partition-monitoring-compaction")) {
        DumpProgress(out, state.Progress, state.RangesCount);
    }
}

void DumpMetadataRebuildInfo(IOutputStream& out, ui64 current, ui64 total)
{
    DumpProgress(out, current, total);
}

void DumpScanDiskInfo(IOutputStream& out, ui64 current, ui64 total)
{
    DumpProgress(out, current, total);
}

void DumpCompactionScoreHistory(
    IOutputStream& out,
    const TTsRingBuffer<TCompactionScores>& scoreHistory)
{
    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Ts"; }
                    TABLED() { out << "Score"; }
                    TABLED() { out << "GarbageScore"; }
                }
            }
            TABLEBODY() {
                for (ui32 i = 0; i < scoreHistory.Size(); ++i) {
                    const auto s = scoreHistory.Get(i);

                    TABLER() {
                        TABLED() { out << s.Ts; }
                        TABLED() { out << s.Value.Score; }
                        TABLED() { out << s.Value.GarbageScore; }
                    }
                }
            }
        }
    }
}

void DumpCleanupScoreHistory(
    IOutputStream& out,
    const TTsRingBuffer<ui32>& scoreHistory)
{
    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Ts"; }
                    TABLED() { out << "QueueSize"; }
                }
            }
            TABLEBODY() {
                for (ui32 i = 0; i < scoreHistory.Size(); ++i) {
                    const auto s = scoreHistory.Get(i);

                    TABLER() {
                        TABLED() { out << s.Ts; }
                        TABLED() { out << s.Value; }
                    }
                }
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo(
    const NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    using THttpHandler = void(TPartitionActor::*)(
        const NActors::TActorContext&,
        const TCgiParameters&,
        TRequestInfoPtr);

    using THttpHandlers = THashMap<TString, THttpHandler>;

    static const THttpHandlers postActions {{
        {"addGarbage",       &TPartitionActor::HandleHttpInfo_AddGarbage      },
        {"collectGarbage",   &TPartitionActor::HandleHttpInfo_CollectGarbage  },
        {"compact",          &TPartitionActor::HandleHttpInfo_ForceCompaction },
        {"compactAll",       &TPartitionActor::HandleHttpInfo_ForceCompaction },
        {"rebuildMetadata",  &TPartitionActor::HandleHttpInfo_RebuildMetadata },
        {"scanDisk",         &TPartitionActor::HandleHttpInfo_ScanDisk        }
    }};

    static const THttpHandlers getActions {{
        {"check",             &TPartitionActor::HandleHttpInfo_Check          },
        {"describe",          &TPartitionActor::HandleHttpInfo_Describe       },
        {"view",              &TPartitionActor::HandleHttpInfo_View           }
    }};

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "HttpInfo",
        requestInfo->CallContext->RequestId);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] HTTP request: %s",
        TabletID(),
        msg->Query.Quote().data());

    if (State && State->IsLoadStateFinished()) {
        auto methodType = GetHttpMethodType(*msg);
        auto params = GatherHttpParameters(*msg);
        const auto& action = params.Get("action");

        if (auto* handler = postActions.FindPtr(action)) {
            if (methodType != HTTP_METHOD_POST) {
                RejectHttpRequest(ctx, *requestInfo, "Wrong HTTP method");
                return;
            }

            std::invoke(*handler, this, ctx, params, requestInfo);
            return;
        }

        if (auto* handler = getActions.FindPtr(action)) {
            std::invoke(*handler, this, ctx, params, requestInfo);
            return;
        }

        HandleHttpInfo_Default(ctx, params, requestInfo);
        return;
    }

    TStringStream out;
    DumpTabletNotReady(out);

    LWTRACK(
        ResponseSent_Partition,
        requestInfo->CallContext->LWOrbit,
        "HttpInfo",
        requestInfo->CallContext->RequestId);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));
}

void TPartitionActor::HandleHttpInfo_Default(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    TStringStream out;
    HTML(out) {
        DIV_CLASS_ID("container-fluid", "tabs") {
            BuildPartitionTabs(out);

            DIV_CLASS("tab-content") {
                DIV_CLASS_ID("tab-pane active", "Overview") {
                    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);
                    DumpMonitoringPartitionLink(out, *DiagnosticsConfig);

                    TAG(TH3) { out << "State"; }
                    State->DumpHtml(out);

                    TAG(TH3) { out << "Partition Statistics"; }
                    DumpPartitionStats(out, State->GetConfig(), State->GetStats(), State->GetUnflushedFreshBlocksCount());

                    TAG(TH3) { out << "Partition Counters"; }
                    DumpPartitionCounters(out, State->GetStats());

                    TAG(TH3) { out << "Partition Config"; }
                    DumpPartitionConfig(out, State->GetConfig());

                    TAG(TH3) { out << "Misc"; }
                    TABLE_CLASS("table table-condensed") {
                        TABLEBODY() {
                            TABLER() {
                                TABLED() { out << "Executor Reject Probability"; }
                                TABLED() { out << Executor()->GetRejectProbability(); }
                            }
                        }
                    }
                }

                DIV_CLASS_ID("tab-pane", "Tables") {
                    TAG(TH3) {
                        out << "Checkpoints";
                    }

                    DumpCheckpoints(
                        out,
                        *Info(),
                        State->GetUnflushedFreshBlocksCount(),
                        State->GetBlockSize(),
                        State->GetCheckpoints().Get(),
                        State->GetCheckpoints().GetMapping());

                    TAG(TH3) {
                        if (!State->IsForcedCompactionRunning()) {
                            BuildMenuButton(out, "compact-all");
                        }
                        out << "CompactionQueue";
                    }

                    if (State->IsForcedCompactionRunning()) {
                        DumpCompactionInfo(out, State->GetForcedCompactionState());
                    } else {
                        out << "<div class='collapse form-group' id='compact-all'>";
                        BuildForceCompactionButton(out, TabletID());
                        out << "</div>";
                    }

                    TAG(TH3) {
                        out << "ByScore";
                    }

                    DumpCompactionMap(
                        out,
                        *Info(),
                        State->GetCompactionMap().GetTop(10),
                        State->GetCompactionMap().GetRangeSize()
                    );

                    TAG(TH3) {
                        out << "ByGarbageScore";
                    }

                    DumpCompactionMap(
                        out,
                        *Info(),
                        State->GetCompactionMap().GetTopByGarbageBlockCount(10),
                        State->GetCompactionMap().GetRangeSize()
                    );

                    TAG(TH3) {
                        out << "CompactionScoreHistory";
                    }

                    DumpCompactionScoreHistory(
                        out,
                        State->GetCompactionScoreHistory()
                    );

                    DumpCleanupQueue(out, *Info(), State->GetCleanupQueue());

                    TAG(TH3) {
                        out << "CleanupScoreHistory";
                    }

                    DumpCleanupScoreHistory(
                        out,
                        State->GetCleanupScoreHistory()
                    );

                    TAG(TH3) { out << "NewBlobs"; }
                    DumpBlobs(out, *Info(), State->GetGarbageQueue().GetNewBlobs());

                    TAG(TH3) {
                        BuildMenuButton(out, "garbage-options");
                        out << "GarbageBlobs";
                    }

                    out << "<div class='collapse form-group' id='garbage-options'>";
                    PARA() {
                        out << "<a href='' data-toggle='modal' data-target='#collect-garbage'>Collect Garbage</a>";
                    }
                    PARA() {
                        out << "<a href='' data-toggle='modal' data-target='#set-hard-barriers'>Set Hard Barriers</a>";
                    }
                    BuildAddGarbageButton(out, TabletID());
                    BuildCollectGarbageButton(out, TabletID());
                    BuildSetHardBarriers(out, TabletID());
                    out << "</div>";

                    DumpBlobs(out, *Info(), State->GetGarbageQueue().GetGarbageBlobs());

                    TAG(TH3) {
                        BuildMenuButton(out, "metadata-rebuild");
                        out << "Rebuild metadata";
                    }

                    if (State->IsMetadataRebuildStarted()) {
                        const auto progress = State->GetMetadataRebuildProgress();
                        DumpMetadataRebuildInfo(out, progress.Processed, progress.Total);
                    } else {
                        out << "<div class='collapse form-group' id='metadata-rebuild'>";
                        for (const auto rangesPerBatch : {1, 10, 100}) {
                            BuildRebuildMetadataButton(out, TabletID(), rangesPerBatch);
                        }
                        out << "</div>";
                    }

                    TAG(TH3) {
                        BuildMenuButton(out, "scan-disk");
                        out << "Scan disk";
                    }

                    if (State->IsScanDiskStarted()) {
                        const auto progress = State->GetScanDiskProgress();
                        DumpScanDiskInfo(
                            out,
                            progress.ProcessedBlobs,
                            progress.TotalBlobs);
                    } else {
                        out << "<div class='collapse form-group' id='scan-disk'>";
                        for (const auto blobsPerBatch : {1, 10, 100}) {
                            BuildScanDiskButton(out, TabletID(), blobsPerBatch);
                        }
                        out << "</div>";
                    }
                }

                DIV_CLASS_ID("tab-pane", "Channels") {
                    DumpDownGroups(
                        out,
                        ctx.Now(),
                        *State,
                        *Info(),
                        *DiagnosticsConfig);

                    TAG(TH3) {
                        BuildMenuButton(out, "reassign-all");
                        out << "Channels";
                    }
                    out << "<div class='collapse form-group' id='reassign-all'>";
                    BuildReassignChannelsButton(
                        out,
                        GetHiveTabletId(Config, ctx),
                        Info()->TabletID);
                    out << "</div>";
                    DumpChannels(
                        out,
                        *State,
                        *Info(),
                        *DiagnosticsConfig,
                        GetHiveTabletId(Config, ctx));
                }

                DIV_CLASS_ID("tab-pane", "Index") {
                    DumpDescribeHeader(out, *Info());
                    DumpCheckHeader(out, *Info());
                }
            }
        }

        GeneratePartitionTabsJs(out);
        GenerateBlobviewJS(out);
        GenerateActionsJS(out);
    }
    SendHttpResponse(ctx, *requestInfo, out.Str());
}

void TPartitionActor::RejectHttpRequest(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION, message);

    SendHttpResponse(ctx, requestInfo, std::move(message), EAlertLevel::DANGER);
}

void TPartitionActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message,
    EAlertLevel alertLevel)
{
    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, message, TabletID(), alertLevel);

    SendHttpResponse(ctx, requestInfo, std::move(out.Str()));
}

void TPartitionActor::SendHttpResponse(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    TString message)
{
    LWTRACK(
        ResponseSent_Partition,
        requestInfo.CallContext->LWOrbit,
        "HttpInfo",
        requestInfo.CallContext->RequestId);

    NCloud::Reply(
        ctx,
        requestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(message)));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
