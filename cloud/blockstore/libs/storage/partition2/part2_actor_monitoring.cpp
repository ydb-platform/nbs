#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/hostname.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/tenant.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>

#include <cloud/storage/core/libs/common/format.h>

#include <contrib/ydb/core/base/appdata.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NKikimr;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

void DumpChannels(
    IOutputStream& out,
    const TPartitionState& state,
    const TTabletStorageInfo& storage,
    const TDiagnosticsConfig& config,
    ui64 hiveTabletId)
{
    HTML(out) {
        DIV() {
            out << "<a href='app?TabletID=" << hiveTabletId
                << "&page=Groups"
                << "&tablet_id=" << storage.TabletID
                << "'>Channel history</a>";
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
                                TString dataKind;
                                if (channel.Channel <
                                    static_cast<ui32>(cps.size()))
                                {
                                    dataKind = TStringBuilder()
                                               << static_cast<EChannelDataKind>(
                                                      cps[channel.Channel]
                                                          .GetDataKind());
                                }
                                out << "<a href='"
                                    << GetMonitoringYDBGroupUrl(
                                           config,
                                           latestEntry->GroupID,
                                           channel.StoragePool,
                                           dataKind)
                                    << "'>Graphs</a>";
                                auto monitoringDashboardUrl =
                                    GetMonitoringDashboardYDBGroupUrl(
                                        config,
                                        latestEntry->GroupID);
                                if (!monitoringDashboardUrl.empty()) {
                                    out << "<br>" << "<a href='"
                                        << monitoringDashboardUrl
                                        << "'>Group dashboard</a>";
                                }
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
    ui32 freshBlockCount,
    ui32 blockSize,
    const TVector<NProto::TCheckpointMeta>& items)
{
    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "CheckpointId"; }
                    TABLED() { out << "CommitId"; }
                    TABLED() { out << "IdempotenceId"; }
                    TABLED() { out << "Time"; }
                    TABLED() { out << "Size"; }
                }
            }
            TABLEBODY() {
                for (const auto& item: items) {
                    TABLER() {
                        TABLED() { out << item.GetCheckpointId(); }
                        TABLED() { out << item.GetCommitId(); }
                        TABLED() { out << item.GetIdempotenceId(); }
                        TABLED() {
                            auto ts = TInstant::MicroSeconds(item.GetDateCreated());
                            out << FormatTimestamp(ts);
                        }
                        TABLED() {
                            auto blockCount = freshBlockCount
                                            + item.GetStats().GetMergedBlocksCount();
                            out << FormatByteSize(blockCount * blockSize);
                        }
                    }
                }
            }
        }
    }
}

void DumpGarbageQueue(
    IOutputStream& out,
    const TTabletStorageInfo& storage,
    const TVector<TBlobCounter>& items)
{
    HTML(out) {
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "CommitId"; }
                    TABLED() { out << "BlobId"; }
                    TABLED() { out << "GarbageBlocks"; }
                }
            }
            TABLEBODY() {
                for (const auto& kv: items) {
                    TABLER() {
                        TABLED() { DumpCommitId(out, kv.first.CommitId()); }
                        TABLED_CLASS("view") { DumpBlobId(out, storage, kv.first); }
                        TABLED() { out << kv.second; }
                    }
                }
            }
        }
    }
}

void DumpCompactionInfo(
    IOutputStream& out,
    const TForcedCompactionState& state)
{
    if (state.IsRunning && (state.OperationId == "partition-monitoring-compaction")) {
        HTML(out) {
            DIV_CLASS("progress") {
                ui32 percents = (state.Progress * 100 / state.RangeCount);
                out << "<div class='progress-bar' role='progressbar' aria-valuemin='0'"
                    << " style='width: " << percents << "%'"
                    << " aria-valuenow='" << state.Progress
                    << "' aria-valuemax='" << state.RangeCount << "'>"
                    << percents << "%</div>";
            }
            out << state.Progress << " of " << state.RangeCount;
        }
    }
}

void DumpCleanupInfo(
    IOutputStream& out,
    const TForcedCleanupState& state)
{
    Y_UNUSED(state);

    HTML(out) {
        DIV_CLASS("progress") {
            out << "<div class='progress-bar' role='progressbar' aria-valuemin='0'"
                << " style='width: 0%'"
                << " aria-valuenow='0'"
                << " aria-valuemax='100'>"
                << "0%</div>";
        }
        out << "Cleanup in progress";
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
        {"compactAll",       &TPartitionActor::HandleHttpInfo_ForceCompaction },
        {"compact",          &TPartitionActor::HandleHttpInfo_ForceCompaction },
        {"addGarbage",       &TPartitionActor::HandleHttpInfo_AddGarbage      },
        {"collectGarbage",   &TPartitionActor::HandleHttpInfo_CollectGarbage  }
    }};

    static const THttpHandlers getActions  {{
        {"describe",           &TPartitionActor::HandleHttpInfo_Describe      },
        {"view",               &TPartitionActor::HandleHttpInfo_View          }
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

        if (auto* handler = postActions.FindPtr(action))
        {
            if (methodType != HTTP_METHOD_POST) {
                RejectHttpRequest(ctx, *requestInfo, "Wrong HTTP method");
                return;
            }

            std::invoke(*handler, this, ctx, params, std::move(requestInfo));
            return;
        }

        if (auto* handler = getActions.FindPtr(action))
        {
            std::invoke(*handler, this, ctx, params, std::move(requestInfo));
            return;
        }

        HandleHttpInfo_Default(ctx, params, std::move(requestInfo));
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
                    DumpPartitionStats(out, State->GetConfig(), State->GetStats(), State->GetFreshBlockCount());

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
                        State->GetFreshBlockCount(),
                        State->GetBlockSize(),
                        State->GetCheckpoints().Get());

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

                    DumpCompactionMap(
                        out,
                        *Info(),
                        State->GetCompactionMap().GetTop(10),
                        State->GetCompactionMap().GetRangeSize()
                    );

                    TAG(TH3) { out << "GarbageQueue"; }
                    DumpGarbageQueue(
                        out,
                        *Info(),
                        State->GetBlobs().GetTopGarbage(
                            10,
                            Max<size_t>()
                        ).BlobCounters
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
                        if (!State->IsForcedCleanupRunning()) {
                            BuildMenuButton(out, "cleanup-all");
                        }
                        out << "CleanupQueue";
                    }

                    if (State->IsForcedCleanupRunning()) {
                        DumpCleanupInfo(out, State->GetForcedCleanupState());
                    } else {
                        out << "<div class='collapse form-group' id='cleanup-all'>";
                        BuildForceCleanupButton(out, TabletID());
                        out << "</div>";
                    }
                }

                DIV_CLASS_ID("tab-pane", "Channels") {
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
                }
            }
        }

        GeneratePartitionTabsJs(out);
        GenerateBlobviewJS(out);
        GenerateActionsJS(out);
    }

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
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

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
