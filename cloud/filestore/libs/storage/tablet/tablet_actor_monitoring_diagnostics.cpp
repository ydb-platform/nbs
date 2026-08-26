#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/aggregate.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <cloud/storage/core/libs/common/simple_template.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/resource/resource.h>

#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

#include <tuple>
#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NActors::NMon;

namespace {

////////////////////////////////////////////////////////////////////////////////

void DumpDiagnosticsPage(IOutputStream& out, ui64 tabletId)
{
    OutputTemplate(
        NResource::Find("html/diagnostics-main.html"),
        {{"STYLE", NResource::Find("css/diagnostics.css")},
         {"JS", NResource::Find("js/diagnostics.js")},
         {"TABLET_ID", ToString(tabletId)}},
        out);
}

void ReplyDiagnosticsError(
    const TActorContext& ctx,
    const TRequestInfoPtr& requestInfo,
    TString message)
{
    TStringStream out;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
    writer.BeginObject();
    writer.WriteKey("error");
    writer.WriteString(message);
    writer.EndObject();

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<TEvRemoteJsonInfoRes>(std::move(out.Str())));
}

struct TShardRow
{
    TString ShardId;
    ui64 Load = 0;
    ui32 Suffer = 0;
    ui64 UsedBlocks = 0;
    ui64 TotalBlocks = 0;
    ui64 Nodes = 0;
};

struct TNodeRow
{
    TString ShardId;
    ui64 NodeId = 0;
    ui64 Requests = 0;
    double AccessScore = 0;
    ui64 LastAccessedUs = 0;
};

struct TLatency
{
    ui64 RequestCount = 0;
    ui64 TotalLatencyUs = 0;
    double TotalDecayedLatencyUs = 0;
    ui64 LastAccessedTimestampUs = 0;

    void Add(const TLatency& other)
    {
        RequestCount += other.RequestCount;
        TotalLatencyUs += other.TotalLatencyUs;
        TotalDecayedLatencyUs += other.TotalDecayedLatencyUs;
        LastAccessedTimestampUs =
            Max(LastAccessedTimestampUs, other.LastAccessedTimestampUs);
    }

    double GetAverageDecayedLatencyUs() const
    {
        return RequestCount ? TotalDecayedLatencyUs / RequestCount : 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsActor final: public TActorBootstrapped<TDiagnosticsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;
    ui32 TopLoaded;
    TString SortBy;
    ui32 TopAccessed;
    ui32 SlowestNodes;
    ui32 SlowestRequests;
    ui32 SlowestShards;

    using TLatencyResult = NAggregation::TResult<TLatency>;

    TVector<TShardRow> Shards;
    TVector<TString> ShardIds;
    TVector<TNodeRow> Nodes;
    TVector<NAggregation::TRow<TLatency>> Latency;
    TVector<TLatencyResult> NodeLatency;
    TVector<TLatencyResult> RequestLatency;
    TVector<TLatencyResult> ShardLatency;
    TVector<TString> Warnings;

    ui32 ShardIndex = 0;
    ui32 InFlight = 0;

public:
    TDiagnosticsActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        ui32 topLoaded,
        TString sortBy,
        ui32 topAccessed,
        ui32 slowestNodes,
        ui32 slowestRequests,
        ui32 slowestShards)
        : RequestInfo(std::move(requestInfo))
        , FileSystemId(std::move(fileSystemId))
        , TopLoaded(topLoaded)
        , SortBy(std::move(sortBy))
        , TopAccessed(topAccessed)
        , SlowestNodes(slowestNodes)
        , SlowestRequests(slowestRequests)
        , SlowestShards(slowestShards)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto request =
            std::make_unique<TEvIndexTablet::TEvGetStorageStatsRequest>();
        request->Record.SetFileSystemId(FileSystemId);
        request->Record.SetCacheTTL(0);
        request->Record.SetMode(
            NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvIndexTablet::TEvGetStorageStatsResponse,
                HandleStorageStats);
            HFunc(
                TEvIndexTablet::TEvGetDiagnosticStatsResponse,
                HandleDiagnosticStats);
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::TABLET_WORKER,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleStorageStats(
        const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& response = ev->Get()->Record;
        if (HasError(response.GetError())) {
            ReplyError(ctx, response.GetError());
            return;
        }

        for (const auto& stats: response.GetStats().GetShardStats()) {
            Shards.push_back(
                {stats.GetShardId(),
                 stats.GetCurrentLoad(),
                 stats.GetSuffer(),
                 stats.GetUsedBlocksCount(),
                 stats.GetTotalBlocksCount(),
                 stats.GetUsedNodesCount()});
            ShardIds.push_back(stats.GetShardId());
        }

        SendDiagnosticRequests(ctx);
    }

    void SendDiagnosticRequests(const TActorContext& ctx)
    {
        while (ShardIndex < ShardIds.size()) {
            auto request = std::make_unique<
                TEvIndexTablet::TEvGetDiagnosticStatsRequest>();
            request->Record.SetFileSystemId(ShardIds[ShardIndex]);
            request->Record.SetLimit(
                Max(TopAccessed,
                    Max(SlowestNodes, Max(SlowestRequests, SlowestShards))));
            ctx.Send(
                MakeIndexTabletProxyServiceId(),
                request.release(),
                0,
                ShardIndex++);
            ++InFlight;
        }

        if (!InFlight && ShardIndex == ShardIds.size()) {
            Reply(ctx);
        }
    }

    void HandleDiagnosticStats(
        const TEvIndexTablet::TEvGetDiagnosticStatsResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& response = ev->Get()->Record;
        --InFlight;
        if (HasError(response.GetError())) {
            const TString shardId = ev->Cookie < ShardIds.size()
                                        ? ShardIds[ev->Cookie]
                                        : TString("unknown");
            Warnings.push_back(
                TStringBuilder()
                << "Failed to read diagnostic stats for shard " << shardId
                << ": " << FormatError(response.GetError()));
        } else {
            ProcessAccessStats(response.GetNodeStats(), Nodes);
            ProcessLatencyStats(response.GetLatencyStats(), Latency);
        }

        SendDiagnosticRequests(ctx);
    }

    void ProcessAccessStats(
        const google::protobuf::RepeatedPtrField<
            NCloud::NFileStore::NProtoPrivate::TNodeStats>& accessStats,
        TVector<TNodeRow>& accessRows)
    {
        for (const auto& stats: accessStats) {
            accessRows.push_back(
                {stats.GetShardId(),
                 stats.GetNodeId(),
                 stats.GetRequestCount(),
                 stats.GetAccessScore(),
                 stats.GetLastAccessedTimestampUs()});
        }
    }

    void ProcessLatencyStats(
        const google::protobuf::RepeatedPtrField<
            NCloud::NFileStore::NProtoPrivate::TNodeLatencyStats>& latencyStats,
        TVector<NAggregation::TRow<TLatency>>& latencyRows)
    {
        for (const auto& stats: latencyStats) {
            NAggregation::TRow<TLatency> row;
            row.Labels = {
                ToString(stats.GetNodeId()),
                stats.GetShardId(),
                stats.GetRequestType()};
            row.Data.RequestCount = stats.GetRequestCount();
            row.Data.TotalLatencyUs = stats.GetTotalLatencyUs();
            row.Data.TotalDecayedLatencyUs =
                stats.GetAverageLatencyDecayedUs() * stats.GetRequestCount();
            row.Data.LastAccessedTimestampUs =
                stats.GetLastAccessedTimestampUs();
            latencyRows.push_back(std::move(row));
        }
    }

    void GroupLatencyCombinations(
        const TVector<TLatencyResult>& latencyAggregates,
        TVector<TLatencyResult>& nodeLatencyRows,
        TVector<TLatencyResult>& requestLatencyRows,
        TVector<TLatencyResult>& shardLatencyRows)
    {
        for (const auto& aggregate: latencyAggregates) {
            const bool hasNodeId = !aggregate.Labels[0].empty();
            const bool hasShardId = !aggregate.Labels[1].empty();
            const bool hasRequestType = !aggregate.Labels[2].empty();

            if (hasNodeId && hasShardId && hasRequestType) {
                nodeLatencyRows.push_back(std::move(aggregate));
            } else if (!hasNodeId && hasShardId && hasRequestType) {
                requestLatencyRows.push_back(std::move(aggregate));
            } else if (!hasNodeId && hasShardId && !hasRequestType) {
                shardLatencyRows.push_back(std::move(aggregate));
            }
        }
    }

    static bool CompareShardRows(const TShardRow& lhs, const TShardRow& rhs)
    {
        // CurrentLoad DESC, Suffer DESC, ShardId ASC
        return std::tie(rhs.Load, rhs.Suffer, lhs.ShardId) <
               std::tie(lhs.Load, lhs.Suffer, rhs.ShardId);
    }

    static bool CompareAccessRows(const TNodeRow& lhs, const TNodeRow& rhs)
    {
        // AccessScore DESC, ShardId ASC, NodeId ASC
        return std::tie(rhs.AccessScore, lhs.ShardId, lhs.NodeId) <
               std::tie(lhs.AccessScore, rhs.ShardId, rhs.NodeId);
    }

    static bool CompareNodeLatencyRows(
        const TLatencyResult& lhs,
        const TLatencyResult& rhs)
    {
        const auto lhsLatency = lhs.GroupAggregate.GetAverageDecayedLatencyUs();
        const auto rhsLatency = rhs.GroupAggregate.GetAverageDecayedLatencyUs();
        const auto lhsNodeId = FromString<ui64>(lhs.Labels[0]);
        const auto rhsNodeId = FromString<ui64>(rhs.Labels[0]);

        // AverageLatencyDecayed DESC, NodeId ASC
        if (lhsLatency != rhsLatency) {
            return lhsLatency > rhsLatency;
        }

        return lhsNodeId < rhsNodeId;
    }

    static bool CompareTotalLatency(
        const TLatencyResult& lhs,
        const TLatencyResult& rhs)
    {
        const auto lhsLatency = lhs.GroupAggregate.TotalDecayedLatencyUs;
        const auto rhsLatency = rhs.GroupAggregate.TotalDecayedLatencyUs;

        // TotalLatencyDecayed DESC,  LastAccessedTimestamp DESC
        return std::tie(
                   rhsLatency,
                   rhs.GroupAggregate.LastAccessedTimestampUs) <
               std::tie(lhsLatency, lhs.GroupAggregate.LastAccessedTimestampUs);
    }

    template <typename T>
    static void WriteTimestamp(NJsonWriter::TBuf& writer, const T& row)
    {
        writer.WriteKey("last_accessed_us");
        writer.WriteString(
            ToString(row.GroupAggregate.LastAccessedTimestampUs));
        writer.WriteKey("last_accessed");
        writer.WriteString(
            row.GroupAggregate.LastAccessedTimestampUs
                ? TInstant::MicroSeconds(
                      row.GroupAggregate.LastAccessedTimestampUs)
                      .ToStringUpToSeconds()
                : "-");
    }

    void Reply(const TActorContext& ctx)
    {
        auto latencyAggregates = NAggregation::Aggregate(Latency);
        GroupLatencyCombinations(
            latencyAggregates,
            NodeLatency,
            RequestLatency,
            ShardLatency);

        if (SortBy == "load") {
            Sort(Shards, CompareShardRows);
        }
        Sort(Nodes, CompareAccessRows);
        Sort(NodeLatency, CompareNodeLatencyRows);
        Sort(RequestLatency, CompareTotalLatency);
        Sort(ShardLatency, CompareTotalLatency);

        TStringStream out;
        NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
        writer.BeginObject();
        writer.WriteKey("filesystem_id");
        writer.WriteString(FileSystemId);
        writer.WriteKey("shard_count");
        writer.WriteULongLong(Shards.size());

        writer.WriteKey("shards");
        writer.BeginList();
        for (ui32 i = 0; i < Min<size_t>(TopLoaded, Shards.size()); ++i) {
            const auto& row = Shards[i];
            writer.BeginObject();
            writer.WriteKey("rank");
            writer.WriteULongLong(i + 1);
            writer.WriteKey("shard_id");
            writer.WriteString(row.ShardId);
            writer.WriteKey("load");
            writer.WriteULongLong(row.Load);
            writer.WriteKey("suffer");
            writer.WriteULongLong(row.Suffer);
            writer.WriteKey("used_blocks");
            writer.WriteULongLong(row.UsedBlocks);
            writer.WriteKey("total_blocks");
            writer.WriteULongLong(row.TotalBlocks);
            writer.WriteKey("nodes");
            writer.WriteULongLong(row.Nodes);
            writer.EndObject();
        }
        writer.EndList();

        writer.WriteKey("node_access");
        writer.BeginList();
        for (ui32 i = 0; i < Min<size_t>(TopAccessed, Nodes.size()); ++i) {
            const auto& row = Nodes[i];
            writer.BeginObject();
            writer.WriteKey("rank");
            writer.WriteULongLong(i + 1);
            writer.WriteKey("shard_id");
            writer.WriteString(row.ShardId);
            writer.WriteKey("node_id");
            writer.WriteString(ToString(row.NodeId));
            writer.WriteKey("requests");
            writer.WriteULongLong(row.Requests);
            writer.WriteKey("access_score");
            writer.WriteDouble(row.AccessScore);
            writer.WriteKey("last_accessed_us");
            writer.WriteString(ToString(row.LastAccessedUs));
            writer.WriteKey("last_accessed");
            writer.WriteString(
                row.LastAccessedUs ? TInstant::MicroSeconds(row.LastAccessedUs)
                                         .ToStringUpToSeconds()
                                   : "-");
            writer.EndObject();
        }
        writer.EndList();

        WriteLatencyList(writer, "node_latency", NodeLatency, SlowestNodes);
        WriteLatencyList(
            writer,
            "request_latency",
            RequestLatency,
            SlowestRequests);
        WriteLatencyList(writer, "shard_latency", ShardLatency, SlowestShards);

        writer.WriteKey("warnings");
        writer.BeginList();
        for (const auto& warning: Warnings) {
            writer.WriteString(warning);
        }
        writer.EndList();
        writer.EndObject();

        NCloud::Reply(
            ctx,
            *RequestInfo,
            std::make_unique<TEvRemoteJsonInfoRes>(std::move(out.Str())));
        Die(ctx);
    }

    static void WriteLatencyList(
        NJsonWriter::TBuf& writer,
        TStringBuf name,
        const TVector<TLatencyResult>& rows,
        ui32 limit)
    {
        writer.WriteKey(name);
        writer.BeginList();
        for (ui32 i = 0; i < Min<size_t>(limit, rows.size()); ++i) {
            const auto& row = rows[i];
            writer.BeginObject();
            writer.WriteKey("rank");
            writer.WriteULongLong(i + 1);
            writer.WriteKey("shard_id");
            writer.WriteString(row.Labels[1]);
            if (row.Labels[0]) {
                writer.WriteKey("node_id");
                writer.WriteString(row.Labels[0]);
            }
            if (row.Labels[2]) {
                writer.WriteKey("request_type");
                writer.WriteString(row.Labels[2]);
            }
            writer.WriteKey("requests");
            writer.WriteULongLong(row.GroupAggregate.RequestCount);
            writer.WriteKey("avg_decayed_us");
            writer.WriteDouble(row.GroupAggregate.GetAverageDecayedLatencyUs());
            writer.WriteKey("total_decayed_us");
            writer.WriteDouble(row.GroupAggregate.TotalDecayedLatencyUs);
            writer.WriteKey("total_us");
            writer.WriteULongLong(row.GroupAggregate.TotalLatencyUs);
            WriteTimestamp(writer, row);
            writer.EndObject();
        }
        writer.EndList();
    }

    void ReplyError(const TActorContext& ctx, const NProto::TError& error)
    {
        ReplyDiagnosticsError(ctx, RequestInfo, FormatError(error));
        Die(ctx);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr&,
        const TActorContext& ctx)
    {
        ReplyError(ctx, MakeError(E_REJECTED, "diagnostics request cancelled"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleHttpInfo_Diagnostics(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (params.Get("getContent") != "1") {
        TStringStream out;
        DumpDiagnosticsPage(out, TabletID());
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvRemoteHttpInfoRes>(std::move(out.Str())));
        return;
    }

    const auto parseParams = [&](TStringBuf name, ui32& value)
    {
        if (!params.Has(name) || !TryFromString(params.Get(name), value)) {
            ReplyDiagnosticsError(
                ctx,
                requestInfo,
                TStringBuilder() << "Missing or invalid parameter: " << name);
            return false;
        }
        return true;
    };

    ui32 topLoaded;
    ui32 topAccessed;
    ui32 slowestNodes;
    ui32 slowestRequests;
    ui32 slowestShards;

    if (!parseParams("topLoaded", topLoaded) ||
        !parseParams("topAccessed", topAccessed) ||
        !parseParams("slowestNodes", slowestNodes) ||
        !parseParams("slowestRequests", slowestRequests) ||
        !parseParams("slowestShards", slowestShards))
    {
        return;
    }

    if (!params.Has("sortBy") || params.Get("sortBy") != "load") {
        ReplyDiagnosticsError(
            ctx,
            requestInfo,
            "Missing or invalid parameter: sortBy");
        return;
    }
    TString sortBy = params.Get("sortBy");

    NCloud::Register<TDiagnosticsActor>(
        ctx,
        std::move(requestInfo),
        GetFileSystemId(),
        topLoaded,
        std::move(sortBy),
        topAccessed,
        slowestNodes,
        slowestRequests,
        slowestShards);
}

}   // namespace NCloud::NFileStore::NStorage
