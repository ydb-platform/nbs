#include "tablet_actor.h"

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

constexpr ui32 MaxTop = 1000;
constexpr ui32 MaxInFlight = 10;

void DumpDiagnosticsPage(IOutputStream& out, ui64 tabletId)
{
    OutputTemplate(NResource::Find("html/diagnostics-main.html"), {
        {"STYLE", NResource::Find("css/diagnostics.css")},
        {"JS", NResource::Find("js/diagnostics.js")},
        {"TABLET_ID", ToString(tabletId)}}, out);
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

struct TLatencyRow
{
    TString ShardId;
    TString RequestType;
    TString NodeId;
    ui64 Requests = 0;
    ui64 TotalLatencyUs = 0;
    double TotalDecayedLatencyUs = 0;
    ui64 LastAccessedUs = 0;

    double AverageDecayedLatencyUs() const
    {
        return Requests ? TotalDecayedLatencyUs / Requests : 0;
    }
};

TLatencyRow* FindLatencyRow(
    TVector<TLatencyRow>& rows,
    const TString& shardId,
    const TString& requestType,
    const TString& nodeId)
{
    for (auto& row: rows) {
        if (row.ShardId == shardId && row.RequestType == requestType &&
            row.NodeId == nodeId)
        {
            return &row;
        }
    }

    rows.push_back({shardId, requestType, nodeId});
    return &rows.back();
}

TLatencyRow* FindRequestLatencyRow(
    TVector<TLatencyRow>& rows,
    const TString& shardId,
    const TString& requestType)
{
    return FindLatencyRow(rows, shardId, requestType, {});
}

TLatencyRow* FindShardLatencyRow(
    TVector<TLatencyRow>& rows,
    const TString& shardId)
{
    return FindLatencyRow(rows, shardId, {}, {});
}

void AddLatency(
    TLatencyRow& row,
    const NProtoPrivate::TNodeLatencyStats& stats)
{
    row.Requests += stats.GetRequestCount();
    row.TotalLatencyUs += stats.GetTotalLatencyUs();
    row.TotalDecayedLatencyUs +=
        stats.GetAverageLatencyDecayedUs() * stats.GetRequestCount();
    row.LastAccessedUs = Max(row.LastAccessedUs, stats.GetLastAccessedTimestampUs());
}

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsActor final
    : public TActorBootstrapped<TDiagnosticsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;
    const ui32 Top;
    const ui32 TopNodes;

    TVector<TShardRow> Shards;
    TVector<TNodeRow> Nodes;
    TVector<TLatencyRow> NodeLatency;
    TVector<TLatencyRow> RequestLatency;
    TVector<TLatencyRow> ShardLatency;
    TVector<TString> Warnings;
    TVector<TString> ShardIds;

    ui32 NextShard = 0;
    ui32 InFlight = 0;

public:
    TDiagnosticsActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        ui32 top,
        ui32 topNodes)
        : RequestInfo(std::move(requestInfo))
        , FileSystemId(std::move(fileSystemId))
        , Top(top)
        , TopNodes(topNodes)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto request = std::make_unique<TEvIndexTablet::TEvGetStorageStatsRequest>();
        request->Record.SetFileSystemId(FileSystemId);
        request->Record.SetCacheTTL(1000);
        request->Record.SetMode(
            NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
        ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvIndexTablet::TEvGetStorageStatsResponse,
                HandleStorageStats);
            HFunc(TEvIndexTablet::TEvGetNodeLatencyStatsResponse,
                HandleNodeLatencyStats);
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
            Shards.push_back({
                stats.GetShardId(),
                stats.GetCurrentLoad(),
                stats.GetSuffer(),
                stats.GetUsedBlocksCount(),
                stats.GetTotalBlocksCount(),
                stats.GetUsedNodesCount()});
            ShardIds.push_back(stats.GetShardId());
        }

        for (const auto& stats: response.GetStats().GetNodeStats()) {
            Nodes.push_back({
                stats.GetShardId(),
                stats.GetNodeId(),
                stats.GetRequestCount(),
                stats.GetAccessScore(),
                stats.GetLastAccessedTimestampUs()});
        }

        SendMoreLatencyRequests(ctx);
    }

    void SendMoreLatencyRequests(const TActorContext& ctx)
    {
        while (InFlight < MaxInFlight && NextShard < ShardIds.size()) {
            auto request =
                std::make_unique<TEvIndexTablet::TEvGetNodeLatencyStatsRequest>();
            request->Record.SetFileSystemId(ShardIds[NextShard]);
            request->Record.SetLimit(TopNodes);
            ctx.Send(
                MakeIndexTabletProxyServiceId(),
                request.release(),
                0,
                NextShard++);
            ++InFlight;
        }

        if (!InFlight && NextShard == ShardIds.size()) {
            Reply(ctx);
        }
    }

    void HandleNodeLatencyStats(
        const TEvIndexTablet::TEvGetNodeLatencyStatsResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& response = ev->Get()->Record;
        --InFlight;
        if (HasError(response.GetError())) {
            const TString shardId = ev->Cookie < ShardIds.size()
                ? ShardIds[ev->Cookie]
                : TString("unknown");
            Warnings.push_back(TStringBuilder()
                << "Failed to read latency stats for shard "
                << shardId
                << ": " << FormatError(response.GetError()));
        } else {
            for (const auto& stats: response.GetLatencyStats()) {
                const auto shardId = stats.GetShardId();
                const auto requestType = stats.GetRequestType();
                const auto nodeId = ToString(stats.GetNodeId());
                AddLatency(*FindLatencyRow(
                    NodeLatency, shardId, requestType, nodeId), stats);
                AddLatency(*FindRequestLatencyRow(
                    RequestLatency, shardId, requestType), stats);
                AddLatency(*FindShardLatencyRow(ShardLatency, shardId), stats);
            }
        }

        SendMoreLatencyRequests(ctx);
    }

    static void SortRows(
        TVector<TShardRow>& rows,
        TVector<TNodeRow>& nodes,
        TVector<TLatencyRow>& nodeLatency,
        TVector<TLatencyRow>& requestLatency,
        TVector<TLatencyRow>& shardLatency)
    {
        Sort(rows, [] (const auto& l, const auto& r) {
            return std::tie(r.Load, r.Suffer, l.ShardId) <
                   std::tie(l.Load, l.Suffer, r.ShardId);
        });
        Sort(nodes, [] (const auto& l, const auto& r) {
            return std::tie(r.AccessScore, l.ShardId, l.NodeId) <
                   std::tie(l.AccessScore, r.ShardId, r.NodeId);
        });
        Sort(nodeLatency, [] (const auto& l, const auto& r) {
            const auto lAverage = l.AverageDecayedLatencyUs();
            const auto rAverage = r.AverageDecayedLatencyUs();
            return std::tie(rAverage, l.NodeId) <
                   std::tie(lAverage, r.NodeId);
        });
        auto total = [] (const auto& l, const auto& r) {
            return std::tie(r.TotalDecayedLatencyUs, r.LastAccessedUs) <
                   std::tie(l.TotalDecayedLatencyUs, l.LastAccessedUs);
        };
        Sort(requestLatency, total);
        Sort(shardLatency, total);
    }

    template <typename T>
    static void WriteTimestamp(
        NJsonWriter::TBuf& writer,
        const T& row)
    {
        writer.WriteKey("last_accessed_us");
        writer.WriteString(ToString(row.LastAccessedUs));
        writer.WriteKey("last_accessed");
        writer.WriteString(
            row.LastAccessedUs
                ? TInstant::MicroSeconds(row.LastAccessedUs).ToStringUpToSeconds()
                : "-");
    }

    void Reply(const TActorContext& ctx)
    {
        SortRows(Shards, Nodes, NodeLatency, RequestLatency, ShardLatency);

        TStringStream out;
        NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
        writer.BeginObject();
        writer.WriteKey("filesystem_id");
        writer.WriteString(FileSystemId);
        writer.WriteKey("shard_count");
        writer.WriteULongLong(Shards.size());

        writer.WriteKey("shards");
        writer.BeginList();
        for (ui32 i = 0; i < Min<size_t>(Top, Shards.size()); ++i) {
            const auto& row = Shards[i];
            writer.BeginObject();
            writer.WriteKey("rank"); writer.WriteULongLong(i + 1);
            writer.WriteKey("shard_id"); writer.WriteString(row.ShardId);
            writer.WriteKey("load"); writer.WriteULongLong(row.Load);
            writer.WriteKey("suffer"); writer.WriteULongLong(row.Suffer);
            writer.WriteKey("used_blocks"); writer.WriteULongLong(row.UsedBlocks);
            writer.WriteKey("total_blocks"); writer.WriteULongLong(row.TotalBlocks);
            writer.WriteKey("nodes"); writer.WriteULongLong(row.Nodes);
            writer.EndObject();
        }
        writer.EndList();

        writer.WriteKey("node_access");
        writer.BeginList();
        for (ui32 i = 0; i < Min<size_t>(TopNodes, Nodes.size()); ++i) {
            const auto& row = Nodes[i];
            writer.BeginObject();
            writer.WriteKey("rank"); writer.WriteULongLong(i + 1);
            writer.WriteKey("shard_id"); writer.WriteString(row.ShardId);
            writer.WriteKey("node_id"); writer.WriteString(ToString(row.NodeId));
            writer.WriteKey("requests"); writer.WriteULongLong(row.Requests);
            writer.WriteKey("access_score"); writer.WriteDouble(row.AccessScore);
            writer.WriteKey("last_accessed_us"); writer.WriteString(ToString(row.LastAccessedUs));
            writer.WriteKey("last_accessed"); writer.WriteString(
                row.LastAccessedUs ? TInstant::MicroSeconds(row.LastAccessedUs).ToStringUpToSeconds() : "-");
            writer.EndObject();
        }
        writer.EndList();

        WriteLatencyList(writer, "node_latency", NodeLatency, Top);
        WriteLatencyList(writer, "request_latency", RequestLatency, MaxTop);
        WriteLatencyList(writer, "shard_latency", ShardLatency, MaxTop);

        writer.WriteKey("warnings");
        writer.BeginList();
        for (const auto& warning: Warnings) writer.WriteString(warning);
        writer.EndList();
        writer.EndObject();

        NCloud::Reply(ctx, *RequestInfo,
            std::make_unique<TEvRemoteJsonInfoRes>(std::move(out.Str())));
        Die(ctx);
    }

    static void WriteLatencyList(
        NJsonWriter::TBuf& writer,
        TStringBuf name,
        const TVector<TLatencyRow>& rows,
        ui32 limit)
    {
        writer.WriteKey(name);
        writer.BeginList();
        for (ui32 i = 0; i < Min<size_t>(limit, rows.size()); ++i) {
            const auto& row = rows[i];
            writer.BeginObject();
            writer.WriteKey("rank"); writer.WriteULongLong(i + 1);
            writer.WriteKey("shard_id"); writer.WriteString(row.ShardId);
            if (row.NodeId) {
                writer.WriteKey("node_id"); writer.WriteString(row.NodeId);
            }
            if (row.RequestType) {
                writer.WriteKey("request_type"); writer.WriteString(row.RequestType);
            }
            writer.WriteKey("requests"); writer.WriteULongLong(row.Requests);
            writer.WriteKey("avg_decayed_us"); writer.WriteDouble(row.AverageDecayedLatencyUs());
            writer.WriteKey("total_decayed_us"); writer.WriteDouble(row.TotalDecayedLatencyUs);
            writer.WriteKey("total_us"); writer.WriteULongLong(row.TotalLatencyUs);
            WriteTimestamp(writer, row);
            writer.EndObject();
        }
        writer.EndList();
    }

    void ReplyError(const TActorContext& ctx, const NProto::TError& error)
    {
        TStringStream out;
        NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
        writer.BeginObject();
        writer.WriteKey("error");
        writer.WriteString(FormatError(error));
        writer.EndObject();
        NCloud::Reply(ctx, *RequestInfo,
            std::make_unique<TEvRemoteJsonInfoRes>(std::move(out.Str())));
        Die(ctx);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr&,
        const TActorContext& ctx)
    {
        ReplyError(ctx, MakeError(E_REJECTED, "diagnostics request cancelled"));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleHttpInfo_Diagnostics(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (params.Get("getContent") != "1") {
        TStringStream out;
        DumpDiagnosticsPage(out, TabletID());
        NCloud::Reply(ctx, *requestInfo,
            std::make_unique<TEvRemoteHttpInfoRes>(std::move(out.Str())));
        return;
    }

    const ui32 top = Min<ui32>(
        FromStringWithDefault(params.Get("top"), 5), MaxTop);
    const ui32 topNodes = Min<ui32>(
        FromStringWithDefault(params.Get("topNodes"), 10), MaxTop);

    NCloud::Register<TDiagnosticsActor>(
        ctx,
        std::move(requestInfo),
        GetFileSystemId(),
        top,
        topNodes);
}

}   // namespace NCloud::NFileStore::NStorage
