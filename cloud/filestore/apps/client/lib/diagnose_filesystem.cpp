#include "aggregate.h"
#include "command.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/json/json_writer.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiagnoseFilesystemCommand final: public TFileStoreCommand
{
private:
    struct TShardRow
    {
        TString ShardId;
        ui64 CurrentLoad = 0;
        ui32 Suffer = 0;
        ui64 UsedBlocksCount = 0;
        ui64 TotalBlocksCount = 0;
        ui64 UsedNodesCount = 0;
    };

    struct TNodeRow
    {
        TString ShardId;
        ui64 NodeId = 0;
        ui64 RequestCount = 0;
        double AccessScore = 0;
        ui64 LastAccessedTimestampUs = 0;
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

    using TLatencyResult = NAggregation::TResult<TLatency>;

    ui32 TopLoaded;
    TString SortBy;
    ui32 TopAccessed;
    ui32 BatchSize;
    ui32 SlowestNodes;
    ui32 SlowestRequests;
    ui32 SlowestShards;

public:
    TDiagnoseFilesystemCommand()
    {
        Opts.AddLongOption("top-loaded", "number of most loaded shards")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&TopLoaded);
        Opts.AddLongOption("sort-by", "way of sorting")
            .RequiredArgument("STR")
            .Choices({"load"})
            .DefaultValue("load")
            .StoreResult(&SortBy);
        Opts.AddLongOption("top-accessed", "number of most accessed nodes")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&TopAccessed);
        Opts.AddLongOption(
                "slowest-nodes",
                "number of slowest (node + request + shard) tuples")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&SlowestNodes);
        Opts.AddLongOption(
                "slowest-requests",
                "number of slowest (request + shard) tuples")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&SlowestRequests);
        Opts.AddLongOption("slowest-shards", "number of slowest shards")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&SlowestShards);
        Opts.AddLongOption(
                "batch-size",
                "number of concurrent diagnostic requests")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&BatchSize);
    }

private:
    template <typename TRequest, typename TResponse>
    void ExecuteAction(
        const TString& action,
        const TRequest& requestProto,
        TResponse* responseProto)
    {
        TString input;
        google::protobuf::util::MessageToJsonString(requestProto, &input);

        STORAGE_DEBUG("Reading ExecuteAction request");
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction(action);
        request->SetInput(std::move(input));

        STORAGE_DEBUG("Sending ExecuteAction request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(Client->ExecuteAction(
            MakeIntrusive<TCallContext>(FileSystemId, requestId),
            std::move(request)));

        STORAGE_DEBUG("Received ExecuteAction response");

        if (HasError(result)) {
            responseProto->MutableError()->CopyFrom(result.GetError());
            return;
        }

        auto parsed = google::protobuf::util::JsonStringToMessage(
                          result.GetOutput(),
                          responseProto)
                          .ok();

        if (!parsed) {
            responseProto->MutableError()->CopyFrom(MakeError(
                E_BADMSG,
                TStringBuilder() << "failed to parse response json: "
                                 << result.GetOutput()));
        }
    }

    void ProcessShardLoadStats(
        const google::protobuf::RepeatedPtrField<
            NCloud::NFileStore::NProtoPrivate::TShardStats>& shardStats,
        TVector<TShardRow>& shardRows)
    {
        for (const auto& stats: shardStats) {
            shardRows.push_back(
                {stats.GetShardId(),
                 stats.GetCurrentLoad(),
                 stats.GetSuffer(),
                 stats.GetUsedBlocksCount(),
                 stats.GetTotalBlocksCount(),
                 stats.GetUsedNodesCount()});
        }
    }

    void CollectAccessAndLatencyStats(
        const google::protobuf::RepeatedPtrField<
            NCloud::NFileStore::NProtoPrivate::TShardStats>& shardStats,
        TVector<TNodeRow>& accessRows,
        TVector<NAggregation::TRow<TLatency>>& latencyRows)
    {
        for (int batchStart = 0; batchStart < shardStats.size();
             batchStart += BatchSize)
        {
            TVector<NThreading::TFuture<NProto::TExecuteActionResponse>>
                futures;
            const auto batchEnd =
                Min<size_t>(batchStart + BatchSize, shardStats.size());
            futures.reserve(batchEnd - batchStart);

            for (size_t i = batchStart; i < batchEnd; ++i) {
                NProtoPrivate::TGetDiagnosticStatsRequest request;
                request.SetFileSystemId(shardStats[i].GetShardId());
                const ui32 latencyLimit =
                    Max(SlowestNodes, Max(SlowestRequests, SlowestShards));

                const ui32 requestLimit = Max(TopAccessed, latencyLimit);
                request.SetLimit(requestLimit);
                futures.push_back(SendAction("getdiagnosticstats", request));
            }

            for (auto& future: futures) {
                auto result = WaitFor(std::move(future));
                if (HasError(result)) {
                    STORAGE_WARN(
                        "Diagnostic stats request failed: "
                        << FormatError(result.GetError()));
                    continue;
                }

                NProtoPrivate::TGetDiagnosticStatsResponse response;
                if (!google::protobuf::util::JsonStringToMessage(
                         result.GetOutput(),
                         &response)
                         .ok())
                {
                    STORAGE_WARN(
                        "Failed to parse response json: "
                        << FormatError(MakeError(
                               E_BADMSG,
                               TStringBuilder()
                                   << "Failed to parse response json: "
                                   << result.GetOutput())));
                    continue;
                }

                ProcessAccessStats(response.GetNodeStats(), accessRows);
                ProcessLatencyStats(response.GetLatencyStats(), latencyRows);
            }
        }
    }

    template <typename TRequest>
    NThreading::TFuture<NProto::TExecuteActionResponse> SendAction(
        const TString& action,
        const TRequest& requestProto)
    {
        TString input;
        google::protobuf::util::MessageToJsonString(requestProto, &input);

        STORAGE_DEBUG("Reading SendAction request");
        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction(action);
        request->SetInput(std::move(input));
        return Client->ExecuteAction(
            MakeIntrusive<TCallContext>(FileSystemId, GetRequestId(*request)),
            std::move(request));
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
        return std::tie(rhs.CurrentLoad, rhs.Suffer, lhs.ShardId) <
               std::tie(lhs.CurrentLoad, lhs.Suffer, rhs.ShardId);
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
                   lhs.GroupAggregate.LastAccessedTimestampUs) <
               std::tie(lhsLatency, rhs.GroupAggregate.LastAccessedTimestampUs);
    }

    static constexpr TStringBuf ConsoleMagenta = "\033[95m";
    static constexpr TStringBuf ConsoleRed = "\033[91m";
    static constexpr TStringBuf ConsoleEnd = "\033[0m";

    template <typename TValue>
    static TString
    TableCell(const TValue& value, size_t width, bool highlight = false)
    {
        TString text = ToString(value);
        if (text.size() < width) {
            text = TString(width - text.size(), ' ') + text;
        }

        if (highlight) {
            return TStringBuilder() << ConsoleRed << text << ConsoleEnd;
        }

        return text;
    }

    static void PrintTableHeader(TStringBuf title, TStringBuf columns)
    {
        Cout << Endl << ConsoleMagenta << title << ConsoleEnd << Endl;
        Cout << ConsoleMagenta << columns << ConsoleEnd << Endl;
    }

    static void PrintNoData()
    {
        Cout << "(no data)" << Endl;
    }

    void PrintShardTable(const TVector<TShardRow>& rows, size_t limit) const
    {
        PrintTableHeader(
            TStringBuilder() << "Shard stats (top " << limit << ")",
            "#   | Shard        | Load       | Suffer   | Used blocks   | "
            "Total blocks  | Nodes");
        Cout << "--------------------------------------------------------------"
                "------------------"
             << Endl;

        if (!limit) {
            PrintNoData();
            return;
        }

        const auto maxLoad = rows.front().CurrentLoad;
        for (size_t i = 0; i < limit; ++i) {
            const auto& row = rows[i];
            const bool highlight = row.CurrentLoad == maxLoad;
            Cout << TableCell(i + 1, 3) << " | "
                 << TableCell(row.ShardId, 12, highlight) << " | "
                 << TableCell(row.CurrentLoad, 10, highlight) << " | "
                 << TableCell(row.Suffer, 8) << " | "
                 << TableCell(row.UsedBlocksCount, 13) << " | "
                 << TableCell(row.TotalBlocksCount, 13) << " | "
                 << TableCell(row.UsedNodesCount, 5) << Endl;
        }
    }

    void PrintAccessTable(const TVector<TNodeRow>& rows, size_t limit) const
    {
        PrintTableHeader(
            TStringBuilder() << "Node access stats (top " << limit << ")",
            "#   | Shard        | Node                 | Requests   | Access score | "
            "Last accessed");
        Cout << "--------------------------------------------------------------"
                "--------------------"
             << Endl;

        if (!limit) {
            PrintNoData();
            return;
        }

        const auto maxScore = rows.front().AccessScore;
        for (size_t i = 0; i < limit; ++i) {
            const auto& row = rows[i];
            const bool highlight = row.AccessScore == maxScore;
            Cout << TableCell(i + 1, 3) << " | "
                 << TableCell(row.ShardId, 12, highlight) << " | "
                 << TableCell(row.NodeId, 20, highlight) << " | "
                 << TableCell(row.RequestCount, 10) << " | "
                 << TableCell(row.AccessScore, 11, highlight) << " | "
                 << TableCell(
                        TInstant::MicroSeconds(row.LastAccessedTimestampUs)
                            .ToStringUpToSeconds(),
                        13)
                 << Endl;
        }
    }

    void PrintNodeLatencyTable(
        const TVector<TLatencyResult>& rows,
        size_t limit) const
    {
        PrintTableHeader(
            TStringBuilder() << "Node latency stats (top " << limit << ")",
            "#   | Node                 | Shard        | Request type | Avg latency | "
            "Total latency | Requests");
        Cout << "--------------------------------------------------------------"
                "------------------------------"
             << Endl;

        if (!limit) {
            PrintNoData();
            return;
        }

        const auto maxLatency =
            rows.front().GroupAggregate.GetAverageDecayedLatencyUs();
        for (size_t i = 0; i < limit; ++i) {
            const auto& row = rows[i];
            const auto latency =
                row.GroupAggregate.GetAverageDecayedLatencyUs();
            const bool highlight = latency == maxLatency;
            Cout << TableCell(i + 1, 3) << " | "
                 << TableCell(FromString<ui64>(row.Labels[0]), 20, highlight)
                 << " | " << TableCell(row.Labels[1], 12, highlight) << " | "
                 << TableCell(row.Labels[2], 12) << " | "
                 << TableCell(latency, 11, highlight) << " | "
                 << TableCell(row.GroupAggregate.TotalLatencyUs, 13) << " | "
                 << TableCell(row.GroupAggregate.RequestCount, 8) << Endl;
        }
    }

    void PrintRequestLatencyTable(
        const TVector<TLatencyResult>& rows,
        size_t limit) const
    {
        PrintTableHeader(
            TStringBuilder() << "Request latency stats (top " << limit << ")",
            "#   | Shard        | Request type | Avg latency | Total latency | "
            "Requests");
        Cout << "--------------------------------------------------------------"
                "----------"
             << Endl;

        if (!limit) {
            PrintNoData();
            return;
        }

        const auto maxLatency =
            rows.front().GroupAggregate.TotalDecayedLatencyUs;
        for (size_t i = 0; i < limit; ++i) {
            const auto& row = rows[i];
            const bool highlight =
                row.GroupAggregate.TotalDecayedLatencyUs == maxLatency;
            Cout << TableCell(i + 1, 3) << " | "
                 << TableCell(row.Labels[1], 12, highlight) << " | "
                 << TableCell(row.Labels[2], 12) << " | "
                 << TableCell(
                        row.GroupAggregate.GetAverageDecayedLatencyUs(),
                        11)
                 << " | "
                 << TableCell(
                        row.GroupAggregate.TotalDecayedLatencyUs,
                        13,
                        highlight)
                 << " | " << TableCell(row.GroupAggregate.RequestCount, 8)
                 << Endl;
        }
    }

    void PrintShardLatencyTable(
        const TVector<TLatencyResult>& rows,
        size_t limit) const
    {
        PrintTableHeader(
            TStringBuilder() << "Shard latency stats (top " << limit << ")",
            "#   | Shard        | Avg latency | Total latency | Requests");
        Cout << "----------------------------------------------------------"
             << Endl;

        if (!limit) {
            PrintNoData();
            return;
        }

        const auto maxLatency =
            rows.front().GroupAggregate.TotalDecayedLatencyUs;
        for (size_t i = 0; i < limit; ++i) {
            const auto& row = rows[i];
            const bool highlight =
                row.GroupAggregate.TotalDecayedLatencyUs == maxLatency;
            Cout << TableCell(i + 1, 3) << " | "
                 << TableCell(row.Labels[1], 12, highlight) << " | "
                 << TableCell(
                        row.GroupAggregate.GetAverageDecayedLatencyUs(),
                        11)
                 << " | "
                 << TableCell(
                        row.GroupAggregate.TotalDecayedLatencyUs,
                        13,
                        highlight)
                 << " | " << TableCell(row.GroupAggregate.RequestCount, 8)
                 << Endl;
        }
    }

    static NJson::TJsonValue MakeShardJson(const TShardRow& row)
    {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["shard_id"] = row.ShardId;
        result["current_load"] = row.CurrentLoad;
        result["suffer"] = row.Suffer;
        result["used_blocks_count"] = row.UsedBlocksCount;
        result["total_blocks_count"] = row.TotalBlocksCount;
        result["used_nodes_count"] = row.UsedNodesCount;
        return result;
    }

    static NJson::TJsonValue MakeNodeJson(const TNodeRow& row)
    {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["shard_id"] = row.ShardId;
        result["node_id"] = row.NodeId;
        result["request_count"] = row.RequestCount;
        result["access_score"] = row.AccessScore;
        result["last_accessed_timestamp_us"] = row.LastAccessedTimestampUs;
        result["last_accessed"] =
            TInstant::MicroSeconds(row.LastAccessedTimestampUs)
                .ToStringUpToSeconds();
        return result;
    }

    static NJson::TJsonValue MakeNodeLatencyJson(const TLatencyResult& row)
    {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["node_id"] = FromString<ui64>(row.Labels[0]);
        result["shard_id"] = row.Labels[1];
        result["request_type"] = row.Labels[2];
        result["avg_latency_decayed"] =
            row.GroupAggregate.GetAverageDecayedLatencyUs();
        result["total_latency"] = row.GroupAggregate.TotalLatencyUs;
        result["request_count"] = row.GroupAggregate.RequestCount;
        result["last_timestamp_us"] =
            row.GroupAggregate.LastAccessedTimestampUs;
        return result;
    }

    static NJson::TJsonValue MakeRequestLatencyJson(const TLatencyResult& row)
    {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["shard_id"] = row.Labels[1];
        result["request_type"] = row.Labels[2];
        result["avg_node_latency"] =
            row.GroupAggregate.GetAverageDecayedLatencyUs();
        result["total_node_latency"] = row.GroupAggregate.TotalDecayedLatencyUs;
        result["request_count"] = row.GroupAggregate.RequestCount;
        result["last_timestamp_us"] =
            row.GroupAggregate.LastAccessedTimestampUs;
        return result;
    }

    static NJson::TJsonValue MakeShardLatencyJson(const TLatencyResult& row)
    {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["shard_id"] = row.Labels[1];
        result["avg_node_latency"] =
            row.GroupAggregate.GetAverageDecayedLatencyUs();
        result["total_node_latency"] = row.GroupAggregate.TotalDecayedLatencyUs;
        result["request_count"] = row.GroupAggregate.RequestCount;
        result["last_timestamp_us"] =
            row.GroupAggregate.LastAccessedTimestampUs;
        return result;
    }

    template <typename TRow, typename TJsonFactory>
    static NJson::TJsonValue MakeJsonArray(
        const TVector<TRow>& rows,
        size_t limit,
        TJsonFactory jsonFactory)
    {
        NJson::TJsonValue result(NJson::JSON_ARRAY);
        const auto count = Min(limit, rows.size());
        for (size_t i = 0; i < count; ++i) {
            result.AppendValue(jsonFactory(rows[i]));
        }
        return result;
    }

    NJson::TJsonValue MakeJsonResult(
        const TVector<TShardRow>& rows,
        const TVector<TNodeRow>& accessRows,
        const TVector<TLatencyResult>& nodeLatencyRows,
        const TVector<TLatencyResult>& requestLatencyRows,
        const TVector<TLatencyResult>& shardLatencyRows,
        size_t limit,
        size_t nodeLimit,
        size_t nodeLatencyLimit,
        size_t requestLatencyLimit,
        size_t shardLatencyLimit) const
    {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["filesystem_id"] = FileSystemId;
        result["shard_count"] = rows.size();
        result["shards"] = MakeJsonArray(rows, limit, MakeShardJson);
        result["nodes"] = MakeJsonArray(accessRows, nodeLimit, MakeNodeJson);
        result["node_latency"] = MakeJsonArray(
            nodeLatencyRows,
            nodeLatencyLimit,
            MakeNodeLatencyJson);
        result["request_latency"] = MakeJsonArray(
            requestLatencyRows,
            requestLatencyLimit,
            MakeRequestLatencyJson);
        result["shard_latency"] = MakeJsonArray(
            shardLatencyRows,
            shardLatencyLimit,
            MakeShardLatencyJson);
        return result;
    }

public:
    bool Execute() override
    {
        NProtoPrivate::TGetStorageStatsRequest request;
        request.SetFileSystemId(FileSystemId);
        request.SetCacheTTL(0);   // disable caching
        request.SetMode(NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
        NProtoPrivate::TGetStorageStatsResponse response;
        ExecuteAction("getstoragestats", request, &response);
        CheckResponse(response);

        TVector<TShardRow> rows;
        TVector<TNodeRow> accessRows;
        TVector<NAggregation::TRow<TLatency>> latencyRows;

        const auto& stats = response.GetStats();
        rows.reserve(stats.ShardStatsSize());
        const auto& shardStats = stats.GetShardStats();

        ProcessShardLoadStats(shardStats, rows);
        CollectAccessAndLatencyStats(shardStats, accessRows, latencyRows);

        auto latencyAggregates = NAggregation::Aggregate(latencyRows);

        TVector<TLatencyResult> nodeLatencyRows;
        TVector<TLatencyResult> requestLatencyRows;
        TVector<TLatencyResult> shardLatencyRows;

        GroupLatencyCombinations(
            latencyAggregates,
            nodeLatencyRows,
            requestLatencyRows,
            shardLatencyRows);

        Sort(rows, CompareShardRows);
        Sort(accessRows, CompareAccessRows);
        Sort(nodeLatencyRows, CompareNodeLatencyRows);
        Sort(requestLatencyRows, CompareTotalLatency);
        Sort(shardLatencyRows, CompareTotalLatency);

        const size_t limit = Min<size_t>(TopLoaded, rows.size());
        const size_t nodeLimit = Min<size_t>(TopAccessed, accessRows.size());
        const size_t nodeLatencyLimit =
            Min<size_t>(SlowestNodes, nodeLatencyRows.size());
        const size_t requestLatencyLimit =
            Min<size_t>(SlowestRequests, requestLatencyRows.size());
        const size_t shardLatencyLimit =
            Min<size_t>(SlowestShards, shardLatencyRows.size());

        if (JsonOutput) {
            auto resultJson = MakeJsonResult(
                rows,
                accessRows,
                nodeLatencyRows,
                requestLatencyRows,
                shardLatencyRows,
                limit,
                nodeLimit,
                nodeLatencyLimit,
                requestLatencyLimit,
                shardLatencyLimit);
            NJson::WriteJson(&Cout, &resultJson, false, true, true);

            return true;
        }

        Cout << "Filesystem: " << FileSystemId << Endl
             << "Shard count: " << rows.size() << Endl;

        PrintShardTable(rows, limit);
        PrintAccessTable(accessRows, nodeLimit);
        PrintNodeLatencyTable(nodeLatencyRows, nodeLatencyLimit);
        PrintRequestLatencyTable(requestLatencyRows, requestLatencyLimit);
        PrintShardLatencyTable(shardLatencyRows, shardLatencyLimit);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDiagnoseFilesystemCommand()
{
    return std::make_shared<TDiagnoseFilesystemCommand>();
}

}   // namespace NCloud::NFileStore::NClient
