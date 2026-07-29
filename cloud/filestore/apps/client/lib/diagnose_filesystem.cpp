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
        ui64 TotalLatencyMs = 0;
        double TotalDecayedLatencyMs = 0;
        ui64 LastAccessedTimestampUs = 0;

        void Add(const TLatency& other)
        {
            RequestCount += other.RequestCount;
            TotalLatencyMs += other.TotalLatencyMs;
            TotalDecayedLatencyMs += other.TotalDecayedLatencyMs;
            LastAccessedTimestampUs =
                Max(LastAccessedTimestampUs, other.LastAccessedTimestampUs);
        }

        double GetAverageDecayedLatencyMs() const
        {
            return RequestCount ? TotalDecayedLatencyMs / RequestCount : 0;
        }
    };

    ui32 Top;
    TString SortBy;
    ui32 TopNodes;

public:
    TDiagnoseFilesystemCommand()
    {
        Opts.AddLongOption("top", "number of most loaded shards")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&Top);
        Opts.AddLongOption("sort-by", "way of sorting")
            .RequiredArgument("STR")
            .Choices({"load"})
            .DefaultValue("load")
            .StoreResult(&SortBy);
        Opts.AddLongOption("top-nodes", "number of most accessed nodes")
            .RequiredArgument("NUM")
            .DefaultValue(10)
            .StoreResult(&TopNodes);
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
        TVector<NAggregation::TRow<TLatency>> latencyRows;
        TVector<TNodeRow> nodeRows;

        const auto& stats = response.GetStats();
        rows.reserve(stats.ShardStatsSize());
        latencyRows.reserve(stats.LatencyStatsSize());

        for (const auto& shardStats: stats.GetShardStats()) {
            TShardRow row;
            row.ShardId = shardStats.GetShardId();
            row.CurrentLoad = shardStats.GetCurrentLoad();
            row.Suffer = shardStats.GetSuffer();
            row.UsedBlocksCount = shardStats.GetUsedBlocksCount();
            row.TotalBlocksCount = shardStats.GetTotalBlocksCount();
            row.UsedNodesCount = shardStats.GetUsedNodesCount();
            rows.push_back(std::move(row));
        }
        for (const auto& nodeStats: stats.GetNodeStats()) {
            nodeRows.push_back(
                {nodeStats.GetShardId(),
                 nodeStats.GetNodeId(),
                 nodeStats.GetRequestCount(),
                 nodeStats.GetAccessScore(),
                 nodeStats.GetLastAccessedTimestampUs()});
        }

        for (const auto& latencyStats: stats.GetLatencyStats()) {
            NAggregation::TRow<TLatency> row;
            row.Labels = {
                ToString(latencyStats.GetNodeId()),
                latencyStats.GetShardId(),
                latencyStats.GetRequestType()};
            row.Data.RequestCount = latencyStats.GetRequestCount();
            row.Data.TotalLatencyMs = latencyStats.GetTotalLatencyMs();
            row.Data.TotalDecayedLatencyMs =
                latencyStats.GetAverageLatencyDecayedMs() *
                latencyStats.GetRequestCount();
            row.Data.LastAccessedTimestampUs =
                latencyStats.GetLastAccessedTimestampUs();
            latencyRows.push_back(std::move(row));
        }

        auto latencyAggregates = NAggregation::Aggregate(latencyRows);

        using TLatencyResult = NAggregation::TResult<TLatency>;
        TVector<TLatencyResult> nodeLatencyRows;
        TVector<TLatencyResult> requestLatencyRows;
        TVector<TLatencyResult> shardLatencyRows;

        for (auto& aggregate: latencyAggregates) {
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

        Sort(
            nodeLatencyRows,
            [](const TLatencyResult& l, const TLatencyResult& r)
            {
                const auto lAverage =
                    l.GroupAggregate.GetAverageDecayedLatencyMs();
                const auto rAverage =
                    r.GroupAggregate.GetAverageDecayedLatencyMs();
                const auto lNodeId = FromString<ui64>(l.Labels[0]);
                const auto rNodeId = FromString<ui64>(r.Labels[0]);
                return std::tie(rAverage, lNodeId) <
                       std::tie(lAverage, rNodeId);
            });

        Sort(
            nodeRows,
            [](const TNodeRow& l, const TNodeRow& r)
            {
                if (l.AccessScore != r.AccessScore) {
                    return l.AccessScore > r.AccessScore;
                }

                if (l.ShardId != r.ShardId) {
                    return l.ShardId < r.ShardId;
                }

                return l.NodeId < r.NodeId;
            });

        auto totalLatencyComparator =
            [](const TLatencyResult& l, const TLatencyResult& r)
        {
            return std::tie(
                       r.GroupAggregate.TotalDecayedLatencyMs,
                       r.GroupAggregate.LastAccessedTimestampUs) <
                   std::tie(
                       l.GroupAggregate.TotalDecayedLatencyMs,
                       l.GroupAggregate.LastAccessedTimestampUs);
        };

        Sort(requestLatencyRows, totalLatencyComparator);
        Sort(shardLatencyRows, totalLatencyComparator);

        Sort(
            rows,
            [](const TShardRow& l, const TShardRow& r)
            {
                if (l.CurrentLoad != r.CurrentLoad) {
                    return l.CurrentLoad > r.CurrentLoad;
                }

                if (l.Suffer != r.Suffer) {
                    return l.Suffer > r.Suffer;
                }

                return l.ShardId < r.ShardId;
            });

        const size_t limit = Min<size_t>(Top, rows.size());
        const size_t nodeLatencyLimit =
            Min<size_t>(Top, nodeLatencyRows.size());
        const size_t nodeLimit = Min<size_t>(TopNodes, nodeRows.size());

        if (JsonOutput) {
            NJson::TJsonValue resultJson(NJson::JSON_MAP);
            NJson::TJsonValue shardsJson(NJson::JSON_ARRAY);
            NJson::TJsonValue nodesLatencyJson(NJson::JSON_ARRAY);
            NJson::TJsonValue requestsLatencyJson(NJson::JSON_ARRAY);
            NJson::TJsonValue shardsLatencyJson(NJson::JSON_ARRAY);

            resultJson["filesystem_id"] = FileSystemId;
            resultJson["shard_count"] = rows.size();

            for (size_t i = 0; i < limit; ++i) {
                const auto& row = rows[i];

                NJson::TJsonValue shardJson(NJson::JSON_MAP);
                shardJson["shard_id"] = row.ShardId;
                shardJson["current_load"] = row.CurrentLoad;
                shardJson["suffer"] = row.Suffer;
                shardJson["used_blocks_count"] = row.UsedBlocksCount;
                shardJson["total_blocks_count"] = row.TotalBlocksCount;
                shardJson["used_nodes_count"] = row.UsedNodesCount;

                shardsJson.AppendValue(std::move(shardJson));
            }

            resultJson["shards"] = std::move(shardsJson);

            for (size_t i = 0; i < nodeLatencyLimit; ++i) {
                const auto& nodeLatencyRow = nodeLatencyRows[i];
                NJson::TJsonValue nodeLatencyJson(NJson::JSON_MAP);
                nodeLatencyJson["node_id"] =
                    FromString<ui64>(nodeLatencyRow.Labels[0]);
                nodeLatencyJson["request_type"] = nodeLatencyRow.Labels[2];
                nodeLatencyJson["avg_latency_decayed"] =
                    nodeLatencyRow.GroupAggregate.GetAverageDecayedLatencyMs();
                nodeLatencyJson["total_latency"] =
                    nodeLatencyRow.GroupAggregate.TotalLatencyMs;
                nodeLatencyJson["request_count"] =
                    nodeLatencyRow.GroupAggregate.RequestCount;
                nodeLatencyJson["last_timestamp_us"] =
                    nodeLatencyRow.GroupAggregate.LastAccessedTimestampUs;
                nodeLatencyJson["shard_id"] = nodeLatencyRow.Labels[1];

                nodesLatencyJson.AppendValue(std::move(nodeLatencyJson));
            }

            resultJson["node_latency"] = std::move(nodesLatencyJson);

            for (const auto& requestLatencyRow: requestLatencyRows) {
                NJson::TJsonValue requestLatencyJson(NJson::JSON_MAP);
                requestLatencyJson["request_type"] =
                    requestLatencyRow.Labels[2];
                requestLatencyJson["avg_node_latency"] =
                    requestLatencyRow.GroupAggregate
                        .GetAverageDecayedLatencyMs();
                requestLatencyJson["total_node_latency"] =
                    requestLatencyRow.GroupAggregate.TotalDecayedLatencyMs;
                requestLatencyJson["request_count"] =
                    requestLatencyRow.GroupAggregate.RequestCount;
                requestLatencyJson["last_timestamp_us"] =
                    requestLatencyRow.GroupAggregate.LastAccessedTimestampUs;
                requestLatencyJson["shard_id"] = requestLatencyRow.Labels[1];

                requestsLatencyJson.AppendValue(std::move(requestLatencyJson));
            }

            resultJson["request_latency"] = std::move(requestsLatencyJson);

            for (const auto& shardLatencyRow: shardLatencyRows) {
                NJson::TJsonValue shardLatencyJson(NJson::JSON_MAP);
                shardLatencyJson["avg_node_latency"] =
                    shardLatencyRow.GroupAggregate.GetAverageDecayedLatencyMs();
                shardLatencyJson["shard_id"] = shardLatencyRow.Labels[1];
                shardLatencyJson["last_timestamp_us"] =
                    shardLatencyRow.GroupAggregate.LastAccessedTimestampUs;
                shardLatencyJson["total_node_latency"] =
                    shardLatencyRow.GroupAggregate.TotalDecayedLatencyMs;
                shardLatencyJson["request_count"] =
                    shardLatencyRow.GroupAggregate.RequestCount;

                shardsLatencyJson.AppendValue(std::move(shardLatencyJson));
            }

            resultJson["shard_latency"] = std::move(shardsLatencyJson);

            NJson::TJsonValue nodesJson(NJson::JSON_ARRAY);

            for (size_t i = 0; i < nodeLimit; ++i) {
                const auto& node = nodeRows[i];

                NJson::TJsonValue nodeJson(NJson::JSON_MAP);
                nodeJson["shard_id"] = node.ShardId;
                nodeJson["node_id"] = node.NodeId;
                nodeJson["request_count"] = node.RequestCount;
                nodeJson["access_score"] = node.AccessScore;
                nodeJson["last_accessed_timestamp_us"] =
                    node.LastAccessedTimestampUs;

                nodesJson.AppendValue(std::move(nodeJson));
            }

            resultJson["nodes"] = std::move(nodesJson);
            NJson::WriteJson(&Cout, &resultJson, false, true, true);

            return true;
        }

        Cout << "Filesystem: " << FileSystemId << Endl;
        Cout << "Shard count: " << rows.size() << Endl;
        Cout << "Top loaded shards:" << Endl;

        for (size_t i = 0; i < limit; ++i) {
            const auto& row = rows[i];
            Cout << i + 1 << ". " << row.ShardId << "  load=" << row.CurrentLoad
                 << "  suffer=" << row.Suffer
                 << "  blocks=" << row.UsedBlocksCount << "/"
                 << row.TotalBlocksCount << "  nodes=" << row.UsedNodesCount
                 << Endl;
        }

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
