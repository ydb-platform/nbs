#include "command.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/set.h>

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

    struct TNodeLatencyRow
    {
        TString ShardId;
        TString RequestType = GetFileStoreRequestName(EFileStoreRequest::MAX);
        ui64 NodeId = 0;
        ui64 RequestCount = 0;
        ui64 TotalLatencyMs = 0;
        double AverageLatencyDecayedMs = 0;
        ui64 LastAccessedTimestampUs = 0;
    };

    struct TRequestLatencyRow
    {
        TString ShardId;
        TString RequestType = GetFileStoreRequestName(EFileStoreRequest::MAX);
        ui64 RequestCount = 0;
        double TotalNodeLatencyMs = 0;
        double AverageNodeLatencyMs = 0;
        ui64 LastUsedTimestampUs = 0;
    };

    struct TShardLatencyRow
    {
        TString ShardId;
        ui64 RequestCount = 0;
        double TotalNodeLatencyMs = 0;
        double AverageNodeLatencyMs = 0;
        ui64 LastUsedTimestampUs = 0;
    };

    struct TRequestLatencyComparator
    {
        bool operator()(
            const TRequestLatencyRow& lhs,
            const TRequestLatencyRow& rhs) const
        {
            // TotalNodeLatencyMs DESC, LastUsedTimestamp DESC
            return std::tie(rhs.TotalNodeLatencyMs, rhs.LastUsedTimestampUs) <
                   std::tie(lhs.TotalNodeLatencyMs, lhs.LastUsedTimestampUs);
        }
    };

    ui32 Top;
    TString SortBy;

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
        TVector<TNodeLatencyRow> nodeLatencyRows;

        using RequestKey = std::pair<TString, TString>;
        TSet<TRequestLatencyRow, TRequestLatencyComparator> requestRanking;
        THashMap<RequestKey, TSet<TRequestLatencyRow>::iterator>
            request2Latency;

        THashMap<TString, TShardLatencyRow> shard2Latency;
        TVector<TShardLatencyRow> shardLatencyRows;

        const auto& stats = response.GetStats();
        rows.reserve(stats.ShardStatsSize());
        shardLatencyRows.reserve(stats.ShardStatsSize());

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

        for (const auto& latencyStats: stats.GetLatencyStats()) {
            TNodeLatencyRow row;
            row.ShardId = latencyStats.GetShardId();
            row.RequestType = latencyStats.GetRequestType();
            row.NodeId = latencyStats.GetNodeId();
            row.RequestCount = latencyStats.GetRequestCount();
            row.TotalLatencyMs = latencyStats.GetTotalLatencyMs();
            row.AverageLatencyDecayedMs =
                latencyStats.GetAverageLatencyDecayedMs();
            row.LastAccessedTimestampUs =
                latencyStats.GetLastAccessedTimestampUs();
            nodeLatencyRows.push_back(std::move(row));

            RequestKey key = {
                latencyStats.GetShardId(),
                latencyStats.GetRequestType()};
            auto it = request2Latency.find(key);
            TRequestLatencyRow latencyRow;
            if (it != request2Latency.end()) {
                latencyRow = *it->second;
                requestRanking.erase(it->second);
            } else {
                latencyRow.RequestType = latencyStats.GetRequestType();
                latencyRow.ShardId = latencyStats.GetShardId();
            }
            latencyRow.RequestCount += latencyStats.GetRequestCount();
            latencyRow.LastUsedTimestampUs =
                Max(latencyRow.LastUsedTimestampUs,
                    latencyStats.GetLastAccessedTimestampUs());
            latencyRow.TotalNodeLatencyMs +=
                latencyStats.GetAverageLatencyDecayedMs() *
                latencyStats.GetRequestCount();
            latencyRow.AverageNodeLatencyMs =
                latencyRow.TotalNodeLatencyMs / latencyRow.RequestCount;

            auto [newRequestLatencyIt, inserted] =
                requestRanking.insert(latencyRow);
            Y_ABORT_UNLESS(inserted);
            request2Latency[key] = newRequestLatencyIt;

            auto shardLatencyIt = shard2Latency.find(latencyStats.GetShardId());
            TShardLatencyRow shardLatencyRow;
            if (shardLatencyIt != shard2Latency.end()) {
                shardLatencyRow = shardLatencyIt->second;
            } else {
                shardLatencyRow.ShardId = latencyStats.GetShardId();
            }
            shardLatencyRow.RequestCount += latencyStats.GetRequestCount();
            shardLatencyRow.LastUsedTimestampUs =
                Max(shardLatencyRow.LastUsedTimestampUs,
                    latencyStats.GetLastAccessedTimestampUs());
            shardLatencyRow.TotalNodeLatencyMs +=
                latencyStats.GetAverageLatencyDecayedMs() *
                latencyStats.GetRequestCount();
            shardLatencyRow.AverageNodeLatencyMs =
                shardLatencyRow.TotalNodeLatencyMs / latencyRow.RequestCount;

            shard2Latency[latencyStats.GetShardId()] = shardLatencyRow;
        }

        for (auto const& shardIt: shard2Latency) {
            shardLatencyRows.push_back(shardIt.second);
        }

        Sort(
            shardLatencyRows,
            [](const TShardLatencyRow& l, const TShardLatencyRow& r)
            {
                // AverageNodeLatencyMs DESC, LastUsedTimestampUs DESC
                return std::tie(r.TotalNodeLatencyMs, r.LastUsedTimestampUs) <
                       std::tie(l.TotalNodeLatencyMs, l.LastUsedTimestampUs);
            });

        Sort(
            nodeLatencyRows,
            [](const TNodeLatencyRow& l, TNodeLatencyRow& r)
            {
                // AverageNodeLatency DESC, NodeId ASC
                return std::tie(r.AverageLatencyDecayedMs, l.NodeId) <
                       std::tie(l.AverageLatencyDecayedMs, r.NodeId);
            });

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
                nodeLatencyJson["node_id"] = nodeLatencyRow.NodeId;
                nodeLatencyJson["request_type"] = nodeLatencyRow.RequestType;
                nodeLatencyJson["avg_latency_decayed"] =
                    nodeLatencyRow.AverageLatencyDecayedMs;
                nodeLatencyJson["total_latency"] =
                    nodeLatencyRow.TotalLatencyMs;
                nodeLatencyJson["request_count"] = nodeLatencyRow.RequestCount;
                nodeLatencyJson["last_timestamp_us"] =
                    nodeLatencyRow.LastAccessedTimestampUs;
                nodeLatencyJson["shard_id"] = nodeLatencyRow.ShardId;

                nodesLatencyJson.AppendValue(std::move(nodeLatencyJson));
            }

            resultJson["node_latency"] = std::move(nodesLatencyJson);

            for (const auto& requestLatencyRow: requestRanking) {
                NJson::TJsonValue requestLatencyJson(NJson::JSON_MAP);
                requestLatencyJson["request_type"] =
                    requestLatencyRow.RequestType;
                requestLatencyJson["avg_node_latency"] =
                    requestLatencyRow.AverageNodeLatencyMs;
                requestLatencyJson["total_node_latency"] =
                    requestLatencyRow.TotalNodeLatencyMs;
                requestLatencyJson["request_count"] =
                    requestLatencyRow.RequestCount;
                requestLatencyJson["last_timestamp_us"] =
                    requestLatencyRow.LastUsedTimestampUs;
                requestLatencyJson["shard_id"] = requestLatencyRow.ShardId;

                requestsLatencyJson.AppendValue(std::move(requestLatencyJson));
            }

            resultJson["request_latency"] = std::move(requestsLatencyJson);

            for (const auto& shardLatencyRow: shardLatencyRows) {
                NJson::TJsonValue shardLatencyJson(NJson::JSON_MAP);
                shardLatencyJson["avg_node_latency"] =
                    shardLatencyRow.AverageNodeLatencyMs;
                shardLatencyJson["shard_id"] = shardLatencyRow.ShardId;
                shardLatencyJson["last_timestamp_us"] =
                    shardLatencyRow.LastUsedTimestampUs;
                shardLatencyJson["total_node_latency"] =
                    shardLatencyRow.TotalNodeLatencyMs;
                shardLatencyJson["request_count"] =
                    shardLatencyRow.RequestCount;

                shardsLatencyJson.AppendValue(std::move(shardLatencyJson));
            }

            resultJson["shard_latency"] = std::move(shardsLatencyJson);

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
