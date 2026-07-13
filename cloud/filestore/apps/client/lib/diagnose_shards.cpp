#include "command.h"

#include <library/cpp/json/json_writer.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDiagnoseShardsCommand final
    : public TFileStoreCommand
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

    ui32 Top = 10;
    TString SortBy = "load";

public:
    TDiagnoseShardsCommand()
    {
        Opts.AddLongOption("top", "number of most loaded shards")
            .RequiredArgument("NUM")
            .StoreResult(&Top);
        Opts.AddLongOption("sort-by", "way of sorting")
            .RequiredArgument("STR")
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
            responseProto).ok();

        if (!parsed) {
            responseProto->MutableError()->CopyFrom(MakeError(
                E_FAIL,
                TStringBuilder() << "failed to parse response json: "
                    << result.GetOutput()));
        }
    }

public:
    bool Execute() override
    {
        if (SortBy != "load") {
            ythrow yexception() << "unsupported --sort-by: " << SortBy;
        }

        NProtoPrivate::TGetStorageStatsRequest request;
        request.SetFileSystemId(FileSystemId);
        request.SetCacheTTL(0); // disable caching
        request.SetMode(NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
        NProtoPrivate::TGetStorageStatsResponse response;
        ExecuteAction("getstoragestats", request, &response);
        CheckResponse(response);

        TVector<TShardRow> rows;

        const auto& stats = response.GetStats();
        rows.reserve(stats.ShardStatsSize());

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

        Sort(rows, [] (const TShardRow& l, const TShardRow& r) {
            if (l.CurrentLoad != r.CurrentLoad) {
                return l.CurrentLoad > r.CurrentLoad;
            }

            if (l.Suffer != r.Suffer) {
                return l.Suffer > r.Suffer;
            }

            return l.ShardId < r.ShardId;
        });

        const size_t limit = Min<size_t>(Top, rows.size());

        if (JsonOutput) {
            NJson::TJsonValue resultJson(NJson::JSON_MAP);
            NJson::TJsonValue shardsJson(NJson::JSON_ARRAY);

            resultJson["filesystem_id"] = FileSystemId;
            resultJson["shard_count"] = static_cast<ui64>(rows.size());

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
            NJson::WriteJson(&Cout, &resultJson, false, true, true);

            return true;
        }

        Cout << "Filesystem: " << FileSystemId << Endl;
        Cout << "Shard count: " << rows.size() << Endl;
        Cout << "Top loaded shards:" << Endl;

        for (size_t i = 0; i < limit; ++i) {
            const auto& row = rows[i];
            Cout << i + 1 << ". "
                << row.ShardId
                << "  load=" << row.CurrentLoad
                << "  suffer=" << row.Suffer
                << "  blocks=" << row.UsedBlocksCount
                << "/" << row.TotalBlocksCount
                << "  nodes=" << row.UsedNodesCount
                << Endl;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDiagnoseShardsCommand()
{
    return std::make_shared<TDiagnoseShardsCommand>();
}
  
}   // namespace NCloud::NFileStore::NClient
