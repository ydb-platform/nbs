#include "describe_blocks.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/core/base/logoblob.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString FormatBlobId(const NJson::TJsonValue& blobIdJson)
{
    if (!blobIdJson.IsDefined() || blobIdJson.IsNull()) {
        return {};
    }
    const ui64 raw1 = blobIdJson["rawX1"].GetUIntegerRobust();
    const ui64 raw2 = blobIdJson["rawX2"].GetUIntegerRobust();
    const ui64 raw3 = blobIdJson["rawX3"].GetUIntegerRobust();
    if (!raw1 && !raw2 && !raw3) {
        return {};
    }
    return NKikimr::TLogoBlobID(raw1, raw2, raw3).ToString();
}

struct TBlockRange
{
    ui32 Start = 0;
    ui32 End = 0;
    TString Description;
};

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlocksCommand final
    : public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui32 BlocksCount = 0;
    TString CheckpointId;
    bool PrettyPrint = false;

public:
    explicit TDescribeBlocksCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&StartIndex);

        Opts.AddLongOption("blocks-count", "number of blocks to describe")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("checkpoint-id", "checkpoint identifier")
            .RequiredArgument("STR")
            .StoreResult(&CheckpointId);

        Opts.AddLongOption("pretty-print", "format output as sorted block ranges")
            .StoreTrue(&PrettyPrint);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        auto request = std::make_shared<NProto::TExecuteActionRequest>();

        if (!Proto && !CheckOpts()) {
            return false;
        }

        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            NJson::TJsonValue jsonInput;
            jsonInput["DiskId"] = DiskId;
            jsonInput["StartIndex"] = StartIndex;
            jsonInput["BlocksCount"] = BlocksCount;
            jsonInput["IndexOnly"] = true;
            if (CheckpointId) {
                jsonInput["CheckpointId"] = CheckpointId;
            }

            request->SetAction("describeblocks");
            request->SetInput(NJson::WriteJson(jsonInput, false));
        }

        STORAGE_DEBUG("Sending DescribeBlocks request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribeBlocks response");

        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        if (!PrettyPrint) {
            output << result.GetOutput() << Endl;
            return true;
        }

        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(result.GetOutput(), &json, false)) {
            output << "Failed to parse response JSON" << Endl;
            return false;
        }

        TVector<TBlockRange> ranges;

        const auto& freshRanges = json["freshBlockRanges"];
        if (freshRanges.IsArray()) {
            for (const auto& item : freshRanges.GetArray()) {
                const ui32 start = item["startIndex"].GetUIntegerRobust();
                const ui32 count = item["blocksCount"].GetUIntegerRobust();
                TStringBuilder desc;
                desc << "fresh";
                const TString blobId = FormatBlobId(item["blobId"]);
                if (blobId) {
                    desc << "  blobId=" << blobId;
                }
                ranges.push_back({start, start + count - 1, desc});
            }
        }

        const auto& blobPieces = json["blobPieces"];
        if (blobPieces.IsArray()) {
            for (const auto& piece : blobPieces.GetArray()) {
                const TString blobId = FormatBlobId(piece["blobId"]);
                // proto3 JSON encodes BSGroupId as "bSGroupId"
                const ui32 bsGroup = piece["bSGroupId"].GetUIntegerRobust();
                const auto& pieceRanges = piece["ranges"];
                if (!pieceRanges.IsArray()) {
                    continue;
                }
                for (const auto& r : pieceRanges.GetArray()) {
                    const ui32 blockIndex  = r["blockIndex"].GetUIntegerRobust();
                    const ui32 blocksCount = r["blocksCount"].GetUIntegerRobust();
                    const ui32 blobOffset  = r["blobOffset"].GetUIntegerRobust();
                    TStringBuilder desc;
                    desc << "blob=" << blobId
                         << "  bsGroup=" << bsGroup
                         << "  offset=" << blobOffset;
                    ranges.push_back({
                        blockIndex,
                        blockIndex + blocksCount - 1,
                        desc});
                }
            }
        }

        Sort(ranges, [](const TBlockRange& a, const TBlockRange& b) {
            return a.Start < b.Start;
        });

        for (const auto& r : ranges) {
            output << "[" << r.Start << ", " << r.End << "]  " << r.Description << Endl;
        }

        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!ParseResultPtr->FindLongOptParseResult("disk-id")) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }
        if (!ParseResultPtr->FindLongOptParseResult("blocks-count")) {
            STORAGE_ERROR("Blocks count is required");
            return false;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeBlocksCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDescribeBlocksCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
