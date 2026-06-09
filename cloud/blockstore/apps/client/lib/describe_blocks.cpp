#include "describe_blocks.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/core/base/logoblob.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <google/protobuf/util/json_util.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlocksCommand final
    : public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui32 BlocksCount = 0;
    TString CheckpointId;

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
            NPrivateProto::TDescribeBlocksRequest requestProto;
            requestProto.SetDiskId(DiskId);
            requestProto.SetStartIndex(StartIndex);
            requestProto.SetBlocksCount(BlocksCount);
            requestProto.SetIndexOnly(true);
            if (CheckpointId) {
                requestProto.SetCheckpointId(CheckpointId);
            }

            TString input;
            google::protobuf::util::MessageToJsonString(requestProto, &input);
            request->SetAction("describeblocks");
            request->SetInput(std::move(input));
        }

        STORAGE_DEBUG("Sending DescribeBlocks request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribeBlocks response");

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        if (Proto) {
            NPrivateProto::TDescribeBlocksResponse responseProto;
            auto parsed = google::protobuf::util::JsonStringToMessage(
                result.GetOutput(),
                &responseProto).ok();

            if (!parsed) {
                auto error = MakeError(
                    E_BADMSG,
                    TStringBuilder() << "failed to parse response json: "
                        << result.GetOutput());
                output << FormatError(error) << Endl;
                return false;
            }
            SerializeToTextFormat(responseProto, output);
            return true;
        }

        output << result.GetOutput() << Endl;
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
