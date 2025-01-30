#include "check_range.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeCommand final: public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex;
    ui64 BlocksCount;
    ui64 BlocksPerRequest = 0;
    bool ShowSuccess = false;

public:
    TCheckRangeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&StartIndex);

        Opts.AddLongOption("blocks-count", "number of blocks to check")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("blocks-per-request", "blocks per request")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksPerRequest);

        Opts.AddLongOption(
                "show-success",
                "show logs for the intervals where request was successfully "
                "completed")
            .RequiredArgument("BOOL")
            .StoreResult(&ShowSuccess);
    }

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        auto& output = GetOutputStream();

        STORAGE_DEBUG("Handling CheckRange");

        ui32 errorCount = 0;

        ui64 remainingBlocks = BlocksCount;
        ui64 currentBlockIndex = StartIndex;
        if (BlocksPerRequest <= 0) {
            BlocksPerRequest = 1024;
        }

        while (remainingBlocks > 0) {

            ui32 blocksInThisRequest =
                std::min(remainingBlocks, BlocksPerRequest);

            auto request = std::make_shared<NProto::TCheckRangeRequest>();
            request->SetDiskId(DiskId);
            request->SetStartIndex(currentBlockIndex);
            request->SetBlocksCount(blocksInThisRequest);

            STORAGE_DEBUG("Sending CheckRange request");
            const auto requestId = GetRequestId(*request);
            auto result = WaitFor(ClientEndpoint->CheckRange(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)));

            STORAGE_DEBUG("Received CheckRange response");
            if (Proto) {
                SerializeToTextFormat(result, output);
                return true;
            }

            if (HasError(result)) {
                if (result.GetError().GetCode() == E_ARGUMENT) {
                    output << "E_ARGUMENT error : "
                           << FormatError(result.GetError()) << Endl;
                    break;
                } else {
                    output << "Error in range [" << currentBlockIndex << ", "
                           << (currentBlockIndex + blocksInThisRequest - 1)
                           << "]: " << FormatError(result.GetError()) << Endl;
                    errorCount++;
                }
            } else {
                if (ShowSuccess) {
                    output << "Ok in range: [" << currentBlockIndex << ", "
                           << (currentBlockIndex + blocksInThisRequest - 1) << "]"
                           << Endl;
                }
            }

            remainingBlocks -= blocksInThisRequest;
            currentBlockIndex += blocksInThisRequest;
        }

        output << "Total errors caught: " << errorCount << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        const auto* diskId = ParseResultPtr->FindLongOptParseResult("disk-id");
        const auto* blockIdx =
            ParseResultPtr->FindLongOptParseResult("start-index");
        const auto* blockCount =
            ParseResultPtr->FindLongOptParseResult("blocks-count");

        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        if (!blockIdx) {
            STORAGE_ERROR("start-index is required");
            return false;
        }

        if (!blockCount) {
            STORAGE_ERROR("blocks-count is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCheckRangeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCheckRangeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
