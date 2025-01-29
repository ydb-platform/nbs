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
    ui64 BlockIdx;
    ui64 BlockCount;

public:
    TCheckRangeCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&BlockIdx);

        Opts.AddLongOption("blocks-count", "number of blocks to check")
            .RequiredArgument("NUM")
            .StoreResult(&BlockCount);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& output = GetOutputStream();

        STORAGE_DEBUG("Handling CheckRange");

        ui32 errorCount = 0;

        const ui32 maxBlocksPerRequest = 4 * 1024;
        ui32 remainingBlocks = BlockCount;
        ui64 currentBlockIdx = BlockIdx;

        while (remainingBlocks > 0) {
            bool isOutOfBound = false;

            ui32 blocksInThisRequest =
                std::min(remainingBlocks, maxBlocksPerRequest);

            auto request = std::make_shared<NProto::TCheckRangeRequest>();
            request->SetDiskId(DiskId);
            request->SetBlockIdx(currentBlockIdx);
            request->SetBlockCount(blocksInThisRequest);

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
                    isOutOfBound = true;
                } else {
                    output << "Error in range [" << currentBlockIdx << ", "
                           << (currentBlockIdx + blocksInThisRequest - 1)
                           << "]: " << FormatError(result.GetError()) << Endl;
                    errorCount++;
                }
            } else {
                output << "Ok in range: [" << currentBlockIdx << ", "
                       << (currentBlockIdx + blocksInThisRequest - 1) << "]"
                       << Endl;
            }

            remainingBlocks -= blocksInThisRequest;
            currentBlockIdx += blocksInThisRequest;
            if (isOutOfBound) {
                break;
            }
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
        const auto* blockCount = ParseResultPtr->FindLongOptParseResult("blocks-count");
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
