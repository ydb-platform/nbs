#include "get_changed_blocks.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/bitops.h>

#include <climits>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetChangedBlocksCommand final: public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;

    TString LowCheckpointId;
    TString HighCheckpointId;

public:
    TGetChangedBlocksCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&StartIndex);

        Opts.AddLongOption(
                "blocks-count",
                "maximum number of blocks stored in volume")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("low-checkpoint-id", "Checkpoint identifier")
            .RequiredArgument("STR")
            .StoreResult(&LowCheckpointId);

        Opts.AddLongOption("high-checkpoint-id", "Checkpoint identifier")
            .RequiredArgument("STR")
            .StoreResult(&HighCheckpointId);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading GetChangedBlocks request");
        auto request = std::make_shared<NProto::TGetChangedBlocksRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetDiskId(DiskId);
            request->SetStartIndex(StartIndex);
            request->SetBlocksCount(BlocksCount);
            request->SetLowCheckpointId(LowCheckpointId);
            request->SetHighCheckpointId(HighCheckpointId);
        }

        STORAGE_DEBUG("Sending GetChangedBlocks request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->GetChangedBlocks(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received GetChangedBlocks response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        for (const auto& byte: result.GetMask()) {
            for (int i = 0; i < CHAR_BIT; ++i) {
                if ((byte >> i) & 1) {
                    output << i << Endl;
                }
            }
        }

        return true;
    }

private:
    bool CheckOpts() const
    {
        const auto* diskId = ParseResultPtr->FindLongOptParseResult("disk-id");
        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        const auto* startIndex =
            ParseResultPtr->FindLongOptParseResult("start-index");
        if (!startIndex) {
            STORAGE_ERROR("Start index is required");
            return false;
        }

        const auto* blocksCount =
            ParseResultPtr->FindLongOptParseResult("blocks-count");
        if (!blocksCount) {
            STORAGE_ERROR("Blocks count is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewGetChangedBlocksCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TGetChangedBlocksCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
