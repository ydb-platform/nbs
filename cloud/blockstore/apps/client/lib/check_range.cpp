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

class TCheckRangeCommand final
    : public TCommand
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

        Opts.AddLongOption("block-idx", "first block index")
            .RequiredArgument("NUM")
            .StoreResult(&BlockIdx);

        Opts.AddLongOption("size", "first block index")
            .RequiredArgument("NUM")
            .StoreResult(&BlockCount);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        //auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading CheckRange request");
        auto request = std::make_shared<NProto::TCheckRangeRequest>();
        /*
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetDiskId(DiskId);
        }
        */
        request->SetDiskId(DiskId);
        request->SetBlockIdx(BlockIdx);
        request->SetBlockCount(BlockCount);

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
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "Sucsess: " << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        const auto* diskId = ParseResultPtr->FindLongOptParseResult("disk-id");
        const auto* blockIdx = ParseResultPtr->FindLongOptParseResult("block-idx");
        const auto* blockCount = ParseResultPtr->FindLongOptParseResult("size");
        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        if (!blockIdx) {
            STORAGE_ERROR("Block index is required");
            return false;
        }

        if (!blockCount) {
            STORAGE_ERROR("Block count is required");
            return false;
        }

        return true;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCheckRangeCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TCheckRangeCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
