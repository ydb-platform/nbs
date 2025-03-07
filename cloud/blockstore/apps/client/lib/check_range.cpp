#include "check_range.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <sstream>
namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeCommand final: public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;
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

        STORAGE_DEBUG("Sending StatVolume request");
        auto statVolumeRequest = std::make_shared<NProto::TStatVolumeRequest>();
        statVolumeRequest->SetDiskId(DiskId);
        const auto statVolumeRequestId = GetRequestId(*statVolumeRequest);
        auto result = WaitFor(ClientEndpoint->StatVolume(
            MakeIntrusive<TCallContext>(statVolumeRequestId),
            std::move(statVolumeRequest)));

        ui64 diskBlockCount = result.GetVolume().blockscount();

        STORAGE_DEBUG("Received StatVolume response");

        ui64 remainingBlocks = diskBlockCount;
        ui64 currentBlockIndex = StartIndex;

        Cout << remainingBlocks<< Endl;
        Cout << BlocksCount<< Endl;
        Cout << diskBlockCount<< Endl;
        Cout << "-----------"<< Endl;

        if (BlocksCount) {
            if (BlocksCount + StartIndex <= diskBlockCount) {
                remainingBlocks = BlocksCount;
            } else {
                remainingBlocks = diskBlockCount - StartIndex;
            }
        }
        Cout << remainingBlocks<< Endl;
        Cout << BlocksCount<< Endl;
        Cout << diskBlockCount<< Endl;


        if (BlocksPerRequest <= 0) {
            BlocksPerRequest = 1024;
        }

        while (remainingBlocks > 0) {

            ui32 blocksInThisRequest =
                std::min(remainingBlocks, BlocksPerRequest);

            auto request = std::make_shared<NProto::TExecuteActionRequest>();

            std::ostringstream reqStringStream;
            reqStringStream << "{" << "\"DiskId\": \"" << DiskId << "\", "
                            << "\"StartIndex\": " << currentBlockIndex << ", "
                            << "\"BlocksCount\": " << blocksInThisRequest
                            << "}";

            request->SetAction("checkrange");
            request->SetInput(reqStringStream.str());

            STORAGE_DEBUG("Sending CheckRange request");
            const auto requestId = GetRequestId(*request);
            auto result = WaitFor(ClientEndpoint->ExecuteAction(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)));

            STORAGE_DEBUG("Received CheckRange response");
            if (Proto) {
                SerializeToTextFormat(result, output);
                return true;
            }

                output<< result.GetOutput().Data();

            if (HasError(result)) {
                if (result.GetError().GetCode() == E_ARGUMENT) {
                    output << "Wrong argument : "
                           << FormatError(result.GetError()) << Endl;
                    output << "Total errors caught: " << errorCount << Endl;
                    return true;
                }

                output << "CheckRange went wrong : "
                       << FormatError(result.GetError()) << Endl;
                return true;
            } /*else {
                if (result.GetStatus().GetCode() != S_OK) {
                    errorCount++;
                    output << "ReadBlocks error in range [" << currentBlockIndex << ", "
                           << (currentBlockIndex + blocksInThisRequest - 1)
                           << "]: " << FormatError(result.GetStatus()) << Endl;
                } else if (ShowSuccess) {
                    output << "Ok in range: [" << currentBlockIndex << ", "
                           << (currentBlockIndex + blocksInThisRequest - 1)
                           << "]" << Endl;
                }
            }*/

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

        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
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
