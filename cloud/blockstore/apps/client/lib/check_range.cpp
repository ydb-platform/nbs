#include "check_range.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/easy_parse/json_easy_parser.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

#include <sstream>
namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

void ExtractStatusValues(const TString& jsonStr, ui32& code, TString& message) {
    NJson::TJsonValue json;

    if (!NJson::ReadJsonTree(jsonStr, &json)) {
        std::cerr << "JSON parsing error" << std::endl;
        return;
    }

    NJson::TJsonValue* jsonCode = json.GetValueByPath("Status.Code");
    NJson::TJsonValue* jsonMsg = json.GetValueByPath("Status.Message");

    if (jsonCode) {
        code = jsonCode->GetUIntegerSafe();
    }

    if (jsonMsg) {
        message = jsonMsg->GetStringSafe();
    }
}

void SaveResultToFile(const TString& content, const TString& diskID, ui64 startIndex, ui64 blocksInThisRequest) {
    const TString folder = "./checkRange_" + diskID;
    TFsPath dir(folder);

    if (!dir.Exists()) {
        dir.MkDir();
    }

    TString fileName = Sprintf("%s/result_%lu_%lu.json", folder.c_str(), startIndex, startIndex + blocksInThisRequest - 1);
    TOFStream file(fileName, EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr);
    file.Write(content);
}

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeCommand final: public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;
    ui64 BlocksPerRequest = 0;
    bool ShowSuccessEnabled = false;
    bool SaveResultsEnabled = false;

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
            .StoreResult(&ShowSuccessEnabled);

        Opts.AddLongOption(
                "save-results",
                "saving result of checkRange operations to the folder './checkRange', each request in own file")
            .RequiredArgument("BOOL")
            .StoreResult(&SaveResultsEnabled);
    }

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        auto& output = GetOutputStream();

        ui32 errorCount = 0;

        auto statVolumeRequest = std::make_shared<NProto::TStatVolumeRequest>();
        statVolumeRequest->SetDiskId(DiskId);
        const auto statVolumeRequestId = GetRequestId(*statVolumeRequest);
        auto result = WaitFor(ClientEndpoint->StatVolume(
            MakeIntrusive<TCallContext>(statVolumeRequestId),
            std::move(statVolumeRequest)));

        if (HasError(result)) {
            output << "StatVolume error: " << FormatError(result.GetError())
                   << Endl;
            return false;
        }

        ui64 diskBlockCount = result.GetVolume().blockscount();

        ui64 remainingBlocks = diskBlockCount;
        ui64 currentBlockIndex = StartIndex;

        if (BlocksCount) {
            if (BlocksCount + StartIndex <= diskBlockCount) {
                remainingBlocks = BlocksCount;
            } else {
                remainingBlocks = diskBlockCount - StartIndex;
            }
        }

        if (BlocksPerRequest <= 0) {
            BlocksPerRequest = 512;
        }

        while (remainingBlocks > 0) {

            ui32 blocksInThisRequest =
                std::min(remainingBlocks, BlocksPerRequest);

            auto request = std::make_shared<NProto::TExecuteActionRequest>();

            std::ostringstream input;
            input << "{" << "\"DiskId\": \"" << DiskId << "\", "
                            << "\"StartIndex\": " << currentBlockIndex << ", "
                            << "\"BlocksCount\": " << blocksInThisRequest
                            << "}";

            request->SetAction("checkrange");
            request->SetInput(input.str());

            const auto requestId = GetRequestId(*request);
            auto result = WaitFor(ClientEndpoint->ExecuteAction(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)));


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

            } else {
                ui32 statusCode;
                TString statusMessage;
                ExtractStatusValues(result.GetOutput().Data(), statusCode, statusMessage);

                if (statusCode != S_OK) {
                    errorCount++;
                    output << "ReadBlocks error in range [" << currentBlockIndex << ", "
                           << (currentBlockIndex + blocksInThisRequest - 1)
                           << "]: " << FormatError(MakeError(statusCode, statusMessage)) << Endl;
                } else if (ShowSuccessEnabled) {
                    output << "Ok in range: [" << currentBlockIndex << ", "
                           << (currentBlockIndex + blocksInThisRequest - 1)
                           << "]" << Endl;
                }
            }

            if (SaveResultsEnabled) {
                SaveResultToFile(
                    result.GetOutput().Data(),
                    DiskId,
                    currentBlockIndex,
                    blocksInThisRequest);
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
