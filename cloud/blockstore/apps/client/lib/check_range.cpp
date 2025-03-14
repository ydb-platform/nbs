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

void SaveResultToFile(const TString& content, const TString& folderPath, const TString& fileName) {
    TFsPath dir(folderPath);

    if (!dir.Exists()) {
        dir.MkDir();
    }

    TString fullFilePath = dir / fileName;
    TOFStream file(fullFilePath, EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr);
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
    bool ShowReadErrorsEnabled = false;
    bool SaveResultsEnabled = false;
    bool CalculateChecksums = false;
    TString FolderPostfix;

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
                "show-read-errors",
                "show logs for the intervals where request was successfully "
                "completed")
            .RequiredArgument("BOOL")
            .StoreResult(&ShowReadErrorsEnabled);

        Opts.AddLongOption(
                "save-results",
                "saving result of checkRange operations to the folder './checkRange', each request in own file")
            .RequiredArgument("BOOL")
            .StoreResult(&SaveResultsEnabled);

        Opts.AddLongOption(
                "calculate-checksums")
            .RequiredArgument("BOOL")
            .StoreResult(&CalculateChecksums);

        Opts.AddLongOption(
                "folder-postfix",
                "select folder postfix: full folder name - (checkRange_ + "
                "DiskId + postfix)")
            .RequiredArgument("STR")
            .StoreResult(&FolderPostfix);
    }

protected:
    bool DoExecute() override
    {
        if (!CheckOpts()) {
            return false;
        }

        auto& output = GetOutputStream();

        ui32 errorCount = 0;
        ui32 requestCount = 0;

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
            BlocksPerRequest = 1024;
        }

        while (remainingBlocks > 0) {
            ui32 blocksInThisRequest =
                std::min(remainingBlocks, BlocksPerRequest);

            auto request = std::make_shared<NProto::TExecuteActionRequest>();

            std::ostringstream input;
            input << "{" << "\"DiskId\": \"" << DiskId << "\", "
                  << "\"StartIndex\": " << currentBlockIndex << ", "
                  << "\"BlocksCount\": " << blocksInThisRequest << ", "
                  << "\"CalculateChecksums\": "
                  << (CalculateChecksums ? "true" : "false") << "}";

            request->SetAction("checkrange");
            request->SetInput(input.str());

            const auto requestId = GetRequestId(*request);
            auto result = WaitFor(ClientEndpoint->ExecuteAction(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)));

            ++requestCount;
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

                if (statusCode != S_OK && ShowReadErrorsEnabled) {
                    errorCount++;
                    output << "ReadBlocks error in range [" << currentBlockIndex << ", "
                           << (currentBlockIndex + blocksInThisRequest - 1)
                           << "]: " << FormatError(MakeError(statusCode, statusMessage)) << Endl;
                }
            }

            if (SaveResultsEnabled) {
                TString folderPath = "./checkRange_" + DiskId;
                if (!FolderPostfix.Empty()) {
                    folderPath += "_" + FolderPostfix;
                }

                TString fileName = Sprintf(
                    "result_%lu_%lu.json",
                    currentBlockIndex,
                    currentBlockIndex + blocksInThisRequest - 1);

                SaveResultToFile(
                    result.GetOutput().Data(),
                    folderPath,
                    fileName);
            }

            remainingBlocks -= blocksInThisRequest;
            currentBlockIndex += blocksInThisRequest;
        }

        output << "Total requests caught: " << requestCount << Endl;
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
