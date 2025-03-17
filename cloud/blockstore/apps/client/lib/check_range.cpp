#include "check_range.h"

#include <cloud/blockstore/libs/common/block_range.h>
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

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError
ExtractStatusValues(const TString& jsonStr)
{
    NJson::TJsonValue json;
    ui32 code = S_OK;
    TString message;

    if (!NJson::ReadJsonTree(jsonStr, &json)) {
        return MakeError(E_ARGUMENT, "JSON parsing error");
    }

    NJson::TJsonValue* jsonCode = json.GetValueByPath("Status.Code");
    NJson::TJsonValue* jsonMsg = json.GetValueByPath("Status.Message");

    if (jsonCode){
        code = jsonCode->GetUIntegerRobust();
    }
    if (jsonMsg){
        message = jsonMsg->GetStringRobust();
    }

    return MakeError(code, message);
}

void SaveResultToFile(
    const TString& content,
    const TString& folderPath,
    const TString& fileName)
{
    TFsPath dir(folderPath);

    if (!dir.Exists()) {
        dir.MkDirs();
    }

    TString fullFilePath = dir / fileName;
    TOFStream file(
        fullFilePath,
        EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly);
    file.Write(content);
}

TString
CreateNextInput(TBlockRange64 range, TString& diskID, bool calculateChecksums)
{
    NJson::TJsonValue input;
    input["DiskId"] = diskID;
    input["StartIndex"] = range.Start;
    input["BlocksCount"] = range.Size();
    input["CalculateChecksums"] = calculateChecksums;
    return input.GetStringRobust();
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

        Opts.AddLongOption("calculate-checksums", "calulate checksums")
            .RequiredArgument("BOOL")
            .StoreResult(&CalculateChecksums);

        Opts.AddLongOption(
                "show-read-errors",
                "show logs for the intervals where errors occurred")
            .RequiredArgument("BOOL")
            .StoreResult(&ShowReadErrorsEnabled);

        Opts.AddLongOption(
                "save-results",
                "saving result of checkRange operations to the folder "
                "'./checkRange_$disk-id*', each request in own file")
            .RequiredArgument("BOOL")
            .StoreResult(&SaveResultsEnabled);

        Opts.AddLongOption(
                "folder-postfix",
                "select result folder postfix: full folder name - (checkRange_ "
                "+ $diskId + _$postfix)")
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

        ui64 diskBlockCount = result.GetVolume().GetBlocksCount();

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

            const auto range = TBlockRange64::MakeHalfOpenInterval(
                currentBlockIndex,
                currentBlockIndex + blocksInThisRequest);

            auto request = std::make_shared<NProto::TExecuteActionRequest>();

            request->SetAction("checkrange");
            request->SetInput(CreateNextInput(range, DiskId, CalculateChecksums));

            const auto requestId = GetRequestId(*request);
            auto result = WaitFor(ClientEndpoint->ExecuteAction(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)));

            ++requestCount;
            if (const auto& error = result.GetError(); HasError(error)) {
                if (result.GetError().GetCode() == E_ARGUMENT) {
                    output << "Wrong argument : " << FormatError(error) << Endl;
                    output << "Total errors caught: " << errorCount << Endl;
                    return true;
                }

                output << "CheckRange went wrong : "
                       << FormatError(result.GetError()) << Endl;
                return true;
            }

            const auto& error = ExtractStatusValues(
                result.GetOutput().Data());
            if (HasError(error) ) {
                errorCount++;
                if (ShowReadErrorsEnabled) {
                    output << "ReadBlocks error in range " << range << ": "
                           << FormatError(error) << Endl;
                }
            }

            if (SaveResultsEnabled) {
                TString folderPath = "./checkRange_" + DiskId;
                if (!FolderPostfix.Empty()) {
                    folderPath += "_" + FolderPostfix;
                }

                TString fileName =
                    Sprintf("result_%lu_%lu.json", range.Start, range.End);

                SaveResultToFile(result.GetOutput(), folderPath, fileName);
            }

            remainingBlocks -= blocksInThisRequest;
            currentBlockIndex += blocksInThisRequest;
        }

        output << "Total requests sended: " << requestCount << Endl;
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
